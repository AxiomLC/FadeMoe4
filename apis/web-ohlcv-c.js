/* ==========================================
 * web-ohlcv-c.js   8 Oct 2025
 * Continuous WebSocket OHLCV Collector
 *
 * Streams 1m candle data from Binance, Bybit, and OKX
 * Inserts closed candles into the database
 * Logs status once per minute per exchange
 * ========================================== */

const WebSocket = require('ws');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');
require('dotenv').config();

const SCRIPT_NAME = 'web-ohlcv-c.js';

/* ==========================================
 * MT (MARKET TREND) CONFIGURATION
 * ========================================== */
// MT_SYMBOLS: Base symbols for synthetic MT index (from Binance 'bin-ohlcv' 1m OHLCV)
// Add/remove dynamically; OHLC = avg per field, v = sum across available
const MT_SYMBOLS = ['ETH', 'BTC', 'XRP', 'SOL'];
const MT_SYMBOL = 'MT'; // Insert as this symbol under 'bin-ohlcv'

/* ==========================================
 * EXCHANGE CONFIGURATION
 * ========================================== */
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-ohlcv',
    DB_INTERVAL: '1m',
    mapSymbol: sym => `${sym.toLowerCase()}usdt`,
    getWsUrl: sym => `wss://fstream.binance.com/ws/${sym.toLowerCase()}usdt@kline_1m`
  },
  BYBIT: {
    PERPSPEC: 'byb-ohlcv',
    WS_URL: 'wss://stream.bybit.com/v5/public/linear',
    DB_INTERVAL: '1m',
    // Bybit uses 1000X prefix for certain meme coins
    mapSymbol: sym => {
      const memeCoins = ['BONK', 'PEPE', 'FLOKI', 'TOSHI'];
      return memeCoins.includes(sym) ? `1000${sym}USDT` : `${sym}USDT`;
    },
    // Reverse mapping for received data
    unmapSymbol: topic => {
      const memeCoins = ['BONK', 'PEPE', 'FLOKI', 'TOSHI'];
      for (const coin of memeCoins) {
        if (topic === `1000${coin}USDT`) return coin;
      }
      return topic.replace('USDT', '');
    }
  },
  OKX: {
    PERPSPEC: 'okx-ohlcv',
    WS_URL: 'wss://ws.okx.com:8443/ws/v5/business',
    DB_INTERVAL: '1m',
    mapSymbol: sym => `${sym}-USDT-SWAP`
  }
};

// Track completed symbols per exchange per minute
const completedSymbols = {
  'bin-ohlcv': new Set(),
  'byb-ohlcv': new Set(),
  'okx-ohlcv': new Set()
};

// Track Bybit active subscriptions
const bybitActiveSymbols = new Set();

/* ==========================================
 * MT MODULE: Real-Time Synthetic MT Index Creation
 * ========================================== */
// In-memory tracking: Latest full OHLCV per MT symbol (for forward-fill and aggregation)
// Key: baseSymbol, Value: {ts: BigInt, o: number, h: number, l: number, c: number, v: number}
let mtLatestData = new Map(); // e.g., mtLatestData.set('ETH', {ts: BigInt, o: 2500, h: 2520, l: 2490, c: 2510, v: 12345.67})

// Initialize last known for each MT symbol (empty on start; will populate from streams)
MT_SYMBOLS.forEach(sym => mtLatestData.set(sym, null));

// Compute MT record for a given ts: Average OHLC per field, sum volumes (forward-fill per symbol)
function computeMTRecord(currentTs) {
  let totalO = 0, totalH = 0, totalL = 0, totalC = 0, totalV = 0;
  let count = 0; // Count of symbols with valid full candle

  for (const sym of MT_SYMBOLS) {
    const latest = mtLatestData.get(sym);
    if (latest && latest.ts <= currentTs && 
        latest.o !== null && latest.h !== null && latest.l !== null && latest.c !== null && latest.v !== null &&
        !isNaN(latest.o) && !isNaN(latest.h) && !isNaN(latest.l) && !isNaN(latest.c) && !isNaN(latest.v)) {
      // Forward-fill: Use latest full candle <= currentTs (real-time streams should align, but safe)
      totalO += latest.o;
      totalH += latest.h;
      totalL += latest.l;
      totalC += latest.c;
      totalV += latest.v; // Sum volumes for total market activity
      count++;
    }
  }

  if (count === 0) return null; // No valid data available - skip

  const avgO = totalO / count;
  const avgH = totalH / count;
  const avgL = totalL / count;
  const avgC = totalC / count;

  return {
    ts: currentTs, // Same ts as the minute's closes
    symbol: MT_SYMBOL,
    source: 'bin-ohlcv',
    perpspec: 'bin-ohlcv',
    interval: '1m',
    o: avgO,      // Avg open
    h: avgH,      // Avg high
    l: avgL,      // Avg low
    c: avgC,      // Avg close
    v: totalV     // Sum of volumes (total activity)
  };
}

// Insert MT record (single record per minute)
async function insertMT(perpspec, mtRecord) {
  try {
    if (!mtRecord) return; // Skip if null

    await dbManager.insertData(perpspec, [mtRecord]);
    console.log(`✅ MT Market Trend token creation 1m poll`);
  } catch (error) {
    await apiUtils.logScriptError(
      dbManager,
      SCRIPT_NAME,
      'INTERNAL',
      'MT_INSERT_FAILED',
      error.message,
      { perpspec: 'bin-ohlcv', symbol: MT_SYMBOL }
    );
    console.error(`❌ MT insert failed: ${error.message}`);
  }
}

/* ==========================================
 * PARSE -- INSERTION
 * ========================================== */

async function processAndInsert(exchange, baseSymbol, rawData) {
  const config = EXCHANGE_CONFIG[exchange];
  if (!config) return;

  const perpspec = config.PERPSPEC;
  let record = null;
//=========================BINANCE=============================
  try {
    if (exchange === 'BINANCE') {
      const k = rawData.k;
      const ts = apiUtils.toMillis(BigInt(k.t));

      record = {
        ts,
        symbol: baseSymbol,
        source: perpspec,
        perpspec,
        interval: config.DB_INTERVAL,
        o: parseFloat(k.o),
        h: parseFloat(k.h),
        l: parseFloat(k.l),
        c: parseFloat(k.c),
        v: parseFloat(k.q) // was = (k.v) wrong, contract
      };

      // MT Update: If this is an MT symbol, track full latest OHLCV for aggregation
      if (MT_SYMBOLS.includes(baseSymbol)) {
        mtLatestData.set(baseSymbol, { 
          ts, 
          o: record.o, 
          h: record.h, 
          l: record.l, 
          c: record.c, 
          v: record.v 
        });
      }
//=========================BYBIT===============================
    } else if (exchange === 'BYBIT') {
      const k = rawData.data && rawData.data[0];
      if (!k) return;

      const ts = BigInt(k.start);

      record = {
        ts,
        symbol: baseSymbol,
        source: perpspec,
        perpspec,
        interval: config.DB_INTERVAL,
        o: parseFloat(k.open),
        h: parseFloat(k.high),
        l: parseFloat(k.low),
        c: parseFloat(k.close),
        v: parseFloat(k.turnover || 0) //was (k.volume || 0), wrong, contract
      };
//=========================OKX================================
    } else if (exchange === 'OKX') {
      const c = rawData.data && rawData.data[0];
      if (!c) return;

      const ts = apiUtils.toMillis(BigInt(c[0]));

      record = {
        ts,
        symbol: baseSymbol,
        source: perpspec,
        perpspec,
        interval: config.DB_INTERVAL,
        o: parseFloat(c[1]),
        h: parseFloat(c[2]),
        l: parseFloat(c[3]),
        c: parseFloat(c[4]),
        v: parseFloat(c[7] || 0) // was (vol) wrong, contract
      };
    }

    if (!record) return;

    // Insert into database
    await dbManager.insertData(perpspec, [record]);

    // Track completion for status logging
    completedSymbols[perpspec].add(baseSymbol);

    // Log status once all symbols for this exchange complete
    const expectedCount = (exchange === 'BYBIT') ? bybitActiveSymbols.size : perpList.length;
    
    if (completedSymbols[perpspec].size === expectedCount) {
      const message = `${perpspec} 1min pull for ${expectedCount} symbols`;
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message);
      console.log(message);

      // MT Creation: After Binance minute completes (runs "last"), compute/insert MT for this ts
      if (perpspec === 'bin-ohlcv') {
        // Use the latest ts from any MT symbol (they should align for the minute)
        const latestTs = Array.from(mtLatestData.values()).find(data => data && data.ts)?.ts || null;
        if (latestTs) {
          const mtRecord = computeMTRecord(latestTs);
          await insertMT('bin-ohlcv', mtRecord);
        }
      }

      completedSymbols[perpspec].clear();
    }

  } catch (error) {
    await apiUtils.logScriptError(
      dbManager,
      SCRIPT_NAME,
      'INTERNAL',
      'INSERT_FAILED',
      error.message,
      { perpspec, symbol: baseSymbol }
    );
  }
}

/* ==========================================
 * BINANCE WEBSOCKET
 * One connection per symbol
 * ========================================== */
async function binanceWebSocket() {
  for (const baseSymbol of perpList) {
    const config = EXCHANGE_CONFIG.BINANCE;
    const wsUrl = config.getWsUrl(baseSymbol);

    const ws = new WebSocket(wsUrl);

    ws.on('message', async (data) => {
      try {
        const message = JSON.parse(data);
        if (message.k && message.k.x) {
          await processAndInsert('BINANCE', baseSymbol, message);
        }
      } catch (error) {
        await apiUtils.logScriptError(
          dbManager,
          SCRIPT_NAME,
          'API',
          'PARSE_ERROR',
          error.message,
          { perpspec: config.PERPSPEC, symbol: baseSymbol }
        );
      }
    });

    ws.on('error', async (error) => {
      await apiUtils.logScriptError(
        dbManager,
        SCRIPT_NAME,
        'API',
        'WEBSOCKET_ERROR',
        error.message,
        { perpspec: config.PERPSPEC, symbol: baseSymbol }
      );
    });

    ws.on('close', () => {
      setTimeout(() => binanceWebSocket(), 5000);
    });
  }
}

/* ==========================================
 * BYBIT WEBSOCKET 
 * CRITICAL: Bybit requires individual subscriptions per symbol.
 * Batch subscriptions with invalid symbols cause fail. Subscribing individually allows
 * valid symbols to work while gracefully handling invalid ones.
 * 
 * Symbol Naming: Bybit uses "1000X" prefix forsome meme coins e.g., BONK → 1000BONKUSDT)
 * ========================================== */
async function bybitWebSocket() {
  const config = EXCHANGE_CONFIG.BYBIT;
  const ws = new WebSocket(config.WS_URL);
  let isConnected = false;

  ws.on('open', () => {
    isConnected = true;
    
    // Subscribe to each symbol individually
    perpList.forEach((sym, index) => {
      const bybitSymbol = config.mapSymbol(sym);
      const subscribeMsg = {
        op: 'subscribe',
        args: [`kline.1.${bybitSymbol}`]
      };
      
      // Stagger subscriptions to avoid rate limits
      setTimeout(() => {
        if (isConnected) {
          ws.send(JSON.stringify(subscribeMsg));
        }
      }, index * 50);
    });
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);

      // Handle subscription responses
      if (message.op === 'subscribe') {
        if (!message.success) {
          const failedTopic = message.ret_msg || '';
          console.log(`⚠️  Bybit subscription failed: ${failedTopic}`);
        }
        return;
      }

      // Handle ping/pong
      if (message.op === 'ping') {
        ws.send(JSON.stringify({ op: 'pong' }));
        return;
      }

      // Validate data exists
      if (!message.data || !Array.isArray(message.data) || message.data.length === 0) {
        return;
      }

      // Extract symbol from topic
      const topic = message.topic || '';
      if (!topic.startsWith('kline.1.')) return;
      
      const bybitSymbol = topic.split('.')[2];
      const baseSymbol = config.unmapSymbol(bybitSymbol);

      // Track active symbols (ones we're receiving data for)
      if (!bybitActiveSymbols.has(baseSymbol)) {
        bybitActiveSymbols.add(baseSymbol);
      }

      const kline = message.data[0];
      
      // CRITICAL: Only confirmed (closed) candles
      // Bybit sends confirm=false for in-progress candles
      // and confirm=true when the candle period completes
      if (kline && kline.confirm === true) {
        await processAndInsert('BYBIT', baseSymbol, message);
      }

    } catch (error) {
      await apiUtils.logScriptError(
        dbManager,
        SCRIPT_NAME,
        'API',
        'PARSE_ERROR',
        error.message,
        { perpspec: config.PERPSPEC }
      );
    }
  });

  ws.on('error', async (error) => {
    await apiUtils.logScriptError(
      dbManager,
      SCRIPT_NAME,
      'API',
      'WEBSOCKET_ERROR',
      error.message,
      { perpspec: config.PERPSPEC }
    );
  });

  ws.on('close', () => {
    isConnected = false;
    bybitActiveSymbols.clear();
    setTimeout(() => bybitWebSocket(), 5000);
  });
}

/* ==========================================
 * OKX WEBSOCKET
 * Single connection, multiple subscriptions
 * ========================================== */
async function okxWebSocket() {
  const config = EXCHANGE_CONFIG.OKX;
  const ws = new WebSocket(config.WS_URL);

  ws.on('open', () => {
    const args = perpList.map(sym => ({
      channel: 'candle1m',
      instId: config.mapSymbol(sym)
    }));
    ws.send(JSON.stringify({ op: 'subscribe', args }));
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);

      if (!message.data || !Array.isArray(message.data) || message.data.length === 0) return;

      const instId = message.arg && message.arg.instId;
      if (!instId) return;

      const baseSymbol = perpList.find(sym => config.mapSymbol(sym) === instId);
      if (!baseSymbol) return;

      const c = message.data[0];
      const confirm = (c.length > 8) ? c[8] : undefined;

      if (confirm === "1" || confirm === true) {
        await processAndInsert('OKX', baseSymbol, message);
      }
    } catch (error) {
      await apiUtils.logScriptError(
        dbManager,
        SCRIPT_NAME,
        'API',
        'PARSE_ERROR',
        error.message,
        { perpspec: config.PERPSPEC }
      );
    }
  });

  ws.on('error', async (error) => {
    await apiUtils.logScriptError(
      dbManager,
      SCRIPT_NAME,
      'API',
      'WEBSOCKET_ERROR',
      error.message,
      { perpspec: config.PERPSPEC }
    );
  });

  ws.on('close', () => {
    setTimeout(() => okxWebSocket(), 5000);
  });
}

/* ==========================================
 * MAIN EXECUTION
 * ========================================== */
async function execute() {
  console.log(`🚀 Starting ${SCRIPT_NAME} - WebSocket OHLCV streaming`);

  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} connected`);

  // Start all WebSocket connections
  binanceWebSocket();
  bybitWebSocket();
  okxWebSocket();

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log(`\n${SCRIPT_NAME} received SIGINT, stopping...`);
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'stopped', `${SCRIPT_NAME} stopped smoothly`);
    process.exit(0);
  });
}

/* ==========================================
 * MODULE ENTRY POINT
 * ========================================== */
if (require.main === module) {
  execute()
    .catch(err => {
      console.error('💥 WebSocket OHLCV streaming failed:', err);
      apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'INITIALIZATION_FAILED', err.message);
      process.exit(1);
    });
}

module.exports = { execute };