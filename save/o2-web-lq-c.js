/* ==========================================
 * web-lq-c.js   6 Oct 2025
 * Continuous WebSocket Liquidation Collector
 *
 * Streams liquidation data from Binance, Bybit, and OKX
 * Inserts liquidation events into the database
 * Tracks liquidation side, price, and quantity
 * ========================================== */

const WebSocket = require('ws');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');
require('dotenv').config();

const SCRIPT_NAME = 'web-lq-c.js';

/* ==========================================
 * EXCHANGE CONFIGURATION
 * Defines WebSocket URLs, perpspec names, and symbol mapping
 * ========================================== */
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-lq',
    WS_BASE: 'wss://fstream.binance.com/ws',
    mapSymbol: sym => `${sym.toLowerCase()}usdt`,
    getWsUrl: sym => `wss://fstream.binance.com/ws/${sym.toLowerCase()}usdt@forceOrder`
  },
  BYBIT: {
    PERPSPEC: 'byb-lq',
    WS_URL: 'wss://stream.bybit.com/v5/public/linear',
    mapSymbol: sym => `${sym}USDT`
  },
  OKX: {
    PERPSPEC: 'okx-lq',
    WS_URL: 'wss://ws.okx.com:8443/ws/v5/public',
    mapSymbol: sym => `${sym}-USDT-SWAP`
  }
};

// Track liquidations per exchange for periodic status logging
const liquidationCounts = {
  'bin-lq': 0,
  'byb-lq': 0,
  'okx-lq': 0
};

// Last status log time per perpspec
const lastStatusLog = {
  'bin-lq': Date.now(),
  'byb-lq': Date.now(),
  'okx-lq': Date.now()
};

const STATUS_LOG_INTERVAL = 60000; // 1 minute

/* ==========================================
 * DATA PROCESSING & INSERTION
 * Parse WebSocket messages and insert into database
 * ========================================== */

/**
 * Process and insert liquidation record into database
 */
async function processAndInsert(exchange, baseSymbol, rawData) {
  const config = EXCHANGE_CONFIG[exchange];
  if (!config) return;

  const perpspec = config.PERPSPEC;
  let record = null;

  try {
    if (exchange === 'BINANCE') {
      const o = rawData.o;
      const ts = apiUtils.toMillis(BigInt(o.T));
      
      // Binance side: BUY = long liquidated, SELL = short liquidated
      const side = o.S === 'BUY' ? 'short' : 'long';
      const price = parseFloat(o.p);
      const quantity = parseFloat(o.q);

      record = {
        ts,
        symbol: baseSymbol,
        source: perpspec,
        perpspec,
        lqside: side,
        lqprice: price,
        lqqty: quantity
      };

    } else if (exchange === 'BYBIT') {
      const ts = apiUtils.toMillis(BigInt(rawData.T));
      
      // Bybit side: Buy = long liquidated, Sell = short liquidated
      const side = rawData.S === 'Buy' ? 'long' : 'short';
      const price = parseFloat(rawData.p);
      const quantity = parseFloat(rawData.v);

      record = {
        ts,
        symbol: baseSymbol,
        source: perpspec,
        perpspec,
        lqside: side,
        lqprice: price,
        lqqty: quantity
      };

    } else if (exchange === 'OKX') {
      const ts = apiUtils.toMillis(BigInt(rawData.ts));
      
      // OKX side: buy = short liquidated, sell = long liquidated
      const side = rawData.side === 'buy' ? 'short' : 'long';
      const price = parseFloat(rawData.bkPx);
      const quantity = parseFloat(rawData.sz);

      record = {
        ts,
        symbol: baseSymbol,
        source: perpspec,
        perpspec,
        lqside: side,
        lqprice: price,
        lqqty: quantity
      };
    }

    if (!record) return;

    // Insert into database
    await dbManager.insertData(perpspec, [record]);

    // Track liquidation count
    liquidationCounts[perpspec]++;

    // Periodic status logging (every 1 minute)
    const now = Date.now();
    if (!lastStatusLog[perpspec] || now - lastStatusLog[perpspec] >= STATUS_LOG_INTERVAL) {
        const message = `${perpspec} posted ${liquidationCounts[perpspec] || 0} liquidations`;
        await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message);
        console.log(message);
        lastStatusLog[perpspec] = now;
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

    ws.on('open', () => {
      // Connection established
    });

    ws.on('message', async (data) => {
      try {
        const message = JSON.parse(data);
        if (message.o) {
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
      setTimeout(() => binanceWebSocket(), 5000);
    });

    ws.on('close', () => {
      setTimeout(() => binanceWebSocket(), 5000);
    });
  }
}

/* ==========================================
 * BYBIT WEBSOCKET
 * Single connection, multiple subscriptions
 * ========================================== */
async function bybitWebSocket() {
  const config = EXCHANGE_CONFIG.BYBIT;
  const ws = new WebSocket(config.WS_URL);

  // ========= below here opanAI fix for Bybit ===========
  ws.on('open', () => {
  const args = perpList.map(sym => `allLiquidation.${config.mapSymbol(sym)}`);

  // commented out- console.log("📡 Subscribing to Bybit topics:", args);

  // Chunk into groups of 3
  for (let i = 0; i < args.length; i += 3) {
    const chunk = args.slice(i, i + 3);
    ws.send(JSON.stringify({ op: 'subscribe', args: chunk }));
    // commented out - console.log("➡️ Sent subscription chunk:", chunk);
  }
 /* ====Commented out original ============== 
  ws.on('open', () => {
    const args = perpList.map(sym => `allLiquidation.${config.mapSymbol(sym)}`);
    ws.send(JSON.stringify({ op: 'subscribe', args }));
*/
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);

      if (!message.data || !Array.isArray(message.data)) return;

      const topic = message.topic || '';
      const symbolFromTopic = topic.split('.')[1];

      const baseSymbol = perpList.find(sym => config.mapSymbol(sym) === symbolFromTopic);
      if (!baseSymbol) return;

      // Process each liquidation in the array
      for (const lqEvent of message.data) {
        await processAndInsert('BYBIT', baseSymbol, lqEvent);
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
    setTimeout(() => bybitWebSocket(), 5000);
  });

  ws.on('close', () => {
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
      channel: 'liquidation-orders',
      instType: 'SWAP',
      instId: config.mapSymbol(sym)
    }));
    ws.send(JSON.stringify({ op: 'subscribe', args }));
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);

      if (!message.data || !Array.isArray(message.data) || message.data.length === 0) return;

      // Loop through each instrument in data array
      for (const instrument of message.data) {
        const instId = instrument.instId;
        if (!instId) continue;

        const baseSymbol = perpList.find(sym => config.mapSymbol(sym) === instId);
        if (!baseSymbol) continue;

        // Loop through liquidation details (nested array)
        if (!instrument.details || !Array.isArray(instrument.details)) continue;
        
        for (const lqEvent of instrument.details) {
          await processAndInsert('OKX', baseSymbol, lqEvent);
        }
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
    setTimeout(() => okxWebSocket(), 5000);
  });

  ws.on('close', () => {
    setTimeout(() => okxWebSocket(), 5000);
  });
}

/* ==========================================
 * MAIN EXECUTION
 * Start all WebSocket connections and log status
 * ========================================== */
async function execute() {
  console.log(`🚀 Starting ${SCRIPT_NAME} - WebSocket liquidation streaming`);

  // Status: Started
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} connected`);

  // Start all WebSocket connections
  binanceWebSocket();
  bybitWebSocket();
  okxWebSocket();

  // Graceful shutdown handler
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
      console.error('💥 WebSocket liquidation streaming failed:', err);
      apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'INITIALIZATION_FAILED', err.message);
      process.exit(1);
    });
}

module.exports = { execute };