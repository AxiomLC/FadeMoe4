/* ==========================================
 * web-lq-c.js   15 Oct 2025
 * Continuous WebSocket Liquidation Collector with 1-minute Bucketing
 *
 * Streams liquidation data from Binance, Bybit, and OKX
 * Aggregates liquidation events into 1-minute buckets per symbol
 * Inserts aggregated liquidation data into the database
 * Tracks liquidation side (majority), average price, and total quantity per minute
 * ========================================== */

const WebSocket = require('ws');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');
require('dotenv').config();

const SCRIPT_NAME = 'web-lq-c.js';
const STATUS_COLOR = '\x1b[96m'; // Bright cyan (blue-green)
const RESET = '\x1b[0m';

const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-lq',
    WS_BASE: 'wss://fstream.binance.com/ws/ws',
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

// Track liquidation counts for status logging
const liquidationCounts = {
  'bin-lq': 0,
  'byb-lq': 0,
  'okx-lq': 0
};

const lastStatusLog = {
  'bin-lq': Date.now(),
  'byb-lq': Date.now(),
  'okx-lq': Date.now()
};

const STATUS_LOG_INTERVAL = 60000; // 1 minute

// In-memory buckets for aggregation: { perpspec: { symbol: { bucketTs: bucketData } } }
const buckets = {
  'bin-lq': new Map(),
  'byb-lq': new Map(),
  'okx-lq': new Map()
};

// Helper: Get bucket timestamp (floor to nearest minute)
function getBucketTs(ts) {
  return Math.floor(ts / 60000) * 60000;
}

// Helper: Update or create bucket for given perpspec and symbol
function updateBucket(perpspec, symbol, ts, side, price, qty) {
  if (!buckets[perpspec].has(symbol)) {
    buckets[perpspec].set(symbol, new Map());
  }
  const symbolBuckets = buckets[perpspec].get(symbol);
  const bucketTs = getBucketTs(ts);

  if (!symbolBuckets.has(bucketTs)) {
    symbolBuckets.set(bucketTs, {
      ts: bucketTs,
      symbol,
      perpspec,
      lqsideCounts: { long: 0, short: 0 },
      lqpriceSum: 0,
      lqpriceCount: 0,
      lqqtySum: 0
    });
  }

  const bucket = symbolBuckets.get(bucketTs);

  // Count sides
  if (side === 'long') {
    bucket.lqsideCounts.long += 1;
  } else if (side === 'short') {
    bucket.lqsideCounts.short += 1;
  }

  // Sum price and qty
  bucket.lqpriceSum += price;
  bucket.lqpriceCount += 1;
  bucket.lqqtySum += qty;
}

// Helper: Determine majority side with tie-breaker
async function determineLqside(bucket) {
  const { long, short } = bucket.lqsideCounts;
  if (long > short) return 'long';
  if (short > long) return 'short';

  // Tie-breaker: compare avg price to OHLCV high/low for symbol and bucketTs
  const avgPrice = bucket.lqpriceSum / bucket.lqpriceCount;

  // Query OHLCV for symbol and bucketTs
  const query = `
    SELECT h, l FROM perp_data
    WHERE symbol = $1 AND perpspec LIKE '%ohlcv'
      AND ts = $2
    LIMIT 1
  `;
  try {
    const result = await dbManager.pool.query(query, [bucket.symbol, BigInt(bucket.ts)]);
    if (result.rows.length === 0) {
      // No OHLCV data, default to 'long'
      return 'long';
    }
    const { h, l } = result.rows[0];
    if (h === null || l === null) return 'long';

    // Determine which is closer
    const distToLow = Math.abs(avgPrice - parseFloat(l));
    const distToHigh = Math.abs(avgPrice - parseFloat(h));
    return distToLow <= distToHigh ? 'long' : 'short';
  } catch (error) {
    console.error(`Error fetching OHLCV for tie-breaker: ${error.message}`);
    return 'long'; // default fallback
  }
}

// Flush buckets older than threshold (e.g., older than now - 1 minute)
async function flushBuckets() {
  const now = Date.now();
  const threshold = now - 60000; // 1 minute ago

  for (const perpspec of Object.keys(buckets)) {
    const symbolBucketsMap = buckets[perpspec];
    for (const [symbol, bucketMap] of symbolBucketsMap.entries()) {
      for (const [bucketTs, bucket] of bucketMap.entries()) {
        if (bucketTs < threshold) {
          // Determine lqside with tie-breaker if needed
          bucket.lqside = await determineLqside(bucket);
          bucket.lqprice = bucket.lqpriceSum / bucket.lqpriceCount;
          bucket.lqqty = bucket.lqqtySum;

          // Prepare record for DB insert - ts as BigInt here
          const record = {
            ts: BigInt(bucket.ts),
            symbol: bucket.symbol,
            source: perpspec,
            perpspec,
            lqside: bucket.lqside,
            lqprice: bucket.lqprice,
            lqqty: bucket.lqqty
          };

          try {
            await dbManager.insertData(perpspec, [record]);
            liquidationCounts[perpspec] = (liquidationCounts[perpspec] || 0) + 1;
          } catch (error) {
            console.error(`Error inserting bucketed liquidation for ${perpspec} ${symbol} at ${bucket.ts}: ${error.message}`);
          }

          // Remove flushed bucket
          bucketMap.delete(bucketTs);
        }
      }
      // Remove symbol if no buckets left
      if (bucketMap.size === 0) {
        symbolBucketsMap.delete(symbol);
      }
    }
  }
}

// Periodic flush timer
setInterval(() => {
  flushBuckets().catch(err => {
    console.error('Error flushing liquidation buckets:', err);
  });
}, 15000); // every 15 seconds

/* ==========================================
 * Helper to safely convert timestamp to Number
 * Accepts string, number, or BigInt
 * ========================================== */
function toNumberTimestamp(value) {
  if (typeof value === 'bigint') return Number(value);
  if (typeof value === 'string') return Number(BigInt(value));
  if (typeof value === 'number') return value;
  throw new Error('Invalid timestamp type');
}

/* ==========================================
 * Process and insert liquidation record from raw event
 * Instead of inserting raw event, update bucket
 * ========================================== */
async function processAndInsert(exchange, baseSymbol, rawData) {
  const config = EXCHANGE_CONFIG[exchange];
  if (!config) return;

  const perpspec = config.PERPSPEC;

  try {
    let ts, side, price, quantity;

    if (exchange === 'BINANCE') {
      const o = rawData.o;
      ts = toNumberTimestamp(o.T);
      side = o.S === 'BUY' ? 'short' : 'long'; // Binance side logic
      price = parseFloat(o.p);
      quantity = parseFloat(o.q);

    } else if (exchange === 'BYBIT') {
      ts = toNumberTimestamp(rawData.T);
      side = rawData.S === 'Buy' ? 'long' : 'short'; // Bybit side logic
      price = parseFloat(rawData.p);
      quantity = parseFloat(rawData.v);

    } else if (exchange === 'OKX') {
      ts = toNumberTimestamp(rawData.ts);
      side = rawData.side === 'buy' ? 'short' : 'long'; // OKX side logic
      price = parseFloat(rawData.bkPx);
      quantity = parseFloat(rawData.sz);

    } else {
      return;
    }

    // Update bucket with event data
    updateBucket(perpspec, baseSymbol, ts, side, price, quantity);

    // Periodic status logging
    const now = Date.now();
    if (!lastStatusLog[perpspec] || now - lastStatusLog[perpspec] >= STATUS_LOG_INTERVAL) {
      const message = `👾 ${perpspec} posted ${liquidationCounts[perpspec] || 0} liquidation buckets`;
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message, { perpspec });
      console.log(`${STATUS_COLOR}${message}${RESET}`);
      lastStatusLog[perpspec] = now;
    }
  } catch (error) {
    await apiUtils.logScriptError(
      dbManager,
      SCRIPT_NAME,
      'API',
      'PARSE_ERROR',
      error.message,
      { perpspec, symbol: baseSymbol }
    );
    console.error(`[${perpspec}] Parse error for ${baseSymbol}:`, error.message);
  }
}

/* ==========================================
 * BINANCE WEBSOCKET
 * Separate connection per symbol
 * ========================================== */
function binanceWebSocket() {
  const config = EXCHANGE_CONFIG.BINANCE;

  perpList.forEach(baseSymbol => {
    const url = config.getWsUrl(baseSymbol);
    const ws = new WebSocket(url);

    ws.on('message', async (data) => {
      try {
        const message = JSON.parse(data);
        if (message.e !== 'forceOrder') return;
        await processAndInsert('BINANCE', baseSymbol, message);
      } catch (error) {
        await apiUtils.logScriptError(
          dbManager,
          SCRIPT_NAME,
          'API',
          'PARSE_ERROR',
          error.message,
          { perpspec: config.PERPSPEC, symbol: baseSymbol }
        );
        console.error(`[${config.PERPSPEC}] Parse error for ${baseSymbol}:`, error.message);
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
      console.error(`[${config.PERPSPEC}] WebSocket error for ${baseSymbol}:`, error.message);
    });

    ws.on('close', () => {
      setTimeout(() => binanceWebSocket(), 5000);
    });
  });
}

/* ==========================================
 * BYBIT WEBSOCKET
 * Single connection, multiple subscriptions
 * ========================================== */
function bybitWebSocket() {
  const config = EXCHANGE_CONFIG.BYBIT;
  const ws = new WebSocket(config.WS_URL);

  ws.on('open', () => {
    // Subscribe to allLiquidation.{symbol} for each symbol
    const args = perpList.map(sym => `allLiquidation.${config.mapSymbol(sym)}`);
    ws.send(JSON.stringify({ op: 'subscribe', args }));
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);
      if (!message.data || !Array.isArray(message.data)) return;

      for (const lqEvent of message.data) {
        const baseSymbol = perpList.find(sym => config.mapSymbol(sym) === lqEvent.s);
        if (!baseSymbol) continue;

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
      console.error(`[${config.PERPSPEC}] Parse error:`, error.message);
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
    console.error(`[${config.PERPSPEC}] WebSocket error:`, error.message);
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

      for (const instrument of message.data) {
        const instId = instrument.instId;
        if (!instId) continue;

        const baseSymbol = perpList.find(sym => config.mapSymbol(sym) === instId);
        if (!baseSymbol) continue;

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
      console.error(`[${config.PERPSPEC}] Parse error:`, error.message);
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
    console.error(`[${config.PERPSPEC}] WebSocket error:`, error.message);
  });

  ws.on('close', () => {
    setTimeout(() => okxWebSocket(), 5000);
  });
}

/* ==========================================
 * MAIN EXECUTION
 * ========================================== */
async function execute() {
  console.log(`${STATUS_COLOR}💀 Starting ${SCRIPT_NAME} - WebSocket liquidation streaming${RESET}`);

  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} connected`);

  binanceWebSocket();
  bybitWebSocket();
  okxWebSocket();

  process.on('SIGINT', async () => {
    console.log(`💀 ${STATUS_COLOR}\n${SCRIPT_NAME} received SIGINT, stopping...${RESET}`);
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