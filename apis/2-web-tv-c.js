/* ==========================================
 * web-tv-c.js   22 Oct 2025 - Unified Schema
 * Real-Time WebSocket Taker Volume Collector
 *
 * MAJOR CHANGE: Switched from 5-minute bucketing to 1-minute real-time processing
 * - Aggregates trades into 1-minute windows
 * - Queries current + previous 1-minute OHLCV for price delta bias calculation
 * - Inserts single 1-minute tbv/tsv record immediately when minute completes
 * - Uses same BIAS_STRENGTH formula as bin-tv-h.js for uniformity
 * - No lag - inserts happen ~1-2 seconds after minute boundary
 *
 * Unified: insertData (partial DO UPDATE); no source/interval; explicit exchange
 * Streams trade data from Binance, Bybit, and OKX
 * Calculates taker buy/sell volume with price direction bias
 * FIXED: OKX contract normalization - converts sz (contracts) to base tokens via ctVal
 * ========================================== */

const WebSocket = require('ws');
const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');
require('dotenv').config();

const SCRIPT_NAME = 'web-tv-c.js';

// ============================================================================
// USER CONFIGURATION
// ============================================================================
const STATUS_COLOR = '\x1b[92m'; // Standard green for status logs
const RESET = '\x1b[0m'; // Reset console color

// Bias strength for tbv/tsv distribution (0-1 range, 0.6 = 60% bias toward price direction)
const BIAS_STRENGTH = 0.6;

// Flush interval: Check for completed 1-minute windows (ms)
const FLUSH_INTERVAL = 5000; // Check every 5 seconds

// Status log interval
const STATUS_LOG_INTERVAL = 60000; // 1 minute

// ============================================================================
// OKX CONTRACT VALUE CACHE
// Maps instId to ctVal for volume normalization (contracts → base tokens)
// ============================================================================
const okxContractMap = {}; // { "BTC-USDT-SWAP": 0.01, "ETH-USDT-SWAP": 0.1, ... }

async function loadOkxContracts() {
  try {
    const res = await axios.get("https://www.okx.com/api/v5/public/instruments?instType=SWAP", {
      timeout: 10000
    });
    
    if (res.data && res.data.data) {
      for (const inst of res.data.data) {
        okxContractMap[inst.instId] = parseFloat(inst.ctVal);
      }
      // Status log removed per user request
    }
  } catch (err) {
    console.error('❌ Failed to load OKX contracts:', err.message);
    await apiUtils.logScriptError(
      dbManager,
      SCRIPT_NAME,
      'API',
      'OKX_CONTRACT_LOAD_ERROR',
      err.message
    );
  }
}

// ============================================================================
// EXCHANGE CONFIGURATION
// Defines WebSocket URLs, perpspec names, symbol mapping, and OHLCV exchange for biasing
// ============================================================================
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-tv',
    OHLCV_EXCHANGE: 'bin',  // For v/c weighting queries
    WS_BASE: 'wss://fstream.binance.com/ws',
    mapSymbol: sym => `${sym.toLowerCase()}usdt`,
    getWsUrl: sym => `wss://fstream.binance.com/ws/${sym.toLowerCase()}usdt@aggTrade`
  },
  BYBIT: {
    PERPSPEC: 'byb-tv',
    OHLCV_EXCHANGE: 'byb',  // For v/c weighting queries
    WS_URL: 'wss://stream.bybit.com/v5/public/linear',
    mapSymbol: sym => `${sym}USDT`
  },
  OKX: {
    PERPSPEC: 'okx-tv',
    OHLCV_EXCHANGE: 'okx',  // For v/c weighting queries
    WS_URL: 'wss://ws.okx.com:8443/ws/v5/public',
    mapSymbol: sym => `${sym}-USDT-SWAP`
  }
};

// ============================================================================
// 1-MINUTE AGGREGATION BUCKETS
// Structure: { perpspec: { symbol: { tbv_total, tsv_total, windowStart } } }
// Each bucket represents a single 1-minute window
// ============================================================================
const volumeBuckets = {
  'bin-tv': {},
  'byb-tv': {},
  'okx-tv': {}
};

// Session totals for status logging (cumulative since start)
let sessionInsertCounts = {
  'bin-tv': 0,
  'byb-tv': 0,
  'okx-tv': 0
};

// Last status log time per perpspec
const lastStatusLog = {
  'bin-tv': Date.now(),
  'byb-tv': Date.now(),
  'okx-tv': Date.now()
};

// ============================================================================
// CONNECTION TRACKING
// Tracks first successful message from each exchange for "connected" status log
// ============================================================================
const connectionStatus = {
  'bin-tv': false,
  'byb-tv': false,
  'okx-tv': false
};

let allConnected = false;

async function checkAndLogAllConnected() {
  if (!allConnected && connectionStatus['bin-tv'] && 
      connectionStatus['byb-tv'] && connectionStatus['okx-tv']) {
    allConnected = true;
    const message = '🚦 bin-tv, byb-tv, okx-tv successful connections; fetching.';
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', message);
    console.log(`${STATUS_COLOR}${message}${RESET}`);
  }
}

// ============================================================================
// 1-MINUTE AGGREGATION HELPERS
// Manage 1-minute volume aggregation windows
// ============================================================================

/**
 * Get or create 1-minute volume bucket for symbol
 * @param {string} perpspec - Exchange perpspec (bin-tv, byb-tv, okx-tv)
 * @param {string} baseSymbol - Base symbol (BTC, ETH, etc)
 * @param {number} timestamp - Trade timestamp in ms
 * @returns {object} Bucket object with tbv_total, tsv_total, windowStart
 */
function getVolumeBucket(perpspec, baseSymbol, timestamp) {
  const windowStart = Math.floor(timestamp / 60000) * 60000; // Floor to 1-minute boundary
  
  if (!volumeBuckets[perpspec][baseSymbol]) {
    volumeBuckets[perpspec][baseSymbol] = {
      tbv_total: 0,
      tsv_total: 0,
      windowStart: windowStart
    };
  }

  const bucket = volumeBuckets[perpspec][baseSymbol];

  // Check if we've moved to a new 1-minute window
  if (bucket.windowStart !== windowStart) {
    // Flush old bucket before creating new one
    flushBucket(perpspec, baseSymbol);
    
    // Create new bucket for current minute
    volumeBuckets[perpspec][baseSymbol] = {
      tbv_total: 0,
      tsv_total: 0,
      windowStart: windowStart
    };
    return volumeBuckets[perpspec][baseSymbol];
  }

  return bucket;
}

/**
 * Flush 1-minute bucket: Query current + previous OHLCV, calculate bias, insert single record
 * NEW APPROACH: Uses 1-minute processing instead of 5-minute bucketing
 * - Queries current minute and previous minute OHLCV for price delta
 * - Applies same BIAS_STRENGTH formula as bin-tv-h.js
 * - Inserts single 1-minute record immediately
 * 
 * @param {string} perpspec - Exchange perpspec
 * @param {string} baseSymbol - Base symbol
 */
async function flushBucket(perpspec, baseSymbol) {
  const bucket = volumeBuckets[perpspec][baseSymbol];
  if (!bucket || (bucket.tbv_total === 0 && bucket.tsv_total === 0)) return;

  const currentMinuteTs = bucket.windowStart;  // Current 1-minute start
  const previousMinuteTs = currentMinuteTs - 60000;  // Previous 1-minute start
  const tbv_total = bucket.tbv_total;
  const tsv_total = bucket.tsv_total;
  const config = EXCHANGE_CONFIG[Object.keys(EXCHANGE_CONFIG).find(key => 
    EXCHANGE_CONFIG[key].PERPSPEC === perpspec)];

  try {
    // Query OHLCV for current and previous minute (need price delta for bias calculation)
    const queryStart = previousMinuteTs;
    const queryEnd = currentMinuteTs + 60000;
    let ohlcvData = await dbManager.queryPerpData(config.OHLCV_EXCHANGE, baseSymbol, queryStart, queryEnd);

    // Filter for 1m interval, sort by timestamp
    ohlcvData = ohlcvData
      .filter(d => d.interval === '1m')
      .sort((a, b) => a.ts - b.ts);

    // Dedup by timestamp (handle potential duplicates)
    const uniqueOhlcv = [];
    const seenTs = new Set();
    for (const row of ohlcvData) {
      if (!seenTs.has(row.ts)) {
        uniqueOhlcv.push(row);
        seenTs.add(row.ts);
      }
    }
    ohlcvData = uniqueOhlcv;

    // Find current and previous minute candles
    const currentCandle = ohlcvData.find(d => d.ts === currentMinuteTs);
    const previousCandle = ohlcvData.find(d => d.ts === previousMinuteTs);

    let tbv, tsv;

    // Calculate bias based on price direction (same logic as bin-tv-h.js)
    if (currentCandle && previousCandle) {
      const currC = parseFloat(currentCandle.c || 0);
      const prevC = parseFloat(previousCandle.c || 0);
      const price_delta = currC - prevC;

      // Price direction signal: +1 (up), -1 (down), 0 (flat)
      const s = price_delta > 0 ? 1 : (price_delta < 0 ? -1 : 0);

      // Apply bias: price up = more tbv, price down = more tsv
      // Formula matches bin-tv-h.js line 132-133
      const tbv_weight = 0.5 + s * BIAS_STRENGTH * 0.5;  // Range: 0.2 to 0.8 (with 0.6 bias)
      const tsv_weight = 0.5 - s * BIAS_STRENGTH * 0.5;  // Range: 0.8 to 0.2 (with 0.6 bias)

      tbv = tbv_total * tbv_weight;
      tsv = tsv_total * tsv_weight;
    } else {
      // Fallback: No bias if OHLCV unavailable (even split)
      tbv = tbv_total * 0.5;
      tsv = tsv_total * 0.5;
    }

    // Insert single 1-minute record (unified format)
  const record = {
  ts: apiUtils.toMillis(BigInt(currentMinuteTs)),
  symbol: baseSymbol,
  exchange: config.OHLCV_EXCHANGE,  // Fixed: Use OHLCV exchange (e.g., 'bin' for bin-tv)
  perpspec: perpspec, // String; insertData wraps to array
  tbv,
  tsv
};

    await dbManager.insertData([record]);
    sessionInsertCounts[perpspec] += 1;

    // Clear bucket after successful flush
    delete volumeBuckets[perpspec][baseSymbol];

  } catch (error) {
    console.error(`❌ [${perpspec}] Flush error for ${baseSymbol}:`, error.message);
    await apiUtils.logScriptError(
      dbManager,
      SCRIPT_NAME,
      'INTERNAL',
      'FLUSH_ERROR',
      error.message,
      { perpspec, symbol: baseSymbol }
    );
  }
}

/**
 * Flush all completed 1-minute buckets for a perpspec
 * Only flushes buckets where the minute has fully elapsed
 * 
 * @param {string} perpspec - Exchange perpspec
 */
async function flushAllBuckets(perpspec) {
  const symbols = Object.keys(volumeBuckets[perpspec]);
  const now = Date.now();
  const currentWindow = Math.floor(now / 60000) * 60000;  // Current 1-minute boundary

  for (const symbol of symbols) {
    try {
      const bucket = volumeBuckets[perpspec][symbol];
      if (!bucket) continue;

      // Only flush completed 1-minute windows (not current active minute)
      if (bucket.windowStart < currentWindow) {
        await flushBucket(perpspec, symbol);
      }
    } catch (err) {
      console.error(`Error flushing bucket for ${perpspec} ${symbol}:`, err);
      await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'INTERNAL', 'FLUSH_ALL_BUCKETS_ERROR', err.message, { perpspec, symbol });
    }
  }
}

// ============================================================================
// TRADE PROCESSING
// Process incoming trades and aggregate into 1-minute totals
// FIXED: OKX volume normalized to base tokens via ctVal
// ============================================================================

/**
 * Process individual trade and add to 1-minute aggregation bucket
 * 
 * @param {string} exchange - Exchange name (BINANCE, BYBIT, OKX)
 * @param {string} baseSymbol - Base symbol (BTC, ETH, etc)
 * @param {object} rawData - Raw trade data from WebSocket
 */
async function processTrade(exchange, baseSymbol, rawData) {
  const config = EXCHANGE_CONFIG[exchange];
  if (!config) return;

  const perpspec = config.PERPSPEC;

  // Mark connection as successful on first trade
  if (!connectionStatus[perpspec]) {
    connectionStatus[perpspec] = true;
    await checkAndLogAllConnected();
  }

  try {
    let timestamp, isBuy, volume;

    if (exchange === 'BINANCE') {
      timestamp = parseInt(rawData.T);
      isBuy = !rawData.m; // m=false = taker buy
      volume = parseFloat(rawData.q);  // Quote volume (USDT)
    } else if (exchange === 'BYBIT') {
      timestamp = parseInt(rawData.T);
      isBuy = rawData.S === 'Buy';
      volume = parseFloat(rawData.v);  // Base volume (tokens)
    } else if (exchange === 'OKX') {
      timestamp = parseInt(rawData.ts);
      isBuy = rawData.side === 'buy';
      
      // FIX: Convert contracts to base tokens
      const instId = rawData.instId;
      const ctVal = okxContractMap[instId] || 0.01;  // Fallback to 0.01 if not loaded
      volume = parseFloat(rawData.sz) * ctVal;  // sz (contracts) × ctVal = base tokens
    }

    if (isNaN(timestamp) || isNaN(volume) || volume <= 0) return;

    // Get or create 1-minute bucket
    const bucket = getVolumeBucket(perpspec, baseSymbol, timestamp);

    // Add to totals
    if (isBuy) {
      bucket.tbv_total += volume;
    } else {
      bucket.tsv_total += volume;
    }

  } catch (error) {
    await apiUtils.logScriptError(
      dbManager,
      SCRIPT_NAME,
      'INTERNAL',
      'PROCESS_ERROR',
      error.message,
      { perpspec, symbol: baseSymbol }
    );
  }
}

// ============================================================================
// WEBSOCKET FUNCTIONS
// Establish WebSocket connections and route trades to processTrade()
// ============================================================================

/**
 * Binance WebSocket: One connection per symbol
 * Subscribes to aggTrade stream for each symbol
 */
async function binanceWebSocket() {
  for (const baseSymbol of perpList) {
    const config = EXCHANGE_CONFIG.BINANCE;
    const wsUrl = config.getWsUrl(baseSymbol);

    const ws = new WebSocket(wsUrl);

    ws.on('open', () => {
      // Silent connection (no log per user request)
    });

    ws.on('message', async (data) => {
      try {
        const message = JSON.parse(data);
        await processTrade('BINANCE', baseSymbol, message);
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
      console.error(`❌ Binance WS error for ${baseSymbol}:`, error.message);
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
      console.log(`🔄 Binance WS closed for ${baseSymbol}, reconnecting...`);
      setTimeout(() => binanceWebSocket(), 5000);
    });
  }
}

/**
 * Bybit WebSocket: Single connection, multi-symbol subscription
 * Subscribes to publicTrade channel for all symbols
 */
async function bybitWebSocket() {
  const config = EXCHANGE_CONFIG.BYBIT;
  const ws = new WebSocket(config.WS_URL);

  ws.on('open', () => {
    const args = perpList.map(sym => `publicTrade.${config.mapSymbol(sym)}`);
    // Subscribe in chunks of 200 (Bybit limit)
    for (let i = 0; i < args.length; i += 200) {
      const chunk = args.slice(i, i + 200);
      ws.send(JSON.stringify({ op: 'subscribe', args: chunk }));
    }
    // Silent subscription (no log per user request)
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);

      if (!message.data || !Array.isArray(message.data)) return;

      const topic = message.topic || '';
      const symbolFromTopic = topic.split('.')[1];

      const baseSymbol = perpList.find(sym => config.mapSymbol(sym) === symbolFromTopic);
      if (!baseSymbol) return;

      for (const trade of message.data) {
        await processTrade('BYBIT', baseSymbol, trade);
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
    console.error(`❌ Bybit WS error:`, error.message);
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
    console.log(`🔄 Bybit WS closed, reconnecting...`);
    setTimeout(() => bybitWebSocket(), 5000);
  });
}

/**
 * OKX WebSocket: Single connection, multi-symbol subscription
 * Subscribes to trades channel for all symbols
 */
async function okxWebSocket() {
  const config = EXCHANGE_CONFIG.OKX;
  const ws = new WebSocket(config.WS_URL);

  ws.on('open', () => {
    const args = perpList.map(sym => ({
      channel: 'trades',
      instId: config.mapSymbol(sym)
    }));
    ws.send(JSON.stringify({ op: 'subscribe', args }));
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);

      if (message.event === 'error') {
        console.error('OKX WS error event:', message);
        return;
      }

      if (!message.data || !Array.isArray(message.data)) return;

      for (const tradeData of message.data) {
        const instId = tradeData.instId;
        if (!instId) continue;

        const baseSymbol = perpList.find(sym => config.mapSymbol(sym) === instId);
        if (!baseSymbol) continue;

        for (const trade of tradeData.data || [tradeData]) {
          await processTrade('OKX', baseSymbol, trade);
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
    console.error(`❌ OKX WS error:`, error.message);
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
    console.log(`🔄 OKX WS closed, reconnecting...`);
    setTimeout(() => okxWebSocket(), 5000);
  });
}

// ============================================================================
// PERIODIC BUCKET FLUSHING
// Check every 5 seconds for completed 1-minute windows and flush them
// ============================================================================
async function startPeriodicFlush() {
  setInterval(async () => {
    // Flush completed 1-minute windows for all exchanges
    for (const perpspec of Object.keys(volumeBuckets)) {
      await flushAllBuckets(perpspec);
    }

    // Periodic status: Log running status every minute
    const nowLog = Date.now();
    for (const perpspec of Object.keys(EXCHANGE_CONFIG)) {
      const key = EXCHANGE_CONFIG[perpspec].PERPSPEC;
      if (!lastStatusLog[key] || nowLog - lastStatusLog[key] >= STATUS_LOG_INTERVAL) {
        const message = `🚥 ${key} 1m pull & calc`;
        await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message);
        console.log(`${STATUS_COLOR}${message}${RESET}`);
        lastStatusLog[key] = nowLog;
      }
    }
  }, FLUSH_INTERVAL);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================
async function execute() {
  console.log(`${STATUS_COLOR}🚦 *TV Starting ${SCRIPT_NAME} - WebSocket taker volume stream${RESET}`);

  // Load OKX contract values for normalization
  await loadOkxContracts();

  // Status: Started
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} started`);

  // Start all WebSocket connections
  binanceWebSocket();
  bybitWebSocket();
  okxWebSocket();

  // Start periodic flushing and status logging
  startPeriodicFlush();

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log(`\n🚦 ${SCRIPT_NAME} received SIGINT, stopping...`);
    
    // Flush all remaining buckets
    for (const perpspec of Object.keys(volumeBuckets)) {
      await flushAllBuckets(perpspec);
    }
    
    // Final status: stopped
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'stopped', `${SCRIPT_NAME} stopped smoothly`);
    process.exit(0);
  });
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================
if (require.main === module) {
  execute()
    .catch(err => {
      console.error('💥 WebSocket taker volume streaming failed:', err);
      apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'INITIALIZATION_FAILED', err.message);
      process.exit(1);
    });
}

module.exports = { execute };