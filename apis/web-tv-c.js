/* ==========================================
 * web-tv-c.js   8 Oct 2025
 * Continuous WebSocket Taker Volume Collector
 *
 * Streams trade data from Binance, Bybit, and OKX
 * Aggregates taker buy/sell volume into 5-minute windows
 * Uses OHLCV (exchange-specific: bin-ohlcv/byb-ohlcv/okx-ohlcv) to distribute into biased 1m tbv/tsv
 * Inserts 5 x 1m records per 5m window (even split fallback if OHLCV incomplete/lagging)
 * ========================================== */

const WebSocket = require('ws');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');
require('dotenv').config();

const SCRIPT_NAME = 'web-tv-c.js';
const AGGREGATION_WINDOW = 5 * 60 * 1000; // 5 minutes for tbv/tsv totals
const FLUSH_INTERVAL = 60000; // Check/flush every 1 minute (for completed 5m windows)

/* ==========================================
 * EXCHANGE CONFIGURATION
 * Defines WebSocket URLs, perpspec names, symbol mapping, and OHLCV perpspec for biasing
 * ========================================== */
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-tv',
    OHLCV_PERPSPEC: 'bin-ohlcv',  // For v/c weighting
    WS_BASE: 'wss://fstream.binance.com/ws',
    mapSymbol: sym => `${sym.toLowerCase()}usdt`,
    getWsUrl: sym => `wss://fstream.binance.com/ws/${sym.toLowerCase()}usdt@aggTrade`
  },
  BYBIT: {
    PERPSPEC: 'byb-tv',
    OHLCV_PERPSPEC: 'byb-ohlcv',  // For v/c weighting
    WS_URL: 'wss://stream.bybit.com/v5/public/linear',
    mapSymbol: sym => `${sym}USDT`
  },
  OKX: {
    PERPSPEC: 'okx-tv',
    OHLCV_PERPSPEC: 'okx-ohlcv',  // For v/c weighting
    WS_URL: 'wss://ws.okx.com:8443/ws/v5/public',
    mapSymbol: sym => `${sym}-USDT-SWAP`
  }
};

// Aggregation buckets: { perpspec: { symbol: { tbv_total, tsv_total, windowStart } } }
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

const STATUS_LOG_INTERVAL = 60000; // 1 minute

/* ==========================================
 * AGGREGATION HELPERS
 * Manage 5-minute volume aggregation windows
 * ========================================== */

/**
 * Get or create volume bucket for symbol (5min total tbv/tsv)
 */
function getVolumeBucket(perpspec, baseSymbol, timestamp) {
  const windowStart = Math.floor(timestamp / AGGREGATION_WINDOW) * AGGREGATION_WINDOW;
  
  if (!volumeBuckets[perpspec][baseSymbol]) {
    volumeBuckets[perpspec][baseSymbol] = {
      tbv_total: 0,
      tsv_total: 0,
      windowStart: windowStart
    };
  }

  const bucket = volumeBuckets[perpspec][baseSymbol];

  // Check if we've moved to a new window
  if (bucket.windowStart !== windowStart) {
    // Flush old bucket before creating new one
    flushBucket(perpspec, baseSymbol);
    
    // Create new bucket
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
 * Flush 5min bucket: Query OHLCV, distribute biased tbv/tsv into 5 x 1m records, insert batch
 */
async function flushBucket(perpspec, baseSymbol) {
  const bucket = volumeBuckets[perpspec][baseSymbol];
  if (!bucket || (bucket.tbv_total === 0 && bucket.tsv_total === 0)) return;

  const timestamp = bucket.windowStart;  // 5m start
  const tbv_total = bucket.tbv_total;
  const tsv_total = bucket.tsv_total;
  const endTs = timestamp + AGGREGATION_WINDOW;
  const ohlcvPerpspec = EXCHANGE_CONFIG[Object.keys(EXCHANGE_CONFIG).find(key => EXCHANGE_CONFIG[key].PERPSPEC === perpspec)].OHLCV_PERPSPEC;

  try {
    // Query 1m OHLCV for this 5m window
    let ohlcvData = await dbManager.queryPerpData(ohlcvPerpspec, baseSymbol, timestamp, endTs);

    // Filter/sort: 1m, in range
    ohlcvData = ohlcvData
      .filter(d => d.interval === '1m' && d.ts >= timestamp && d.ts < endTs)
      .sort((a, b) => a.ts - b.ts);

    // Dedup by ts (handle potential dups from live/backfill)
    const uniqueOhlcv = [];
    const seenTs = new Set();
    for (const row of ohlcvData) {
      if (!seenTs.has(row.ts)) {
        uniqueOhlcv.push(row);
        seenTs.add(row.ts);
      }
    }
    ohlcvData = uniqueOhlcv;

    // If <5 rows (OHLCV lag/incomplete), fallback to even split across 5 slots
    let processedRecords = [];
    if (ohlcvData.length < 5) {
      // Even split (live fallback - no warn, as OHLCV may lag)
      for (let i = 0; i < 5; i++) {
        const minuteTs = timestamp + (i * 60 * 1000);
        processedRecords.push({
          ts: apiUtils.toMillis(BigInt(minuteTs)),
          symbol: baseSymbol,
          source: perpspec,
          perpspec: perpspec,
          interval: '1m',
          tbv: tbv_total / 5,
          tsv: tsv_total / 5
        });
      }
    } else {
      // Verify consecutive (strict for historical match, but live may skip if not exact)
      let expectedTs = timestamp;
      let consecutive = true;
      for (let row of ohlcvData) {
        if (row.ts !== expectedTs) {
          consecutive = false;
          break;
        }
        expectedTs += 60 * 1000;
      }

      if (!consecutive || ohlcvData.length !== 5) {
        // Even split fallback (live tolerance)
        for (let i = 0; i < 5; i++) {
          const minuteTs = timestamp + (i * 60 * 1000);
          processedRecords.push({
            ts: apiUtils.toMillis(BigInt(minuteTs)),
            symbol: baseSymbol,
            source: perpspec,
            perpspec: perpspec,
            interval: '1m',
            tbv: tbv_total / 5,
            tsv: tsv_total / 5
          });
        }
      } else {
        // Biased distribution (full OHLCV available)
        const total_v = ohlcvData.reduce((sum, d) => sum + parseFloat(d.v || 0), 0);

        if (total_v === 0) {
          // Even if no volume
          for (let i = 0; i < 5; i++) {
            const minuteTs = timestamp + (i * 60 * 1000);
            processedRecords.push({
              ts: apiUtils.toMillis(BigInt(minuteTs)),
              symbol: baseSymbol,
              source: perpspec,
              perpspec: perpspec,
              interval: '1m',
              tbv: tbv_total / 5,
              tsv: tsv_total / 5
            });
          }
        } else {
          // Compute weights (same as bin-tv-h.js)
          let prevC = null;
          let tbv_weights = [];
          let tsv_weights = [];
          let tbv_sum_weight = 0;
          let tsv_sum_weight = 0;

          for (let i = 0; i < 5; i++) {
            const currV = parseFloat(ohlcvData[i].v || 0);
            const currC = parseFloat(ohlcvData[i].c || 0);
            let price_delta = 0;
            if (i > 0) {
              price_delta = currC - prevC;
            }
            prevC = currC;

            const vol_weight = currV / total_v;
            const s = price_delta > 0 ? 1 : (price_delta < 0 ? -1 : 0);

            const tbv_w = vol_weight * (1 + s) / 2;
            const tsv_w = vol_weight * (1 - s) / 2;

            tbv_weights.push(tbv_w);
            tsv_weights.push(tsv_w);
            tbv_sum_weight += tbv_w;
            tsv_sum_weight += tsv_w;
          }

          // Fallback even if sums too low
          const even_weight = 1 / 5;
          if (tbv_sum_weight < 1e-6) {
            for (let i = 0; i < 5; i++) tbv_weights[i] = even_weight;
            tbv_sum_weight = 1;
          }
          if (tsv_sum_weight < 1e-6) {
            for (let i = 0; i < 5; i++) tsv_weights[i] = even_weight;
            tsv_sum_weight = 1;
          }

          // Allocate to 1m records
          for (let i = 0; i < 5; i++) {
            const minuteTs = timestamp + (i * 60 * 1000);
            const tbv = (tbv_weights[i] / tbv_sum_weight) * tbv_total;
            const tsv = (tsv_weights[i] / tsv_sum_weight) * tsv_total;

            processedRecords.push({
              ts: apiUtils.toMillis(BigInt(minuteTs)),
              symbol: baseSymbol,
              source: perpspec,
              perpspec: perpspec,
              interval: '1m',
              tbv,
              tsv
            });
          }
        }
      }
    }

    if (processedRecords.length > 0) {
      // Batch insert 5 x 1m records
      await dbManager.insertData(perpspec, processedRecords);
      sessionInsertCounts[perpspec] += processedRecords.length;  // Cumulative session total
    }

    // Clear bucket after flush
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
 * Flush all buckets for a perpspec (for completed windows only)
 */
async function flushAllBuckets(perpspec) {
  const symbols = Object.keys(volumeBuckets[perpspec]);
  for (const symbol of symbols) {
    const bucket = volumeBuckets[perpspec][symbol];
    const now = Date.now();
    const currentWindow = Math.floor(now / AGGREGATION_WINDOW) * AGGREGATION_WINDOW;
    if (bucket.windowStart < currentWindow) {  // Only completed 5m
      await flushBucket(perpspec, symbol);
    }
  }
}

/* ==========================================
 * TRADE PROCESSING
 * Process incoming trades and aggregate into 5min totals
 * ========================================== */

/**
 * Process trade and add to 5min aggregation bucket
 */
async function processTrade(exchange, baseSymbol, rawData) {
  const config = EXCHANGE_CONFIG[exchange];
  if (!config) return;

  const perpspec = config.PERPSPEC;

  try {
    let timestamp, isBuy, volume;

    if (exchange === 'BINANCE') {
      timestamp = parseInt(rawData.T);
      isBuy = !rawData.m; // m=false = taker buy
      volume = parseFloat(rawData.q);
    } else if (exchange === 'BYBIT') {
      timestamp = parseInt(rawData.T);
      isBuy = rawData.S === 'Buy';
      volume = parseFloat(rawData.v);
    } else if (exchange === 'OKX') {
      timestamp = parseInt(rawData.ts);
      isBuy = rawData.side === 'buy';
      volume = parseFloat(rawData.sz);
    }

    if (isNaN(timestamp) || isNaN(volume) || volume <= 0) return;

    // Get or create 5min bucket
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

/* ==========================================
 * WEBSOCKET FUNCTIONS (UNCHANGED STRUCTURE)
 * ========================================== */

// Binance: One WS per symbol
async function binanceWebSocket() {
  for (const baseSymbol of perpList) {
    const config = EXCHANGE_CONFIG.BINANCE;
    const wsUrl = config.getWsUrl(baseSymbol);

    const ws = new WebSocket(wsUrl);

    ws.on('open', () => {
        console.log(`✅ Binance WS subscribed to ${perpList.length} symbols`);  // Single line like others
          
    });
//console.log(`✅ Binance WS open for ${baseSymbol}`);

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
      setTimeout(() => binanceWebSocket(), 5000);  // Restart per symbol? Or global—adjust if needed
    });

    ws.on('close', () => {
      console.log(`🔄 Binance WS closed for ${baseSymbol}, reconnecting...`);
      setTimeout(() => binanceWebSocket(), 5000);
    });
  }
}

// Bybit: Single WS, multi-subscribe
async function bybitWebSocket() {
  const config = EXCHANGE_CONFIG.BYBIT;
  const ws = new WebSocket(config.WS_URL);

  ws.on('open', () => {
    const args = perpList.map(sym => `publicTrade.${config.mapSymbol(sym)}`);
    // Chunk subscriptions (Bybit limit ~200 total)
    for (let i = 0; i < args.length; i += 200) {
      const chunk = args.slice(i, i + 200);
      ws.send(JSON.stringify({ op: 'subscribe', args: chunk }));
    }
    console.log(`✅ Bybit WS subscribed to ${perpList.length} symbols`);
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);

      if (!message.data || !Array.isArray(message.data)) return;

      const topic = message.topic || '';
      const symbolFromTopic = topic.split('.')[1];  // e.g., 'BTCUSDT'

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

// OKX: Single WS, multi-subscribe
async function okxWebSocket() {
  const config = EXCHANGE_CONFIG.OKX;
  const ws = new WebSocket(config.WS_URL);

  ws.on('open', () => {
    const args = perpList.map(sym => ({
      channel: 'trades',
      instId: config.mapSymbol(sym)
    }));
    // OKX limit ~480 channels; chunk if needed
    ws.send(JSON.stringify({ op: 'subscribe', args }));
    console.log(`✅ OKX WS subscribed to ${perpList.length} symbols`);
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

        for (const trade of tradeData.data || [tradeData]) {  // Handle array or single
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

/* ==========================================
 * PERIODIC BUCKET FLUSHING
 * Flush completed 5-minute windows every 1min
 * ========================================== */
async function startPeriodicFlush() {
  setInterval(async () => {
    const now = Date.now();
    const currentWindow = Math.floor(now / AGGREGATION_WINDOW) * AGGREGATION_WINDOW;

    // Flush completed windows for all exchanges
    for (const perpspec of Object.keys(volumeBuckets)) {
      await flushAllBuckets(perpspec);
    }

    // Periodic status: Log session total inserted (cumulative since start)
    const nowLog = Date.now();
    for (const perpspec of Object.keys(EXCHANGE_CONFIG)) {
      const key = EXCHANGE_CONFIG[perpspec].PERPSPEC;
      if (!lastStatusLog[key] || nowLog - lastStatusLog[key] >= STATUS_LOG_INTERVAL) {
        const message = `${key} total inserted ${sessionInsertCounts[key]} 1m records (session)`;
        await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message);
        console.log(message);  // Optional console for monitoring
        lastStatusLog[key] = nowLog;
      }
    }
  }, FLUSH_INTERVAL);
}

/* ==========================================
 * MAIN EXECUTION
 * ========================================== */
async function execute() {
  console.log(`🚀 Starting ${SCRIPT_NAME} - WebSocket taker volume streaming (5m biased to 1m)`);

  // Status: Started
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} connected`);

  // Start all WebSocket connections
  binanceWebSocket();
  bybitWebSocket();
  okxWebSocket();

  // Start periodic flushing and status
  startPeriodicFlush();

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log(`\n${SCRIPT_NAME} received SIGINT, stopping...`);
    
    // Flush all remaining buckets
    for (const perpspec of Object.keys(volumeBuckets)) {
      await flushAllBuckets(perpspec);
    }
    
    // Final status: stopped
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
      console.error('💥 WebSocket taker volume streaming failed:', err);
      apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'INITIALIZATION_FAILED', err.message);
      process.exit(1);
    });
}

module.exports = { execute };