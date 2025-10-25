// SCRIPT: 1-ohlcv-h.js rev 21 Oct 2025
// Unified OHLCV Backfill Script for Binance, Bybit, and OKX
// DRY Refactored: Single fetch/process wrapper, reusable MT token creation

const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const weightMonitor = require('../b-weight');
const pLimit = require('p-limit');

const SCRIPT_NAME = '1-ohlcv-h.js';
const DAYS_TO_FETCH = 10;
const NOW = Date.now();
const STATUS_LOG_COLOR = '\x1b[38;2;135;206;235m';
const COLOR_RESET = '\x1b[0m';
const HEARTBEAT_INTERVAL_MS = 15000;
const RECENT_RECORDS_COUNT = 4;

// ============================================================================
// EXCHANGE CONFIGURATIONS
// ============================================================================
const EXCHANGES = {
  BINANCE: {
    perpspec: 'bin-ohlcv', // PERPSPEC: bin-ohlcv
    url: 'https://fapi.binance.com/fapi/v1/klines',
    limit: 800,
    rateDelay: 300,
    concurrency: 6,
    timeout: 10000,
    apiInterval: '1m',
    mapSymbol: sym => `${sym}USDT`
  },
  BYBIT: {
    perpspec: 'byb-ohlcv', // PERPSPEC: byb-ohlcv
    url: 'https://api.bybit.com/v5/market/kline',
    limit: 1000,
    rateDelay: 200,
    concurrency: 10,
    timeout: 10000,
    apiInterval: '1',
    mapSymbol: sym => `${sym}USDT`
  },
  OKX: {
    perpspec: 'okx-ohlcv', // PERPSPEC: okx-ohlcv
    url: 'https://www.okx.com/api/v5/market/history-candles',
    limit: 300,
    rateDelay: 70,
    concurrency: 3,
    timeout: 9000,
    retrySleepMs: 500,
    apiInterval: '1m',
    mapSymbol: sym => `${sym}-USDT-SWAP`
  }
};

const MT_SYMBOLS = ['ETH', 'BTC', 'XRP', 'SOL'];
const MT_SYMBOL = 'MT';

// ============================================================================
// UTILITIES
// ============================================================================
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function logStatus(status, message) {
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, status, message);
  console.log(`${STATUS_LOG_COLOR}${message}${COLOR_RESET}`);
}

// ============================================================================
// DATA PROCESSING FUNCTIONS (Exchange-specific parsers)
// ============================================================================
function processBinanceData(rawCandles, baseSymbol, config) {
  return rawCandles.map(candle => ({
    ts: BigInt(candle[0]),
    symbol: baseSymbol,
    perpspec: config.perpspec,
    o: parseFloat(candle[1]),
    h: parseFloat(candle[2]),
    l: parseFloat(candle[3]),
    c: parseFloat(candle[4]),
    v: parseFloat(candle[7])
  })).filter(item => item.ts !== undefined && !isNaN(item.o) && !isNaN(item.h) && 
                     !isNaN(item.l) && !isNaN(item.c) && !isNaN(item.v));
}

function processBybitData(rawCandles, baseSymbol, config) {
  return rawCandles.map(candle => ({
    ts: BigInt(candle[0]),
    symbol: baseSymbol,
    perpspec: config.perpspec,
    o: parseFloat(candle[1]),
    h: parseFloat(candle[2]),
    l: parseFloat(candle[3]),
    c: parseFloat(candle[4]),
    v: parseFloat(candle[6])
  })).filter(item => item.ts !== undefined && !isNaN(item.o) && !isNaN(item.h) && 
                     !isNaN(item.l) && !isNaN(item.c) && !isNaN(item.v));
}

function processOKXData(rawCandles, baseSymbol, config) {
  return rawCandles.map(candle => ({
    ts: BigInt(candle[0]),
    symbol: baseSymbol,
    perpspec: config.perpspec,
    o: parseFloat(candle[1]),
    h: parseFloat(candle[2]),
    l: parseFloat(candle[3]),
    c: parseFloat(candle[4]),
    v: parseFloat(candle[7])
  })).filter(item => item.ts !== undefined && !isNaN(item.o) && !isNaN(item.h) && 
                     !isNaN(item.l) && !isNaN(item.c) && !isNaN(item.v));
}

// ============================================================================
// CORE FETCH FUNCTIONS (Historical backfill - full DAYS_TO_FETCH)
// ============================================================================
async function fetchBinanceOHLCV(symbol, config) {
  const totalCandles = DAYS_TO_FETCH * 1440;
  const totalRequests = Math.ceil(totalCandles / config.limit);
  let allCandles = [];
  let startTime = NOW - DAYS_TO_FETCH * 24 * 60 * 60 * 1000;

  for (let i = 0; i < totalRequests; i++) {
    const params = { symbol, interval: config.apiInterval, limit: config.limit, startTime };

    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      weightMonitor.logRequest('bin-ohlcv', '/fapi/v1/klines', 1);
      const data = response.data;
      if (!data || data.length === 0) break;

      allCandles.push(...data);
      startTime = data[data.length - 1][0] + 60000;
      if (data.length < config.limit) break;

      await sleep(config.rateDelay);
    } catch (error) {
      if (error.response?.status === 429) {
        console.error(`\x1b[31m❌ [${config.perpspec}] ${symbol}: Rate limit (429)\x1b[0m`);
      } else {
        console.error(`\x1b[31m❌ [${config.perpspec}] ${symbol}: ${error.message}\x1b[0m`);
      }
      throw error;
    }
  }

  return allCandles;
}

async function fetchBybitOHLCV(symbol, config) {
  const totalCandles = DAYS_TO_FETCH * 1440;
  const totalRequests = Math.ceil(totalCandles / config.limit);
  let allCandles = [];
  let end = NOW;

  for (let i = 0; i < totalRequests; i++) {
    const params = { category: 'linear', symbol, interval: config.apiInterval, limit: config.limit, end };

    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      const data = response.data;
      if (!data.result?.list || data.result.list.length === 0) break;

      allCandles.push(...data.result.list);
      end = data.result.list[data.result.list.length - 1][0] - 1;

      await sleep(config.rateDelay);
    } catch (error) {
      if (error.response?.status === 429) {
        console.error(`\x1b[31m❌ [${config.perpspec}] ${symbol}: Rate limit (429)\x1b[0m`);
      } else {
        console.error(`\x1b[31m❌ [${config.perpspec}] ${symbol}: ${error.message}\x1b[0m`);
      }
      throw error;
    }
  }

  return allCandles;
}
//========================= OKX ===========================================
async function fetchOKXOHLCV(symbol, config, onBatchReady = null) {
  const totalCandles = DAYS_TO_FETCH * 1440;
  const totalRequests = Math.ceil(totalCandles / config.limit);
  let allCandles = [];
  let after = null;
  const targetOldTs = NOW - DAYS_TO_FETCH * 24 * 60 * 60 * 1000;
  const batchSize = 1000; // Process every 1000 records

  for (let i = 0; i < totalRequests; i++) {
    let url = `${config.url}?instId=${symbol}&bar=${config.apiInterval}&limit=${config.limit}`;
    if (after !== null) url += `&after=${after}`;

    try {
      const response = await axios.get(url, { timeout: config.timeout });
      const data = response.data;

      if (data.code !== '0' || !data.data || data.data.length === 0) break;

      const confirmedCandles = data.data.filter(c => c[8] === '1');
      allCandles = [...confirmedCandles, ...allCandles];
      
      // Progressive batch processing for OKX
      if (onBatchReady && allCandles.length >= batchSize) {
        await onBatchReady(allCandles);
        allCandles = [];
      }
      
      const oldestTs = confirmedCandles.length > 0
        ? parseInt(confirmedCandles[confirmedCandles.length - 1][0]) - 1
        : targetOldTs - 1;
      after = oldestTs;

      if (confirmedCandles.length < config.limit || oldestTs < targetOldTs) break;

      await sleep(config.rateDelay);
    } catch (error) {
      if (error.response?.status === 429) {
        console.error(`\x1b[31m❌ [${config.perpspec}] ${symbol}: Rate limit (429)\x1b[0m`);
        await sleep(config.retrySleepMs);
        i--;
      } else {
        console.error(`\x1b[31m❌ [${config.perpspec}] ${symbol}: ${error.message}\x1b[0m`);
        throw error;
      }
    }
  }

  // Return any remaining candles
  return allCandles;
}

// ============================================================================
// UNIFIED FETCH & PROCESS WRAPPER (DRY: Don't Repeat Yourself)
// Fetches raw data, processes to unified format, returns ready-to-insert array
// ============================================================================
async function fetchAndProcessSymbol(baseSym, config, options = {}) {
  const { 
    startTime = null,  // Binance startTime filter
    end = null,        // Bybit end filter
    limit = null,      // Override config.limit (for final loop)
    historicalMode = false  // If true, use full backfill functions
  } = options;

  const symbol = config.mapSymbol(baseSym);
  
  try {
    let rawCandles;
    
    // FETCH: Choose historical or targeted recent fetch
    if (historicalMode) {
      // Historical backfill (full DAYS_TO_FETCH)
      if (config.perpspec === 'bin-ohlcv') {
        rawCandles = await fetchBinanceOHLCV(symbol, config);
      } else if (config.perpspec === 'byb-ohlcv') {
        rawCandles = await fetchBybitOHLCV(symbol, config)
//===========================================================================
      } else {
  // OKX with progressive batching
  const okxBatches = [];
  rawCandles = await fetchOKXOHLCV(symbol, config, async (batch) => {
    const processed = processOKXData(batch, baseSym, config);
    okxBatches.push(...processed);
  });
  // Return accumulated batches plus final remainder
  return [...okxBatches, ...processOKXData(rawCandles, baseSym, config)];
}    
    
    } else {      
  //=============== Targeted fetch (for final loop - recent data only) ============
      if (config.perpspec === 'bin-ohlcv') {
        const params = { 
          symbol, 
          interval: config.apiInterval, 
          limit: limit || RECENT_RECORDS_COUNT,
          ...(startTime && { startTime })
        };
        const response = await axios.get(config.url, { params, timeout: config.timeout });
        weightMonitor.logRequest('bin-ohlcv', '/fapi/v1/klines', 1);
        rawCandles = response.data || [];
        
      } else if (config.perpspec === 'byb-ohlcv') {
        const params = { 
          category: 'linear', 
          symbol, 
          interval: config.apiInterval, 
          limit: limit || RECENT_RECORDS_COUNT,
          ...(end && { end })
        };
        const response = await axios.get(config.url, { params, timeout: config.timeout });
        rawCandles = response.data?.result?.list || [];
        
// New code here: Replace the original OKX 'else' block (the one starting with '// OKX: No after param = most recent') with this expanded version. This keeps the if-else chain intact and only modifies the OKX targeted fetch logic.
      } else {
        // OKX: Fetch recent confirmed + 1 unconfirmed current (final loop only)
        const limitNum = limit || RECENT_RECORDS_COUNT;
        const historyUrl = `${config.url}?instId=${symbol}&bar=${config.apiInterval}&limit=${limitNum}`;
        const historyResp = await axios.get(historyUrl, { timeout: config.timeout });
        let rawCandlesLocal = (historyResp.data.code === '0' && historyResp.data.data) 
          ? historyResp.data.data.filter(c => c[8] === '1')  // Confirmed only from history
          : [];

        // Append 1 latest unconfirmed (if newer) for present-time alignment in final loop
        const currentUrl = `https://www.okx.com/api/v5/market/candles?instId=${symbol}&bar=${config.apiInterval}&limit=1`;
        const currentResp = await axios.get(currentUrl, { timeout: config.timeout });
        if (currentResp.data.code === '0' && currentResp.data.data && currentResp.data.data.length > 0) {
          const currentCandle = currentResp.data.data[0];  // Newest first
          const currentTs = parseInt(currentCandle[0]);
          const lastConfirmedTs = rawCandlesLocal.length > 0 ? parseInt(rawCandlesLocal[rawCandlesLocal.length - 1][0]) : 0;

          if (currentTs > lastConfirmedTs) {
            rawCandlesLocal.push(currentCandle);  // Add exactly 1 more at end
          }
        }

        // Sort ascending by ts (history is old-to-new, appended is newest)
        rawCandlesLocal.sort((a, b) => parseInt(a[0]) - parseInt(b[0]));

        // Assign to outer rawCandles to match the rest of the function
        rawCandles = rawCandlesLocal;
      }
  //=================================================================
    }
    if (rawCandles.length === 0) return [];

    // PROCESS: Convert to unified format based on exchange
    let processedData;
    if (config.perpspec === 'bin-ohlcv') {
      processedData = processBinanceData(rawCandles, baseSym, config);
    } else if (config.perpspec === 'byb-ohlcv') {
      processedData = processBybitData(rawCandles, baseSym, config);
    } else {
      processedData = processOKXData(rawCandles, baseSym, config);
    }

    return processedData;
    
  } catch (error) {
    console.error(`\x1b[31m❌ [${config.perpspec}] ${baseSym}: ${error.message}\x1b[0m`);
    return [];
  }
}

// ============================================================================
// BATCH PROCESSOR (Handles concurrency + batching + insert)
// Collects all symbols for an exchange, then does single batch insert in 50k chunks
// ============================================================================

const insertedCounts = {
  'bin-ohlcv': 0,
  'byb-ohlcv': 0,
  'okx-ohlcv': 0
};

async function batchFetchAndInsert(config, options = {}) {
  const limit = pLimit(config.concurrency);
  const allBatchData = [];

  const promises = perpList.map(baseSym => limit(async () => {
    const processedData = await fetchAndProcessSymbol(baseSym, config, options);
    if (processedData.length > 0) {
      allBatchData.push(...processedData);
    }
  }));

  await Promise.all(promises);

  // Insert in chunks of 50k
  const chunkSize = 50000;
  for (let i = 0; i < allBatchData.length; i += chunkSize) {
    const chunk = allBatchData.slice(i, i + chunkSize);
    await dbManager.insertBackfillData(chunk);
    insertedCounts[config.perpspec] += chunk.length;
  }

  return allBatchData.length;
}

// ============================================================================
// MAIN BACKFILL ORCHESTRATOR
// ============================================================================

async function backfill() {
  const startTime = Date.now();

  try {
    await logStatus('started', `🔗 Starting ${SCRIPT_NAME} backfill for ${perpList.length} symbols`);

    // Heartbeat with inserted counts (lightweight, no DB count query)
    const heartbeatInterval = setInterval(async () => {
      try {
        const messages = Object.entries(insertedCounts).map(([perpspec, count]) =>
          `${perpspec}db insert: ${count}`
        ).join(', ');
        await logStatus('running', `OHLCV backfilling; ${messages}`);
      } catch (error) {
        await logStatus('running', 'OHLCV backfilling');
      }
    }, HEARTBEAT_INTERVAL_MS);

    // MAIN BACKFILL: All exchanges in parallel
    await Promise.all([
      backfillExchange(EXCHANGES.BINANCE),
      backfillExchange(EXCHANGES.BYBIT),
      backfillExchange(EXCHANGES.OKX)
    ]);

    clearInterval(heartbeatInterval);

    // MT TOKEN: Main creation
    await createMTTokenWrapper(false);

    // FINAL LOOP: Recent data refresh (parallel)
    await logStatus('running', '🔗 Final loop started');
    await Promise.all([
      fetchRecentDataFast(EXCHANGES.BINANCE),
      fetchRecentDataFast(EXCHANGES.BYBIT),
      fetchRecentDataFast(EXCHANGES.OKX)
    ]);

    // MT TOKEN: Final loop
    await createMTTokenWrapper(true);

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    await logStatus('completed', `🕒 OHLCV backfill completed in ${duration}s`);
  } catch (error) {
    console.error('\x1b[31m❌ Backfill error:', error.message, '\x1b[0m');
    throw error;
  }
}

// ============================================================================
// MT TOKEN WRAPPER (Reusable for main + final loop)
// Queries DB for MT_SYMBOLS data, averages to create MT token
// ============================================================================

async function createMTTokenWrapper(recentOnly = false) {
  try {
    const now = Date.now();
    const startTs = recentOnly ? (now - RECENT_RECORDS_COUNT * 60 * 1000) : null;

    const mtData = await Promise.all(MT_SYMBOLS.map(async (sym) => {
      const query = startTs
        ? `SELECT ts, o::numeric AS open, h::numeric AS high, l::numeric AS low, 
                  c::numeric AS close, v::numeric AS volume
           FROM perp_data
           WHERE symbol = $1 AND perpspec @> '["bin-ohlcv"]'::jsonb AND ts >= $2
           ORDER BY ts ASC`
        : `SELECT ts, o::numeric AS open, h::numeric AS high, l::numeric AS low, 
                  c::numeric AS close, v::numeric AS volume
           FROM perp_data
           WHERE symbol = $1 AND perpspec @> '["bin-ohlcv"]'::jsonb
           ORDER BY ts ASC`;
      
      const result = startTs 
        ? await dbManager.pool.query(query, [sym, startTs])
        : await dbManager.pool.query(query, [sym]);
      
      return result.rows.map(row => ({
        ts: BigInt(row.ts),
        open: parseFloat(row.open),
        high: parseFloat(row.high),
        low: parseFloat(row.low),
        close: parseFloat(row.close),
        volume: parseFloat(row.volume)
      })).filter(p => !isNaN(p.open) && !isNaN(p.high) && !isNaN(p.low) && 
                     !isNaN(p.close) && !isNaN(p.volume));
    }));

    if (mtData.some(data => data.length === 0)) {
      if (!recentOnly) {
        console.error('\x1b[31m❌ Missing data for some MT symbols\x1b[0m');
      }
      return;
    }

    const allTs = [...new Set(mtData.flatMap(data => data.map(p => p.ts.toString())))].sort();
    const mtRecords = allTs.map(ts => {
      const tsBigInt = BigInt(ts);
      let totalO = 0, totalH = 0, totalL = 0, totalC = 0, totalV = 0, count = 0;

      for (const data of mtData) {
        let lastValid = null;
        for (const p of data) {
          if (p.ts <= tsBigInt) lastValid = p;
          else break;
        }
        if (lastValid) {
          totalO += lastValid.open;
          totalH += lastValid.high;
          totalL += lastValid.low;
          totalC += lastValid.close;
          totalV += lastValid.volume;
          count++;
        }
      }

      if (count === 0) return null;

      return {
        ts: tsBigInt,
        symbol: MT_SYMBOL,
        perpspec: 'bin-ohlcv',
        o: totalO / count,
        h: totalH / count,
        l: totalL / count,
        c: totalC / count,
        v: totalV
      };
    }).filter(record => record !== null);

    if (mtRecords.length > 0) {
      await dbManager.insertBackfillData(mtRecords);
      const msg = recentOnly ? '🔗 MT token final loop completed' : '🔗 MT token created';
      await logStatus('completed', msg);
    }
  } catch (error) {
    console.error('\x1b[31m❌ MT creation failed:', error.message, '\x1b[0m');
  }
}

// ============================================================================
// MAIN BACKFILL (Historical full fetch)
// ============================================================================
async function backfillExchange(config) {
  await batchFetchAndInsert(config, { historicalMode: true });
}

// ============================================================================
// FINAL LOOP (Recent data only - fast targeted fetch)
// ============================================================================
async function fetchRecentDataFast(config) {
  const now = Date.now();
  const startTs = now - (RECENT_RECORDS_COUNT * 60 * 1000);

  const options = {
    limit: RECENT_RECORDS_COUNT,
    startTime: startTs,  // Binance uses this
    end: now,           // Bybit uses this
    historicalMode: false  // Use targeted fetch
  };

  return await batchFetchAndInsert(config, options);
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================
if (require.main === module) {
  backfill()
    .then(() => process.exit(0))
    .catch(err => {
      console.error('\x1b[31m💥 OHLCV backfill failed:', err, '\x1b[0m');
      process.exit(1);
    });
}

module.exports = { backfill };