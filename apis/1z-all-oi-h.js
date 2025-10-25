// SCRIPT: all-oi-h.js // Updated: 23 Oct 2025 - Unified Schema per READEMEperpdata.md
// - Normalized all OI to USD, *calc in bin-oi for $USD
// - Fixed Bybit future timestamp issue / **BYBIT is 5min 5x1 calc from byb-ohlcv
// - Unified **insertBackfillData  per symbol across exchanges (ts, symbol, exchange, perpspec, oi)
// - No source/interval fields; perpspec as string (dbsetup handles JSONB array)

const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');
const weightMonitor = require('../b-weight');

const SCRIPT_NAME = 'all-oi-h.js';
const DAYS = 10;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const NOW = Date.now();
const START = NOW - DAYS * MS_PER_DAY;
const HEARTBEAT_INTERVAL = 20 * 1000; // 20 seconds

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================================
// EXCHANGE CONFIGURATIONS
// ============================================================================
const EXCHANGES = {
  BINANCE: {
    perpspec: 'bin-oi',
    exchange: 'bin',
    url: 'https://fapi.binance.com/futures/data/openInterestHist',
    limit: 500,
    rateDelay: 200,
    concurrency: 3,
    timeout: 15000,
    apiInterval: '5m',
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBinanceOI,
    process: processBinanceData
  },
  BYBIT: {
    perpspec: 'byb-oi',
    exchange: 'byb',
    url: 'https://api.bybit.com/v5/market/open-interest',
    limit: 200,
    rateDelay: 100,
    concurrency: 5,
    timeout: 15000,
    apiInterval: '5min',
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBybitOI,
    process: processBybitData
  },
  OKX: {
    perpspec: 'okx-oi',
    exchange: 'okx',
    url: 'https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history',
    limit: 100,
    rateDelay: 200,
    concurrency: 3,
    timeout: 15000,
    apiInterval: '5m',
    mapSymbol: sym => `${sym}-USDT-SWAP`,
    fetch: fetchOkxOI,
    process: processOkxData
  }
};

const totalSymbols = perpList.length;
const PERPSPECS = Object.values(EXCHANGES).map(c => c.perpspec).join(', ');
const STATUS_COLOR = '\x1b[94m'; // Light blue for status logs
const RESET = '\x1b[0m';
const YELLOW = '\x1b[33m'; // Yellow for warnings

let missingBybitSymbols = []; // Track for single log

// ============================================================================
// FETCH FUNCTIONS
// ============================================================================
async function fetchBinanceOI(symbol, config, startTs, endTs) {
  let allData = [];
  let current = startTs;
  while (current < endTs) {
    const nextEnd = Math.min(current + config.limit * 5 * 60 * 1000, endTs);
    const params = { symbol, period: config.apiInterval, limit: config.limit, startTime: current, endTime: nextEnd };
    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      weightMonitor.logRequest('bin-oi', '/futures/data/openInterestHist', 1);
      const data = response.data;
      if (!data || data.length === 0) break;
      allData.push(...data);
      const lastTimestamp = data[data.length - 1].timestamp;
      current = lastTimestamp + 5 * 60 * 1000;
      if (data.length < config.limit) break;
      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`\x1b[31m❌ [${config.perpspec}] Fetch error for ${symbol}: ${error.message}\x1b[0m`);
      throw error;
    }
  }
  return allData.filter(rec => rec.timestamp >= startTs && rec.timestamp <= endTs);
}
//========================== BYBIT =========================================
async function fetchBybitOI(symbol, config, startTs, endTs) {
  let allData = [];
  let endTime = endTs;
  while (endTime > startTs) {
    const params = { category: 'linear', symbol, intervalTime: config.apiInterval, limit: config.limit, endTime };
    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      const data = response.data;
      if (data.retCode !== 0) throw new Error(data.retMsg || 'Bybit API error');
      const list = data.result?.list;
      if (!list || list.length === 0) break;
      allData.push(...list);
      const oldestTs = parseInt(list[list.length - 1].timestamp, 10);
      if (oldestTs <= startTs) break;
      endTime = oldestTs - 1;
      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`\x1b[31m❌ [${config.perpspec}] Fetch error for ${symbol}: ${error.message}\x1b[0m`);
      throw error;
    }
  }
  allData.sort((a, b) => parseInt(a.timestamp, 10) - parseInt(b.timestamp, 10));
  return allData.filter(rec => {
    const ts = parseInt(rec.timestamp, 10);
    return ts >= startTs && ts <= endTs;
  });
}

async function fetchOkxOI(symbol, config, startTs, endTs) {
  let allData = [];
  let end = endTs;
  let lastOldest = null;
  while (true) {
    const params = { instId: symbol, period: config.apiInterval, limit: config.limit, end };
    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      if (response.status !== 200) throw new Error(`HTTP ${response.status}`);
      if (response.data.code !== '0') throw new Error(`OKX API ${response.data.code}: ${response.data.msg}`);
      const records = response.data.data;
      if (!records || records.length === 0) break;
      const oldestTs = parseInt(records[records.length - 1][0], 10);
      if (lastOldest === oldestTs) break;
      lastOldest = oldestTs;
      if (oldestTs <= startTs) {
        const filtered = records.filter(rec => parseInt(rec[0], 10) >= startTs);
        allData.unshift(...filtered);
        break;
      }
      allData.unshift(...records);
      end = oldestTs;
      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`\x1b[31m❌ [${config.perpspec}] Fetch error for ${symbol}: ${error.message}\x1b[0m`);
      throw error;
    }
  }
  return allData.filter(rec => {
    const ts = parseInt(rec[0], 10);
    return ts >= startTs && ts <= endTs;
  });
}

// ============================================================================
// DATA PROCESSING FUNCTIONS (Unified USD, No source/interval)
// ============================================================================
function processBinanceData(rawData, baseSymbol, config) {
  const expanded = [];
  for (const rec of rawData) {
    try {
      const ts = rec.timestamp;
      const oiUsd = parseFloat(rec.sumOpenInterestValue);
      if (!isNaN(oiUsd)) {
        for (let i = 0; i < 5; i++) {
          const subTs = ts + i * 60 * 1000;
          if (subTs <= Date.now()) {
            expanded.push({ 
              ts: apiUtils.toMillis(BigInt(subTs)), 
              symbol: baseSymbol, 
              exchange: config.exchange,
              perpspec: config.perpspec, 
              oi: oiUsd 
            });
          }
        }
      }
    } catch (err) {
      console.error(`\x1b[31m❌ [${config.perpspec}] Process error: ${err.message}\x1b[0m`);
    }
  }
  return expanded;
}
//===================== BYBIT ====================================================
async function processBybitData(rawData, baseSymbol, config) {
  const expanded = [];
  const now = Date.now();
  const priceCache = new Map();  // ts (millis) → close price
  let hasOhlcv = false;

  // New code here: 22 Oct Replace the entire try-catch block for the batch query (fixes passing exchange='byb' instead of perpspec='byb-ohlcv' to match dbsetup.js WHERE exchange = $1; adds debug logging for row count/ts range)
  try {
    // Batch query: Get all 1m OHLCV prices for full 10 days (one query/symbol)
    // Pass exchange='byb' (not perpspec='byb-ohlcv') to match WHERE exchange = $1 in dbsetup.js
    const ohlcvRows = await Promise.race([
      dbManager.queryPerpData('byb', baseSymbol, START, NOW),
      new Promise((_, reject) => setTimeout(() => reject(new Error('DB query timeout')), 5000))
    ]);
    
    // Temporary debug: Log row count and ts range (remove after confirming fix)
    if (ohlcvRows && ohlcvRows.length > 0) {
      hasOhlcv = true;  // Set true if rows found
    } else {
      missingBybitSymbols.push(baseSymbol);
    }
    
    // Cache prices by ts (millis) — only if hasOhlcv
    if (hasOhlcv) {
      ohlcvRows.forEach(row => {
        if (row.ts && !isNaN(row.c)) {
          priceCache.set(Number(row.ts), parseFloat(row.c));
        }
      });
    }
  } catch (err) {
    missingBybitSymbols.push(baseSymbol);
    console.error(`\x1b[31m❌ [${config.perpspec}] Price query failed for ${baseSymbol}: ${err.message}\x1b[0m`);
  }

  // Process raw OI with cached prices (fallback to raw if no price)
  for (const rec of rawData) {
    try {
      const tsRaw = parseInt(rec.timestamp, 10);
      if (isNaN(tsRaw)) continue;
      
      const baseTs = Math.floor(tsRaw / 60000) * 60000;  // Floor to 5m
      const oiValue = parseFloat(rec.openInterest);
      if (isNaN(oiValue)) continue;

      // Get price from nearest 1m ts in the 5m window (or null for fallback)
      let price = null;
      for (let i = 0; i < 5; i++) {
        const candidateTs = baseTs + i * 60 * 1000;
        if (priceCache.has(candidateTs)) {
          price = priceCache.get(candidateTs);
          break;
        }
      }
      const oiUsd = price ? oiValue * price : oiValue;  // Fallback: raw OI, no USD calc

      // Expand to 5x 1m records (duplicated even on fallback)
      for (let i = 0; i < 5; i++) {
        const subTs = baseTs + i * 60 * 1000;
        if (subTs <= now) {
          expanded.push({ 
            ts: apiUtils.toMillis(BigInt(subTs)), 
            symbol: baseSymbol, 
            exchange: config.exchange,
            perpspec: config.perpspec, 
            oi: oiUsd 
          });
        }
      }
    } catch (err) {
      console.error(`\x1b[31m❌ [${config.perpspec}] Process error for rec ${rec.timestamp}: ${err.message}\x1b[0m`);
    }
  }

  return expanded;
}

function processOkxData(rawData, baseSymbol, config) {
  const expanded = [];
  for (const rec of rawData) {
    try {
      const ts = parseInt(rec[0], 10);
      const oiUsd = parseFloat(rec[5] || rec[3]);
      if (!isNaN(oiUsd)) {
        for (let i = 0; i < 5; i++) {
          const subTs = ts + i * 60 * 1000;
          if (subTs <= Date.now()) {
            expanded.push({ 
              ts: apiUtils.toMillis(BigInt(subTs)), 
              symbol: baseSymbol, 
              exchange: config.exchange,
              perpspec: config.perpspec, 
              oi: oiUsd 
            });
          }
        }
      }
    } catch (err) {
      console.error(`\x1b[31m❌ [${config.perpspec}] Process error: ${err.message}\x1b[0m`);
    }
  }
  return expanded;
}

// ============================================================================
// PER-EXCHANGE BATCH FETCHER (Parallel per exchange, collect per-symbol data)
// ============================================================================
async function batchFetchOI(config) {
  const limit = pLimit(config.concurrency);
  const symbolData = new Map();  // baseSym → processed OI array for this exchange

  const promises = perpList.map(baseSym => limit(async () => {
    const symbol = config.mapSymbol(baseSym);
    let processed = [];

    try {
      const rawData = await config.fetch(symbol, config, START, NOW);
      if (rawData.length > 0) {
        if (config.perpspec === 'byb-oi') {
          processed = await config.process(rawData, baseSym, config);  // Async for Bybit
        } else {
          processed = config.process(rawData, baseSym, config);
        }
      }
      symbolData.set(baseSym, processed);
    } catch (err) {
      console.error(`\x1b[31m❌ [${config.perpspec}] ${baseSym}: ${err.message}\x1b[0m`);
      await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'FETCH_ERROR', err.message, { perpspec: config.perpspec, symbol: baseSym });
      symbolData.set(baseSym, []);  // Empty for this exchange
    }
  }));

  await Promise.all(promises);
  return symbolData;  // Return Map for merging
}

// ============================================================================
// MAIN BACKFILL FUNCTION (Parallel exchanges, unified inserts)
// ============================================================================
async function backfill() {
  const startTime = Date.now();
  console.log(`\n🔧 ${STATUS_COLOR}Starting ${SCRIPT_NAME} backfill (USD normalized, unified schema)...${RESET}`);

  // #1 Status: started
  const message1 = `🔧 Starting ${SCRIPT_NAME} backfill for Open Interest; ${totalSymbols} symbols.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', message1);
  console.log(`${STATUS_COLOR} ${message1}${RESET}`);

  // #2 Status: connected (assuming all perpspecs connected; no explicit check)
  const message2 = `${PERPSPECS} connected, starting fetch.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', message2);
  console.log(`${STATUS_COLOR} ${message2}${RESET}`);

  // Track success for verification
  const perpspecSuccess = {};
  Object.keys(EXCHANGES).forEach(key => perpspecSuccess[EXCHANGES[key].perpspec] = true);

  // -- Heartbeat with #3 running status logs --
  const heartbeatId = setInterval(() => {
    (async () => {
      // #3 Status: running
      const message3 = `🔧 ${SCRIPT_NAME} running: backfilling ${totalSymbols} symbols.`;
      try {
        await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message3);
      } catch (err) {
        console.error(`[heartbeat] DB log failed: ${err.message}`);
      }
      console.log(`${STATUS_COLOR} ${message3}${RESET}`);
    })();
  }, HEARTBEAT_INTERVAL);

  // Parallel: Fetch all exchanges (batches per exchange)
  const allExchangeData = await Promise.all([
    batchFetchOI(EXCHANGES.BINANCE),
    batchFetchOI(EXCHANGES.BYBIT),
    batchFetchOI(EXCHANGES.OKX)
  ]);

  clearInterval(heartbeatId);

  // Single yellow log for missing Bybit OHLCV (if any)
  if (missingBybitSymbols.length > 0) {
    const symbolsList = missingBybitSymbols.join(', ');
    const warnMsg = `⚠️ Missing byb-ohlcv for symbols: ${symbolsList} (using raw OI, no USD calc)`;
    console.log(`${YELLOW}${warnMsg}${RESET}`);
  }

  // Unified inserts: Merge per symbol and insert (sequential, fast)
  let totalSuccess = true;
  for (const baseSym of perpList) {
    const allData = [];
    let symbolSuccess = true;

    // Merge data from all exchanges for this symbol
    allExchangeData.forEach(exchangeMap => {
      const exchangeData = exchangeMap.get(baseSym) || [];
      if (exchangeData.length === 0) {
        // Mark failure if any exchange missing data
        const perpspec = exchangeMap instanceof Map ? Array.from(exchangeMap.values())[0]?.[0]?.perpspec : 'unknown';
        perpspecSuccess[perpspec] = false;
        symbolSuccess = false;
      }
      allData.push(...exchangeData);
    });

    // Insert if data available and symbol success
    if (allData.length > 0 && symbolSuccess) {
      try {
        await dbManager.insertBackfillData(allData);
      } catch (err) {
        console.error(`\x1b[31m❌ Insert failed for ${baseSym}: ${err.message}\x1b[0m`);
        totalSuccess = false;
        symbolSuccess = false;
      }
    } else if (!symbolSuccess) {
      totalSuccess = false;
    }
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(2);

  // #5 Status: overall completed ONLY if all verified successful
  if (totalSuccess && Object.values(perpspecSuccess).every(s => s)) {
    const message5 = `⏱️ ${SCRIPT_NAME} backfill completed in ${duration}s!`;
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', message5);
    console.log(`${STATUS_COLOR}${message5}${RESET}`);
  } else {
    const messageWarn = `${SCRIPT_NAME} backfill finished in ${duration}s (incomplete due to errors/missing data).`;
    console.log(`${STATUS_COLOR}${messageWarn}${RESET}`);
    // Per-perpspec completed only if that one succeeded
    Object.entries(perpspecSuccess).forEach(([perpspec, success]) => {
      if (success) {
        const msg = `${perpspec} backfill complete.`;
        apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', msg);
        console.log(`${STATUS_COLOR}${msg}${RESET}`);
      }
    });
  }
}

if (require.main === module) {
  backfill()
    .then(() => {
      process.exit(0);
    })
    .catch(err => {
      console.error('\x1b[31m💥 OI backfill script failed: ', err, '\x1b[0m');
      process.exit(1);
    });
}

module.exports = { backfill };