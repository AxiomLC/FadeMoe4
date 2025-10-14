// SCRIPT: all-oi-h.js  (Unified USD Normalization + Heartbeat)
// Updated: 13 Oct 2025
// Unified Open Interest Backfill Script for Binance, Bybit, and OKX
// - Normalized all OI to USD
// - Fixed Bybit future timestamp issue
// - Added reliable 10s heartbeat (console + DB)
// - Removed old interim status logging

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
const HEARTBEAT_INTERVAL = 20 * 1000; // 20 seconds (as original; adjust if needed for 10s)

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================================
// EXCHANGE CONFIGURATIONS
// ============================================================================
const EXCHANGES = {
  BINANCE: {
    perpspec: 'bin-oi',
    url: 'https://fapi.binance.com/futures/data/openInterestHist',
    limit: 500,
    rateDelay: 100,
    concurrency: 3,
    timeout: 15000,
    apiInterval: '5m',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBinanceOI,
    process: processBinanceData
  },
  BYBIT: {
    perpspec: 'byb-oi',
    url: 'https://api.bybit.com/v5/market/open-interest',
    limit: 200,
    rateDelay: 100,
    concurrency: 5,
    timeout: 15000,
    apiInterval: '5min',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBybitOI,
    process: processBybitData
  },
  OKX: {
    perpspec: 'okx-oi',
    url: 'https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history',
    limit: 100,
    rateDelay: 200,
    concurrency: 3,
    timeout: 15000,
    apiInterval: '5m',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}-USDT-SWAP`,
    fetch: fetchOkxOI,
    process: processOkxData
  }
};

const totalSymbols = perpList.length;
const PERPSPECS = Object.values(EXCHANGES).map(c => c.perpspec).join(', ');
const STATUS_COLOR = '\x1b[94m'; // Light blue for status logs
const RESET = '\x1b[0m';

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
      //*********************line 84 added 13 Oct for B-Weight */
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      weightMonitor.logRequest('bin-oi', '/futures/data/openInterestHist', 1); //************added 13 Oct for B-Weight */
      const data = response.data;
      if (!data || data.length === 0) break;
      allData.push(...data);
      const lastTimestamp = data[data.length - 1].timestamp;
      current = lastTimestamp + 5 * 60 * 1000;
      if (data.length < config.limit) break;
      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
      throw error;
    }
  }
  return allData.filter(rec => rec.timestamp >= startTs && rec.timestamp <= endTs);
}

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
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
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
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
      throw error;
    }
  }
  return allData.filter(rec => {
    const ts = parseInt(rec[0], 10);
    return ts >= startTs && ts <= endTs;
  });
}

// ============================================================================
// DATA PROCESSING FUNCTIONS (Unified USD)
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
            expanded.push({ ts: apiUtils.toMillis(BigInt(subTs)), symbol: baseSymbol, source: config.perpspec, perpspec: config.perpspec, interval: config.dbInterval, oi: oiUsd });
          }
        }
      }
    } catch {}
  }
  return expanded;
}

function processBybitData(rawData, baseSymbol, config) {
  const expanded = [];
  for (const rec of rawData) {
    try {
      const tsRaw = parseInt(rec.timestamp, 10);
      const now = Date.now();
      const baseTs = Math.floor(tsRaw / 60000) * 60000;
      const oiValue = parseFloat(rec.openInterest);
      if (isNaN(oiValue)) continue;
      let price = null;
      const rows = dbManager.queryPerpData('byb-ohlcv', baseSymbol, baseTs - 5 * 60 * 1000, baseTs + 5 * 60 * 1000);
      if (rows?.length > 0) price = rows[rows.length - 1].c;
      const oiUsd = price ? oiValue * price : oiValue;
      for (let i = 0; i < 5; i++) {
        const ts = baseTs + i * 60 * 1000;
        if (ts <= now) {
          expanded.push({ ts: apiUtils.toMillis(BigInt(ts)), symbol: baseSymbol, source: config.perpspec, perpspec: config.perpspec, interval: config.dbInterval, oi: oiUsd });
        }
      }
    } catch {}
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
            expanded.push({ ts: apiUtils.toMillis(BigInt(subTs)), symbol: baseSymbol, source: config.perpspec, perpspec: config.perpspec, interval: config.dbInterval, oi: oiUsd });
          }
        }
      }
    } catch {}
  }
  return expanded;
}

// ============================================================================
// MAIN BACKFILL FUNCTION (with revised status logging)
// ============================================================================
async function backfill() {
  const startTime = Date.now();
  console.log(`\n🐬 Starting ${SCRIPT_NAME} backfill (USD normalized)...`);

  // #1 Status: started
  const message1 = `Starting ${SCRIPT_NAME} backfill for Open Interest; ${totalSymbols} symbols.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', message1);
  console.log(`${STATUS_COLOR}🐬 ${message1}${RESET}`);

  // #2 Status: connected (assuming all perpspecs connected; no explicit check)
  const message2 = `${PERPSPECS} connected, starting fetch.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', message2);
  console.log(`${STATUS_COLOR}🐳 ${message2}${RESET}`);

  // Initialize tracking for completion per perpspec
  const completedCounts = {};
  const completedLogged = new Set();
  for (const exKey of Object.keys(EXCHANGES)) {
    completedCounts[EXCHANGES[exKey].perpspec] = 0;
  }

  // -- Heartbeat with #3 running status logs (per perpspec, only if not completed) --
  const heartbeatId = setInterval(() => {
    (async () => {
      console.log(`${STATUS_COLOR}${SCRIPT_NAME} running: backfilling ${totalSymbols} symbols${RESET}`);
      for (const exKey of Object.keys(EXCHANGES)) {
        const cfg = EXCHANGES[exKey];
        if (!completedLogged.has(cfg.perpspec)) {
          // #3 Status: running
          const message = `${cfg.perpspec} backfilling db.`;
          try {
            await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message);
          } catch (err) {
            console.error(`[heartbeat] DB log failed for ${cfg.perpspec}:`, err.message);
          }
          console.log(`${STATUS_COLOR}${message}${RESET}`);
        }
      }
    })();
  }, HEARTBEAT_INTERVAL);

  const promises = [];
  for (const exKey of Object.keys(EXCHANGES)) {
    const config = EXCHANGES[exKey];
    const limit = pLimit(config.concurrency);
    for (const baseSym of perpList) {
      promises.push(limit(async () => {
        try {
          const symbol = config.mapSymbol(baseSym);
          const rawData = await config.fetch(symbol, config, START, NOW);
          let processed = [];
          if (rawData.length > 0) {
            processed = config.process(rawData, baseSym, config);
            if (processed.length > 0) {
              await dbManager.insertData(config.perpspec, processed);
            }
          }
          // Increment count on successful fetch (even if no data inserted)
          completedCounts[config.perpspec]++;
          // #4 Status: completed for this perpspec if all symbols processed
          if (completedCounts[config.perpspec] === totalSymbols && !completedLogged.has(config.perpspec)) {
            const message = `${config.perpspec} backfill complete.`;
            await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', message);
            console.log(`${STATUS_COLOR}${message}${RESET}`);
            completedLogged.add(config.perpspec);
          }
        } catch (err) {
          console.error(`❌ [${config.perpspec}] ${baseSym}: ${err.message}`);
          await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'FETCH_ERROR', err.message, { perpspec: config.perpspec, symbol: baseSym });
          // Note: Do not increment on error; perpspec completion requires all symbols to succeed
        }
      }));
    }
  }

  await Promise.all(promises);
  clearInterval(heartbeatId);

  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  // #5 Status: overall completed only if all perpspecs are full
  if (completedLogged.size === Object.keys(EXCHANGES).length) {
    const message5 = `${SCRIPT_NAME} backfill completed in ${duration}s!`;
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', message5);
    console.log(`${STATUS_COLOR}🐬 ${message5}${RESET}`);
  } else {
    const messageWarn = `${SCRIPT_NAME} backfill finished in ${duration}s (some perpspecs incomplete due to errors).`;
    console.log(`${STATUS_COLOR}${messageWarn}${RESET}`);
  }

  console.log(`\n🐬 ${SCRIPT_NAME} completed successfully.`);
}

if (require.main === module) {
  backfill()
    .then(() => {
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 OI backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { backfill };