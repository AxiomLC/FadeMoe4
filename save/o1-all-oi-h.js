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
const HEARTBEAT_INTERVAL = 20 * 1000; // 10 seconds

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
// MAIN BACKFILL FUNCTION (with reliable 10s heartbeat)
// ============================================================================
async function backfill() {
  console.log(`\n🚀 Starting ${SCRIPT_NAME} backfill (USD normalized)...`);
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} initialized.`);

  const totalSymbols = perpList.length;
  const startTime = Date.now();

  // -- Heartbeat --
  const heartbeatId = setInterval(() => {
    (async () => {
      const ts = new Date().toISOString();
      console.log(`${SCRIPT_NAME} running: backfilling ${totalSymbols} symbols`);
      for (const exKey of Object.keys(EXCHANGES)) {
        const cfg = EXCHANGES[exKey];
        try {
          await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `${cfg.perpspec} backfilling for ${totalSymbols} symbols.`);
        } catch (err) {
          console.error(`[heartbeat] DB log failed for ${cfg.perpspec}:`, err.message);
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
          if (!rawData.length) return;
          const processed = config.process(rawData, baseSym, config);
          if (!processed.length) return;
          await dbManager.insertData(config.perpspec, processed);
          // old status log - console.log(`✅ [${config.perpspec}] ${baseSym}: ${processed.length} records`);
        } catch (err) {
          console.error(`❌ [${config.perpspec}] ${baseSym}: ${err.message}`);
          await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'FETCH_ERROR', err.message, { perpspec: config.perpspec, symbol: baseSym });
        }
      }));
    }
  }

  await Promise.all(promises);
  clearInterval(heartbeatId);

  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`\nAll Open Interest backfills completed in ${duration}s!`);

  for (const exKey of Object.keys(EXCHANGES)) {
    const cfg = EXCHANGES[exKey];
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'complete', `${cfg.perpspec} backfill complete.`);
    console.log(`✅ ${cfg.perpspec} backfill complete.`);
  }

  console.log(`\n🎉 ${SCRIPT_NAME} completed successfully.`);
}

if (require.main === module) {
  backfill()
    .then(() => {
      // console.log('✅ Historic OI backfill complete (USD normalized).');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 OI backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { backfill };
