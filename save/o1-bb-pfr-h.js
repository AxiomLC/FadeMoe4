// SCRIPT: bb-pfr-h.js  12 Oct 2025
// Unified Premium Funding Rate Backfill Script for Binance and Bybit
// Version: Binance/Bybit only, single slice for conservative backfill

const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = 'bb-pfr-h.js';
const weightMonitor = require('../b-weight');

// ============================================================================
// USER SPEED SETTINGS (adjust these for performance tuning)
// ============================================================================
const DAYS = 10;                      // Number of days back to fill
const SLICE_HOURS = 12;               // Size of each slice (12h typical)
const NUM_SLICES = 1;                 // Number of slices (1 for conservative)
const BIN_BYB_CONCURRENCY = 12;       // Binance/Bybit concurrency
const TIMEOUT_MS = 10000;             // HTTP request timeout
const RATE_DELAY = 100;                 // Global throttle, normally 0
const HEARTBEAT_INTERVAL = 10000;     // 10s heartbeat interval

const MS_PER_DAY = 24 * 60 * 60 * 1000;
const NOW = Date.now();
const START = NOW - DAYS * MS_PER_DAY;

// ============================================================================
// UTILITIES
// ============================================================================
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function floorToMinute(ts) { return Math.floor(ts / 60000) * 60000; }
function normalizeNumeric(v) { const n = parseFloat(v); return isNaN(n) ? null : n; }

// ============================================================================
// EXCHANGE CONFIGURATIONS
// ============================================================================
const EXCHANGES = {
  BINANCE: {
    perpspec: 'bin-pfr',
    url: 'https://fapi.binance.com/fapi/v1/premiumIndexKlines',
    limit: 1000,
    concurrency: BIN_BYB_CONCURRENCY,
    apiInterval: '1m',
    mapSymbol: s => `${s}USDT`,
    fetch: fetchBinancePFR,
    process: processStandardData
  },
  BYBIT: {
    perpspec: 'byb-pfr',
    url: 'https://api.bybit.com/v5/market/premium-index-price-kline',
    limit: 1000,
    concurrency: BIN_BYB_CONCURRENCY,
    apiInterval: '1',
    mapSymbol: s => `${s}USDT`,
    fetch: fetchBybitPFR,
    process: processStandardData
  }
};

// ============================================================================
// FETCH FUNCTIONS
// ============================================================================

async function fetchBinancePFR(symbol, config, startTs, endTs) {
  const slice = SLICE_HOURS * 60 * 60 * 1000;
  const sliceDuration = (endTs - startTs) / NUM_SLICES;
  const ranges = [];
  for (let i = 0; i < NUM_SLICES; i++) {
    const sliceStart = startTs + i * sliceDuration;
    const sliceEnd = Math.min(sliceStart + sliceDuration, endTs);
    for (let t = sliceStart; t < sliceEnd; t += slice)
      ranges.push([t, Math.min(t + slice, sliceEnd)]);
  }
//************************** below, lime 81, added 13 Oct - for B-Weight */
  const limit = pLimit(config.concurrency);
  const results = await Promise.allSettled(ranges.map(([s, e]) => limit(async () => {
    try {
      const res = await axios.get(config.url, { params: { symbol, interval: config.apiInterval, startTime: s, endTime: e, limit: config.limit }, timeout: TIMEOUT_MS });
      weightMonitor.logRequest('bin-pfr', '/fapi/v1/premiumIndexKlines', 1);
      await sleep(RATE_DELAY);
      return res.data || [];
    } catch { return []; }
  })));
  return results.flatMap(r => (r.status === 'fulfilled' ? r.value : []));
}

async function fetchBybitPFR(symbol, config, startTs, endTs) {
  const slice = SLICE_HOURS * 60 * 60 * 1000;
  const sliceDuration = (endTs - startTs) / NUM_SLICES;
  const ranges = [];
  for (let i = 0; i < NUM_SLICES; i++) {
    const sliceStart = startTs + i * sliceDuration;
    const sliceEnd = Math.min(sliceStart + sliceDuration, endTs);
    for (let t = sliceStart; t < sliceEnd; t += slice)
      ranges.push([t, Math.min(t + slice, sliceEnd)]);
  }

  const limit = pLimit(config.concurrency);
  const results = await Promise.allSettled(ranges.map(([s, e]) => limit(async () => {
    try {
      const res = await axios.get(config.url, { params: { category: 'linear', symbol, interval: config.apiInterval, limit: config.limit, start: s, end: e }, timeout: TIMEOUT_MS });
      const list = res.data.result?.list || [];
      await sleep(RATE_DELAY);
      return list;
    } catch { return []; }
  })));
  return results.flatMap(r => (r.status === 'fulfilled' ? r.value : []));
}

// ============================================================================
// DATA PROCESSING
// ============================================================================
function processStandardData(rawData, baseSymbol, config) {
  const perpspec = config.perpspec;
  const result = [];
  for (const d of rawData) {
    const ts = d[0];
    const pfr = parseFloat(d[4]);
    if (!isNaN(pfr)) result.push({ ts, symbol: baseSymbol, source: perpspec, perpspec, interval: '1m', pfr });
  }
  return result;
}

// ============================================================================
// HEARTBEAT + ERROR SUMMARY STATE
// ============================================================================
const heartbeatErrors = {
  bin: new Set(),
  byb: new Set()
};

async function heartbeatLoop() {
  while (!heartbeatStop) {
    for (const [key, cfg] of Object.entries(EXCHANGES)) {
      const perpspec = cfg.perpspec;
      const errSet = heartbeatErrors[key.toLowerCase()] || new Set();
      const errMsg = errSet.size > 0 ? `${perpspec} 429 errors on ${errSet.size} symbols.` : `${perpspec} backfilling ${perpList.length} symbols.`;
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', errMsg);
      console.log(`[hb] ${errMsg}`);
    }
    await sleep(HEARTBEAT_INTERVAL);
  }
}

let heartbeatStop = false;

// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================
async function backfill() {
  const startTime = Date.now();
  console.log(`\n🚀 Starting ${SCRIPT_NAME} predicted Funding Rate polling...`);
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} predicted Funding Rate polling.`);

  heartbeatStop = false;
  heartbeatLoop(); // start heartbeat async

  const tasks = [];
  for (const [key, cfg] of Object.entries(EXCHANGES)) {
    const limit = pLimit(cfg.concurrency);
    for (const baseSym of perpList) {
      tasks.push(limit(async () => {
        const symbol = cfg.mapSymbol(baseSym);
        try {
          const raw = await cfg.fetch(symbol, cfg, START, NOW);
          const processed = cfg.process(raw, baseSym, cfg);
          if (processed.length) await dbManager.insertData(cfg.perpspec, processed);
        } catch (err) {
          if ([429,418,500].includes(err.response?.status)) heartbeatErrors[key.toLowerCase()].add(symbol);
          else await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'FETCH_ERROR', `${cfg.perpspec} ${baseSym} failed: ${err.message}`);
        }
      }));
    }
  }

  await Promise.allSettled(tasks);
  heartbeatStop = true;

  const dur = ((Date.now() - startTime) / 1000).toFixed(1);
  const msg = `🎉 ${SCRIPT_NAME} completed in ${dur}s\n✅ Fast PFR backfill done.`;
  console.log(msg);
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', msg);
}

if (require.main === module) {
  backfill()
    .then(() => process.exit(0))
    .catch(err => { console.error('💥 Backfill failed:', err); process.exit(1); });
}

module.exports = { backfill };