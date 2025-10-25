// SCRIPT: 1-bb-pfr-h.js (Unified Premium Funding Rate Backfill - Binance & Bybit)
// Updated: 22 Oct 2025 - Unified perp_data schema
// - Changed insertData() to ** insertBackfillData
// - Added apiUtils.toMillis() to floor timestamps to nearest minute (fixes odd-second timestamps)
// - 'perpspec' remains as string (dbManager handles JSONB array conversion internally)
// - Unified backfill for Binance (bin-pfr) and Bybit (byb-pfr) premium funding rates
// - Conservative single-slice, ts normalization to 1-minute intervals via apiUtils.toMillis()- Chunked concurrent fetching with p-limit rate control


const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = '1-bb-pfr-h.js';
const weightMonitor = require('../b-weight');

// ============================================================================
// USER SPEED SETTINGS
// ============================================================================
const STATUS_COLOR = '\x1b[94m'; // Light blue for status logs
const RESET = '\x1b[0m';
const DAYS = 10;                      // Days to backfill
const SLICE_HOURS = 12;               // Time slice size (12h)
const NUM_SLICES = 1;                 // Number of slices (1 = conservative)
const BIN_BYB_CONCURRENCY = 6;       // Concurrent symbols per exchange
const TIMEOUT_MS = 10000;             // Request timeout
const RATE_DELAY = 100;               // Throttle delay between requests
const HEARTBEAT_INTERVAL = 10000;     // Status heartbeat interval (10s)

const MS_PER_DAY = 24 * 60 * 60 * 1000;
const NOW = Date.now();
const START = NOW - DAYS * MS_PER_DAY;

// ============================================================================
// UTILITIES
// ============================================================================
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function normalizeNumeric(v) { const n = parseFloat(v); return isNaN(n) ? null : n; }

// ============================================================================
// EXCHANGE CONFIGURATIONS
// Defines API endpoints, limits, symbol mapping, and processing functions
// ============================================================================
const EXCHANGES = {
  BINANCE: {
    perpspec: 'bin-pfr',
    exchange: 'bin',
    url: 'https://fapi.binance.com/fapi/v1/premiumIndexKlines',
    limit: 900,
    concurrency: BIN_BYB_CONCURRENCY,
    apiInterval: '1m',
    mapSymbol: s => `${s}USDT`,
    fetch: fetchBinancePFR,
    process: processStandardData
  },
  BYBIT: {
    perpspec: 'byb-pfr',
    exchange: 'byb',
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
// Retrieves historical premium funding rate data from exchange APIs
// Uses time slicing to break large date ranges into manageable chunks
// ============================================================================

// Binance PFR fetch - uses premiumIndexKlines endpoint
async function fetchBinancePFR(symbol, config, startTs, endTs) {
  const slice = SLICE_HOURS * 60 * 60 * 1000;
  const sliceDuration = (endTs - startTs) / NUM_SLICES;
  const ranges = [];
  
  // Build time slice ranges
  for (let i = 0; i < NUM_SLICES; i++) {
    const sliceStart = startTs + i * sliceDuration;
    const sliceEnd = Math.min(sliceStart + sliceDuration, endTs);
    for (let t = sliceStart; t < sliceEnd; t += slice)
      ranges.push([t, Math.min(t + slice, sliceEnd)]);
  }
  
  // Concurrent fetch with rate limiting
  const limit = pLimit(config.concurrency);
  const results = await Promise.allSettled(ranges.map(([s, e]) => limit(async () => {
    try {
      const res = await axios.get(config.url, { 
        params: { 
          symbol, 
          interval: config.apiInterval, 
          startTime: s, 
          endTime: e, 
          limit: config.limit 
        }, 
        timeout: TIMEOUT_MS 
      });
      weightMonitor.logRequest('bin-pfr', '/fapi/v1/premiumIndexKlines', 1);
      await sleep(RATE_DELAY);
      return res.data || [];
    } catch { 
      return []; 
    }
  })));
  
  return results.flatMap(r => (r.status === 'fulfilled' ? r.value : []));
}

// Bybit PFR fetch - uses premium-index-price-kline endpoint
async function fetchBybitPFR(symbol, config, startTs, endTs) {
  const slice = SLICE_HOURS * 60 * 60 * 1000;
  const sliceDuration = (endTs - startTs) / NUM_SLICES;
  const ranges = [];

  // Build time slice ranges
  for (let i = 0; i < NUM_SLICES; i++) {
    const sliceStart = startTs + i * sliceDuration;
    const sliceEnd = Math.min(sliceStart + sliceDuration, endTs);
    for (let t = sliceStart; t < sliceEnd; t += slice)
      ranges.push([t, Math.min(t + slice, sliceEnd)]);
  }

  // Concurrent fetch with rate limiting
  const limit = pLimit(config.concurrency);
  const results = await Promise.allSettled(ranges.map(([s, e]) => limit(async () => {
    try {
      const res = await axios.get(config.url, { 
        params: { 
          category: 'linear', 
          symbol, 
          interval: config.apiInterval, 
          limit: config.limit, 
          start: s, 
          end: e 
        }, 
        timeout: TIMEOUT_MS 
      });
      const list = res.data.result?.list || [];
      await sleep(RATE_DELAY);
      return list;
    } catch { 
      return []; 
    }
  })));
  
  return results.flatMap(r => (r.status === 'fulfilled' ? r.value : []));
}

// ============================================================================
// DATA PROCESSING
// Converts raw API responses to unified perp_data format
// Key changes: BigInt timestamps, exchange field, no source/interval fields
// Timestamps floored to nearest minute using apiUtils.toMillis()
// ============================================================================
function processStandardData(rawData, baseSymbol, config) {
  const result = [];
  
  for (const d of rawData) {
    const rawTs = parseInt(d[0], 10);
    const ts = BigInt(apiUtils.toMillis(rawTs)); // Floor to nearest minute
    const pfr = normalizeNumeric(d[4]);
    
    if (pfr !== null && !isNaN(rawTs)) {
      result.push({ 
        ts: ts,
        symbol: baseSymbol, 
        exchange: config.exchange,
        perpspec: config.perpspec,
        pfr: pfr
      });
    }
  }
  
  return result;
}

// ============================================================================
// HEARTBEAT + ERROR SUMMARY STATE
// Tracks 429 errors per exchange for heartbeat logging
// ============================================================================
const heartbeatErrors = {
  bin: new Set(),
  byb: new Set()
};

// ============================================================================
// STATUS LOGGING AND BACKFILL ORCHESTRATOR
// Manages parallel backfill for all exchanges with progress tracking
// ============================================================================
const completedPerpspecs = new Set();

async function backfill() {
  const totalSymbols = perpList.length;

  // Log #1: Script start
  const startMessage = `🔧 Starting ${SCRIPT_NAME} backfill for Premium Funding Rates; ${totalSymbols} symbols.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', startMessage);
  console.log(`${STATUS_COLOR}${startMessage}${RESET}`);

  // Log #2: Perpspecs connected
  const perpspecs = Object.values(EXCHANGES).map(cfg => cfg.perpspec).join(', ');
  const connectMessage = `${perpspecs} connected, starting fetch.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', connectMessage);
  console.log(`${STATUS_COLOR}🔧 ${connectMessage}${RESET}`);

  const startTime = Date.now();

  // Heartbeat for perpspec running status
  const heartbeatId = setInterval(async () => {
    for (const [key, cfg] of Object.entries(EXCHANGES)) {
      if (!completedPerpspecs.has(cfg.perpspec)) {
        // Log #3: Perpspec running with error tracking
        const errSet = heartbeatErrors[key.toLowerCase().slice(0, 3)] || new Set();
        const runningMessage = errSet.size > 0 
          ? `${cfg.perpspec} 429 errors on ${errSet.size} symbols.` 
          : `${cfg.perpspec} backfilling db.`;
        await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', runningMessage, { perpspec: cfg.perpspec });
        console.log(`🔧${STATUS_COLOR} ${runningMessage}${RESET}`);
      }
    }
  }, HEARTBEAT_INTERVAL);

  // Build all fetch tasks
  const tasks = [];
  for (const [key, cfg] of Object.entries(EXCHANGES)) {
    const limit = pLimit(cfg.concurrency);
    const completedSymbols = new Set();

    for (const baseSym of perpList) {
      tasks.push(limit(async () => {
        const symbol = cfg.mapSymbol(baseSym);
        try {
          // Fetch raw data
          const raw = await cfg.fetch(symbol, cfg, START, NOW);
          
          // Process to unified format
          const processed = cfg.process(raw, baseSym, cfg);
          
          // Insert if data exists
          if (processed.length) {
            await dbManager.insertBackfillData(processed);
          }
          
          completedSymbols.add(baseSym);

          // Log #4: Perpspec completed (when all symbols done)
          if (completedSymbols.size === totalSymbols && !completedPerpspecs.has(cfg.perpspec)) {
            completedPerpspecs.add(cfg.perpspec);
            const completeMessage = `${cfg.perpspec} backfill complete.`;
            await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', completeMessage, { perpspec: cfg.perpspec });
            console.log(`${STATUS_COLOR}🔧 ${completeMessage}${RESET}`);
          }
        } catch (err) {
          // Track 429 errors for heartbeat
          if ([429, 418, 500].includes(err.response?.status)) {
            heartbeatErrors[key.toLowerCase().slice(0, 3)].add(symbol);
          } else {
            await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'FETCH_ERROR', `${cfg.perpspec} ${baseSym} failed: ${err.message}`);
          }
        }
      }));
    }
  }

  // Execute all tasks in parallel
  await Promise.allSettled(tasks);
  clearInterval(heartbeatId);

  // Log #5: Full script completion
  if (completedPerpspecs.size === Object.keys(EXCHANGES).length) {
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    const finalMessage = `⏱️ ${SCRIPT_NAME} backfill completed in ${duration}s!`;
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', finalMessage);
    console.log(`${finalMessage}`);
  }
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================
if (require.main === module) {
  backfill()
    .then(() => process.exit(0))
    .catch(err => { 
      console.error('💥 Backfill failed:', err); 
      process.exit(1); 
    });
}

module.exports = { backfill };