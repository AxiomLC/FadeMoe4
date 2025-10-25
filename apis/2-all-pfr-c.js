/* ==========================================
 * 2-all-pfr-c.js   21 Oct 2025
 * Continuous Premium Funding Rate Polling Script - Unified Schema
 *
 * REVISION NOTES: Unified  Removed 'source' and 'interval' fields 
 * - Changed insertData() from dbManager.insertData(perpspec, data) to dbManager.insertData(data)
 * - Timestamps already floored to 1-minute boundaries via Math.floor(now / 60000) * 60000
 * - Removed apiUtils.toMillis() wrapper - timestamps already in milliseconds and floored
 * FEATURES:
 * - Fetches PFR data from Binance, Bybit, and OKX
 * - Inserts 1m record immediately at current 1m boundary
 * - Caches 4 additional records for next 4 minutes (1m upserts)
 * - Adaptive 5m polling based on actual exchange update intervals */

const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');
require('dotenv').config();

const SCRIPT_NAME = '2-all-pfr-c.js';
const STATUS_COLOR = '\x1b[32m'; // Standard green for status logs
const RESET = '\x1b[0m';
const POLL_INTERVAL = 60 * 1000; // 1 minute heartbeat
const RETRY_INTERVAL = 10 * 1000; // 10s retry if no new data
const WAIT_BUFFER = 5 * 1000; // +5s after 5m
const DEFAULT_5M_TIME = 300 * 1000; // 5min default
const ERROR_THRESHOLD = 50; // Max errors/min
const PERPSPECS = ['bin-pfr', 'byb-pfr', 'okx-pfr'];
let errorCount = 0; // Reset per heartbeat

/* ==========================================
 * EXCHANGE CONFIGURATION
 * Maps exchange-specific API details and symbol formats
 * ========================================== */
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-pfr',
    EXCHANGE: 'bin',
    URL: 'https://fapi.binance.com/fapi/v1/premiumIndex',
    API_INTERVAL: '5m',
    mapSymbol: sym => `${sym}USDT`
  },
  BYBIT: {
    PERPSPEC: 'byb-pfr',
    EXCHANGE: 'byb',
    URL: 'https://api.bybit.com/v5/market/funding/history',
    API_INTERVAL: '5min',
    mapSymbol: sym => {  // Updated: Handle meme coins like in 1z-web-ohlcv-c.js (prevents fetch failures)
      const memeCoins = ['BONK', 'PEPE', 'FLOKI', 'TOSHI'];
      return memeCoins.includes(sym) ? `1000${sym}USDT` : `${sym}USDT`;
    }
  },
  OKX: {
    PERPSPEC: 'okx-pfr',
    EXCHANGE: 'okx',
    URL: 'https://www.okx.com/api/v5/public/funding-rate-history',
    API_INTERVAL: '5m',
    mapSymbol: sym => `${sym}-USDT-SWAP`  // Unchanged, but consistent with OHLCV
  }
};

/* ==========================================
 * STATE TRACKING
 * ========================================== */
// Track per symbol: lastTs (ms), cache {pfr, offsets: [60k,120k,180k,240k]}, fiveMTime (ms delay)
const symbolData = new Map(); // key: perpspec:symbol, value: {lastTs, cache, fiveMTime}

// Track connected perpspecs for Log #2
const connectedPerpspecs = new Set();

// Track completed 1m pulls for Log #3
const completedPulls = new Map(PERPSPECS.map(p => [p, new Set()]));

/* ==========================================
 * SHARED POLLING FUNCTION  * Implements A-D logic for all exchanges:
 * A) Fetch latest data   * B) Check if new (timestamp > lastTs)
 * C) If new: insert immediate 1m record, cache next 4  * D) Schedule next poll based on adaptive 5m timing
 * ========================================== */
async function pollPFR(baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  const exchange = config.EXCHANGE;
  const key = `${perpspec}:${baseSymbol}`;
  const exchangeSymbol = config.mapSymbol(baseSymbol);
  const now = Date.now();

  try {
    // A) Fetch latest data from exchange
    let rawData = null;
    switch (perpspec) {
      case 'bin-pfr':
        rawData = await fetchBinancePFR(exchangeSymbol, config);
        break;
      case 'byb-pfr':
        rawData = await fetchBybitPFR(exchangeSymbol, config);
        break;
      case 'okx-pfr':
        rawData = await fetchOkxPFR(exchangeSymbol, config);  // Fixed: Pass exchangeSymbol (was instId)
        break;
    }

    // Log #2: First successful response for perpspec
    if (!connectedPerpspecs.has(perpspec)) {
      connectedPerpspecs.add(perpspec);
      if (connectedPerpspecs.size === PERPSPECS.length) {
        const message = `🚥 ${PERPSPECS.join(', ')} connected; fetching.`;
        await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', message);
        console.log(`${STATUS_COLOR}${message}${RESET}`);
      }
    }

    // Parse PFR value from exchange response (with validation)
    let pfr = null;
    switch (perpspec) {
      case 'bin-pfr':
        const binPoint = Array.isArray(rawData) ? rawData[0] : rawData;
        if (!binPoint) throw new Error(`No data for ${baseSymbol}`);
        pfr = parseFloat(binPoint.lastFundingRate);
        break;
      case 'byb-pfr':
        const bybPoint = rawData.list[0];  // Fixed: rawData is already .result from fetch
        if (!bybPoint) throw new Error(`No data for ${baseSymbol}`);
        pfr = parseFloat(bybPoint.fundingRate);
        break;
      case 'okx-pfr':
        const okxPoint = rawData[0];
        if (!okxPoint) throw new Error(`No data for ${baseSymbol}`);
        pfr = parseFloat(okxPoint.fundingRate);
        break;
    }
    if (isNaN(pfr) || pfr === null) {  // Added: Skip invalid PFR to avoid bad inserts/null propagation
      console.warn(`[${perpspec}] Invalid PFR (${pfr}) for ${baseSymbol}; skipping.`);
      setTimeout(() => pollPFR(baseSymbol, config), RETRY_INTERVAL);
      return;
    }

    // B) Use current 1min boundary for timestamp (standardized flooring for consistency)
    const timestamp = Math.floor(now / 60000) * 60000;  // Explicit 1m floor in ms

    const data = symbolData.get(key) || {lastTs: 0, cache: null, fiveMTime: DEFAULT_5M_TIME};
    
    // Check if new data (timestamp must be > lastTs)
    if (timestamp <= data.lastTs) {
      // No new data; retry after delay
      setTimeout(() => pollPFR(baseSymbol, config), RETRY_INTERVAL);
      return;
    }

    // C) New record detected
    // Calculate adaptive 5m timing based on actual interval
    data.fiveMTime = data.lastTs ? timestamp - data.lastTs : DEFAULT_5M_TIME;
    data.lastTs = timestamp;
    data.cache = {pfr, offsets: [60*1000, 120*1000, 180*1000, 240*1000]}; // Cache at +1min to +4min
    symbolData.set(key, data);

    // Insert immediate 1m record (unified format: perpspec as string; only pfr provided - COALESCE in insertData preserves existing OHLCV/other fields from prior inserts like 1z-web-ohlcv-c.js)
    await dbManager.insertData([{
      ts: BigInt(timestamp),
      symbol: baseSymbol,
      exchange: exchange,
      perpspec: perpspec,  // String; _mergeRawData wraps to array & appends uniquely (e.g., adds 'bin-pfr' to existing ['bin-ohlcv'])
      pfr: pfr
    }]);

    // Log #3: Perpspec 1m pull completion
    completedPulls.get(perpspec).add(baseSymbol);
    const expectedCount = perpList.length;
    if (completedPulls.get(perpspec).size === expectedCount) {
      const message = `${perpspec} 1m pull.`;
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message, { perpspec });
      console.log(`${STATUS_COLOR}🚥 ${message}${RESET}`);
      completedPulls.get(perpspec).clear(); // Reset for next 1m cycle
    }

    // D) Schedule next poll based on adaptive timing
    setTimeout(() => pollPFR(baseSymbol, config), data.fiveMTime + WAIT_BUFFER);

  } catch (error) {
    errorCount++;
    console.error(`[${perpspec}] Error polling ${baseSymbol}:`, error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'POLL_ERROR', error.message, {
      exchange: exchange,
      symbol: baseSymbol,
      perpspec
    });
    setTimeout(() => pollPFR(baseSymbol, config), RETRY_INTERVAL);
  }
}

/* ==========================================
 * EXCHANGE-SPECIFIC FETCH FUNCTIONS
 * Raw API calls to retrieve latest PFR data
 * ========================================== */

async function fetchBinancePFR(symbol, config) {  // Unchanged
  const params = { symbol };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (!response.data) throw new Error(`No data for ${symbol}`);
  return response.data;
}

async function fetchBybitPFR(symbol, config) {  // Fixed: Return .result directly (matches parse logic); limit=1 for latest
  const params = { category: 'linear', symbol, period: config.API_INTERVAL, limit: 1 };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (!response.data.result?.list.length) throw new Error(`No data for ${symbol}`);
  return response.data.result;  // Return .result (list is inside)
}

async function fetchOkxPFR(exchangeSymbol, config) {  // Fixed: Param name to exchangeSymbol for clarity; return .data
  const params = { instId: exchangeSymbol, limit: 1 };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (response.data.code !== '0' || !response.data.data.length) throw new Error(`No data for ${exchangeSymbol}`);
  return response.data.data;  // Return .data array directly
}

/* ==========================================
 * CACHE RELEASE HEARTBEAT
 * Releases cached 1m records at appropriate intervals
 * Runs every POLL_INTERVAL (60s)
 * ========================================== */

async function pollAllSymbols() {
  errorCount = 0; // Reset counter

  // Release cached 1m records when their time arrives
  for (const [key, data] of symbolData) {
    const {cache: c, lastTs} = data;
    if (!c || !c.offsets.length) continue;
    
    const [perpspec, baseSymbol] = key.split(':');
    const nextTs = lastTs + c.offsets[0];
    
    if (Date.now() >= nextTs) {
      try {
        const config = Object.values(EXCHANGE_CONFIG).find(cfg => cfg.PERPSPEC === perpspec);  // Fixed: Use 'config' var for clarity
        if (!config) throw new Error(`No config for ${perpspec}`);
        
        // Added: Validate cached pfr before insert (same as immediate)
        if (isNaN(c.pfr) || c.pfr === null) {
          console.warn(`[${perpspec}] Invalid cached PFR for ${baseSymbol}; skipping release.`);
          c.offsets.shift();
          if (!c.offsets.length) delete data.cache;
          continue;
        }
        
        // Insert cached record (unified format: same as immediate insert - perpspec string; only pfr; COALESCE preserves OHLCV etc.)
        await dbManager.insertData([{
          ts: BigInt(nextTs),
          symbol: baseSymbol,
          exchange: config.EXCHANGE,
          perpspec: perpspec,  // String; appends to existing perpspec array via insertData (additive, no overwrite of OHLCV/o/h/l/c/v)
          pfr: c.pfr
        }]);
        
        c.offsets.shift();
        if (!c.offsets.length) delete data.cache;

        // Log #3: Perpspec 1m pull completion (for cached records)
        completedPulls.get(perpspec).add(baseSymbol);
        const expectedCount = perpList.length;
        if (completedPulls.get(perpspec).size === expectedCount) {
          const message = `${perpspec} 1m pull.`;
          await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message, { perpspec });
          console.log(`${STATUS_COLOR}🚥 ${message}${RESET}`);
          completedPulls.get(perpspec).clear(); // Reset for next 1m cycle
        }
      } catch (error) {
        errorCount++;
        console.error(`[${perpspec}] Error releasing cache for ${baseSymbol}:`, error.message);
        await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'CACHE_RELEASE_ERROR', error.message, {
          exchange: perpspec.split('-')[0],
          symbol: baseSymbol,
          perpspec
        });
      }
    }
  }

  // Error threshold check
  if (errorCount > ERROR_THRESHOLD) {
    console.error('Script halted: Error threshold exceeded');
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'ERROR_THRESHOLD_EXCEEDED', 'Script halted: Error threshold exceeded');
    process.exit(1);
  }
}

/* ==========================================
 * MAIN EXECUTION
 * Initializes polling for all symbols and starts heartbeat
 * ========================================== */

async function execute() {
  // Log #1: Script start
  const totalSymbols = perpList.length;
  const startMessage = `🚦 Starting ${SCRIPT_NAME} real-time 1m pull; ${totalSymbols} symbols.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', startMessage);
  console.log(`${STATUS_COLOR}${startMessage}${RESET}`);

  // Initial poll all symbols with concurrency control
  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(10);
  await Promise.all(perpList.flatMap(symbol => 
    Object.values(EXCHANGE_CONFIG).map(config => 
      limit(() => pollPFR(symbol, config))
    )
  ));

  // Start heartbeat interval for cache release
  const heartbeatId = setInterval(async () => {
    try {
      await pollAllSymbols();
    } catch (error) {
      console.error('Error in polling cycle:', error.message);
      await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'POLL_CYCLE_ERROR', error.message);
    }
  }, POLL_INTERVAL);

  // Log #4: Graceful shutdown handler
  process.on('SIGINT', async () => {
    clearInterval(heartbeatId);
    const stopMessage = `🛑 ${SCRIPT_NAME} smoothly stopped.`;
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', stopMessage);
    console.log(`${STATUS_COLOR}${stopMessage}${RESET}`);
    process.exit(0);
  });
}

/* ==========================================
 * MODULE ENTRY POINT
 * ========================================== */

if (require.main === module) {
  execute()
    .catch(err => {
      console.error(`💥 ${SCRIPT_NAME} failed:`, err.message);
      apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'INITIALIZATION_FAILED', err.message);
      process.exit(1);
    });
}

module.exports = { execute };