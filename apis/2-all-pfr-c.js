/* ==========================================
 * all-pfr-c.js   14 Oct 2025
 * Continuous Premium Funding Rate Polling Script
 *
 * Fetches PFR data from Binance, Bybit, and OKX
 * Inserts 1m record immediately at current 1m boundary, caches 4 for 1m upserts
 * Logs high-level status messages for UI and monitoring
 * ========================================== */

const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');
require('dotenv').config();

const SCRIPT_NAME = 'all-pfr-c.js';
const STATUS_COLOR = '\x1b[32m'; // Standard green (lighter than \x1b[92m) for status logs
const RESET = '\x1b[0m'; // Reset console color
const POLL_INTERVAL = 60 * 1000; // 1 minute heartbeat
const RETRY_INTERVAL = 10 * 1000; // 10s retry if no new
const WAIT_BUFFER = 5 * 1000; // +5s after 5m
const DEFAULT_5M_TIME = 300 * 1000; // 5min default
const ERROR_THRESHOLD = 50; // Max errors/min
const PERPSPECS = ['bin-pfr', 'byb-pfr', 'okx-pfr'];
let errorCount = 0; // Reset per heartbeat

/* ==========================================
 * EXCHANGE CONFIGURATION
 * ========================================== */
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-pfr',
    URL: 'https://fapi.binance.com/fapi/v1/premiumIndex',
    DB_INTERVAL: '1m',
    API_INTERVAL: '5m',
    mapSymbol: sym => `${sym}USDT`
  },
  BYBIT: {
    PERPSPEC: 'byb-pfr',
    URL: 'https://api.bybit.com/v5/market/funding/history',
    DB_INTERVAL: '1m',
    API_INTERVAL: '5min',
    mapSymbol: sym => `${sym}USDT`
  },
  OKX: {
    PERPSPEC: 'okx-pfr',
    URL: 'https://www.okx.com/api/v5/public/funding-rate-history',
    DB_INTERVAL: '1m',
    API_INTERVAL: '5m',
    mapSymbol: sym => `${sym}-USDT-SWAP`
  }
};

// Track per symbol: lastTs (ms), cache {pfr, offsets: [60k,120k,180k,240k]}, fiveMTime (ms delay)
const symbolData = new Map(); // key: perpspec:symbol, value: {lastTs, cache, fiveMTime}
// Track connected perpspecs for Log #2
const connectedPerpspecs = new Set();
// Track completed 1m pulls for Log #3
const completedPulls = new Map(PERPSPECS.map(p => [p, new Set()]));

/* ==========================================
 * SHARED POLLING FUNCTION
 * Handles A-D logic for all exchanges
 * ========================================== */
async function pollPFR(baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  const key = `${perpspec}:${baseSymbol}`;
  const exchangeSymbol = config.mapSymbol(baseSymbol);
  const exchangeName = perpspec.split('-')[0];
  const now = Date.now();

  try {
    // Fetch latest
    let rawData = null;
    switch (perpspec) {
      case 'bin-pfr':
        rawData = await fetchBinancePFR(exchangeSymbol, config);
        break;
      case 'byb-pfr':
        rawData = await fetchBybitPFR(exchangeSymbol, config);
        break;
      case 'okx-pfr':
        rawData = await fetchOkxPFR(exchangeSymbol, config);
        break;
    }

    // Log #2: First successful response for perpspec
    if (!connectedPerpspecs.has(perpspec)) {
      connectedPerpspecs.add(perpspec);
      if (connectedPerpspecs.size === PERPSPECS.length) {
        const message = `🧪 ${PERPSPECS.join(', ')} connected; fetching.`;
        await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', message);
        console.log(`${STATUS_COLOR}${message}${RESET}`);
      }
    }

    // Process
    let pfr = null;
    switch (perpspec) {
      case 'bin-pfr':
        const binPoint = Array.isArray(rawData) ? rawData[0] : rawData;
        if (!binPoint) throw new Error(`No data for ${baseSymbol}`);
        pfr = parseFloat(binPoint.lastFundingRate);
        break;
      case 'byb-pfr':
        const bybPoint = rawData.list[0];
        if (!bybPoint) throw new Error(`No data for ${baseSymbol}`);
        pfr = parseFloat(bybPoint.fundingRate);
        break;
      case 'okx-pfr':
        const okxPoint = rawData[0];
        if (!okxPoint) throw new Error(`No data for ${baseSymbol}`);
        pfr = parseFloat(okxPoint.fundingRate);
        break;
    }
    if (isNaN(pfr)) throw new Error(`Invalid PFR for ${baseSymbol}`);

    // Use current 1min boundary for first record
    const timestamp = Math.floor(now / (60*1000)) * (60*1000);

    const data = symbolData.get(key) || {lastTs: 0, cache: null, fiveMTime: DEFAULT_5M_TIME};
    if (timestamp <= data.lastTs) {
      // No new; retry
      setTimeout(() => pollPFR(baseSymbol, config), RETRY_INTERVAL);
      return;
    }

    // New record: Calc fiveMTime
    data.fiveMTime = data.lastTs ? timestamp - data.lastTs : DEFAULT_5M_TIME;
    data.lastTs = timestamp;
    data.cache = {pfr, offsets: [60*1000, 120*1000, 180*1000, 240*1000]}; // Cache at +1min to +4min
    symbolData.set(key, data);

    // Upsert first 1m
    await dbManager.insertData(perpspec, [{
      ts: apiUtils.toMillis(BigInt(timestamp)),
      symbol: baseSymbol,
      source: perpspec,
      perpspec,
      interval: config.DB_INTERVAL,
      pfr
    }]);

    // Log #3: Perpspec 1m pull completion
    completedPulls.get(perpspec).add(baseSymbol);
    const expectedCount = perpList.length;
    if (completedPulls.get(perpspec).size === expectedCount) {
      const message = `${perpspec} 1m pull.`;
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message, { perpspec });
      console.log(`${STATUS_COLOR}${message}${RESET}`);
      completedPulls.get(perpspec).clear(); // Reset for next 1m cycle
    }

    // Schedule next poll
    setTimeout(() => pollPFR(baseSymbol, config), data.fiveMTime + WAIT_BUFFER);

  } catch (error) {
    errorCount++;
    console.error(`[${perpspec}] Error polling ${baseSymbol}:`, error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'POLL_ERROR', error.message, {
      exchange: exchangeName,
      symbol: baseSymbol,
      perpspec
    });
    setTimeout(() => pollPFR(baseSymbol, config), RETRY_INTERVAL);
  }
}

/* ==========================================
 * EXCHANGE-SPECIFIC FETCH FUNCTIONS
 * ========================================== */

async function fetchBinancePFR(symbol, config) {
  const params = { symbol };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (!response.data) throw new Error(`No data for ${symbol}`);
  return response.data;
}

async function fetchBybitPFR(symbol, config) {
  const params = { category: 'linear', symbol, period: config.API_INTERVAL, limit: 1 };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (!response.data.result?.list.length) throw new Error(`No data for ${symbol}`);
  return response.data.result;
}

async function fetchOkxPFR(instId, config) {
  const params = { instId, limit: 1 };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (response.data.code !== '0' || !response.data.data.length) throw new Error(`No data for ${instId}`);
  return response.data.data;
}

/* ==========================================
 * POLL ALL SYMBOLS
 * Heartbeat for cache release
 * ========================================== */

async function pollAllSymbols() {
  errorCount = 0; // Reset counter

  // Release cached 1m records
  for (const [key, data] of symbolData) {
    const {cache: c, lastTs} = data;
    if (!c || !c.offsets.length) continue;
    const [perpspec, baseSymbol] = key.split(':');
    const nextTs = lastTs + c.offsets[0];
    if (Date.now() >= nextTs) {
      try {
        const exchange = Object.values(EXCHANGE_CONFIG).find(config => config.PERPSPEC === perpspec);
        if (!exchange) throw new Error(`No config for ${perpspec}`);
        await dbManager.insertData(perpspec, [{
          ts: apiUtils.toMillis(BigInt(nextTs)),
          symbol: baseSymbol,
          source: perpspec,
          perpspec,
          interval: exchange.DB_INTERVAL,
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
          console.log(`${STATUS_COLOR}${message}${RESET}`);
          completedPulls.get(perpspec).clear(); // Reset for next 1m cycle
        }
      } catch (error) {
        errorCount++;
        console.error(`[${perpspec}] Error polling ${baseSymbol}:`, error.message);
        await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'POLL_ERROR', error.message, {
          exchange: perpspec.split('-')[0],
          symbol: baseSymbol,
          perpspec
        });
      }
    }
  }

  if (errorCount > ERROR_THRESHOLD) {
    console.error('Script halted: Error threshold exceeded');
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'ERROR_THRESHOLD_EXCEEDED', 'Script halted: Error threshold exceeded');
    process.exit(1);
  }
}

/* ==========================================
 * MAIN EXECUTION
 * Start polling and log status
 * ========================================== */

async function execute() {
  // Log #1: Script start
  const totalSymbols = perpList.length;
  const startMessage = `🧪 Starting ${SCRIPT_NAME} real-time 1m pull; ${totalSymbols} symbols.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', startMessage);
  console.log(`${STATUS_COLOR}${startMessage}${RESET}`);

  // Initial poll all symbols
  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(10);
  await Promise.all(perpList.flatMap(symbol => Object.values(EXCHANGE_CONFIG).map(config => limit(() => pollPFR(symbol, config)))));

  // Heartbeat interval
  const heartbeatId = setInterval(async () => {
    try {
      await pollAllSymbols();
    } catch (error) {
      console.error('Error in polling cycle:', error.message);
      await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'POLL_CYCLE_ERROR', error.message);
    }
  }, POLL_INTERVAL);

  // Log #4: Graceful shutdown
  process.on('SIGINT', async () => {
    clearInterval(heartbeatId);
    const stopMessage = `🧪 ${SCRIPT_NAME} smoothly stopped.`;
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