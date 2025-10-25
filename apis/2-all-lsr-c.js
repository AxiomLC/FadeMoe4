/* ==========================================
 * all-lsr-c.js   22 Oct 2025 - Unified Schema
 * Revised Continuous Long/Short Ratio Polling Script
 *
 * Applies unified insertData method per READEMEperpdata.md
 * Maintains existing functions and logic (A-D, cache offsets)
 * Unified: insertData (partial DO UPDATE); no source/interval; explicit exchange
 * ========================================== */

const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');
require('dotenv').config();

const SCRIPT_NAME = 'all-lsr-c.js';
const STATUS_COLOR = '\x1b[92m'; // Light green for status logs
const RESET = '\x1b[0m'; // Reset console color
const POLL_INTERVAL = 60 * 1000; // 1 minute heartbeat
const RETRY_INTERVAL = 10 * 1000; // 10s retry if no new
const WAIT_BUFFER = 5 * 1000; // +5s after 5m
const DEFAULT_5M_TIME = 300 * 1000; // 5min default
const ERROR_THRESHOLD = 50; // Max errors/min
const PERPSPECS = ['bin-lsr', 'byb-lsr', 'okx-lsr'];
let errorCount = 0; // Reset per heartbeat

/* ==========================================
 * EXCHANGE CONFIGURATION
 * ========================================== */
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-lsr',
    DB_EXCHANGE: 'bin',
    URL: 'https://fapi.binance.com/futures/data/globalLongShortAccountRatio',
    DB_INTERVAL: '1m',
    API_INTERVAL: '5m',
    mapSymbol: sym => `${sym}USDT`
  },
  BYBIT: {
    PERPSPEC: 'byb-lsr',
    DB_EXCHANGE: 'byb',
    URL: 'https://api.bybit.com/v5/market/account-ratio',
    DB_INTERVAL: '1m',
    API_INTERVAL: '5min',
    mapSymbol: sym => {  // Added: Handle meme coins like in 1z-web-ohlcv-c.js (prevents fetch failures)
      const memeCoins = ['BONK', 'PEPE', 'FLOKI', 'TOSHI'];
      return memeCoins.includes(sym) ? `1000${sym}USDT` : `${sym}USDT`;
    }
  },
  OKX: {
    PERPSPEC: 'okx-lsr',
    DB_EXCHANGE: 'okx',
    URL: 'https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio-contract',
    DB_INTERVAL: '1m',
    API_INTERVAL: '5m',
    mapSymbol: sym => `${sym}-USDT-SWAP`
  }
};

// Track per symbol: lastTs (ms), cache {lsr, offsets: [60k,120k,180k,240k]}, fiveMTime (ms delay)
const symbolData = new Map(); // key: perpspec:symbol, value: {lastTs, cache, fiveMTime}
// Track connected perpspecs for Log #2
const connectedPerpspecs = new Set();
// Track completed 1m pulls for Log #3
const completedPulls = new Map(PERPSPECS.map(p => [p, new Set()]));

/* ==========================================
 * SHARED POLLING FUNCTION
 * Handles A-D logic for all exchanges
 * ========================================== */
async function pollLSR(baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  const key = `${perpspec}:${baseSymbol}`;
  const exchangeSymbol = config.mapSymbol(baseSymbol);
  const now = Date.now();

  try {
    // Fetch latest
    let rawData = null;
    switch (perpspec) {
      case 'bin-lsr':
        rawData = await fetchBinanceLSR(exchangeSymbol, config);
        break;
      case 'byb-lsr':
        rawData = await fetchBybitLSR(exchangeSymbol, config);
        break;
      case 'okx-lsr':
        rawData = await fetchOkxLSR(exchangeSymbol, config);
        break;
    }

    // Log #2: First successful response for perpspec
    if (!connectedPerpspecs.has(perpspec) && rawData) {
      connectedPerpspecs.add(perpspec);
      if (connectedPerpspecs.size === PERPSPECS.length) {
        const message = `🍾 ${PERPSPECS.join(', ')} connected; fetching.`;
        await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', message);
        console.log(`${STATUS_COLOR}${message}${RESET}`);
      }
    }

    // Process
    let lsr = null;
    switch (perpspec) {
      case 'bin-lsr':
        const binPoint = rawData[rawData.length - 1];
        if (!binPoint) throw new Error(`No data for ${baseSymbol}`);
        lsr = parseFloat(binPoint.longShortRatio);
        break;
      case 'byb-lsr':
        const bybPoint = rawData.list[0];  // Fixed: rawData is .result
        if (!bybPoint) throw new Error(`No data for ${baseSymbol}`);
        const buyRatio = parseFloat(bybPoint.buyRatio);
        const sellRatio = parseFloat(bybPoint.sellRatio);
        if (isNaN(buyRatio) || sellRatio <= 0) {  // Added: Validate sellRatio > 0
          console.warn(`[byb-lsr] Invalid ratios for ${baseSymbol} (buy=${buyRatio}, sell=${sellRatio}); skipping.`);
          setTimeout(() => pollLSR(baseSymbol, config), RETRY_INTERVAL);
          return;
        }
        lsr = buyRatio / sellRatio;
        break;
      case 'okx-lsr':
        const okxPoint = rawData[0];
        if (!okxPoint) throw new Error(`No data for ${baseSymbol}`);
        lsr = parseFloat(okxPoint[1]);
        break;
    }
    if (isNaN(lsr) || lsr <= 0) {  // Added: Validate lsr
      console.warn(`[${perpspec}] Invalid LSR (${lsr}) for ${baseSymbol}; skipping.`);
      setTimeout(() => pollLSR(baseSymbol, config), RETRY_INTERVAL);
      return;
    }

    // Use current 1min boundary for first record (standardized flooring)
    const timestamp = Math.floor(now / 60000) * 60000;

    const data = symbolData.get(key) || {lastTs: 0, cache: null, fiveMTime: DEFAULT_5M_TIME};
    if (timestamp <= data.lastTs) {
      // No new; retry
      setTimeout(() => pollLSR(baseSymbol, config), RETRY_INTERVAL);
      return;
    }

    // New record: Calc fiveMTime
    data.fiveMTime = data.lastTs ? timestamp - data.lastTs : DEFAULT_5M_TIME;
    data.lastTs = timestamp;
    data.cache = {lsr, offsets: [60*1000, 120*1000, 180*1000, 240*1000]}; // Cache at +1min to +4min
    symbolData.set(key, data);

    // Upsert first 1m using unified insertData (only lsr set; preserves OHLCV/etc. via COALESCE)
    await dbManager.insertData([{
      ts: apiUtils.toMillis(BigInt(timestamp)),
      symbol: baseSymbol,
      exchange: config.DB_EXCHANGE,
      perpspec, // String; insertData wraps to array & appends uniquely
      lsr
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
    setTimeout(() => pollLSR(baseSymbol, config), data.fiveMTime + WAIT_BUFFER);

  } catch (error) {
    errorCount++;
    console.error(`[${perpspec}] Error polling ${baseSymbol}:`, error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'POLL_ERROR', error.message, {
      exchange: config.DB_EXCHANGE,  // Fixed: Use config.DB_EXCHANGE
      symbol: baseSymbol,
      perpspec
    });
    setTimeout(() => pollLSR(baseSymbol, config), RETRY_INTERVAL);
  }
}

/* ==========================================
 * EXCHANGE-SPECIFIC FETCH FUNCTIONS
 * ========================================== */

async function fetchBinanceLSR(symbol, config) {
  const params = { symbol, period: config.API_INTERVAL, limit: 1 };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (!response.data || response.data.length === 0) throw new Error(`No data for ${symbol}`);  // Added: Validate
  return response.data;
}

async function fetchBybitLSR(symbol, config) {
  const params = { category: 'linear', symbol, period: config.API_INTERVAL, limit: 1 };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (!response.data.result?.list.length) throw new Error(`No data for ${symbol}`);
  return response.data.result;  // Return .result
}

async function fetchOkxLSR(instId, config) {  // Param: instId for OKX
  const params = { instId, period: config.API_INTERVAL, limit: 1 };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (response.data.code !== '0' || !response.data.data.length) throw new Error(`No data for ${instId}`);
  return response.data.data;  // Return .data
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
      let config = null;  // Fixed: Declare config outside try (avoids undeclared var if error before assignment)
      try {
        config = Object.values(EXCHANGE_CONFIG).find(cfg => cfg.PERPSPEC === perpspec);  // Fixed: Use config
        if (!config) throw new Error(`No config for ${perpspec}`);
        
        // Added: Validate cached lsr before insert
        if (isNaN(c.lsr) || c.lsr <= 0) {
          console.warn(`[${perpspec}] Invalid cached LSR for ${baseSymbol}; skipping release.`);
          c.offsets.shift();
          if (!c.offsets.length) delete data.cache;
          continue;
        }
        
        await dbManager.insertData([{
          ts: apiUtils.toMillis(BigInt(nextTs)),
          symbol: baseSymbol,
          exchange: config.DB_EXCHANGE,
          perpspec, // String; insertData wraps to array & appends uniquely (additive, no overwrite)
          lsr: c.lsr
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
        console.error(`[${perpspec}] Error releasing cache for ${baseSymbol}:`, error.message);
        await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'CACHE_RELEASE_ERROR', error.message, {  // Fixed: CACHE_RELEASE_ERROR
          exchange: config ? config.DB_EXCHANGE : perpspec.split('-')[0],  // Safe fallback if config null
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
  const startMessage = `🍾 Starting ${SCRIPT_NAME} real-time 1m pull; ${totalSymbols} symbols.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', startMessage);
  console.log(`${STATUS_COLOR}${startMessage}${RESET}`);

  // Initial poll all symbols
  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(10);
  await Promise.all(perpList.flatMap(symbol => Object.values(EXCHANGE_CONFIG).map(config => limit(() => pollLSR(symbol, config)))));

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
    const stopMessage = `🍾 ${SCRIPT_NAME} smoothly stopped.`;
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