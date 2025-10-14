/* ==========================================
 * all-lsr-c.js   11 Oct 2025
 * Continuous Long/Short Ratio Polling Script
 *
 * Fetches LSR data from Binance, Bybit, and OKX
 * Inserts 1m record immediately at current 1m boundary, caches 4 for 1m upserts
 * Logs high-level status messages for UI and monitoring
 * ========================================== */

const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');
require('dotenv').config();

const SCRIPT_NAME = 'all-lsr-c.js';
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
    URL: 'https://fapi.binance.com/futures/data/globalLongShortAccountRatio',
    DB_INTERVAL: '1m',
    API_INTERVAL: '5m',
    mapSymbol: sym => `${sym}USDT`
  },
  BYBIT: {
    PERPSPEC: 'byb-lsr',
    URL: 'https://api.bybit.com/v5/market/account-ratio',
    DB_INTERVAL: '1m',
    API_INTERVAL: '5min',
    mapSymbol: sym => `${sym}USDT`
  },
  OKX: {
    PERPSPEC: 'okx-lsr',
    URL: 'https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio-contract',
    DB_INTERVAL: '1m',
    API_INTERVAL: '5m',
    mapSymbol: sym => `${sym}-USDT-SWAP`
  }
};

// Track per symbol: lastTs (ms), cache {lsr, offsets: [60k,120k,180k,240k]}, fiveMTime (ms delay)
const symbolData = new Map(); // key: perpspec:symbol, value: {lastTs, cache, fiveMTime}

/* ==========================================
 * SHARED POLLING FUNCTION
 * Handles A-D logic for all exchanges
 * ========================================== */
async function pollLSR(baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  const key = `${perpspec}:${baseSymbol}`;
  const exchangeSymbol = config.mapSymbol(baseSymbol);
  const exchangeName = perpspec.split('-')[0];
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

    // Process
    let lsr = null;
    switch (perpspec) {
      case 'bin-lsr':
        const binPoint = rawData[rawData.length - 1];
        if (!binPoint) throw new Error(`No data for ${baseSymbol}`);
        lsr = parseFloat(binPoint.longShortRatio);
        break;
      case 'byb-lsr':
        const bybPoint = rawData.list[0];
        if (!bybPoint) throw new Error(`No data for ${baseSymbol}`);
        const buyRatio = parseFloat(bybPoint.buyRatio);
        const sellRatio = parseFloat(bybPoint.sellRatio);
        if (isNaN(buyRatio) || sellRatio === 0) throw new Error(`Invalid ratio for ${baseSymbol}`);
        lsr = buyRatio / sellRatio;
        break;
      case 'okx-lsr':
        const okxPoint = rawData[0];
        if (!okxPoint) throw new Error(`No data for ${baseSymbol}`);
        lsr = parseFloat(okxPoint[1]);
        break;
    }
    if (isNaN(lsr)) throw new Error(`Invalid LSR for ${baseSymbol}`);

    // Use current 1min boundary for first record
    const timestamp = Math.floor(now / (60*1000)) * (60*1000);

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

    // Upsert first 1m
    await dbManager.insertData(perpspec, [{
      ts: apiUtils.toMillis(BigInt(timestamp)),
      symbol: baseSymbol,
      source: perpspec,
      perpspec,
      interval: config.DB_INTERVAL,
      lsr
    }]);

    // Schedule next poll
    setTimeout(() => pollLSR(baseSymbol, config), data.fiveMTime + WAIT_BUFFER);

  } catch (error) {
    errorCount++;
    console.error(`[${perpspec}] Error polling ${baseSymbol}:`, error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'POLL_ERROR', error.message, {
      exchange: exchangeName,
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
  if (response.data.length === 0) throw new Error(`No data for ${symbol}`);
  return response.data;
}

async function fetchBybitLSR(symbol, config) {
  const params = { category: 'linear', symbol, period: config.API_INTERVAL, limit: 1 };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (!response.data.result?.list.length) throw new Error(`No data for ${symbol}`);
  return response.data.result;
}

async function fetchOkxLSR(instId, config) {
  const params = { instId, period: config.API_INTERVAL, limit: 1 };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (response.data.code !== '0' || !response.data.data.length) throw new Error(`No data for ${instId}`);
  return response.data.data;
}

/* ==========================================
 * POLL ALL SYMBOLS
 * Heartbeat for running log and cache release
 * ========================================== */

async function pollAllSymbols() {
  errorCount = 0; // Reset counter
  const successCounts = PERPSPECS.reduce((acc, p) => ({...acc, [p]: 0}), {});

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
          lsr: c.lsr
        }]);
        c.offsets.shift();
        if (!c.offsets.length) delete data.cache;
        successCounts[perpspec]++;
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
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'stopped', 'Script halted: Error threshold exceeded');
    process.exit(1);
  }

  // Log running heartbeat
  // *Unneeded - console.log(`\n[${new Date().toISOString().slice(11, 19)}] Polling ${perpList.length} symbols...`);
  for (const perpspec of PERPSPECS) {
    console.log(`${perpspec} 1min pull ${successCounts[perpspec]} symbols.`);
  }
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `${PERPSPECS.join(', ')} polling LSR ${perpList.length} symbols, 5x1min`);
}

/* ==========================================
 * MAIN EXECUTION
 * Start polling and log status
 * ========================================== */

async function execute() {
  console.log(`⏰ Starting ${SCRIPT_NAME} - Continuous Long/Short Ratio polling`);
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} connected`);

  // Initial poll all symbols
  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(10);
  await Promise.all(perpList.flatMap(symbol => Object.values(EXCHANGE_CONFIG).map(config => limit(() => pollLSR(symbol, config)))));

  // Log running status immediately
  console.log(`${SCRIPT_NAME} initial poll complete`);
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `${SCRIPT_NAME} initial poll complete`);

  // Heartbeat interval
  const heartbeatId = setInterval(async () => {
    try {
      await pollAllSymbols();
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `${SCRIPT_NAME} 1min lsr pull`);
    } catch (error) {
      console.error('Error in polling cycle:', error.message);
      await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'POLL_CYCLE_ERROR', error.message);
    }
  }, POLL_INTERVAL);

  // Graceful shutdown
  process.on('SIGINT', async () => {
    clearInterval(heartbeatId);
    console.log(`\n${SCRIPT_NAME} received SIGINT, stopping...`);
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
      console.error(`💥 ${SCRIPT_NAME} failed:`, err.message);
      apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'INITIALIZATION_FAILED', err.message);
      process.exit(1);
    });
}

module.exports = { execute };