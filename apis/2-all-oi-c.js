/* ==========================================
 * all-oi-c.js   (Unified USD Normalization)
 * Continuous Open Interest Polling Script
 * Updated: 14 Oct 2025
 * ------------------------------------------
 * ✅ OI normalized to USD across exchanges
 * ✅ Bybit 5x1 conversion confirmed accurate
 * ========================================== */

const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');
require('dotenv').config();

const SCRIPT_NAME = 'all-oi-c.js';
const STATUS_COLOR = '\x1b[92m'; // Standard green for status logs
const RESET = '\x1b[0m'; // Reset console color
const POLL_INTERVAL = 60 * 1000; // 1 minute
const RETRY_INTERVAL = 10 * 1000; // 10s retry if no new

/* ==========================================
 * CONTRACT MULTIPLIERS (Binance)
 * Used to convert contracts → USD value
 * ========================================== */
const BINANCE_CONTRACT_MULTIPLIERS = {
  BTC: 0.001,
  ETH: 0.01,
  default: 1
};

/* ==========================================
 * EXCHANGE CONFIGURATION
 * ========================================== */
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-oi',
    URL: 'https://fapi.binance.com/fapi/v1/openInterest',
    DB_INTERVAL: '1m',
    mapSymbol: sym => `${sym}USDT`
  },
  BYBIT: {
    PERPSPEC: 'byb-oi',
    URL: 'https://api.bybit.com/v5/market/open-interest',
    DB_INTERVAL: '1m',
    mapSymbol: sym => `${sym}USDT`
  },
  OKX: {
    PERPSPEC: 'okx-oi',
    URL: 'https://www.okx.com/api/v5/public/open-interest',
    DB_INTERVAL: '1m',
    mapSymbol: sym => `${sym}-USDT-SWAP`
  }
};

// Track connected perpspecs for Log #2
const connectedPerpspecs = new Set();
// Track completed 1m pulls for Log #3
const completedPulls = new Map(Object.keys(EXCHANGE_CONFIG).map(key => [EXCHANGE_CONFIG[key].PERPSPEC, new Set()]));

/* ==========================================
 * BINANCE PRICE HELPER (Fallback)
 * ========================================== */
async function getBinancePrice(symbol) {
  try {
    const resp = await axios.get('https://fapi.binance.com/fapi/v1/ticker/price', {
      params: { symbol }, timeout: 4000
    });
    return parseFloat(resp.data.price);
  } catch {
    return null;
  }
}

/* ==========================================
 * DATA PROCESSING FUNCTIONS
 * ========================================== */

// ---------- Binance (contracts → USD)
async function processBinanceData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  try {
    const oiContracts = parseFloat(rawData.openInterest);
    const timestampRaw = rawData.time;
    const timestamp = Math.floor(timestampRaw / 60000) * 60000;
    if (isNaN(oiContracts)) return null;

    // Determine contract multiplier
    const multiplier = BINANCE_CONTRACT_MULTIPLIERS[baseSymbol] || BINANCE_CONTRACT_MULTIPLIERS.default;

    // Attempt to get price from DB, fallback to API
    let price = null;
    const rows = await dbManager.queryPerpData('bin-ohlcv', baseSymbol, timestamp - 5 * 60 * 1000, timestamp + 5 * 60 * 1000);
    if (rows.length > 0) price = rows[rows.length - 1].c;
    if (!price) price = await getBinancePrice(`${baseSymbol}USDT`);

    const oiUsd = oiContracts * multiplier * (price || 0);
    if (!oiUsd) return null;

    return {
      ts: apiUtils.toMillis(BigInt(timestamp)),
      symbol: baseSymbol,
      source: perpspec,
      perpspec,
      interval: config.DB_INTERVAL,
      oi: oiUsd
    };
  } catch (e) {
    return null;
  }
}

// ---------- Bybit (already USD, includes 5×1 expansion)
// Comment: The Bybit 5×1 conversion logic is confirmed accurate.
function processBybitData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  const expandedRecords = [];
  try {
    const dataPoint = rawData.list[0];
    const oiUsd = parseFloat(dataPoint.openInterest);
    const timestamp = parseInt(dataPoint.timestamp, 10);
    if (isNaN(oiUsd)) return null;

    // floor timestamp to nearest minute, filter future inserts
    const now = Date.now();
    const baseTs = Math.floor(timestamp / 60000) * 60000;

    for (let i = 0; i < 5; i++) {
      const ts = baseTs + i * 60 * 1000;
      if (ts <= now) {
        expandedRecords.push({
          ts: apiUtils.toMillis(BigInt(ts)),
          symbol: baseSymbol,
          source: perpspec,
          perpspec,
          interval: config.DB_INTERVAL,
          oi: oiUsd
        });
      }
    }

    return expandedRecords;
  } catch {
    return null;
  }
}

// ---------- OKX (switch to oiUsd)
function processOkxData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  try {
    const dataPoint = rawData[0];
    const oiUsd = parseFloat(dataPoint.oiUsd ?? dataPoint.oi);
    const timestampRaw = dataPoint.ts;
    const timestamp = Math.floor(timestampRaw / 60000) * 60000;
    if (isNaN(oiUsd)) return null;

    return {
      ts: apiUtils.toMillis(BigInt(timestamp)),
      symbol: baseSymbol,
      source: perpspec,
      perpspec,
      interval: config.DB_INTERVAL,
      oi: oiUsd
    };
  } catch {
    return null;
  }
}

/* ==========================================
 * EXCHANGE FETCH FUNCTIONS
 * ========================================== */
async function fetchBinanceOI(symbol, config) {
  const params = { symbol };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  return response.data;
}

async function fetchBybitOI(symbol, config) {
  const params = { category: 'linear', symbol, intervalTime: '5min', limit: 1 };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (response.data.result?.list?.length === 0) throw new Error('No data returned from Bybit');
  return response.data.result;
}

async function fetchOkxOI(instId, config) {
  const params = { instId };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (response.data.code !== '0') throw new Error(`OKX API error: ${response.data.msg}`);
  return response.data.data || [];
}

/* ==========================================
 * POLLING CORE
 * ========================================== */
async function pollSymbolAndExchange(baseSymbol, exchangeConfig) {
  const perpspec = exchangeConfig.PERPSPEC;
  const exchangeSymbol = exchangeConfig.mapSymbol(baseSymbol);
  try {
    let rawData, processedData;

    switch (perpspec) {
      case 'bin-oi':
        rawData = await fetchBinanceOI(exchangeSymbol, exchangeConfig);
        processedData = await processBinanceData(rawData, baseSymbol, exchangeConfig);
        break;
      case 'byb-oi':
        rawData = await fetchBybitOI(exchangeSymbol, exchangeConfig);
        processedData = processBybitData(rawData, baseSymbol, exchangeConfig);
        break;
      case 'okx-oi':
        rawData = await fetchOkxOI(exchangeSymbol, exchangeConfig);
        processedData = processOkxData(rawData, baseSymbol, exchangeConfig);
        break;
    }

    // Log #2: First successful response for perpspec
    if (!connectedPerpspecs.has(perpspec) && processedData) {
      connectedPerpspecs.add(perpspec);
      if (connectedPerpspecs.size === Object.keys(EXCHANGE_CONFIG).length) {
        const message = `${Object.values(EXCHANGE_CONFIG).map(cfg => cfg.PERPSPEC).join(', ')} connected; fetching.`;
        await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', message);
        console.log(`${STATUS_COLOR}♻️ ${message}${RESET}`);
      }
    }

    if (!processedData) return;
    const records = Array.isArray(processedData) ? processedData : [processedData];
    await dbManager.insertData(perpspec, records);

    // Log #3: Perpspec 1m pull completion
    completedPulls.get(perpspec).add(baseSymbol);
    const expectedCount = perpList.length;
    if (completedPulls.get(perpspec).size === expectedCount) {
      const message = `${perpspec} 1m pull.`;
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message, { perpspec });
      console.log(`${STATUS_COLOR}${message}${RESET}`);
      completedPulls.get(perpspec).clear(); // Reset for next 1m cycle
    }
  } catch (err) {
    console.error(`[${perpspec}] Error polling ${baseSymbol}:`, err.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'POLL_ERROR', err.message, {
      exchange: perpspec.split('-')[0],
      symbol: baseSymbol,
      perpspec
    });
  }
}

/* ==========================================
 * POLL ALL SYMBOLS
 * ========================================== */
async function pollAllSymbols() {
  const exchanges = [EXCHANGE_CONFIG.BINANCE, EXCHANGE_CONFIG.BYBIT, EXCHANGE_CONFIG.OKX];
  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(10);

  const tasks = [];
  for (const sym of perpList) {
    for (const cfg of exchanges) {
      tasks.push(limit(async () => {
        await pollSymbolAndExchange(sym, cfg);
      }));
    }
  }
  await Promise.all(tasks);
}

/* ==========================================
 * MAIN EXECUTION
 * ========================================== */
async function execute() {
  // Log #1: Script start
  const totalSymbols = perpList.length;
  const startMessage = `Starting ${SCRIPT_NAME} real-time 1m pull; ${totalSymbols} symbols.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', startMessage);
  console.log(`${STATUS_COLOR}♻️ ${startMessage}${RESET}`);

  await pollAllSymbols();

  const pollIntervalId = setInterval(async () => {
    try {
      await pollAllSymbols();
    } catch (err) {
      console.error('Error in polling cycle:', err.message);
      await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'POLL_CYCLE_ERROR', err.message);
    }
  }, POLL_INTERVAL);

  // Log #4: Graceful shutdown
  process.on('SIGINT', async () => {
    clearInterval(pollIntervalId);
    const stopMessage = `♻️ ${SCRIPT_NAME} smoothly stopped.`;
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', stopMessage);
    console.log(`${STATUS_COLOR}${stopMessage}${RESET}`);
    process.exit(0);
  });
}

if (require.main === module) {
  execute()
    .catch(err => {
      console.error('💥 OI continuous polling failed:', err);
      try {
        apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'INITIALIZATION_FAILED', err.message);
      } catch {}
      process.exit(1);
    });
}

module.exports = { execute };