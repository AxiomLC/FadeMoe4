/* ==========================================
 * all-oi-c.js   5 OCt 2025
 * Continuous Open Interest Polling Script
 *
 * Fetches open interest data from Binance, Bybit, and OKX
 * Inserts data into the database
 * Logs high-level status messages for UI and monitoring
 * ========================================== */

const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');
require('dotenv').config();

const SCRIPT_NAME = 'all-oi-c.js';
const POLL_INTERVAL = 60 * 1000; // 1 minute

/* ==========================================
 * EXCHANGE CONFIGURATION
 * Defines API URLs and perpspec names for each exchange
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

/* ==========================================
 * DATA PROCESSING FUNCTIONS
 * Parse raw API data into normalized records
 * ========================================== */

/**
 * Process Binance Open Interest snapshot
 * Returns normalized record or null if invalid
 */
function processBinanceData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;

  try {
    const oi = parseFloat(rawData.openInterest);
    const timestamp = rawData.time;

    if (isNaN(oi)) {
      return null;
    }

    return {
      ts: apiUtils.toMillis(BigInt(timestamp)),
      symbol: baseSymbol,
      source: perpspec,
      perpspec,
      interval: config.DB_INTERVAL,
      oi
    };
  } catch (e) {
    return null;
  }
}

/**
 * Process Bybit Open Interest snapshot
 * Expands 5m data to 5 x 1m records
 * Returns array of normalized records or null if invalid
 */
function processBybitData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  const expandedRecords = [];

  try {
    const dataPoint = rawData.list[0];
    const oi = parseFloat(dataPoint.openInterest);
    const timestamp = parseInt(dataPoint.timestamp, 10);

    if (isNaN(oi)) {
      return null;
    }

    for (let i = 0; i < 5; i++) {
      expandedRecords.push({
        ts: apiUtils.toMillis(BigInt(timestamp + i * 60 * 1000)),
        symbol: baseSymbol,
        source: perpspec,
        perpspec: perpspec,
        interval: config.DB_INTERVAL,
        oi
      });
    }

    return expandedRecords;
  } catch (e) {
    return null;
  }
}

/**
 * Process OKX Open Interest snapshot
 * Returns normalized record or null if invalid
 */
function processOkxData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;

  try {
    const dataPoint = rawData[0];
    const oi = parseFloat(dataPoint.oi);
    const timestamp = dataPoint.ts;

    if (isNaN(oi)) {
      return null;
    }

    return {
      ts: apiUtils.toMillis(BigInt(timestamp)),
      symbol: baseSymbol,
      source: perpspec,
      perpspec,
      interval: config.DB_INTERVAL,
      oi
    };
  } catch (e) {
    return null;
  }
}

/* ==========================================
 * EXCHANGE-SPECIFIC FETCH FUNCTIONS
 * Fetch current open interest data from each exchange API
 * ========================================== */

async function fetchBinanceOI(symbol, config) {
  const params = { symbol: symbol };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  return response.data;
}

async function fetchBybitOI(symbol, config) {
  const params = {
    category: 'linear',
    symbol: symbol,
    intervalTime: '5min',
    limit: 1
  };

  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (response.data.result?.list?.length === 0) {
    throw new Error('No data returned from Bybit');
  }
  return response.data.result;
}

async function fetchOkxOI(instId, config) {
  const params = { instId: instId };

  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (response.data.code !== "0") {
    throw new Error(`OKX API error: ${response.data.msg}`);
  }
  return response.data.data || [];
}

/* ==========================================
 * POLLING ORCHESTRATION
 * Poll all symbols for all exchanges concurrently
 * ========================================== */

async function pollSymbolAndExchange(baseSymbol, exchangeConfig) {
  const perpspec = exchangeConfig.PERPSPEC;
  const exchangeName = perpspec.split('-')[0];
  const exchangeSymbol = exchangeConfig.mapSymbol(baseSymbol);

  try {
    let rawData = null;
    let processedData = null;

    switch (perpspec) {
      case EXCHANGE_CONFIG.BINANCE.PERPSPEC:
        rawData = await fetchBinanceOI(exchangeSymbol, exchangeConfig);
        processedData = processBinanceData(rawData, baseSymbol, exchangeConfig);
        break;
      case EXCHANGE_CONFIG.BYBIT.PERPSPEC:
        rawData = await fetchBybitOI(exchangeSymbol, exchangeConfig);
        processedData = processBybitData(rawData, baseSymbol, exchangeConfig);
        break;
      case EXCHANGE_CONFIG.OKX.PERPSPEC:
        rawData = await fetchOkxOI(exchangeSymbol, exchangeConfig);
        processedData = processOkxData(rawData, baseSymbol, exchangeConfig);
        break;
    }

    if (!processedData) {
      return;
    }

    const recordsToInsert = Array.isArray(processedData) ? processedData : [processedData];
    await dbManager.insertData(perpspec, recordsToInsert);

  } catch (error) {
    console.error(`[${perpspec}] Error polling ${baseSymbol}:`, error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'POLL_ERROR', error.message, {
      exchange: exchangeName,
      symbol: baseSymbol,
      perpspec
    });
  }
}

/* ==========================================
 * POLL ALL SYMBOLS
 * Poll all exchanges concurrently for all symbols
 * ========================================== */

async function pollAllSymbols() {
  console.log(`\n[${new Date().toISOString().slice(11, 19)}] Polling ${perpList.length} symbols...`);

  const exchangesToFetch = [
    EXCHANGE_CONFIG.BINANCE,
    EXCHANGE_CONFIG.BYBIT,
    EXCHANGE_CONFIG.OKX
  ];

  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(10); // Higher concurrency for real-time snapshots
  const promises = [];

  // Track successful polls per perpspec
  const successCounts = {
    'bin-oi': 0,
    'byb-oi': 0,
    'okx-oi': 0
  };

  for (const baseSymbol of perpList) {
    for (const config of exchangesToFetch) {
      promises.push(limit(async () => {
        try {
          await pollSymbolAndExchange(baseSymbol, config);
          successCounts[config.PERPSPEC]++;
        } catch (error) {
          // Error already logged in pollSymbolAndExchange
        }
      }));
    }
  }

  await Promise.all(promises);

  // Log summary per perpspec
  for (const config of exchangesToFetch) {
    console.log(`[${config.PERPSPEC}] Polling ${successCounts[config.PERPSPEC]} symbols.`);
  }
}

/* ==========================================
 * MAIN EXECUTION
 * Start polling and log status
 * ========================================== */

async function execute() {
  console.log(`🚀 Starting ${SCRIPT_NAME} - Continuous Open Interest polling`);
  console.log(`⏰ Poll interval: ${POLL_INTERVAL / 1000} seconds`);

  // #1 Log script start ONCE - This makes it appear in "Current Operations"
  // The status 'started' or 'running' keeps it visible in the UI
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} connected`);

  // Initial poll
  await pollAllSymbols();

  // #2 Log running status after initial poll
  // CRITICAL: Status must be 'running' to appear in "Current Operations"
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `${SCRIPT_NAME} initial poll complete`);
  console.log(`${SCRIPT_NAME} initial poll complete`);

  // Set up recurring polling
  const pollIntervalId = setInterval(async () => {
    try {
      await pollAllSymbols();
      // #3 Log running status after each poll cycle
      // This keeps the script visible in "Current Operations"
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `${SCRIPT_NAME} 1min pull done`);
    } catch (error) {
      console.error('Error in polling cycle:', error.message);
      await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'POLL_CYCLE_ERROR', error.message);
    }
  }, POLL_INTERVAL);

  // Graceful shutdown handler
  process.on('SIGINT', async () => {
    clearInterval(pollIntervalId);
    console.log(`\n${SCRIPT_NAME} received SIGINT, stopping...`);

    // #4 Log script stop ONCE - Removes from "Current Operations"
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'stopped', `${SCRIPT_NAME} stopped smoothly`);

    process.exit(0);
  });
}

/* ==========================================
 * MODULE ENTRY POINT
 * Execute script if run directly
 * ========================================== */

if (require.main === module) {
  execute()
    .then(() => {
      console.log('✅ OI continuous polling started');
    })
    .catch(err => {
      console.error('💥 OI continuous polling failed:', err);
      try {
        apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'INITIALIZATION_FAILED', err.message);
      } catch (logError) {
        console.error('Failed to log initial execution error:', logError.message);
      }
      process.exit(1);
    });
}

module.exports = { execute };