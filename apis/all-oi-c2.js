/* ==========================================
 * all-oi-c.js
 *
 * Continuous Open Interest Polling Script
 *
 * Fetches open interest data from Binance, Bybit, and OKX
 * Inserts data into the database
 * Logs high-level status messages for UI and monitoring
 * ========================================== */

const fs = require('fs');
const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');

const SCRIPT_NAME = 'all-oi-c.js';
const POLL_INTERVAL = 60 * 1000; // 1 minute

/* ==========================================
 * EXCHANGE CONFIGURATION
 *
 * Defines API URLs, perpspec, and source names for each exchange
 * ========================================== */
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-oi',
    SOURCE: 'bin-oi',
    URL: 'https://fapi.binance.com/fapi/v1/openInterest',
    DB_INTERVAL: '1m'
  },
  BYBIT: {
    PERPSPEC: 'byb-oi',
    SOURCE: 'byb-oi',
    URL: 'https://api.bybit.com/v5/market/open-interest',
    DB_INTERVAL: '1m'
  },
  OKX: {
    PERPSPEC: 'okx-oi',
    SOURCE: 'okx-oi',
    URL: 'https://www.okx.com/api/v5/public/open-interest',
    DB_INTERVAL: '1m'
  }
};

/* ==========================================
 * DATA PROCESSING FUNCTIONS
 *
 * Parse raw API data into normalized records
 * ========================================== */

function processBinanceData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  const source = config.SOURCE;

  try {
    const oi = parseFloat(rawData.openInterest);
    const timestamp = rawData.time;

    if (isNaN(oi)) {
      return null; // Skip invalid data
    }

    return {
      ts: apiUtils.toMillis(BigInt(timestamp)),
      symbol: baseSymbol,
      source: source,
      perpspec: perpspec,
      interval: config.DB_INTERVAL,
      oi
    };
  } catch (e) {
    return null; // Skip on error
  }
}

function processBybitData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  const source = config.SOURCE;

  try {
    const dataPoint = rawData.list[0];
    const oi = parseFloat(dataPoint.openInterest);
    const timestamp = dataPoint.timestamp;

    if (isNaN(oi)) {
      return null; // Skip invalid data
    }

    return {
      ts: apiUtils.toMillis(BigInt(timestamp)),
      symbol: baseSymbol,
      source: source,
      perpspec: perpspec,
      interval: config.DB_INTERVAL,
      oi
    };
  } catch (e) {
    return null; // Skip on error
  }
}

function processOkxData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  const source = config.SOURCE;

  try {
    const dataPoint = rawData[0];
    const oi = parseFloat(dataPoint.oi);
    const timestamp = dataPoint.ts;

    if (isNaN(oi)) {
      return null; // Skip invalid data
    }

    return {
      ts: apiUtils.toMillis(BigInt(timestamp)),
      symbol: baseSymbol,
      source: source,
      perpspec: perpspec,
      interval: config.DB_INTERVAL,
      oi
    };
  } catch (e) {
    return null; // Skip on error
  }
}

/* ==========================================
 * EXCHANGE-SPECIFIC FETCH FUNCTIONS
 *
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
 *
 * Poll all symbols for all exchanges concurrently
 * ========================================== */

async function pollSymbolAndExchange(baseSymbol, exchangeConfig) {
  const perpspec = exchangeConfig.PERPSPEC;
  const source = exchangeConfig.SOURCE;

  const symbolMap = {}; // Define your symbol mapping logic here

  let exchangeSymbol = symbolMap[baseSymbol]; // Use the mapping logic

  if (!exchangeSymbol) {
    return; // Skip if no symbol mapping
  }

  try {
    let rawData = null;

    // Fetch current snapshot
    switch (perpspec) {
      case EXCHANGE_CONFIG.BINANCE.PERPSPEC:
        rawData = await fetchBinanceOI(exchangeSymbol, exchangeConfig);
        break;
      case EXCHANGE_CONFIG.BYBIT.PERPSPEC:
        rawData = await fetchBybitOI(exchangeSymbol, exchangeConfig);
        break;
      case EXCHANGE_CONFIG.OKX.PERPSPEC:
        rawData = await fetchOkxOI(exchangeSymbol, exchangeConfig);
        break;
    }

    if (!rawData) {
      return; // No data to process
    }

    let processedData = null;
    switch (perpspec) {
      case EXCHANGE_CONFIG.BINANCE.PERPSPEC:
        processedData = processBinanceData(rawData, baseSymbol, exchangeConfig);
        break;
      case EXCHANGE_CONFIG.BYBIT.PERPSPEC:
        processedData = processBybitData(rawData, baseSymbol, exchangeConfig);
        break;
      case EXCHANGE_CONFIG.OKX.PERPSPEC:
        processedData = processOkxData(rawData, baseSymbol, exchangeConfig);
        break;
    }

    if (!processedData) {
      return; // No valid processed data
    }

    await dbManager.insertData(perpspec, [processedData]);
  } catch (error) {
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'POLL_ERROR', error.message, {
      exchange: perpspec.split('-')[0],
      symbol: baseSymbol,
      perpspec
    });
  }
}

/* ==========================================
 * POLL ALL SYMBOLS
 *
 * Load static symbol list and poll all exchanges concurrently
 * ========================================== */

async function pollAllSymbols() {
  const perpspecs = Object.values(EXCHANGE_CONFIG).map(c => c.PERPSPEC);

  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(10);
  const promises = [];

  for (const baseSymbol of ['SOL', 'PENGU', 'ETH']) { // Use static list directly
    for (const perpspec of perpspecs) {
      const exchangeConfig = Object.values(EXCHANGE_CONFIG).find(c => c.PERPSPEC === perpspec);
      promises.push(limit(() => pollSymbolAndExchange(baseSymbol, exchangeConfig)));
    }
  }

  await Promise.all(promises);
}

/* ==========================================
 * MAIN EXECUTION
 *
 * Start polling and log high-level statuses
 * ========================================== */

async function execute() {
  console.log(`🚀 Starting ${SCRIPT_NAME} - Continuous Open Interest polling`);

  // Log script started status ONCE
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', 'script started');

  const perpspecs = Object.values(EXCHANGE_CONFIG).map(c => c.PERPSPEC);

  // Heartbeat status for each perpspec staggered every ~20 seconds
  let index = 0;
  const heartbeatInterval = setInterval(async () => {
    if (index >= perpspecs.length) index = 0;
    const perpspec = perpspecs[index];
    try {
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `${perpspec} 1min pull`);
      console.log(`${perpspec} 1min pull`);
    } catch (error) {
      console.error('Heartbeat logging failed:', error.message);
    }
    index++;
  }, 60000); // Change to 1 minute

  await pollAllSymbols();

  // Polling loop
  const pollIntervalId = setInterval(async () => {
    try {
      await pollAllSymbols();
    } catch (error) {
      console.error('Error in polling cycle:', error.message);
      await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'POLL_CYCLE_ERROR', error.message);
    }
  }, POLL_INTERVAL);

  // Graceful shutdown handler
  process.on('SIGINT', async () => {
    clearInterval(heartbeatInterval);
    clearInterval(pollIntervalId);
    console.log(`\n${SCRIPT_NAME} received SIGINT, stopping...`);
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'stopped', 'script stopped smoothly');
    process.exit(0);
  });
}

if (require.main === module) {
  execute()
    .then(() => {
      console.log('✅ OI continuous polling started');
    })
    .catch(err => {
      console.error('💥 OI continuous polling failed:', err);
      process.exit(1);
    });
}

module.exports = { execute };