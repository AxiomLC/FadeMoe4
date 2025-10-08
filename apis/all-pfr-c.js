/* ==========================================
 * all-pfr-c.js  5 OCt 2025
 * Continuous Premium Funding Rate Polling Script
 *"Premium Index Kline" Bin/Byb, "Premium Index" Okx
 * ========================================== */

const fs = require('fs');
const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');

const SCRIPT_NAME = 'all-pfr-c2.js';
const POLL_INTERVAL = 60 * 1000; // 1 minute

/* ==========================================
 * EXCHANGE CONFIGURATION
 * Defines API URLs, perpspec, and source names for each exchange
 * ========================================== */
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-pfr',
    SOURCE: 'bin-pfr',
    URL: 'https://fapi.binance.com/fapi/v1/premiumIndexKlines',
    API_INTERVAL: '1m',
    DB_INTERVAL: '1m'
  },
  BYBIT: {
    PERPSPEC: 'byb-pfr',
    SOURCE: 'byb-pfr',
    URL: 'https://api.bybit.com/v5/market/premium-index-price-kline',
    API_INTERVAL: '1',
    DB_INTERVAL: '1m'
  },
  OKX: {
    PERPSPEC: 'okx-pfr',
    SOURCE: 'okx-pfr',
    URL: 'https://www.okx.com/api/v5/public/premium-history',
    DB_INTERVAL: '1m'
  }
};

//* ==========================================
 //* SYMBOL TRANSLATION FUNCTIONS

const perpList = require('../perp-list');

//Translate symbol for Binance and Bybit (e.g., 'SOL' -> 'SOLUSDT')
function translateSymbolBinanceBybit(symbol) {
  return symbol + 'USDT';
}

 //* Translate symbol for OKX (e.g., 'SOL' -> 'SOL-USDT-SWAP')
function translateSymbolOkx(symbol) {
  return `${symbol}-USDT-SWAP`;
}
/* ==========================================
 * DATA PROCESSING FUNCTIONS
 *
 * Parse raw API data into normalized records
 * ========================================== */

function processBinanceData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  const source = config.SOURCE;

  return rawData.map(dataPoint => {
    try {
      const timestamp = dataPoint[0];
      const pfr = parseFloat(dataPoint[4]);

      if (isNaN(pfr)) {
        return null; // Skip invalid data
      }

      return {
        ts: apiUtils.toMillis(BigInt(timestamp)),
      symbol: baseSymbol,
        source: source,
        perpspec: perpspec,
        interval: config.DB_INTERVAL,
        pfr
      };
    } catch (e) {
      return null; // Skip on error
  }
  }).filter(item => item !== null);
}

function processBybitData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  const source = config.SOURCE;

  return rawData.map(dataPoint => {
  try {
      const timestamp = dataPoint[0];
      const pfr = parseFloat(dataPoint[4]);

      if (isNaN(pfr)) {
        return null;
  }

      return {
        ts: apiUtils.toMillis(BigInt(timestamp)),
        symbol: baseSymbol,
        source: source,
        perpspec: perpspec,
        interval: config.DB_INTERVAL,
        pfr
      };
    } catch (e) {
      return null;
    }
  }).filter(item => item !== null);
}

function processOkxData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;
  const source = config.SOURCE;

  return rawData.map(dataPoint => {
    try {
      const timestamp = dataPoint.ts;
      const pfr = parseFloat(dataPoint.premium);

      if (isNaN(pfr)) {
        return null;
    }

      return {
        ts: apiUtils.toMillis(BigInt(timestamp)),
        symbol: baseSymbol,
        source: source,
        perpspec: perpspec,
        interval: config.DB_INTERVAL,
        pfr
      };
    } catch (e) {
      return null;
    }
  }).filter(item => item !== null);
}

/* ==========================================
 * EXCHANGE-SPECIFIC FETCH FUNCTIONS
 *
 * Fetch latest premium funding rate data from each exchange API
 * ========================================== */

async function fetchBinancePFR(symbol, config) {
  const params = {
    symbol: symbol,
    interval: config.API_INTERVAL,
    limit: 1
  };

  const response = await axios.get(config.URL, { params, timeout: 5000 });
  return response.data;
}

async function fetchBybitPFR(symbol, config) {
  const params = {
    category: 'linear',
    symbol: symbol,
    interval: config.API_INTERVAL,
    limit: 1
  };

  const response = await axios.get(config.URL, { params, timeout: 5000 });
  return response.data.result?.list || [];
}

async function fetchOkxPFR(instId, config) {
  const params = {
    instId: instId,
    limit: 1 // Fetch only the latest record
  };

  const response = await axios.get(config.URL, { params, timeout: 5000 });
  if (response.data.code !== "0") {
    throw new Error(`OKX API error: ${response.data.msg}`);
  }
  // The response.data.data is an array of objects, e.g., [{"instId": "BTC-USDT-SWAP", "premium": "...", "ts": "..."}]
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

  let exchangeSymbol;
  switch (perpspec) {
    case 'bin-pfr':
      exchangeSymbol = translateSymbolBinanceBybit(baseSymbol);
      break;
    case 'byb-pfr':
      exchangeSymbol = translateSymbolBinanceBybit(baseSymbol);
      break;
    case 'okx-pfr':
      exchangeSymbol = translateSymbolOkx(baseSymbol);
      break;
    default:
      return; // Unknown perpspec
  }
    try {
    let rawData = [];

    switch (perpspec) {
      case 'bin-pfr':
        rawData = await fetchBinancePFR(exchangeSymbol, exchangeConfig);
        break;
      case 'byb-pfr':
        rawData = await fetchBybitPFR(exchangeSymbol, exchangeConfig);
        break;
      case 'okx-pfr':
        rawData = await fetchOkxPFR(exchangeSymbol, exchangeConfig);
        break;
    }

    if (!rawData || rawData.length === 0) {
      // console.log(`  - No data received for ${perpspec} - ${baseSymbol}`); // Optional: log when no data is returned
      return; // No data to process
}

    let processedData = [];
    switch (perpspec) {
      case 'bin-pfr':
        processedData = processBinanceData(rawData, baseSymbol, exchangeConfig);
        break;
      case 'byb-pfr':
        processedData = processBybitData(rawData, baseSymbol, exchangeConfig);
        break;
      case 'okx-pfr':
        processedData = processOkxData(rawData, baseSymbol, exchangeConfig);
        break;
    }

    if (processedData.length === 0) {
      // console.log(`  - No valid processed data for ${perpspec} - ${baseSymbol}`); // Optional: log when no valid data after processing
      return; // No valid processed data
    }

    await dbManager.insertData(perpspec, processedData);
  } catch (error) {
    // Log API errors with context
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'POLL_ERROR', error.message, {
      exchange: perpspec.split('-')[0],
      symbol: baseSymbol,
      perpspec
    });
  }
}

/* ==========================================
 * POLL ALL SYMBOLS
 * Load static symbol list and poll all exchanges concurrently
 * ========================================== */

async function pollAllSymbols() {
  const perpspecs = Object.values(EXCHANGE_CONFIG).map(c => c.PERPSPEC);

  // Using p-limit to control concurrency
  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(10); // Limit concurrent requests to 10
  const promises = [];

  for (const baseSymbol of perpList) {
    for (const perpspec of perpspecs) {
      const exchangeConfig = Object.values(EXCHANGE_CONFIG).find(c => c.PERPSPEC === perpspec);
      if (exchangeConfig) {
      promises.push(limit(() => pollSymbolAndExchange(baseSymbol, exchangeConfig)));
    }
  }
}

  await Promise.all(promises);
}

/* ==========================================
 * MAIN EXECUTION
 * Start polling and log status
 * ========================================== */

async function execute() {
  // Simplified start message for console
  console.log(`🚀 Starting ${SCRIPT_NAME} - Continuous Predicted FR polling`);
  // #1 Log script start ONCE
  // Status: "started", Message: "{scriptName} connected"
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} connected`);

  const perpspecs = Object.values(EXCHANGE_CONFIG).map(c => c.PERPSPEC);

  // Perform an initial poll to get data and log completion statuses
  await pollAllSymbols();

  // Log completion status for each perpspec after the initial poll cycle
  // Status: "running", Message: "{perpspec} 1min pull complete"
  for (const perpspec of perpspecs) {
    const message = `${perpspec} 1min pfr pull`;
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message);
    console.log(message); // Console output matches DB message
  }

  // Polling loop
  const pollIntervalId = setInterval(async () => {
    try {
      await pollAllSymbols();
      // Log completion status for each perpspec after each polling cycle
      for (const perpspec of perpspecs) {
        const message = `${perpspec} 1min pfr pull complete`;
        await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message);
        console.log(message); // Console output matches DB message
    }
    } catch (error) {
      // Log system errors during the polling cycle
      console.error('Error in polling cycle:', error.message);
      await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'POLL_CYCLE_ERROR', error.message);
    }
  }, POLL_INTERVAL);

  // Graceful shutdown handler
  process.on('SIGINT', async () => {
    clearInterval(pollIntervalId); // Clear the polling interval first
    console.log(`\n${SCRIPT_NAME} received SIGINT, stopping...`);

    // #3 Log script stop ONCE
    // Status: "stopped", Message: "{scriptName} stopped smoothly"
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'stopped', `${SCRIPT_NAME} stopped smoothly`);

    process.exit(0);
  });
}

if (require.main === module) {
  execute()
    .then(() => {
    })
    .catch(err => {
      console.error('💥 PFR continuous polling failed:', err);

      // Consider a fallback console log if dbManager is not ready.
      try {
        apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'INITIALIZATION_FAILED', err.message);
      } catch (logError) {
        console.error('Failed to log initial execution error:', logError.message);
}
      process.exit(1);
    });
}

module.exports = { execute };

