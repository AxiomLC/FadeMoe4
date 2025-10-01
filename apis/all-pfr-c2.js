/* ==========================================
 * all-pfr-c2.js
 *
 * Continuous Premium Funding Rate Polling Script
 *
 * Fetches premium funding rates from Binance, Bybit, and OKX
 * Inserts data into the database
 * Logs high-level status messages for UI and monitoring
 *
 * ========================================== */

const fs = require('fs');
const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');

const SCRIPT_NAME = 'all-pfr-c2.js';
const POLL_INTERVAL = 60 * 1000; // 1 minute

/* ==========================================
 * EXCHANGE CONFIGURATION
 *
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

/* ==========================================
 * SYMBOL TRANSLATION FUNCTIONS
 *
 * Translate static list symbols to exchange-specific symbols
 * ========================================== */

const perpList = require('../perp-list');

/**
 * Translate symbol for Binance and Bybit (e.g., 'SOL' -> 'SOLUSDT')
 * Binance and Bybit use concatenated symbols without dashes
 */
function translateSymbolBinanceBybit(symbol) {
  return symbol + 'USDT';
}

/**
 * Translate symbol for OKX (e.g., 'SOL' -> 'SOL-USDT-SWAP')
 * OKX uses dash-separated symbols with '-SWAP' suffix
 */
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
    limit: 1
  };

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
      return; // No valid processed data
    }

    await dbManager.insertData(perpspec, processedData);
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

  for (const baseSymbol of perpList) {
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
  console.log(`🚀 Starting ${SCRIPT_NAME} - Continuous Premium Funding Rate polling`);

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
  }, 20000);

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
    console.log(`
${SCRIPT_NAME} received SIGINT, stopping...`);
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'stopped', 'script stopped smoothly');
    process.exit(0);
  });
}

if (require.main === module) {
  execute()
    .then(() => {
      console.log('✅ PFR continuous polling started');
    })
    .catch(err => {
      console.error('💥 PFR continuous polling failed:', err);
      process.exit(1);
    });
}

module.exports = { execute };

