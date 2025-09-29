const fs = require('fs');
const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
require('dotenv').config();

const SCRIPT_NAME = 'all-pfr-c.js';
const POLL_INTERVAL = 60 * 1000; // 1 minute

// ============================================================================
// EXCHANGE CONFIGURATION
// ============================================================================
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-pfr',
    URL: 'https://fapi.binance.com/fapi/v1/premiumIndexKlines',
    API_INTERVAL: '1m',
    DB_INTERVAL: '1m'
  },
  BYBIT: {
    PERPSPEC: 'byb-pfr',
    URL: 'https://api.bybit.com/v5/market/premium-index-price-kline',
    API_INTERVAL: '1',
    DB_INTERVAL: '1m'
  },
  OKX: {
    PERPSPEC: 'okx-pfr',
    URL: 'https://www.okx.com/api/v5/public/premium-history',
    DB_INTERVAL: '1m'
  }
};

// ============================================================================
// DATA PROCESSING FUNCTIONS
// ============================================================================

/**
 * Process Binance Premium Index Klines data
 * Returns: [timestamp, open, high, low, close, ...]
 */
function processBinanceData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;

  return rawData.map(dataPoint => {
    try {
      const timestamp = dataPoint[0];
      const pfr = parseFloat(dataPoint[4]); // Close

      if (isNaN(pfr)) {
        console.warn(`[${perpspec}] Invalid PFR for ${baseSymbol}:`, dataPoint[4]);
        return null;
      }

      return {
        ts: apiUtils.toMillis(BigInt(timestamp)),
        symbol: baseSymbol,
        source: perpspec,
        perpspec,
        interval: config.DB_INTERVAL,
        pfr
      };
    } catch (e) {
      console.warn(`[${perpspec}] Error processing ${baseSymbol}:`, e.message);
      return null;
    }
  }).filter(item => item !== null);
}

/**
 * Process Bybit Premium Index data
 */
function processBybitData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;

  return rawData.map(dataPoint => {
    try {
      const timestamp = dataPoint[0];
      const pfr = parseFloat(dataPoint[4]); // Close

      if (isNaN(pfr)) {
        console.warn(`[${perpspec}] Invalid PFR for ${baseSymbol}:`, dataPoint[4]);
        return null;
      }

      return {
        ts: apiUtils.toMillis(BigInt(timestamp)),
        symbol: baseSymbol,
        source: perpspec,
        perpspec,
        interval: config.DB_INTERVAL,
        pfr
      };
    } catch (e) {
      console.warn(`[${perpspec}] Error processing ${baseSymbol}:`, e.message);
      return null;
    }
  }).filter(item => item !== null);
}

/**
 * Process OKX Premium History data
 */
function processOkxData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;

  return rawData.map(dataPoint => {
    try {
      const timestamp = dataPoint.ts;
      const pfr = parseFloat(dataPoint.premium);

      if (isNaN(pfr)) {
        console.warn(`[${perpspec}] Invalid PFR for ${baseSymbol}:`, dataPoint.premium);
        return null;
      }

      return {
        ts: apiUtils.toMillis(BigInt(timestamp)),
        symbol: baseSymbol,
        source: perpspec,
        perpspec,
        interval: config.DB_INTERVAL,
        pfr
      };
    } catch (e) {
      console.warn(`[${perpspec}] Error processing ${baseSymbol}:`, e.message);
      return null;
    }
  }).filter(item => item !== null);
}

// ============================================================================
// EXCHANGE-SPECIFIC FETCH FUNCTIONS
// ============================================================================

/**
 * Fetches latest Premium Index from Binance
 */
async function fetchBinancePFR(symbol, config) {
  const params = {
    symbol: symbol,
    interval: config.API_INTERVAL,
    limit: 1
  };

  const response = await axios.get(config.URL, { params, timeout: 5000 });
  return response.data;
}

/**
 * Fetches latest Premium Index from Bybit
 */
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

/**
 * Fetches latest Premium from OKX
 */
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

// ============================================================================
// POLLING ORCHESTRATION
// ============================================================================

async function pollSymbolAndExchange(baseSymbol, exchangeConfig, dynamicSymbols) {
  const perpspec = exchangeConfig.PERPSPEC;
  const exchangeName = perpspec.split('-')[0];

  const symbolMap = dynamicSymbols[baseSymbol];
  let exchangeSymbol = symbolMap?.[perpspec];

  if (exchangeName === 'okx' && !exchangeSymbol) {
    exchangeSymbol = symbolMap['okx-swap'] || `${baseSymbol}-USDT-SWAP`;
  }
  if (!exchangeSymbol) {
    return;
  }

  try {
    let rawData = [];

    // Fetch latest data
    switch (perpspec) {
      case EXCHANGE_CONFIG.BINANCE.PERPSPEC:
        rawData = await fetchBinancePFR(exchangeSymbol, exchangeConfig);
        break;
      case EXCHANGE_CONFIG.BYBIT.PERPSPEC:
        rawData = await fetchBybitPFR(exchangeSymbol, exchangeConfig);
        break;
      case EXCHANGE_CONFIG.OKX.PERPSPEC:
        rawData = await fetchOkxPFR(exchangeSymbol, exchangeConfig);
        break;
    }

    if (!rawData || rawData.length === 0) {
      return;
    }

    // Process data
    let processedData = [];
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

    if (processedData.length === 0) {
      return;
    }

    // Insert into database
    const expectedFields = Object.keys(processedData[0]);
    await apiUtils.ensureColumnsExist(dbManager, expectedFields);
    await apiUtils.updatePerpspecSchema(dbManager, perpspec, expectedFields);
    await dbManager.insertData(perpspec, processedData);

    console.log(`[${perpspec}] ✅ ${baseSymbol}: PFR=${processedData[0].pfr}`);

  } catch (error) {
    console.error(`[${perpspec}] Error polling ${baseSymbol}:`, error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'POLL_ERROR', error.message, {
      exchange: exchangeName,
      symbol: baseSymbol,
      perpspec
    });
  }
}

// ============================================================================
// MAIN POLLING LOOP
// ============================================================================

async function pollAllSymbols() {
  let dynamicSymbols;
  try {
    dynamicSymbols = JSON.parse(fs.readFileSync('dynamic-symbols.json', 'utf8'));
  } catch (error) {
    console.error('Could not read dynamic-symbols.json');
    return;
  }

  console.log(`\n[${new Date().toISOString().slice(11, 19)}] Polling ${Object.keys(dynamicSymbols).length} symbols...`);

  const exchangesToFetch = [
    EXCHANGE_CONFIG.BINANCE,
    EXCHANGE_CONFIG.BYBIT,
    EXCHANGE_CONFIG.OKX
  ];

  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(10); // Higher concurrency for real-time
  const promises = [];

  for (const baseSymbol of Object.keys(dynamicSymbols)) {
    for (const config of exchangesToFetch) {
      promises.push(limit(() => pollSymbolAndExchange(baseSymbol, config, dynamicSymbols)));
    }
  }

  await Promise.all(promises);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function execute() {
  console.log(`🚀 Starting ${SCRIPT_NAME} - Continuous Premium Funding Rate polling`);
  console.log(`⏰ Poll interval: ${POLL_INTERVAL / 1000} seconds`);

  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', 'Continuous polling started');

  // Initial poll
  await pollAllSymbols();

  // Set up recurring polling
  setInterval(async () => {
    try {
      await pollAllSymbols();
    } catch (error) {
      console.error('Error in polling cycle:', error.message);
      await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'POLL_CYCLE_ERROR', error.message);
    }
  }, POLL_INTERVAL);
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================

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