const fs = require('fs');
const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
require('dotenv').config();

const SCRIPT_NAME = 'all-oi-c.js';
const POLL_INTERVAL = 60 * 1000; // 1 minute

// ============================================================================
// EXCHANGE CONFIGURATION
// ============================================================================
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-oi',
    URL: 'https://fapi.binance.com/fapi/v1/openInterest',
    DB_INTERVAL: '1m'
  },
  BYBIT: {
    PERPSPEC: 'byb-oi',
    URL: 'https://api.bybit.com/v5/market/open-interest',
    DB_INTERVAL: '1m'
  },
  OKX: {
    PERPSPEC: 'okx-oi',
    URL: 'https://www.okx.com/api/v5/public/open-interest',
    DB_INTERVAL: '1m'
  }
};

// ============================================================================
// DATA PROCESSING FUNCTIONS
// ============================================================================

/**
 * Process Binance Open Interest snapshot
 * Returns: { openInterest, symbol, time }
 */
function processBinanceData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;

  try {
    const oi = parseFloat(rawData.openInterest);
    const timestamp = rawData.time;

    if (isNaN(oi)) {
      console.warn(`[${perpspec}] Invalid OI for ${baseSymbol}:`, rawData.openInterest);
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
    console.warn(`[${perpspec}] Error processing ${baseSymbol}:`, e.message);
    return null;
  }
}

/**
 * Process Bybit Open Interest snapshot
 * Returns: result.list[0] = { openInterest, timestamp }
 */
function processBybitData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;

  try {
    const dataPoint = rawData.list[0];
    const oi = parseFloat(dataPoint.openInterest);
    const timestamp = dataPoint.timestamp;

    if (isNaN(oi)) {
      console.warn(`[${perpspec}] Invalid OI for ${baseSymbol}:`, dataPoint.openInterest);
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
    console.warn(`[${perpspec}] Error processing ${baseSymbol}:`, e.message);
    return null;
  }
}

/**
 * Process OKX Open Interest snapshot
 * Returns: data[0] = { oi, oiCcy, ts }
 */
function processOkxData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;

  try {
    const dataPoint = rawData[0];
    const oi = parseFloat(dataPoint.oi);
    const timestamp = dataPoint.ts;

    if (isNaN(oi)) {
      console.warn(`[${perpspec}] Invalid OI for ${baseSymbol}:`, dataPoint.oi);
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
    console.warn(`[${perpspec}] Error processing ${baseSymbol}:`, e.message);
    return null;
  }
}

// ============================================================================
// EXCHANGE-SPECIFIC FETCH FUNCTIONS
// ============================================================================

/**
 * Fetches current Open Interest from Binance
 */
async function fetchBinanceOI(symbol, config) {
  const params = { symbol: symbol };
  const response = await axios.get(config.URL, { params, timeout: 5000 });
  return response.data;
}

/**
 * Fetches current Open Interest from Bybit
 */
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

/**
 * Fetches current Open Interest from OKX
 */
async function fetchOkxOI(instId, config) {
  const params = { instId: instId };

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
    let rawData = null;
    let processedData = null;

    // Fetch current snapshot
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

    // Insert into database
    const expectedFields = Object.keys(processedData);
    await apiUtils.ensureColumnsExist(dbManager, expectedFields);
    await apiUtils.updatePerpspecSchema(dbManager, perpspec, expectedFields);
    await dbManager.insertData(perpspec, [processedData]);

    console.log(`[${perpspec}] ✅ ${baseSymbol}: OI=${processedData.oi}`);

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
  const limit = pLimit(10); // Higher concurrency for real-time snapshots
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
  console.log(`🚀 Starting ${SCRIPT_NAME} - Continuous Open Interest polling`);
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
      console.log('✅ OI continuous polling started');
    })
    .catch(err => {
      console.error('💥 OI continuous polling failed:', err);
      process.exit(1);
    });
}

module.exports = { execute };