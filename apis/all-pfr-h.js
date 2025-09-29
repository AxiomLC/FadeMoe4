const fs = require('fs');
const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
require('dotenv').config();

const SCRIPT_NAME = 'all-pfr-h.js';
const DAYS_TO_FETCH = 10;
const TOTAL_CANDLES_TARGET = DAYS_TO_FETCH * 1440; // 14400 1-minute records

// ============================================================================
// EXCHANGE CONFIGURATION
// ============================================================================
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-pfr',
    LIMIT: 500,
    RATE_DELAY: 200,
    URL: 'https://fapi.binance.com/fapi/v1/premiumIndexKlines',
    API_INTERVAL: '1m',
    DB_INTERVAL: '1m',
    API_CANDLES_TARGET: TOTAL_CANDLES_TARGET
  },
  BYBIT: {
    PERPSPEC: 'byb-pfr',
    LIMIT: 200,
    RATE_DELAY: 200,
    URL: 'https://api.bybit.com/v5/market/premium-index-price-kline',
    API_INTERVAL: '1',
    DB_INTERVAL: '1m',
    API_CANDLES_TARGET: TOTAL_CANDLES_TARGET
  },
  OKX: {
    PERPSPEC: 'okx-pfr',
    LIMIT: 100,
    RATE_DELAY: 100,
    URL: 'https://www.okx.com/api/v5/public/premium-history',
    API_INTERVAL: '1m',
    DB_INTERVAL: '1m',
    API_CANDLES_TARGET: TOTAL_CANDLES_TARGET
  }
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================================
// DATA PROCESSING FUNCTIONS
// ============================================================================

/**
 * Process Binance Premium Index Klines data
 * Returns: [timestamp, open, high, low, close, ...]
 * We use 'close' as the premium funding rate value
 */
function processBinanceData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;

  return rawData.map(dataPoint => {
    try {
      const timestamp = dataPoint[0]; // Open time
      const pfr = parseFloat(dataPoint[4]); // Close price

      if (isNaN(pfr)) {
        console.warn(`[${perpspec}] Skipping invalid PFR value for ${baseSymbol}:`, dataPoint[4]);
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
      console.warn(`[${perpspec}] Skipping invalid data point for ${baseSymbol}:`, dataPoint, e.message);
      return null;
    }
  }).filter(item => item !== null);
}

/**
 * Process Bybit Premium Index data
 * Returns: result.list[i] = [timestamp, open, high, low, close]
 * We use list[4] (close) as the premium funding rate value
 */
function processBybitData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;

  return rawData.map(dataPoint => {
    try {
      const timestamp = dataPoint[0]; // Timestamp string
      const pfr = parseFloat(dataPoint[4]); // Close price

      if (isNaN(pfr)) {
        console.warn(`[${perpspec}] Skipping invalid PFR value for ${baseSymbol}:`, dataPoint[4]);
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
      console.warn(`[${perpspec}] Skipping invalid data point for ${baseSymbol}:`, dataPoint, e.message);
      return null;
    }
  }).filter(item => item !== null);
}

/**
 * Process OKX Premium History data
 * Returns: data[i] = { premium, ts }
 * We use 'premium' as the premium funding rate value
 */
function processOkxData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;

  return rawData.map(dataPoint => {
    try {
      const timestamp = dataPoint.ts; // Timestamp string in milliseconds
      const pfr = parseFloat(dataPoint.premium);

      if (isNaN(pfr)) {
        console.warn(`[${perpspec}] Skipping invalid PFR value for ${baseSymbol}:`, dataPoint.premium);
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
      console.warn(`[${perpspec}] Skipping invalid data point for ${baseSymbol}:`, dataPoint, e.message);
      return null;
    }
  }).filter(item => item !== null);
}

// ============================================================================
// EXCHANGE-SPECIFIC FETCH FUNCTIONS
// ============================================================================

/**
 * Fetches Premium Index Klines from Binance
 * Pages backward using 'endTime' parameter
 */
async function fetchBinancePFR(symbol, config) {
  const perpspec = config.PERPSPEC;
  const limit = config.LIMIT;
  const apiTarget = config.API_CANDLES_TARGET;
  const totalRequests = Math.ceil(apiTarget / limit);

  console.log(`[${perpspec}] Backfill: Target ${apiTarget} 1m records, ~${totalRequests} requests for ${symbol}`);

  let allData = [];
  let endTime = Date.now();

  for (let i = 0; i < totalRequests; i++) {
    const params = {
      symbol: symbol,
      interval: config.API_INTERVAL,
      limit: limit,
      endTime: endTime
    };

    try {
      const response = await axios.get(config.URL, { params, timeout: 10000 });
      const data = response.data;

      if (!data || data.length === 0) {
        console.log(`[${perpspec}] No more data available at time ${endTime}`);
        break;
      }

      const newestTimestamp = data[0][0];
      const oldestTimestamp = data[data.length - 1][0];

      const newData = data.filter(d => d[0] < endTime);
      allData.unshift(...newData);

      endTime = oldestTimestamp - 1;

      console.log(`[${perpspec}] Req ${i + 1}/${totalRequests}: +${newData.length} records (Total: ${allData.length}). Range: ${oldestTimestamp} to ${newestTimestamp}`);

      if (allData.length >= apiTarget) {
        console.log(`[${perpspec}] 🎯 API Target reached!`);
        break;
      }

      await sleep(config.RATE_DELAY);
    } catch (error) {
      console.error(`[${perpspec}] Request ${i + 1} error:`, error.message);
      break;
    }
  }

  return allData;
}

/**
 * Fetches Premium Index Klines from Bybit
 * Pages backward using 'end' timestamp parameter
 */
async function fetchBybitPFR(symbol, config) {
  const perpspec = config.PERPSPEC;
  const limit = config.LIMIT;
  const apiTarget = config.API_CANDLES_TARGET;
  const totalRequests = Math.ceil(apiTarget / limit);

  console.log(`[${perpspec}] Backfill: Target ${apiTarget} 1m records, ~${totalRequests} requests for ${symbol}`);

  let allData = [];
  let end = Date.now();

  for (let i = 0; i < totalRequests; i++) {
    const params = {
      category: 'linear',
      symbol: symbol,
      interval: config.API_INTERVAL,
      limit: limit,
      end: end
    };

    try {
      const response = await axios.get(config.URL, { params, timeout: 10000 });
      const data = response.data;
      const list = data.result?.list;

      if (!list || list.length === 0) {
        console.log(`[${perpspec}] No more data available at time ${end}`);
        break;
      }

      const newestTimestamp = parseInt(list[0][0], 10);
      const oldestTimestamp = parseInt(list[list.length - 1][0], 10);

      const newData = list.filter(d => parseInt(d[0], 10) < end);
      allData.unshift(...newData);

      end = oldestTimestamp - 1;

      console.log(`[${perpspec}] Req ${i + 1}/${totalRequests}: +${newData.length} records (Total: ${allData.length}). Range: ${oldestTimestamp} to ${newestTimestamp}`);

      if (allData.length >= apiTarget) {
        console.log(`[${perpspec}] 🎯 API Target reached!`);
        break;
      }

      await sleep(config.RATE_DELAY);
    } catch (error) {
      console.error(`[${perpspec}] Request ${i + 1} error:`, error.message);
      break;
    }
  }

  return allData;
}

/**
 * Fetches Premium History from OKX
 * Pages backward using 'before' timestamp parameter
 */
async function fetchOkxPFR(instId, config) {
  const perpspec = config.PERPSPEC;
  const limit = config.LIMIT;
  const apiTarget = config.API_CANDLES_TARGET;
  const totalRequests = Math.ceil(apiTarget / limit);

  console.log(`[${perpspec}] Backfill: Target ${apiTarget} 1m records, ~${totalRequests} requests for ${instId}`);

  let allData = [];
  let before = null;

  for (let i = 0; i < totalRequests; i++) {
    const params = {
      instId: instId,
      limit: limit
    };

    if (before !== null) {
      params.before = before;
    }

    try {
      const response = await axios.get(config.URL, { params, timeout: 10000 });
      const data = response.data;
      const list = data.data;

      if (data.code !== "0" || !list || list.length === 0) {
        console.log(`[${perpspec}] No more data available or request failed:`, data.msg || 'No data');
        break;
      }

      const newestTimestamp = list[0].ts;
      const oldestTimestamp = list[list.length - 1].ts;

      const newData = list.filter(d => before === null || d.ts < before);
      allData.unshift(...newData);

      before = oldestTimestamp;

      console.log(`[${perpspec}] Req ${i + 1}/${totalRequests}: +${newData.length} records (Total: ${allData.length}). Range: ${oldestTimestamp} to ${newestTimestamp}`);

      if (allData.length >= apiTarget) {
        console.log(`[${perpspec}] 🎯 API Target reached!`);
        break;
      }

      await sleep(config.RATE_DELAY);
    } catch (error) {
      if (error.response && error.response.status === 404) {
        console.error(`[${perpspec}] Request ${i + 1} error: 404 - Symbol ${instId} not found`);
      } else {
        console.error(`[${perpspec}] Request ${i + 1} error:`, error.message);
      }
      break;
    }
  }

  return allData;
}

// ============================================================================
// BACKFILL ORCHESTRATION
// ============================================================================

async function backfillSymbolAndExchange(baseSymbol, exchangeConfig) {
  const perpspec = exchangeConfig.PERPSPEC;
  const exchangeName = perpspec.split('-')[0];
  const dynamicSymbols = JSON.parse(fs.readFileSync('dynamic-symbols.json', 'utf8'));

  const symbolMap = dynamicSymbols[baseSymbol];
  let exchangeSymbol = symbolMap?.[perpspec];

  if (exchangeName === 'okx' && !exchangeSymbol) {
    exchangeSymbol = symbolMap['okx-swap'] || `${baseSymbol}-USDT-SWAP`;
  }
  if (!exchangeSymbol) {
    console.warn(`[${perpspec}] No symbol mapping found for ${baseSymbol}, skipping.`);
    return;
  }

  console.log(`[${perpspec}] Starting backfill for ${baseSymbol} (${exchangeSymbol})`);

  try {
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `Backfill started for ${baseSymbol} on ${exchangeName}`, {
      symbol: exchangeSymbol,
      interval: exchangeConfig.DB_INTERVAL,
      perpspec
    });

    let rawData = [];

    // Fetch raw data based on exchange
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

    console.log(`[${perpspec}] Fetched ${rawData.length} raw records for ${exchangeSymbol}`);

    if (rawData.length === 0) {
      const warningMsg = `No PFR data returned for ${exchangeSymbol}`;
      console.warn(`[${perpspec}] ${warningMsg}`);
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'warning', warningMsg, { symbol: exchangeSymbol });
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

    console.log(`[${perpspec}] Processed ${processedData.length} final records for insertion.`);

    if (processedData.length === 0) {
      const warningMsg = `No valid PFR data after processing for ${exchangeSymbol}`;
      console.warn(`[${perpspec}] ${warningMsg}`);
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'warning', warningMsg, { symbol: exchangeSymbol });
      return;
    }

    // Insert into database
    const expectedFields = Object.keys(processedData[0]);
    await apiUtils.ensureColumnsExist(dbManager, expectedFields);
    await apiUtils.updatePerpspecSchema(dbManager, perpspec, expectedFields);
    await dbManager.insertData(perpspec, processedData);

    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'success', `Backfill completed for ${baseSymbol} on ${exchangeName}`, {
      records_inserted: processedData.length,
      symbol: exchangeSymbol,
      perpspec
    });

    console.log(`[${perpspec}] ✅ Backfill completed for ${baseSymbol}`);

  } catch (error) {
    console.error(`[${perpspec}] Error during backfill for ${baseSymbol}:`, error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'FETCH_INSERT_ERROR', error.message, {
      exchange: exchangeName,
      symbol: baseSymbol,
      perpspec
    });
  }
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function execute() {
  console.log(`🚀 Starting ${SCRIPT_NAME} backfill for Premium Funding Rates...`);

  let dynamicSymbols;
  try {
    dynamicSymbols = JSON.parse(fs.readFileSync('dynamic-symbols.json', 'utf8'));
    console.log(`📊 Found ${Object.keys(dynamicSymbols).length} symbols to process`);
  } catch (error) {
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'FILE', 'MISSING_SYMBOLS', 'Could not read dynamic-symbols.json. Run symbol discovery first!');
    return;
  }

  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(3);
  const promises = [];

  const exchangesToFetch = [
    EXCHANGE_CONFIG.BINANCE,
    EXCHANGE_CONFIG.BYBIT,
    EXCHANGE_CONFIG.OKX
  ];

  for (const baseSymbol of Object.keys(dynamicSymbols)) {
    for (const config of exchangesToFetch) {
      promises.push(limit(() => backfillSymbolAndExchange(baseSymbol, config)));
    }
  }

  await Promise.all(promises);

  console.log('🎉 All Premium Funding Rate backfills completed!');
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================

if (require.main === module) {
  execute()
    .then(() => {
      console.log('✅ PFR backfill script completed successfully');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 PFR backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { execute };