const fs = require('fs');
const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
require('dotenv').config();

const SCRIPT_NAME = 'all-oi-h.js';
const DAYS_TO_FETCH = 10;
// Target for the DB insertion (10 days @ 1 minute)
const TOTAL_CANDLES_TARGET = DAYS_TO_FETCH * 1440; // 14400 records total desired in DB

// ============================================================================
// EXCHANGE CONFIGURATION
// ============================================================================
// Define API limits, intervals, and rate limit delays per exchange
// All exchanges provide 5-minute minimum for historical OI data
// We expand this to 1-minute records for the database

const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-oi',
    LIMIT: 500,
    RATE_DELAY: 200,
    URL: 'https://fapi.binance.com/futures/data/openInterestHist',
    API_INTERVAL: '5m',
    DB_INTERVAL: '1m',
    API_CANDLES_TARGET: TOTAL_CANDLES_TARGET / 5 
  },
  BYBIT: {
    PERPSPEC: 'byb-oi',
    LIMIT: 200,
    RATE_DELAY: 200,
    URL: 'https://api.bybit.com/v5/market/open-interest',
    API_INTERVAL: '5min',
    DB_INTERVAL: '1m',
    API_CANDLES_TARGET: TOTAL_CANDLES_TARGET / 5
  },
  OKX: {
    PERPSPEC: 'okx-oi',
    LIMIT: 100, // OKX max is 100 records per request
    RATE_DELAY: 100,
    URL: 'https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history',
    API_INTERVAL: '5m', // OKX provides 5-minute minimum for OI history
    DB_INTERVAL: '1m',
    API_CANDLES_TARGET: TOTAL_CANDLES_TARGET / 5
  }
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Utility to pause execution.
 * @param {number} ms Milliseconds to sleep.
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================================
// DATA PROCESSING FUNCTIONS
// ============================================================================

/**
 * Generic function to expand a 5-minute Open Interest record into 5 x 1-minute records.
 * This is used for exchanges like Binance, Bybit, and OKX that only offer 5m minimum for historical OI.
 * Each 5-minute candle is duplicated 5 times with timestamps 1 minute apart.
 * 
 * @param {Array<Object>} rawData Array of raw 5m data points.
 * @param {string} baseSymbol Base asset (e.g., BTC).
 * @param {object} config Exchange-specific configuration.
 * @param {string} oiKey Key for the Open Interest value (e.g., 'sumOpenInterest', 'openInterest', 'oi').
 * @param {string} tsKey Key for the timestamp value (e.g., 'timestamp', 'ts').
 * @returns {Array<Object>} Array of standardized 1m data objects for DB insertion.
 */
function expandFiveMinToOImData(rawData, baseSymbol, config, oiKey, tsKey) {
  const perpspec = config.PERPSPEC;
  const expandedData = [];
  const MINUTE_IN_MS = 60 * 1000;

  rawData.forEach(dataPoint => {
    try {
      const oiValue = Array.isArray(dataPoint) ? dataPoint[oiKey] : dataPoint[oiKey];
      const oiTimestamp = Array.isArray(dataPoint) ? dataPoint[tsKey] : dataPoint[tsKey];
      const oi = parseFloat(oiValue);
      if (isNaN(oi)) {
        console.warn(`[${perpspec}] Skipping invalid OI value for ${baseSymbol}:`, oiValue);
        return;
      }

      // The timestamp from the exchange is the start of the 5-minute candle.
      const baseTs = apiUtils.toMillis(BigInt(oiTimestamp)); 

      // Create 5 records, one for each minute within the 5-minute candle,
      // all sharing the same OI value, satisfying the backtester's 1m need.
      for (let m = 0; m < 5; m++) {
        // Calculate the timestamp for the start of the m-th minute.
        const currentMinuteTs = baseTs + BigInt(m) * BigInt(MINUTE_IN_MS);

        expandedData.push({
          ts: currentMinuteTs,
          symbol: baseSymbol,
          source: perpspec,
          perpspec,
          interval: config.DB_INTERVAL,
          oi
        });
      }

    } catch (e) {
      console.warn(`[${perpspec}] Skipping invalid data point for ${baseSymbol}:`, dataPoint, e.message);
    }
  });

  return expandedData;
}

/**
 * Processes data for native 1m intervals (currently unused as all exchanges provide 5m minimum).
 * Kept for future compatibility if an exchange offers native 1m OI data.
 * 
 * @param {Array<Object|Array>} rawData Array of raw data points from an exchange.
 * @param {string} baseSymbol Base asset (e.g., BTC).
 * @param {object} config Exchange-specific configuration.
 * @returns {Array<Object>} Array of standardized data objects for DB insertion.
 */
function processNativeOIData(rawData, baseSymbol, config) {
  const perpspec = config.PERPSPEC;

  return rawData.map(dataPoint => {
    try {
      let oiValue, timestamp;

      // OKX: OI contracts is oi (String), time is ts (String MS)
      oiValue = dataPoint.oi;
      timestamp = dataPoint.ts;
      
      const ts = apiUtils.toMillis(BigInt(timestamp));
      const oi = parseFloat(oiValue);

      if (isNaN(oi)) {
          console.warn(`[${perpspec}] Skipping invalid OI value for ${baseSymbol}:`, oiValue);
          return null;
      }

      return {
        ts,
        symbol: baseSymbol,
        source: perpspec,
        perpspec,
        interval: config.DB_INTERVAL,
        oi
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
 * Fetches Open Interest history from Binance.
 * Pages backward using 'endTime' parameter.
 * Returns array of 5-minute OI records.
 */
async function fetchBinanceOI(symbol, config) {
  const perpspec = config.PERPSPEC;
  const limit = config.LIMIT;
  const apiTarget = config.API_CANDLES_TARGET;
  const totalRequests = Math.ceil(apiTarget / limit);

  console.log(`[${perpspec}] Backfill: Target ${apiTarget} 5m records, ~${totalRequests} requests for ${symbol}`);

  let allData = [];
  let endTime = Date.now(); // Start fetching from now and go backward

  for (let i = 0; i < totalRequests; i++) {
    const params = {
      symbol: symbol,
      period: config.API_INTERVAL, // 5m
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

      // Binance returns latest first. We prepend data and update endTime to the oldest timestamp found.
      const newestTimestamp = data[0].timestamp;
      const oldestTimestamp = data[data.length - 1].timestamp;

      // Filter out any overlap
      const newData = data.filter(d => d.timestamp < endTime);
      allData.unshift(...newData);

      // The next request will end just before the oldest record found in this response.
      endTime = oldestTimestamp - 1;

      console.log(`[${perpspec}] Req ${i + 1}/${totalRequests}: +${newData.length} 5m records (Total: ${allData.length} 5m records). Range: ${oldestTimestamp} to ${newestTimestamp}`);

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
 * Fetches Open Interest history from Bybit.
 * Pages backward using 'end' timestamp parameter.
 * Returns array of 5-minute OI records.
 */
async function fetchBybitOI(symbol, config) {
  const perpspec = config.PERPSPEC;
  const limit = config.LIMIT;
  const apiTarget = config.API_CANDLES_TARGET;
  const totalRequests = Math.ceil(apiTarget / limit);

  console.log(`[${perpspec}] Backfill: Target ${apiTarget} 5min records, ~${totalRequests} requests for ${symbol}`);

  let allData = [];
  let end = Date.now(); // Start fetching from now and go backward

  for (let i = 0; i < totalRequests; i++) {
    const params = {
      category: 'linear',
      symbol: symbol,
      intervalTime: config.API_INTERVAL, // 5min
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

      // Bybit list is sorted newest first. Prepend and update 'end'.
      const newestTimestamp = parseInt(list[0].timestamp, 10);
      const oldestTimestamp = parseInt(list[list.length - 1].timestamp, 10);

      // Filter to prevent overlap (though 'end' should handle it)
      const newData = list.filter(d => parseInt(d.timestamp, 10) < end);
      allData.unshift(...newData);

      // The next request will end just before the oldest record found in this response.
      end = oldestTimestamp - 1;

      console.log(`[${perpspec}] Req ${i + 1}/${totalRequests}: +${newData.length} 5min records (Total: ${allData.length} 5min records). Range: ${oldestTimestamp} to ${newestTimestamp}`);

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
 * Fetches Open Interest history from OKX.
 * Pages backward using 'before' timestamp parameter.
 * Uses the /rubik/stat/contracts/open-interest-history endpoint.
 * Returns array of 5-minute OI records.
 */
async function fetchOkxOI(instId, config) {
  const perpspec = config.PERPSPEC;
  const limit = config.LIMIT;
  const apiTarget = config.API_CANDLES_TARGET;
  const totalRequests = Math.ceil(apiTarget / limit);

  console.log(`[${perpspec}] Backfill: Target ${apiTarget} 5m records, ~${totalRequests} requests for ${instId}`);

  let allData = [];
  let before = null; // Start with null to get the newest data first

  for (let i = 0; i < totalRequests; i++) {
    const params = {
      instId: instId,
      period: config.API_INTERVAL, // Use 'period' parameter with '5m' value
      limit: limit
    };

    // Only add 'before' if it has a value
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

      // OKX list is sorted newest first. Prepend and update 'before'.
      const newestTimestamp = list[0][0];
      const oldestTimestamp = list[list.length - 1][0];

      // Filter to prevent overlap 
      const newData = list.filter(d => before === null || d[0] < before);
      allData.unshift(...newData);

      // The next request will query data *before* the oldest record found in this response.
      before = oldestTimestamp;

      console.log(`[${perpspec}] Req ${i + 1}/${totalRequests}: +${newData.length} 5m records (Total: ${allData.length} 5m records). Range: ${oldestTimestamp} to ${newestTimestamp}`);

      if (allData.length >= apiTarget) {
        console.log(`[${perpspec}] 🎯 API Target reached!`);
        break;
      }

      await sleep(config.RATE_DELAY);
    } catch (error) {
      if (error.response && error.response.status === 404) {
        console.error(`[${perpspec}] Request ${i + 1} error: 404 - Symbol ${instId} not found or parameter combination not supported.`);
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

/**
 * Executes the backfill process for a single symbol and exchange.
 * 1. Loads symbol mapping from dynamic-symbols.json
 * 2. Fetches raw 5m data from the exchange API
 * 3. Expands 5m records to 1m records
 * 4. Inserts processed data into the database
 * 
 * @param {string} baseSymbol Base asset (e.g., BTC).
 * @param {object} exchangeConfig Configuration object for the target exchange.
 */
async function backfillSymbolAndExchange(baseSymbol, exchangeConfig) {
  const perpspec = exchangeConfig.PERPSPEC;
  const exchangeName = perpspec.split('-')[0]; // e.g., 'bin' -> 'binance'
  const dynamicSymbols = JSON.parse(fs.readFileSync('dynamic-symbols.json', 'utf8'));

  // Get the symbol/instrument ID for the current exchange
  const symbolMap = dynamicSymbols[baseSymbol];
  let exchangeSymbol = symbolMap?.[perpspec];

  // OKX uses a different ID format (e.g., BTC-USDT-SWAP) often stored under a different key in symbol map
  if (exchangeName === 'okx' && !exchangeSymbol) {
     // Fallback to a common pattern:
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

    // ========================================================================
    // STEP 1: Fetch raw data based on exchange
    // ========================================================================
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

    console.log(`[${perpspec}] Fetched ${rawData.length} raw records for ${exchangeSymbol}`);

    if (rawData.length === 0) {
      const warningMsg = `No OI data returned for ${exchangeSymbol}`;
      console.warn(`[${perpspec}] ${warningMsg}`);
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'warning', warningMsg, { symbol: exchangeSymbol });
      return;
    }

    // ========================================================================
    // STEP 2: Process data - expand 5m to 1m records
    // ========================================================================
    let processedData = [];

    // All exchanges now use 5m data that needs expansion to 1m
    if (exchangeConfig.API_INTERVAL === '5m' || exchangeConfig.API_INTERVAL === '5min') {
      let oiKey, tsKey;
      
      // Define the field names for OI value and timestamp based on exchange
      if (perpspec === EXCHANGE_CONFIG.BINANCE.PERPSPEC) {
        oiKey = 'sumOpenInterest';
        tsKey = 'timestamp';
      } else if (perpspec === EXCHANGE_CONFIG.BYBIT.PERPSPEC) {
        oiKey = 'openInterest';
        tsKey = 'timestamp';
      } else if (perpspec === EXCHANGE_CONFIG.OKX.PERPSPEC) {
        oiKey = 1;  // OI contracts is at index 1 in the array
        tsKey = 0;  // Timestamp is at index 0
      }
      
      processedData = expandFiveMinToOImData(rawData, baseSymbol, exchangeConfig, oiKey, tsKey);
    } else {
      // Fallback for native 1m data (currently unused)
      processedData = processNativeOIData(rawData, baseSymbol, exchangeConfig);
    }
    
    // Log the final number of records for insertion
    console.log(`[${perpspec}] Processed ${processedData.length} final 1m records for insertion.`);

    if (processedData.length === 0) {
      const warningMsg = `No valid OI data after processing for ${exchangeSymbol}`;
      console.warn(`[${perpspec}] ${warningMsg}`);
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'warning', warningMsg, { symbol: exchangeSymbol });
      return;
    }

    // ========================================================================
    // STEP 3: Insert into database
    // ========================================================================
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

/**
 * Main execution function.
 * Orchestrates backfill across all symbols and exchanges in parallel.
 * Uses concurrency limiting to avoid overwhelming exchange APIs.
 */
async function execute() {
  console.log(`🚀 Starting ${SCRIPT_NAME} backfill for OI contracts...`);

  let dynamicSymbols;
  try {
    dynamicSymbols = JSON.parse(fs.readFileSync('dynamic-symbols.json', 'utf8'));
    console.log(`📊 Found ${Object.keys(dynamicSymbols).length} symbols to process`);
  } catch (error) {
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'FILE', 'MISSING_SYMBOLS', 'Could not read dynamic-symbols.json. Run symbol discovery first!');
    return;
  }

  const pLimit = (await import('p-limit')).default;
  // Use a sensible concurrency limit to avoid hitting multiple exchange limits too hard
  const limit = pLimit(3);
  const promises = [];

  const exchangesToFetch = [
    EXCHANGE_CONFIG.BINANCE,
    EXCHANGE_CONFIG.BYBIT,
    EXCHANGE_CONFIG.OKX
  ];

  // Queue tasks for each symbol/exchange combination
  for (const baseSymbol of Object.keys(dynamicSymbols)) {
    for (const config of exchangesToFetch) {
      promises.push(limit(() => backfillSymbolAndExchange(baseSymbol, config)));
    }
  }

  await Promise.all(promises);

  console.log('🎉 All Open Interest backfills completed!');
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================

if (require.main === module) {
  execute()
    .then(() => {
      console.log('🏁 OI backfill script completed successfully');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 OI backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { execute };