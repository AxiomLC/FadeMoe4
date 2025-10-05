// SCRIPT: all-ohlcv-h.js 5 OCt 2025
// Unified OHLCV Backfill Script for Binance, Bybit, and OKX
// Optimized for maximum speed - no DB checks, no schema validation
// Direct fetch → process → insert with ON CONFLICT DO NOTHING

const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = 'all-ohlcv-h.js';
const INTERVAL = '1m';
const DAYS_TO_FETCH = 10;

// ============================================================================
// SHARED UTILITIES
// ============================================================================

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================================
// EXCHANGE CONFIGURATIONS
// ⚡ SPEED SETTINGS: Adjust concurrency, rateDelay, and limit for optimal performance
// ============================================================================

const EXCHANGES = {
  BINANCE: {
    perpspec: 'bin-ohlcv',
    source: 'bin-ohlcv',
    url: 'https://fapi.binance.com/fapi/v1/klines',
    limit: 1000,              // ⚡ Max 1000 = weight 5 per request (1500 = weight 10)
    rateDelay: 200,           // ⚡ Milliseconds between requests
    concurrency: 3,           // ⚡ Parallel symbol processing
    timeout: 10000,
    apiInterval: '1m',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBinanceOHLCV
  },
  BYBIT: {
    perpspec: 'byb-ohlcv',
    source: 'byb-ohlcv',
    url: 'https://api.bybit.com/v5/market/kline',
    limit: 1000,              // ⚡ Max candles per request
    rateDelay: 200,           // ⚡ Milliseconds between requests
    concurrency: 3,           // ⚡ Parallel symbol processing
    timeout: 10000,
    apiInterval: '1',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBybitOHLCV
  },
  OKX: {
    perpspec: 'okx-ohlcv',
    source: 'okx-ohlcv',
    url: 'https://www.okx.com/api/v5/market/history-candles',
    limit: 100,               // ⚡ Max candles per request (OKX limitation)
    rateDelay: 100,           // ⚡ Milliseconds between requests
    concurrency: 3,           // ⚡ Parallel symbol processing
    timeout: 10000,
    apiInterval: '1m',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}-USDT-SWAP`,
    fetch: fetchOKXOHLCV
  }
};

// ============================================================================
// FETCH FUNCTIONS
// ============================================================================

//=================BINANCE========================================
async function fetchBinanceOHLCV(symbol, config) {
  const totalCandles = DAYS_TO_FETCH * 1440;
  const totalRequests = Math.ceil(totalCandles / config.limit);

  let allCandles = [];
  let startTime = Date.now() - DAYS_TO_FETCH * 24 * 60 * 60 * 1000;

  for (let i = 0; i < totalRequests; i++) {
    const params = {
      symbol: symbol,
      interval: config.apiInterval,
      limit: config.limit,
      startTime: startTime
    };

    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      const data = response.data;

      if (!data || data.length === 0) break;

      allCandles.push(...data);

      // Move startTime to 1ms after the last candle's openTime
      const lastCandleTime = data[data.length - 1][0];
      startTime = lastCandleTime + 60000; // Move to next 1m candle

      if (data.length < config.limit) break;

      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
      throw error;
    }
  }

  return allCandles;
}

//=================BYBIT========================================
async function fetchBybitOHLCV(symbol, config) {
  const totalCandles = DAYS_TO_FETCH * 1440;
  const totalRequests = Math.ceil(totalCandles / config.limit);

  let allCandles = [];
  let end = Date.now();

  for (let i = 0; i < totalRequests; i++) {
    const params = {
      category: 'linear',
      symbol: symbol,
      interval: config.apiInterval,
      limit: config.limit,
      end: end
    };

    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      const data = response.data;

      if (!data.result?.list || data.result.list.length === 0) break;

      allCandles.push(...data.result.list);

      end = data.result.list[data.result.list.length - 1][0] - 1;

      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
      throw error;
    }
  }

  return allCandles;
}

//=================OKX========================================
async function fetchOKXOHLCV(symbol, config) {
  const totalCandles = DAYS_TO_FETCH * 1440;
  const totalRequests = Math.ceil(totalCandles / config.limit);

  let allCandles = [];
  let after = null;

  for (let i = 0; i < totalRequests; i++) {
    let url = `${config.url}?instId=${symbol}&bar=${config.apiInterval}&limit=${config.limit}`;
    if (after) url += `&after=${after}`;

    try {
      const response = await axios.get(url, { timeout: config.timeout });
      const data = response.data;

      if (data.code !== '0' || !data.data || data.data.length === 0) break;

      allCandles.push(...data.data);

      after = data.data[data.data.length - 1][0];

      if (allCandles.length >= totalCandles) break;

      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
      throw error;
    }
  }

  return allCandles;
}

// ============================================================================
// DATA PROCESSING FUNCTIONS
// ============================================================================

//=================BINANCE========================================
function processBinanceData(rawCandles, baseSymbol, config) {
  const result = [];

  for (const candle of rawCandles) {
    try {
      const ts = apiUtils.toMillis(BigInt(candle[0]));
      result.push({
        ts,
        symbol: baseSymbol,
        source: config.source,
        perpspec: config.perpspec,
        interval: config.dbInterval,
        o: parseFloat(candle[1]),
        h: parseFloat(candle[2]),
        l: parseFloat(candle[3]),
        c: parseFloat(candle[4]),
        v: parseFloat(candle[5])
      });
    } catch (e) {
      // Skip invalid records
    }
  }

  return result;
}

//=================BYBIT========================================
function processBybitData(rawCandles, baseSymbol, config) {
  const result = [];

  for (const candle of rawCandles) {
    try {
      const ts = apiUtils.toMillis(BigInt(candle[0]));
      result.push({
        ts,
        symbol: baseSymbol,
        source: config.source,
        perpspec: config.perpspec,
        interval: config.dbInterval,
        o: parseFloat(candle[1]),
        h: parseFloat(candle[2]),
        l: parseFloat(candle[3]),
        c: parseFloat(candle[4]),
        v: parseFloat(candle[5])
      });
    } catch (e) {
      // Skip invalid records
    }
  }

  return result;
}

//=================OKX========================================
function processOKXData(rawCandles, baseSymbol, config) {
  const result = [];

  for (const candle of rawCandles) {
    try {
      const ts = apiUtils.toMillis(BigInt(candle[0]));
      result.push({
        ts,
        symbol: baseSymbol,
        source: config.source,
        perpspec: config.perpspec,
        interval: config.dbInterval,
        o: parseFloat(candle[1]),
        h: parseFloat(candle[2]),
        l: parseFloat(candle[3]),
        c: parseFloat(candle[4]),
        v: parseFloat(candle[5])
      });
    } catch (e) {
      // Skip invalid records
    }
  }

  return result;
}

// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================

async function backfill() {
  const startTime = Date.now();

  console.log(`\n🚀 Starting ${SCRIPT_NAME} backfill for OHLCV data...`);

  const perpspecs = Object.values(EXCHANGES).map(ex => ex.perpspec).join(', ');

  // STATUS #1: Starting
  await apiUtils.logScriptStatus(
    dbManager,
    SCRIPT_NAME,
    'running',
    `${SCRIPT_NAME} backfill`
);
  await apiUtils.logScriptStatus(
    dbManager,
    SCRIPT_NAME,
    '🚀started',
    `Starting ${SCRIPT_NAME} backfill for OHLCV data for ${perpspecs}.`
  );

  // Track connection status per exchange
  const connectedLogged = {};
  for (const exKey of Object.keys(EXCHANGES)) {
    connectedLogged[EXCHANGES[exKey].perpspec] = false;
  }

  const promises = [];

  // Process each exchange
  for (const exKey of Object.keys(EXCHANGES)) {
    const config = EXCHANGES[exKey];
    const limit = pLimit(config.concurrency);

    // Assign processing function based on exchange
    let processFunc;
    if (exKey === 'BINANCE') processFunc = processBinanceData;
    else if (exKey === 'BYBIT') processFunc = processBybitData;
    else if (exKey === 'OKX') processFunc = processOKXData;

    for (const baseSymbol of perpList) {
      promises.push(limit(async () => {
        const symbol = config.mapSymbol(baseSymbol);

        try {
          // STATUS #2: Log connected on first successful start
          if (!connectedLogged[config.perpspec]) {
            await apiUtils.logScriptStatus(
              dbManager,
              SCRIPT_NAME,
              'connected',
              `${config.perpspec} connected, starting fetch for ${baseSymbol}`
            );
            connectedLogged[config.perpspec] = true;
          }

          // Fetch OHLCV data
          const rawCandles = await config.fetch(symbol, config);

          if (rawCandles.length === 0) return;

          // Process data
          const processedData = processFunc(rawCandles, baseSymbol, config);

          if (processedData.length === 0) return;

          // Insert to DB (ON CONFLICT DO NOTHING handles duplicates)
          await dbManager.insertData(config.perpspec, processedData);

          console.log(`✅ [${config.perpspec}] ${baseSymbol}: ${processedData.length} records`);

        } catch (error) {
          console.error(`❌ [${config.perpspec}] ${baseSymbol}: ${error.message}`);

          // Determine error type
          const errorCode = error.response?.status === 429 ? 'RATE_LIMIT' :
                           error.message.includes('timeout') ? 'TIMEOUT' :
                           error.message.includes('404') ? 'NOT_FOUND' : 'FETCH_ERROR';

          // Log error
          await apiUtils.logScriptError(
            dbManager,
            SCRIPT_NAME,
            'API',
            errorCode,
            `${config.perpspec} error for ${baseSymbol}: ${error.message}`,
            { perpspec: config.perpspec, symbol: baseSymbol }
          );

          // Log internal error if connection never established
          if (!connectedLogged[config.perpspec]) {
            await apiUtils.logScriptError(
              dbManager,
              SCRIPT_NAME,
              'INTERNAL',
              'INSERT_FAILED',
              `${config.perpspec} failed to establish connection for ${baseSymbol}`,
              { perpspec: config.perpspec, symbol: baseSymbol }
            );
          }
        }
      }));
    }
  }

  await Promise.all(promises);

  // STATUS #3: Complete per exchange
  for (const exKey of Object.keys(EXCHANGES)) {
    const config = EXCHANGES[exKey];
    await apiUtils.logScriptStatus(
      dbManager,
      SCRIPT_NAME,
      'complete',
      `${config.perpspec} backfill complete.`
    );
    console.log(`✅ ${config.perpspec} backfill complete.`);
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`\n🎉 All OHLCV backfills completed in ${duration}s!`);
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================

if (require.main === module) {
  backfill()
    .then(() => {
      console.log('✅ OHLCV backfill script completed successfully');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 OHLCV backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { backfill };