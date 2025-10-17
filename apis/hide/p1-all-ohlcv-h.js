// SCRIPT: all-ohlcv-h.js
// Unified OHLCV Backfill Script for Binance, Bybit, and OKX
// Updated OKX to use /api/v5/market/history-candles with volume data
// Added Final Loop MT for most recent MT token records

const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');
const NOW = Date.now();

// ============================================================================
// PROXY CONFIGURATION
// ============================================================================
const PROXY_CONFIG = {
  host: '206.53.49.228',
  port: 13324,
  protocol: 'socks5',
  auth: {
    username: '14a233d28dd8f',
    password: 'bf64d81ae2'
  }
};

// Create Axios instances with proxy configuration
const axiosBinance = axios.create({
  proxy: {
    ...PROXY_CONFIG,
    protocol: 'socks5'
  },
  timeout: 10000
});

const axiosBybit = axios.create({
  proxy: {
    host: '206.53.49.228',
    port: 12324, // Using existing working port for Bybit
    protocol: 'socks5',
    auth: {
      username: '14a233d28dd8f',
      password: 'bf64d81ae2'
    }
  },
  timeout: 10000
});

const axiosOKX = axios.create({
  proxy: {
    host: '206.53.49.228',
    port: 12324, // Using existing working port for OKX
    protocol: 'socks5',
    auth: {
      username: '14a233d28dd8f',
      password: 'bf64d81ae2'
    }
  },
  timeout: 10000
});

const SCRIPT_NAME = 'all-ohlcv-h.js';
const INTERVAL = '1m';
const DAYS_TO_FETCH = 10;
const weightMonitor = require('../b-weight');

// ============================================================================
// USER CONFIGURATION
// ============================================================================
const STATUS_LOG_COLOR = '\x1b[38;2;136;43;135m'; // #882b87ff
const COLOR_RESET = '\x1b[0m';

// User-adjustable final pull records count for ALL exchanges
const RECENT_RECORDS_COUNT = 4; // Default 3 minutes - adjust as needed

// **USER ADJUSTABLE: Heartbeat interval in milliseconds (default: 10000ms = 10 seconds)**
const HEARTBEAT_INTERVAL_MS = 30000;

// ============================================================================
// EXCHANGE CONFIGURATIONS
// ============================================================================
const EXCHANGES = {
  BINANCE: {
    perpspec: 'bin-ohlcv',
    source: 'bin-ohlcv',
    url: 'https://fapi.binance.com/fapi/v1/klines',
    limit: 800,
    rateDelay: 300,
    concurrency: 6,
    timeout: 10000,
    apiInterval: '1m',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBinanceOHLCV,
    axiosInstance: axiosBinance
  },
  BYBIT: {
    perpspec: 'byb-ohlcv',
    source: 'byb-ohlcv',
    url: 'https://api.bybit.com/v5/market/kline',
    limit: 1000,
    rateDelay: 200,
    concurrency: 10,
    timeout: 10000,
    apiInterval: '1',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBybitOHLCV,
    axiosInstance: axiosBybit
  },
  OKX: {
    perpspec: 'okx-ohlcv',
    source: 'okx-ohlcv',
    url: 'https://www.okx.com/api/v5/market/history-candles',
    limit: 300,
    rateDelay: 200,
    concurrency: 5,
    timeout: 9000,
    retrySleepMs: 500,
    apiInterval: '1m',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}-USDT-SWAP`,
    fetch: fetchOKXOHLCV,
    axiosInstance: axiosOKX
  }
};

// ============================================================================
// MT CONFIGURATION
// ============================================================================
const MT_SYMBOLS = ['ETH', 'BTC', 'XRP', 'SOL'];
const MT_SYMBOL = 'MT';

// ============================================================================
// STATUS TRACKING
// ============================================================================
const connectionStatus = {
  'bin-ohlcv': false,
  'byb-ohlcv': false,
  'okx-ohlcv': false
};

const completionStatus = {
  'bin-ohlcv': false,
  'byb-ohlcv': false,
  'okx-ohlcv': false
};

let allConnected = false;

// ============================================================================
// SHARED UTILITIES
// ============================================================================
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function sortAndDeduplicateByTs(data) {
  const seen = new Set();
  return data
    .sort((a, b) => parseInt(a[0]) - parseInt(b[0]))
    .filter(item => {
      const ts = item[0];
      if (seen.has(ts)) return false;
      seen.add(ts);
      return true;
    });
}

// ============================================================================
// STATUS LOGGING HELPERS
// ============================================================================
async function logStatus(status, message) {
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, status, message);
  console.log(`${STATUS_LOG_COLOR}${message}${COLOR_RESET}`);
}

async function checkAndLogAllConnected() {
  if (!allConnected && connectionStatus['bin-ohlcv'] &&
      connectionStatus['byb-ohlcv'] && connectionStatus['okx-ohlcv']) {
    allConnected = true;
    const perpspecs = 'bin-ohlcv, byb-ohlcv, okx-ohlcv';
    await logStatus('connected', `🤖 ${perpspecs} connected, starting fetch.`);
  }
}

async function checkAndLogCompletion(perpspec) {
  if (!completionStatus[perpspec]) {
    completionStatus[perpspec] = true;
    await logStatus('completed', `${perpspec} backfill complete.`);
  }
}

async function checkAndLogAllCompleted(startTime) {
  if (completionStatus['bin-ohlcv'] && completionStatus['byb-ohlcv'] &&
      completionStatus['okx-ohlcv']) {
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    await logStatus('completed', `🤖 ${SCRIPT_NAME} backfill completed in ${duration}s!`);
  }
}

// ============================================================================
// FETCH FUNCTIONS
// ============================================================================
async function fetchBinanceOHLCV(symbol, config) {
  const totalCandles = DAYS_TO_FETCH * 1440;
  const totalRequests = Math.ceil(totalCandles / config.limit);

  let allCandles = [];
  let startTime = NOW - DAYS_TO_FETCH * 24 * 60 * 60 * 1000;

  for (let i = 0; i < totalRequests; i++) {
    const params = {
      symbol: symbol,
      interval: config.apiInterval,
      limit: config.limit,
      startTime: startTime
    };

    try {
      const response = await config.axiosInstance.get(config.url, { params });
      weightMonitor.logRequest('bin-ohlcv', '/fapi/v1/klines', 1);
      const data = response.data;

      // Mark connection as successful on first response
      if (!connectionStatus['bin-ohlcv']) {
        connectionStatus['bin-ohlcv'] = true;
        await checkAndLogAllConnected();
      }

      if (!data || data.length === 0) break;

      allCandles.push(...data);

      const lastCandleTime = data[data.length - 1][0];
      startTime = lastCandleTime + 60000;

      if (data.length < config.limit) break;

      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`❌ [${config.perpspec}] ${symbol}: ${error.message}`);
      throw error;
    }
  }

  return allCandles;
}

// ... [rest of the fetch functions remain the same, using their respective axios instances] ...

// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================
async function backfill() {
  const startTime = Date.now();
  const symbolCount = perpList.length;

  // ========================================================================
  // STATUS LOG #1: Script Started
  // ========================================================================
  await logStatus('started', `🤖 Starting ${SCRIPT_NAME} backfill for OHLCV data; ${symbolCount} symbols.`);

  // ========================================================================
  // HEARTBEAT MONITORING - logs perpspec 'running' status every interval
  // **USER ADJUSTABLE: Change HEARTBEAT_INTERVAL_MS at top of script**
  // ========================================================================
  const heartbeatInterval = setInterval(async () => {
    for (const perpspec of ['bin-ohlcv', 'byb-ohlcv', 'okx-ohlcv']) {
      if (!completionStatus[perpspec]) {
        await logStatus('running', `${perpspec} backfilling db.`);
      }
    }
  }, HEARTBEAT_INTERVAL_MS);

  // ========================================================================
  // MAIN BACKFILL - Fetch historical data for all exchanges
  // ========================================================================
  const promises = [];
  for (const exKey of Object.keys(EXCHANGES)) {
    const config = EXCHANGES[exKey];
    const limit = pLimit(config.concurrency);
    const processFunc = exKey === 'BINANCE' ? processBinanceData :
                       exKey === 'BYBIT' ? processBybitData :
                       processOKXData;

    for (const baseSymbol of perpList) {
      promises.push(limit(async () => {
        const symbol = config.mapSymbol(baseSymbol);
        try {
          const rawCandles = await config.fetch(symbol, config);
          if (rawCandles.length === 0) return;

          // Filter for confirmed candles where needed
          let processedCandles = rawCandles;
          if (config.perpspec === 'okx-ohlcv') {
            processedCandles = rawCandles.filter(c => c[8] === '1');
          }

          const processedData = processFunc(processedCandles, baseSymbol, config);
          if (processedData.length === 0) return;

          await dbManager.insertData(config.perpspec, processedData);
        } catch (error) {
          console.error(`❌ [${config.perpspec}] ${baseSymbol}: ${error.message}`);
        }
      }));
    }
  }

  await Promise.all(promises);

  // ========================================================================
  // STATUS LOG #4: Individual perpspec completions
  // ========================================================================
  await checkAndLogCompletion('bin-ohlcv');
  await checkAndLogCompletion('byb-ohlcv');
  await checkAndLogCompletion('okx-ohlcv');

  // MT token creation (console only - no DB log)
  await createMTToken();

  // ========================================================================
  // FINAL LOOP - Fetch most recent data for all exchanges (sequential)
  // Console notification only - no DB log
  // ========================================================================
  clearInterval(heartbeatInterval);
  console.log(`🤖 ${STATUS_LOG_COLOR}${SCRIPT_NAME} Final Loop started.${COLOR_RESET}`);

  try {
    // 1. Binance first
    await fetchRecentData(EXCHANGES.BINANCE);

    // 2. Bybit with special handling
    await fetchRecentData(EXCHANGES.BYBIT);

    // 3. OKX last with special handling
    await fetchRecentData(EXCHANGES.OKX);

    // 4. Final MT Loop - create most recent MT records (console only - no DB log)
    await createMTTokenFinalLoop();

  } catch (error) {
    console.error('❌ Error during final loops:', error);
  }

  // ========================================================================
  // STATUS LOG #5: All perpspecs completed
  // ========================================================================
  await checkAndLogAllCompleted(startTime);
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================
if (require.main === module) {
  backfill()
    .then(() => {
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 OHLCV backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { backfill };