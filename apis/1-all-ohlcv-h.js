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

const SCRIPT_NAME = 'all-ohlcv-h.js';
const INTERVAL = '1m';
const DAYS_TO_FETCH = 10;
const weightMonitor = require('../b-weight');

// ============================================================================
// USER CONFIGURATION
// ============================================================================
const STATUS_LOG_COLOR = '\x1b[38;2;135;206;235m'; // #87CEEB Sky Blue
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
    rateDelay: 200,
    concurrency: 9,
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
    limit: 1000,
    rateDelay: 200,
    concurrency: 10,
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
    limit: 300,
    rateDelay: 40,
    concurrency: 5,
    timeout: 9000,
    retrySleepMs: 500,
    apiInterval: '1m',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}-USDT-SWAP`,
    fetch: fetchOKXOHLCV
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
    await logStatus('connected', `${perpspecs} connected, starting fetch.`);
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
      const response = await axios.get(config.url, { params, timeout: config.timeout });
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

async function fetchBybitOHLCV(symbol, config) {
  const totalCandles = DAYS_TO_FETCH * 1440;
  const totalRequests = Math.ceil(totalCandles / config.limit);

  let allCandles = [];
  let end = NOW;

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

      // Mark connection as successful on first response
      if (!connectionStatus['byb-ohlcv']) {
        connectionStatus['byb-ohlcv'] = true;
        await checkAndLogAllConnected();
      }

      if (!data.result?.list || data.result.list.length === 0) break;

      allCandles.push(...data.result.list);
      end = data.result.list[data.result.list.length - 1][0] - 1;

      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`❌ [${config.perpspec}] ${symbol}: ${error.message}`);
      throw error;
    }
  }

  return allCandles;
}

let okxRateLimitCount = 0;
let okxLastRateLimitLog = Date.now();

async function fetchOKXOHLCV(symbol, config) {
  const totalCandles = DAYS_TO_FETCH * 1440;
  const totalRequests = Math.ceil(totalCandles / config.limit);
  let allCandles = [];
  let after = null;

  const targetOldTs = NOW - DAYS_TO_FETCH * 24 * 60 * 60 * 1000;

  for (let i = 0; i < totalRequests; i++) {
    let url = `${config.url}?instId=${symbol}&bar=${config.apiInterval}&limit=${config.limit}`;
    if (after !== null) url += `&after=${after}`;

    try {
      const response = await axios.get(url, { timeout: config.timeout });
      const data = response.data;

      // Mark connection as successful on first response
      if (!connectionStatus['okx-ohlcv']) {
        connectionStatus['okx-ohlcv'] = true;
        await checkAndLogAllConnected();
      }

      if (data.code !== '0' || !data.data || data.data.length === 0) {
        console.error(`❌ [${config.perpspec}] ${symbol}: Empty response`);
        break;
      }

      // Filter for confirmed candles only (index 8)
      const confirmedCandles = data.data.filter(c => c[8] === '1');
      allCandles = [...confirmedCandles, ...allCandles];
      const oldestTs = confirmedCandles.length > 0
        ? parseInt(confirmedCandles[confirmedCandles.length - 1][0]) - 1
        : targetOldTs - 1;
      after = oldestTs;

      if (confirmedCandles.length < config.limit || oldestTs < targetOldTs) break;

      await sleep(config.rateDelay);
    } catch (error) {
      if (error.response?.status === 429) {
        okxRateLimitCount++;
        const now = Date.now();
        if (now - okxLastRateLimitLog >= 30000) {
          console.warn(`[${config.perpspec}] Rate limit hit ${okxRateLimitCount} times in last 30s`);
          okxRateLimitCount = 0;
          okxLastRateLimitLog = now;
        }
        await sleep(config.retrySleepMs);
        i--;
      } else {
        console.error(`❌ [${config.perpspec}] ${symbol}: ${error.message}`);
        throw error;
      }
    }
  }

  return allCandles;
}

// ============================================================================
// DATA PROCESSING FUNCTIONS
// ============================================================================
function processBinanceData(rawCandles, baseSymbol, config) {
  return rawCandles.map(candle => {
    const ts = BigInt(candle[0]);
    return {
      ts,
      symbol: baseSymbol,
      source: config.source,
      perpspec: config.perpspec,
      interval: config.dbInterval,
      o: parseFloat(candle[1]),
      h: parseFloat(candle[2]),
      l: parseFloat(candle[3]),
      c: parseFloat(candle[4]),
      v: parseFloat(candle[7]) // [7] (quoteVolume)
    };
  }).filter(item => item.ts !== undefined);
}

function processBybitData(rawCandles, baseSymbol, config) {
  return rawCandles.map(candle => {
    const ts = BigInt(candle[0]);
    return {
      ts,
      symbol: baseSymbol,
      source: config.source,
      perpspec: config.perpspec,
      interval: config.dbInterval,
      o: parseFloat(candle[1]),
      h: parseFloat(candle[2]),
      l: parseFloat(candle[3]),
      c: parseFloat(candle[4]),
      v: parseFloat(candle[6])
    };
  }).filter(item => item.ts !== undefined);
}

function processOKXData(rawCandles, baseSymbol, config) {
  return rawCandles.map(candle => {
    const ts = BigInt(candle[0]);
    return {
      ts,
      symbol: baseSymbol,
      source: config.source,
      perpspec: config.perpspec,
      interval: config.dbInterval,
      o: parseFloat(candle[1]),
      h: parseFloat(candle[2]),
      l: parseFloat(candle[3]),
      c: parseFloat(candle[4]),
      v: parseFloat(candle[7]) // Volume in quote currency $ USD
    };
  }).filter(item => item.ts !== undefined);
}

// ============================================================================
// MT CREATION FUNCTION (Console only - no DB log)
// ============================================================================
async function createMTToken() {
  try {
    console.log('🤖 Creating MT token...');
    
    const mtData = await Promise.all(MT_SYMBOLS.map(async (sym) => {
      const query = `SELECT ts, o::numeric AS open, h::numeric AS high, l::numeric AS low, c::numeric AS close, v::numeric AS volume
                     FROM perp_data
                     WHERE symbol = $1 AND perpspec = $2 AND interval = $3
                     ORDER BY ts ASC`;
      const result = await dbManager.pool.query(query, [sym, 'bin-ohlcv', '1m']);
      return result.rows.map(row => ({
        ts: BigInt(row.ts),
        open: parseFloat(row.open),
        high: parseFloat(row.high),
        low: parseFloat(row.low),
        close: parseFloat(row.close),
        volume: parseFloat(row.volume)
      })).filter(p => !isNaN(p.open) && !isNaN(p.high) &&
                     !isNaN(p.low) && !isNaN(p.close) &&
                     !isNaN(p.volume));
    }));

    if (mtData.some(data => data.length === 0)) {
      console.error('Missing data for some MT symbols—skipping MT creation');
      return;
    }

    const allTs = [...new Set(mtData.flatMap(data => data.map(p => p.ts.toString())))].sort();
    const mtRecords = allTs.map(ts => {
      const tsBigInt = BigInt(ts);
      let totalO = 0, totalH = 0, totalL = 0, totalC = 0, totalV = 0;
      let count = 0;

      for (const data of mtData) {
        let lastValid = null;
        for (const p of data) {
          if (p.ts <= tsBigInt) lastValid = p;
          else break;
        }
        if (lastValid) {
          totalO += lastValid.open;
          totalH += lastValid.high;
          totalL += lastValid.low;
          totalC += lastValid.close;
          totalV += lastValid.volume;
          count++;
        }
      }

      if (count === 0) return null;

      return {
        ts: tsBigInt,
        symbol: MT_SYMBOL,
        source: 'bin-ohlcv',
        perpspec: 'bin-ohlcv',
        interval: '1m',
        o: totalO / count,
        h: totalH / count,
        l: totalL / count,
        c: totalC / count,
        v: totalV
      };
    }).filter(record => record !== null);

    if (mtRecords.length > 0) {
      await dbManager.insertData('bin-ohlcv', mtRecords);
      console.log('✅ MT token created');
    }
  } catch (error) {
    console.error('❌ MT creation failed:', error.message);
  }
}

// ============================================================================
// FINAL LOOP FUNCTIONS
// =======================BINANCE  =====================================================

async function fetchBinanceFinalLoop(symbol, config) {
  const now = Date.now();
  const startTime = now - (RECENT_RECORDS_COUNT * 60 * 1000);
  const params = {
    symbol: symbol,
    interval: config.apiInterval,
    startTime: startTime,
    endTime: now,
    limit: RECENT_RECORDS_COUNT
  };

  try {
    const response = await axios.get(config.url, { params, timeout: config.timeout });
    weightMonitor.logRequest('bin-ohlcv', '/fapi/v1/klines', 1);
    const data = response.data;

    if (!data || data.length === 0) return [];
    return data;
  } catch (error) {
    console.error(`❌ [BINANCE] Final loop error for ${symbol}: ${error.message}`);
    return [];
  }
}

//***************************BYBIT*************************************************
// Special Bybit final loop that gets the most recent records
async function fetchBybitFinalLoop(symbol, config) {
  const now = Date.now();
  const startTs = now - (RECENT_RECORDS_COUNT * 60 * 1000);

  try {
    const params = {
      category: 'linear',
      symbol: symbol,
      interval: config.apiInterval,
      limit: config.limit,
      start: startTs,
      end: now
    };

    const response = await axios.get(config.url, { params, timeout: config.timeout });
    const data = response.data;

    if (!data.result?.list || data.result.list.length === 0) {
      return [];
    }

    let recentCandles = data.result.list;

    // If we don't have enough confirmed candles, get the most recent ones regardless
    if (recentCandles.filter(c => c[8] === true).length < RECENT_RECORDS_COUNT) {
      const recentParams = {
        category: 'linear',
        symbol: symbol,
        interval: config.apiInterval,
        limit: 5
      };

      const recentResponse = await axios.get(config.url, { params: recentParams, timeout: config.timeout });
      if (recentResponse.data.result?.list) {
        const supplemental = recentResponse.data.result.list
          .filter(c => parseInt(c[0]) > startTs)
          .slice(0, RECENT_RECORDS_COUNT);

        recentCandles = [...recentCandles, ...supplemental];
        recentCandles = sortAndDeduplicateByTs(recentCandles);
      }
    }

    return recentCandles;
  } catch (error) {
    console.error(`❌ [BYBIT] Final loop error for ${symbol}: ${error.message}`);
    return [];
  }
}

//=====================================OKX=================================
async function fetchOKXFinalLoop(symbol, config, startTs, endTs) {
  let allCandles = [];
  let before = null;
  const maxAttempts = 3;
  const OKX_CONFIRMATION_DELAY = 5 * 60 * 1000;
  const adjustedStartTs = startTs - OKX_CONFIRMATION_DELAY;
  const adjustedEndTs = endTs + OKX_CONFIRMATION_DELAY;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      let url = `${config.url}?instId=${symbol}&bar=${config.apiInterval}&limit=${config.limit}`;
      if (before !== null) url += `&before=${before}`;

      const response = await axios.get(url, { timeout: config.timeout });
      const data = response.data;

      if (data.code !== '0' || !data.data) {
        throw new Error(`API error: ${data.msg || 'Unknown error'}`);
      }

      const confirmedCandles = data.data
        .filter(c => c[8] === '1' &&
               parseInt(c[0]) >= adjustedStartTs &&
               parseInt(c[0]) <= adjustedEndTs);

      if (confirmedCandles.length === 0) {
        if (attempt === maxAttempts - 1) return [];
        await sleep(1000);
        continue;
      }

      allCandles = [...confirmedCandles, ...allCandles];
      const oldestTs = parseInt(confirmedCandles[confirmedCandles.length - 1][0]);
      if (oldestTs <= adjustedStartTs || confirmedCandles.length < config.limit) break;

      before = oldestTs - 1;
      await sleep(config.rateDelay);

    } catch (error) {
      if (error.response?.status === 429) {
        await sleep(config.retrySleepMs);
        continue;
      } else if (attempt === maxAttempts - 1) {
        console.error(`❌ [OKX] Final loop error for ${symbol}: ${error.message}`);
        return [];
      }
      await sleep(1000);
    }
  }

  return allCandles;
}

//=============================FINAL LOOP Fetch=======================================
async function fetchRecentData(config) {
  const limit = pLimit(config.concurrency);
  const now = Date.now();
  const recentPromises = [];

  const startTs = now - (RECENT_RECORDS_COUNT * 60 * 1000);
  const endTs = now;

  for (const baseSym of perpList) {
    recentPromises.push(limit(async () => {
      const symbol = config.mapSymbol(baseSym);
      try {
        // Use specialized final loop functions
        const fetchFunc = config.perpspec === 'okx-ohlcv' ? fetchOKXFinalLoop :
                         config.perpspec === 'byb-ohlcv' ? fetchBybitFinalLoop :
                         config.perpspec === 'bin-ohlcv' ? fetchBinanceFinalLoop :
                         config.fetch;

        let rawCandles = config.perpspec === 'okx-ohlcv'
          ? await fetchFunc(symbol, config, startTs, endTs)
          : await fetchFunc(symbol, config);

        if (rawCandles.length === 0) return;

        // Filter and process based on exchange
        if (config.perpspec === 'okx-ohlcv') {
          rawCandles = rawCandles.filter(c => c[8] === '1');
        }

        rawCandles = sortAndDeduplicateByTs(rawCandles);

        const processedData = (config.perpspec === 'bin-ohlcv') ? processBinanceData(rawCandles, baseSym, config) :
                            (config.perpspec === 'byb-ohlcv') ? processBybitData(rawCandles, baseSym, config) :
                            processOKXData(rawCandles, baseSym, config);

        if (processedData.length > 0) {
          await dbManager.insertData(config.perpspec, processedData);
        }
      } catch (error) {
        console.error(`❌ [${config.perpspec}] ${baseSym} recent fetch error: ${error.message}`);
      }
    }));
  }

  await Promise.all(recentPromises);
}

// ============================================================================
// FINAL LOOP MT - CREATE MOST RECENT MT RECORDS (Console only - no DB log)
// ============================================================================
async function createMTTokenFinalLoop() {
  try {
    console.log('ohlcv MT Final Loop');
    
    const now = Date.now();
    const startTs = now - (RECENT_RECORDS_COUNT * 60 * 1000); // hardcoded to 2 , to finish script

    // Fetch most recent data for all MT component symbols
    const mtData = await Promise.all(MT_SYMBOLS.map(async (sym) => {
      const query = `SELECT ts, o::numeric AS open, h::numeric AS high, l::numeric AS low, c::numeric AS close, v::numeric AS volume
                     FROM perp_data
                     WHERE symbol = $1 AND perpspec = $2 AND interval = $3 AND ts >= $4
                     ORDER BY ts ASC`;
      const result = await dbManager.pool.query(query, [sym, 'bin-ohlcv', '1m', startTs.toString()]);
      return result.rows.map(row => ({
        ts: BigInt(row.ts),
        open: parseFloat(row.open),
        high: parseFloat(row.high),
        low: parseFloat(row.low),
        close: parseFloat(row.close),
        volume: parseFloat(row.volume)
      })).filter(p => !isNaN(p.open) && !isNaN(p.high) &&
                     !isNaN(p.low) && !isNaN(p.close) &&
                     !isNaN(p.volume));
    }));

    if (mtData.some(data => data.length === 0)) {
      console.warn('⚠️  Missing recent data for some MT symbols—skipping final MT creation');
      return;
    }

    // Get all unique timestamps from recent data
    const allTs = [...new Set(mtData.flatMap(data => data.map(p => p.ts.toString())))].sort();
    
    const mtRecords = allTs.map(ts => {
      const tsBigInt = BigInt(ts);
      let totalO = 0, totalH = 0, totalL = 0, totalC = 0, totalV = 0;
      let count = 0;

      for (const data of mtData) {
        let lastValid = null;
        for (const p of data) {
          if (p.ts <= tsBigInt) lastValid = p;
          else break;
        }
        if (lastValid) {
          totalO += lastValid.open;
          totalH += lastValid.high;
          totalL += lastValid.low;
          totalC += lastValid.close;
          totalV += lastValid.volume;
          count++;
        }
      }

      if (count === 0) return null;

      return {
        ts: tsBigInt,
        symbol: MT_SYMBOL,
        source: 'bin-ohlcv',
        perpspec: 'bin-ohlcv',
        interval: '1m',
        o: totalO / count,
        h: totalH / count,
        l: totalL / count,
        c: totalC / count,
        v: totalV
      };
    }).filter(record => record !== null);

    if (mtRecords.length > 0) {
      await dbManager.insertData('bin-ohlcv', mtRecords);
      console.log(`${STATUS_LOG_COLOR}Final MT loop completed ${mtRecords.length} records${COLOR_RESET}`);
    } else {
      console.warn('⚠️  No MT records to create in final loop');
    }
  } catch (error) {
    console.error('❌ Final MT creation failed:', error.message);
  }
}

// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================
async function backfill() {
  const startTime = Date.now();
  const symbolCount = perpList.length;

  // ========================================================================
  // STATUS LOG #1: Script Started
  // ========================================================================
  await logStatus('started', `Starting ${SCRIPT_NAME} backfill for OHLCV data; ${symbolCount} symbols.`);

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