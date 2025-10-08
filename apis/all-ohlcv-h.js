// SCRIPT: all-ohlcv-h.js 5 OCt 2025
// Unified OHLCV Backfill Script for Binance, Bybit, and OKX
// Optimized for maximum speed - no DB checks, no schema validation
// Direct fetch → process → insert with ON CONFLICT DO NOTHING
// MT Creation: Average closes from MT_SYMBOLS, insert as 'MT' under 'bin-ohlcv'
// Simplified logging: Success/error only

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

// Retry utility for rate limits
async function fetchWithRetry(url, options, maxRetries = 1) {
  for (let attempt = 1; attempt <= maxRetries + 1; attempt++) {
    try {
      return await axios.get(url, options);
    } catch (error) {
      if (error.response?.status === 429 && attempt <= maxRetries) {
        await sleep(200); // Short sleep for limit
      } else {
        throw error;
      }
    }
  }
}

// Target old ts (now - 10 days)
function getTargetOldTs() {
  return Date.now() - DAYS_TO_FETCH * 24 * 60 * 60 * 1000;
}

// ============================================================================
// EXCHANGE CONFIGURATIONS
// ⚡ SPEED SETTINGS: OKX concurrency=6 (faster bursts under limit)
const EXCHANGES = {
  BINANCE: {
    perpspec: 'bin-ohlcv',
    source: 'bin-ohlcv',
    url: 'https://fapi.binance.com/fapi/v1/klines',
    limit: 1000,              // Max 1000
    rateDelay: 200,           // Delay between requests
    concurrency: 3,           // Parallel symbols
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
    limit: 1000,              // Max 1000
    rateDelay: 200,           // Delay between requests
    concurrency: 3,           // Parallel symbols
    timeout: 10000,
    apiInterval: '1',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBybitOHLCV
  },
  OKX: {
    perpspec: 'okx-ohlcv',
    source: 'okx-ohlcv',
    url: 'https://www.okx.com/api/v5/market/history-mark-price-candles',
    limit: 100,               // Max 100
    rateDelay: 100,           // Delay between requests (under 20/2s limit)
    concurrency: 6,           // Parallel symbols (6 bursts ~6/sec, under 10/sec)
    timeout: 10000,
    apiInterval: '1m',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}-USDT-SWAP`,
    fetch: fetchOKXOHLCV
  }
};

// ============================================================================
// MT CONFIGURATION
// ============================================================================

const MT_SYMBOLS = ['ETH', 'BTC', 'DOGE', 'XRP', 'SOL']; // Dynamic: Add/remove symbols here; avg = sum / length
const MT_SYMBOL = 'MT'; // Insert as this symbol under 'bin-ohlcv'

// ============================================================================
// FETCH FUNCTIONS (OKX Optimized: Prepend, after = oldest - 1ms, early break)
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
      console.error(`❌ [${config.perpspec}] ${symbol}: ${error.message}`);
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
  let after = null; // Initial: null for latest 100

  const targetOldTs = getTargetOldTs(); // Now - 10 days

  for (let i = 0; i < totalRequests; i++) {
    let url = `${config.url}?instId=${symbol}&bar=${config.apiInterval}&limit=${config.limit}`;
    if (after !== null) url += `&after=${after}`; // After for earlier than ts

    try {
      const response = await fetchWithRetry(url, { timeout: config.timeout }, 1);
      const data = response.data;

      if (data.code !== '0' || !data.data || data.data.length === 0) {
        console.error(`❌ [${config.perpspec}] ${symbol}: Empty response (code: ${data.code || 'unknown'})`);
        break;
      }

      // Prepend (response is newest first; prepend for backward chronological build)
      allCandles = [...data.data, ...allCandles];

      // Update after to oldest ts from this batch - 1ms (for next earlier batch)
      const oldestTs = parseInt(data.data[data.data.length - 1][0]) - 1;
      after = oldestTs;

      // Break if batch < limit or oldest < target old
      if (data.data.length < config.limit || oldestTs < targetOldTs) {
        break;
      }

      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`❌ [${config.perpspec}] ${symbol}: ${error.message}`);
      if (error.response?.status === 429) {
        await sleep(200);
        i--; // Retry
      } else {
        throw error;
      }
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
// MT CREATION FUNCTION
// ============================================================================
// MT Creation: Average 1m closes across MT_SYMBOLS (dynamic count), insert as symbol='MT' under 'bin-ohlcv'
async function createMTToken() {
  try {
    console.log(`Creating MT token from ${MT_SYMBOLS.length} symbols...`);
    const numSymbols = MT_SYMBOLS.length; // Dynamic count for avg

    // Fetch 1m data for all MT symbols (reuse query style)
    const mtData = await Promise.all(MT_SYMBOLS.map(async (sym) => {
      const query = `SELECT ts, c::numeric AS close FROM perp_data WHERE symbol = $1 AND perpspec = $2 AND interval = $3 ORDER BY ts ASC`;
      const result = await dbManager.pool.query(query, [sym, 'bin-ohlcv', '1m']); // Assume bin-ohlcv schema
      return result.rows.map(row => {
        const ts = Number(apiUtils.toMillis(BigInt(row.ts))); // Convert BigInt to Number for JS ops
        const close = parseFloat(row.close);
        return { ts, close };
      }).filter(p => !isNaN(p.ts) && !isNaN(p.close) && isFinite(p.close));
    }));

    if (mtData.some(data => data.length === 0)) {
      console.error('Missing data for some MT symbols—skipping MT creation');
      return;
    }

    // Align by unique ts across all MT data (union ts, forward-fill per symbol)
    const allTs = [...new Set(mtData.flatMap(data => data.map(p => p.ts)))].sort((a, b) => a - b); // Numbers now
    const mtRecords = allTs.map(ts => {
      let totalClose = 0, count = 0;
      for (const data of mtData) {
        // Find closest <= ts (forward-fill)
        let lastValidClose = null;
        for (const p of data) {
          if (p.ts <= ts) lastValidClose = p.close;
          else break;
        }
        if (lastValidClose !== null) {
          totalClose += lastValidClose;
          count++;
        }
      }
      const avgClose = count > 0 ? totalClose / count : null; // Dynamic avg
      if (avgClose === null) return null; // Skip if no valid data

      // Insert as OHLCV (o=h=l=c=avg, v=0 or sum if you fetch volume)
      return {
        ts: BigInt(Math.round(ts)), // Back to BigInt for DB insert
        symbol: MT_SYMBOL,
        source: 'bin-ohlcv', // Match schema
        perpspec: 'bin-ohlcv',
        interval: '1m',
        o: avgClose, h: avgClose, l: avgClose, c: avgClose, v: 0 // v=0 or sum if you fetch volume
      };
    }).filter(record => record !== null);

    if (mtRecords.length === 0) {
      console.error('No MT records generated—skipping insert');
      return;
    }

    // Bulk insert with ON CONFLICT DO NOTHING (fills gaps, skips duplicates)
    await dbManager.insertData('bin-ohlcv', mtRecords);
    console.log(`✅ Created ${mtRecords.length} MT records under 'bin-ohlcv'`);
  } catch (error) {
    console.error('❌ MT creation failed:', error.message);
    // Optional: Log via apiUtils if needed
  }
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
    'started',
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
                           error.response?.status === 400 || error.response?.status === 404 ? 'INVALID_SYMBOL' : 'FETCH_ERROR';

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

  // Create MT token after all individual backfills (ensures data loaded)
  await createMTToken();

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