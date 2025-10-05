// SCRIPT: all-oi-h.js  5 Oct 2025
// Unified Open Interest Backfill Script for Binance, Bybit, and OKX
// Optimized for maximum speed - no DB checks, direct fetch + insert with ON CONFLICT DO NOTHING
// Special feature: Expands 5m API data to 5x 1m records for universal 1m database

const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = 'all-oi-h.js';
const DAYS = 10;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const NOW = Date.now();
const START = NOW - DAYS * MS_PER_DAY;

// ============================================================================
// SHARED UTILITIES
// ============================================================================

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================================
// EXCHANGE CONFIGURATIONS
// ============================================================================

const EXCHANGES = {
  BINANCE: {
    perpspec: 'bin-oi',
    source: 'bin-oi',
    url: 'https://fapi.binance.com/futures/data/openInterestHist',
    limit: 500,
    rateDelay: 100,
    concurrency: 5,
    timeout: 15000,
    apiInterval: '5m',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBinanceOI
  },
  BYBIT: {
    perpspec: 'byb-oi',
    source: 'byb-oi',
    url: 'https://api.bybit.com/v5/market/open-interest',
    limit: 200,
    rateDelay: 100,
    concurrency: 5,
    timeout: 15000,
    apiInterval: '5min',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBybitOI
  },
  OKX: {
    perpspec: 'okx-oi',
    source: 'okx-oi',
    url: 'https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-history',
    limit: 100,
    rateDelay: 250,
    concurrency: 2,
    timeout: 15000,
    apiInterval: '5m',
    dbInterval: '1m',
    mapSymbol: sym => `${sym}-USDT-SWAP`,
    fetch: fetchOkxOI
  }
};

// ============================================================================
// FETCH FUNCTIONS
// ============================================================================

//=================BINANCE========================================
async function fetchBinanceOI(symbol, config, startTs, endTs) {
  let allData = [];
  let current = startTs;

  while (current < endTs) {
    const nextEnd = Math.min(current + config.limit * 5 * 60 * 1000, endTs);
    const params = {
      symbol: symbol,
      period: config.apiInterval,
      limit: config.limit,
      startTime: current,
      endTime: nextEnd
    };

    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      const data = response.data;

      if (!data || data.length === 0) break;

      allData.push(...data);

      const lastTimestamp = data[data.length - 1].timestamp;
      current = lastTimestamp + 5 * 60 * 1000;

      if (data.length < config.limit) break;

      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
      throw error;
    }
  }

  // Filter by time range
  return allData.filter(rec => rec.timestamp >= startTs && rec.timestamp <= endTs);
}

//=================BYBIT========================================
async function fetchBybitOI(symbol, config, startTs, endTs) {
  let allData = [];
  let endTime = endTs;

  while (endTime > startTs) {
    const params = {
      category: 'linear',
      symbol: symbol,
      intervalTime: config.apiInterval,
      limit: config.limit,
      endTime: endTime
    };

    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      const data = response.data;

      if (data.retCode !== 0) {
        throw new Error(data.retMsg || 'Bybit API error');
      }

      const list = data.result?.list;
      if (!list || list.length === 0) break;

      allData.push(...list);

      const oldestTs = parseInt(list[list.length - 1].timestamp, 10);
      if (oldestTs <= startTs) break;

      endTime = oldestTs - 1;
      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
      throw error;
    }
  }

  // Filter by time range and sort
  const filtered = allData.filter(rec => {
    const ts = parseInt(rec.timestamp, 10);
    return ts >= startTs && ts <= endTs;
  });
  
  filtered.sort((a, b) => parseInt(a.timestamp, 10) - parseInt(b.timestamp, 10));
  return filtered;
}

//=================OKX========================================
async function fetchOkxOI(symbol, config, startTs, endTs) {
  let allData = [];
  let end = endTs;
  let lastOldest = null;

  while (true) {
    const params = {
      instId: symbol,
      period: config.apiInterval,
      limit: config.limit,
      end: end
    };

    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });

      // Surface HTTP errors
      if (response.status !== 200) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      // Surface API errors
      if (response.data.code !== '0') {
        throw new Error(`OKX API Error ${response.data.code}: ${response.data.msg}`);
      }

      const records = response.data.data;
      if (!records || records.length === 0) break;

      const oldestTs = parseInt(records[records.length - 1][0], 10);

      // Stop if cursor stuck (reached oldest available data)
      if (lastOldest === oldestTs) {
        break;
      }
      lastOldest = oldestTs;

      // Stop if reached start time
      if (oldestTs <= startTs) {
        const filtered = records.filter(rec => parseInt(rec[0], 10) >= startTs);
        allData.unshift(...filtered);
        break;
      }

      allData.unshift(...records);
      end = oldestTs;

      await sleep(config.rateDelay);

    } catch (error) {
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
      if (error.response) {
        console.error(`  HTTP Status: ${error.response.status}`);
        console.error(`  Response: ${JSON.stringify(error.response.data)}`);
      }
      throw error;
    }
  }

  // Filter by time range
  return allData.filter(rec => {
    const ts = parseInt(rec[0], 10);
    return ts >= startTs && ts <= endTs;
  });
}

// ============================================================================
// DATA PROCESSING FUNCTIONS
// ============================================================================

//=================BINANCE========================================
function processBinanceData(rawData, baseSymbol, config) {
  const expanded = [];

  for (const rec of rawData) {
    try {
      const ts = rec.timestamp;
      const oi = parseFloat(rec.sumOpenInterest);

      if (!isNaN(oi)) {
        // Expand 5m → 5x 1m records
        for (let i = 0; i < 5; i++) {
          expanded.push({
            ts: apiUtils.toMillis(BigInt(ts + i * 60 * 1000)),
            symbol: baseSymbol,
            source: config.source,
            perpspec: config.perpspec,
            interval: config.dbInterval,
            oi
          });
        }
      }
    } catch (e) {
      // Skip invalid records
    }
  }

  return expanded;
}

//=================BYBIT========================================
function processBybitData(rawData, baseSymbol, config) {
  const expanded = [];

  for (const rec of rawData) {
    try {
      const ts = parseInt(rec.timestamp, 10);
      const oi = parseFloat(rec.openInterest);

      if (!isNaN(oi)) {
        // Expand 5m → 5x 1m records
        for (let i = 0; i < 5; i++) {
          expanded.push({
            ts: apiUtils.toMillis(BigInt(ts + i * 60 * 1000)),
            symbol: baseSymbol,
            source: config.source,
            perpspec: config.perpspec,
            interval: config.dbInterval,
            oi
          });
        }
      }
    } catch (e) {
      // Skip invalid records
    }
  }

  return expanded;
}

//=================OKX========================================
function processOkxData(rawData, baseSymbol, config) {
  const expanded = [];

  for (const rec of rawData) {
    try {
      const ts = parseInt(rec[0], 10);
      const oi = parseFloat(rec[3]);

      if (!isNaN(oi)) {
        // Expand 5m → 5x 1m records
        for (let i = 0; i < 5; i++) {
          expanded.push({
            ts: apiUtils.toMillis(BigInt(ts + i * 60 * 1000)),
            symbol: baseSymbol,
            source: config.source,
            perpspec: config.perpspec,
            interval: config.dbInterval,
            oi
          });
        }
      }
    } catch (e) {
      // Skip invalid records
    }
  }

  return expanded;
}

// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================

async function backfill() {
  const startTime = Date.now();

  console.log(`\n🚀 Starting ${SCRIPT_NAME} backfill for Open Interest...`);

  const perpspecs = Object.values(EXCHANGES).map(ex => ex.perpspec).join(', ');

  // STATUS #1: Starting
  await apiUtils.logScriptStatus(
    dbManager,
    SCRIPT_NAME,
    '🚀started',
    `Starting ${SCRIPT_NAME} backfill for Open Interest for ${perpspecs}.`
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
    else if (exKey === 'OKX') processFunc = processOkxData;

    for (const baseSym of perpList) {
      promises.push(limit(async () => {
        const symbol = config.mapSymbol(baseSym);

        try {
          // STATUS #2: Log connected on first successful start
          if (!connectedLogged[config.perpspec]) {
            await apiUtils.logScriptStatus(
              dbManager,
              SCRIPT_NAME,
              'connected',
              `${config.perpspec} connected, starting fetch for ${baseSym}`
            );
            connectedLogged[config.perpspec] = true;
          }

          // Fetch 10 days of data
          const rawData = await config.fetch(symbol, config, START, NOW);

          if (rawData.length === 0) return;

          // Process data (includes 5m → 5x 1m expansion)
          const processedData = processFunc(rawData, baseSym, config);

          if (processedData.length === 0) return;

          // Insert to DB (ON CONFLICT DO NOTHING handles duplicates)
          await dbManager.insertData(config.perpspec, processedData);

          console.log(`✅ [${config.perpspec}] ${baseSym}: ${processedData.length} records`);

        } catch (error) {
          console.error(`❌ [${config.perpspec}] ${baseSym}: ${error.message}`);

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
            `${config.perpspec} error for ${baseSym}: ${error.message}`,
            { perpspec: config.perpspec, symbol: baseSym }
          );

          // Log internal error if connection never established
          if (!connectedLogged[config.perpspec]) {
            await apiUtils.logScriptError(
              dbManager,
              SCRIPT_NAME,
              'INTERNAL',
              'INSERT_FAILED',
              `${config.perpspec} failed to establish connection for ${baseSym}`,
              { perpspec: config.perpspec, symbol: baseSym }
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
  console.log(`\n🎉 All Open Interest backfills completed in ${duration}s!`);
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================

if (require.main === module) {
  backfill()
    .then(() => {
      console.log('✅ OI backfill script completed successfully');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 OI backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { backfill };