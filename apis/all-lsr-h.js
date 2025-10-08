// SCRIPT: all-lsr-h.js  6 Oct 2025
// Unified Long/Short Ratio Backfill Script for Binance, Bybit, and OKX
// Optimized for maximum speed with improved status logging

const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = 'all-lsr-h.js';
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

function floorToMinute(ts) {
  return Math.floor(ts / 60000) * 60000;
}

function normalizeNumeric(value) {
  if (value === null || value === undefined || value === '') return null;
  const parsed = parseFloat(value);
  return isNaN(parsed) ? null : parsed;
}

// ============================================================================
// EXCHANGE CONFIGURATIONS
// ============================================================================

const EXCHANGES = {
  BINANCE: {
    perpspec: 'bin-lsr',
    source: 'bin-lsr',
    url: 'https://fapi.binance.com/futures/data/globalLongShortAccountRatio',
    limit: 500,
    rateDelay: 25,  // 40 req/sec = 25ms delay
    concurrency: 10,
    timeout: 15000,
    apiInterval: '5m',
    dbInterval: '1m',
    apiCandlesTarget: DAYS * 288,  // 288 x 5min candles per day
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBinanceLSR,
    process: processBinanceData
  },
  BYBIT: {
    perpspec: 'byb-lsr',
    source: 'byb-lsr',
    url: 'https://api.bybit.com/v5/market/account-ratio',
    limit: 500,
    rateDelay: 8,  // 120 req/sec = ~8ms delay
    concurrency: 10,
    timeout: 10000,
    apiInterval: '5min',
    dbInterval: '1m',
    apiCandlesTarget: DAYS * 288,
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBybitLSR,
    process: processBybitData
  },
  OKX: {
    perpspec: 'okx-lsr',
    source: 'okx-lsr',
    url: 'https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio-contract',
    limit: 100,
    rateDelay: 400,  // 2.5 req/sec = 400ms delay
    concurrency: 5,
    timeout: 8000,
    apiInterval: '5m',
    dbInterval: '1m',
    apiCandlesTarget: 5 * 288,  // OKX only returns 5 days max
    mapSymbol: sym => `${sym}-USDT-SWAP`,
    fetch: fetchOKXLSR,
    process: processOKXData
  }
};

// ============================================================================
// FETCH FUNCTIONS
// ============================================================================
//=================BINANCE========================================
async function fetchBinanceLSR(symbol, config, startTs, endTs) {
  let allData = [];
  let currentStart = floorToMinute(startTs);
  const flooredEnd = floorToMinute(endTs);

  if (flooredEnd <= currentStart) {
    console.error(`[${config.perpspec}] Invalid interval: start >= end`);
    return [];
  }

  while (currentStart < flooredEnd) {
    // FIX: Add endTime parameter to properly limit the time range
    const nextEnd = Math.min(currentStart + config.limit * 5 * 60 * 1000, flooredEnd);
    
    const params = {
      symbol: symbol,
      period: config.apiInterval,
      limit: config.limit,
      startTime: currentStart,
      endTime: nextEnd  // ADD THIS LINE
    };

    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      const data = response.data;

      if (!data || data.length === 0) break;

      allData.push(...data);

      const lastTimestamp = data[data.length - 1].timestamp;
      currentStart = lastTimestamp + 5 * 60 * 1000;

      if (data.length < config.limit) break;

      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
      throw error;
    }
  }

  return allData.filter(d => d.timestamp >= startTs && d.timestamp <= endTs);
}

//=================BYBIT========================================
async function fetchBybitLSR(symbol, config, startTs, endTs) {
  let allData = [];
  let currentEnd = endTs;
  const totalRequests = Math.ceil(config.apiCandlesTarget / config.limit);

  for (let i = 0; i < totalRequests; i++) {
    const params = {
      category: 'linear',
      symbol: symbol,
      period: config.apiInterval,
      limit: config.limit,
      endTime: currentEnd
    };

    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      const data = response.data;
      const list = data.result?.list;

      if (!list || list.length === 0) break;

      const newData = list.filter(d => parseInt(d.timestamp, 10) < currentEnd);
      allData.unshift(...newData);

      const oldestTimestamp = parseInt(list[list.length - 1].timestamp, 10);
      currentEnd = oldestTimestamp - 1;

      if (allData.length >= config.apiCandlesTarget) break;

      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
      throw error;
    }
  }

  return allData.filter(d => {
    const ts = parseInt(d.timestamp, 10);
    return ts >= startTs && ts <= endTs;
  });
}

//=================OKX========================================
async function fetchOKXLSR(symbol, config, startTs, endTs) {
  const allData = [];
  const seenTimestamps = new Set();
  
  let currentEnd = endTs;  // CHANGE: Use currentEnd instead of currentBefore
  let hasMoreData = true;
  let zeroNewCount = 0;

  while (hasMoreData && currentEnd > startTs) {
    const params = {
      instId: symbol,
      period: config.apiInterval,
      end: currentEnd.toString(),  // CHANGE: Use 'end' instead of 'before'
      limit: config.limit
    };

    try {
      const response = await axios.get(config.url, { 
        params, 
        timeout: config.timeout
      });

      if (response.data.code !== '0') break;

      const records = response.data.data || [];
      
      if (records.length === 0) break;

      let newRecordsCount = 0;
      let oldestTimestamp = currentEnd;

      for (const record of records) {
        const ts = parseInt(record[0]);  // timestamp is first element in array
        if (seenTimestamps.has(ts) || ts < startTs || ts > endTs) continue;
        
        seenTimestamps.add(ts);
        allData.push(record);
        newRecordsCount++;
        
        if (ts < oldestTimestamp) oldestTimestamp = ts;
      }

      // Fast exit on duplicates
      if (newRecordsCount === 0) {
        zeroNewCount++;
        if (zeroNewCount >= 2) break;
      } else {
        zeroNewCount = 0;
      }

      if (oldestTimestamp <= startTs) break;
      if (records.length < config.limit) break;

      currentEnd = oldestTimestamp - 1;  // This was correct
      await sleep(config.rateDelay);

    } catch (error) {
      if (error.response?.status === 429) {
        await sleep(1000);
        continue;
      }
      throw error;
    }
  }

  return allData;
}

// ============================================================================
// DATA PROCESSING FUNCTIONS
// ============================================================================

//=================BINANCE========================================
function processBinanceData(rawData, baseSymbol, config) {
  const perpspec = config.perpspec;
  const result = [];

  for (const dataPoint of rawData) {
    try {
      const timestamp = dataPoint.timestamp;
      const lsr = parseFloat(dataPoint.longShortRatio);

      if (!isNaN(lsr)) {
        // Create 5 x 1m records from each 5m candle
        for (let i = 0; i < 5; i++) {
          const minuteTs = timestamp + (i * 60 * 1000);
          result.push({
            ts: apiUtils.toMillis(BigInt(minuteTs)),
            symbol: baseSymbol,
            source: perpspec,
            perpspec,
            interval: config.dbInterval,
            lsr
          });
        }
      }
    } catch (e) {
      // Skip invalid records
    }
  }

  return result;
}

//=================BYBIT========================================
function processBybitData(rawData, baseSymbol, config) {
  const perpspec = config.perpspec;
  const result = [];

  for (const dataPoint of rawData) {
    try {
      const timestamp = parseInt(dataPoint.timestamp, 10);
      const buyRatio = parseFloat(dataPoint.buyRatio);
      const sellRatio = parseFloat(dataPoint.sellRatio);

      // Calculate LSR: buyRatio / sellRatio
      if (!isNaN(buyRatio) && !isNaN(sellRatio) && sellRatio !== 0) {
        const lsr = buyRatio / sellRatio;

        // Create 5 x 1m records from each 5m candle
        for (let i = 0; i < 5; i++) {
          const minuteTs = timestamp + (i * 60 * 1000);
          result.push({
            ts: apiUtils.toMillis(BigInt(minuteTs)),
            symbol: baseSymbol,
            source: perpspec,
            perpspec,
            interval: config.dbInterval,
            lsr
          });
        }
      }
    } catch (e) {
      // Skip invalid records
    }
  }

  return result;
}

//=================OKX========================================
function processOKXData(rawData, baseSymbol, config) {
  const perpspec = config.perpspec;
  const result = [];

  for (const record of rawData) {
    const timestamp = parseInt(record[0]);
    const lsr = normalizeNumeric(record[1]);

    if (lsr !== null && !isNaN(timestamp)) {
      // Create 5 x 1m records from each 5m candle
      for (let i = 0; i < 5; i++) {
        const minuteTs = timestamp + (i * 60 * 1000);
        result.push({
          ts: apiUtils.toMillis(BigInt(minuteTs)),
          symbol: baseSymbol,
          source: perpspec,
          perpspec,
          interval: config.dbInterval,
          lsr
        });
      }
    }
  }

  return result;
}

// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================

async function backfill() {
  const startTime = Date.now();
  
  console.log(`\n🚀 Starting ${SCRIPT_NAME} backfill for Long/Short Ratios...`);

  const perpspecs = Object.values(EXCHANGES).map(ex => ex.perpspec).join(', ');
  
  // STATUS #1: Starting
  await apiUtils.logScriptStatus(
    dbManager,
    SCRIPT_NAME,
    'started',
    `Starting ${SCRIPT_NAME} backfill for Long/Short Ratios for ${perpspecs}.`
  );

  await apiUtils.logScriptStatus(
    dbManager,
    SCRIPT_NAME,
    'running',
    `${SCRIPT_NAME} backfill in progress...`
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
              `${config.perpspec} starting fetch`
            );
            connectedLogged[config.perpspec] = true;
          }

          // Fetch data
          const intervalStart = floorToMinute(START);
          const intervalEnd = floorToMinute(NOW);
          
          const rawData = await config.fetch(symbol, config, intervalStart, intervalEnd);

          if (rawData.length === 0) return;

          // Process data
          const processedData = config.process(rawData, baseSym, config);
          
          if (processedData.length === 0) return;

          // Insert to DB
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
  console.log(`\n🎉 All Long/Short Ratio backfills completed in ${duration}s!`);
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================

if (require.main === module) {
  backfill()
    .then(() => {
      console.log('✅ LSR backfill script completed successfully');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 LSR backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { backfill };