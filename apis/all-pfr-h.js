// SCRIPT: all-pfr-h.js  5 Oct 2025
// Unified Premium Funding Rate Backfill Script for Binance, Bybit, and OKX
// Optimized for maximum speed with improved status logging

const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = 'all-pfr-h.js';
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
    perpspec: 'bin-pfr',
    source: 'bin-pfr',
    url: 'https://fapi.binance.com/fapi/v1/premiumIndexKlines',
    limit: 500,
    rateDelay: 50,
    concurrency: 10,
    timeout: 15000,
    apiInterval: '1m',
    dbInterval: '1m',
    apiCandlesTarget: DAYS * 1440,
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBinancePFR,
    process: processStandardData
  },
  BYBIT: {
    perpspec: 'byb-pfr',
    source: 'byb-pfr',
    url: 'https://api.bybit.com/v5/market/premium-index-price-kline',
    limit: 200,
    rateDelay: 50,
    concurrency: 10,
    timeout: 10000,
    apiInterval: '1',
    dbInterval: '1m',
    apiCandlesTarget: DAYS * 1440,
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBybitPFR,
    process: processStandardData
  },
  OKX: {
    perpspec: 'okx-pfr',
    source: 'okx-pfr',
    url: 'https://www.okx.com/api/v5/public/premium-history',
    limit: 100,
    rateDelay: 50,
    concurrency: 20,
    timeout: 8000,
    dbInterval: '1m',
    apiCandlesTarget: DAYS * 1440,
    mapSymbol: sym => `${sym}-USDT-SWAP`,
    fetch: fetchOKXPremium,
    process: processOKXData
  }
};

// ============================================================================
// FETCH FUNCTIONS
// ============================================================================

//=================BINANCE========================================
async function fetchBinancePFR(symbol, config, startTs, endTs) {
  let allData = [];
  let currentStart = floorToMinute(startTs);
  const flooredEnd = floorToMinute(endTs);

  if (flooredEnd <= currentStart) {
    console.error(`[${config.perpspec}] Invalid interval: start >= end`);
    return [];
  }

  while (currentStart < flooredEnd) {
    const params = {
      symbol: symbol,
      interval: config.apiInterval,
      limit: config.limit,
      startTime: currentStart,
      endTime: flooredEnd
    };

    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      const data = response.data;

      if (!data || data.length === 0) break;

      allData.push(...data);

      const lastTimestamp = data[data.length - 1][0];
      currentStart = lastTimestamp + 60 * 1000;

      if (data.length < config.limit) break;

      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
      throw error;
    }
  }

  return allData.filter(d => d[0] >= startTs && d[0] <= endTs);
}

//=================BYBIT========================================
async function fetchBybitPFR(symbol, config, startTs, endTs) {
  let allData = [];
  let end = endTs;
  const totalRequests = Math.ceil(config.apiCandlesTarget / config.limit);

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
      const list = data.result?.list;

      if (!list || list.length === 0) break;

      const newData = list.filter(d => parseInt(d[0], 10) < end);
      allData.unshift(...newData);

      const oldestTimestamp = parseInt(list[list.length - 1][0], 10);
      end = oldestTimestamp - 1;

      if (allData.length >= config.apiCandlesTarget) break;

      await sleep(config.rateDelay);
    } catch (error) {
      console.error(`[${config.perpspec}] Fetch error for ${symbol}:`, error.message);
      throw error;
    }
  }

  return allData.filter(d => {
    const ts = parseInt(d[0], 10);
    return ts >= startTs && ts <= endTs;
  });
}

//=================OKX========================================
async function fetchOKXPremium(symbol, config, startTs, endTs) {
  const allData = [];
  const seenTimestamps = new Set();
  
  let currentAfter = endTs;
  let hasMoreData = true;
  let zeroNewCount = 0;

  while (hasMoreData && currentAfter > startTs) {
    const params = {
      instId: symbol,
      after: currentAfter.toString(),
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
      let oldestTimestamp = currentAfter;

      for (const record of records) {
        const ts = parseInt(record.ts);
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

      currentAfter = oldestTimestamp - 1;
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

//=================STANDARD (BINANCE & BYBIT)========================================
function processStandardData(rawData, baseSymbol, config) {
  const perpspec = config.perpspec;
  const result = [];

  for (const dataPoint of rawData) {
    try {
      const timestamp = dataPoint[0];
      const pfr = parseFloat(dataPoint[4]);

      if (!isNaN(pfr)) {
        result.push({
          ts: apiUtils.toMillis(BigInt(timestamp)),
          symbol: baseSymbol,
          source: perpspec,
          perpspec,
          interval: config.dbInterval,
          pfr
        });
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
    const timestamp = parseInt(record.ts);
    const premium = normalizeNumeric(record.premium);

    if (premium !== null && !isNaN(timestamp)) {
      result.push({
        ts: apiUtils.toMillis(BigInt(timestamp)),
        symbol: baseSymbol,
        source: perpspec,
        perpspec,
        interval: config.dbInterval,
        pfr: premium
      });
    }
  }

  return result;
}

// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================

async function backfill() {
  const startTime = Date.now();
  
  console.log(`\n🚀 Starting ${SCRIPT_NAME} backfill for Premium Funding Rates...`);

  const perpspecs = Object.values(EXCHANGES).map(ex => ex.perpspec).join(', ');
  
  // STATUS #1: Starting
  await apiUtils.logScriptStatus(
    dbManager, 
    SCRIPT_NAME, 
    '🚀started', 
    `Starting ${SCRIPT_NAME} backfill for Premium Funding Rates for ${perpspecs}.`
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
              `${config.perpspec} connected, starting fetch for ${baseSym}`
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
  console.log(`\n🎉 All Premium Funding Rate backfills completed in ${duration}s!`);
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================

if (require.main === module) {
  backfill()
    .then(() => {
      console.log('✅ PFR backfill script completed successfully');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 PFR backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { backfill };