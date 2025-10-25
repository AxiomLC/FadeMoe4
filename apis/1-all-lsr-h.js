// SCRIPT: all-lsr-h.js  22 Oct 2025
// Revised Unified Long/Short Ratio Backfill Script for Binance, Bybit, and OKX
// - Uses unified **insertBackfillData with merged rows
// - Improved status logging with connection verification and heartbeat
// - Drops interval/source fields per new schema

const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');
const weightMonitor = require('../b-weight');

const SCRIPT_NAME = 'all-lsr-h.js';
const DAYS = 10;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const NOW = Date.now();
const START = NOW - DAYS * MS_PER_DAY;

const STATUS_COLOR = '\x1b[36m'; // Cyan/light blue
const RESET = '\x1b[0m';

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
    url: 'https://fapi.binance.com/futures/data/globalLongShortAccountRatio',
    limit: 500,
    rateDelay: 200,
    concurrency: 5,
    timeout: 8000,
    apiInterval: '5m',
    apiCandlesTarget: DAYS * 288,
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBinanceLSR,
    process: processBinanceData
  },
  BYBIT: {
    perpspec: 'byb-lsr',
    url: 'https://api.bybit.com/v5/market/account-ratio',
    limit: 500,
    rateDelay: 100,
    concurrency: 8,
    timeout: 8000,
    apiInterval: '5min',
    apiCandlesTarget: DAYS * 288,
    mapSymbol: sym => `${sym}USDT`,
    fetch: fetchBybitLSR,
    process: processBybitData
  },
  OKX: {
    perpspec: 'okx-lsr',
    url: 'https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio-contract',
    limit: 100,
    rateDelay: 300,
    concurrency: 6,
    timeout: 8000,
    apiInterval: '5m',
    apiCandlesTarget: 5 * 288,
    mapSymbol: sym => `${sym}-USDT-SWAP`,
    fetch: fetchOKXLSR,
    process: processOKXData
  }
};

// ============================================================================
// FETCH FUNCTIONS
// ============================================================================

async function fetchBinanceLSR(symbol, config, startTs, endTs) {
  let allData = [];
  let currentStart = floorToMinute(startTs);
  const flooredEnd = floorToMinute(endTs);

  if (flooredEnd <= currentStart) {
    console.error(`[${config.perpspec}] Invalid interval: start >= end`);
    return [];
  }

  while (currentStart < flooredEnd) {
    const nextEnd = Math.min(currentStart + config.limit * 5 * 60 * 1000, flooredEnd);
    const params = {
      symbol: symbol,
      period: config.apiInterval,
      limit: config.limit,
      startTime: currentStart,
      endTime: nextEnd
    };
    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      weightMonitor.logRequest('bin-lsr', '/futures/data/globalLongShortAccountRatio', 1);
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

async function fetchOKXLSR(symbol, config, startTs, endTs) {
  const allData = [];
  const seenTimestamps = new Set();
  let currentEnd = endTs;
  let hasMoreData = true;
  let zeroNewCount = 0;

  while (hasMoreData && currentEnd > startTs) {
    const params = {
      instId: symbol,
      period: config.apiInterval,
      end: currentEnd.toString(),
      limit: config.limit
    };

    try {
      const response = await axios.get(config.url, { params, timeout: config.timeout });
      if (response.data.code !== '0') break;
      const records = response.data.data || [];
      if (records.length === 0) break;
      let newRecordsCount = 0;
      let oldestTimestamp = currentEnd;
      for (const record of records) {
        const ts = parseInt(record[0]);
        if (seenTimestamps.has(ts) || ts < startTs || ts > endTs) continue;
        seenTimestamps.add(ts);
        allData.push(record);
        newRecordsCount++;
        if (ts < oldestTimestamp) oldestTimestamp = ts;
      }
      if (newRecordsCount === 0) {
        zeroNewCount++;
        if (zeroNewCount >= 2) break;
      } else {
        zeroNewCount = 0;
      }
      if (oldestTimestamp <= startTs) break;
      if (records.length < config.limit) break;
      currentEnd = oldestTimestamp - 1;
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
// ========================  Binance  =========================================
function processBinanceData(rawData, baseSymbol, config) {
  const perpspec = config.perpspec;
  const result = [];
  const now = Date.now();
  const floorNow = Math.floor(now / 60000) * 60000;
  const nextMinute = floorNow + 60000;

  for (const dataPoint of rawData) {
    try {
      const timestamp = dataPoint.timestamp;
      const lsr = parseFloat(dataPoint.longShortRatio);
      if (!isNaN(lsr)) {
        for (let i = 0; i < 5; i++) {
          const minuteTs = timestamp + (i * 60 * 1000);
          // Only include timestamps up to the next minute after now
          if (minuteTs <= nextMinute) {
            result.push({
              ts: apiUtils.toMillis(BigInt(minuteTs)),
              symbol: baseSymbol,
              perpspec,
              lsr
            });
          }
          // else skip future timestamps
        }
      }
    } catch {}
  }
  return result;
}
//======================= bybit =============================
function processBybitData(rawData, baseSymbol, config) {
  const perpspec = config.perpspec;
  const result = [];
  const now = Date.now();
  const floorNow = Math.floor(now / 60000) * 60000;
  const nextMinute = floorNow + 60000;

  for (const dataPoint of rawData) {
    try {
      const timestamp = parseInt(dataPoint.timestamp, 10);
      const buyRatio = parseFloat(dataPoint.buyRatio);
      const sellRatio = parseFloat(dataPoint.sellRatio);
      if (!isNaN(buyRatio) && !isNaN(sellRatio) && sellRatio !== 0) {
        const lsr = buyRatio / sellRatio;
        for (let i = 0; i < 5; i++) {
          const minuteTs = timestamp + (i * 60 * 1000);
          if (minuteTs <= nextMinute) {
            result.push({
              ts: apiUtils.toMillis(BigInt(minuteTs)),
              symbol: baseSymbol,
              perpspec,
              lsr
            });
          }
        }
      }
    } catch {}
  }
  return result;
}
//=============  OKX  ==================================
function processOKXData(rawData, baseSymbol, config) {
  const perpspec = config.perpspec;
  const result = [];
  const now = Date.now();
  const floorNow = Math.floor(now / 60000) * 60000;
  const nextMinute = floorNow + 60000;

  let lastLsr = null;
  let lastTimestamp = null;

  // Process raw data into 1-minute intervals
  for (const record of rawData) {
    const timestamp = parseInt(record[0]);
    const lsr = normalizeNumeric(record[1]);
    if (lsr !== null && !isNaN(timestamp)) {
      for (let i = 0; i < 5; i++) {
        const minuteTs = timestamp + (i * 60 * 1000);
        if (minuteTs <= nextMinute) {
          result.push({
            ts: apiUtils.toMillis(BigInt(minuteTs)),
            symbol: baseSymbol,
            perpspec,
            lsr
          });
          lastLsr = lsr;
          lastTimestamp = minuteTs;
        }
      }
    }
  }

  // Fill missing minutes up to nextMinute by repeating last known LSR
  if (lastTimestamp !== null && lastTimestamp < nextMinute && lastLsr !== null) {
    let fillTs = lastTimestamp + 60000;
    while (fillTs <= nextMinute) {
      result.push({
        ts: apiUtils.toMillis(BigInt(fillTs)),
        symbol: baseSymbol,
        perpspec,
        lsr: lastLsr
      });
      fillTs += 60000;
    }
  }

  return result;
}


// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================

async function backfill() {
  const startTime = Date.now();
  const totalSymbols = perpList.length;
  const perpspecs = Object.values(EXCHANGES).map(ex => ex.perpspec);
  const perpspecsStr = perpspecs.join(', ');

  // #1 STATUS: started ////// STATUSES ==================================
  const message1 = `Starting ${SCRIPT_NAME} backfill for Long/Short Ratios; ${totalSymbols} symbols.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', message1);
  console.log(`${STATUS_COLOR}${message1}${RESET}`);

  // Track connection and completion
  const connectedPerpspecs = new Set();
  const completedSymbolsPerPerpspec = {};
  const completedPerpspecs = new Set();
  for (const p of perpspecs) {
    completedSymbolsPerPerpspec[p] = new Set();
  }

  // #2 STATUS: connected when all perpspecs connected
  let connectedLogged = false;

  // #3 STATUS: heartbeat running logs per perpspec if not completed
  let stopHeartbeat = false;
  const heartbeatInterval = setInterval(async () => {
    if (stopHeartbeat) return;
    for (const p of perpspecs) {
      if (!completedPerpspecs.has(p)) {
        const msg = `${p} backfilling db.`;
        try {
          await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', msg);
        } catch (err) {
          console.error(`[heartbeat] DB log failed for ${p}:`, err.message);
        }
        console.log(`${STATUS_COLOR}${msg}${RESET}`);
      }
    }
  }, 20000);

  //  ///////////////// PROCESSING ====================================
  // Parallel processing per symbol
  const limiters = {};
  for (const exKey of Object.keys(EXCHANGES)) {
    limiters[exKey] = pLimit(EXCHANGES[exKey].concurrency);
  }

  const promises = perpList.map(baseSym => (async () => {
    const allData = [];
    const connectedExchanges = new Set();

    for (const exKey of Object.keys(EXCHANGES)) {
      const config = EXCHANGES[exKey];
      const symbol = config.mapSymbol(baseSym);

      try {
        if (!connectedPerpspecs.has(config.perpspec)) {
          connectedPerpspecs.add(config.perpspec);
          if (connectedPerpspecs.size === perpspecs.length && !connectedLogged) {
            const connectedMsg = `${perpspecsStr} connected, starting fetch.`;
            await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', connectedMsg);
            console.log(`${STATUS_COLOR}🔧 ${connectedMsg}${RESET}`);
            connectedLogged = true;
          }
        }

        const rawData = await config.fetch(symbol, config, START, NOW);

        if (rawData.length > 0) {
          const processed = config.process(rawData, baseSym, config);
          allData.push(...processed);
          connectedExchanges.add(config.perpspec);
        }

        completedSymbolsPerPerpspec[config.perpspec].add(baseSym);

        const expectedCount = perpList.length;
        if (completedSymbolsPerPerpspec[config.perpspec].size === expectedCount && !completedPerpspecs.has(config.perpspec)) {
          const completeMsg = `${config.perpspec} backfill complete.`;
          await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', completeMsg);
          console.log(`${STATUS_COLOR}🔧 ${completeMsg}${RESET}`);
          completedPerpspecs.add(config.perpspec);
        }
      } catch (err) {
        console.error(`❌ [${config.perpspec}] ${baseSym}: ${err.message}`);
        const errorCode = err.response?.status === 429 ? 'RATE_LIMIT' :
          err.message.includes('timeout') ? 'TIMEOUT' :
          err.message.includes('404') ? 'NOT_FOUND' : 'FETCH_ERROR';
        await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', errorCode, `${config.perpspec} error for ${baseSym}: ${err.message}`, { perpspec: config.perpspec, symbol: baseSym });
        if (!connectedPerpspecs.has(config.perpspec)) {
          await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'INTERNAL', 'INSERT_FAILED', `${config.perpspec} failed to establish connection for ${baseSym}`, { perpspec: config.perpspec, symbol: baseSym });
        }
      }
    }

    if (allData.length > 0) {
      await dbManager.insertBackfillData(allData);
    } else {
      console.warn(`⚠️ No LSR data for ${baseSym} across exchanges.`);
    }
  })());

  await Promise.all(promises);

  clearInterval(heartbeatInterval);

  // Single completion log only
  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  const finalMsg = `⏱️  ${SCRIPT_NAME} backfill completed in ${duration}s!`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', finalMsg);
  console.log(finalMsg);
}

// ... rest of existing code ...


if (require.main === module) {
  backfill()
    .then(() => process.exit(0))
    .catch(err => {
      console.error('💥 LSR backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { backfill };


