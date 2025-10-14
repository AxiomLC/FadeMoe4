// apis/bin-tv-h.js
// SCRIPT: bin-tv-h.js (Binance Taker Buy/Sell Volume Backfill + Streamlined Status Logging + ANSI Color)
// Updated: 14 Oct 2025
// Binance Taker Buy/Sell Volume Backfill Script (Improved Distribution)
// OKX and Bybit omitted (no historical taker volume API available)
// Optimized for maximum speed
// Distributes 5m tbv/tsv into 1m slots based on 1m OHLCV volume and price direction
// Uses DB query for bin-ohlcv to get v and c for weighting
// On conflict do nothing for inserts (idempotent backfill)
// Fixes: Dedup OHLCV rows by ts to handle duplicates; ignore last 10min for warnings

const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = 'bin-tv-h.js';
const STATUS_COLOR = '\x1b[94m'; // Light blue for status logs
const RESET = '\x1b[0m'; // Reset console color
const DAYS = 10;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const NOW = Date.now();
const START = NOW - DAYS * MS_PER_DAY;
const RECENT_THRESHOLD = NOW - 10 * 60 * 1000;  // Ignore warnings for last 10min windows
const weightMonitor = require('../b-weight');

// ============================================================================
// SHARED UTILITIES
// ============================================================================
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function floorToMinute(ts) {
  return Math.floor(ts / 60000) * 60000;
}

// ============================================================================
// BINANCE CONFIGURATION
// ============================================================================
const BINANCE = {
  perpspec: 'bin-tv',
  source: 'bin-tv',
  url: 'https://fapi.binance.com/futures/data/takerlongshortRatio',
  limit: 500,
  rateDelay: 100,  // 40 req/sec = 25ms delay
  concurrency: 8,
  timeout: 15000,
  apiInterval: '5m',
  dbInterval: '1m',
  mapSymbol: sym => `${sym}USDT`
};

// ============================================================================
// BINANCE FETCH FUNCTION (UNCHANGED)
// ============================================================================
async function fetchBinanceTV(symbol, startTs, endTs) {
  let allData = [];
  let currentStart = floorToMinute(startTs);
  const flooredEnd = floorToMinute(endTs);

  if (flooredEnd <= currentStart) {
    console.error(`[${BINANCE.perpspec}] Invalid interval: start >= end`);
    return [];
  }

  while (currentStart < flooredEnd) {
    const nextEnd = Math.min(currentStart + BINANCE.limit * 5 * 60 * 1000, flooredEnd);
    
    const params = {
      symbol: symbol,
      period: BINANCE.apiInterval,
      limit: BINANCE.limit,
      startTime: currentStart,
      endTime: nextEnd
    };
    try {
      const response = await axios.get(BINANCE.url, { params, timeout: BINANCE.timeout });
      weightMonitor.logRequest('bin-tv', '/futures/data/takerlongshortRatio', 1);
      const data = response.data;

      if (!data || data.length === 0) break;

      allData.push(...data);

      const lastTimestamp = data[data.length - 1].timestamp;
      currentStart = lastTimestamp + 5 * 60 * 1000;

      if (data.length < BINANCE.limit) break;

      await sleep(BINANCE.rateDelay);
    } catch (error) {
      console.error(`[${BINANCE.perpspec}] Fetch error for ${symbol}:`, error.message);
      throw error;
    }
  }

  return allData.filter(d => d.timestamp >= startTs && d.timestamp <= endTs);
}

// ============================================================================
// BINANCE DATA PROCESSING (IMPROVED: ASYNC WITH DB QUERY + DEDUP FIX)
// ============================================================================
async function processBinanceData(rawData, baseSymbol) {
  const result = [];

  for (const dataPoint of rawData) {
    try {
      const timestamp = dataPoint.timestamp;  // Start of 5m candle
      const tbv_total = parseFloat(dataPoint.buyVol);
      const tsv_total = parseFloat(dataPoint.sellVol);

      if (isNaN(tbv_total) || isNaN(tsv_total)) {
        continue;
      }

      const endTs = timestamp + 5 * 60 * 1000;

      // Query DB for corresponding 1m OHLCV data
      let ohlcvData = await dbManager.queryPerpData('bin-ohlcv', baseSymbol, timestamp, endTs);

      // Filter and sort: only 1m interval, within range, sorted by ts
      ohlcvData = ohlcvData
        .filter(d => d.interval === '1m' && d.ts >= timestamp && d.ts < endTs)
        .sort((a, b) => a.ts - b.ts);

      // FIX: Dedup by ts (handle duplicates from OHLCV inserts; keep first after sort)
      const uniqueOhlcv = [];
      const seenTs = new Set();
      for (const row of ohlcvData) {
        if (!seenTs.has(row.ts)) {
          uniqueOhlcv.push(row);
          seenTs.add(row.ts);
        }
      }
      ohlcvData = uniqueOhlcv;

      // Fallback to even split if not exactly 5 consecutive minutes
      if (ohlcvData.length !== 5) {
        const isRecent = timestamp >= RECENT_THRESHOLD;
        if (!isRecent) {
          console.warn(`[${BINANCE.perpspec}] incomplete ohlcv ${baseSymbol} ${new Date(timestamp).toISOString()}, use split`);
        }
        // Even split for 5 slots
        for (let i = 0; i < 5; i++) {
          const minuteTs = timestamp + (i * 60 * 1000);
          result.push({
            ts: apiUtils.toMillis(BigInt(minuteTs)),
            symbol: baseSymbol,
            source: BINANCE.perpspec,
            perpspec: BINANCE.perpspec,
            interval: BINANCE.dbInterval,
            tbv: tbv_total / 5,
            tsv: tsv_total / 5
          });
        }
        continue;
      }

      // Verify consecutive 1m intervals
      let expectedTs = timestamp;
      let consecutive = true;
      for (let row of ohlcvData) {
        if (row.ts !== expectedTs) {
          consecutive = false;
          break;
        }
        expectedTs += 60 * 1000;
      }

      if (!consecutive) {
        const isRecent = timestamp >= RECENT_THRESHOLD;
        if (!isRecent) {
          console.warn(`[${BINANCE.perpspec}] incomplete ohlcv ${baseSymbol} ${new Date(timestamp).toISOString()}, use split`);
        }
        // Even split fallback
        for (let i = 0; i < 5; i++) {
          const minuteTs = timestamp + (i * 60 * 1000);
          result.push({
            ts: apiUtils.toMillis(BigInt(minuteTs)),
            symbol: baseSymbol,
            source: BINANCE.perpspec,
            perpspec: BINANCE.perpspec,
            interval: BINANCE.dbInterval,
            tbv: tbv_total / 5,
            tsv: tsv_total / 5
          });
        }
        continue;
      }

      // Compute total volume
      const total_v = ohlcvData.reduce((sum, d) => sum + parseFloat(d.v || 0), 0);

      if (total_v === 0) {
        // Even split if no volume
        for (let i = 0; i < 5; i++) {
          const minuteTs = timestamp + (i * 60 * 1000);
          result.push({
            ts: apiUtils.toMillis(BigInt(minuteTs)),
            symbol: baseSymbol,
            source: BINANCE.perpspec,
            perpspec: BINANCE.perpspec,
            interval: BINANCE.dbInterval,
            tbv: tbv_total / 5,
            tsv: tsv_total / 5
          });
        }
        continue;
      }

      // Compute weights
      let prevC = null;
      let tbv_weights = [];
      let tsv_weights = [];
      let tbv_sum_weight = 0;
      let tsv_sum_weight = 0;

      for (let i = 0; i < 5; i++) {
        const currV = parseFloat(ohlcvData[i].v || 0);
        const currC = parseFloat(ohlcvData[i].c || 0);
        let price_delta = 0;
        if (i > 0) {
          price_delta = currC - prevC;
        }
        prevC = currC;

        const vol_weight = currV / total_v;
        const s = price_delta > 0 ? 1 : (price_delta < 0 ? -1 : 0);
        const BIAS_STRENGTH = 0.6;  // 60% bias (adjust 0-1)

        const tbv_w = vol_weight * (0.5 + s * BIAS_STRENGTH * 0.5);  // Range: 20%-80%
        const tsv_w = vol_weight * (0.5 - s * BIAS_STRENGTH * 0.5);  // Range: 80%-20%

        tbv_weights.push(tbv_w);
        tsv_weights.push(tsv_w);
        tbv_sum_weight += tbv_w;
        tsv_sum_weight += tsv_w;
      }

      // Fallback to even if sum weights too low (e.g., all down for tbv)
      const even_weight = 1 / 5;
      if (tbv_sum_weight < 1e-6) {
        const isRecent = timestamp >= RECENT_THRESHOLD;
        if (!isRecent) {
          console.warn(`[${BINANCE.perpspec}] zero tbv weight for ${baseSymbol} ${new Date(timestamp).toISOString()}, use split for tbv`);
        }
        for (let i = 0; i < 5; i++) {
          tbv_weights[i] = even_weight;
        }
        tbv_sum_weight = 1;
      }
      if (tsv_sum_weight < 1e-6) {
        const isRecent = timestamp >= RECENT_THRESHOLD;
        if (!isRecent) {
          console.warn(`[${BINANCE.perpspec}] zero tsv weight for ${baseSymbol} ${new Date(timestamp).toISOString()}, use split for tsv`);
        }
        for (let i = 0; i < 5; i++) {
          tsv_weights[i] = even_weight;
        }
        tsv_sum_weight = 1;
      }

      // Allocate
      for (let i = 0; i < 5; i++) {
        const minuteTs = timestamp + (i * 60 * 1000);
        const tbv = (tbv_weights[i] / tbv_sum_weight) * tbv_total;
        const tsv = (tsv_weights[i] / tsv_sum_weight) * tsv_total;

        result.push({
          ts: apiUtils.toMillis(BigInt(minuteTs)),
          symbol: baseSymbol,
          source: BINANCE.perpspec,
          perpspec: BINANCE.perpspec,
          interval: BINANCE.dbInterval,
          tbv,
          tsv
        });
      }

    } catch (e) {
      const isRecent = dataPoint.timestamp >= RECENT_THRESHOLD;
      if (!isRecent) {
        console.error(`[${BINANCE.perpspec}] calc error for ${baseSymbol}`);
      }
      // Fallback: even split for this 5m (no throw)
      const timestamp = dataPoint.timestamp;
      const tbv_total = parseFloat(dataPoint.buyVol);
      const tsv_total = parseFloat(dataPoint.sellVol);
      for (let i = 0; i < 5; i++) {
        const minuteTs = timestamp + (i * 60 * 1000);
        result.push({
          ts: apiUtils.toMillis(BigInt(minuteTs)),
          symbol: baseSymbol,
          source: BINANCE.perpspec,
          perpspec: BINANCE.perpspec,
          interval: BINANCE.dbInterval,
          tbv: tbv_total / 5,
          tsv: tsv_total / 5
        });
      }
    }
  }

  return result;
}

// ============================================================================
// STATUS LOGGING AND BACKFILL
// ============================================================================
// Tracks completion status for the perpspec
const completedPerpspecs = new Set();
const HEARTBEAT_INTERVAL = 10000; // 10s heartbeat interval

async function backfill() {
  // Calculate total symbols
  const totalSymbols = perpList.length;

  // Log #1: Script start
  const startMessage = `Starting ${SCRIPT_NAME} backfill for Long/Short Ratios; ${totalSymbols} symbols.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', startMessage);
  console.log(`${STATUS_COLOR}🧞 ${startMessage}${RESET}`);

  // Log #2: Perpspecs connected
  const connectMessage = `${BINANCE.perpspec} connected, starting fetch.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', connectMessage);
  console.log(`${STATUS_COLOR}🧞 ${connectMessage}${RESET}`);

  const startTime = Date.now();

  // Heartbeat for perpspec running status
  const heartbeatId = setInterval(async () => {
    if (!completedPerpspecs.has(BINANCE.perpspec)) {
      // Log #3: Perpspec running
      const runningMessage = `${BINANCE.perpspec} backfilling db.`;
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', runningMessage, { perpspec: BINANCE.perpspec });
      console.log(`${STATUS_COLOR}${runningMessage}${RESET}`);
    }
  }, HEARTBEAT_INTERVAL);

  const limit = pLimit(BINANCE.concurrency);
  const completedSymbols = new Set();
  const promises = [];

  for (const baseSym of perpList) {
    promises.push(limit(async () => {
      const symbol = BINANCE.mapSymbol(baseSym);

      try {
        const intervalStart = floorToMinute(START);
        const intervalEnd = floorToMinute(NOW);
        
        const rawData = await fetchBinanceTV(symbol, intervalStart, intervalEnd);

        if (rawData.length === 0) return;

        const processedData = await processBinanceData(rawData, baseSym);
        
        if (processedData.length === 0) return;

        await dbManager.insertData(BINANCE.perpspec, processedData);

        completedSymbols.add(baseSym);

        // Log #4: Perpspec completed (when all symbols done)
        if (completedSymbols.size === totalSymbols && !completedPerpspecs.has(BINANCE.perpspec)) {
          completedPerpspecs.add(BINANCE.perpspec);
          const completeMessage = `${BINANCE.perpspec} backfill complete.`;
          await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', completeMessage, { perpspec: BINANCE.perpspec });
          console.log(`${STATUS_COLOR}🧞 ${completeMessage}${RESET}`);
        }

      } catch (error) {
        console.error(`❌ [${BINANCE.perpspec}] ${baseSym}: ${error.message}`);
        
        const errorCode = error.response?.status === 429 ? 'RATE_LIMIT' :
                         error.message.includes('timeout') ? 'TIMEOUT' :
                         error.message.includes('404') ? 'NOT_FOUND' : 'FETCH_ERROR';

        const shortMsg = error.message.includes('insert') ? 'insert error' : 
                         error.message.includes('calc') ? 'calc error' : error.message;

        await apiUtils.logScriptError(
          dbManager, 
          SCRIPT_NAME, 
          'API', 
          errorCode, 
          shortMsg,
          { perpspec: BINANCE.perpspec, symbol: baseSym }
        );
      }
    }));
  }

  await Promise.all(promises);
  clearInterval(heartbeatId);

  // Log #5: Full script completion
  if (completedPerpspecs.has(BINANCE.perpspec)) {
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    const finalMessage = `${SCRIPT_NAME} backfill completed in ${duration}s!`;
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', finalMessage);
    console.log(`${STATUS_COLOR}🧞 ${finalMessage}${RESET}`);
  }
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
      console.error('💥 TV backfill script (h) failed:', err);
      process.exit(1);
    });
}

module.exports = { backfill };