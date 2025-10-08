// apis/bin-tv-h.js
// SCRIPT: bin-tv-h.js  8 Oct 2025
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
const DAYS = 10;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const NOW = Date.now();
const START = NOW - DAYS * MS_PER_DAY;
const RECENT_THRESHOLD = NOW - 10 * 60 * 1000;  // Ignore warnings for last 10min windows

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
  rateDelay: 25,  // 40 req/sec = 25ms delay
  concurrency: 10,
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

        const tbv_w = vol_weight * (1 + s) / 2;  // 1 (up), 0.5 (flat), 0 (down)
        const tsv_w = vol_weight * (1 - s) / 2;  // 0 (up), 0.5 (flat), 1 (down)

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
// MAIN BACKFILL FUNCTION (MINIMAL CONSOLE LOGS)
// ============================================================================

async function backfill() {
  const startTime = Date.now();
  
  console.log(`\n🚀 Starting ${SCRIPT_NAME} backfill for Taker Buy/Sell Volumes (Improved)...`);

  // #1: Starting
  await apiUtils.logScriptStatus(
    dbManager,
    SCRIPT_NAME,
    'started',
    `${SCRIPT_NAME} backfill taker b/s vol, binance only, 1min calc.`
  );

  let symbolsProcessed = 0;
  let connectedLogged = false;
  const limit = pLimit(BINANCE.concurrency);
  const promises = [];

  for (const baseSym of perpList) {
    promises.push(limit(async () => {
      const symbol = BINANCE.mapSymbol(baseSym);

      try {
        // Log connected on first successful start (but toned down - only once)
        if (!connectedLogged) {
          connectedLogged = true;
        }

        // Fetch data
        const intervalStart = floorToMinute(START);
        const intervalEnd = floorToMinute(NOW);
        
        const rawData = await fetchBinanceTV(symbol, intervalStart, intervalEnd);

        if (rawData.length === 0) return;

        // Process data (now async)
        const processedData = await processBinanceData(rawData, baseSym);
        
        if (processedData.length === 0) return;

        // Insert to DB (assumes insertData uses ON CONFLICT (ts, symbol, perpspec, interval) DO NOTHING)
        await dbManager.insertData(BINANCE.perpspec, processedData);

        symbolsProcessed++;

      } catch (error) {
        console.error(`❌ [${BINANCE.perpspec}] ${baseSym}: ${error.message}`);
        
        // Determine error type (API pass-through)
        const errorCode = error.response?.status === 429 ? 'RATE_LIMIT' :
                         error.message.includes('timeout') ? 'TIMEOUT' :
                         error.message.includes('404') ? 'NOT_FOUND' : 'FETCH_ERROR';

        // Shorten internal insert/calc errors if applicable, but keep API details
        const shortMsg = error.message.includes('insert') ? 'insert error' : 
                         error.message.includes('calc') ? 'calc error' : error.message;

        // Log error
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

  // #2: Running heartbeat (at end, with total count)
  await apiUtils.logScriptStatus(
    dbManager,
    SCRIPT_NAME,
    'running',
    `bin-tv completed (${symbolsProcessed}) symbols`
  );

  // #3: Complete
  await apiUtils.logScriptStatus(
    dbManager, 
    SCRIPT_NAME, 
    'complete', 
    `${SCRIPT_NAME} completed taker b/s vol backfill in ${((Date.now() - startTime) / 1000).toFixed(2)}s`
  );

  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`\n🎉 Taker Volume backfill (h) completed in ${duration}s!`);
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================

if (require.main === module) {
  backfill()
    .then(() => {
      console.log('✅ TV backfill script (h) completed successfully');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 TV backfill script (h) failed:', err);
      process.exit(1);
    });
}

module.exports = { backfill };