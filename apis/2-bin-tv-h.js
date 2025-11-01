// SCRIPT: 2-bin-tv-h.js (Unified Schema) BINANCE ONLY
// Updated: 22 Oct 2025 - Unified per READEMEperpdata.md
// Binance *ONLY Taker Buy/Sell Volume Backfill Script (Improved Distribution)
// - Distributes 5m tbv/tsv into 1m slots based on 1m OHLCV volume and price direction
// - Uses DB query for bin exchange OHLCV (c and v fields) to get weighting (no perpspec filter)
// - For partial last bar: Query up to current max ts in DB; distribute to available/recent 1m slots
// - On conflict: Additive update via insertBackfillData (COALESCE preserves existing)
// - Fixes: Dedup OHLCV rows by ts; simplified logging and structure

const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');
const weightMonitor = require('../b-weight');
const affectedSymbols = new Set();  // Track unique symbols with incomplete OHLCV for count-only warning

const SCRIPT_NAME = '2-bin-tv-h.js';
const STATUS_COLOR = '\x1b[36m'; // Light blue for status logs
const RESET = '\x1b[0m';
const DAYS = 10;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const NOW = Date.now();
const START = NOW - DAYS * MS_PER_DAY;

// Helper functions
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function floorToMinute(ts) {
  return Math.floor(ts / 60000) * 60000;
}

// Helper to get max ts for symbol (for partial last bar)
async function getMaxTsForSymbol(baseSymbol) {
  try {
    const query = `SELECT MAX(ts) as max_ts FROM perp_data WHERE symbol = $1 AND exchange = 'bin' AND v IS NOT NULL`;
    const result = await dbManager.pool.query(query, [baseSymbol]);
    return result.rows[0].max_ts ? Number(result.rows[0].max_ts) : NOW;
  } catch (err) {
    console.warn(`[bin-tv] Max ts query failed for ${baseSymbol}: ${err.message}; fallback to NOW`);
    return NOW;
  }
}

// ============================================================================
// BINANCE CONFIGURATION
// ============================================================================
const BINANCE = {
  perpspec: 'bin-tv',
  exchange: 'bin',
  url: 'https://fapi.binance.com/futures/data/takerlongshortRatio',
  limit: 500,
  rateDelay: 100,  // 40 req/sec = 25ms delay
  concurrency: 8,
  timeout: 15000,
  apiInterval: '5m',
  mapSymbol: sym => `${sym}USDT`
};

// ============================================================================
// BINANCE FETCH FUNCTION (Up to NOW for latest partial bar)
// ============================================================================
async function fetchBinanceTV(symbol, startTs, endTs) {
  let allData = [];
  let currentStart = floorToMinute(startTs);
  const flooredEnd = endTs;  // No floor for end=NOW to include partial bar

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
// BINANCE DATA PROCESSING (Weighted distribution; partial last bar uses recent OHLCV)
// ============================================================================
async function processBinanceData(rawData, baseSymbol) {
  const result = [];

  for (let i = 0; i < rawData.length; i++) {
    const dataPoint = rawData[i];
    const timestamp = dataPoint.timestamp;  // Start of 5m candle
    const tbv_total = parseFloat(dataPoint.buyVol);
    const tsv_total = parseFloat(dataPoint.sellVol);

    if (isNaN(tbv_total) || isNaN(tsv_total)) {
      continue;
    }

    const endTs = timestamp + 5 * 60 * 1000;
    const isLastBar = i === rawData.length - 1 && timestamp > NOW - 5 * 60 * 1000;  // Detect partial last bar

    let ohlcvData;

// 23 OCT Grok -- Inside the for (let i = 0; i < rawData.length; i++) loop, replace the entire "if (isLastBar)" block with:
if (i === rawData.length - 1) {
      const freshNow = Date.now();  // Fresh timestamp to handle script runtime delays
      const currentMinute = floorToMinute(freshNow);
      const barEnd = timestamp + 5 * 60 * 1000;
      
      // Calculate overlapping slots from bar start to fresh now (cap at 5 for the bar's scope)
      let startSlot = timestamp;
      const elapsedMs = freshNow - timestamp;
      let expectedSlots = Math.min(5, Math.max(1, Math.floor(elapsedMs / 60000) + 1));
      
      // If bar is "old" (ended >1 min ago), shift to fill the most recent 5 slots up to now (project TV forward)
      if (barEnd < freshNow - 60000) {
        // Last complete bar: Distribute to last 5 1m slots ending at currentMinute
        startSlot = currentMinute - 4 * 60 * 1000;  // e.g., if now=14:23, slots=14:19,14:20,14:21,14:22,14:23
        expectedSlots = 5;       
      }
      
      if (expectedSlots > 0) {
        const even_tbv = tbv_total / expectedSlots;
        const even_tsv = tsv_total / expectedSlots;
        for (let j = 0; j < expectedSlots; j++) {
          const minuteTs = startSlot + (j * 60 * 1000);
          if (minuteTs <= currentMinute) {  // Only up to current minute (no future)
            result.push({
              ts: apiUtils.toMillis(BigInt(minuteTs)),
              symbol: baseSymbol,
              perpspec: BINANCE.perpspec,
              tbv: even_tbv,
              tsv: even_tsv
            });
          }
        }
       }
      continue;  // Skip to next dataPoint
    }

    // Historical/full bars: Query OHLCV and do weighted distribution with fallbacks
    ohlcvData = await dbManager.queryPerpData(BINANCE.exchange, baseSymbol, timestamp, endTs);

    // Filter to rows within range with v and c non-null; sort and dedup by ts
    ohlcvData = ohlcvData
      .filter(d => d.v !== null && d.c !== null && d.ts >= timestamp && d.ts < endTs)
      .sort((a, b) => a.ts - b.ts);

    // Dedup by ts (keep first after sort)
    const uniqueOhlcv = [];
    const seenTs = new Set();
    for (const row of ohlcvData) {
      if (!seenTs.has(row.ts)) {
        uniqueOhlcv.push(row);
        seenTs.add(row.ts);
      }
    }
    ohlcvData = uniqueOhlcv;

    const slotCount = ohlcvData.length;  // Now defined for historical

        // Fallback to even split if <5 slots (track unique symbols for aggregated count; no per-bar log)
    if (slotCount < 5) {
      affectedSymbols.add(baseSymbol);  // Add to Set (unique; count at end)
      for (let j = 0; j < 5; j++) {
        const minuteTs = timestamp + (j * 60 * 1000);
        result.push({
          ts: apiUtils.toMillis(BigInt(minuteTs)),
          symbol: baseSymbol,
          perpspec: BINANCE.perpspec,
          tbv: tbv_total / 5,
          tsv: tsv_total / 5
        });
      }
      continue;
    }

    // Verify consecutive 1m intervals
    let consecutive = true;
    let expectedTs = timestamp;
    for (const row of ohlcvData) {
      if (row.ts !== expectedTs) {
        consecutive = false;
        break;
      }
      expectedTs += 60 * 1000;
    }

    if (!consecutive) {
      console.warn(`[bin-tv] Non-consecutive OHLCV for ${baseSymbol} at ${new Date(timestamp).toISOString()}; even split to 5 slots`);
      for (let j = 0; j < 5; j++) {
        const minuteTs = timestamp + (j * 60 * 1000);
        result.push({
          ts: apiUtils.toMillis(BigInt(minuteTs)),
          symbol: baseSymbol,
          perpspec: BINANCE.perpspec,
          tbv: tbv_total / 5,
          tsv: tsv_total / 5
        });
      }
      continue;
    }

    // Compute total volume
    const total_v = ohlcvData.reduce((sum, d) => sum + (d.v || 0), 0);

    if (total_v === 0) {
      // Even split if no volume
      for (let j = 0; j < slotCount; j++) {
        const minuteTs = ohlcvData[j].ts;
        result.push({
          ts: apiUtils.toMillis(BigInt(minuteTs)),
          symbol: baseSymbol,
          perpspec: BINANCE.perpspec,
          tbv: tbv_total / slotCount,
          tsv: tsv_total / slotCount
        });
      }
      continue;
    }

    // Compute weights based on volume and price direction
    let prevC = null;
    let tbv_weights = [];
    let tsv_weights = [];
    let tbv_sum_weight = 0;
    let tsv_sum_weight = 0;
    const BIAS_STRENGTH = 0.6;

    for (let j = 0; j < slotCount; j++) {
      const currV = ohlcvData[j].v || 0;
      const currC = ohlcvData[j].c || 0;
      let price_delta = 0;
      if (j > 0) {
        price_delta = currC - prevC;
      }
      prevC = currC;

      const vol_weight = currV / total_v;
      const s = price_delta > 0 ? 1 : (price_delta < 0 ? -1 : 0);

      const tbv_w = vol_weight * (0.5 + s * BIAS_STRENGTH * 0.5);
      const tsv_w = vol_weight * (0.5 - s * BIAS_STRENGTH * 0.5);

      tbv_weights.push(tbv_w);
      tsv_weights.push(tsv_w);
      tbv_sum_weight += tbv_w;
      tsv_sum_weight += tsv_w;
    }

    // Normalize weights (fallback to even if sums low)
    if (tbv_sum_weight < 1e-6 || tsv_sum_weight < 1e-6) {
      const even_weight = 1 / slotCount;
      tbv_weights.fill(even_weight);
      tsv_weights.fill(even_weight);
      tbv_sum_weight = 1;
      tsv_sum_weight = 1;
      console.warn(`[bin-tv] Low weights for ${baseSymbol} at ${new Date(timestamp).toISOString()}; using even split`);
    }

    // Allocate to slots
    for (let j = 0; j < slotCount; j++) {
      const minuteTs = ohlcvData[j].ts;
      const tbv = (tbv_weights[j] / tbv_sum_weight) * tbv_total;
      const tsv = (tsv_weights[j] / tsv_sum_weight) * tsv_total;

      result.push({
        ts: apiUtils.toMillis(BigInt(minuteTs)),
        symbol: baseSymbol,
        perpspec: BINANCE.perpspec,
        tbv,
        tsv
      });
    }
  }

  return result;
}

// ============================================================================
// BACKFILL FUNCTION (Simplified logging: start, connected, running heartbeat, completed)
// ============================================================================
const completedPerpspecs = new Set();
const HEARTBEAT_INTERVAL = 10000; // 10s heartbeat

async function backfill() {
  const totalSymbols = perpList.length;

  // Log #1: Script start
  const startMessage = `*TV Starting ${SCRIPT_NAME} backfill for Taker Buy/Sell Volume; ${totalSymbols} symbols.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', startMessage);
  console.log(`${STATUS_COLOR}${startMessage}${RESET}`);

  // Log #2: Perpspec connected
  const connectMessage = `${BINANCE.perpspec} connected, starting fetch.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', connectMessage);
  // console.log(`${STATUS_COLOR}${connectMessage}${RESET}`);

  const startTime = Date.now();

  // Heartbeat for running status
  const heartbeatId = setInterval(async () => {
    if (!completedPerpspecs.has(BINANCE.perpspec)) {
      // Log #3: Perpspec running
      const runningMessage = `*TV ${BINANCE.perpspec} backfilling db.`;
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
        const intervalEnd = NOW;  // Up to now for partial bar
        
        const rawData = await fetchBinanceTV(symbol, intervalStart, intervalEnd);

        if (rawData.length === 0) return;

        const processedData = await processBinanceData(rawData, baseSym);
        
        if (processedData.length === 0) return;

        // Use insertBackfillData (additive COALESCE update)
        await dbManager.insertBackfillData(processedData);

        completedSymbols.add(baseSym);

        // Log #4: Perpspec completed (when all symbols done)
        if (completedSymbols.size === totalSymbols && !completedPerpspecs.has(BINANCE.perpspec)) {
          completedPerpspecs.add(BINANCE.perpspec);
          const completeMessage = `*TV ${BINANCE.perpspec} backfill complete.`;
          await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', completeMessage, { perpspec: BINANCE.perpspec });
          // console.log(`${completeMessage}`);
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

    // Aggregated warning for incomplete OHLCV (one log per run, count-only)
  if (affectedSymbols.size > 0) {
    const symbolCount = affectedSymbols.size;
    const warningMsg = `bin-tv-h incomplete ohlcv for ${symbolCount} symbols; revert to even split.`;
    console.warn(`${STATUS_COLOR}${warningMsg}${RESET}`);  // Light blue; change to '\x1b[93m' for yellow if preferred
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'warning', warningMsg, { incompleteSymbolCount: symbolCount });
  }

  // Log #5: Full script completion
  if (completedPerpspecs.has(BINANCE.perpspec)) {
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    const finalMessage = `*TV ${SCRIPT_NAME} backfill completed in ${duration}s!`;
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', finalMessage);
    console.log(`⏱️ ${finalMessage}`);
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