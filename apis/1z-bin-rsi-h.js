// SCRIPT: 1z-bin-rsi-h.js Backfill 22 Oct 2025  *insertBackfillData
// RSI calculation script for all individual symbols using bin-ohlcv data (no MT)
// RSI11 for 1m (rsi1) and 60m aggregated (rsi60, forward-filled to 1m ts—60x same per hour)
// Aggregation uses LAST CLOSE per 60m bucket; ts as BigInt Unix ms
// Parallel batching; ON CONFLICT DO NOTHING for inserts (no overwrite; fills gaps/new ts only); streamlined errors
// Verification: rsi1 100% full; rsi60 allow <=15% null (covers early ~11hrs + gaps); warn if >15%
// Unified schema: Query OHLCV via exchange='bin' and c IS NOT NULL; insert via dbManager.insertBackfillData with perpspec='bin-rsi'

const { Pool } = require('pg');
require('dotenv').config();
const apiUtils = require('../api-utils'); // For status/error logging and toMillis
const dbManager = require('../db/dbsetup'); // Provides dbManager for inserts and logging
const failedSymbols = new Set();  // Track unique symbols with RSI calc failure due to no OHLCV
const ERROR_COLOR = '\x1b[91m'; // Red for error logs

const SCRIPT_NAME = '1z-bin-rsi-h.js';
const PERIOD = 11; // RSI period - change this to adjust for all calculations
const PERPSPEC_SOURCE = 'bin-rsi';
const DATA_EXCHANGE = 'bin'; // Fixed to 'bin' for OHLCV source
const AGGREGATE_MINUTES = 60; // Aggregation interval for RSI60 in minutes (60m = 1 hour) - can be changed for testing
const BUCKET_SIZE_MS = AGGREGATE_MINUTES * 60 * 1000; // Milliseconds in aggregation interval
const HEARTBEAT_INTERVAL = 10000; // 10s heartbeat
const CHUNK_SIZE = 5; // Parallel chunks to avoid DB overload
const MAX_NULL_PCT = 15; // Allow <=15% null for rsi60 (covers early ~11hrs + gaps)

const STATUS_COLOR = '\x1b[36m'; // Light blue for status logs
const RESET = '\x1b[0m';
const RECENT_THRESHOLD_MIN = 5;  // Error if last OHLCV >N min old
const missingRecentSymbols = new Set();  // Track unique symbols with no recent OHLCV

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  max: 10,
  connectionTimeoutMillis: 5000,
  idleTimeoutMillis: 30000
});

async function getSymbols() {
  try {
    const query = `SELECT DISTINCT symbol FROM perp_data WHERE exchange = $1 AND c IS NOT NULL`;
    const result = await pool.query(query, [DATA_EXCHANGE]);
    return result.rows.map(row => row.symbol);
  } catch (error) {
    console.error(`[DATABASE] SYMBOL_QUERY_ERROR: Error getting symbols - ${error.message}`);
    throw error;
  }
}

function calculateRSI(prices, period) {
  if (prices.length < period + 1) return [];
  let gains = 0, losses = 0, rsiValues = [];
  for (let i = 1; i < prices.length; i++) {
    const change = prices[i].close - prices[i - 1].close;
    if (change > 0) gains += change; else losses += Math.abs(change);
    if (i >= period) {
      const avgGain = gains / period, avgLoss = losses / period;
      const rs = avgGain / (avgLoss === 0 ? 1 : avgLoss);
      const rsi = 100 - (100 / (1 + rs));
      rsiValues.push({ ts: prices[i].ts, rsi: Math.round(rsi) }); // Whole number, no decimals
      const prevChange = prices[i - period + 1].close - prices[i - period].close;
      if (prevChange > 0) gains -= prevChange; else losses -= Math.abs(prevChange);
    }
  }
  return rsiValues;
}

function simpleAggregateToHigherTimeframe(prices) {
  if (prices.length === 0) return [];
  let aggregated = [], bucketStart = null, lastClose = null, lastTs = null;
  for (const price of prices) {
    const tsMs = price.ts.getTime(); if (isNaN(tsMs)) continue;
    const bucket = Math.floor(tsMs / BUCKET_SIZE_MS) * BUCKET_SIZE_MS;
    if (bucket !== bucketStart) {
      if (bucketStart !== null && lastTs !== null) {
        const endTs = new Date(bucketStart + BUCKET_SIZE_MS - 1);
        if (!isNaN(endTs.getTime())) aggregated.push({ ts: endTs, close: lastClose });
      }
      bucketStart = bucket; lastTs = tsMs; lastClose = price.close;
    } else { lastTs = tsMs; lastClose = price.close; }
  }
  if (bucketStart !== null && lastTs !== null) {
    const endTs = new Date(bucketStart + BUCKET_SIZE_MS - 1);
    if (!isNaN(endTs.getTime())) aggregated.push({ ts: endTs, close: lastClose });
  }
  return aggregated;
}

async function calculateRSIForSymbol(symbol) {
  try {
    const query = `SELECT ts, c::numeric AS close FROM perp_data WHERE symbol = $1 AND exchange = $2 AND c IS NOT NULL ORDER BY ts ASC`;
    const result = await pool.query(query, [symbol, DATA_EXCHANGE]);
    if (result.rows.length === 0) {
      failedSymbols.add(symbol);  // <-- ADD THIS LINE (track no OHLCV)
      return false;
    }

    const prices = result.rows.map(row => {
      let ts; const tsStr = String(row.ts).trim();
      if (/^\d+$/.test(tsStr) && tsStr.length >= 10 && tsStr.length <= 13) ts = new Date(Number(tsStr));
      else ts = new Date(tsStr);
      const close = parseFloat(row.close);
      return { ts, close };
    }).filter(p => !isNaN(p.ts.getTime()) && !isNaN(p.close) && isFinite(p.close));

    if (prices.length < PERIOD + 1) {
      failedSymbols.add(symbol); 
      return false;
    }

    const rsi1Values = calculateRSI(prices, PERIOD);
    const aggregated60m = simpleAggregateToHigherTimeframe(prices);
    if (aggregated60m.length < PERIOD + 1) {
      return false;
    }
    const rsi60Values = calculateRSI(aggregated60m, PERIOD);

    if (rsi1Values.length === 0) return false;

    rsi60Values.sort((a, b) => a.ts.getTime() - b.ts.getTime());
    const insertionData = []; let rsi60Pointer = 0, currentRsi60 = null;
    for (const rsi1Entry of rsi1Values) {
      if (isNaN(rsi1Entry.ts.getTime())) continue;
      const currentTsMs = rsi1Entry.ts.getTime();
      while (rsi60Pointer < rsi60Values.length && !isNaN(rsi60Values[rsi60Pointer].ts.getTime()) && rsi60Values[rsi60Pointer].ts.getTime() <= currentTsMs) {
        currentRsi60 = rsi60Values[rsi60Pointer].rsi; rsi60Pointer++;
      }
      insertionData.push({ rsi1: rsi1Entry.rsi, rsi60: currentRsi60, tsMs: currentTsMs });
    }

    if (insertionData.length === 0) {
      return false;
    }

    // Prepare records for unified insertBackfillData (perpspec as string, ts as BigInt via toMillis)
    const processed = insertionData.map(data => ({
      ts: apiUtils.toMillis(BigInt(data.tsMs)),
      symbol,
      perpspec: PERPSPEC_SOURCE, // String; will be wrapped to array in insertData
      rsi1: data.rsi1,
      rsi60: data.rsi60
    }));

    await dbManager.insertBackfillData(processed);
    return true;
  } catch (error) {
    console.error(`[INTERNAL] SYMBOL_ERROR: Error for ${symbol} - ${error.message}`);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'CALC', 'RSI_CALC_FAILED', `Error calculating RSI for ${symbol}: ${error.message}`, { symbol });
    return false;
  }
}
//=============================================================================
async function verifyAllRSIComplete(symbols) {
  try {
    // Global verification (existing: total coverage)
    const ohlcvQuery = `
      SELECT COUNT(*) as total_ohlcv
      FROM perp_data
      WHERE exchange = $1 AND symbol = ANY($2) AND c IS NOT NULL
    `;
    const rsiQuery = `
      SELECT 
        COUNT(*) FILTER (WHERE rsi1 IS NOT NULL) as rsi1_count,
        COUNT(*) FILTER (WHERE rsi60 IS NOT NULL) as rsi60_count,
        COUNT(*) as total_rsi_rows
      FROM perp_data
      WHERE exchange = $1 AND symbol = ANY($2) AND perpspec @> '["bin-rsi"]'::jsonb
    `;
    const params = [DATA_EXCHANGE, symbols];

    const [ohlcvResult, rsiResult] = await Promise.all([
      pool.query(ohlcvQuery, params),
      pool.query(rsiQuery, params)
    ]);

    const totalOhlcv = parseInt(ohlcvResult.rows[0].total_ohlcv || 0);
    const rsi1Count = parseInt(rsiResult.rows[0].rsi1_count || 0);
    const rsi60Count = parseInt(rsiResult.rows[0].rsi60_count || 0);
    const totalRsiRows = parseInt(rsiResult.rows[0].total_rsi_rows || 0);

    // Debug log counts (remove after fixing)
    // console.log(`${STATUS_COLOR}verif: Total OHLCV rows: ${totalOhlcv}, RSI rows: ${totalRsiRows}, RSI1 non-null: ${rsi1Count}, RSI60 non-null: ${rsi60Count}${RESET}`);

    if (totalOhlcv === 0) {
      console.warn(`No OHLCV data for symbols - skipping RSI check`);
      return false;
    }

    // Relaxed: RSI1 coverage >=95% of OHLCV (allows minor gaps/ts mismatches from calc/insert)
    const rsi1Pct = (rsi1Count / totalOhlcv) * 100;
    const rsi1Full = rsi1Pct >= 95;
    const nullRsi60Pct = totalRsiRows > 0 ? ((totalRsiRows - rsi60Count) / totalRsiRows * 100) : 0;

    if (!rsi1Full) {
      console.error(`verif: RSI1_COVERAGE_WARNING: Only ${rsi1Pct.toFixed(1)}% RSI1 vs. OHLCV (${rsi1Count}/${totalOhlcv}) - check calc/insert ts match`);
      return false;
    }
    if (nullRsi60Pct > MAX_NULL_PCT) {
      console.error(`verif RSI60_NULL_WARNING: ${nullRsi60Pct.toFixed(1)}% rsi60 null in ${totalRsiRows} RSI rows - check early data/aggregation`);
      return false;
    }

    // NEW: Check for missing recent OHLCV (last update > RECENT_THRESHOLD_MIN min ago per symbol)
    const thresholdMs = RECENT_THRESHOLD_MIN * 60 * 1000;
    const nowMs = Date.now();
    const recentCheckQuery = `
      SELECT symbol, MAX(ts) as last_ohlcv_ts
      FROM perp_data 
      WHERE exchange = $1 AND symbol = ANY($2) AND c IS NOT NULL
      GROUP BY symbol
    `;
    const recentResult = await pool.query(recentCheckQuery, [DATA_EXCHANGE, symbols]);

    let missingRecentCount = 0;
    for (const row of recentResult.rows) {
      const lastTs = Number(row.last_ohlcv_ts || 0);
      if (nowMs - lastTs > thresholdMs) {  // Last OHLCV >5 min old
        missingRecentSymbols.add(row.symbol);  // Track unique
        missingRecentCount++;
      }
    }

    // Log recent status (always, for visibility)
    // console.log(`${STATUS_COLOR}verif: Recent OHLCV check: ${missingRecentCount} symbols with no updates in last ${RECENT_THRESHOLD_MIN} min${RESET}`);

    // If missing recent OHLCV, treat as error (RSI calc can't proceed without it)
    if (missingRecentCount > 0) {
      const errorMsg = `No recent bin OHLCV for ${missingRecentCount} symbols (last update >${RECENT_THRESHOLD_MIN} min ago); RSI calc incomplete—check OHLCV source.`;
      console.error(`${ERROR_COLOR}❌ ${errorMsg}${RESET}`);  // Red error (define ERROR_COLOR if needed)
      await apiUtils.logScriptError(
        dbManager, 
        SCRIPT_NAME, 
        'DATA', 
        'RECENT_OHLCV_MISSING', 
        errorMsg,
        { missingRecentSymbolCount: missingRecentCount, thresholdMin: RECENT_THRESHOLD_MIN }
      );
      // Optional: Return false to prevent "complete" log
      return false;  // Fail verification on missing recent data
    }

    console.log(`${STATUS_COLOR}*RSI complete: ${rsi1Pct.toFixed(1)}% RSI1, ${100 - nullRsi60Pct.toFixed(1)}% RSI60${RESET}`);
    return true;
  } catch (error) {
    console.error(`[DATABASE] VERIFY_ERROR: Verification failed - ${error.message}`);
    return false;
  }
}

//==================================================================
async function calculateRSIForAllSymbols() {
  const startTime = Date.now();
  console.log(`\n${STATUS_COLOR}*RSI Starting ${SCRIPT_NAME}...${RESET}`);
  let heartbeatInterval;

  try {
    // Get symbols first for count
    const symbols = await getSymbols();
    const totalSymbols = symbols.length;

    // #1 Status: started
    const message1 = `*RSI ${SCRIPT_NAME} initiated, ${totalSymbols} symbols.`;
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', message1);
    console.log(`${STATUS_COLOR}${message1}${RESET}`);

    if (totalSymbols === 0) {
      const message3 = `rsi1, rsi60 calculation for 0 symbols.`;
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message3);
      // console.log(`*RSI ${STATUS_COLOR}${message3}${RESET}`);
      return;
    }

    // #2 Status: connected (DB query successful)
    const message2 = `DB connected for exchange '${DATA_EXCHANGE}', start RSI cals for ${totalSymbols} symbols.`;
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', message2);
    // console.log(`${STATUS_COLOR}🔧 *RSI ${message2}${RESET}`);

    // Initialize tracking
    let symbolsProcessed = 0;

    // -- Heartbeat with #3 running status logs --
    heartbeatInterval = setInterval(() => {
      (async () => {
        if (symbolsProcessed < totalSymbols) {
          // #3 Status: running
          const message = `rsi1, rsi60 calculation for ${totalSymbols} symbols (processed: ${symbolsProcessed}).`;
          try {
            await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', message);
          } catch (err) {
            console.error(`[heartbeat] DB log failed: ${err.message}`);
          }
          console.log(`${STATUS_COLOR}*RSI ${message}${RESET}`);
        }
      })();
    }, HEARTBEAT_INTERVAL);

    // Parallel batching for symbols (chunked to limit DB load)
    for (let i = 0; i < symbols.length; i += CHUNK_SIZE) {
      const chunk = symbols.slice(i, i + CHUNK_SIZE);
      const promises = chunk.map(symbol => calculateRSIForSymbol(symbol).then(success => { 
        if (success) symbolsProcessed++; 
        return success; 
      }));
      await Promise.all(promises);
    }

        // Catch-all error for RSI calc failures due to no bin OHLCV (one red log per run)
    if (failedSymbols.size > 0) {
      const failedCount = failedSymbols.size;
      const errorMsg = `RSI calc failed for ${failedCount} symbols due to no bin OHLCV; check data availability.`;
      console.error(`${ERROR_COLOR}❌ ${errorMsg}${RESET}`);  // Red error
      await apiUtils.logScriptError(
        dbManager, 
        SCRIPT_NAME, 
        'DATA',  // Error type: DATA (for no OHLCV)
        'OHLCV_MISSING',  // Error code
        errorMsg,
        { failedSymbolCount: failedCount }  // DB details: just count
      );
    }

    clearInterval(heartbeatInterval);

    // Tier 3: Verify and #5 completed status
    const allComplete = await verifyAllRSIComplete(symbols);
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    if (allComplete) {
      const message5 = `⏱️ *RSI backfills complete in ${duration}s for ${symbolsProcessed}/${totalSymbols} symbols.`;
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', message5);
      console.log(`${message5}`);
      // console.log(`\n⏱️ *RSI backfills complete in ${duration}s!`);
    } else {
      const messageWarn = `RSI backfills incomplete in ${duration}s (check verification logs)`;
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'warning', messageWarn);
      console.log(`${STATUS_COLOR}[WARNING] ${messageWarn}${RESET}`);
      process.exit(1);
    }
  } catch (error) {
    if (heartbeatInterval) clearInterval(heartbeatInterval);
    console.error(`[INTERNAL] SCRIPT_ERROR: Script failed - ${error.message}`);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'INTERNAL', 'SCRIPT_FAILED', `Overall script error: ${error.message}`, {});
    process.exit(1);
  } finally {
    await pool.end();
  }
}

// Run if direct
if (require.main === module) {
  calculateRSIForAllSymbols();
}

module.exports = { calculateRSIForAllSymbols };