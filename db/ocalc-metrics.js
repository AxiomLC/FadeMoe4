// ============================================================================
// METRICS CALCULATOR  calc-metrics.js  22 Oct 2025 (UNIFIED SCHEMA)
// Calculates rolling % changes for all parameters across all exchanges (bin, byb, okx)
// Runs every 1 minute to keep perp_metrics table fresh for backtester
// Uses unified queries (by exchange + field presence); ON CONFLICT DO UPDATE for upserts
// ============================================================================

const dbManager = require('./dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = 'calc-metrics.js';
const STATUS_LOG_COLOR = '\x1b[38;2;147;112;219m'; // Purple
const COLOR_RESET = '\x1b[0m';

// ============================================================================
// USER CONFIGURATION
// ============================================================================
const DB_RETENTION_DAYS = 10;
const LOOKBACK_MINUTES = 15;
const WINDOW_SIZES = [1, 5, 10];
const CALCULATION_INTERVAL_MS = 60000; // 1 minute
const HEARTBEAT_INTERVAL_MS = 60000;   // 1 minute status update
const PARALLEL_SYMBOLS = 8;            // Process 8 symbols concurrently

const EXCHANGES = ['bin', 'byb', 'okx'];

// existing code ...
// ============================================================================
// UTILITIES
// ============================================================================
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function logStatus(status, message) {
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, status, message);
  console.log(`${STATUS_LOG_COLOR}${message}${COLOR_RESET}`);
}

function calculatePercentChange(current, previous) {
  if (previous === null || previous === undefined || previous === 0) return null;
  if (current === null || current === undefined) return null;
  const change = ((current - previous) / Math.abs(previous)) * 100;
  return Math.min(Math.max(parseFloat(change.toFixed(4)), -99.9999), 99.9999);
}

// Helper: Get majority lqside over a window (count + qty-weighted tie-breaker)
function getWindowMajoritySide(windowData) {
  if (!windowData || windowData.length === 0) return null;

  let longCount = 0, shortCount = 0;
  let longQty = 0, shortQty = 0;

  for (const row of windowData) {
    if (row.lqside === 'long') {
      longCount++;
      if (row.lqqty) longQty += row.lqqty;
    } else if (row.lqside === 'short') {
      shortCount++;
      if (row.lqqty) shortQty += row.lqqty;
    }
  }

  if (longCount > shortCount) return 'long';
  if (shortCount > longCount) return 'short';

  // Tie: Use qty-weighted
  if (longQty > shortQty) return 'long';
  if (shortQty > longQty) return 'short';

  return null; // True tie, no clear majority
}

// ============================================================================
// GRACEFUL SHUTDOWN (Top-Level - Add Once on Module Load)
// ============================================================================
let runInterval = null;  // Global for clear

async function gracefulShutdown(signal) {
  console.log(`\n⚠️  Received ${signal}, shutting down gracefully...`);
  if (runInterval) clearInterval(runInterval);
  await logStatus('stopped', `${SCRIPT_NAME} stopped by ${signal}.`);
  await dbManager.close();
  process.exit(0);
}

// Add listeners once at module level (outside functions; guard against duplicates)
if (!process.listeners('SIGINT').length) {
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  process.on('SIGINT', () => gracefulShutdown('SIGINT'));
}

// ============================================================================
// FETCH AND AGGREGATE DATA BY EXCHANGE (UNIFIED SCHEMA)
// ============================================================================
async function fetchAndAggregateData(symbol, startTs, endTs) {
  let allData = [];

  // Single query per exchange (unified: filter by exchange + field presence)
  for (const exchange of EXCHANGES) {
    // Base query for all fields in range
    const baseQuery = `
      SELECT ts, symbol, exchange, o, h, l, c, v, oi, pfr, lsr, 
             lqside, lqprice, lqqty, rsi1, rsi60, tbv, tsv
      FROM perp_data
      WHERE symbol = $1 AND exchange = $2 AND ts >= $3 AND ts <= $4
      ORDER BY ts ASC
    `;
    try {
      const result = await dbManager.query(baseQuery, [symbol, exchange, BigInt(startTs), BigInt(endTs)]);
      allData = allData.concat(result.rows.map(row => ({
        ts: Number(row.ts),
        symbol: row.symbol,
        exchange: row.exchange,
        o: row.o ? parseFloat(row.o) : null,
        h: row.h ? parseFloat(row.h) : null,
        l: row.l ? parseFloat(row.l) : null,
        c: row.c ? parseFloat(row.c) : null,
        v: row.v ? parseFloat(row.v) : null,
        oi: row.oi ? parseFloat(row.oi) : null,
        pfr: row.pfr ? parseFloat(row.pfr) : null,
        lsr: row.lsr ? parseFloat(row.lsr) : null,
        lqside: row.lqside || null,
        lqprice: row.lqprice ? parseFloat(row.lqprice) : null,
        lqqty: row.lqqty ? parseFloat(row.lqqty) : null,
        rsi1: row.rsi1 ? parseFloat(row.rsi1) : null,
        rsi60: row.rsi60 ? parseFloat(row.rsi60) : null,
        tbv: row.tbv ? parseFloat(row.tbv) : null,
        tsv: row.tsv ? parseFloat(row.tsv) : null
      })));
    } catch (error) {
      console.error(`Error fetching data for ${exchange} ${symbol}:`, error.message);
    }
  }

  if (allData.length === 0) return { bin: [], byb: [], okx: [] };

  // Group by ts and exchange; merge fields (unified: check field presence)
  const grouped = { bin: {}, byb: {}, okx: {} };
  
  allData.forEach(row => {
    const ts = row.ts;
    const exchange = row.exchange;

    // Initialize row for this ts/exchange
    if (!grouped[exchange][ts]) {
      grouped[exchange][ts] = { 
        ts, symbol: row.symbol, exchange,
        o: null, h: null, l: null, c: null, v: null,
        oi: null, pfr: null, lsr: null,
        rsi1: null, rsi60: null,
        tbv: null, tsv: null,
        lqside: null, lqprice: null, lqqty: null
      };
    }

    const targetRow = grouped[exchange][ts];

    // Merge based on field presence (unified schema)
    if (row.o !== null) { // OHLCV
      targetRow.o = row.o; targetRow.h = row.h; targetRow.l = row.l; 
      targetRow.c = row.c; targetRow.v = row.v; 
    }
    if (row.oi !== null) targetRow.oi = row.oi;
    if (row.pfr !== null) targetRow.pfr = row.pfr;
    if (row.lsr !== null) targetRow.lsr = row.lsr;
    if (row.rsi1 !== null) { // RSI (bin-only, but applied here if present)
      targetRow.rsi1 = row.rsi1; targetRow.rsi60 = row.rsi60;
    }
    if (row.tbv !== null) { targetRow.tbv = row.tbv; targetRow.tsv = row.tsv; }
    if (row.lqside !== null) { 
      targetRow.lqside = row.lqside; targetRow.lqprice = row.lqprice; targetRow.lqqty = row.lqqty; 
    }
  });

  return {
    bin: Object.values(grouped.bin).sort((a, b) => a.ts - b.ts),
    byb: Object.values(grouped.byb).sort((a, b) => a.ts - b.ts),
    okx: Object.values(grouped.okx).sort((a, b) => a.ts - b.ts)
  };
}

// ============================================================================
// CALCULATE METRICS FOR AN EXCHANGE DATASET
// ============================================================================
function calculateMetricsForExchange(data) {
  if (data.length === 0) return [];

  const metrics = [];
  data.sort((a, b) => a.ts - b.ts);

  for (let i = 0; i < data.length; i++) {
    const current = data[i];
    const metricRow = {
      ts: BigInt(current.ts),
      symbol: current.symbol,
      exchange: current.exchange,
      o: current.o, h: current.h, l: current.l, c: current.c, v: current.v,
      oi: current.oi, pfr: current.pfr, lsr: current.lsr,
      rsi1: current.rsi1, rsi60: current.rsi60,
      tbv: current.tbv, tsv: current.tsv,
      lqside: current.lqside, lqprice: current.lqprice, lqqty: current.lqqty,
      // 1m changes
      c_chg_1m: null, v_chg_1m: null, oi_chg_1m: null, pfr_chg_1m: null, lsr_chg_1m: null,
      rsi1_chg_1m: null, rsi60_chg_1m: null, tbv_chg_1m: null, tsv_chg_1m: null,
      lqside_chg_1m: null, lqprice_chg_1m: null, lqqty_chg_1m: null,
      // 5m changes
      c_chg_5m: null, v_chg_5m: null, oi_chg_5m: null, pfr_chg_5m: null, lsr_chg_5m: null,
      rsi1_chg_5m: null, rsi60_chg_5m: null, tbv_chg_5m: null, tsv_chg_5m: null,
      lqside_chg_5m: null, lqprice_chg_5m: null, lqqty_chg_5m: null,
      // 10m changes
      c_chg_10m: null, v_chg_10m: null, oi_chg_10m: null, pfr_chg_10m: null, lsr_chg_10m: null,
      rsi1_chg_10m: null, rsi60_chg_10m: null, tbv_chg_10m: null, tsv_chg_10m: null,
      lqside_chg_10m: null, lqprice_chg_10m: null, lqqty_chg_10m: null
    };

    // 1m changes
    if (i >= 1) {
      const prev = data[i - 1];
      metricRow.c_chg_1m = calculatePercentChange(current.c, prev.c);
      metricRow.v_chg_1m = calculatePercentChange(current.v, prev.v);
      metricRow.oi_chg_1m = calculatePercentChange(current.oi, prev.oi);
      metricRow.pfr_chg_1m = calculatePercentChange(current.pfr, prev.pfr);
      metricRow.lsr_chg_1m = calculatePercentChange(current.lsr, prev.lsr);
      metricRow.rsi1_chg_1m = calculatePercentChange(current.rsi1, prev.rsi1);
      metricRow.rsi60_chg_1m = calculatePercentChange(current.rsi60, prev.rsi60);
      metricRow.tbv_chg_1m = calculatePercentChange(current.tbv, prev.tbv);
      metricRow.tsv_chg_1m = calculatePercentChange(current.tsv, prev.tsv);
      metricRow.lqside_chg_1m = (current.lqside !== prev.lqside) ? current.lqside : null;
      metricRow.lqprice_chg_1m = calculatePercentChange(current.lqprice, prev.lqprice);
      metricRow.lqqty_chg_1m = calculatePercentChange(current.lqqty, prev.lqqty);
    }

    // 5m changes
    if (i >= 5) {
      const prev = data[i - 5];
      metricRow.c_chg_5m = calculatePercentChange(current.c, prev.c);
      metricRow.v_chg_5m = calculatePercentChange(current.v, prev.v);
      metricRow.oi_chg_5m = calculatePercentChange(current.oi, prev.oi);
      metricRow.pfr_chg_5m = calculatePercentChange(current.pfr, prev.pfr);
      metricRow.lsr_chg_5m = calculatePercentChange(current.lsr, prev.lsr);
      metricRow.rsi1_chg_5m = calculatePercentChange(current.rsi1, prev.rsi1);
      metricRow.rsi60_chg_5m = calculatePercentChange(current.rsi60, prev.rsi60);
      metricRow.tbv_chg_5m = calculatePercentChange(current.tbv, prev.tbv);
      metricRow.tsv_chg_5m = calculatePercentChange(current.tsv, prev.tsv);
      metricRow.lqside_chg_5m = (current.lqside !== prev.lqside) ? current.lqside : null;
      metricRow.lqprice_chg_5m = calculatePercentChange(current.lqprice, prev.lqprice);
      metricRow.lqqty_chg_5m = calculatePercentChange(current.lqqty, prev.lqqty);
      // Window majority for lqside_chg_5m (over last 5 rows, including current)
      const window5 = data.slice(Math.max(0, i - 4), i + 1);
      metricRow.lqside_chg_5m = getWindowMajoritySide(window5);
    }

    // 10m changes
    if (i >= 10) {
      const prev = data[i - 10];
      metricRow.c_chg_10m = calculatePercentChange(current.c, prev.c);
      metricRow.v_chg_10m = calculatePercentChange(current.v, prev.v);
      metricRow.oi_chg_10m = calculatePercentChange(current.oi, prev.oi);
      metricRow.pfr_chg_10m = calculatePercentChange(current.pfr, prev.pfr);
      metricRow.lsr_chg_10m = calculatePercentChange(current.lsr, prev.lsr);
      metricRow.rsi1_chg_10m = calculatePercentChange(current.rsi1, prev.rsi1);
      metricRow.rsi60_chg_10m = calculatePercentChange(current.rsi60, prev.rsi60);
      metricRow.tbv_chg_10m = calculatePercentChange(current.tbv, prev.tbv);
      metricRow.tsv_chg_10m = calculatePercentChange(current.tsv, prev.tsv);
      metricRow.lqside_chg_10m = (current.lqside !== prev.lqside) ? current.lqside : null;
      metricRow.lqprice_chg_10m = calculatePercentChange(current.lqprice, prev.lqprice);
      metricRow.lqqty_chg_10m = calculatePercentChange(current.lqqty, prev.lqqty);
      // Window majority for lqside_chg_10m (over last 10 rows, including current)
      const window10 = data.slice(Math.max(0, i - 9), i + 1);
      metricRow.lqside_chg_10m = getWindowMajoritySide(window10);
    }

    metrics.push(metricRow);
  }

  return metrics;
}

// ============================================================================
// PROCESS SINGLE SYMBOL (for parallel execution)
// ============================================================================
async function processSymbol(symbol, startTs, endTs) {
  try {
    const aggregatedData = await fetchAndAggregateData(symbol, startTs, endTs);
    let metricsInserted = 0;
    let allExchangeMetrics = [];  // Collect metrics from all exchanges for this symbol

    for (const exchange of EXCHANGES) {
      const data = aggregatedData[exchange];
      if (data.length === 0) continue;

      const metrics = calculateMetricsForExchange(data);
      if (metrics.length > 0) {
        allExchangeMetrics = allExchangeMetrics.concat(metrics);  // Collect per-exchange metrics
        metricsInserted += metrics.length;  // Update count
      }
    }

    return { success: true, metrics: allExchangeMetrics, metricsInserted, symbol };
  } catch (error) {
    console.error(`❌ Error processing ${symbol}:`, error.message);
    await dbManager.logError(SCRIPT_NAME, 'calculation_error', 'CALC_SYMBOL_FAIL', 
      error.message, { symbol, error: error.stack });
    return { success: false, metrics: [], metricsInserted: 0, symbol };
  }
}

// ============================================================================
// MAIN CALCULATION FUNCTION
// Parallel processing with minimal console output
// ============================================================================
async function calculateAllMetrics() {
  const startTime = Date.now();
  const now = Date.now();
  const startTs = now - (LOOKBACK_MINUTES * 60 * 1000);
  const endTs = now;

  try {
    let totalMetricsCalculated = 0;
    let successCount = 0;
    let errorCount = 0;

    // Parallel processing with p-limit
    const limit = pLimit(PARALLEL_SYMBOLS);
    const tasks = perpList.map(symbol => 
      limit(async () => {
        const result = await processSymbol(symbol, startTs, endTs);
        return result;
      })
    );

    const results = await Promise.all(tasks);

    // Collect all metrics globally
    let allMetrics = [];
    results.forEach(result => {
      if (result.success) {
        successCount++;
        totalMetricsCalculated += result.metricsInserted;
        if (result.metrics && result.metrics.length > 0) {
          allMetrics = allMetrics.concat(result.metrics);
        }
      } else {
        errorCount++;
      }
    });

    // Sort the collected metrics globally (ts asc, symbol asc, exchange asc)
    allMetrics.sort((a, b) => {
      if (a.ts !== b.ts) return Number(a.ts) - Number(b.ts);  // ts ascending (BigInt to Number)
      if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);  // symbol ascending
      return a.exchange.localeCompare(b.exchange);  // exchange ascending
    });

    // Single insert for all sorted metrics
    if (allMetrics.length > 0) {
      const globalInsertResult = await dbManager.insertMetrics(allMetrics);
      totalMetricsCalculated = globalInsertResult.rowCount || allMetrics.length;  // Update total from global insert
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    const success = errorCount === 0;

    if (success) {
      await logStatus('running', 
        `Calc complete: ${totalMetricsCalculated} metrics in ${duration}s (${successCount}/${perpList.length} symbols)`);
    } else {
      await logStatus('error', 
        `Calc completed with ${errorCount} errors: ${totalMetricsCalculated} metrics in ${duration}s`);
    }

    return { success, metricsCalculated: totalMetricsCalculated, errors: errorCount };

  } catch (error) {
    console.error('❌ Metrics calculation failed:', error);
    await dbManager.logError(SCRIPT_NAME, 'calculation_error', 'CALC_FAIL', 
      error.message, { error: error.stack });
    throw error;
  }
}

// ============================================================================
// CONTINUOUS RUN MODE (Updated - Remove Duplicate Listeners)
// ============================================================================
async function runContinuously() {
  console.log(`\n🔄 Starting ${SCRIPT_NAME} in continuous mode...`);
  console.log(`📊 Calculation interval: ${CALCULATION_INTERVAL_MS / 1000}s`);
  console.log(`📅 Database retention: ${DB_RETENTION_DAYS} days\n`);

  // Run backfill first
  const backfiller = require('./backfill-metrics');
  try {
    console.log('🔄 Running backfill before starting real-time mode...\n');
    await backfiller.runBackfill();
    console.log('\n✅ Backfill complete - starting real-time metrics...\n');
  } catch (error) {
    const summary = `Backfill failed: ${error.code} - ${error.message} (line ${error.position || 'unknown'})`;
    console.error('💥', summary);  // Short summary
    await dbManager.logError(SCRIPT_NAME, 'backfill_error', 'BACKFILL_FAIL', 
      summary, { code: error.code, position: error.position });  // Log details
  }

  await logStatus('running', `${SCRIPT_NAME} continuous mode started.`);

  // Run calculation every minute
  runInterval = setInterval(async () => {
    try {
      await calculateAllMetrics();
    } catch (error) {
      console.error('⚠️  Calculation cycle error:', error.message);
    }
  }, CALCULATION_INTERVAL_MS);

  // Graceful cleanup (moved listeners to top-level)
  const cleanup = (signal) => {
    clearInterval(runInterval);
    gracefulShutdown(signal);
  };
  // Remove old process.on calls from here - already at top-level
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================
if (require.main === module) {
  const args = process.argv.slice(2);
  const continuousMode = args.includes('--continuous') || args.includes('-c');

  if (continuousMode) {
    runContinuously().catch(err => {
      console.error('💥 Continuous mode failed:', err);
      process.exit(1);
    });
  } else {
    calculateAllMetrics()
      .then(() => {
        console.log('✅ Metrics calculation completed successfully');
        process.exit(0);
      })
      .catch(err => {
        console.error('💥 Metrics calculation failed:', err);
        process.exit(1);
      });
  }
}

module.exports = { calculateAllMetrics, runContinuously, stopContinuously: gracefulShutdown };
