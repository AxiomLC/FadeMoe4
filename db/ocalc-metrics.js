// ============================================================================
// METRICS CALCULATOR  calc-metrics.js  17 Oct 2025 (OPTIMIZED)
// Calculates rolling % changes for all parameters across all exchanges (bin, byb, okx)
// Runs every 1 minute to keep perp_metrics table fresh for backtester
// Uses ON CONFLICT DO UPDATE for upserts - no console spam
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

// ============================================================================
// FETCH AND AGGREGATE DATA BY EXCHANGE
// ============================================================================
async function fetchAndAggregateData(symbol, startTs, endTs) {
  const allPerpspecs = [
    ...EXCHANGES.map(ex => [`${ex}-ohlcv`, `${ex}-oi`, `${ex}-pfr`, `${ex}-lsr`, `${ex}-tv`, `${ex}-lq`]).flat(),
    'bin-rsi' //RSI edited 18 Oct to bin-rsi
  ];

  let allData = [];
  for (const perpspec of allPerpspecs) {
    const query = `
      SELECT ts, symbol, perpspec, o, h, l, c, v, oi, pfr, lsr, 
             lqside, lqprice, lqqty, rsi1, rsi60, tbv, tsv
      FROM perp_data
      WHERE symbol = $1 AND perpspec = $2 AND ts >= $3 AND ts <= $4
      ORDER BY ts ASC
    `;
    try {
      const result = await dbManager.pool.query(query, [symbol, perpspec, BigInt(startTs), BigInt(endTs)]);
      allData = allData.concat(result.rows.map(row => ({
        ts: Number(row.ts),
        symbol: row.symbol,
        perpspec: row.perpspec,
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
      console.error(`Error fetching ${perpspec} for ${symbol}:`, error.message);
    }
  }

  if (allData.length === 0) return { bin: [], byb: [], okx: [] };

  // Group by ts and exchange
  const grouped = { bin: {}, byb: {}, okx: {} };
  
  allData.forEach(row => {
    const ts = row.ts;
    const exchange = getExchangeFromPerpspec(row.perpspec);

    // Initialize all exchange rows for this ts
    if (!grouped.bin[ts]) grouped.bin[ts] = { ts, symbol: row.symbol, exchange: 'bin', ...defaultFields() };
    if (!grouped.byb[ts]) grouped.byb[ts] = { ts, symbol: row.symbol, exchange: 'byb', ...defaultFields() };
    if (!grouped.okx[ts]) grouped.okx[ts] = { ts, symbol: row.symbol, exchange: 'okx', ...defaultFields() };

    // Merge fields
    if (exchange) {
      const targetRow = grouped[exchange][ts];
      if (row.perpspec.includes('-ohlcv')) { 
        targetRow.o = row.o; targetRow.h = row.h; targetRow.l = row.l; 
        targetRow.c = row.c; targetRow.v = row.v; 
      }
      if (row.perpspec.includes('-oi')) targetRow.oi = row.oi;
      if (row.perpspec.includes('-pfr')) targetRow.pfr = row.pfr;
      if (row.perpspec.includes('-lsr')) targetRow.lsr = row.lsr;
      if (row.perpspec.includes('-tv')) { targetRow.tbv = row.tbv; targetRow.tsv = row.tsv; }
      if (row.perpspec.includes('-lq')) { 
        targetRow.lqside = row.lqside; targetRow.lqprice = row.lqprice; targetRow.lqqty = row.lqqty; 
      }
    }
    
    // Add RSI to all exchanges
    if (row.perpspec === 'bin-rsi') {  //edited 18 Oct for bin-rsi
      grouped.bin[ts].rsi1 = row.rsi1; grouped.bin[ts].rsi60 = row.rsi60;
      grouped.byb[ts].rsi1 = row.rsi1; grouped.byb[ts].rsi60 = row.rsi60;
      grouped.okx[ts].rsi1 = row.rsi1; grouped.okx[ts].rsi60 = row.rsi60;
    }
  });

  return {
    bin: Object.values(grouped.bin).sort((a, b) => a.ts - b.ts),
    byb: Object.values(grouped.byb).sort((a, b) => a.ts - b.ts),
    okx: Object.values(grouped.okx).sort((a, b) => a.ts - b.ts)
  };
}

function getExchangeFromPerpspec(perpspec) {
  if (perpspec.startsWith('bin-')) return 'bin';
  if (perpspec.startsWith('byb-')) return 'byb';
  if (perpspec.startsWith('okx-')) return 'okx';
  return null;
}

function defaultFields() {
  return {
    o: null, h: null, l: null, c: null, v: null, oi: null, pfr: null, lsr: null,
    lqside: null, lqprice: null, lqqty: null, rsi1: null, rsi60: null, tbv: null, tsv: null
  };
}

// ============================================================================
// CALCULATE METRICS FOR AN EXCHANGE DATASET
// ============================================================================

// ==========18 Oct +++Helper: Get majority lqside over a window (count + qty-weighted tie-breaker)
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
//=======================================================


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
      window_sizes: WINDOW_SIZES,
      o: current.o, h: current.h, l: current.l, c: current.c, v: current.v,
      oi: current.oi, pfr: current.pfr, lsr: current.lsr,
      lqside: current.lqside, lqprice: current.lqprice, lqqty: current.lqqty,
      rsi1: current.rsi1, rsi60: current.rsi60, tbv: current.tbv, tsv: current.tsv,
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
      // NEW: Window majority for lqside_chg_5m (over last 5 rows, including current)
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
      // NEW: Window majority for lqside_chg_10m (over last 10 rows, including current)
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

    for (const exchange of EXCHANGES) {
      const data = aggregatedData[exchange];
      if (data.length === 0) continue;

      const metrics = calculateMetricsForExchange(data);
      if (metrics.length > 0) {
        const result = await dbManager.insertMetrics(metrics);
        metricsInserted += result.rowCount || 0;
      }
    }

    return { success: true, metricsInserted, symbol };
  } catch (error) {
    console.error(`❌ Error processing ${symbol}:`, error.message);
    await dbManager.logError(SCRIPT_NAME, 'calculation_error', 'CALC_SYMBOL_FAIL', 
      error.message, { symbol, error: error.stack });
    return { success: false, metricsInserted: 0, symbol };
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
      limit(() => processSymbol(symbol, startTs, endTs))
    );

    const results = await Promise.all(tasks);

    // Aggregate results
    results.forEach(result => {
      if (result.success) {
        successCount++;
        totalMetricsCalculated += result.metricsInserted;
      } else {
        errorCount++;
      }
    });

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
// CONTINUOUS RUN MODE
// Runs backfill once, then continuous 1-minute calculations
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
    console.error('💥 Backfill failed:', error);
    await dbManager.logError(SCRIPT_NAME, 'backfill_error', 'BACKFILL_FAIL', 
      error.message, { error: error.stack });
  }

  await logStatus('running', `${SCRIPT_NAME} continuous mode started.`);

  // Run calculation every minute
  const runInterval = setInterval(async () => {
    try {
      await calculateAllMetrics();
    } catch (error) {
      console.error('⚠️  Calculation cycle error:', error.message);
    }
  }, CALCULATION_INTERVAL_MS);

  // Graceful cleanup
  const cleanup = (signal) => {
    clearInterval(runInterval);
    gracefulShutdown(signal);
  };

  process.on('SIGTERM', () => cleanup('SIGTERM'));
  process.on('SIGINT', () => cleanup('SIGINT'));
}

// ============================================================================
// GRACEFUL SHUTDOWN
// ============================================================================
async function gracefulShutdown(signal) {
  console.log(`\n⚠️  Received ${signal}, shutting down gracefully...`);
  await logStatus('stopped', `${SCRIPT_NAME} stopped by ${signal}.`);
  process.exit(0);
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