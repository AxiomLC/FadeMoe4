// ============================================================================
// BACKFILL METRICS MODULE  backfill-metrics.js  17 Oct 2025 (OPTIMIZED)
// Backfills perp_metrics from perp_data using ON CONFLICT DO NOTHING
// No gap detection - just fills from oldest existing record forward
// Runs once on startup, then signals 'completed' for calc-metrics handover
// ============================================================================

const dbManager = require('./dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = 'backfill-metrics.js';
const STATUS_LOG_COLOR = '\x1b[38;2;147;112;219m'; // Purple
const COLOR_RESET = '\x1b[0m';

// ============================================================================
// USER CONFIGURATION
// ============================================================================
const DB_RETENTION_DAYS = 10;        // Must match calc-metrics.js
const WINDOW_SIZES = [1, 5, 10];     // Must match calc-metrics.js
const BATCH_SIZE = 1000;             // Increased for performance
const PARALLEL_SYMBOLS = 8;          // Process 8 symbols at once
const HEARTBEAT_INTERVAL_MS = 15000; // 15s heartbeat

const EXCHANGES = ['bin', 'byb', 'okx'];

// ============================================================================
// UTILITIES
// ============================================================================
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
// GET BACKFILL START TIMESTAMP
// Finds oldest ts in perp_metrics for this symbol/exchange, or retention start
// ============================================================================
async function getBackfillStartTs(symbol, exchange, retentionStart) {
  const query = `
    SELECT MIN(ts) as min_ts FROM perp_metrics
    WHERE symbol = $1 AND exchange = $2
  `;
  try {
    const result = await dbManager.pool.query(query, [symbol, exchange]);
    const minTs = result.rows[0]?.min_ts;
    return minTs ? Number(minTs) : retentionStart;
  } catch (error) {
    console.error(`Error getting start ts for ${symbol} ${exchange}:`, error.message);
    return retentionStart;
  }
}

// ============================================================================
// FETCH AND AGGREGATE DATA FOR BACKFILL
// ============================================================================
async function fetchAndAggregateDataForBackfill(symbol, exchange, startTs, endTs) {
  const lookbackBuffer = 10 * 60 * 1000; // 10min buffer for windows
  const bufferedStart = startTs - lookbackBuffer;

  const allPerpspecs = [
    `${exchange}-ohlcv`, `${exchange}-oi`, `${exchange}-pfr`, 
    `${exchange}-lsr`, `${exchange}-tv`, `${exchange}-lq`,
    'rsi'
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
      const result = await dbManager.pool.query(query, [symbol, perpspec, BigInt(bufferedStart), BigInt(endTs)]);
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
      console.error(`Error fetching ${perpspec} for backfill ${symbol}:`, error.message);
    }
  }

  if (allData.length === 0) return [];

  // Group and merge by ts/exchange
  const grouped = {};
  allData.forEach(row => {
    const ts = row.ts;
    if (!grouped[ts]) {
      grouped[ts] = {
        ts,
        symbol: row.symbol,
        exchange,
        o: null, h: null, l: null, c: null, v: null, oi: null, pfr: null, lsr: null,
        lqside: null, lqprice: null, lqqty: null, rsi1: null, rsi60: null, tbv: null, tsv: null
      };
    }
    const targetRow = grouped[ts];

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
    if (row.perpspec === 'rsi') { targetRow.rsi1 = row.rsi1; targetRow.rsi60 = row.rsi60; }
  });

  return Object.values(grouped).filter(row => row.ts >= startTs).sort((a, b) => a.ts - b.ts);
}

// ============================================================================
// CALCULATE METRICS FOR BACKFILL
// ============================================================================
function calculateMetricsForBackfill(data, gapStartTs) {
  if (data.length === 0) return [];

  const metrics = [];
  data.sort((a, b) => a.ts - b.ts);

  for (let i = 0; i < data.length; i++) {
    const current = data[i];
    if (current.ts < gapStartTs) continue;

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
    }

    metrics.push(metricRow);
  }

  return metrics;
}

// ============================================================================
// BACKFILL SINGLE SYMBOL/EXCHANGE
// ============================================================================
async function backfillSymbolExchange(symbol, exchange, retentionStart, endTs) {
  try {
    const startTs = await getBackfillStartTs(symbol, exchange, retentionStart);
    
    // Skip if already up to date
    if (startTs >= endTs) return { records: 0, skipped: true };

    const data = await fetchAndAggregateDataForBackfill(symbol, exchange, startTs, endTs);
    if (data.length === 0) return { records: 0, skipped: false };

    const metrics = calculateMetricsForBackfill(data, startTs);
    if (metrics.length === 0) return { records: 0, skipped: false };

    // Batch insert with ON CONFLICT DO NOTHING
    let totalInserted = 0;
    for (let i = 0; i < metrics.length; i += BATCH_SIZE) {
      const batch = metrics.slice(i, i + BATCH_SIZE);
      const result = await dbManager.insertMetrics(batch);
      totalInserted += result.rowCount || 0;
    }

    return { records: totalInserted, skipped: false };
  } catch (error) {
    console.error(`Error backfilling ${symbol} ${exchange}:`, error.message);
    await dbManager.logError(SCRIPT_NAME, 'backfill_error', 'BACKFILL_FAIL', error.message, 
      { symbol, exchange, error: error.stack });
    return { records: 0, skipped: false, error: true };
  }
}

// ============================================================================
// MAIN BACKFILL FUNCTION
// Parallel processing with heartbeat progress
// ============================================================================
async function runBackfill() {
  const startTime = Date.now();
  const now = Date.now();
  const retentionStart = now - (DB_RETENTION_DAYS * 24 * 60 * 60 * 1000);
  const endTs = now;

  console.log(`\n${'='.repeat(80)}`);
  console.log(`🔧 PERP_METRICS BACKFILL - Starting optimized backfill`);
  console.log(`${'='.repeat(80)}\n`);

  await logStatus('started', `${SCRIPT_NAME} started - backfilling perp_metrics.`);

  try {
    let totalRecords = 0;
    let processedCount = 0;
    let skippedCount = 0;
    let errorCount = 0;
    const totalTasks = perpList.length * EXCHANGES.length;

    // Heartbeat for progress
    const heartbeatInterval = setInterval(() => {
      const progress = ((processedCount / totalTasks) * 100).toFixed(1);
      logStatus('running', 
        `Backfilling: ${processedCount}/${totalTasks} (${progress}%) - ${totalRecords} records inserted`);
    }, HEARTBEAT_INTERVAL_MS);

    // Parallel processing with p-limit
    const limit = pLimit(PARALLEL_SYMBOLS);
    const tasks = [];

    for (const symbol of perpList) {
      for (const exchange of EXCHANGES) {
        tasks.push(
          limit(async () => {
            const result = await backfillSymbolExchange(symbol, exchange, retentionStart, endTs);
            processedCount++;
            if (result.skipped) skippedCount++;
            if (result.error) errorCount++;
            totalRecords += result.records;
            return result;
          })
        );
      }
    }

    await Promise.all(tasks);
    clearInterval(heartbeatInterval);

    // Final summary
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    await logStatus('completed', 
      `Backfill complete: ${totalRecords} records inserted, ${skippedCount} skipped, ${errorCount} errors in ${duration}s`);

    console.log(`\n${'='.repeat(80)}`);
    console.log(`✅ BACKFILL COMPLETE`);
    console.log(`   Tasks processed: ${processedCount}/${totalTasks}`);
    console.log(`   Records inserted: ${totalRecords}`);
    console.log(`   Already current: ${skippedCount}`);
    console.log(`   Errors: ${errorCount}`);
    console.log(`   Duration: ${duration}s`);
    console.log(`   Status: Ready for real-time calc-metrics.js`);
    console.log(`${'='.repeat(80)}\n`);

    return { recordsInserted: totalRecords, tasksProcessed: processedCount, errors: errorCount };

  } catch (error) {
    console.error('\n❌ Backfill failed:', error);
    await dbManager.logError(SCRIPT_NAME, 'backfill_error', 'BACKFILL_FAIL', error.message, 
      { error: error.stack });
    await logStatus('error', `Backfill failed: ${error.message}`);
    throw error;
  }
}

// ============================================================================
// GRACEFUL SHUTDOWN
// ============================================================================
async function gracefulShutdown(signal) {
  console.log(`\n⚠️  Received ${signal}, shutting down gracefully...`);
  await logStatus('stopped', `${SCRIPT_NAME} stopped by ${signal}.`);
  process.exit(0);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================
if (require.main === module) {
  runBackfill()
    .then(() => {
      console.log('✅ Backfill module completed successfully');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 Backfill module failed:', err);
      process.exit(1);
    });
}

module.exports = { runBackfill };