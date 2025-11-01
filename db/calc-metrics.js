// ============================================================================
// METRICS CALCULATOR  calc-metrics.js  31 Oct 2025 (MEMORY OPTIMIZED)
// Calculates rolling % changes for all parameters across all exchanges (bin, byb, okx)
// Runs every 1 minute to keep perp_metrics table fresh for backtester
// Uses unified queries (by exchange + field presence); ON CONFLICT DO UPDATE for upserts
//
// MEMORY FIX: Batch inserts per N symbols instead of accumulating all globally
// ============================================================================

const dbManager = require('./dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = 'calc-metrics.js';
const STATUS_LOG_COLOR = '\x1b[38;2;147;112;219m'; // Purple
const COLOR_RESET = '\x1b[0m';

// ============================================================================
// USER CONFIGURATION - TUNE FOR YOUR SYSTEM
// ============================================================================
const DB_RETENTION_DAYS = 10;
const LOOKBACK_MINUTES = 15;
const CALCULATION_INTERVAL_MS = 60000; // 1 minute
const HEARTBEAT_INTERVAL_MS = 15000;   // 1 minute status update

// BATCH CONTROL - Adjust these to balance speed vs memory:
// - SYMBOL_BATCH_SIZE: How many symbols to process before inserting (1 = insert per symbol, 3 = every 3 symbols)
// - INSERT_CHUNK_SIZE: Max rows per DB insert (not used in calc-metrics, kept for consistency)
// - PARALLEL_SYMBOLS: How many symbols to process at once (more = faster but more memory)
const SYMBOL_BATCH_SIZE = 2;      // Insert after every N symbols (1-6 recommended; 2=balanced)
const PARALLEL_SYMBOLS = 3;       // Concurrent symbols (2-8 recommended; 3=balanced for continuous)

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
  return Math.min(Math.max(parseFloat(change.toFixed(3)), -9999.999), 9999.999);
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
             lql, lqs, rsi1, rsi60, tbv, tsv
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
        lql: row.lql ? parseFloat(row.lql) : null,
        lqs: row.lqs ? parseFloat(row.lqs) : null,
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

  // Group by exchange (no merging - use raw rows like backfill)
  const grouped = { bin: [], byb: [], okx: [] };
  
  allData.forEach(row => {
    grouped[row.exchange].push(row);
  });

  return {
    bin: grouped.bin.sort((a, b) => a.ts - b.ts),
    byb: grouped.byb.sort((a, b) => a.ts - b.ts),
    okx: grouped.okx.sort((a, b) => a.ts - b.ts)
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
      lql: current.lql, lqs: current.lqs,
      // 1m changes
      c_chg_1m: null, v_chg_1m: null, oi_chg_1m: null, pfr_chg_1m: null, lsr_chg_1m: null,
      rsi1_chg_1m: null, rsi60_chg_1m: null, tbv_chg_1m: null, tsv_chg_1m: null,
      lql_chg_1m: null, lqs_chg_1m: null,
      // 5m changes
      c_chg_5m: null, v_chg_5m: null, oi_chg_5m: null, pfr_chg_5m: null, lsr_chg_5m: null,
      rsi1_chg_5m: null, rsi60_chg_5m: null, tbv_chg_5m: null, tsv_chg_5m: null,
      lql_chg_5m: null, lqs_chg_5m: null,
      // 10m changes
      c_chg_10m: null, v_chg_10m: null, oi_chg_10m: null, pfr_chg_10m: null, lsr_chg_10m: null,
      rsi1_chg_10m: null, rsi60_chg_10m: null, tbv_chg_10m: null, tsv_chg_10m: null,
      lql_chg_10m: null, lqs_chg_10m: null
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
      metricRow.lql_chg_1m = calculatePercentChange(current.lql, prev.lql);
      metricRow.lqs_chg_1m = calculatePercentChange(current.lqs, prev.lqs);
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
      metricRow.lql_chg_5m = calculatePercentChange(current.lql, prev.lql);
      metricRow.lqs_chg_5m = calculatePercentChange(current.lqs, prev.lqs);
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
      metricRow.lql_chg_10m = calculatePercentChange(current.lql, prev.lql);
      metricRow.lqs_chg_10m = calculatePercentChange(current.lqs, prev.lqs);
    }

    metrics.push(metricRow);
  }

  return metrics;
}

// ============================================================================
// PROCESS SINGLE SYMBOL (Returns metrics for batching)
// ============================================================================
async function processSymbol(symbol, startTs, endTs) {
  try {
    const aggregatedData = await fetchAndAggregateData(symbol, startTs, endTs);
    let allExchangeMetrics = [];

    for (const exchange of EXCHANGES) {
      const data = aggregatedData[exchange];
      if (data.length === 0) continue;

      const metrics = calculateMetricsForExchange(data);
      if (metrics.length > 0) {
        allExchangeMetrics = allExchangeMetrics.concat(metrics);
      }
    }

    return { success: true, metrics: allExchangeMetrics, symbol };
  } catch (error) {
    console.error(`❌ Error processing ${symbol}:`, error.message);
    await dbManager.logError(SCRIPT_NAME, 'calculation_error', 'CALC_SYMBOL_FAIL', 
      error.message, { symbol, error: error.stack });
    return { success: false, metrics: [], symbol };
  }
}

// ============================================================================
// MAIN CALCULATION FUNCTION (Batch inserts every N symbols)
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
    let pendingMetrics = []; // Accumulator for batch

    // Process symbols sequentially with batching
    for (let i = 0; i < perpList.length; i++) {
      const symbol = perpList[i];
      const result = await processSymbol(symbol, startTs, endTs);

      if (result.success) {
        successCount++;
        pendingMetrics = pendingMetrics.concat(result.metrics);
      } else {
        errorCount++;
      }

      // Insert when batch is full or at end of list
      const isBatchFull = pendingMetrics.length > 0 && ((i + 1) % SYMBOL_BATCH_SIZE === 0 || i === perpList.length - 1);
      
      if (isBatchFull) {
        // Sort batch before insert
        pendingMetrics.sort((a, b) => {
          if (a.ts !== b.ts) return Number(a.ts) - Number(b.ts);
          if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
          return a.exchange.localeCompare(b.exchange);
        });

        const globalInsertResult = await dbManager.insertMetrics(pendingMetrics);
        totalMetricsCalculated += globalInsertResult.rowCount || pendingMetrics.length;
        pendingMetrics = []; // Clear batch
      }
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
// CONTINUOUS RUN MODE
// ============================================================================
async function runContinuously() {
  console.log(`\n🔄 Starting ${SCRIPT_NAME} in continuous mode...`);
  console.log(`📊 Calculation interval: ${CALCULATION_INTERVAL_MS / 1000}s`);
  console.log(`📅 Database retention: ${DB_RETENTION_DAYS} days`);
  console.log(`📦 Symbol batch size: ${SYMBOL_BATCH_SIZE} (insert every ${SYMBOL_BATCH_SIZE} symbols)`);
  console.log(`⚡ Parallel symbols: ${PARALLEL_SYMBOLS}\n`);

  // Run backfill first
  const backfiller = require('./backfill-metrics');
  try {
    console.log('🔄 Running backfill before starting real-time mode...\n');
    await backfiller.runBackfill();
    console.log('\n✅ Backfill complete - starting real-time metrics...\n');
  } catch (error) {
    const summary = `Backfill failed: ${error.code} - ${error.message} (line ${error.position || 'unknown'})`;
    console.error('💥', summary);
    await dbManager.logError(SCRIPT_NAME, 'backfill_error', 'BACKFILL_FAIL', 
      summary, { code: error.code, position: error.position });
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