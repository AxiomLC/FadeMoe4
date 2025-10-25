// ============================================================================
// BACKFILL METRICS MODULE  backfill-metrics.js  25 Oct 2025 (SIMPLIFIED - MATCHES CALC-METRICS)
// Backfills perp_metrics from perp_data: Fetch all fields, calc % changes for available params (nulls skip), chunk insert with upsert.
// No special screening (RSI/LQ/TV calc if data present). For empty history, inserts raw + _chg_ per row.
// Runs once on startup, signals 'completed' for calc-metrics handover.
// 
// NOTES:
// - RSI only on Binance (bin-rsi); Bybit/OKX omit RSI (no errors thrown)
// - TV (tbv/tsv) on Binance reliable; Bybit/OKX sparse (no errors thrown)
// - LQ (lqside/lqprice/lqqty) on Binance only (no errors thrown for others)
// - Expected fill rates: bin/byb/okx OHLCV+PFR ~80%, bin/byb OI+LSR ~80%, okx OI+LSR ~40%
// ============================================================================

const dbManager = require('./dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = 'backfill-metrics.js';
const STATUS_LOG_COLOR = '\x1b[38;2;147;112;219m'; // Purple
const COLOR_RESET = '\x1b[0m';

const DB_RETENTION_DAYS = 10;
const CHUNK_SIZE = 100000;
const MAX_RETRIES = 2;
const BUFFER_MS = 15 * 60 * 1000; // 15min buffer for window calcs
const PARALLEL_SYMBOLS = 8;
const HEARTBEAT_INTERVAL_MS = 15000;

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
  return Math.min(Math.max(parseFloat(change.toFixed(3)), -9999.999), 9999.999); // Changed from 4 decimals to 3
}

function getWindowMajoritySide(windowData) {
  if (!windowData || windowData.length === 0) return null;
  let longCount = 0, shortCount = 0;
  let longQty = 0, shortQty = 0;
  for (const row of windowData) {
    if (row.lqside === 'long') { longCount++; if (row.lqqty) longQty += row.lqqty; }
    else if (row.lqside === 'short') { shortCount++; if (row.lqqty) shortQty += row.lqqty; }
  }
  if (longCount > shortCount) return 'long';
  if (shortCount > longCount) return 'short';
  if (longQty > shortQty) return 'long';
  if (shortQty > longQty) return 'short';
  return null;
}

async function retryOperation(operation, ...args) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await operation(...args);
    } catch (error) {
      if (attempt < MAX_RETRIES) {
        console.warn(`${STATUS_LOG_COLOR}Retry ${attempt}/${MAX_RETRIES}: ${error.message}${COLOR_RESET}`);
        await sleep(500 * attempt);
        continue;
      }
      throw error;
    }
  }
}

// ============================================================================
// GET BACKFILL START TIMESTAMP
// ============================================================================
async function getBackfillStartTs(symbol, exchange, retentionStart) {
  const query = `
    SELECT MIN(ts) as min_ts FROM perp_metrics
    WHERE symbol = $1 AND exchange = $2
  `;
  try {
    const result = await retryOperation(dbManager.query.bind(dbManager), query, [symbol, exchange]);
    const minTs = result.rows[0]?.min_ts;
    return minTs ? Number(minTs) : retentionStart;
  } catch (error) {
    console.error(`\x1b[33mWarning: Could not get start ts for ${symbol}-${exchange}: ${error.message}${COLOR_RESET}`);
    return retentionStart;
  }
}

// ============================================================================
// FETCH AND AGGREGATE DATA (Copy from calc-metrics)
// ============================================================================
async function fetchAndAggregateDataForBackfill(symbol, startTs, endTs) {
  let allData = [];

  for (const exchange of EXCHANGES) {
    const baseQuery = `
      SELECT ts, symbol, exchange, o, h, l, c, v, oi, pfr, lsr, 
             lqside, lqprice, lqqty, rsi1, rsi60, tbv, tsv
      FROM perp_data
      WHERE symbol = $1 AND exchange = $2 AND ts >= $3 AND ts <= $4
      ORDER BY ts ASC
    `;
    try {
      const bufferedStart = BigInt(startTs - BUFFER_MS);
      const result = await retryOperation(dbManager.query.bind(dbManager), baseQuery, [symbol, exchange, bufferedStart, BigInt(endTs)]);
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
      console.error(`\x1b[33mWarning: Fetch error for ${exchange} ${symbol}: ${error.message}${COLOR_RESET}`);
    }
  }

  if (allData.length === 0) return { bin: [], byb: [], okx: [] };

  // Group by ts and exchange; merge fields
  const grouped = { bin: {}, byb: {}, okx: {} };
  allData.forEach(row => {
    const ts = row.ts;
    const exchange = row.exchange;
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
    if (row.o !== null) { targetRow.o = row.o; targetRow.h = row.h; targetRow.l = row.l; targetRow.c = row.c; targetRow.v = row.v; }
    if (row.oi !== null) targetRow.oi = row.oi;
    if (row.pfr !== null) targetRow.pfr = row.pfr;
    if (row.lsr !== null) targetRow.lsr = row.lsr;
    if (row.rsi1 !== null) { targetRow.rsi1 = row.rsi1; targetRow.rsi60 = row.rsi60; }
    if (row.tbv !== null) { targetRow.tbv = row.tbv; targetRow.tsv = row.tsv; }
    if (row.lqside !== null) { targetRow.lqside = row.lqside; targetRow.lqprice = row.lqprice; targetRow.lqqty = row.lqqty; }
  });

  // Filter to startTs+ (drop buffer)
  return {
    bin: Object.values(grouped.bin).filter(r => r.ts >= startTs).sort((a, b) => a.ts - b.ts),
    byb: Object.values(grouped.byb).filter(r => r.ts >= startTs).sort((a, b) => a.ts - b.ts),
    okx: Object.values(grouped.okx).filter(r => r.ts >= startTs).sort((a, b) => a.ts - b.ts)
  };
}

// ============================================================================
// CALCULATE METRICS FOR EXCHANGE (Copy from calc-metrics)
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
      c_chg_1m: null, v_chg_1m: null, oi_chg_1m: null, pfr_chg_1m: null, lsr_chg_1m: null,
      rsi1_chg_1m: null, rsi60_chg_1m: null, tbv_chg_1m: null, tsv_chg_1m: null,
      lqside_chg_1m: null, lqprice_chg_1m: null, lqqty_chg_1m: null,
      c_chg_5m: null, v_chg_5m: null, oi_chg_5m: null, pfr_chg_5m: null, lsr_chg_5m: null,
      rsi1_chg_5m: null, rsi60_chg_5m: null, tbv_chg_5m: null, tsv_chg_5m: null,
      lqside_chg_5m: null, lqprice_chg_5m: null, lqqty_chg_5m: null,
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
      const window5 = data.slice(Math.max(0, i - 4), i + 1);
      metricRow.lqside_chg_5m = getWindowMajoritySide(window5);
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
      const window10 = data.slice(Math.max(0, i - 9), i + 1);
      metricRow.lqside_chg_10m = getWindowMajoritySide(window10);
      metricRow.lqprice_chg_10m = calculatePercentChange(current.lqprice, prev.lqprice);
      metricRow.lqqty_chg_10m = calculatePercentChange(current.lqqty, prev.lqqty);
    }

    metrics.push(metricRow);
  }

  return metrics;
}

// ============================================================================
// BACKFILL SINGLE SYMBOL
// ============================================================================
async function backfillSymbolExchange(symbol, retentionStart, endTs) {
  try {
    let allMetrics = [];
    let records = 0;
    
    for (const exchange of EXCHANGES) {
      const startTs = await getBackfillStartTs(symbol, exchange, retentionStart);
      if (startTs >= endTs) continue;

      const aggregatedData = await fetchAndAggregateDataForBackfill(symbol, startTs, endTs);
      const data = aggregatedData[exchange];
      if (data.length === 0) continue;

      const metrics = calculateMetricsForExchange(data);
      if (metrics.length > 0) {
        allMetrics = allMetrics.concat(metrics);
        records += metrics.length;
      }
    }

    return { records, metrics: allMetrics, success: true, symbol };
  } catch (error) {
    console.error(`\x1b[31mError backfilling ${symbol}: ${error.message}${COLOR_RESET}`);
    await dbManager.logError(SCRIPT_NAME, 'backfill_error', 'BACKFILL_SYMBOL_FAIL', error.message, { symbol });
    return { records: 0, metrics: [], success: false, symbol };
  }
}

// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================
async function runBackfill() {
  const startTime = Date.now();
  const now = Date.now();
  const retentionStart = now - (DB_RETENTION_DAYS * 24 * 60 * 60 * 1000);
  const endTs = now;

  console.log(`\n${'='.repeat(80)}`);
  console.log(`🔧 PERP_METRICS BACKFILL - Starting simplified backfill`);
  console.log(`${'='.repeat(80)}\n`);

  await logStatus('started', `${SCRIPT_NAME} started - backfilling perp_metrics.`);

  let totalRecords = 0;
  let processedCount = 0;
  let errorCount = 0;
  const totalTasks = perpList.length;
  let allMetrics = [];

  try {
    const heartbeatInterval = setInterval(() => {
      const progress = ((processedCount / totalTasks) * 100).toFixed(1);
      logStatus('running', `Backfilling: ${processedCount}/${totalTasks} symbols (${progress}%) - ${totalRecords} records queued`);
    }, HEARTBEAT_INTERVAL_MS);

    const limit = pLimit(PARALLEL_SYMBOLS);
    const tasks = perpList.map(symbol => 
      limit(async () => {
        const result = await backfillSymbolExchange(symbol, retentionStart, endTs);
        processedCount++;
        if (result.success) {
          totalRecords += result.records;
          if (result.metrics && result.metrics.length > 0) {
            allMetrics = allMetrics.concat(result.metrics);
          }
        } else {
          errorCount++;
        }
        return result;
      })
    );

    await Promise.all(tasks);
    clearInterval(heartbeatInterval);

    // Sort global metrics
    allMetrics.sort((a, b) => {
      if (a.ts !== b.ts) return Number(a.ts) - Number(b.ts);
      if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
      return a.exchange.localeCompare(b.exchange);
    });

    // Chunked insert with upsert (matching calc-metrics)
    let globalInserted = 0;
    if (allMetrics.length > 0) {
      for (let i = 0; i < allMetrics.length; i += CHUNK_SIZE) {
        const chunk = allMetrics.slice(i, i + CHUNK_SIZE);
        try {
          const insertResult = await retryOperation(dbManager.insertMetrics.bind(dbManager), chunk);
          globalInserted += insertResult.rowCount || chunk.length;
        } catch (insertError) {
          console.error(`\x1b[31mInsert chunk fail: ${insertError.message}${COLOR_RESET}`);
          await dbManager.logError(SCRIPT_NAME, 'insert_error', 'CHUNK_INSERT_FAIL', insertError.message, { chunkSize: chunk.length });
          throw insertError;
        }
      }
      totalRecords = globalInserted;
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    const status = (errorCount / totalTasks < 0.1 && globalInserted > 0) ? 'completed' : 'partial';
    await logStatus(status, `Backfill ${status}: ${globalInserted} records, ${errorCount} errors in ${duration}s`);

    console.log(`\n${'='.repeat(80)}`);
    console.log(`✅ BACKFILL ${status.toUpperCase()}`);
    console.log(`   Symbols processed: ${processedCount}/${totalTasks}`);
    console.log(`   Records inserted: ${globalInserted}`);
    console.log(`   Errors: ${errorCount}`);
    console.log(`   Duration: ${duration}s`);
    console.log(`   Ready for calc-metrics.js`);
    console.log(`${'='.repeat(80)}\n`);

    return { status, recordsInserted: globalInserted, errors: errorCount };

  } catch (error) {
    console.error('\n💥 Backfill failed:', error);
    await dbManager.logError(SCRIPT_NAME, 'backfill_error', 'BACKFILL_FAIL', error.message);
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
  await dbManager.close();
  process.exit(0);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================
if (require.main === module) {
  runBackfill()
    .then(result => {
      console.log('✅ Backfill module completed:', result.status);
      process.exit(result.errors > 0 ? 1 : 0);
    })
    .catch(err => {
      console.error('💥 Backfill module failed:', err);
      process.exit(1);
    });
}

module.exports = { runBackfill };