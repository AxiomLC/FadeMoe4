// ============================================================================
// METRICS BACKFILL - High-speed full _chg_ param calculation and insert
// Independent from calc-metrics.js, optimized for max throughput
// Simplified: always uses ON CONFLICT DO UPDATE with conditional update on detect column
// ============================================================================

const dbManager = require('./dbsetup');
const perpList = require('../perp-list');
const pLimit = require('p-limit');
const format = require('pg-format');
const apiUtils = require('../api-utils');
const SCRIPT_NAME = 'backfill-metrics.js';

const STATUS_LOG_COLOR = '\x1b[35m'; // Bright cyan
const COLOR_RESET = '\x1b[0m';

// ============================================================================
// USER CONFIGURATION - MAX SPEED SETTINGS & DETECT COLUMN
// ============================================================================

// Backfill retention and buffer
const DB_RETENTION_DAYS = 10;
const BUFFER_MS = 10 * 60 * 1000; // 10min buffer for window calcs

// Performance tuning
const INSERT_CHUNK_SIZE = 6000;    // Large chunk size for max throughput
const PARALLEL_SYMBOLS = 6;        // High concurrency
const INTER_CHUNK_DELAY_MS = 0;    // No delay between chunks
const HEARTBEAT_INTERVAL_MS = 7000;

// Column to detect if row is populated and skip update if not null
const DETECT_COLUMN = 'c_chg_1m';
const symbolsToBackfill = perpList.includes('MT') ? perpList : [...perpList, 'MT'];
// ============================================================================
// UTILITIES
// ============================================================================

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function logStatus(status, message) {
  try {
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, status, message);
  } catch {
    // Ignore logging errors to avoid slowing down main process
  }
  console.log(`${STATUS_LOG_COLOR}${message}${COLOR_RESET}`);
}

function calculatePercentChange(current, previous) {
  if (previous === null || previous === undefined || previous === 0) return null;
  if (current === null || current === undefined) return null;
  const change = ((current - previous) / Math.abs(previous)) * 100;
  return Math.min(Math.max(parseFloat(change.toFixed(3)), -9999.999), 9999.999);
}

// ============================================================================
// FETCH RAW DATA FOR SYMBOL & EXCHANGE
// ============================================================================

async function fetchRawData(symbol, exchange, startTs, endTs) {
  const baseQuery = `
    SELECT ts, symbol, exchange, o, h, l, c, v, oi, pfr, lsr,
           lql, lqs, rsi1, rsi60
    FROM perp_data
    WHERE symbol = $1 AND exchange = $2 AND ts >= $3 AND ts <= $4
    ORDER BY ts ASC
  `;

  const bufferedStart = BigInt(startTs - BUFFER_MS);
  const result = await dbManager.query(baseQuery, [symbol, exchange, bufferedStart, BigInt(endTs)]);
  const isMT = symbol === 'MT';
  return result.rows.map(row => ({
  ts: Number(row.ts),
  symbol: row.symbol,
  exchange: row.exchange,
  o: row.o !== null ? parseFloat(row.o) : null,
  h: row.h !== null ? parseFloat(row.h) : null,
  l: row.l !== null ? parseFloat(row.l) : null,
  c: row.c !== null ? parseFloat(row.c) : null,
  v: row.v !== null ? parseFloat(row.v) : null,
  oi: isMT ? null : (row.oi !== null ? parseFloat(row.oi) : null),
  pfr: isMT ? null : (row.pfr !== null ? parseFloat(row.pfr) : null),
  lsr: isMT ? null : (row.lsr !== null ? parseFloat(row.lsr) : null),
  lql: isMT ? null : (row.lql !== null ? parseFloat(row.lql) : null),
  lqs: isMT ? null : (row.lqs !== null ? parseFloat(row.lqs) : null),
  rsi1: row.rsi1 !== null ? parseFloat(row.rsi1) : null,
  rsi60: row.rsi60 !== null ? parseFloat(row.rsi60) : null,
}));
}

// ============================================================================
// CALCULATE METRICS FOR EXCHANGE DATA
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
      lql: current.lql, lqs: current.lqs,
      // 1m changes (exclude o,h,l)
      c_chg_1m: null, v_chg_1m: null,
      oi_chg_1m: null, pfr_chg_1m: null, lsr_chg_1m: null,
      rsi1_chg_1m: null, rsi60_chg_1m: null,
      lql_chg_1m: null, lqs_chg_1m: null,
      // 5m changes
      c_chg_5m: null, v_chg_5m: null,
      oi_chg_5m: null, pfr_chg_5m: null, lsr_chg_5m: null,
      rsi1_chg_5m: null, rsi60_chg_5m: null,
      lql_chg_5m: null, lqs_chg_5m: null,
      // 10m changes
      c_chg_10m: null, v_chg_10m: null,
      oi_chg_10m: null, pfr_chg_10m: null, lsr_chg_10m: null,
      rsi1_chg_10m: null, rsi60_chg_10m: null,
      lql_chg_10m: null, lqs_chg_10m: null,
    };

    // 1m changes
    const isMT = data[0].symbol === 'MT';
    if (i >= 1) {
      const prev = data[i - 1];
  metricRow.c_chg_1m = calculatePercentChange(current.c, prev.c);
  metricRow.v_chg_1m = calculatePercentChange(current.v, prev.v);
  if (!isMT) {
    metricRow.oi_chg_1m = calculatePercentChange(current.oi, prev.oi);
    metricRow.pfr_chg_1m = calculatePercentChange(current.pfr, prev.pfr);
    metricRow.lsr_chg_1m = calculatePercentChange(current.lsr, prev.lsr);
  }
  metricRow.rsi1_chg_1m = calculatePercentChange(current.rsi1, prev.rsi1);
  metricRow.rsi60_chg_1m = calculatePercentChange(current.rsi60, prev.rsi60);
  if (!isMT) {
    metricRow.lql_chg_1m = calculatePercentChange(current.lql, prev.lql);
    metricRow.lqs_chg_1m = calculatePercentChange(current.lqs, prev.lqs);
  }
}

    // 5m changes
    if (i >= 5) {
      const prev = data[i - 5];
      metricRow.c_chg_1m = calculatePercentChange(current.c, prev.c);
  metricRow.v_chg_1m = calculatePercentChange(current.v, prev.v);
  if (!isMT) {
    metricRow.oi_chg_1m = calculatePercentChange(current.oi, prev.oi);
    metricRow.pfr_chg_1m = calculatePercentChange(current.pfr, prev.pfr);
    metricRow.lsr_chg_1m = calculatePercentChange(current.lsr, prev.lsr);
  }
  metricRow.rsi1_chg_1m = calculatePercentChange(current.rsi1, prev.rsi1);
  metricRow.rsi60_chg_1m = calculatePercentChange(current.rsi60, prev.rsi60);
  if (!isMT) {
    metricRow.lql_chg_1m = calculatePercentChange(current.lql, prev.lql);
    metricRow.lqs_chg_1m = calculatePercentChange(current.lqs, prev.lqs);
  }
}

    // 10m changes
    if (i >= 10) {
      const prev = data[i - 10];
      metricRow.c_chg_1m = calculatePercentChange(current.c, prev.c);
  metricRow.v_chg_1m = calculatePercentChange(current.v, prev.v);
  if (!isMT) {
    metricRow.oi_chg_1m = calculatePercentChange(current.oi, prev.oi);
    metricRow.pfr_chg_1m = calculatePercentChange(current.pfr, prev.pfr);
    metricRow.lsr_chg_1m = calculatePercentChange(current.lsr, prev.lsr);
  }
  metricRow.rsi1_chg_1m = calculatePercentChange(current.rsi1, prev.rsi1);
  metricRow.rsi60_chg_1m = calculatePercentChange(current.rsi60, prev.rsi60);
  if (!isMT) {
    metricRow.lql_chg_1m = calculatePercentChange(current.lql, prev.lql);
    metricRow.lqs_chg_1m = calculatePercentChange(current.lqs, prev.lqs);
  }
}

    metrics.push(metricRow);
  }

  return metrics;
}

// ============================================================================
// INSERT METRICS BATCH - always DO UPDATE with conditional update on detect column
// ============================================================================

async function insertMetricsBatch(metrics) {
  if (metrics.length === 0) return { totalInserted: 0, totalSkipped: 0 };

  let totalInserted = 0;
  let totalSkipped = 0;

  const fields = [
    'ts', 'symbol', 'exchange', 'o', 'h', 'l', 'c', 'v',
    'oi', 'pfr', 'lsr', 'rsi1', 'rsi60', 'lql', 'lqs',
    'c_chg_1m', 'v_chg_1m',
    'oi_chg_1m', 'pfr_chg_1m', 'lsr_chg_1m',
    'rsi1_chg_1m', 'rsi60_chg_1m',
    'lql_chg_1m', 'lqs_chg_1m',
    'c_chg_5m', 'v_chg_5m',
    'oi_chg_5m', 'pfr_chg_5m', 'lsr_chg_5m',
    'rsi1_chg_5m', 'rsi60_chg_5m',
    'lql_chg_5m', 'lqs_chg_5m',
    'c_chg_10m', 'v_chg_10m',
    'oi_chg_10m', 'pfr_chg_10m', 'lsr_chg_10m',
    'rsi1_chg_10m', 'rsi60_chg_10m',
    'lql_chg_10m', 'lqs_chg_10m'
  ];

  for (let i = 0; i < metrics.length; i += INSERT_CHUNK_SIZE) {
    const chunk = metrics.slice(i, i + INSERT_CHUNK_SIZE);

    const values = chunk.map(row => fields.map(f => {
      let val = row[f];
      if (val === undefined) val = null;
      if (typeof val === 'bigint') val = val.toString();
      return val;
    }));

    const updateClause = `
      o = EXCLUDED.o,
      h = EXCLUDED.h,
      l = EXCLUDED.l,
      c = EXCLUDED.c,
      v = EXCLUDED.v,
      oi = EXCLUDED.oi,
      pfr = EXCLUDED.pfr,
      lsr = EXCLUDED.lsr,
      rsi1 = EXCLUDED.rsi1,
      rsi60 = EXCLUDED.rsi60,
      lql = EXCLUDED.lql,
      lqs = EXCLUDED.lqs,
      c_chg_1m = CASE WHEN perp_metrics.${DETECT_COLUMN} IS NULL THEN EXCLUDED.c_chg_1m ELSE perp_metrics.c_chg_1m END,
      v_chg_1m = CASE WHEN perp_metrics.${DETECT_COLUMN} IS NULL THEN EXCLUDED.v_chg_1m ELSE perp_metrics.v_chg_1m END,
      oi_chg_1m = CASE WHEN perp_metrics.${DETECT_COLUMN} IS NULL THEN EXCLUDED.oi_chg_1m ELSE perp_metrics.oi_chg_1m END,
      pfr_chg_1m = CASE WHEN perp_metrics.${DETECT_COLUMN} IS NULL THEN EXCLUDED.pfr_chg_1m ELSE perp_metrics.pfr_chg_1m END,
      lsr_chg_1m = CASE WHEN perp_metrics.${DETECT_COLUMN} IS NULL THEN EXCLUDED.lsr_chg_1m ELSE perp_metrics.lsr_chg_1m END,
      rsi1_chg_1m = CASE WHEN perp_metrics.${DETECT_COLUMN} IS NULL THEN EXCLUDED.rsi1_chg_1m ELSE perp_metrics.rsi1_chg_1m END,
      rsi60_chg_1m = CASE WHEN perp_metrics.${DETECT_COLUMN} IS NULL THEN EXCLUDED.rsi60_chg_1m ELSE perp_metrics.rsi60_chg_1m END,
      lql_chg_1m = CASE WHEN perp_metrics.${DETECT_COLUMN} IS NULL THEN EXCLUDED.lql_chg_1m ELSE perp_metrics.lql_chg_1m END,
      lqs_chg_1m = CASE WHEN perp_metrics.${DETECT_COLUMN} IS NULL THEN EXCLUDED.lqs_chg_1m ELSE perp_metrics.lqs_chg_1m END,
      c_chg_5m = EXCLUDED.c_chg_5m,
      v_chg_5m = EXCLUDED.v_chg_5m,
      oi_chg_5m = EXCLUDED.oi_chg_5m,
      pfr_chg_5m = EXCLUDED.pfr_chg_5m,
      lsr_chg_5m = EXCLUDED.lsr_chg_5m,
      rsi1_chg_5m = EXCLUDED.rsi1_chg_5m,
      rsi60_chg_5m = EXCLUDED.rsi60_chg_5m,
      lql_chg_5m = EXCLUDED.lql_chg_5m,
      lqs_chg_5m = EXCLUDED.lqs_chg_5m,
      c_chg_10m = EXCLUDED.c_chg_10m,
      v_chg_10m = EXCLUDED.v_chg_10m,
      oi_chg_10m = EXCLUDED.oi_chg_10m,
      pfr_chg_10m = EXCLUDED.pfr_chg_10m,
      lsr_chg_10m = EXCLUDED.lsr_chg_10m,
      rsi1_chg_10m = EXCLUDED.rsi1_chg_10m,
      rsi60_chg_10m = EXCLUDED.rsi60_chg_10m,
      lql_chg_10m = EXCLUDED.lql_chg_10m,
      lqs_chg_10m = EXCLUDED.lqs_chg_10m
    `;

    const query = format(
      `INSERT INTO perp_metrics (${fields.join(', ')})
       VALUES %L
       ON CONFLICT (ts, symbol, exchange) DO UPDATE SET
       ${updateClause}`,
      values
    );

    try {
      const res = await dbManager.query(query);
      totalInserted += res.rowCount || chunk.length;
    } catch (err) {
      console.error(`Insert chunk error: ${err.message}`);
    }

    if (INTER_CHUNK_DELAY_MS > 0 && i + INSERT_CHUNK_SIZE < metrics.length) {
      await sleep(INTER_CHUNK_DELAY_MS);
    }
  }

  return { totalInserted, totalSkipped };
}

// ============================================================================
// BACKFILL SINGLE SYMBOL (Parallel per exchange)
// ============================================================================

async function backfillSymbol(symbol, retentionStart, endTs) {
  let totalInserted = 0;
  let totalSkipped = 0;
  for (const exchange of ['bin', 'byb', 'okx']) {
    try {
      const rawData = await fetchRawData(symbol, exchange, retentionStart, endTs);
      if (rawData.length === 0) continue;
      const metrics = calculateMetricsForExchange(rawData);
      if (metrics.length === 0) continue;
      // Sort metrics before insert
      metrics.sort((a, b) => {
        if (a.ts !== b.ts) return Number(a.ts) - Number(b.ts);
        if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
        return a.exchange.localeCompare(b.exchange);
      });
      const { totalInserted: inserted, totalSkipped: skipped } = await insertMetricsBatch(metrics);
      totalInserted += inserted;
      totalSkipped += skipped;
    } catch (err) {
      console.error(`Error backfilling ${symbol} on ${exchange}: ${err.message}`);
    }
  }
  return { totalInserted, totalSkipped };
}

// ============================================================================
// MAIN BACKFILL LOOP
// ============================================================================

async function runBackfill() {
  const startTime = Date.now();
  const now = Date.now();
  const retentionStart = now - DB_RETENTION_DAYS * 24 * 60 * 60 * 1000;
  const endTs = now;

  console.log(`\n${'='.repeat(80)}`);
  console.log(`\uD83D\uDE80 Starting perp_metrics backfill with high concurrency and chunk size`);
  console.log(`   Insert chunk size: ${INSERT_CHUNK_SIZE}`);
  console.log(`   Parallel symbols: ${PARALLEL_SYMBOLS}`);
  console.log(`   Inter-chunk delay: ${INTER_CHUNK_DELAY_MS}ms`);
  console.log(`   Detect column for conditional update: ${DETECT_COLUMN}`);
  console.log(`${'='.repeat(80)}\n`);

  await logStatus('started', `${SCRIPT_NAME} started - backfilling perp_metrics.`);

  let totalInserted = 0;
  let totalSkipped = 0;
  let processedCount = 0;
  let errorCount = 0;
  const totalTasks = perpList.length;

  // Heartbeat logger
  const heartbeat = setInterval(() => {
    logStatus('running', `Processed ${processedCount}/${totalTasks} symbols, inserted ${totalInserted} rows`);
  }, HEARTBEAT_INTERVAL_MS);

  const limit = pLimit(PARALLEL_SYMBOLS);

  try {
    const tasks = symbolsToBackfill.map(symbol => limit(async () => {
      const { totalInserted: inserted } = await backfillSymbol(symbol, retentionStart, endTs);
      processedCount++;
      totalInserted += inserted;
      if (inserted === 0) errorCount++;
    }));

    await Promise.all(tasks);

    clearInterval(heartbeat);

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    const status = errorCount / totalTasks < 0.1 ? 'completed' : 'partial';

    await logStatus(status, `Backfill ${status}: ${totalInserted} rows inserted, ${errorCount} errors in ${duration}s`);
    console.log(`\n${'='.repeat(80)}`);
    console.log(`\u2705 Backfill ${status.toUpperCase()}`);
    console.log(`   Symbols processed: ${processedCount}/${totalTasks}`);
    console.log(`   Rows inserted: ${totalInserted}`);
    console.log(`   Errors: ${errorCount}`);
    console.log(`   Duration: ${duration}s`);
    console.log(`${'='.repeat(80)}\n`);

  } catch (err) {
    clearInterval(heartbeat);
    console.error(`\n\uD83D\uDCA5 Backfill failed: ${err.message}`);
    await logStatus('error', `Backfill failed: ${err.message}`);
  } finally {
    // await dbManager.close();  COMMENT OUT, Dont close pool- calc-metrics needs it.
  }
}

// ============================================================================
// GRACEFUL SHUTDOWN
// ============================================================================

async function gracefulShutdown(signal) {
  console.log(`\n\u26A0\uFE0F Received ${signal}, shutting down gracefully...`);
  await logStatus('stopped', `${SCRIPT_NAME} stopped by ${signal}.`);
  await dbManager.close();
  process.exit(0);
}

if (!process.listenerCount('SIGINT')) {
  process.on('SIGINT', () => gracefulShutdown('SIGINT'));
}
if (!process.listenerCount('SIGTERM')) {
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================

if (require.main === module) {
  runBackfill()
    .then(() => {
      console.log('\u2705 backfill-metrics completed successfully');
      process.exit(0);
    })
    .catch(err => {
      console.error('\uD83D\uDCA5 backfill-metrics failed:', err);
      process.exit(1);
    });
}

module.exports = { runBackfill };
