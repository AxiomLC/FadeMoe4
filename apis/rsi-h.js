// SCRIPT: rsi-h.js Backfill 7 OCt 2025
// RSI calculation script for all individual symbols using bin-ohlcv data (no MT)
// RSI11 for 1m (rsi1) and 60m aggregated (rsi60, forward-filled to 1m ts—60x same per hour)
// Aggregation uses LAST CLOSE per 60m bucket; ts as BigInt Unix ms
// Under perpspec/source 'rsi' in timescale Postgres 10-day rolling 1m-based DB
// Status: 3-tier logging (started, running heartbeat, completed) via apiUtils to DB/console
// Parallel batching; ON CONFLICT DO NOTHING for inserts (no overwrite; fills gaps/new ts only); streamlined errors
// Verification: rsi1 100% full; rsi60 allow <=15% null (covers early ~11hrs + gaps); warn if >15%

const { Pool } = require('pg');
const format = require('pg-format');
require('dotenv').config();
const apiUtils = require('../api-utils'); // Adjust path; for status/error logging
const dbManager = require('../db/dbsetup'); // Assume this provides dbManager; fallback to pool if needed

const SCRIPT_NAME = 'rsi9.js';
const PERIOD = 11; // RSI period - change this to adjust for all calculations
const INTERVAL = '1m'; // Fixed 1m interval for all insertions (DB is 1m-based)
const AGGREGATE_MINUTES = 60; // Aggregation interval for RSI60 in minutes (60m = 1 hour) - can be changed for testing
const PERPSPEC_SOURCE = 'rsi';
const DATA_PERPSPEC = 'bin-ohlcv';
const BUCKET_SIZE_MS = AGGREGATE_MINUTES * 60 * 1000; // Milliseconds in aggregation interval
const HEARTBEAT_INTERVAL = 10000; // 10s heartbeat
const CHUNK_SIZE = 5; // Parallel chunks to avoid DB overload
const MAX_NULL_PCT = 15; // Allow <=15% null for rsi60 (covers early ~11hrs + gaps)

// Database configuration (fallback if dbManager not used)
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

// Use dbManager or pool for apiUtils
const statusDb = dbManager || pool;

async function logScriptStatus(status, message) {
  console.log(`[${status.toUpperCase()}] ${message}`);
  await apiUtils.logScriptStatus(statusDb, SCRIPT_NAME, status, message);
}

async function logScriptError(type, code, message) {
  console.error(`[${type.toUpperCase()}] ${code}: ${message}`);
  await apiUtils.logScriptError(statusDb, SCRIPT_NAME, type, code, message);
}

async function getSymbols() {
  try {
    const query = `SELECT DISTINCT symbol FROM perp_data WHERE perpspec = $1`;
    const result = await pool.query(query, [DATA_PERPSPEC]);
    return result.rows.map(row => row.symbol);
  } catch (error) {
    await logScriptError('DATABASE', 'SYMBOL_QUERY_ERROR', `Error getting symbols: ${error.message}`);
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
    const query = `SELECT ts, c::numeric AS close FROM perp_data WHERE symbol = $1 AND perpspec = $2 AND interval = $3 ORDER BY ts ASC`;
    const result = await pool.query(query, [symbol, DATA_PERPSPEC, INTERVAL]);
    if (result.rows.length === 0) {
      await logScriptError('DATABASE', 'OHLCV_EXTRACT_ERROR', `Cannot extract OHLCV for ${symbol}`);
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
      await logScriptError('CALCULATION', 'INSUFFICIENT_DATA_ERROR', `Cannot complete calc for ${symbol} (insufficient data)`);
      return false;
    }

    const rsi1Values = calculateRSI(prices, PERIOD);
    const aggregated60m = simpleAggregateToHigherTimeframe(prices);
    if (aggregated60m.length < PERIOD + 1) {
      await logScriptError('CALCULATION', '60M_CALC_ERROR', `Cannot complete 60m calc for ${symbol}`);
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
      await logScriptError('INSERT', 'NO_INSERT_DATA_ERROR', `Cannot insert for ${symbol} (no data)`);
      return false;
    }

    const values = insertionData.map(data => [Math.round(data.rsi1), Math.round(data.rsi60), data.tsMs, symbol, PERPSPEC_SOURCE, PERPSPEC_SOURCE, INTERVAL]); // Explicit round for no decimals
    const upsertQuery = format(`INSERT INTO perp_data (rsi1, rsi60, ts, symbol, source, perpspec, interval) VALUES %L ON CONFLICT (ts, symbol, source) DO NOTHING`, values);
    const resultUpsert = await pool.query(upsertQuery);
    return true;
  } catch (error) {
    if (error.message.includes('OHLCV') || error.code === 'ECONNREFUSED') await logScriptError('DATABASE', 'OHLCV_EXTRACT_ERROR', `Cannot extract OHLCV for ${symbol}: ${error.message}`);
    else if (error.message.includes('60m')) await logScriptError('CALCULATION', '60M_CALC_ERROR', `Cannot complete 60m calc for ${symbol}: ${error.message}`);
    else if (error.message.includes('insert') || error.code === '23505') await logScriptError('INSERT', 'DB_INSERT_ERROR', `Cannot insert for ${symbol}: ${error.message}`);
    else await logScriptError('INTERNAL', 'SYMBOL_ERROR', `Error for ${symbol}: ${error.message}`);
    return false;
  }
}

async function verifyAllRSIComplete(symbols) {
  try {
    const query = `
      SELECT COUNT(*) as total_rows,
             COUNT(rsi1) FILTER (WHERE rsi1 IS NOT NULL) as rsi1_count,
             COUNT(rsi60) FILTER (WHERE rsi60 IS NOT NULL) as rsi60_count
      FROM perp_data
      WHERE perpspec = $1 AND source = $2 AND interval = $3 AND symbol = ANY($4)
    `;
    const params = [PERPSPEC_SOURCE, PERPSPEC_SOURCE, INTERVAL, symbols];
    const result = await pool.query(query, params);
    const row = result.rows[0];
    const total = parseInt(row.total_rows);
    const rsi1Full = parseInt(row.rsi1_count) === total;
    const rsi60Count = parseInt(row.rsi60_count);
    const nullRsi60Pct = total > 0 ? ((total - rsi60Count) / total * 100) : 0;
    if (nullRsi60Pct > MAX_NULL_PCT) {
      await logScriptError('VERIFICATION', 'RSI60_NULL_WARNING', `> ${MAX_NULL_PCT}% rsi60 null (${nullRsi60Pct.toFixed(1)}%) - check early data`);
      return false;
    }
    return total > 0 && rsi1Full && (nullRsi60Pct <= MAX_NULL_PCT);
  } catch (error) {
    await logScriptError('DATABASE', 'VERIFY_ERROR', `Verification failed: ${error.message}`);
    return false;
  }
}

async function calculateRSIForAllSymbols() {
  const startTime = Date.now();
  console.log(`\n🚀 Starting ${SCRIPT_NAME}...`);
  let heartbeatInterval;

  try {
    // Tier 1: Started status
    await logScriptStatus('started', `${SCRIPT_NAME} initiated, rsi1, rsi60 calculations.`);

    const symbols = await getSymbols();
    if (symbols.length === 0) {
      await logScriptStatus('running', `rsi1, rsi60 calculation for 0 symbols.`);
      return;
    }

    // Start 10s heartbeat (running status for UI)
    let symbolsProcessed = 0;
    heartbeatInterval = setInterval(async () => {
      if (symbolsProcessed < symbols.length) {
        await logScriptStatus('running', `rsi1, rsi60 calculation for ${symbols.length} symbols.`);
      }
    }, HEARTBEAT_INTERVAL);

    // Parallel batching for symbols
    for (let i = 0; i < symbols.length; i += CHUNK_SIZE) {
      const chunk = symbols.slice(i, i + CHUNK_SIZE);
      const promises = chunk.map(symbol => calculateRSIForSymbol(symbol).then(success => { if (success) symbolsProcessed++; return success; }));
      await Promise.all(promises);
    }

    clearInterval(heartbeatInterval);

    // Tier 3: Verify and completed status
    const allComplete = await verifyAllRSIComplete(symbols);
    if (allComplete) {
      await logScriptStatus('completed', 'rsi backfills complete');
      console.log(`\n🎉 RSI backfills complete in ${((Date.now() - startTime) / 1000).toFixed(2)}s!`);
    } else {
      await logScriptError('VERIFICATION', 'INCOMPLETE_BACKFILL', 'Not all rsi1/rsi60 full (check null %)');
      process.exit(1);
    }
  } catch (error) {
    clearInterval(heartbeatInterval);
    await logScriptError('INTERNAL', 'SCRIPT_ERROR', `Script failed: ${error.message}`);
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
