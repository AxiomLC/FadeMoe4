/* ==========================================
 * rsi-c.js - Continuous RSI Polling Script
 * Polls new 1m data from bin-ohlcv, computes RSI11 (rsi1) and RSI11 on 60m agg (rsi60)
 * Inserts into rsi perpspec/source at 1m intervals (ON CONFLICT DO NOTHING)
 * Assumes backfill done via rsi9.js; incremental cache for efficiency
 * Hardcoded PERIOD=11 at top, like -h files
 * Fix: Load last 20min data on startup for immediate RSI (no 11-min lag)
 * ========================================== */

const { Pool } = require('pg');
const format = require('pg-format');
require('dotenv').config();
const apiUtils = require('../api-utils'); // For status logging
const dbManager = require('../db/dbsetup'); // For insertData (from dbsetup.js)

const SCRIPT_NAME = 'rsi-c2.js';
const PERIOD = 11; // Hardcoded RSI period
const POLL_INTERVAL = 60 * 1000; // 1min polling
const CACHE_SIZE = 100; // Cache last N bars/symbol for RSI (efficient for live)
const PERPSPEC_SOURCE = 'rsi';
const DATA_PERPSPEC = 'bin-ohlcv';
const INTERVAL = '1m';
const AGGREGATE_MINUTES = 60; // For rsi60 aggregation
const BUCKET_SIZE_MS = AGGREGATE_MINUTES * 60 * 1000;

// Database pool
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

// In-memory cache: { symbol: { '1m': [prices], '60m': [prices] } }
let priceCache = new Map(); // { symbol: { '1m': array of {ts, close}, '60m': array } }

/* ==========================================
 * UTILITY FUNCTIONS
 * ========================================== */

// Get all symbols from bin-ohlcv
async function getSymbols() {
  try {
    const query = `SELECT DISTINCT symbol FROM perp_data WHERE perpspec = $1`;
    const result = await pool.query(query, [DATA_PERPSPEC]);
    return result.rows.map(row => row.symbol);
  } catch (error) {
    console.error('Error getting symbols:', error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'DATABASE', 'SYMBOL_ERROR', error.message);
    return [];
  }
}

// Fetch recent 1m data (last 20min on startup for immediate RSI, then 2min polls)
async function fetchRecentData(symbol) {
  try {
    // Initial load: Last 20min for immediate RSI (enough for PERIOD=11)
    // Subsequent polls: Last 2min (incremental)
    const timeWindow = priceCache.has(symbol) ? 120000 : 1200000; // 2min or 20min
    const query = `
      SELECT ts, c::numeric AS close
      FROM perp_data
      WHERE symbol = $1 AND perpspec = $2 AND interval = $3
      AND ts > (SELECT MAX(ts) FROM perp_data WHERE symbol = $1 AND perpspec = $2 AND interval = $3) - $4  -- Dynamic window
      ORDER BY ts ASC
    `;
    const result = await pool.query(query, [symbol, DATA_PERPSPEC, INTERVAL, timeWindow]);
    return result.rows.map(row => {
      let ts; const tsStr = String(row.ts).trim();
      if (/^\d+$/.test(tsStr) && tsStr.length >= 10 && tsStr.length <= 13) ts = new Date(Number(tsStr));
      else ts = new Date(tsStr);
      const close = parseFloat(row.close);
      return { ts, close };
    }).filter(p => !isNaN(p.ts.getTime()) && !isNaN(p.close) && isFinite(p.close));
  } catch (error) {
    console.error(`Error fetching recent data for ${symbol}:`, error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'DATABASE', 'FETCH_ERROR', error.message, { symbol });
    return [];
  }
}

// Append new data to cache, compute RSI1 (on 1m cache)
function computeRSI1(cachedPrices, newPrices) {
  const allPrices = [...cachedPrices, ...newPrices].sort((a, b) => a.ts.getTime() - b.ts.getTime());
  // Keep only last CACHE_SIZE
  if (allPrices.length > CACHE_SIZE) allPrices.splice(0, allPrices.length - CACHE_SIZE);
  return calculateRSI(allPrices, PERIOD);
}

// Aggregate cached 1m to 60m (last close per bucket)
function aggregateTo60m(cachedPrices) {
  if (cachedPrices.length === 0) return [];
  let agg = [], bucketStart = null, lastClose = null, lastTs = null;
  for (const price of cachedPrices) {
    const tsMs = price.ts.getTime(); if (isNaN(tsMs)) continue;
    const bucket = Math.floor(tsMs / BUCKET_SIZE_MS) * BUCKET_SIZE_MS;
    if (bucket !== bucketStart) {
      if (bucketStart !== null && lastTs !== null) {
        const endTs = new Date(bucketStart + BUCKET_SIZE_MS - 1);
        if (!isNaN(endTs.getTime())) agg.push({ ts: endTs, close: lastClose });
      }
      bucketStart = bucket; lastTs = tsMs; lastClose = price.close;
    } else { lastTs = tsMs; lastClose = price.close; }
  }
  if (bucketStart !== null && lastTs !== null) {
    const endTs = new Date(bucketStart + BUCKET_SIZE_MS - 1);
    if (!isNaN(endTs.getTime())) agg.push({ ts: endTs, close: lastClose });
  }
  return agg;
}

// Compute RSI on prices (from test-rsi9.js)
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
      rsiValues.push({ ts: prices[i].ts, rsi: Math.round(rsi) });
      const prevChange = prices[i - period + 1].close - prices[i - period].close;
      if (prevChange > 0) gains -= prevChange; else losses -= Math.abs(prevChange);
    }
  }
  return rsiValues;
}

// Poll one symbol: Fetch recent, append to cache, compute rsi1/rsi60, insert new
async function pollSymbol(symbol) {
  try {
    const newPrices = await fetchRecentData(symbol);
    if (newPrices.length === 0) return; // No new data

    // Append to cache
    if (!priceCache.has(symbol)) priceCache.set(symbol, { '1m': [], '60m': [] });
    const cache = priceCache.get(symbol);
    cache['1m'].push(...newPrices);
    // Keep only last CACHE_SIZE for efficiency
    if (cache['1m'].length > CACHE_SIZE) cache['1m'].splice(0, cache['1m'].length - CACHE_SIZE);

    // Compute rsi1 (RSI11 on 1m cache)
    const rsi1Values = computeRSI1(cache['1m'], newPrices);
    if (rsi1Values.length === 0) return;

    // Compute rsi60 (aggregate cache to 60m, RSI11 on that)
    const agg60m = aggregateTo60m(cache['1m']);
    const rsi60Values = calculateRSI(agg60m, PERIOD);

    // Prepare insert data (forward-fill rsi60 to 1m ts from rsi1)
    rsi60Values.sort((a, b) => a.ts.getTime() - b.ts.getTime());
    const insertionData = []; let rsi60Pointer = 0, currentRsi60 = null;
    for (const rsi1Entry of rsi1Values) {
      const currentTsMs = rsi1Entry.ts.getTime();
      while (rsi60Pointer < rsi60Values.length && rsi60Values[rsi60Pointer].ts.getTime() <= currentTsMs) {
        currentRsi60 = rsi60Values[rsi60Pointer].rsi;
        rsi60Pointer++;
      }
      insertionData.push({
        rsi1: rsi1Entry.rsi,
        rsi60: currentRsi60,
        tsMs: currentTsMs
      });
    }

    if (insertionData.length === 0) return;

    const values = insertionData.map(data => [data.rsi1, data.rsi60, data.tsMs, symbol, PERPSPEC_SOURCE, PERPSPEC_SOURCE, INTERVAL]);
    const upsertQuery = format(`INSERT INTO perp_data (rsi1, rsi60, ts, symbol, source, perpspec, interval) VALUES %L ON CONFLICT (ts, symbol, source) DO NOTHING`, values);
    await pool.query(upsertQuery);
  } catch (error) {
    console.error(`Error polling ${symbol}:`, error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'POLL', 'SYMBOL_ERROR', error.message, { symbol });
  }
}

// Poll all symbols in parallel
async function pollAllSymbols() {
  const symbols = await getSymbols();
  if (symbols.length === 0) return;

  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(10); // Parallel for symbols (DB safe)
  const promises = symbols.map(symbol => limit(() => pollSymbol(symbol)));
  await Promise.all(promises);
}

// Main execution (like all-pfr-c.js)
async function execute() {
  console.log(`🚀 Starting ${SCRIPT_NAME} - Continuous RSI polling`);
  // Status: started
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} connected`);

  // Initial poll
  await pollAllSymbols();

  // Status: running after initial
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `${SCRIPT_NAME} initial poll complete`);
  console.log(`${SCRIPT_NAME} initial poll complete`);

  // Polling loop
  const pollIntervalId = setInterval(async () => {
    try {
      await pollAllSymbols();
      // Status: running after each cycle
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `${SCRIPT_NAME} 1min rsi pull complete`);
    } catch (error) {
      console.error('Error in polling cycle:', error.message);
      await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'POLL_CYCLE_ERROR', error.message);
    }
  }, POLL_INTERVAL);

  // Graceful shutdown
  process.on('SIGINT', async () => {
    clearInterval(pollIntervalId);
    console.log(`\n${SCRIPT_NAME} received SIGINT, stopping...`);
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'stopped', `${SCRIPT_NAME} stopped smoothly`);
    await pool.end();
    process.exit(0);
  });
}

// Run if direct
if (require.main === module) {
  execute()
    .then(() => console.log('✅ RSI continuous polling started'))
    .catch(err => {
      console.error('💥 RSI continuous polling failed:', err);
      process.exit(1);
    });
}

module.exports = { execute };
