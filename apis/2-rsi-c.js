/* ==========================================
 * rsi-c.js - 16 Oct 2025 Continuous RSI Polling Script
 * Polls new 1m data from bin-ohlcv, computes RSI11 (rsi1) and RSI11 on rolling 60min agg (rsi60)
 * Inserts into rsi perpspec/source at 1m intervals (ON CONFLICT DO NOTHING)
 * Assumes backfill done via rsi9.js; incremental cache for efficiency
 * Hardcoded PERIOD=11 at top, like -h files
 * Fix: Rolling 60min agg for present-time rsi60 (updates every min from last ~60min 1m data)
 * ========================================== */

const { Pool } = require('pg');
const format = require('pg-format');
require('dotenv').config();
const apiUtils = require('../api-utils'); // For status logging
const dbManager = require('../db/dbsetup'); // For insertData (from dbsetup.js)

const SCRIPT_NAME = 'rsi-c.js';
const STATUS_COLOR = '\x1b[92m'; // White, lighter than \x1b[92m
const RESET = '\x1b[0m';
const PERIOD = 11; // Hardcoded RSI period
const POLL_INTERVAL = 60 * 1000; // 1min polling
const CACHE_SIZE = 1440; // Cache last 1 day 1m bars/symbol for history (rsi1) and rolling 60m (rsi60)
const PERPSPEC_SOURCE = 'rsi';
const DATA_PERPSPEC = 'bin-ohlcv';
const INTERVAL = '1m';
const ROLLING_WINDOW_MIN = 60; // For initial fetch
const INITIAL_WINDOW_MS = 24 * 60 * 60 * 1000; // 1 day for initial cache fill
const ROLLING_HOURS = 24; // Use last 24 hours of 1m for ~24 60m bars in rsi60
let isShuttingDown = false;
let isProcessing = false;

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

// In-memory cache: { symbol: [array of {ts, close}] } - 1m only (agg on-the-fly)
let priceCache = new Map(); // { symbol: array of {ts, close} }

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

// Fetch recent 1m data (initial: last 1 day for full cache; polls: last 2min incremental)
async function fetchRecentRecentData(symbol) {
  // Check if we're shutting down
  if (isShuttingDown) {
    console.log(`Shutdown requested, skipping fetch for ${symbol}`);
    return [];
  }

  isProcessing = true;
  try {
    const timeWindow = priceCache.has(symbol) ? 2 * 60 * 1000 : INITIAL_WINDOW_MS; // 2min poll or 1 day initial
    const query = `
      SELECT ts, c::numeric AS close
      FROM perp_data
      WHERE symbol = $1 AND perpspec = $2 AND interval = $3
      AND ts > (SELECT COALESCE(MAX(ts), 0) FROM perp_data WHERE symbol = $1 AND perpspec = $2 AND interval = $3) - $4
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
  } finally {
    isProcessing = false;
  }
}

// Append new data to cache, keep last CACHE_SIZE
function updateCache(cache, newPrices) {
  cache.push(...newPrices);
  // Sort and trim to last CACHE_SIZE (latest data)
  cache.sort((a, b) => a.ts.getTime() - b.ts.getTime());
  if (cache.length > CACHE_SIZE) cache.splice(0, cache.length - CACHE_SIZE);
  return cache;
}

// Compute RSI11 on 1m cache (rsi1 - present time)
function computeRSI1(cachedPrices, newPrices) {
  const allPrices = updateCache(cachedPrices, newPrices);  // Update in place
  if (allPrices.length < PERIOD + 1) return [];
  return calculateRSI(allPrices, PERIOD);
}

// Rolling 60m agg for rsi60: Resample last ~24hrs 1m to hourly bars (last close per hour, current hour = latest close)
function getRolling60mSeries(cachedPrices) {
  if (cachedPrices.length === 0) return [];
  const sorted = [...cachedPrices].sort((a, b) => a.ts.getTime() - b.ts.getTime());
  const hourlyBars = [];
  let currentHour = null;
  let lastClose = null;

  for (const price of sorted) {
    const hour = new Date(Math.floor(price.ts.getTime() / (60 * 60 * 1000)) * (60 * 60 * 1000));
    if (currentHour === null || hour.getTime() !== currentHour.getTime()) {
      if (currentHour !== null) {
        hourlyBars.push({ ts: currentHour, close: lastClose });
      }
      currentHour = hour;
    }
    lastClose = price.close;
  }

  // Add the last hour
  if (currentHour !== null) {
    hourlyBars.push({ ts: currentHour, close: lastClose });
  }

  // Trim to last 60 hours
  const sixtyHoursAgo = new Date(Date.now() - ROLLING_WINDOW_MIN * 60 * 60 * 1000);
  return hourlyBars.filter(bar => bar.ts >= sixtyHoursAgo);
}

// Calculate RSI over period (on array of {ts, close})
function calculateRSI(prices, period) {
  const rsiValues = [];
  if (prices.length < period + 1) return rsiValues;

  let gains = 0, losses = 0;
  for (let i = 1; i <= period; i++) {
    const change = prices[i].close - prices[i - 1].close;
    if (change > 0) gains += change;
    else losses -= change; // Absolute
  }
  let avgGain = gains / period;
  let avgLoss = losses / period || 1e-10;
  let rs = avgGain / avgLoss;
  let rsi = 100 - (100 / (1 + rs));
  rsiValues.push({
    ts: prices[period].ts,
    rsi: Math.round(rsi * 100) / 100
  });

  // Smoothed for subsequent periods
  for (let i = period + 1; i < prices.length; i++) {
    const change = prices[i].close - prices[i - 1].close;
    const gain = change > 0 ? change : 0;
    const loss = change < 0 ? Math.abs(change) : 0;
    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period || 1e-10;
    rs = avgGain / avgLoss;
    rsi = 100 - (100 / (1 + rs));
    rsiValues.push({
      ts: prices[i].ts,
      rsi: Math.round(rsi * 100) / 100
    });
  }
  return rsiValues;
}

// Poll one symbol: Fetch recent, update cache, compute rsi1/rsi60, insert at present ts
async function pollSymbol(symbol) {
  try {
    const newPrices = await fetchRecentRecentData(symbol);
    if (newPrices.length === 0) return; // No new data

    // Initialize/update cache
    if (!priceCache.has(symbol)) priceCache.set(symbol, []);
    const cachedPrices = priceCache.get(symbol);

    // Compute rsi1 (RSI11 on full 1m cache)
    const rsi1Values = computeRSI1(cachedPrices, newPrices);
    if (rsi1Values.length === 0) return;

    // Compute rsi60 (RSI11 on rolling hourly 60m series from updated cache)
    const rolling60m = getRolling60mSeries(cachedPrices);
    const rsi60Values = calculateRSI(rolling60m, PERIOD);

    // Get latest (present) values: Last rsi1 and last rsi60 (or null if insufficient)
    const latestRsi1 = rsi1Values[rsi1Values.length - 1];
    const latestRsi60 = rsi60Values.length > 0 ? rsi60Values[rsi60Values.length - 1] : { rsi: null };

    // Insert at present ts (latest 1m ts, with both RSIs)
    const presentTs = latestRsi1.ts.getTime();
    const insertData = [{
      ts: BigInt(presentTs),
      symbol: symbol,
      source: PERPSPEC_SOURCE,
      perpspec: PERPSPEC_SOURCE,
      interval: INTERVAL,
      rsi1: latestRsi1.rsi,
      rsi60: latestRsi60.rsi  // Present rsi60 from rolling hourly agg
    }];

    // Bulk insert (single row per poll, ON CONFLICT DO NOTHING)
    await dbManager.insertData(PERPSPEC_SOURCE, insertData);

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
  console.log(`${STATUS_COLOR}🚦 Starting ${SCRIPT_NAME} - Continuous RSI polling${RESET}`);
  // Status: started
  const startMsg = `${SCRIPT_NAME} connected`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', startMsg);
  console.log(`${STATUS_COLOR}🚦 ${startMsg}${RESET}`);

  // Initial poll (loads 1 day for full cache)
  await pollAllSymbols();

  // Status: running after initial
  const initialMsg = `${SCRIPT_NAME} initial Calc complete`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', initialMsg);
  console.log(`${STATUS_COLOR}${initialMsg}${RESET}`);

  // Polling loop
  const pollIntervalId = setInterval(async () => {
    try {
      await pollAllSymbols();
      // Status: running after each cycle
      const cycleMsg = `${SCRIPT_NAME}🚥 1m rsi Calc done`;
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', cycleMsg);
      console.log(`${STATUS_COLOR}${cycleMsg}${RESET}`);
    } catch (error) {
      console.error('Error in polling cycle:', error.message);
      await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'POLL_CYCLE_ERROR', error.message);
    }
  }, POLL_INTERVAL);

  // Graceful shutdown
  process.on('SIGINT', async () => {
    clearInterval(pollIntervalId);
    console.log(`${STATUS_COLOR}\n${SCRIPT_NAME} received SIGINT, stopping...${RESET}`);
    isShuttingDown = true;

    // Wait for the current operation to complete
    while (isProcessing) {
      console.log('Waiting for current operation to complete before shutdown...');
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    const stopMsg = `🚥 ${SCRIPT_NAME} stopped smoothly`;
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'stopped', stopMsg);
    console.log(`${STATUS_COLOR}${stopMsg}${RESET}`);
    await pool.end();
    process.exit(0);
  });
}

// Run if direct
if (require.main === module) {
  execute()
    .then(() => console.log(`${STATUS_COLOR}🚦 RSI continuous polling started${RESET}`))
    .catch(err => {
      console.error('💥 RSI continuous polling failed:', err);
      process.exit(1);
    });
}

module.exports = { execute };