// SCRIPT: 1z-lq-h.js // Created: [Current Date] - Unified Liquidations Backfill for Coinalyze API
// - Fetches 1min liquidation data from Coinalyze for Binance, Bybit, and OKX symbols
// - Maps base symbols (from perp-list.js) to exchange-specific formats (e.g., ETH -> ETHUSDT_PERP.A for Binance)
// - Inserts to perp_data: ts (millis), symbol (base e.g., 'ETH'), exchange ('bin', 'byb', 'okx'), perpspec ('bin-lq' etc.), lql (long_usd), lqs (short_usd)
// - No expansion needed (already 1min granularity); nulls for empty ts handled in queries (COALESCE in DB)
// - Unified insertBackfillData per symbol across exchanges (ts, symbol, exchange, perpspec, lql, lqs)
// - Status logging with connection verification and heartbeat (references: 1-all-lsr-h.js, 1z-all-oi-h.js)

const axios = require('axios');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

require('dotenv').config();  // Load .env for COINALYZE_KEY

const SCRIPT_NAME = '1z-lq-h.js';
const DAYS = 10;  // Default days back (user control at top)
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const NOW = Date.now();
const START = NOW - DAYS * MS_PER_DAY;
const HEARTBEAT_INTERVAL = 20 * 1000;  // 20 seconds

const COINALYZE_KEY = process.env.COINALYZE_KEY;
if (!COINALYZE_KEY) {
  console.error('❌ COINALYZE_KEY not found in .env');
  process.exit(1);
}

const STATUS_COLOR = '\x1b[36m'; // Cyan/light blue
const RESET = '\x1b[0m';
const YELLOW = '\x1b[33m'; // Yellow for warnings

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ============================================================================
// EXCHANGE CONFIGURATIONS (for symbol mapping; single API but per-exchange symbols)
// ============================================================================
const EXCHANGES = {
  BINANCE: {
    perpspec: 'bin-lq',
    exchange: 'bin',
    mapSymbol: sym => `${sym}USDT_PERP.A`  // e.g., ETH -> ETHUSDT_PERP.A
  },
  BYBIT: {
    perpspec: 'byb-lq',
    exchange: 'byb',
    mapSymbol: sym => `${sym}USDT.6`  // e.g., ETH -> ETHUSDT.6
  },
  OKX: {
    perpspec: 'okx-lq',
    exchange: 'okx',
    mapSymbol: sym => `${sym}USDT_PERP.3`  // e.g., ETH -> ETHUSDT_PERP.3
  }
};

const totalSymbols = perpList.length;
const PERPSPECS = Object.values(EXCHANGES).map(c => c.perpspec).join(', ');
const API_URL = 'https://api.coinalyze.net/v1/liquidation-history';
const RATE_DELAY = 200;  // ms delay between requests to avoid rate limits
const CONCURRENCY = 5;  // Parallel fetches per symbol (across exchanges)

// ============================================================================
// FETCH FUNCTION (Unified for Coinalyze API)
// ============================================================================
async function fetchCoinalyzeLQ(mappedSymbol, startTs, endTs) {
  const params = {
    symbols: mappedSymbol,
    interval: '1min',
    from: Math.floor(startTs / 1000),  // Unix seconds
    to: Math.floor(endTs / 1000),  // Unix seconds
    convert_to_usd: true
  };

  try {
    const response = await axios.get(API_URL, {
      params,
      headers: { 'api_key': COINALYZE_KEY },
      timeout: 10000  // 10s timeout
    });

    const json = response.data;
    if (!json || !json[0] || !json[0].history || json[0].history.length === 0) {
      return [];  // No data
    }

    return json[0].history;  // Array of {t: unix seconds, l: long, s: short}
  } catch (error) {
    console.error(`❌ Coinalyze API error for ${mappedSymbol}: ${error.message}`);
    throw error;
  }
}

// ============================================================================
// DATA PROCESSING FUNCTION (Unified for all exchanges)
// ============================================================================
function processLQData(rawData, baseSymbol, exchange, perpspec) {
  const processed = [];
  for (const point of rawData) {
    try {
      const tsSeconds = point.t;
      const tsMillis = apiUtils.toMillis(BigInt(tsSeconds * 1000));  // Convert to millis
      const lql = parseFloat(point.l) || null;  // Null if invalid (DB handles)
      const lqs = parseFloat(point.s) || null;  // Null if invalid (DB handles)

      // Safety: Skip if baseSymbol is invalid
      if (typeof baseSymbol !== 'string' || !baseSymbol) {
        console.warn(`⚠️ Skipping invalid symbol for ${perpspec}: ${baseSymbol}`);
        continue;
      }

      processed.push({
        ts: tsMillis,
        symbol: baseSymbol,  // Base symbol, e.g., 'ETH'
        exchange,  // e.g., 'bin'
        perpspec,  // e.g., 'bin-lq' (added to match DB's getExchangeFromPerpspec)
        lql,
        lqs
      });
    } catch (err) {
      console.error(`❌ Process error for ${baseSymbol} (${perpspec}): ${err.message}`);
    }
  }
  return processed;
}

// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================
async function backfill() {
  const startTime = Date.now();
  const totalSymbols = perpList.length;
  const perpspecs = Object.values(EXCHANGES).map(ex => ex.perpspec);
  const perpspecsStr = perpspecs.join(', ');

  // #1 STATUS: started
  const message1 = `*LQ Starting ${SCRIPT_NAME} backfill for Liquidations; ${totalSymbols} symbols across ${perpspecsStr}.`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', message1);
  console.log(`${STATUS_COLOR}${message1}${RESET}`);

  // Track connection and completion
  const connectedPerpspecs = new Set();
  const completedSymbolsPerPerpspec = {};
  const completedPerpspecs = new Set();
  for (const p of perpspecs) {
    completedSymbolsPerPerpspec[p] = new Set();
  }

  // #2 STATUS: connected when all perpspecs connected (simplified since single API)
  let connectedLogged = false;

  // #3 STATUS: heartbeat running logs per perpspec if not completed
  let stopHeartbeat = false;
  const heartbeatInterval = setInterval(async () => {
    if (stopHeartbeat) return;
    for (const p of perpspecs) {
      if (!completedPerpspecs.has(p)) {
        const msg = `${p} backfilling db.`;
        try {
          await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', msg);
        } catch (err) {
          console.error(`[heartbeat] DB log failed for ${p}:`, err.message);
        }
        console.log(`${STATUS_COLOR}${msg}${RESET}`);
      }
    }
  }, HEARTBEAT_INTERVAL);

  // =============== PROCESSING ====================================
  // Parallel processing per symbol (fetch all exchanges for each symbol)
  const limiter = pLimit(CONCURRENCY);
  const promises = perpList.map(baseSym => limiter(async () => {
    if (typeof baseSym !== 'string' || !baseSym) {
      console.warn(`⚠️ Skipping invalid baseSym: ${baseSym}`);
      return;  // Skip invalid symbols
    }

    const allData = [];
    let symbolSuccess = true;

    for (const exKey of Object.keys(EXCHANGES)) {
      const config = EXCHANGES[exKey];
      const mappedSymbol = config.mapSymbol(baseSym);
      let exchangeData = [];

      try {
        // Fetch from Coinalyze
        const rawData = await fetchCoinalyzeLQ(mappedSymbol, START, NOW);
        if (rawData.length > 0) {
          exchangeData = processLQData(rawData, baseSym, config.exchange, config.perpspec);  // Pass perpspec here
          allData.push(...exchangeData);
        } else {
          console.warn(`⚠️ No raw data for ${baseSym} on ${config.exchange} (${mappedSymbol})`);
        }

        // Track connection (simplified for single API)
        if (!connectedPerpspecs.has(config.perpspec)) {
          connectedPerpspecs.add(config.perpspec);
          if (connectedPerpspecs.size === perpspecs.length && !connectedLogged) {
            const connectedMsg = `${perpspecsStr} connected, starting fetch.`;
            await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', connectedMsg);
            console.log(`${STATUS_COLOR}*LQ ${connectedMsg}${RESET}`);
            connectedLogged = true;
          }
        }

        completedSymbolsPerPerpspec[config.perpspec].add(baseSym);

        const expectedCount = perpList.length;
        if (completedSymbolsPerPerpspec[config.perpspec].size === expectedCount && !completedPerpspecs.has(config.perpspec)) {
          const completeMsg = `${config.perpspec} backfill complete.`;
          await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', completeMsg);
          // console.log(`${STATUS_COLOR}*LQ ${completeMsg}${RESET}`);
          completedPerpspecs.add(config.perpspec);
        }

        await sleep(RATE_DELAY);  // Rate limit
      } catch (err) {
        console.error(`❌ [${config.perpspec}] ${baseSym}: ${err.message}`);
        const errorCode = err.response?.status === 429 ? 'RATE_LIMIT' :
          err.message.includes('timeout') ? 'TIMEOUT' :
          err.message.includes('404') ? 'NOT_FOUND' : 'FETCH_ERROR';
        await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', errorCode, `${config.perpspec} error for ${baseSym}: ${err.message}`, { perpspec: config.perpspec, symbol: baseSym });
        symbolSuccess = false;
      }
    }

    // Insert unified data for this symbol (all exchanges) only if allData has entries
    if (allData.length > 0 && symbolSuccess) {
      try {
        await dbManager.insertBackfillData(allData);
        
      } catch (err) {
        console.error(`❌ Insert failed for ${baseSym}: ${err.message}`);
        await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'INTERNAL', 'INSERT_FAILED', `Insert failed for ${baseSym}: ${err.message}`, { symbol: baseSym });
        symbolSuccess = false;
      }
    } else {
      console.warn(`⚠️ No LQ data for ${baseSym} across exchanges (skipping insert).`);
    }
  }));

  await Promise.all(promises);

  clearInterval(heartbeatInterval);
  stopHeartbeat = true;

  // Single completion log
  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  const finalMsg = `⏱️ *LQ ${SCRIPT_NAME} backfill completed in ${duration}s!`;
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', finalMsg);
  console.log(`${finalMsg}`);
}

// Run if main module
if (require.main === module) {
  backfill()
    .then(() => process.exit(0))
    .catch(err => {
      console.error('💥 LQ backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { backfill };