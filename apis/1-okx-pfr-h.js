// SCRIPT: 1-okx-pfr-h.js (OKX Premium Funding Rate Backfill)
// Updated: 21 Oct 2025 - Unified perp_data schema
// Features: Dual IP (direct + proxy), chunked inserts, unified table structure
//** insertBackfillData

const axios = require('axios');
const { HttpsProxyAgent } = require('https-proxy-agent');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = '1-okx-pfr-h.js';
const STATUS_COLOR = '\x1b[36m'; // Light blue for all console logs
const RESET = '\x1b[0m';

// ============================================================================
// USER SPEED SETTINGS
// ============================================================================
const DAYS = 10;

// DIRECT CONNECTION SETTINGS
const DIRECT_CONCURRENCY = 8;
const DIRECT_PAGE_DELAY_MS = 80;
const DIRECT_TIMEOUT_MS = 7000;

// PROXY CONNECTION SETTINGS
const PROXY_CONCURRENCY = 12;
const PROXY_PAGE_DELAY_MS = 100;
const PROXY_TIMEOUT_MS = 9000;

// SHARED SETTINGS
const RETRY_429_MAX = 3;
const RETRY_429_BASE_MS = 700;
const HEARTBEAT_STATUS_INTERVAL = 35000;
const HEARTBEAT_429_INTERVAL = 30000;
const DIRECT_PROXY_SPLIT = 100;
const DB_INSERT_MILESTONE = 50000;
const CHUNK_SIZE = 10000; // Insert in 10k chunks

// ============================================================================
// PROXY CONFIGURATION
// ============================================================================
const PROXY_CONFIG = {
  username: '14a233d28dd8f',
  password: 'bf64d81ae2',
  host: '206.53.49.228',
  port: 12323
};

const proxyUrl = `http://${PROXY_CONFIG.username}:${PROXY_CONFIG.password}@${PROXY_CONFIG.host}:${PROXY_CONFIG.port}`;
const proxyAgent = new HttpsProxyAgent(proxyUrl);

// console.log(`${STATUS_COLOR}okx-pfr Proxy configured: ${PROXY_CONFIG.host}:${PROXY_CONFIG.port}${RESET}`);

// ============================================================================
// DERIVED SETTINGS
// ============================================================================
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const NOW = Date.now();
const START = NOW - DAYS * MS_PER_DAY;

const splitIndex = Math.ceil(perpList.length * (DIRECT_PROXY_SPLIT / 100));
const directSymbols = perpList.slice(0, splitIndex);
const proxySymbols = perpList.slice(splitIndex);

console.log(`${STATUS_COLOR}*okxPFR Symbol split: ${directSymbols.length} direct, ${proxySymbols.length} proxy${RESET}`);

// ============================================================================
// EXCHANGE CONFIGURATION
// ============================================================================
const OKX_CONFIG = {
  perpspec: 'okx-pfr',
  exchange: 'okx',
  url: 'https://www.okx.com/api/v5/public/premium-history',
  limit: 100,
  mapSymbol: s => `${s}-USDT-SWAP`
};

// ============================================================================
// UTILITIES
// ============================================================================
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function normalizeNumeric(v) { const n = parseFloat(v); return isNaN(n) ? null : n; }

// ============================================================================
// STATISTICS TRACKING
// ============================================================================
const stats = {
  recordsFetched: 0,
  recordsInserted: 0,
  symbolsCompleted: 0,
  symbolsFailed: 0,
  totalSymbols: perpList.length,
  totalRequests: 0,
  directRequests: 0,
  proxyRequests: 0,
  error429Count: 0,
  error429Symbols: new Set(),
  direct429Count: 0,
  proxy429Count: 0,
  otherErrors: []
};

let heartbeatStop = false;
let last429Count = 0;
let lastTotalRequests = 0;
let lastInsertLog = 0;

// ============================================================================
// HEARTBEAT LOOPS
// ============================================================================
async function statusHeartbeat() {
  while (!heartbeatStop) {
    const msg = `${OKX_CONFIG.perpspec} fetched ${stats.recordsFetched} records | Direct: ${stats.directRequests} req | Proxy: ${stats.proxyRequests} req`;
    console.log(`${STATUS_COLOR}${msg}${RESET}`);
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', msg);
    await sleep(HEARTBEAT_STATUS_INTERVAL);
  }
}

async function error429Heartbeat() {
  while (!heartbeatStop) {
    await sleep(HEARTBEAT_429_INTERVAL);
    
    const new429s = stats.error429Count - last429Count;
    const newRequests = stats.totalRequests - lastTotalRequests;
    
    if (new429s > 0) {
      const yellowStart = '\x1b[33m';
      const yellowEnd = '\x1b[0m';
      const msg = `${new429s} 429's/${newRequests} requests (Direct: ${stats.direct429Count}, Proxy: ${stats.proxy429Count})`;
      console.log(`${yellowStart}⚠️ (429) ${msg}${yellowEnd}`);
    }
    
    last429Count = stats.error429Count;
    lastTotalRequests = stats.totalRequests;
  }
}

// ============================================================================
// DATA PROCESSING - UNIFIED PERP_DATA STRUCTURE
// Converts OKX API response to unified perp_data format
// KEY FIX: Floor timestamps to nearest minute (e.g., 18:56:29 → 18:56:00)
// OKX returns timestamps in milliseconds, so we divide by 60000, floor, then multiply back
// ============================================================================
function processOKXData(rawData, baseSymbol) {
  const result = [];
  for (const rec of rawData) {
    const rawTs = parseInt(rec.ts, 10);
    const flooredTs = Math.floor(rawTs / 60000) * 60000; // Floor to nearest minute
    const ts = BigInt(flooredTs);
    const pfr = normalizeNumeric(rec.premium);
    
    if (pfr !== null && !isNaN(rawTs)) {
      result.push({
        ts: ts,
        symbol: baseSymbol,
        exchange: OKX_CONFIG.exchange,
        perpspec: OKX_CONFIG.perpspec,
        pfr: pfr
      });
    }
  }
  return result;
}

// ============================================================================
// CHUNKED INSERT HELPER
// ============================================================================
async function insertInChunks(allData) {
  if (allData.length === 0) return;
  
  for (let i = 0; i < allData.length; i += CHUNK_SIZE) {
    const chunk = allData.slice(i, i + CHUNK_SIZE);
    await dbManager.insertBackfillData(chunk);
    stats.recordsInserted += chunk.length;
    
    // Log at milestones
    if (Math.floor(stats.recordsInserted / DB_INSERT_MILESTONE) > Math.floor(lastInsertLog / DB_INSERT_MILESTONE)) {
      const msg = `${OKX_CONFIG.perpspec} inserted ${stats.recordsInserted} records`;
      console.log(`${STATUS_COLOR}${msg}${RESET}`);
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', msg);
      lastInsertLog = stats.recordsInserted;
    }
  }
}

// ============================================================================
// FETCH FUNCTION WITH DUAL IP SUPPORT
// ============================================================================
async function fetchOKXPremium(baseSymbol, useProxy = false) {
  const symbol = OKX_CONFIG.mapSymbol(baseSymbol);
  const allData = [];
  const seenTimestamps = new Set();
  
  const pageDelay = useProxy ? PROXY_PAGE_DELAY_MS : DIRECT_PAGE_DELAY_MS;
  const timeout = useProxy ? PROXY_TIMEOUT_MS : DIRECT_TIMEOUT_MS;
  const connType = useProxy ? 'proxy' : 'direct';
  
  let currentAfter = NOW + 1;
  let zeroNewCount = 0;
  let retries429 = 0;

  const axiosConfig = { timeout: timeout };
  
  if (useProxy) {
    axiosConfig.httpsAgent = proxyAgent;
    axiosConfig.httpAgent = proxyAgent;
  }

  while (currentAfter > START) {
    await sleep(Math.random() * pageDelay);

    try {
      const params = {
        instId: symbol,
        after: currentAfter.toString(),
        limit: OKX_CONFIG.limit
      };

      stats.totalRequests++;
      if (useProxy) stats.proxyRequests++;
      else stats.directRequests++;
      
      const res = await axios.get(OKX_CONFIG.url, { ...axiosConfig, params });

      if (res.data?.code !== '0') {
        console.log(`${STATUS_COLOR}⚠️  [${baseSymbol}/${connType}] API returned code: ${res.data?.code}${RESET}`);
        break;
      }
      
      const records = res.data.data || [];
      if (records.length === 0) break;

      let newRecords = 0;
      let oldestTs = currentAfter;

      for (const rec of records) {
        const ts = parseInt(rec.ts, 10);
        if (Number.isNaN(ts) || ts < START || ts > NOW) continue;
        if (seenTimestamps.has(ts)) continue;
        seenTimestamps.add(ts);
        allData.push(rec);
        newRecords++;
        if (ts < oldestTs) oldestTs = ts;
      }

      if (newRecords === 0) {
        zeroNewCount++;
        if (zeroNewCount >= 2) break;
      } else {
        zeroNewCount = 0;
        retries429 = 0;
      }

      if (oldestTs <= START) break;
      if (records.length < OKX_CONFIG.limit) break;

      currentAfter = oldestTs - 1;

      // Process and insert accumulated data in chunks
      if (allData.length >= CHUNK_SIZE) {
        const toInsert = allData.splice(0, CHUNK_SIZE);
        const processed = processOKXData(toInsert, baseSymbol);
        if (processed.length > 0) {
          await insertInChunks(processed);
          stats.recordsFetched += processed.length;
        }
      }

    } catch (err) {
      const code = err.response?.status;
      
      if (code === 429) {
        stats.error429Count++;
        stats.error429Symbols.add(baseSymbol);
        if (useProxy) stats.proxy429Count++;
        else stats.direct429Count++;
        
        retries429++;
        
        if (retries429 <= RETRY_429_MAX) {
          const backoff = RETRY_429_BASE_MS * Math.pow(2, retries429 - 1);
          await sleep(backoff);
          continue;
        } else {
          const errMsg = `[${baseSymbol}/${connType}] okx-pfr max 429 retries`;
          console.error(`${STATUS_COLOR}${errMsg}${RESET}`);
          stats.otherErrors.push({ symbol: baseSymbol, type: connType, error: 'Max 429 retries' });
          break;
        }
      } else if ([418, 500, 502, 503].includes(code)) {
        stats.error429Count++;
        stats.error429Symbols.add(baseSymbol);
        if (useProxy) stats.proxy429Count++;
        else stats.direct429Count++;
        await sleep(800 + Math.random() * 400);
        continue;
      } else {
        const errMsg = `[${baseSymbol}/${connType}] ${err.message}`;
        console.error(`${STATUS_COLOR}${errMsg}${RESET}`);
        stats.otherErrors.push({ symbol: baseSymbol, type: connType, error: err.message });
        await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'FETCH_FAILED', `${symbol}/${connType} fetch failed: ${err.message}`);
        break;
      }
    }
  }

  // Insert remaining data
  if (allData.length > 0) {
    const processed = processOKXData(allData, baseSymbol);
    if (processed.length > 0) {
      await insertInChunks(processed);
      stats.recordsFetched += processed.length;
    }
  }

  return allData;
}

// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================
async function backfill() {
  const startTime = Date.now();
  
  console.log(`${STATUS_COLOR}*okxPFR Starting ${SCRIPT_NAME} - Premium Funding Rate backfill${RESET}`);
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} started`);

  heartbeatStop = false;
  statusHeartbeat();
  error429Heartbeat();

  const directLimit = pLimit(DIRECT_CONCURRENCY);
  const proxyLimit = pLimit(PROXY_CONCURRENCY);
  
  const tasks = [];

  // Process direct symbols
  for (const baseSym of directSymbols) {
    tasks.push(directLimit(async () => {
      try {
        await fetchOKXPremium(baseSym, false);
        stats.symbolsCompleted++;
      } catch (err) {
        stats.symbolsFailed++;
        stats.symbolsCompleted++;
        console.error(`${STATUS_COLOR}[${baseSym}/direct] Failed: ${err.message}${RESET}`);
        stats.otherErrors.push({ symbol: baseSym, type: 'direct', error: err.message });
      }
    }));
  }

  // Process proxy symbols
  for (const baseSym of proxySymbols) {
    tasks.push(proxyLimit(async () => {
      try {
        await fetchOKXPremium(baseSym, true);
        stats.symbolsCompleted++;
      } catch (err) {
        stats.symbolsFailed++;
        stats.symbolsCompleted++;
        console.error(`${STATUS_COLOR}[${baseSym}/proxy] Failed: ${err.message}${RESET}`);
        stats.otherErrors.push({ symbol: baseSym, type: 'proxy', error: err.message });
      }
    }));
  }

  await Promise.allSettled(tasks);

  const msg = `${OKX_CONFIG.perpspec} backfill complete ${stats.symbolsCompleted} symbols. Final Loop started.`;
  console.log(`${STATUS_COLOR}*${msg}${RESET}`);
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', msg);

  // FINAL LOOP - Fetch recent 5 records per symbol
  const finalTasks = [];
  
  for (const baseSym of directSymbols) {
    finalTasks.push(directLimit(async () => {
      const symbol = OKX_CONFIG.mapSymbol(baseSym);
      try {
        const params = { instId: symbol, limit: 5 };
        stats.totalRequests++;
        stats.directRequests++;
        const res = await axios.get(OKX_CONFIG.url, { params, timeout: DIRECT_TIMEOUT_MS });
        
        if (res.data?.code === '0') {
          const records = res.data.data || [];
          const processed = processOKXData(records, baseSym);
          if (processed.length > 0) {
            await dbManager.insertBackfillData(processed);
            stats.recordsInserted += processed.length;
          }
        }
      } catch (err) {
        if (err.response?.status === 429) {
          stats.error429Count++;
          stats.direct429Count++;
          stats.error429Symbols.add(baseSym);
        }
      }
    }));
  }

  for (const baseSym of proxySymbols) {
    finalTasks.push(proxyLimit(async () => {
      const symbol = OKX_CONFIG.mapSymbol(baseSym);
      try {
        const params = { instId: symbol, limit: 5 };
        stats.totalRequests++;
        stats.proxyRequests++;
        const res = await axios.get(OKX_CONFIG.url, {
          params,
          timeout: PROXY_TIMEOUT_MS,
          httpsAgent: proxyAgent,
          httpAgent: proxyAgent
        });
        
        if (res.data?.code === '0') {
          const records = res.data.data || [];
          const processed = processOKXData(records, baseSym);
          if (processed.length > 0) {
            await dbManager.insertBackfillData(processed);
            stats.recordsInserted += processed.length;
          }
        }
      } catch (err) {
        if (err.response?.status === 429) {
          stats.error429Count++;
          stats.proxy429Count++;
          stats.error429Symbols.add(baseSym);
        }
      }
    }));
  }

  await Promise.allSettled(finalTasks);

  heartbeatStop = true;
  await sleep(1000);

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  const finalMessage = `⏱️ *PFR-okx ${SCRIPT_NAME} completed in ${duration}s`;
  console.log(`${finalMessage}`);
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', finalMessage);
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================
if (require.main === module) {
  backfill()
    .then(() => {
      process.exit(0);
    })
    .catch(err => {
      console.error(`${STATUS_COLOR}💥 Backfill failed: ${err}${RESET}`);
      process.exit(1);
    });
}

module.exports = { backfill };