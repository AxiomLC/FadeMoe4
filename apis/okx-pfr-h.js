// SCRIPT: okx-pfr-h.js
// Premium Funding Rate Backfill for OKX with HTTP/HTTPS Proxy Support
// Features: Separate speed controls for direct vs proxy, chunked DB inserts

const axios = require('axios');
const { HttpsProxyAgent } = require('https-proxy-agent');
const dbManager = require('../db/dbsetup');
const apiUtils = require('../api-utils');
const perpList = require('../perp-list');
const pLimit = require('p-limit');

const SCRIPT_NAME = 'okx-pfr-h.js';

// ============================================================================
// USER SPEED SETTINGS
// ============================================================================
const DAYS = 10;                      // Number of days back to fill

// DIRECT CONNECTION SETTINGS
const DIRECT_CONCURRENCY = 12;         // 12 d. Concurrent symbols on direct (conservative)
const DIRECT_PAGE_DELAY_MS = 200;     // 200 d. Delay between pages - ~8 req/s per symbol
const DIRECT_TIMEOUT_MS = 7000;      // 7000 d. Request timeout for direct

// PROXY CONNECTION SETTINGS  
const PROXY_CONCURRENCY = 12;          // 12 d. Concurrent symbols on proxy (lower - datacenter limits)
const PROXY_PAGE_DELAY_MS = 100;      // 100 d. Slower paging for proxy stability
const PROXY_TIMEOUT_MS = 9000;       // 9000 d. Longer timeout for proxy

// SHARED SETTINGS
const RETRY_429_MAX = 1;              // 1 d. Max retries on 429 errors
const RETRY_429_BASE_MS = 500;        // 500 d. Base delay for 429 retry (exponential backoff)
const HEARTBEAT_STATUS_INTERVAL = 15000;   // Status heartbeat every 15 sec
const HEARTBEAT_429_INTERVAL = 10000;      // 429 error summary every 10 sec
const DIRECT_PROXY_SPLIT = 50;        // 50 d. Percentage of symbols using direct (rest use proxy)
const DB_INSERT_MILESTONE = 25000;    // 25k d. Log inserts every 25k records

// ============================================================================
// PROXY CONFIGURATION - IPRoyal HTTP Datacenter Proxy
// ============================================================================
const PROXY_CONFIG = {
  username: '14a233d28dd8f',
  password: 'bf64d81ae2',
  host: '206.53.49.228',
  port: 12323
};

// Create HTTP/HTTPS proxy URL and agent
const proxyUrl = `http://${PROXY_CONFIG.username}:${PROXY_CONFIG.password}@${PROXY_CONFIG.host}:${PROXY_CONFIG.port}`;
const proxyAgent = new HttpsProxyAgent(proxyUrl);

console.log(`🔧 Proxy configured: ${PROXY_CONFIG.host}:${PROXY_CONFIG.port}`);

// ============================================================================
// DERIVED SETTINGS
// ============================================================================
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const NOW = Date.now();
const START = NOW - DAYS * MS_PER_DAY;

// Split symbols between direct and proxy
const splitIndex = Math.ceil(perpList.length * (DIRECT_PROXY_SPLIT / 100));
const directSymbols = perpList.slice(0, splitIndex);
const proxySymbols = perpList.slice(splitIndex);

console.log(`📊 Symbol split: ${directSymbols.length} direct, ${proxySymbols.length} proxy`);

// ============================================================================
// UTILITIES
// ============================================================================
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function normalizeNumeric(v) { const n = parseFloat(v); return isNaN(n) ? null : n; }

// ============================================================================
// EXCHANGE CONFIGURATION
// ============================================================================
const OKX_CONFIG = {
  perpspec: 'okx-pfr',
  url: 'https://www.okx.com/api/v5/public/premium-history',
  limit: 100,  // OKX allows 100 max per request
  mapSymbol: s => `${s}-USDT-SWAP`
};

// OKX Rate Limit: 20 requests per 2 seconds = 10 req/s
// With 8 direct symbols at ~8 req/s each = ~64 req/s potential
// But p-limit spreads them, so actual ~ 10 req/s total (safe)

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

// Status heartbeat - every 15 seconds
async function statusHeartbeat() {
  while (!heartbeatStop) {
    const msg = `${OKX_CONFIG.perpspec} fetched ${stats.recordsFetched} records | Direct: ${stats.directRequests} req | Proxy: ${stats.proxyRequests} req`;
    console.log(`[STATUS] ${msg}`);
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', msg);
    await sleep(HEARTBEAT_STATUS_INTERVAL);
  }
}

// 429 error heartbeat - every 10 seconds
async function error429Heartbeat() {
  while (!heartbeatStop) {
    await sleep(HEARTBEAT_429_INTERVAL);
    
    const new429s = stats.error429Count - last429Count;
    const newRequests = stats.totalRequests - lastTotalRequests;
    
    if (new429s > 0) {
      const yellowStart = '\x1b[33m';
      const yellowEnd = '\x1b[0m';
      const msg = `${yellowStart}${new429s} 429's received on ${newRequests} requests (Direct: ${stats.direct429Count}, Proxy: ${stats.proxy429Count}). Affected: ${stats.error429Symbols.size} symbols${yellowEnd}`;
      console.log(`[429-SUMMARY] ${msg}`);
    }
    
    last429Count = stats.error429Count;
    lastTotalRequests = stats.totalRequests;
  }
}

// ============================================================================
// FETCH FUNCTION WITH SEPARATE SETTINGS
// ============================================================================
async function fetchOKXPremium(baseSymbol, useProxy = false) {
  const symbol = OKX_CONFIG.mapSymbol(baseSymbol);
  const allData = [];
  const seenTimestamps = new Set();
  
  // Select settings based on connection type
  const pageDelay = useProxy ? PROXY_PAGE_DELAY_MS : DIRECT_PAGE_DELAY_MS;
  const timeout = useProxy ? PROXY_TIMEOUT_MS : DIRECT_TIMEOUT_MS;
  const connType = useProxy ? 'proxy' : 'direct';
  
  let currentAfter = NOW + 1;
  let zeroNewCount = 0;
  let retries429 = 0;

  const axiosConfig = {
    timeout: timeout
  };
  
  // Use HttpsProxyAgent for HTTP/HTTPS proxies (native support)
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
        console.log(`⚠️  [${baseSymbol}/${connType}] API returned code: ${res.data?.code}`);
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
        retries429 = 0; // Reset on success
      }

      if (oldestTs <= START) break;
      if (records.length < OKX_CONFIG.limit) break;

      currentAfter = oldestTs - 1;

      // Process and insert accumulated data
      if (allData.length > 0) {
        const processed = processOKXData(allData.splice(0), baseSymbol);
        if (processed.length > 0) {
          await dbManager.insertData(OKX_CONFIG.perpspec, processed);
          stats.recordsInserted += processed.length;
          stats.recordsFetched += processed.length;
          
          // Log at 25k milestones only
          if (Math.floor(stats.recordsInserted / DB_INSERT_MILESTONE) > Math.floor(lastInsertLog / DB_INSERT_MILESTONE)) {
            const msg = `${OKX_CONFIG.perpspec} inserted ${stats.recordsInserted} records to db`;
            console.log(`[INSERT] ${msg}`);
            await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', msg);
            lastInsertLog = stats.recordsInserted;
          }
          
          allData.length = 0;
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
          const redStart = '\x1b[31m';
          const redEnd = '\x1b[0m';
          const errMsg = `${redStart}[${baseSymbol}/${connType}] Max 429 retries exceeded${redEnd}`;
          console.error(errMsg);
          stats.otherErrors.push({ symbol: baseSymbol, type: connType, error: 'Max 429 retries' });
          break;
        }
      } else if ([418, 500, 502, 503].includes(code)) {
        // Server errors - treat as transient, retry once
        stats.error429Count++;
        stats.error429Symbols.add(baseSymbol);
        if (useProxy) stats.proxy429Count++;
        else stats.direct429Count++;
        await sleep(800 + Math.random() * 400);
        continue;
      } else {
        const redStart = '\x1b[31m';
        const redEnd = '\x1b[0m';
        const errMsg = `${redStart}[${baseSymbol}/${connType}] ${err.message}${redEnd}`;
        console.error(errMsg);
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
      await dbManager.insertData(OKX_CONFIG.perpspec, processed);
      stats.recordsInserted += processed.length;
      stats.recordsFetched += processed.length;
    }
  }

  return allData;
}

// ============================================================================
// DATA PROCESSING
// ============================================================================
function processOKXData(rawData, baseSymbol) {
  const result = [];
  for (const rec of rawData) {
    const ts = parseInt(rec.ts);
    const pfr = normalizeNumeric(rec.premium);
    if (pfr !== null && !isNaN(ts)) {
      result.push({
        ts,
        symbol: baseSymbol,
        source: OKX_CONFIG.perpspec,
        perpspec: OKX_CONFIG.perpspec,
        interval: '1m',
        pfr
      });
    }
  }
  return result;
}

// ============================================================================
// MAIN BACKFILL FUNCTION
// ============================================================================
async function backfill() {
  const startTime = Date.now();
  
  console.log(`\n🚀 Starting ${SCRIPT_NAME} - Premium Funding Rate backfill`);
  console.log(`📊 Config: ${DAYS} days | Direct: ${DIRECT_CONCURRENCY} concurrent | Proxy: ${PROXY_CONCURRENCY} concurrent`);
  console.log(`⚙️  Direct: ${DIRECT_PAGE_DELAY_MS}ms delay, ${DIRECT_TIMEOUT_MS}ms timeout`);
  console.log(`⚙️  Proxy: ${PROXY_PAGE_DELAY_MS}ms delay, ${PROXY_TIMEOUT_MS}ms timeout`);
  console.log(`📈 Processing ${perpList.length} symbols (${directSymbols.length} direct + ${proxySymbols.length} proxy)\n`);
  
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} started`);

  // Start heartbeat loops
  heartbeatStop = false;
  statusHeartbeat();
  error429Heartbeat();

  // Separate p-limit for direct and proxy
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
        const redStart = '\x1b[31m';
        const redEnd = '\x1b[0m';
        console.error(`${redStart}[${baseSym}/direct] Failed: ${err.message}${redEnd}`);
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
        const redStart = '\x1b[31m';
        const redEnd = '\x1b[0m';
        console.error(`${redStart}[${baseSym}/proxy] Failed: ${err.message}${redEnd}`);
        stats.otherErrors.push({ symbol: baseSym, type: 'proxy', error: err.message });
      }
    }));
  }

  await Promise.allSettled(tasks);

  const msg = `${OKX_CONFIG.perpspec} backfilling complete ${stats.symbolsCompleted} symbols. Final Loop started.`;
  console.log(`\n✅ ${msg}`);
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', msg);

  // ========================================================================
  // FINAL LOOP - Fetch recent 5 records per symbol
  // ========================================================================
  console.log('📥 Starting final loop...\n');

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
            await dbManager.insertData(OKX_CONFIG.perpspec, processed);
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
            await dbManager.insertData(OKX_CONFIG.perpspec, processed);
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

  // ========================================================================
  // COMPLETION SUMMARY
  // ========================================================================
  const dur = ((Date.now() - startTime) / 1000).toFixed(1);
  const successRate = ((stats.totalSymbols - stats.symbolsFailed) / stats.totalSymbols * 100).toFixed(1);
  
  const summary = [
    `\n🎉 ${SCRIPT_NAME} completed in ${dur}s`,
    `📊 Records: ${stats.recordsInserted} inserted`,
    `📡 Requests: ${stats.directRequests} direct, ${stats.proxyRequests} proxy`,
    `✅ Success: ${successRate}%`,
    `⚠️  429 errors: ${stats.error429Count} total (Direct: ${stats.direct429Count}, Proxy: ${stats.proxy429Count}) on ${stats.error429Symbols.size} symbols`,
    `❌ Other errors: ${stats.otherErrors.length}`
  ].join('\n');
  
  console.log(summary);
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'completed', `Completed in ${dur}s | ${stats.recordsInserted} records | ${successRate}% success`);
}

// ============================================================================
// MODULE ENTRY POINT
// ============================================================================
if (require.main === module) {
  backfill()
    .then(() => {
      console.log('\n✅ PFR backfill with datacenter proxy complete.');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 Backfill failed:', err);
      process.exit(1);
    });
}

module.exports = { backfill };