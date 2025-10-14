// SCRIPT: b-weight.js
// Binance Futures API Weight Monitor - Standalone
// Tracks weight usage across all scripts hitting Binance Futures API
// Run from root: node b-weight.js

const fs = require('fs');
const path = require('path');

// ============================================================================
// BINANCE FUTURES API LIMITS (from official docs)
// ============================================================================
const BINANCE_LIMITS = {
  WEIGHT_PER_MINUTE: 2400,  // IP limit
  RAW_REQUESTS_PER_MINUTE: 1200,  // Raw request limit
  
  // Endpoint-specific weights
  ENDPOINTS: {
    '/fapi/v1/klines': { weight: 1, desc: 'Kline/Candlestick Data' },
    '/futures/data/openInterestHist': { weight: 1, desc: 'Open Interest History' },
    '/futures/data/globalLongShortAccountRatio': { weight: 1, desc: 'Long/Short Ratio (Accounts)' },
    '/futures/data/takerlongshortRatio': { weight: 1, desc: 'Taker Buy/Sell Volume' },
    '/fapi/v1/premiumIndexKlines': { weight: 1, desc: 'Premium Index Klines' }
  }
};

// ============================================================================
// WEIGHT TRACKING STATE
// ============================================================================
const weightStats = {
  // Per-perpspec tracking
  'bin-ohlcv': { requests: 0, weight: 0, endpoint: '/fapi/v1/klines' },
  'bin-oi': { requests: 0, weight: 0, endpoint: '/futures/data/openInterestHist' },
  'bin-lsr': { requests: 0, weight: 0, endpoint: '/futures/data/globalLongShortAccountRatio' },
  'bin-tv': { requests: 0, weight: 0, endpoint: '/futures/data/takerlongshortRatio' },
  'bin-pfr': { requests: 0, weight: 0, endpoint: '/fapi/v1/premiumIndexKlines' },
  
  // Totals
  totalRequests: 0,
  totalWeight: 0,
  
  // Window tracking (resets every minute)
  windowStart: Date.now(),
  windowRequests: 0,
  windowWeight: 0
};

// ============================================================================
// SHARED STATE FILE (all scripts write to this)
// ============================================================================
const WEIGHT_LOG_FILE = path.join(__dirname, '.binance-weight.json');

// Initialize empty log file
if (!fs.existsSync(WEIGHT_LOG_FILE)) {
  fs.writeFileSync(WEIGHT_LOG_FILE, JSON.stringify({ logs: [] }));
}

// ============================================================================
// COLOR CODES
// ============================================================================
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const RED = '\x1b[31m';
const RESET = '\x1b[0m';
const BOLD = '\x1b[1m';

// ============================================================================
// READ WEIGHT LOGS FROM FILE
// ============================================================================
function readWeightLogs() {
  try {
    const data = fs.readFileSync(WEIGHT_LOG_FILE, 'utf8');
    return JSON.parse(data);
  } catch (err) {
    return { logs: [] };
  }
}

// ============================================================================
// PROCESS LOGS AND UPDATE STATS
// ============================================================================
function processLogs() {
  const logData = readWeightLogs();
  const now = Date.now();
  const oneMinuteAgo = now - 60000;
  
  // Reset stats
  Object.keys(weightStats).forEach(key => {
    if (typeof weightStats[key] === 'object' && weightStats[key].requests !== undefined) {
      weightStats[key].requests = 0;
      weightStats[key].weight = 0;
    }
  });
  weightStats.totalRequests = 0;
  weightStats.totalWeight = 0;
  weightStats.windowRequests = 0;
  weightStats.windowWeight = 0;
  
  // Process logs from last minute
  logData.logs
    .filter(log => log.timestamp > oneMinuteAgo)
    .forEach(log => {
      const perpspec = log.perpspec;
      if (weightStats[perpspec]) {
        weightStats[perpspec].requests++;
        weightStats[perpspec].weight += log.weight;
        weightStats.totalRequests++;
        weightStats.totalWeight += log.weight;
        weightStats.windowRequests++;
        weightStats.windowWeight += log.weight;
      }
    });
  
  // Clean old logs (keep only last 2 minutes)
  const twoMinutesAgo = now - 120000;
  logData.logs = logData.logs.filter(log => log.timestamp > twoMinutesAgo);
  fs.writeFileSync(WEIGHT_LOG_FILE, JSON.stringify(logData));
}

// ============================================================================
// DISPLAY WEIGHT STATS
// ============================================================================
function displayStats() {
  console.clear();
  
  const weightPercent = (weightStats.windowWeight / BINANCE_LIMITS.WEIGHT_PER_MINUTE * 100).toFixed(1);
  const reqPercent = (weightStats.windowRequests / BINANCE_LIMITS.RAW_REQUESTS_PER_MINUTE * 100).toFixed(1);
  
  // Determine color based on usage
  let weightColor = GREEN;
  if (weightPercent > 80) weightColor = RED;
  else if (weightPercent > 60) weightColor = YELLOW;
  
  let reqColor = GREEN;
  if (reqPercent > 80) reqColor = RED;
  else if (reqPercent > 60) reqColor = YELLOW;
  
  console.log(`${BOLD}${GREEN}╔═══════════════════════════════════════════════════════════════════════╗${RESET}`);
  console.log(`${BOLD}${GREEN}║           BINANCE FUTURES API WEIGHT MONITOR (1-MIN WINDOW)           ║${RESET}`);
  console.log(`${BOLD}${GREEN}╚═══════════════════════════════════════════════════════════════════════╝${RESET}\n`);
  
  // Overall limits
  console.log(`${BOLD}${GREEN}OVERALL LIMITS (per minute):${RESET}`);
  console.log(`${weightColor}  Weight:    ${weightStats.windowWeight} / ${BINANCE_LIMITS.WEIGHT_PER_MINUTE} (${weightPercent}%)${RESET}`);
  console.log(`${reqColor}  Requests:  ${weightStats.windowRequests} / ${BINANCE_LIMITS.RAW_REQUESTS_PER_MINUTE} (${reqPercent}%)${RESET}\n`);
  
  // Per-perpspec breakdown
  console.log(`${BOLD}${GREEN}PER-SCRIPT BREAKDOWN:${RESET}`);
  console.log(`${GREEN}┌─────────────┬──────────┬────────┬──────────────────────────────────────────┐${RESET}`);
  console.log(`${GREEN}│ Perpspec    │ Requests │ Weight │ Endpoint Info                            │${RESET}`);
  console.log(`${GREEN}├─────────────┼──────────┼────────┼──────────────────────────────────────────┤${RESET}`);
  
  const perpspecs = ['bin-ohlcv', 'bin-oi', 'bin-lsr', 'bin-tv', 'bin-pfr'];
  perpspecs.forEach(perpspec => {
    const stats = weightStats[perpspec];
    const endpointInfo = BINANCE_LIMITS.ENDPOINTS[stats.endpoint];
    const reqStr = stats.requests.toString().padEnd(8);
    const weightStr = stats.weight.toString().padEnd(6);
    const perpspecStr = perpspec.padEnd(11);
    const desc = endpointInfo.desc.substring(0, 38).padEnd(38);
    
    console.log(`${GREEN}│ ${perpspecStr} │ ${reqStr} │ ${weightStr} │ ${desc} │${RESET}`);
  });
  
  console.log(`${GREEN}└─────────────┴──────────┴────────┴──────────────────────────────────────────┘${RESET}\n`);
  
  // Endpoint details
  console.log(`${BOLD}${GREEN}ENDPOINT DETAILS:${RESET}`);
  perpspecs.forEach(perpspec => {
    const stats = weightStats[perpspec];
    const endpointInfo = BINANCE_LIMITS.ENDPOINTS[stats.endpoint];
    if (stats.requests > 0) {
      const reqPerSec = (stats.requests / 60).toFixed(2);
      console.log(`${GREEN}  ${perpspec.padEnd(12)} → ${stats.endpoint}${RESET}`);
      console.log(`${GREEN}    ${endpointInfo.desc} | Weight: ${endpointInfo.weight} | Rate: ${reqPerSec} req/sec${RESET}`);
    }
  });
  
  // Warnings
  if (weightPercent > 80) {
    console.log(`\n${RED}${BOLD}⚠️  WARNING: Approaching weight limit! (${weightPercent}%)${RESET}`);
  }
  if (reqPercent > 80) {
    console.log(`${RED}${BOLD}⚠️  WARNING: Approaching request limit! (${reqPercent}%)${RESET}`);
  }
  
  console.log(`\n${GREEN}Last updated: ${new Date().toLocaleTimeString()}${RESET}`);
  console.log(`${GREEN}Press Ctrl+C to exit${RESET}\n`);
}

// ============================================================================
// MAIN MONITOR LOOP
// ============================================================================
function startMonitor() {
  console.log(`${GREEN}${BOLD}Starting Binance Weight Monitor...${RESET}`);
  console.log(`${GREEN}Monitoring file: ${WEIGHT_LOG_FILE}${RESET}\n`);
  
  // Initial display
  processLogs();
  displayStats();
  
  // Update every 2 seconds
  setInterval(() => {
    processLogs();
    displayStats();
  }, 2000);
}

// ============================================================================
// CLEANUP ON EXIT
// ============================================================================
process.on('SIGINT', () => {
  console.log(`\n${GREEN}${BOLD}Shutting down monitor...${RESET}`);
  process.exit(0);
});

// ============================================================================
// START
// ============================================================================
if (require.main === module) {
  startMonitor();
}

// ============================================================================
// EXPORT FOR USE IN SCRIPTS
// ============================================================================
module.exports = {
  logRequest: function(perpspec, endpoint, weight = 1) {
    const logData = readWeightLogs();
    logData.logs.push({
      perpspec,
      endpoint,
      weight,
      timestamp: Date.now()
    });
    fs.writeFileSync(WEIGHT_LOG_FILE, JSON.stringify(logData));
  },
  
  WEIGHT_LOG_FILE
};