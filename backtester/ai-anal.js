// backtester/ai-anal.js
// ============================================================================
// AI STATISTICAL ANALYZER - Approach B Discovery Engine
// ============================================================================
// Uses correlation analysis to discover profitable parameter combinations
// without brute-forcing every possibility
// ============================================================================

const db = require('../db/dbsetup');
const fs = require('fs');
const path = require('path');

// ============================================================================
// CONFIGURATION
// ============================================================================
const CONFIG = {
  MIN_CORRELATION: 0.20,        // Minimum correlation to consider
  MIN_SAMPLE_SIZE: 500,         // Minimum data points required
  TOP_N_PARAMS: 20,             // Test top N correlated params
  QUICK_TEST_DAYS: 7,           // Use 7 days for quick testing
  PREDICTION_WINDOW: 3,         // Minutes ahead to check price movement
  CONFIDENCE_THRESHOLD: 0.25,   // |correlation| > 0.25 = "high confidence"
  OUTPUT_DIR: path.join(__dirname, 'ai-suggestions')
};

// ============================================================================
// MAIN DISCOVERY FUNCTION
// ============================================================================
/**
 * Discover profitable parameter combinations via statistical analysis
 * @param {object} options - Override config options
 * @returns {Array} Top algo candidates sorted by profit potential
 */
async function discoverProfitableParams(options = {}) {
  const config = { ...CONFIG, ...options };
  
  console.log('🔍 AI Discovery Engine starting...');
  console.log(`📊 Analyzing correlations (min ${config.MIN_CORRELATION})...`);
  
  const startTime = Date.now();

  try {
    // Step 1: Find params with predictive correlation
    const correlations = await analyzeAllParamCorrelations(config);
    console.log(`✅ Found ${correlations.length} significant correlations`);

    // Step 2: Test top candidates
    const candidates = correlations.slice(0, config.TOP_N_PARAMS);
    console.log(`🧪 Testing top ${candidates.length} candidates...`);
    
    const results = [];
    for (const candidate of candidates) {
      const algoResult = await quickTestAlgo(candidate, config);
      if (algoResult.pf > 0.8) { // Only keep profitable ones
        results.push(algoResult);
      }
    }

    // Step 3: Sort by profit factor
    results.sort((a, b) => b.pf - a.pf);
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`✨ Discovery complete in ${duration}s`);
    console.log(`📈 Found ${results.length} profitable algos`);

    // Step 4: Save results
    await saveDiscoveryResults(results);

    return results.slice(0, 3); // Return top 3

  } catch (error) {
    console.error('❌ Discovery failed:', error.message);
    throw error;
  }
}

// ============================================================================
// CORRELATION ANALYSIS
// ============================================================================
/**
 * Analyze all params for correlation with future price movement
 * @param {object} config
 * @returns {Array} Sorted by absolute correlation
 */
async function analyzeAllParamCorrelations(config) {
  const params = [
    'c', 'v', 'oi', 'pfr', 'lsr', 'rsi1', 'rsi60', 'tbv', 'tsv', 
    'lqprice', 'lqqty'
  ];
  const timeframes = ['1m', '5m', '10m'];
  const exchanges = ['bin', 'byb', 'okx'];

  const allCorrelations = [];

  for (const exchange of exchanges) {
    for (const param of params) {
      for (const timeframe of timeframes) {
        const correlation = await calculateParamCorrelation(
          param, 
          timeframe, 
          exchange, 
          config
        );
        
        if (correlation && Math.abs(correlation.corr) >= config.MIN_CORRELATION) {
          allCorrelations.push(correlation);
        }
      }
    }
  }

  // Sort by absolute correlation (strongest predictors first)
  allCorrelations.sort((a, b) => Math.abs(b.corr) - Math.abs(a.corr));

  return allCorrelations;
}

/**
 * Calculate correlation between param and future price movement
 * @param {string} param - e.g., 'pfr'
 * @param {string} timeframe - '1m', '5m', '10m'
 * @param {string} exchange - 'bin', 'byb', 'okx'
 * @param {object} config
 * @returns {object|null} Correlation data or null if insufficient data
 */
async function calculateParamCorrelation(param, timeframe, exchange, config) {
  const paramCol = `${param}_chg_${timeframe}`;
  
  const query = `
    WITH signals AS (
      SELECT 
        ts, symbol, exchange,
        ${paramCol} as param_value,
        LEAD(c_chg_1m, ${config.PREDICTION_WINDOW}) OVER (
          PARTITION BY symbol, exchange ORDER BY ts
        ) as future_move
      FROM perp_metrics
      WHERE exchange = $1
        AND ${paramCol} IS NOT NULL
        AND ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '10 days')*1000
    )
    SELECT 
      CORR(param_value, future_move) as correlation,
      STDDEV(param_value) as param_stddev,
      AVG(param_value) as param_mean,
      COUNT(*) as sample_size
    FROM signals
    WHERE future_move IS NOT NULL
    HAVING COUNT(*) >= $2;
  `;

  try {
    const result = await db.pool.query(query, [exchange, config.MIN_SAMPLE_SIZE]);
    
    if (result.rows.length === 0) return null;

    const row = result.rows[0];
    const corr = parseFloat(row.correlation);
    
    if (isNaN(corr)) return null;

    return {
      exchange,
      param: paramCol,
      corr,
      mean: parseFloat(row.param_mean),
      stddev: parseFloat(row.param_stddev),
      sampleSize: parseInt(row.sample_size),
      confidence: Math.abs(corr) > config.CONFIDENCE_THRESHOLD ? 'high' : 'medium'
    };

  } catch (error) {
    console.error(`Error analyzing ${exchange}_${paramCol}:`, error.message);
    return null;
  }
}

// ============================================================================
// ALGO TESTING
// ============================================================================
/**
 * Quick test an algo candidate (7 days, simple simulation)
 * @param {object} candidate - Correlation result
 * @param {object} config
 * @returns {object} Test results with AlgoQL
 */
async function quickTestAlgo(candidate, config) {
  const { exchange, param, corr, mean, stddev } = candidate;

  // Determine threshold: mean + 0.5*stddev for positive corr, mean - 0.5*stddev for negative
  const threshold = corr > 0 
    ? (mean + 0.5 * stddev).toFixed(4)
    : (mean - 0.5 * stddev).toFixed(4);
  
  const operator = corr > 0 ? '>' : '<';
  const direction = corr > 0 ? 'Long' : 'Short';

  // Build simple AlgoQL
  const algoql = `ALL;${direction};${exchange}_${param}${operator}${threshold}`;

  // Query matching trades
  const query = `
    SELECT ts, symbol, exchange, c, o, h, l
    FROM perp_metrics
    WHERE exchange = $1
      AND ${param} ${operator} $2
      AND ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '${config.QUICK_TEST_DAYS} days')*1000
    ORDER BY ts ASC
    LIMIT 5000;
  `;

  try {
    const result = await db.pool.query(query, [exchange, parseFloat(threshold)]);
    const trades = result.rows;

    if (trades.length === 0) {
      return {
        algoql,
        correlation: corr,
        pf: 0,
        winRate: '0.0%',
        totalTrades: 0,
        confidence: 'insufficient_data'
      };
    }

    // Simulate trades with simple scheme
    const scheme = direction === 'Long' 
      ? { tp1: 0.5, tp2: 0.8, sl: 0.15 }
      : { tp1: 0.5, tp2: 0.8, sl: 0.15 };

    const outcomes = simulateQuickTrades(trades, scheme, direction);
    const stats = calculateStats(outcomes);

    return {
      algoql,
      correlation: corr,
      pf: stats.pf,
      winRate: stats.winRate,
      totalTrades: stats.total,
      tp1: stats.tp1,
      tp2: stats.tp2,
      losses: stats.losses,
      confidence: candidate.confidence,
      exchange,
      param
    };

  } catch (error) {
    console.error(`Error testing ${algoql}:`, error.message);
    return {
      algoql,
      correlation: corr,
      pf: 0,
      winRate: '0.0%',
      totalTrades: 0,
      confidence: 'error'
    };
  }
}

/**
 * Simplified trade simulation (faster than full backtester)
 * @param {Array} trades - Data points
 * @param {object} scheme - { tp1, tp2, sl }
 * @param {string} direction - 'Long' or 'Short'
 * @returns {Array} Trade outcomes
 */
function simulateQuickTrades(trades, scheme, direction) {
  const outcomes = [];
  
  for (let i = 0; i < trades.length - 5; i++) {
    const entry = trades[i];
    const entryPrice = parseFloat(entry.c);
    
    // Look ahead 5 candles (~5 minutes)
    const future = trades.slice(i + 1, i + 6);
    
    let outcome = 'none';
    
    if (direction === 'Long') {
      const tp1Price = entryPrice * (1 + scheme.tp1 / 100);
      const tp2Price = entryPrice * (1 + scheme.tp2 / 100);
      const slPrice = entryPrice * (1 - scheme.sl / 100);
      
      const hitTP2 = future.some(t => parseFloat(t.h) >= tp2Price);
      const hitTP1 = future.some(t => parseFloat(t.h) >= tp1Price);
      const hitSL = future.some(t => parseFloat(t.l) <= slPrice);
      
      if (hitSL) outcome = 'loss';
      else if (hitTP2) outcome = 'tp2';
      else if (hitTP1) outcome = 'tp1';
      
    } else { // Short
      const tp1Price = entryPrice * (1 - scheme.tp1 / 100);
      const tp2Price = entryPrice * (1 - scheme.tp2 / 100);
      const slPrice = entryPrice * (1 + scheme.sl / 100);
      
      const hitTP2 = future.some(t => parseFloat(t.l) <= tp2Price);
      const hitTP1 = future.some(t => parseFloat(t.l) <= tp1Price);
      const hitSL = future.some(t => parseFloat(t.h) >= slPrice);
      
      if (hitSL) outcome = 'loss';
      else if (hitTP2) outcome = 'tp2';
      else if (hitTP1) outcome = 'tp1';
    }
    
    outcomes.push({ outcome, symbol: entry.symbol, ts: entry.ts });
  }
  
  return outcomes;
}

/**
 * Calculate profit statistics
 * @param {Array} outcomes
 * @returns {object} Statistics
 */
function calculateStats(outcomes) {
  const total = outcomes.length;
  const tp1 = outcomes.filter(t => t.outcome === 'tp1').length;
  const tp2 = outcomes.filter(t => t.outcome === 'tp2').length;
  const losses = outcomes.filter(t => t.outcome === 'loss').length;
  const wins = tp1 + tp2;
  
  const winRate = total > 0 ? ((wins / total) * 100).toFixed(1) : '0.0';
  
  // Simple PF calculation (TP1=0.5%, TP2=0.8%, SL=-0.15%)
  const profit = (tp1 * 0.5) + (tp2 * 0.8);
  const loss = losses * 0.15;
  const pf = loss > 0 ? (profit / loss).toFixed(2) : profit > 0 ? '999.99' : '0.00';
  
  return {
    total,
    tp1,
    tp2,
    losses,
    winRate: winRate + '%',
    pf: parseFloat(pf)
  };
}

// ============================================================================
// FILE MANAGEMENT
// ============================================================================
/**
 * Save discovery results to ai-suggestions folder
 * @param {Array} results
 */
async function saveDiscoveryResults(results) {
  // Ensure directory exists
  if (!fs.existsSync(CONFIG.OUTPUT_DIR)) {
    fs.mkdirSync(CONFIG.OUTPUT_DIR, { recursive: true });
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `ai_discovery_${timestamp}.json`;
  const filepath = path.join(CONFIG.OUTPUT_DIR, filename);

  const output = {
    timestamp: new Date().toISOString(),
    discoveryConfig: {
      minCorrelation: CONFIG.MIN_CORRELATION,
      minSampleSize: CONFIG.MIN_SAMPLE_SIZE,
      testDays: CONFIG.QUICK_TEST_DAYS
    },
    totalCandidates: results.length,
    topAlgos: results.slice(0, 10),
    allResults: results
  };

  fs.writeFileSync(filepath, JSON.stringify(output, null, 2));
  console.log(`💾 Saved results to: ${filename}`);
}

/**
 * Load previous discovery results for AI reference
 * @returns {Array} All previous discoveries
 */
function loadPreviousDiscoveries() {
  if (!fs.existsSync(CONFIG.OUTPUT_DIR)) {
    return [];
  }

  const files = fs.readdirSync(CONFIG.OUTPUT_DIR)
    .filter(f => f.startsWith('ai_discovery_') && f.endsWith('.json'))
    .sort()
    .reverse(); // Most recent first

  const discoveries = [];
  
  // Load up to 10 most recent
  for (const file of files.slice(0, 10)) {
    try {
      const content = fs.readFileSync(path.join(CONFIG.OUTPUT_DIR, file), 'utf8');
      const data = JSON.parse(content);
      discoveries.push(data);
    } catch (error) {
      console.error(`Error loading ${file}:`, error.message);
    }
  }

  return discoveries;
}

// ============================================================================
// EXPORTS
// ============================================================================
module.exports = {
  discoverProfitableParams,
  calculateParamCorrelation,
  quickTestAlgo,
  loadPreviousDiscoveries,
  CONFIG
};

// ============================================================================
// CLI TESTING
// ============================================================================
if (require.main === module) {
  console.log('🚀 AI Statistical Analyzer - Discovery Mode\n');
  
  discoverProfitableParams()
    .then(results => {
      console.log('\n✨ Top 3 Discovered Algos:\n');
      results.forEach((algo, idx) => {
        console.log(`#${idx + 1}: PF ${algo.pf} | ${algo.winRate} WR | ${algo.totalTrades} trades`);
        console.log(`   AlgoQL: ${algo.algoql}`);
        console.log(`   Correlation: ${algo.correlation.toFixed(3)} (${algo.confidence})`);
        console.log('');
      });
    })
    .catch(err => {
      console.error('❌ Discovery failed:', err);
      process.exit(1);
    });
}