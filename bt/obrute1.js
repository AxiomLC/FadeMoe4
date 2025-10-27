/*
  bt/brute1.js - Core Pattern Discovery (Brute Force Single-Param Scanner)
  Description: Tests single-parameter conditions across exchanges/timeframes to find
               profitable base patterns. Outputs top 100 results with best TP/SL schemes.
               EXCLUDES c/v params (reserved for tune1.js refinement).
  
  Output: brute/brute1_{datetime}.json with summary stats and ranked algos
  Usage: node bt/brute1.js
  Date: 27 Oct 2025
*/

const fs = require('fs');
const path = require('path');
const dbManager = require('../db/dbsetup');

// ============================================================================
// USER CONFIGURATION (Edit these values before running)
// ============================================================================
const CONFIG = {
  // Core threshold percentages to test (positive AND negative)
  // Script tests: >3, >5, >10... AND <-3, <-5, <-10...
  corePercent: [3, 5, 10, 30, 50, 80, 100, 150],
  
  // Trade scheme parameters
  coreTrade: {
    posTime: 10,              // Minutes per trade window
    posVal: 1000,             // USD value per trade
    tpPerc: [1.2, 1.5, 2.0, 2.5],  // Take profit % options
    slPerc: [0.6, 0.8, 1.2, 1.8]   // Stop loss % options
  },
  
  // Symbol selection
  targetSymbols: {
    useAll: true,             // If true, uses all symbols from DB
    list: ['BTC', 'ETH', 'SOL', 'DOGE', 'XRP']  // Used if useAll = false
  },
  
  // Data sampling limits
  maxSamples: 10000,          // Max rows to fetch per symbol
  minTrades: 15,              // Minimum trades for valid pattern
  maxTrades: 200,             // Maximum trades (filters noise)
  
  // Parameters to test (EXCLUDES c/v - reserved for tune1)
  params: [
    'oi_chg_1m', 'oi_chg_5m', 'oi_chg_10m',
    'pfr_chg_1m', 'pfr_chg_5m', 'pfr_chg_10m',
    'lsr_chg_1m', 'lsr_chg_5m', 'lsr_chg_10m',
    'rsi1_chg_1m', 'rsi1_chg_5m', 'rsi1_chg_10m',
    'rsi60_chg_1m', 'rsi60_chg_5m', 'rsi60_chg_10m',
    'tbv_chg_1m', 'tbv_chg_5m', 'tbv_chg_10m',
    'tsv_chg_1m', 'tsv_chg_5m', 'tsv_chg_10m'
  ],
  
  // Exchanges to test
  exchanges: ['bin', 'byb', 'okx'],
  
  // Output settings
  outputTop: 100              // Number of top results to save
};

// ============================================================================
// MAIN EXECUTION
// ============================================================================
(async () => {
  console.log('🔥 BRUTE1: Core Pattern Discovery Scanner');
  console.log('=' .repeat(70));
  console.log(`📊 Config:`);
  console.log(`   - Core%: ${CONFIG.corePercent.join(', ')} (± tested)`);
  console.log(`   - Params: ${CONFIG.params.length} (no c/v)`);
  console.log(`   - Exchanges: ${CONFIG.exchanges.join(', ')}`);
  console.log(`   - TP schemes: ${CONFIG.coreTrade.tpPerc.length} × SL: ${CONFIG.coreTrade.slPerc.length}`);
  console.log(`   - Trade limits: ${CONFIG.minTrades}-${CONFIG.maxTrades} trades`);
  console.log('=' .repeat(70));
  
  const startTime = Date.now();
  
  try {
    // Step 1: Fetch sample data
    console.log('\n📥 Step 1: Fetching sample data...');
    const rawData = await fetchSampleData();
    console.log(`✓ Loaded ${rawData.length} data points`);
    
    if (rawData.length < 100) {
      throw new Error('Insufficient data. Need at least 100 rows in perp_metrics.');
    }
    
    // Step 2: Generate candidate algos (single-param only)
    console.log('\n🧬 Step 2: Generating test candidates...');
    const candidates = generateCandidates();
    console.log(`✓ Created ${candidates.length} single-param candidates`);
    
    // Step 3: Test all candidates with all TP/SL schemes
    console.log('\n⚡ Step 3: Testing candidates (this will take time)...');
    console.log(`   Total tests: ${candidates.length} algos × ${CONFIG.coreTrade.tpPerc.length * CONFIG.coreTrade.slPerc.length} schemes`);
    const results = await testCandidates(rawData, candidates);
    console.log(`✓ Found ${results.length} profitable patterns (PF > 1.0)`);
    
    // Step 4: Rank and output
    console.log('\n📈 Step 4: Ranking and saving results...');
    const ranked = rankResults(results);
    const topResults = ranked.slice(0, CONFIG.outputTop);
    
    // Generate output
    const output = generateOutput(topResults, startTime);
    
    // Save to file
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
    const filename = `brute1_${timestamp}.json`;
    const outputDir = path.join(__dirname, 'brute');
    
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
    
    fs.writeFileSync(
      path.join(outputDir, filename),
      JSON.stringify(output, null, 2)
    );
    
    // Console summary
    console.log('\n' + '='.repeat(70));
    console.log('✅ BRUTE1 COMPLETE');
    console.log('='.repeat(70));
    console.log(output.summary.overview);
    console.log('\n🏆 TOP 5 BY WIN RATE:');
    output.summary.topByWinRate.forEach((algo, i) => {
      console.log(`${i + 1}. ${algo.algo} | WR ${algo.wr}% | PF ${algo.pf} | Trades ${algo.trades}`);
    });
    console.log('\n💰 TOP 5 BY PROFIT FACTOR:');
    output.summary.topByPF.forEach((algo, i) => {
      console.log(`${i + 1}. ${algo.algo} | PF ${algo.pf} | WR ${algo.wr}% | Trades ${algo.trades}`);
    });
    console.log(`\n💾 Full results: brute/${filename}`);
    console.log(`⏱️  Runtime: ${((Date.now() - startTime) / 1000 / 60).toFixed(1)} minutes`);
    console.log('='.repeat(70));
    
  } catch (error) {
    console.error('❌ BRUTE1 failed:', error.message);
    console.error(error.stack);
    await dbManager.logError('brute1', 'SCAN', 'EXEC_FAIL', error.message);
  } finally {
    await dbManager.close();
  }
})();

// ============================================================================
// DATA FETCHING
// ============================================================================
async function fetchSampleData() {
  // Determine symbol list
  let symbols = [];
  if (CONFIG.targetSymbols.useAll) {
    const { rows } = await dbManager.pool.query(
      'SELECT DISTINCT symbol FROM perp_metrics WHERE symbol IS NOT NULL ORDER BY symbol'
    );
    symbols = rows.map(r => r.symbol);
  } else {
    symbols = CONFIG.targetSymbols.list;
  }
  
  if (symbols.length === 0) {
    throw new Error('No symbols found in perp_metrics');
  }
  
  console.log(`   Targeting ${symbols.length} symbols: ${symbols.slice(0, 10).join(', ')}${symbols.length > 10 ? '...' : ''}`);
  
  const symbolClause = `symbol IN (${symbols.map(s => `'${s}'`).join(',')})`;
  
  // Fetch all params needed
  const paramList = ['ts', 'symbol', 'exchange', 'c', 'o', 'h', 'l', ...CONFIG.params];
  
  const query = `
    SELECT ${paramList.join(', ')}
    FROM perp_metrics
    WHERE ${symbolClause}
      AND c IS NOT NULL
      AND c > 0
    ORDER BY ts DESC
    LIMIT ${CONFIG.maxSamples};
  `;
  
  const { rows } = await dbManager.pool.query(query);
  
  // Parse data
  return rows.map(row => {
    const converted = {
      ts: Number(row.ts),
      symbol: row.symbol,
      exchange: row.exchange,
      c: parseFloat(row.c || 0),
      o: parseFloat(row.o || 0),
      h: parseFloat(row.h || 0),
      l: parseFloat(row.l || 0)
    };
    
    // Parse all param fields
    CONFIG.params.forEach(p => {
      converted[p] = parseFloat(row[p] || 0);
    });
    
    return converted;
  });
}

// ============================================================================
// CANDIDATE GENERATION
// ============================================================================
function generateCandidates() {
  const candidates = [];
  
  // For each direction (mBuy = Long, mSell = Short)
  for (const direction of ['Long', 'Short']) {
    const operator = direction === 'Long' ? '>' : '<';
    
    // For each exchange
    for (const exchange of CONFIG.exchanges) {
      
      // For each param
      for (const param of CONFIG.params) {
        
        // For each core% threshold (positive and negative)
        for (const coreVal of CONFIG.corePercent) {
          // Positive threshold
          candidates.push({
            direction,
            exchange,
            param,
            operator,
            threshold: coreVal,
            algoString: `${exchange}_${param}${operator}${coreVal}`
          });
          
          // Negative threshold (flip operator)
          const negOperator = operator === '>' ? '<' : '>';
          candidates.push({
            direction,
            exchange,
            param,
            operator: negOperator,
            threshold: -coreVal,
            algoString: `${exchange}_${param}${negOperator}${-coreVal}`
          });
        }
      }
    }
  }
  
  return candidates;
}

// ============================================================================
// TESTING ENGINE
// ============================================================================
async function testCandidates(data, candidates) {
  const results = [];
  const totalTests = candidates.length;
  let tested = 0;
  const progressInterval = Math.max(1, Math.floor(totalTests / 20));
  
  for (const candidate of candidates) {
    tested++;
    if (tested % progressInterval === 0) {
      console.log(`   Progress: ${tested}/${totalTests} (${((tested / totalTests) * 100).toFixed(0)}%) - ${results.length} profitable found`);
    }
    
    // Filter data by condition
    const filtered = filterData(data, candidate);
    
    // Skip if too few or too many matches
    if (filtered.length < CONFIG.minTrades * 2 || filtered.length > CONFIG.maxTrades * 2) {
      continue;
    }
    
    // Test all TP/SL schemes
    let bestScheme = null;
    let bestScore = -Infinity;
    
    for (const tp of CONFIG.coreTrade.tpPerc) {
      for (const sl of CONFIG.coreTrade.slPerc) {
        const trades = simulateTrades(filtered, tp, sl, candidate.direction);
        
        // Apply trade limits
        if (trades.length < CONFIG.minTrades || trades.length > CONFIG.maxTrades) {
          continue;
        }
        
        const stats = calculateStats(trades);
        
        // Only keep profitable
        if (parseFloat(stats.pf) <= 1.0) continue;
        
        // Scoring: Balanced between WR, PF, and sample size
        // Formula: (WR/100 * 30) + (PF * 40) + (min(trades/200, 1) * 30)
        const wrScore = (parseFloat(stats.winRate) / 100) * 30;
        const pfScore = parseFloat(stats.pf) * 40;
        const tradeScore = Math.min(trades.length / CONFIG.maxTrades, 1) * 30;
        const score = wrScore + pfScore + tradeScore;
        
        if (score > bestScore) {
          bestScore = score;
          bestScheme = {
            ...candidate,
            tp,
            sl,
            ...stats,
            score: parseFloat(score.toFixed(2)),
            trades: trades.length
          };
        }
      }
    }
    
    if (bestScheme) {
      results.push(bestScheme);
    }
  }
  
  return results;
}

// Filter data by single condition
function filterData(data, candidate) {
  return data.filter(row => {
    // Match exchange
    if (row.exchange !== candidate.exchange) return false;
    
    // Check threshold
    const val = row[candidate.param];
    if (val === null || val === undefined || isNaN(val)) return false;
    
    switch (candidate.operator) {
      case '>': return val > candidate.threshold;
      case '<': return val < candidate.threshold;
      case '>=': return val >= candidate.threshold;
      case '<=': return val <= candidate.threshold;
      default: return false;
    }
  });
}

// Simulate trades
function simulateTrades(data, tp, sl, direction) {
  const tpDecimal = tp / 100;
  const slDecimal = sl / 100;
  const trades = [];
  
  // Sort by timestamp
  const sorted = [...data].sort((a, b) => a.ts - b.ts);
  
  let inTrade = false;
  let entryPrice = 0;
  let entryTs = 0;
  
  for (const row of sorted) {
    const price = row.c;
    const ts = row.ts;
    
    if (!inTrade) {
      entryPrice = price;
      entryTs = ts;
      inTrade = true;
      continue;
    }
    
    // Check time window
    const elapsedMin = (ts - entryTs) / 60000;
    if (elapsedMin > CONFIG.coreTrade.posTime) {
      // Timeout - close at market
      const pctChange = direction === 'Long' 
        ? (price - entryPrice) / entryPrice 
        : (entryPrice - price) / entryPrice;
      trades.push({
        pnl: pctChange * CONFIG.coreTrade.posVal,
        exitType: 'TIMEOUT'
      });
      inTrade = false;
      continue;
    }
    
    // Check TP/SL
    const pctChange = direction === 'Long' 
      ? (price - entryPrice) / entryPrice 
      : (entryPrice - price) / entryPrice;
    
    if (pctChange >= tpDecimal) {
      trades.push({
        pnl: tpDecimal * CONFIG.coreTrade.posVal,
        exitType: 'TP'
      });
      inTrade = false;
    } else if (pctChange <= -slDecimal) {
      trades.push({
        pnl: -slDecimal * CONFIG.coreTrade.posVal,
        exitType: 'SL'
      });
      inTrade = false;
    }
  }
  
  return trades;
}

// Calculate statistics
function calculateStats(trades) {
  const total = trades.length;
  const wins = trades.filter(t => t.pnl > 0).length;
  const losses = trades.filter(t => t.pnl < 0).length;
  const tp_count = trades.filter(t => t.exitType === 'TP').length;
  
  const grossProfit = trades.filter(t => t.pnl > 0).reduce((sum, t) => sum + t.pnl, 0);
  const grossLoss = Math.abs(trades.filter(t => t.pnl < 0).reduce((sum, t) => sum + t.pnl, 0));
  const pf = grossLoss > 0 ? (grossProfit / grossLoss).toFixed(2) : '999';
  
  return {
    winRate: ((wins / total) * 100).toFixed(1),
    pf: parseFloat(pf),
    netPnl: (grossProfit - grossLoss).toFixed(2),
    avgPnl: (trades.reduce((sum, t) => sum + t.pnl, 0) / total).toFixed(2),
    wins,
    losses,
    tp_count
  };
}

// ============================================================================
// RANKING & OUTPUT
// ============================================================================
function rankResults(results) {
  // Sort by composite score
  return results.sort((a, b) => b.score - a.score);
}

function generateOutput(topResults, startTime) {
  // Generate summary
  const topByWR = [...topResults]
    .sort((a, b) => parseFloat(b.winRate) - parseFloat(a.winRate))
    .slice(0, 5)
    .map(r => ({
      algo: formatAlgo(r),
      wr: r.winRate,
      pf: r.pf,
      trades: r.trades
    }));
  
  const topByPF = [...topResults]
    .sort((a, b) => b.pf - a.pf)
    .slice(0, 5)
    .map(r => ({
      algo: formatAlgo(r),
      pf: r.pf,
      wr: r.winRate,
      trades: r.trades
    }));
  
  const runtime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  
  return {
    metadata: {
      script: 'brute1.js',
      timestamp: new Date().toISOString(),
      runtime: `${runtime} minutes`,
      config: CONFIG
    },
    summary: {
      overview: `Tested ${CONFIG.corePercent.length * 2} thresholds × ${CONFIG.params.length} params × ${CONFIG.exchanges.length} exchanges = ${topResults.length} profitable patterns found`,
      scoreFormula: 'Score = (WinRate/100 * 30) + (PF * 40) + (min(trades/maxTrades, 1) * 30)',
      topByWinRate: topByWR,
      topByPF: topByPF
    },
    results: topResults.map(r => ({
      algo: formatAlgo(r),
      direction: r.direction,
      exchange: r.exchange,
      param: r.param,
      threshold: r.threshold,
      tradeScheme: {
        tp: r.tp,
        sl: r.sl,
        posTime: CONFIG.coreTrade.posTime,
        posVal: CONFIG.coreTrade.posVal
      },
      stats: {
        trades: r.trades,
        winRate: r.winRate,
        pf: r.pf,
        netPnl: r.netPnl,
        avgPnl: r.avgPnl,
        wins: r.wins,
        losses: r.losses,
        tp_count: r.tp_count
      },
      score: r.score
    }))
  };
}

function formatAlgo(result) {
  const symbols = CONFIG.targetSymbols.useAll ? 'ALL' : CONFIG.targetSymbols.list.join(',');
  return `${symbols}; ${result.direction}; ${result.exchange}_${result.param}${result.operator}${result.threshold}`;
}