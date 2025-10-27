/*
  bt/brute1.js - Core Pattern Discovery (Brute Force Single-Param Scanner)
  Description: Tests single-parameter conditions across exchanges/timeframes to find
               profitable base patterns. Outputs top 100 results with best TP/SL schemes.
               EXCLUDES c/v params (reserved for tune1.js refinement).
  
  Output: brute/brute1_MM-DD_HH-MMutc.json with summary stats and ranked algos
  Usage: node bt/brute1.js
  Date: 27 Oct 2025
*/

console.log('Script started!');
(async () => {
  console.log('🔥 BRUTE1: Core Pattern Discovery Scanner');
  })();

  process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection:', reason.message || reason);
});
  // ... rest of the code

const fs = require('fs');
const path = require('path');
const dbManager = require('../db/dbsetup');

// ============================================================================
// USER CONFIGURATION
// ============================================================================
const CONFIG = {
  // Core threshold percentages to test (positive AND negative)
  corePercent: [3, 5, 10, 30, 50, 80, 100, 150, 200, 300, 500, 1000],
  
  // Pattern detection and trade execution
  detection: {
    algoWindow: 30,           // Minutes to look back for signal
    minTrades: 15,            // Minimum trades for valid pattern
    maxTrades: 200            // Maximum trades (filters noise)
  },
  tradeExecution: {
    tradeWindow: 10,          // Max minutes to hold trade
    posVal: 1000,             // USD value per trade
    tpPerc: [1.2, 1.5, 2.0, 2.5],  // Take profit % options
    slPerc: [0.6, 0.8, 1.2, 1.8]   // Stop loss % options
  },
  
  // Symbol selection
  targetSymbols: {
    useAll: true,
    list: ['BTC', 'ETH', 'SOL', 'DOGE', 'XRP']
  },
  
  // Data sampling limits
  maxSamples: 10000,
  
  // Parameters to test (EXCLUDES c/v)
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
  outputTop: 100
};

// ============================================================================
// MAIN EXECUTION
// ============================================================================
(async () => {
  console.log('🔥 BRUTE1: Core Pattern Discovery Scanner');
  console.log('='.repeat(70));
  console.log(`📊 Config:`);
  console.log(`   - Core%: ±${CONFIG.corePercent.join(', ')}`);
  console.log(`   - Params: ${CONFIG.params.length} (no c/v)`);
  console.log(`   - Exchanges: ${CONFIG.exchanges.join(', ')}`);
  console.log(`   - TP schemes: ${CONFIG.tradeExecution.tpPerc.length} × SL: ${CONFIG.tradeExecution.slPerc.length}`);
  console.log(`   - Algo window: ${CONFIG.detection.algoWindow}min, Trade window: ${CONFIG.tradeExecution.tradeWindow}min`);
  console.log(`   - Trade limits: ${CONFIG.detection.minTrades}-${CONFIG.detection.maxTrades} trades`);
  console.log('='.repeat(70));
  
  const startTime = Date.now();
  
  try {
    // Step 1: Fetch sample data
    console.log('\n📥 Step 1: Fetching sample data...');
    const rawData = await fetchSampleData();
    console.log(`✓ Loaded ${rawData.length} data points`);
    
    if (rawData.length < 100) {
      throw new Error('Insufficient data. Need at least 100 rows in perp_metrics.');
    }
    
    // Step 2: Generate candidate algos
    console.log('\n🧬 Step 2: Generating test candidates...');
    const candidates = generateCandidates();
    console.log(`✓ Created ${candidates.length} single-param candidates`);
    
    // Step 3: Test candidates
    console.log('\n⚡ Step 3: Testing candidates...');
    console.log(`   Total tests: ${candidates.length} algos × ${CONFIG.tradeExecution.tpPerc.length * CONFIG.tradeExecution.slPerc.length} schemes`);
    const results = await testCandidates(rawData, candidates);
    console.log(`✓ Found ${results.length} profitable patterns (PF > 1.0)`);
    
    // Step 4: Rank and output
    console.log('\n📈 Step 4: Ranking and saving results...');
    const ranked = rankResults(results);
    const topResults = ranked.slice(0, CONFIG.outputTop);
    
    // Generate output
    const output = generateOutput(topResults, startTime);
    
    // Save to file
    const timestamp = new Date().toISOString().slice(5, 16).replace('T', '_').replace(':', '-') + 'utc';
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
      console.log(`${i + 1}. ${algo.algo} | TP ${algo.tp}% | SL ${algo.sl}% | WR ${algo.wr}% | PF ${algo.pf} | Trades ${algo.trades}`);
    });
    console.log('\n💰 TOP 5 BY PROFIT FACTOR:');
    output.summary.topByPF.forEach((algo, i) => {
      console.log(`${i + 1}. ${algo.algo} | TP ${algo.tp}% | SL ${algo.sl}% | PF ${algo.pf} | WR ${algo.wr}% | Trades ${algo.trades}`);
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
  const paramList = ['ts', 'symbol', 'exchange', 'c', ...CONFIG.params];
  
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
  
  return rows.map(row => {
    const converted = {
      ts: Number(row.ts),
      symbol: row.symbol,
      exchange: row.exchange,
      c: parseFloat(row.c || 0)
    };
    
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
  
  for (const direction of ['Long', 'Short']) {
    for (const exchange of CONFIG.exchanges) {
      for (const param of CONFIG.params) {
        for (const coreVal of CONFIG.corePercent) {
          // Positive threshold
          candidates.push({
            direction,
            exchange,
            param,
            operator: '>',
            threshold: coreVal,
            algoString: `${exchange}_${param}>${coreVal}`
          });
          
          // Negative threshold
          candidates.push({
            direction,
            exchange,
            param,
            operator: '<',
            threshold: -coreVal,
            algoString: `${exchange}_${param}<${-coreVal}`
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
  const progressInterval = Math.max(1, Math.floor(totalTests / 20));
  
  for (const candidate of candidates) {
    if (totalTests % progressInterval === 0) {
      console.log(`   Progress: ${totalTests}/${totalTests} (${((totalTests / totalTests) * 100).toFixed(0)}%) - ${results.length} profitable found`);
    }
    
    // Filter data by condition and symbol/exchange
    const filtered = filterData(data, candidate);
    
    if (filtered.length < CONFIG.detection.minTrades * 2 || filtered.length > CONFIG.detection.maxTrades * 2) {
      continue;
    }
    
    // Test all TP/SL schemes
    let bestScheme = null;
    let bestScore = -Infinity;
    
    for (const tp of CONFIG.tradeExecution.tpPerc) {
      for (const sl of CONFIG.tradeExecution.slPerc) {
        const trades = await simulateTrades(filtered, tp, sl, candidate.direction, candidate.exchange);
        
        if (trades.length < CONFIG.detection.minTrades || trades.length > CONFIG.detection.maxTrades) {
          continue;
        }
        
        const stats = calculateStats(trades);
        
        if (parseFloat(stats.pf) <= 1.0) continue;
        
        const wrScore = (parseFloat(stats.winRate) / 100) * 30;
        const pfScore = parseFloat(stats.pf) * 40;
        const tradeScore = Math.min(trades.length / CONFIG.detection.maxTrades, 1) * 30;
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

// Filter data by condition
function filterData(data, candidate) {
  return data.filter(row => {
    if (row.exchange !== candidate.exchange) return false;
    
    const val = row[candidate.param];
    if (val === null || val === undefined || isNaN(val)) return false;
    
    switch (candidate.operator) {
      case '>': return val > candidate.threshold;
      case '<': return val < candidate.threshold;
      default: return false;
    }
  });
}

// Simulate trades
async function simulateTrades(entries, tp, sl, direction, exchange) {
  const tpDecimal = tp / 100;
  const slDecimal = sl / 100;
  const trades = [];
  
  for (const entry of entries) {
    const { ts: entryTs, symbol, c: entryPrice } = entry;
    
    // Fetch price data for this symbol/exchange within trade window
    const query = `
      SELECT ts, c
      FROM perp_metrics
      WHERE symbol = $1
        AND exchange = $2
        AND ts > $3
        AND ts <= $4
      ORDER BY ts
    `;
    const { rows } = await dbManager.pool.query(query, [
      symbol,
      exchange,
      entryTs,
      entryTs + CONFIG.tradeExecution.tradeWindow * 60000
    ]);
    
    let exited = false;
    for (const row of rows) {
      const price = parseFloat(row.c || 0);
      const ts = Number(row.ts);
      
      // Check TP/SL
      const pctChange = direction === 'Long' 
        ? (price - entryPrice) / entryPrice 
        : (entryPrice - price) / entryPrice;
      
      if (pctChange >= tpDecimal) {
        trades.push({
          symbol,
          entryTs,
          exitTs: ts,
          pnl: tpDecimal * CONFIG.tradeExecution.posVal,
          exitType: 'TP'
        });
        exited = true;
        break;
      } else if (pctChange <= -slDecimal) {
        trades.push({
          symbol,
          entryTs,
          exitTs: ts,
          pnl: -slDecimal * CONFIG.tradeExecution.posVal,
          exitType: 'SL'
        });
        exited = true;
        break;
      }
    }
    
    // Timeout if no TP/SL hit
    if (!exited && rows.length > 0) {
      const lastRow = rows[rows.length - 1];
      const price = parseFloat(lastRow.c || 0);
      const pctChange = direction === 'Long' 
        ? (price - entryPrice) / entryPrice 
        : (entryPrice - price) / entryPrice;
      trades.push({
        symbol,
        entryTs,
        exitTs: lastRow.ts,
        pnl: pctChange * CONFIG.tradeExecution.posVal,
        exitType: 'TIMEOUT'
      });
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
  return results.sort((a, b) => b.score - a.score);
}

function generateOutput(topResults, startTime) {
  const topByWR = [...topResults]
    .sort((a, b) => parseFloat(b.winRate) - parseFloat(a.winRate))
    .slice(0, 5)
    .map(r => ({
      algo: formatAlgo(r),
      tp: r.tp,
      sl: r.sl,
      wr: r.winRate,
      pf: r.pf,
      trades: r.trades
    }));
  
  const topByPF = [...topResults]
    .sort((a, b) => b.pf - a.pf)
    .slice(0, 5)
    .map(r => ({
      algo: formatAlgo(r),
      tp: r.tp,
      sl: r.sl,
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
      overview: `Tested ${CONFIG.corePercent.length * 2} thresholds × ${CONFIG.params.length} params × ${CONFIG.exchanges.length} exchanges × 2 directions = ${topResults.length} profitable patterns found`,
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
        algoWindow: CONFIG.detection.algoWindow,
        tradeWindow: CONFIG.tradeExecution.tradeWindow,
        posVal: CONFIG.tradeExecution.posVal
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