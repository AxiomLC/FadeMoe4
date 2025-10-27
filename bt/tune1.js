/*
  bt/tune1.js - Combo Refinement Engine
  Description: Takes brute1 output and tests 2-condition combos (AND logic) to refine patterns.
               Adds secondary params (can include c/v here) to proven base algos.
  
  Output: tune/tune1_{datetime}.json with improved combos
  Usage: node bt/tune1.js
  Date: 27 Oct 2025
*/

const fs = require('fs');
const path = require('path');
const dbManager = require('../db/dbsetup');

// ============================================================================
// USER CONFIGURATION
// ============================================================================
const CONFIG = {
  // Input file from brute1 (edit this to your brute1 output file)
  inputFile: 'brute1_2025-10-27T14-30-00.json',  // EDIT: Set your brute1 file name
  
  // How many top algos from brute1 to refine
  topAlgosToRefine: 20,
  
  // Secondary params to test as AND conditions (includes c/v for confirmation)
  secondaryParams: {
    useAll: false,  // If true, tests all params below
    list: [
      'c_chg_1m', 'c_chg_5m', 'c_chg_10m',
      'v_chg_1m', 'v_chg_5m', 'v_chg_10m',
      'oi_chg_1m', 'oi_chg_5m', 'oi_chg_10m',
      'pfr_chg_1m', 'pfr_chg_5m', 'pfr_chg_10m',
      'lsr_chg_1m', 'lsr_chg_5m', 'lsr_chg_10m'
    ]
  },
  
  // Threshold ranges for secondary params (will test these values)
  secondaryThresholds: [3, 5, 10, 20, 30, 50],
  
  // Trade settings (inherited from brute1 but can override)
  coreTrade: {
    posTime: 10,
    posVal: 1000,
    tpPerc: [1.2, 1.5, 2.0, 2.5],
    slPerc: [0.6, 0.8, 1.2, 1.8]
  },
  
  // Data limits
  maxSamples: 10000,
  minTrades: 15,
  maxTrades: 200,
  
  // Output
  outputTop: 100
};

// ============================================================================
// MAIN EXECUTION
// ============================================================================
(async () => {
  console.log('🎯 TUNE1: Combo Refinement Engine');
  console.log('=' .repeat(70));
  
  const startTime = Date.now();
  
  try {
    // Step 1: Load brute1 results
    console.log('\n📂 Step 1: Loading brute1 results...');
    const brute1Data = loadBrute1Results();
    console.log(`✓ Loaded ${brute1Data.results.length} algos from ${CONFIG.inputFile}`);
    console.log(`   Refining top ${CONFIG.topAlgosToRefine} algos`);
    
    // Step 2: Fetch data for testing
    console.log('\n📥 Step 2: Fetching sample data...');
    const rawData = await fetchSampleData(brute1Data);
    console.log(`✓ Loaded ${rawData.length} data points`);
    
    if (rawData.length < 100) {
      throw new Error('Insufficient data for refinement');
    }
    
    // Step 3: Generate combo candidates
    console.log('\n🧬 Step 3: Generating 2-param combos...');
    const combos = generateCombos(brute1Data);
    console.log(`✓ Created ${combos.length} combo candidates`);
    
    // Step 4: Test combos
    console.log('\n⚡ Step 4: Testing combos...');
    const results = await testCombos(rawData, combos);
    console.log(`✓ Found ${results.length} improved patterns`);
    
    // Step 5: Compare with base algos
    console.log('\n📊 Step 5: Comparing improvements...');
    const improvements = compareResults(results, brute1Data);
    
    // Step 6: Output
    console.log('\n💾 Step 6: Saving results...');
    const output = generateOutput(improvements, brute1Data, startTime);
    
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
    const filename = `tune1_${timestamp}.json`;
    const outputDir = path.join(__dirname, 'tune');
    
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
    
    fs.writeFileSync(
      path.join(outputDir, filename),
      JSON.stringify(output, null, 2)
    );
    
    // Console summary
    console.log('\n' + '='.repeat(70));
    console.log('✅ TUNE1 COMPLETE');
    console.log('='.repeat(70));
    console.log(output.summary.overview);
    console.log('\n🎖️ TOP 5 IMPROVEMENTS (by score gain):');
    output.summary.topImprovements.forEach((combo, i) => {
      console.log(`${i + 1}. ${combo.comboAlgo}`);
      console.log(`   Base: WR ${combo.baseWR}% PF ${combo.basePF} → Refined: WR ${combo.refinedWR}% PF ${combo.refinedPF}`);
      console.log(`   Gain: +${combo.scoreGain} score | Trades: ${combo.trades}`);
    });
    console.log(`\n💾 Full results: tune/${filename}`);
    console.log(`⏱️  Runtime: ${((Date.now() - startTime) / 1000 / 60).toFixed(1)} minutes`);
    console.log('='.repeat(70));
    
  } catch (error) {
    console.error('❌ TUNE1 failed:', error.message);
    console.error(error.stack);
    await dbManager.logError('tune1', 'REFINE', 'EXEC_FAIL', error.message);
  } finally {
    await dbManager.close();
  }
})();

// ============================================================================
// LOAD BRUTE1 RESULTS
// ============================================================================
function loadBrute1Results() {
  const filepath = path.join(__dirname, 'brute', CONFIG.inputFile);
  
  if (!fs.existsSync(filepath)) {
    throw new Error(`Input file not found: brute/${CONFIG.inputFile}`);
  }
  
  return JSON.parse(fs.readFileSync(filepath, 'utf8'));
}

// ============================================================================
// DATA FETCHING
// ============================================================================
async function fetchSampleData(brute1Data) {
  // Get symbol list from brute1 config
  const symbols = brute1Data.metadata.config.targetSymbols.useAll
    ? await getAllSymbols()
    : brute1Data.metadata.config.targetSymbols.list;
  
  const symbolClause = `symbol IN (${symbols.map(s => `'${s}'`).join(',')})`;
  
  // Get all params needed (primary + secondary)
  const allParams = [
    ...brute1Data.metadata.config.params,
    ...(CONFIG.secondaryParams.useAll 
      ? CONFIG.secondaryParams.list 
      : CONFIG.secondaryParams.list)
  ];
  const uniqueParams = [...new Set(allParams)];
  
  const paramList = ['ts', 'symbol', 'exchange', 'c', 'o', 'h', 'l', ...uniqueParams];
  
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
    
    uniqueParams.forEach(p => {
      converted[p] = parseFloat(row[p] || 0);
    });
    
    return converted;
  });
}

async function getAllSymbols() {
  const { rows } = await dbManager.pool.query(
    'SELECT DISTINCT symbol FROM perp_metrics WHERE symbol IS NOT NULL ORDER BY symbol'
  );
  return rows.map(r => r.symbol);
}

// ============================================================================
// COMBO GENERATION
// ============================================================================
function generateCombos(brute1Data) {
  const combos = [];
  const topAlgos = brute1Data.results.slice(0, CONFIG.topAlgosToRefine);
  
  const secondaryParamList = CONFIG.secondaryParams.useAll
    ? CONFIG.secondaryParams.list
    : CONFIG.secondaryParams.list;
  
  // For each top algo from brute1
  for (const baseAlgo of topAlgos) {
    // For each secondary param
    for (const secParam of secondaryParamList) {
      // Skip if same param as base
      if (secParam === baseAlgo.param) continue;
      
      // For each threshold (positive and negative)
      for (const threshold of CONFIG.secondaryThresholds) {
        // Same direction as base (Long uses >, Short uses <)
        const operator = baseAlgo.direction === 'Long' ? '>' : '<';
        
        combos.push({
          baseAlgo: {
            exchange: baseAlgo.exchange,
            param: baseAlgo.param,
            operator: baseAlgo.operator,
            threshold: baseAlgo.threshold,
            direction: baseAlgo.direction
          },
          secondaryCondition: {
            exchange: baseAlgo.exchange,  // Use same exchange
            param: secParam,
            operator: operator,
            threshold: threshold
          },
          direction: baseAlgo.direction,
          basePF: baseAlgo.stats.pf,
          baseWR: baseAlgo.stats.winRate,
          baseScore: baseAlgo.score
        });
        
        // Also test negative threshold
        const negOperator = operator === '>' ? '<' : '>';
        combos.push({
          baseAlgo: {
            exchange: baseAlgo.exchange,
            param: baseAlgo.param,
            operator: baseAlgo.operator,
            threshold: baseAlgo.threshold,
            direction: baseAlgo.direction
          },
          secondaryCondition: {
            exchange: baseAlgo.exchange,
            param: secParam,
            operator: negOperator,
            threshold: -threshold
          },
          direction: baseAlgo.direction,
          basePF: baseAlgo.stats.pf,
          baseWR: baseAlgo.stats.winRate,
          baseScore: baseAlgo.score
        });
      }
    }
  }
  
  return combos;
}

// ============================================================================
// TESTING ENGINE
// ============================================================================
async function testCombos(data, combos) {
  const results = [];
  const totalTests = combos.length;
  let tested = 0;
  const progressInterval = Math.max(1, Math.floor(totalTests / 20));
  
  for (const combo of combos) {
    tested++;
    if (tested % progressInterval === 0) {
      console.log(`   Progress: ${tested}/${totalTests} (${((tested / totalTests) * 100).toFixed(0)}%) - ${results.length} improvements found`);
    }
    
    // Filter by both conditions (AND logic)
    const filtered = filterByCombo(data, combo);
    
    // Skip if too few/many matches
    if (filtered.length < CONFIG.minTrades * 2 || filtered.length > CONFIG.maxTrades * 2) {
      continue;
    }
    
    // Test all TP/SL schemes
    let bestScheme = null;
    let bestScore = -Infinity;
    
    for (const tp of CONFIG.coreTrade.tpPerc) {
      for (const sl of CONFIG.coreTrade.slPerc) {
        const trades = simulateTrades(filtered, tp, sl, combo.direction);
        
        if (trades.length < CONFIG.minTrades || trades.length > CONFIG.maxTrades) {
          continue;
        }
        
        const stats = calculateStats(trades);
        
        // Only keep if better than base
        if (parseFloat(stats.pf) <= combo.basePF && parseFloat(stats.winRate) <= parseFloat(combo.baseWR)) {
          continue;
        }
        
        // Score calculation
        const wrScore = (parseFloat(stats.winRate) / 100) * 30;
        const pfScore = parseFloat(stats.pf) * 40;
        const tradeScore = Math.min(trades.length / CONFIG.maxTrades, 1) * 30;
        const score = wrScore + pfScore + tradeScore;
        
        if (score > bestScore) {
          bestScore = score;
          bestScheme = {
            ...combo,
            tp,
            sl,
            ...stats,
            score: parseFloat(score.toFixed(2)),
            trades: trades.length,
            scoreGain: parseFloat((score - combo.baseScore).toFixed(2))
          };
        }
      }
    }
    
    if (bestScheme && bestScheme.scoreGain > 0) {
      results.push(bestScheme);
    }
  }
  
  return results;
}

// Filter by 2-condition combo
function filterByCombo(data, combo) {
  return data.filter(row => {
    // Match exchange
    if (row.exchange !== combo.baseAlgo.exchange) return false;
    
    // Check base condition
    const baseVal = row[combo.baseAlgo.param];
    if (baseVal === null || isNaN(baseVal)) return false;
    
    let basePass = false;
    switch (combo.baseAlgo.operator) {
      case '>': basePass = baseVal > combo.baseAlgo.threshold; break;
      case '<': basePass = baseVal < combo.baseAlgo.threshold; break;
      case '>=': basePass = baseVal >= combo.baseAlgo.threshold; break;
      case '<=': basePass = baseVal <= combo.baseAlgo.threshold; break;
    }
    
    if (!basePass) return false;
    
    // Check secondary condition
    const secVal = row[combo.secondaryCondition.param];
    if (secVal === null || isNaN(secVal)) return false;
    
    switch (combo.secondaryCondition.operator) {
      case '>': return secVal > combo.secondaryCondition.threshold;
      case '<': return secVal < combo.secondaryCondition.threshold;
      case '>=': return secVal >= combo.secondaryCondition.threshold;
      case '<=': return secVal <= combo.secondaryCondition.threshold;
      default: return false;
    }
  });
}

// Simulate trades (same as brute1)
function simulateTrades(data, tp, sl, direction) {
  const tpDecimal = tp / 100;
  const slDecimal = sl / 100;
  const trades = [];
  
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
    
    const elapsedMin = (ts - entryTs) / 60000;
    if (elapsedMin > CONFIG.coreTrade.posTime) {
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

// Calculate stats (same as brute1)
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
// COMPARISON & OUTPUT
// ============================================================================
function compareResults(results, brute1Data) {
  // Sort by score gain (improvement over base)
  return results.sort((a, b) => b.scoreGain - a.scoreGain);
}

function generateOutput(improvements, brute1Data, startTime) {
  const topImprovements = improvements.slice(0, 5).map(r => ({
    comboAlgo: formatComboAlgo(r),
    baseWR: r.baseWR,
    basePF: r.basePF,
    refinedWR: r.winRate,
    refinedPF: r.pf,
    scoreGain: r.scoreGain,
    trades: r.trades
  }));
  
  const runtime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  
  return {
    metadata: {
      script: 'tune1.js',
      timestamp: new Date().toISOString(),
      runtime: `${runtime} minutes`,
      inputFile: CONFIG.inputFile,
      config: CONFIG
    },
    summary: {
      overview: `Refined ${CONFIG.topAlgosToRefine} base algos with ${CONFIG.secondaryParams.list.length} secondary params. Found ${improvements.length} improvements.`,
      topImprovements
    },
    results: improvements.slice(0, CONFIG.outputTop).map(r => ({
      comboAlgo: formatComboAlgo(r),
      baseAlgo: formatBaseAlgo(r),
      secondaryCondition: formatSecondary(r),
      direction: r.direction,
      tradeScheme: {
        tp: r.tp,
        sl: r.sl,
        posTime: CONFIG.coreTrade.posTime,
        posVal: CONFIG.coreTrade.posVal
      },
      baseStats: {
        winRate: r.baseWR,
        pf: r.basePF,
        score: r.baseScore
      },
      refinedStats: {
        trades: r.trades,
        winRate: r.winRate,
        pf: r.pf,
        netPnl: r.netPnl,
        avgPnl: r.avgPnl,
        wins: r.wins,
        losses: r.losses,
        tp_count: r.tp_count
      },
      improvement: {
        scoreGain: r.scoreGain,
        wrGain: (parseFloat(r.winRate) - parseFloat(r.baseWR)).toFixed(1),
        pfGain: (r.pf - r.basePF).toFixed(2)
      },
      score: r.score
    }))
  };
}

function formatComboAlgo(result) {
  const base = `${result.baseAlgo.exchange}_${result.baseAlgo.param}${result.baseAlgo.operator}${result.baseAlgo.threshold}`;
  const sec = `${result.secondaryCondition.exchange}_${result.secondaryCondition.param}${result.secondaryCondition.operator}${result.secondaryCondition.threshold}`;
  return `${result.direction}; ${base} AND ${sec}`;
}

function formatBaseAlgo(result) {
  return `${result.baseAlgo.exchange}_${result.baseAlgo.param}${result.baseAlgo.operator}${result.baseAlgo.threshold}`;
}

function formatSecondary(result) {
  return `${result.secondaryCondition.exchange}_${result.secondaryCondition.param}${result.secondaryCondition.operator}${result.secondaryCondition.threshold}`;
}