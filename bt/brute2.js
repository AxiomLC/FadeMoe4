/*
  bt/brute1.js - Core Pattern Discovery (Brute Force Single-Param Scanner)
  Description: Tests single-parameter conditions across exchanges/timeframes to find
               profitable base patterns. Outputs top 100 results with best TP/SL schemes.
               EXCLUDES c/v params (reserved for tune1.js refinement).
  
  Output: brute/brute1_MM-DD_HH-MMutc.json with summary stats and ranked algos
  Usage: node bt/brute1.js
  Date: 27 Oct 2025
*/

const fs = require('fs');
const path = require('path');
const dbManager = require('../db/dbsetup');
const pLimit = require('p-limit');

// ============================================================================
// USER CONFIGURATION
// ============================================================================
const CONFIG = {
  corePercent: [1.5, 2, 3, 5, 10, 30, 60, 100,],
  detection: {
    algoWindow: 30, // not effect speed
    minTrades: 50, // increase could speed script
    maxTrades: 500 // increase slows speed
  },
  tradeExecution: {
    tradeWindow: 10, // smaller speeds script
    posVal: 1000,
    tpPerc: [1.2, 1.5, 2.0, 2.5, 3,],
    slPerc: [0.2, 0.4, 0.6, 1, 1.2,]
  },
  targetSymbols: {
    useAll: true,
    list: ['BTC', 'ETH', 'SOL', 'DOGE', 'XRP']
  },
  maxSamples: 8000, // default 10000, increase samples decrease speed
  params: [
    /*'oi_chg_1m', 'oi_chg_5m', 'oi_chg_10m',*/
    'pfr_chg_1m', 'pfr_chg_5m', 'pfr_chg_10m',
    'lsr_chg_1m', 'lsr_chg_5m', 'lsr_chg_10m',
    'rsi1_chg_1m', 'rsi1_chg_5m', 'rsi1_chg_10m',
    'rsi60_chg_1m', 'rsi60_chg_5m', 'rsi60_chg_10m',
    'tbv_chg_1m', 'tbv_chg_5m', 'tbv_chg_10m',
    'tsv_chg_1m', 'tsv_chg_5m', 'tsv_chg_10m'
  ],
  exchanges: ['bin', /*'byb', 'okx'*/],
  outputTop: 100,
  concurrencyLimit: 40
};

// ============================================================================
// MAIN EXECUTION
// ============================================================================
(async () => {
  console.log('Script started!');
  process.on('unhandledRejection', (reason) => {
    console.error('Unhandled Rejection:', reason.message || reason);
  });

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
    const output = generateOutput(topResults, startTime, candidates.length);

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
    console.log('\n🏆 TOP 20 BY PROFIT FACTOR:');
    if (output.summary.topByProfitFactor && output.summary.topByProfitFactor.length > 0) {
    output.summary.topByProfitFactor.forEach((algo, i) => {
        console.log(`${i + 1}. ${algo.algo} | TP ${algo.tp}% | SL ${algo.sl}% | PF ${algo.pf} | WR ${algo.wr}% | Trades ${algo.trades}`);
    });
    } else {
    console.log('No profitable patterns found to display.');
    }
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
          candidates.push({
            direction,
            exchange,
            param,
            operator: '>',
            threshold: coreVal,
            algoString: `${exchange}_${param}>${coreVal}`
          });
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
  let tested = 0;
  const progressInterval = Math.max(1, Math.floor(totalTests / 20));
  const limit = pLimit(CONFIG.concurrencyLimit);

  const tasks = candidates.map(candidate => limit(async () => {
    tested++;
    if (tested % progressInterval === 0) {
      console.log(`   Progress: ${tested}/${totalTests} (${((tested / totalTests) * 100).toFixed(0)}%) - ${results.length} profitable found`);
    }

    const filtered = filterData(data, candidate);

    if (filtered.length < CONFIG.detection.minTrades * 2 || filtered.length > CONFIG.detection.maxTrades * 2) {
      return null;
    }

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
    return bestScheme;
  }));

  await Promise.all(tasks);
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

// Simulate trades with batch fetching
async function simulateTrades(entries, tp, sl, direction, exchange) {
  const tpDecimal = tp / 100;
  const slDecimal = sl / 100;
  const trades = [];

  const entriesBySymbol = entries.reduce((acc, entry) => {
    if (!acc[entry.symbol]) acc[entry.symbol] = [];
    acc[entry.symbol].push(entry);
    return acc;
  }, {});

  for (const symbol of Object.keys(entriesBySymbol)) {
    const symbolEntries = entriesBySymbol[symbol];
    const minTs = Math.min(...symbolEntries.map(e => e.ts));
    const maxTs = Math.max(...symbolEntries.map(e => e.ts)) + CONFIG.tradeExecution.tradeWindow * 60000;

    const query = `
      SELECT ts, c
      FROM perp_metrics
      WHERE symbol = $1
        AND exchange = $2
        AND ts >= $3
        AND ts <= $4
      ORDER BY ts
    `;
    const { rows } = await dbManager.pool.query(query, [symbol, exchange, minTs, maxTs]);
    const priceData = rows.map(row => ({
      ts: Number(row.ts),
      c: parseFloat(row.c || 0)
    }));

    for (const entry of symbolEntries) {
      const { ts: entryTs, c: entryPrice } = entry;
      // Find next 1-minute ts for entry
      const nextTs = entryTs + 60000;
      const tradePrices = priceData.filter(p => p.ts >= nextTs && p.ts <= entryTs + CONFIG.tradeExecution.tradeWindow * 60000);

      let exited = false;
      for (const priceRow of tradePrices) {
        const price = priceRow.c;
        const ts = priceRow.ts;

        const pctChange = direction === 'Long'
          ? (price - entryPrice) / entryPrice
          : (entryPrice - price) / entryPrice;

        if (pctChange >= tpDecimal) {
          trades.push({
            symbol,
            entryTs: nextTs,
            exitTs: ts,
            pnl: tpDecimal * CONFIG.tradeExecution.posVal,
            exitType: 'TP'
          });
          exited = true;
          break;
        } else if (pctChange <= -slDecimal) {
          trades.push({
            symbol,
            entryTs: nextTs,
            exitTs: ts,
            pnl: -slDecimal * CONFIG.tradeExecution.posVal,
            exitType: 'SL'
          });
          exited = true;
          break;
        }
      }

      if (!exited && tradePrices.length > 0) {
        const lastPrice = tradePrices[tradePrices.length - 1];
        const price = lastPrice.c;
        const pctChange = direction === 'Long'
          ? (price - entryPrice) / entryPrice
          : (entryPrice - price) / entryPrice;
        trades.push({
          symbol,
          entryTs: nextTs,
          exitTs: lastPrice.ts,
          pnl: pctChange * CONFIG.tradeExecution.posVal,
          exitType: 'TIMEOUT'
        });
      }
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

function generateOutput(topResults, startTime, totalCandidates) {
  const topByPF = [...topResults]
    .sort((a, b) => parseFloat(b.pf) - parseFloat(a.pf))
    .slice(0, 20)
    .map(r => ({
      algo: formatAlgo(r),
      tp: r.tp,
      sl: r.sl,
      pf: r.pf,
      wr: r.winRate,
      trades: r.trades
    }));

  const runtime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  const totalTests = totalCandidates * CONFIG.tradeExecution.tpPerc.length * CONFIG.tradeExecution.slPerc.length;

  return {
    metadata: {
      script: 'brute2.js',
      timestamp: new Date().toISOString(),
      runtime: `${runtime} minutes`,
      config: CONFIG
    },
    summary: {
      overview: `Tested ${CONFIG.corePercent.length * 2} thresholds × ${CONFIG.params.length} params × ${CONFIG.exchanges.length} exchanges × 2 directions = ${totalTests} total tests, ${topResults.length} profitable patterns found (PF > 1.0)`,
      scoreFormula: 'Score = (WinRate/100 * 30) + (PF * 40) + (min(trades/maxTrades, 1) * 30)',
      topByProfitFactor: topByPF
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
        losses: r.loses, // Note: Fix typo 'loses' to 'losses'
        tp_count: r.tp_count
      },
      score: r.score
    }))
  };
}
//=================================================
function formatAlgo(result) {
  const symbols = CONFIG.targetSymbols.useAll ? 'ALL' : CONFIG.targetSymbols.list.join(',');
  return `${symbols}; ${result.direction}; ${result.exchange}_${result.param}${result.operator}${result.threshold}`;
}