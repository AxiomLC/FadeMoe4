/* 21 NOV '25  bt/brute15.js - Core Pattern Discovery (Brute Force Single-Param Scanner) - v15
  Description: Tests single-parameter conditions across exchanges/timeframes to find profitable patterns
  Output: bt/brute/brute_MM-DD_HH-MMutc.json with summary stats and ranked algos */

const fs = require('fs');
const path = require('path');
const dbManager = require('../db/dbsetup');
const pLimit = require('p-limit');

// ============================================================================
// USER CONFIGURATION
// ============================================================================
const CONFIG = {
  // Core percent thresholds to test (positive and negative)
  corePercent: [.5, 1.5, 2.5, 5, 10, 30, 60, 100],

  // Detection settings
  detection: {
    minTrades: 100,  // Minimum trades to consider algo valid
    maxTrades: 1500, // Maximum trades to consider algo valid
    minPF: 3       // Minimum profit factor threshold
  },

  // Trade execution settings
  tradeExecution: {
    tradeWindow: 30, // Trade window in minutes
    posVal: 1000,    // Position value
    tpPerc: [1, 1.5, 1.9],
    slPerc: [0.2, 0.4, 0.6]
  },

  // Target symbols
  targetSymbols: {
    useAll: true, // Use all symbols from perp_metrics (excludes MT)
    list: ['ETH', 'SOL', 'DOGE', 'XRP'] // If useAll false, use this list
  },

  // Expanded params list
  params: [
    //'c_chg_1m', 'c_chg_5m', 'c_chg_10m',
    //'v_chg_1m', 'v_chg_5m', 'v_chg_10m',
    'oi_chg_1m', 'oi_chg_5m', 'oi_chg_10m',
    'pfr_chg_1m', 'pfr_chg_5m', 'pfr_chg_10m',
    'lsr_chg_1m', 'lsr_chg_5m', 'lsr_chg_10m',
    //'rsi1_chg_1m', 'rsi1_chg_5m', 'rsi1_chg_10m',
    'rsi60_chg_1m', 'rsi60_chg_5m', 'rsi60_chg_10m',
    'tbv_chg_1m', 'tbv_chg_5m', 'tbv_chg_10m',
    'tsv_chg_1m', 'tsv_chg_5m', 'tsv_chg_10m',
    'lql_chg_1m', 'lql_chg_5m', 'lql_chg_10m',
    'lqs_chg_1m', 'lqs_chg_5m', 'lqs_chg_10m'
  ],

  // Exchanges to test
  exchanges: ['bin'],  // , 'byb', 'okx'],
  
  // Direction control: 'Long', 'Short', or 'Both'
  coreDir: 'Both',

  // Output settings
  output: {
    topResults: 20,    // Console display count
    listResults: 40,   // JSON output count
    sortByPF: true     // true = sort by PF, false = sort by NET$
  },

  // Concurrency and batching controls
  concurrencyLimit: 6,     // Concurrency limit for parallel testing
  batchSizeMinutes: 1440,  // Batch size in minutes (default 1 day)
  chunkConcurrency: 8,     // How many batches to process in parallel
  enablePriceCache: true   // Enable caching of price data
};

// ============================================================================
// MAIN EXECUTION
// ============================================================================
(async () => {
  console.log('Script started!');
  process.on('unhandledRejection', (reason) => {
    console.error('Unhandled Rejection:', reason.message || reason);
  });

  console.log('\n🔥 BRUTE: Core Pattern Discovery Scanner');
  console.log('='.repeat(70));
  console.log('📊 Config:');
  console.log(`   - Core%: ±${CONFIG.corePercent.join(', ')}|Params: ${CONFIG.params.length}`);
  console.log(`   - Exchanges: ${CONFIG.exchanges.join(', ')}|TP schemes: ${CONFIG.tradeExecution.tpPerc.length}×SL: ${CONFIG.tradeExecution.slPerc.length}|trWindow: ${CONFIG.tradeExecution.tradeWindow}min`);
  console.log(`   - Min/Max ${CONFIG.detection.minTrades}-${CONFIG.detection.maxTrades} trades|minPF: ${CONFIG.detection.minPF}|Direction: ${CONFIG.coreDir}`);
  console.log('='.repeat(70));

  const startTime = Date.now();

  try {
    // Step 1: Determine symbols to test (exclude MT)
    let symbols = [];
    if (CONFIG.targetSymbols.useAll) {
      const { rows } = await dbManager.pool.query(
        "SELECT DISTINCT symbol FROM perp_metrics WHERE symbol IS NOT NULL AND symbol != 'MT' ORDER BY symbol"
      );
      symbols = rows.map(r => r.symbol);
    } else {
      symbols = CONFIG.targetSymbols.list;
    }
    if (symbols.length === 0) throw new Error('No symbols found in perp_metrics');

    console.log(`\n   Targeting ${symbols.length} symbols: ${symbols.slice(0, 10).join(', ')}${symbols.length > 10 ? '...' : ''}`);

    // Step 2: Determine overall time range
    const { rows: rangeRows } = await dbManager.pool.query(
      `SELECT MIN(ts) AS min_ts, MAX(ts) AS max_ts FROM perp_metrics`
    );
    const minTs = Number(rangeRows[0].min_ts);
    const maxTs = Number(rangeRows[0].max_ts);
    if (!minTs || !maxTs) throw new Error('Could not determine time range from perp_metrics');

    // Step 3: Generate candidate algos
    const candidates = generateCandidates();
    console.log(`\n   Generated ${candidates.length} single-param candidates`);

    // Step 4: Split time range into batches
    const batchSizeMs = CONFIG.batchSizeMinutes * 60000;
    const batches = [];
    for (let start = minTs; start <= maxTs; start += batchSizeMs) {
      batches.push({ start, end: Math.min(start + batchSizeMs - 1, maxTs) });
    }
    console.log(`   Split time range into ${batches.length} batches of ~${CONFIG.batchSizeMinutes} minutes`);

    // Step 5: Process batches in parallel with limited concurrency
    const batchLimit = pLimit(CONFIG.chunkConcurrency);
    let totalProfitable = 0;
    const allResults = [];

    // Heartbeat timer
    let lastHeartbeat = Date.now();
    const heartbeatIntervalMs = 10000; // 10 seconds

    await Promise.all(batches.map(batch => batchLimit(async () => {
      const batchData = await fetchBatchData(symbols, CONFIG.exchanges, batch.start, batch.end);
      if (batchData.length === 0) return;

      const batchResults = await testCandidates(batchData, candidates);

      if (batchResults.length > 0) {
        allResults.push(...batchResults);
        totalProfitable += batchResults.length;
      }

      const now = Date.now();
      if (now - lastHeartbeat > heartbeatIntervalMs) {
        console.log(`   ⏳ Processing batches... Profitable patterns found so far: ${totalProfitable}`);
        lastHeartbeat = now;
      }
    })));

    // Step 6: Sort and output results
    let ranked;
    if (CONFIG.output.sortByPF) {
      ranked = allResults.sort((a, b) => b.pf - a.pf);
    } else {
      ranked = allResults.sort((a, b) => b.netPnl - a.netPnl);
    }
    
    const topResults = ranked.slice(0, CONFIG.output.topResults);

    // Generate output JSON
    const output = generateOutput(topResults, allResults, startTime, candidates.length);

    // Save to file
    const timestamp = new Date().toISOString().slice(5, 16).replace('T', '_').replace(':', '-') + 'utc';
    const filename = `brute_${timestamp}.json`;
    const outputDir = path.join(__dirname, 'brute');

    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    fs.writeFileSync(
      path.join(outputDir, filename),
      JSON.stringify(output, null, 2)
    );

    // Final console summary
    console.log('\n' + '='.repeat(70));
    console.log('✅ BRUTE COMPLETE');
    console.log('='.repeat(70));
    console.log(output.summary.overview);
    console.log(`\n🏆 TOP 15 BY ${CONFIG.output.sortByPF ? 'PROFIT FACTOR' : 'NET$'}:`);
    if (output.summary.topResults && output.summary.topResults.length > 0) {
      output.summary.topResults.slice(0, 15).forEach((algo, i) => {
        console.log(`${i + 1}. ${algo.algo}|TP${algo.tp}%|SL${algo.sl}%|PF${algo.pf}|WR${algo.wr}%|NET$${algo.netPnl}|TO${algo.timeoutRate}%|Tr${algo.trades}`);
      });
    } else {
      console.log('No profitable patterns found to display.');
    }
    console.log(`\n💾 Full results: brute/${filename}`);
    console.log(`⏱️  Runtime: ${((Date.now() - startTime) / 1000 / 60).toFixed(1)} minutes`);

  } catch (error) {
    console.error('❌ BRUTE failed:', error.message);
    console.error(error.stack);
    await dbManager.logError('brute15', 'SCAN', 'EXEC_FAIL', error.message);
  } finally {
    await dbManager.close();
  }
})();

// ============================================================================
// DATA FETCHING
// ============================================================================
async function fetchBatchData(symbols, exchanges, startTs, endTs) {
  if (symbols.length === 0 || exchanges.length === 0) return [];

  const symbolList = symbols.map(s => `'${s}'`).join(',');
  const exchangeList = exchanges.map(e => `'${e}'`).join(',');

  const query = `
    SELECT ts, symbol, exchange, c, ${CONFIG.params.join(', ')}
    FROM perp_metrics
    WHERE symbol IN (${symbolList})
      AND exchange IN (${exchangeList})
      AND ts >= $1 AND ts <= $2
      AND c IS NOT NULL AND c > 0
    ORDER BY ts ASC
  `;

  try {
    const { rows } = await dbManager.pool.query(query, [startTs, endTs]);
    return rows.map(row => {
      const obj = {
        ts: Number(row.ts),
        symbol: row.symbol,
        exchange: row.exchange,
        c: parseFloat(row.c)
      };
      CONFIG.params.forEach(p => {
        obj[p] = row[p] !== null ? parseFloat(row[p]) : null;
      });
      return obj;
    });
  } catch (err) {
    console.error(`DB query failed for batch:`, err.message);
    return [];
  }
}

// ============================================================================
// CANDIDATE GENERATION
// ============================================================================
function generateCandidates() {
  const candidates = [];

  const directionsToTest = [];
  if (CONFIG.coreDir === 'Both') {
    directionsToTest.push('Long', 'Short');
  } else {
    directionsToTest.push(CONFIG.coreDir);
  }

  for (const direction of directionsToTest) {
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
  const limit = pLimit(CONFIG.concurrencyLimit);
  const priceDataCache = new Map();

  // Sort TP/SL combinations - test most conservative first
  const tpSlPairs = [];
  for (const tp of CONFIG.tradeExecution.tpPerc) {
    for (const sl of CONFIG.tradeExecution.slPerc) {
      tpSlPairs.push({ tp, sl, conservativeScore: sl / tp });
    }
  }
  tpSlPairs.sort((a, b) => b.conservativeScore - a.conservativeScore);

  const tasks = candidates.map(candidate => limit(async () => {
    const filtered = filterData(data, candidate);

    // Early exit on trade count before simulation
    if (filtered.length < CONFIG.detection.minTrades || filtered.length > CONFIG.detection.maxTrades) {
      return null;
    }

    let bestScheme = null;
    let bestScore = -Infinity;
    let passedConservative = false;

    for (const { tp, sl } of tpSlPairs) {
      // Skip if conservative test failed
      if (!passedConservative && bestScheme !== null) continue;

      const trades = await simulateTrades(filtered, tp, sl, candidate.direction, candidate.exchange, priceDataCache);

      if (trades.length < CONFIG.detection.minTrades || trades.length > CONFIG.detection.maxTrades) {
        continue;
      }

      const stats = calculateStats(trades);

      if (stats.pf <= CONFIG.detection.minPF) continue;

      passedConservative = true;

      const wrScore = (stats.winRate / 100) * 30;
      const pfScore = stats.pf * 40;
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

    return bestScheme;
  }));

  const taskResults = await Promise.all(tasks);
  return taskResults.filter(r => r !== null);
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

// Simulate trades with batch fetching and caching
async function simulateTrades(entries, tp, sl, direction, exchange, cache) {
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

    const cacheKey = `${symbol}__${exchange}__${minTs}__${maxTs}`;
    let priceData;

    if (CONFIG.enablePriceCache && cache.has(cacheKey)) {
      priceData = cache.get(cacheKey);
    } else {
      const query = `
        SELECT ts, c
        FROM perp_metrics
        WHERE symbol = $1
          AND exchange = $2
          AND ts >= $3
          AND ts <= $4
        ORDER BY ts
      `;
      try {
        const { rows } = await dbManager.pool.query(query, [symbol, exchange, minTs, maxTs]);
        priceData = rows.map(row => ({ ts: Number(row.ts), c: parseFloat(row.c || 0) }));
        if (CONFIG.enablePriceCache) {
          cache.set(cacheKey, priceData);
        }
      } catch (err) {
        console.error(`DB query failed for ${symbol} ${exchange}:`, err.message);
        continue;
      }
    }

    for (const entry of symbolEntries) {
      const { ts: entryTs, c: entryPrice } = entry;
      const nextTs = entryTs + 60000; // Enter next minute after signal
      const tradePrices = priceData.filter(p => p.ts >= nextTs && p.ts <= nextTs + CONFIG.tradeExecution.tradeWindow * 60000);

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
  const timeouts = trades.filter(t => t.exitType === 'TIMEOUT').length;

  const grossProfit = trades.filter(t => t.pnl > 0).reduce((sum, t) => sum + t.pnl, 0);
  const grossLoss = Math.abs(trades.filter(t => t.pnl < 0).reduce((sum, t) => sum + t.pnl, 0));
  const netPnl = grossProfit - grossLoss;
  const pf = grossLoss > 0 ? grossProfit / grossLoss : (grossProfit > 0 ? 999 : 0);

  return {
    winRate: parseFloat(((wins / total) * 100).toFixed(1)),
    timeoutRate: parseFloat(((timeouts / total) * 100).toFixed(1)),
    pf: parseFloat(pf.toFixed(2)),
    netPnl: parseFloat(netPnl.toFixed(2)),
    avgPnl: parseFloat((trades.reduce((sum, t) => sum + t.pnl, 0) / total).toFixed(2)),
    wins,
    losses,
    timeouts
  };
}

// ============================================================================
// RANKING & OUTPUT
// ============================================================================
function generateOutput(topResults, allResults, startTime, totalCandidates) {
  const sortedForDisplay = CONFIG.output.sortByPF
    ? [...topResults].sort((a, b) => b.pf - a.pf)
    : [...topResults].sort((a, b) => b.netPnl - a.netPnl);

  const topForSummary = sortedForDisplay.slice(0, 15).map(r => ({
    algo: formatAlgo(r),
    tp: r.tp,
    sl: r.sl,
    pf: r.pf,
    wr: r.winRate,
    netPnl: Math.round(r.netPnl),
    timeoutRate: Math.round(r.timeoutRate),
    trades: r.trades
  }));

  const runtime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  const totalTests = totalCandidates * CONFIG.tradeExecution.tpPerc.length * CONFIG.tradeExecution.slPerc.length;

  return {
    metadata: {
      script: 'brute15.js',
      timestamp: new Date().toISOString(),
      runtime: `${runtime} minutes`,
      config: {
        corePercent: CONFIG.corePercent,
        detection: CONFIG.detection,
        tradeExecution: CONFIG.tradeExecution,
        targetSymbols: CONFIG.targetSymbols,
        params: CONFIG.params,
        exchanges: CONFIG.exchanges,
        coreDir: CONFIG.coreDir,
        output: CONFIG.output,
        concurrencyLimit: CONFIG.concurrencyLimit,
        batchSizeMinutes: CONFIG.batchSizeMinutes,
        chunkConcurrency: CONFIG.chunkConcurrency,
        enablePriceCache: CONFIG.enablePriceCache
      }
    },
    summary: {
      overview: `Tested ${CONFIG.corePercent.length * 2} thresholds × ${CONFIG.params.length} params × ${CONFIG.exchanges.length} exchanges × ${CONFIG.coreDir === 'Both' ? 2 : 1} directions = ${totalTests} total tests, ${allResults.length} profitable patterns found (PF > ${CONFIG.detection.minPF})`,
      scoreFormula: 'Score = (WinRate/100 * 30) + (PF * 40) + (min(trades/maxTrades, 1) * 30)',
      sortedBy: CONFIG.output.sortByPF ? 'PF' : 'NET$',
      topResults: topForSummary
    },
    results: allResults.slice(0, CONFIG.output.listResults).map(r => ({
      algo: formatAlgo(r),
      direction: r.direction,
      exchange: r.exchange,
      param: r.param,
      threshold: r.threshold,
      tradeScheme: {
        tp: r.tp,
        sl: r.sl,
        tradeWindow: CONFIG.tradeExecution.tradeWindow,
        posVal: CONFIG.tradeExecution.posVal
      },
      stats: {
        trades: r.trades,
        winRate: r.winRate,
        timeoutRate: r.timeoutRate,
        pf: r.pf,
        netPnl: r.netPnl,
        avgPnl: r.avgPnl,
        wins: r.wins,
        losses: r.losses,
        timeouts: r.timeouts
      },
      score: r.score
    }))
  };
}

// Format algo string for output
function formatAlgo(result) {
  const symbols = CONFIG.targetSymbols.useAll ? 'ALL' : CONFIG.targetSymbols.list.join(',');
  return `${symbols};${result.direction};${result.exchange}_${result.param}${result.operator}${result.threshold}`;
}

// ============================================================================
// End of file
// ============================================================================