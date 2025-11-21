// bt/tune15.js
// 21 Nov 2025 - Optimized Standalone ComboAlgo Backtester v15
// Tests combination algos (algo1 AND algo2 AND algo3 AND algo4) against historical data
// Outputs top performing combos by PF with trade stats
// REQUIRES: npm install p-limit

const dbManager = require('../db/dbsetup');
const fs = require('fs').promises;
const path = require('path');

// ============================================================================
// USER SETTINGS
// ============================================================================

const TradeSettings = {
  minPF: 2,
  tradeDir: 'Short',  // 'Long', 'Short', 'Both'
  tradeSymbol: { useAll: true, list: ['ETH', 'BTC'] },
  trade: {
    tradeWindow: 60,  // minutes
    posVal: 1000,     // position value in $
    tpPerc: [0.6, 1.3, 1.5],  // take profit %
    slPerc: [0.3, 0.5, 0.8]   // stop loss %
  },
  minTrades: 90,
  maxTrades: 1200
};

const ComboAlgos = {
  algo1: 'MT; bin; rsi1_chg_1m; <; [30,50]',
  algo2: 'MT; bin; v_chg_5m; >; [20,40]',
  algo3: 'BTC; bin; [params]; <; [corePerc]',  // Optional - ALL must fire within algoWindow
  algo4: ''   // Optional - ALL must fire within algoWindow
};

const AlgoSettings = {
  algoWindow: 30,  // minutes - ALL algos must fire within this window
  algoSymbol: { useAll: false, list: ['ETH', 'BTC'] },
  corePerc: [0.2, 0.4, 0.7, 1.2, 5, 35, 100],
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
  ]
};

const Output = {
  topAlgos: 15,
  listAlgos: 30,
  tradeTS: false,
  sortByPF: false  // true = sort by PF, false = sort by NET$
};

// ============================================================================
// SPEED SETTINGS
// ============================================================================

const SpeedConfig = {
  fetchParallel: 8,       // Parallel algo fetches
  cascadeParallel: 8,     // Parallel cascade operations
  simulateParallel: 8,    // Parallel trade simulations
  batchPriceFetch: true   // Fetch all symbol prices in single query (faster)
};

// ============================================================================
// CORE FUNCTIONS
// ============================================================================

function parseAlgo(str) {
  const [symbol, exchange, param, operator, value] = str.split(';').map(s => s.trim());
  return { symbol, exchange, param, operator, value };
}

async function getAllSymbolsExceptMT() {
  try {
    const result = await dbManager.query(
      `SELECT DISTINCT symbol FROM perp_metrics WHERE symbol != 'MT' ORDER BY symbol`
    );
    return result.rows.map(r => r.symbol);
  } catch (error) {
    console.error(`Error fetching symbols: ${error.message}`);
    return ['BTC', 'ETH', 'XRP', 'SOL', 'BNB', 'ADA', 'DOGE', 'MATIC', 'DOT', 'AVAX'];
  }
}

async function expandAlgo(algoStr, algoSymbol, params, corePerc) {
  const parsed = parseAlgo(algoStr);
  const combos = [];

  // Expand symbols
  let symbols = parsed.symbol === 'All' ? 
    (algoSymbol.useAll ? await getAllSymbolsExceptMT() : algoSymbol.list) :
    parsed.symbol.match(/\[(.*)\]/) ? parsed.symbol.slice(1, -1).split(',').map(s => s.trim()) :
    [parsed.symbol];

  // Expand exchanges
  let exchanges = parsed.exchange === 'All' ? ['bin', 'byb', 'okx'] :
    parsed.exchange.match(/\[(.*)\]/) ? parsed.exchange.slice(1, -1).split(',').map(s => s.trim()) :
    [parsed.exchange];

  // Expand params
  let paramList = parsed.param === '[params]' ? params :
    parsed.param.match(/\[(.*)\]/) ? parsed.param.slice(1, -1).split(',').map(s => s.trim()) :
    [parsed.param];

  // Expand values
  let values = [];
  if (parsed.value === '[corePerc]') {
    values = parsed.operator === '<' ? corePerc.map(v => -Math.abs(v)) : corePerc.map(v => Math.abs(v));
  } else if (parsed.value.match(/\[(.*)\]/)) {
    const vals = parsed.value.slice(1, -1).split(',').map(s => parseFloat(s.trim()));
    values = parsed.operator === '<' ? vals.map(v => -Math.abs(v)) : vals.map(v => Math.abs(v));
  } else {
    const val = parseFloat(parsed.value);
    values = [parsed.operator === '<' ? -Math.abs(val) : val];
  }

  // Handle <> operator
  if (parsed.operator === '<>') {
    const temp = [];
    values.forEach(v => { temp.push(Math.abs(v)); temp.push(-Math.abs(v)); });
    values = temp;
  }

  // Generate combinations
  for (const sym of symbols) {
    for (const exch of exchanges) {
      for (const param of paramList) {
        for (const val of values) {
          combos.push({
            symbol: sym, exchange: exch, param, 
            operator: val < 0 ? '<' : '>', 
            value: Math.abs(val), 
            originalValue: val
          });
        }
      }
    }
  }
  return combos;
}

async function fetchAlgoTimestamps(combo, startTs, endTs) {
  const query = `
    SELECT DISTINCT ts, symbol, exchange
    FROM perp_metrics
    WHERE symbol = $1 AND exchange = $2 AND ${combo.param} ${combo.operator} $3
      AND ts >= $4 AND ts <= $5
    ORDER BY ts ASC
  `;
  try {
    const result = await dbManager.query(query, [combo.symbol, combo.exchange, combo.originalValue, startTs, endTs]);
    return result.rows;
  } catch (error) {
    console.error(`Error fetching ${combo.symbol}_${combo.param}: ${error.message}`);
    return [];
  }
}

// Binary search cascade - ALL algos must fire within algoWindow
function cascadeAlgos(algoTimestamps, algoWindowMs) {
  if (algoTimestamps.length < 2) return [];

  let current = algoTimestamps[0]; // Start with algo1
  
  // Cascade through each subsequent algo
  for (let i = 1; i < algoTimestamps.length; i++) {
    const nextAlgo = algoTimestamps[i].sort((a, b) => Number(a.ts) - Number(b.ts));
    const matches = [];

    for (const t1 of current) {
      const windowStart = Number(t1.ts);
      const windowEnd = windowStart + algoWindowMs;
      
      // Binary search for window start
      let left = 0, right = nextAlgo.length - 1;
      while (left <= right) {
        const mid = Math.floor((left + right) / 2);
        if (Number(nextAlgo[mid].ts) <= windowStart) left = mid + 1;
        else right = mid - 1;
      }

      // Check window range
      for (let j = left; j < nextAlgo.length && Number(nextAlgo[j].ts) <= windowEnd; j++) {
        if (Number(nextAlgo[j].ts) > windowStart) {
          matches.push({
            ts: Number(nextAlgo[j].ts),
            symbol: nextAlgo[j].symbol,
            exchange: nextAlgo[j].exchange
          });
          break;
        }
      }
    }
    
    current = matches;
    if (current.length === 0) return [];
  }
  
  return current;
}

async function simulateTrades(triggers, tradeSymbols, tpPerc, slPerc, tradeWindowMs, tradeDir, posVal) {
  if (triggers.length === 0) return null;

  // Group by symbol
  const triggersBySymbol = {};
  for (const t of triggers) {
    if (!triggersBySymbol[t.symbol]) triggersBySymbol[t.symbol] = [];
    triggersBySymbol[t.symbol].push(t);
  }

  // Batch fetch prices for all symbols
  const priceCache = new Map();
  
  if (SpeedConfig.batchPriceFetch) {
    // OPTIMIZED: Single query for all symbols
    const symbolList = Object.keys(triggersBySymbol).filter(s => 
      tradeSymbols.includes('All') || tradeSymbols.includes(s)
    );
    
    if (symbolList.length > 0) {
      const minTs = Math.min(...triggers.map(t => Number(t.ts)));
      const maxTs = Math.max(...triggers.map(t => Number(t.ts))) + tradeWindowMs;
      
      try {
        const result = await dbManager.query(
          `SELECT ts, symbol, c FROM perp_metrics 
           WHERE symbol = ANY($1) AND exchange = 'bin' AND ts >= $2 AND ts <= $3 
           ORDER BY symbol, ts ASC`,
          [symbolList, minTs, maxTs]
        );
        
        // Build cache
        for (const row of result.rows) {
          if (!priceCache.has(row.symbol)) {
            priceCache.set(row.symbol, { map: new Map(), sorted: [] });
          }
          const cache = priceCache.get(row.symbol);
          cache.map.set(Number(row.ts), Number(row.c));
          cache.sorted.push({ ts: Number(row.ts), c: Number(row.c) });
        }
      } catch (error) {
        console.error(`Batch price fetch error: ${error.message}`);
      }
    }
  } else {
    // Original per-symbol fetch
    for (const symbol of Object.keys(triggersBySymbol)) {
      const symbolTriggers = triggersBySymbol[symbol];
      const minTs = Math.min(...symbolTriggers.map(t => Number(t.ts)));
      const maxTs = Math.max(...symbolTriggers.map(t => Number(t.ts))) + tradeWindowMs;

      try {
        const result = await dbManager.query(
          `SELECT ts, c FROM perp_metrics WHERE symbol = $1 AND exchange = 'bin' 
           AND ts >= $2 AND ts <= $3 ORDER BY ts ASC`,
          [symbol, minTs, maxTs]
        );
        
        const priceMap = new Map();
        const sortedPrices = result.rows;
        for (const row of sortedPrices) {
          priceMap.set(Number(row.ts), Number(row.c));
        }
        priceCache.set(symbol, { map: priceMap, sorted: sortedPrices });
      } catch (error) {
        console.error(`Price fetch error for ${symbol}: ${error.message}`);
        priceCache.set(symbol, { map: new Map(), sorted: [] });
      }
    }
  }

  // Simulate trades
  const trades = [];
  let totalPnL = 0, wins = 0, timeouts = 0;

  for (const trigger of triggers) {
    const priceData = priceCache.get(trigger.symbol);
    if (!priceData) continue;

    const entry = priceData.map.get(Number(trigger.ts));
    if (!entry) continue;

    const exitWindowEnd = Number(trigger.ts) + tradeWindowMs;
    const tpLevel = tradeDir === 'Long' ? entry * (1 + tpPerc / 100) : entry * (1 - tpPerc / 100);
    const slLevel = tradeDir === 'Long' ? entry * (1 - slPerc / 100) : entry * (1 + slPerc / 100);

    let exitPrice = null, exitType = 'timeout';
    
    for (const p of priceData.sorted) {
      const ts = Number(p.ts);
      if (ts <= Number(trigger.ts) || ts > exitWindowEnd) continue;
      const price = Number(p.c);
      if (!price) continue;

      if (tradeDir === 'Long') {
        if (price >= tpLevel) { exitPrice = tpLevel; exitType = 'tp'; break; }
        if (price <= slLevel) { exitPrice = slLevel; exitType = 'sl'; break; }
      } else {
        if (price <= tpLevel) { exitPrice = tpLevel; exitType = 'tp'; break; }
        if (price >= slLevel) { exitPrice = slLevel; exitType = 'sl'; break; }
      }
    }

    if (!exitPrice) {
      const lastPrice = priceData.sorted.filter(p => 
        Number(p.ts) <= exitWindowEnd && Number(p.ts) > Number(trigger.ts)
      ).pop();
      exitPrice = lastPrice ? Number(lastPrice.c) : entry;
      timeouts++;
    }

    const pnl = tradeDir === 'Long' ? 
      ((exitPrice - entry) / entry) * posVal : 
      ((entry - exitPrice) / entry) * posVal;

    totalPnL += pnl;
    if (pnl > 0) wins++;
    trades.push({ entryTs: trigger.ts, symbol: trigger.symbol, entry, exit: exitPrice, exitType, pnl });
  }

  if (trades.length === 0) return null;

  const totalLoss = trades.filter(t => t.pnl < 0).reduce((sum, t) => sum + Math.abs(t.pnl), 0);
  const totalProfit = trades.filter(t => t.pnl > 0).reduce((sum, t) => sum + t.pnl, 0);
  const pf = totalLoss === 0 ? (totalProfit > 0 ? 999 : 0) : totalProfit / totalLoss;

  return {
    trades,
    count: trades.length,
    netPnL: totalPnL,
    winRate: (wins / trades.length) * 100,
    timeoutRate: (timeouts / trades.length) * 100,
    profitFactor: pf
  };
}

function formatComboAlgo(algoComboArray, stats, tpPerc, slPerc, tradeSymbols, tradeDir) {
  const symbols = Array.isArray(tradeSymbols) ? tradeSymbols.join(',') : tradeSymbols;
  const algoStrs = algoComboArray.map(a => 
    `${a.symbol}_${a.exchange}_${a.param}${a.operator}${a.value}`
  );
  const algoStr = algoStrs.join(' + ');
  return `${symbols};${tradeDir};${algoStr}|TP${tpPerc}%|SL${slPerc}%|Tr${stats.count}|TO${Math.round(stats.timeoutRate)}%|NET$${Math.round(stats.netPnL)}|WR${Math.round(stats.winRate)}%|PF${stats.profitFactor.toFixed(2)}`;
}

async function writeJsonOutput(results, metadata) {
  try {
    const tuneDir = path.join(__dirname, 'tune');
    await fs.mkdir(tuneDir, { recursive: true });
    
    const now = new Date();
    const dateStr = now.toISOString().replace(/T/, '_').replace(/\..+/, '').replace(/:/g, '-').slice(0, 16);
    const filename = `tune_${dateStr}_utc.json`;
    const filepath = path.join(tuneDir, filename);
    
    const output = {
      metadata,
      results: results.slice(0, Output.listAlgos).map(r => ({
        algoCombo: r.combo.map(c => ({
          symbol: c.symbol,
          exchange: c.exchange,
          param: c.param,
          operator: c.operator,
          value: c.value
        })),
        stats: {
          tradeCount: r.stats.count,
          netPnL: Math.round(r.stats.netPnL * 100) / 100,
          winRate: Math.round(r.stats.winRate * 100) / 100,
          timeoutRate: Math.round(r.stats.timeoutRate * 100) / 100,
          profitFactor: Math.round(r.stats.profitFactor * 100) / 100
        },
        tpPercent: r.tp,
        slPercent: r.sl,
        formattedString: formatComboAlgo(r.combo, r.stats, r.tp, r.sl, metadata.tradeSymbols, metadata.tradeDir)
      }))
    };
    
    await fs.writeFile(filepath, JSON.stringify(output, null, 2));
    console.log(`\n📄 JSON output: ${filename}`);
  } catch (error) {
    console.error(`Error writing JSON: ${error.message}`);
  }
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function runTune() {
  const startTime = Date.now();
  console.log('\n🔥 Starting Tune Script');
  console.log('══════════════════════════════════════════════════════════════════════');

  // Collect algos
  const algoInputs = [];
  for (let i = 1; i <= 4; i++) {
    if (ComboAlgos[`algo${i}`] && ComboAlgos[`algo${i}`] !== '') {
      algoInputs.push({ num: i, str: ComboAlgos[`algo${i}`] });
    }
  }

  if (algoInputs.length < 2) {
    console.error('❌ Need at least 2 algos for combo testing');
    await dbManager.close();
    return;
  }

  algoInputs.forEach(a => console.log(`Algo${a.num}: "${a.str}"`));
  const symbols = TradeSettings.tradeSymbol.useAll ? 'All' : TradeSettings.tradeSymbol.list.join(',');
  console.log(`Trade Settings: minPF:${TradeSettings.minPF} | Dir:${TradeSettings.tradeDir} | Symbols:${symbols} | trW:${TradeSettings.trade.tradeWindow}min | minTr:${TradeSettings.minTrades} | maxTr:${TradeSettings.maxTrades} | algoW:${AlgoSettings.algoWindow}min`);
  console.log('══════════════════════════════════════════════════════════════════════');

  const endTs = Date.now();
  const startTs = endTs - (3 * 24 * 60 * 60 * 1000);

  // Fetch all algos
  const pLimit = (await import('p-limit')).default;
  const allAlgoResults = [];

  for (const algo of algoInputs) {
    const stepStart = Date.now();
    console.log(`\n🔥 STEP ${algo.num}: Fetching Algo${algo.num}...`);
    
    const combos = await expandAlgo(algo.str, AlgoSettings.algoSymbol, AlgoSettings.params, AlgoSettings.corePerc);
    console.log(`   Algo${algo.num}: ${combos.length} combo(s)`);

    const limit = pLimit(SpeedConfig.fetchParallel);
    const results = await Promise.all(combos.map(c => limit(() => fetchAlgoTimestamps(c, startTs, endTs))));
    const totalTriggers = results.reduce((sum, r) => sum + r.length, 0);
    console.log(`   ✓ Fetched ${totalTriggers} algo${algo.num} triggers (${Date.now() - stepStart}ms)`);
    
    allAlgoResults.push(results.map((ts, i) => ({ combo: combos[i], timestamps: ts })));
  }

  // Generate all algo combinations
  console.log(`\n🔗 STEP ${algoInputs.length + 1}: Cascading combos in algoWindow...`);
  const stepStart = Date.now();
  const algoWindowMs = AlgoSettings.algoWindow * 60 * 1000;
  
  function cartesianProduct(arrays) {
    if (arrays.length === 0) return [[]];
    const [first, ...rest] = arrays;
    const restProduct = cartesianProduct(rest);
    const result = [];
    for (const item of first) {
      for (const combo of restProduct) {
        result.push([item, ...combo]);
      }
    }
    return result;
  }

  const allCombinations = cartesianProduct(allAlgoResults);
  console.log(`   Testing ${allCombinations.length} algo combinations...`);

  const cascadeLimit = pLimit(SpeedConfig.cascadeParallel);
  const cascadeResults = await Promise.all(
    allCombinations.map(algoCombo => cascadeLimit(() => {
      const timestamps = algoCombo.map(a => a.timestamps);
      const triggers = cascadeAlgos(timestamps, algoWindowMs);
      if (triggers.length > 0) {
        return { algoCombos: algoCombo.map(a => a.combo), triggers };
      }
      return null;
    }))
  );

  const validCascade = cascadeResults.filter(r => r !== null);
  console.log(`   ${validCascade.length} combos passed cascade (${Date.now() - stepStart}ms)`);

  const failedMin = validCascade.filter(r => r.triggers.length < TradeSettings.minTrades).length;
  const failedMax = validCascade.filter(r => r.triggers.length > TradeSettings.maxTrades).length;
  console.log(`   ${failedMin} failed minTrades; ${failedMax} failed maxTrades`);

  const validCombos = validCascade.filter(r => 
    r.triggers.length >= TradeSettings.minTrades && r.triggers.length <= TradeSettings.maxTrades
  );
  console.log(`   ${validCombos.length} combos ready for simulation`);

  // Simulate trades with conservative-first optimization
  console.log(`\n💰 STEP ${algoInputs.length + 2}: Simulating trades...`);
  const simStart = Date.now();
  const tradeSymbols = TradeSettings.tradeSymbol.useAll ? await getAllSymbolsExceptMT() : TradeSettings.tradeSymbol.list;
  const tradeWindowMs = TradeSettings.trade.tradeWindow * 60 * 1000;

  // Sort TP/SL combinations - test most conservative first (highest SL/TP ratio)
  const tpSlPairs = [];
  for (const tp of TradeSettings.trade.tpPerc) {
    for (const sl of TradeSettings.trade.slPerc) {
      tpSlPairs.push({ tp, sl, conservativeScore: sl / tp });
    }
  }
  tpSlPairs.sort((a, b) => b.conservativeScore - a.conservativeScore);

  const results = [];
  const simLimit = pLimit(SpeedConfig.simulateParallel);
  
  for (const combo of validCombos) {
    let passedConservative = false;
    
    const comboResults = await Promise.all(
      tpSlPairs.map(({ tp, sl }) => simLimit(async () => {
        // Skip if conservative test failed and this isn't the first test
        if (!passedConservative && results.length > 0) return null;
        
        const stats = await simulateTrades(
          combo.triggers, tradeSymbols, tp, sl,
          tradeWindowMs, TradeSettings.tradeDir, TradeSettings.trade.posVal
        );
        
        if (stats && stats.profitFactor >= TradeSettings.minPF) {
          passedConservative = true;
          return { combo: combo.algoCombos, stats, tp, sl };
        }
        return null;
      }))
    );
    
    results.push(...comboResults.filter(r => r !== null));
  }

  console.log(`   ${validCombos.length * tpSlPairs.length - results.length} combos failed minPF`);
  console.log(`   ${results.length} combos passed minPF! (${Date.now() - simStart}ms)`);

  // Output results
  console.log('\n📊 RESULTS:');
  console.log('══════════════════════════════════════════════════════════════════════');

  if (results.length === 0) {
    console.log('No combos passed all filters.');
  } else {
    // Sort by user preference
    if (Output.sortByPF) {
      results.sort((a, b) => b.stats.profitFactor - a.stats.profitFactor);
    } else {
      results.sort((a, b) => b.stats.netPnL - a.stats.netPnL);
    }
    
    const topResults = results.slice(0, Output.topAlgos);
    
    topResults.forEach((r, i) => {
      console.log(`${i + 1}. ${formatComboAlgo(r.combo, r.stats, r.tp, r.sl, symbols, TradeSettings.tradeDir)}`);
    });
    
    // Write JSON output
    const metadata = {
      timestamp: new Date().toISOString(),
      runtimeMinutes: ((Date.now() - startTime) / 1000 / 60).toFixed(2),
      settings: {
        minPF: TradeSettings.minPF,
        tradeDir: TradeSettings.tradeDir,
        tradeSymbols: symbols,
        tradeWindow: TradeSettings.trade.tradeWindow,
        posVal: TradeSettings.trade.posVal,
        minTrades: TradeSettings.minTrades,
        maxTrades: TradeSettings.maxTrades,
        algoWindow: AlgoSettings.algoWindow
      },
      algos: algoInputs.map(a => ({ num: a.num, definition: a.str })),
      totalCombinationsTested: allCombinations.length,
      totalPassedFilters: results.length,
      sortedBy: Output.sortByPF ? 'PF' : 'NET$'
    };
    
    await writeJsonOutput(results, metadata);
  }

  const runtime = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
  console.log(`\n⏱️  Runtime: ${runtime} min`);

  await dbManager.close();
}

if (require.main === module) {
  runTune().catch(err => {
    console.error('❌ Error:', err);
    process.exit(1);
  });
}

module.exports = { runTune };