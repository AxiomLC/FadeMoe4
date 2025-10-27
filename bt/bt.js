/*
  bt/bt.js - Core Backtesting Engine (Renamed from backtester.js)
  Description: Loads strategy.json, builds SQL query for perp_metrics (new schema: % chg columns),
               fetches qualifying data points, optimizes TP/SL schemes via grid search (125 combos),
               simulates trades (forward-walk on 1-min bars), summarizes results (PF, winRate),
               outputs results.json. Logs to perp_status/errors via dbManager.
               Per README: Multi-exchange conditions, MT joins, 10k row limit, scoring formula.
  Date: 24 Oct 2025
  Version: 2.0 (Completed functions, schema-adapted, bt/ paths)
*/

const fs = require('fs');
const path = require('path');

// ===== IMPORTS (For new dbsetup.js) =====
const dbManager = require('../db/dbsetup');  // Instance: pool.query(), logStatus(), etc.

// ===== MAIN FUNCTIONS =====

// --- runBacktest: Entry Point (Full Flow) ---
async function runBacktest() {
  const strategyPath = path.join(__dirname, 'strategy.json');  // Resolves to bt/strategy.json
  if (!fs.existsSync(strategyPath)) {
    throw new Error('strategy.json not found in bt/');
  }
  const strategy = JSON.parse(fs.readFileSync(strategyPath, 'utf8'));
  console.log(`🚀 Running Backtest: ${strategy.name || 'Unnamed'}`);

  try {
    // Build and execute query
    const query = await buildQuery(strategy);
    console.log('📝 Generated Query (perp_metrics chg columns):', query);

    const { rows } = await dbManager.pool.query(query);
    console.log(`📊 Found ${rows.length} qualifying data points`);

    if (rows.length === 0) {
      console.log('⚠️ No data matched conditions');
      const emptyResults = { 
        stats: { total: 0, wins: 0, losses: 0, tp1: 0, tp2: 0, winRate: '0.0%', pf: 0 }, 
        trades: [],
        bestScheme: null
      };
      fs.writeFileSync(path.join(__dirname, 'results.json'), JSON.stringify(emptyResults, null, 2));  // bt/results.json
      await dbManager.logStatus('bt.js', 'warning', 'NO_DATA', { strategy: strategy.name, rows: 0 });
      return emptyResults;
    }

    // Process data: Handle BigInt ts and NUMERIC chg (new schema)
    const processedData = rows.map(row => ({
      ...row,
      ts: Number(row.ts),  // Convert BigInt
      c: parseFloat(row.c || 0),
      o: parseFloat(row.o || 0),
      h: parseFloat(row.h || 0),
      l: parseFloat(row.l || 0),
      // Chg fields: NUMERIC(7,3) to float
      c_chg_1m: parseFloat(row.c_chg_1m || 0),
      c_chg_5m: parseFloat(row.c_chg_5m || 0),
      c_chg_10m: parseFloat(row.c_chg_10m || 0),
      v_chg_1m: parseFloat(row.v_chg_1m || 0),
      oi_chg_1m: parseFloat(row.oi_chg_1m || 0),
      pfr_chg_1m: parseFloat(row.pfr_chg_1m || 0),
      lsr_chg_1m: parseFloat(row.lsr_chg_1m || 0),
      // Add more as needed (e.g., rsi1_chg_5m)
    }));

    // Optimize scheme
    const bestScheme = findBestScheme(processedData, strategy.direction);
    console.log('✨ Best scheme (TP1/TP2/SL %):', bestScheme);

    // Simulate and summarize
    const trades = simulateTrades(processedData, bestScheme, strategy.direction);
    const stats = summarizeResults(trades);

    const results = { stats, trades, bestScheme, strategy };  // Include original strategy
    const resultsPath = path.join(__dirname, 'results.json');
    fs.writeFileSync(resultsPath, JSON.stringify(results, null, 2));
    
    console.log(`✅ Backtest complete. ${trades.length} trades. Win Rate: ${stats.winRate}`);
    console.log('📈 Full Stats:', JSON.stringify(stats, null, 2));
    
    await dbManager.logStatus('bt.js', 'success', 'BACKTEST_DONE', { 
      strategy: strategy.name, 
      totalTrades: trades.length, 
      winRate: stats.winRate,
      pf: stats.pf 
    });
    
    return results;
  } catch (error) {
    console.error('❌ Backtest failed:', error.message);
    await dbManager.logError('bt.js', 'BACKTEST', 'EXEC_FAIL', error.message, { strategy: strategy.name });
    throw error;
  }
}

// --- buildQuery: SQL Builder (Multi-Exchange, MT Join) ---
// --- buildQuery: SQL Builder (Multi-Exchange, MT Join) ---  // UPDATED: Fix MT conditional (length >0 for empty [])
async function buildQuery(strategy) {
  const { symbols, conditions, mtTokens, direction } = strategy;
  if (!symbols || !conditions) throw new Error('Invalid strategy: missing symbols/conditions');

  const symbolList = symbols.map(s => `'${s}'`).join(',');
  
  // Group conditions by exchange
  const exchangeConditions = {};
  conditions.forEach(c => {
    const exch = c.exchange || 'bin';  // Default
    if (!exchangeConditions[exch]) exchangeConditions[exch] = [];
    exchangeConditions[exch].push(`${c.param} ${c.operator || '>'} ${c.value}`);
  });

  // WHERE clause: OR groups per exchange
  const whereParts = Object.entries(exchangeConditions).map(([exch, conds]) => 
    `(exchange = '${exch}' AND ${conds.join(' AND ')})`
  );
  const whereClause = whereParts.join(' OR ');

  // MT join and fields: ONLY if mtTokens is non-empty array (fixes empty [] bug)
  let mtJoin = '';
  let mtWhere = '';
  let mtSelect = '';  // NEW: Separate var for SELECT fields
  if (mtTokens && mtTokens.length > 0) {  // UPDATED: Explicit length check (empty [] skips)
    const mtList = mtTokens.map(s => `'${s}'`).join(',');
    mtJoin = `LEFT JOIN perp_metrics mt ON pm.ts = mt.ts AND mt.symbol IN (${mtList})`;
    mtWhere = ' AND mt.symbol IS NOT NULL';
    mtSelect = ', mt.c as mt_c, mt.v_chg_1m as mt_v_chg_1m';  // Only add if joining
  }

  // Query: Target perp_metrics chg columns (new schema)
  const query = `
    SELECT pm.ts, pm.symbol, pm.exchange, pm.c, pm.o, pm.h, pm.l,
           pm.c_chg_1m, pm.c_chg_5m, pm.c_chg_10m,
           pm.v_chg_1m, pm.oi_chg_1m, pm.pfr_chg_1m, pm.lsr_chg_1m
           ${mtSelect}  /* MT fields only if mtTokens non-empty */
    FROM perp_metrics pm
    ${mtJoin}
    WHERE pm.symbol IN (${symbolList})
      AND (${whereClause})
      ${mtWhere}
    ORDER BY pm.ts ASC
    LIMIT 10000;  /* Per README: Performance cap */
  `;

  return query;
}

// --- findBestScheme: Grid Search Optimization ---
function findBestScheme(data, direction) {
  // Options per README (as decimals for calc)
  const tp1Options = [0.003, 0.0045, 0.006, 0.0075, 0.010];
  const tp2Options = [0.006, 0.0075, 0.010, 0.0125, 0.015];
  const slOptions = [0.0005, 0.00085, 0.0012, 0.0015, 0.002];

  let bestScheme = null;
  let bestScore = -Infinity;

  for (const tp1 of tp1Options) {
    for (const tp2 of tp2Options) {
      if (tp2 <= tp1) continue;  // TP2 > TP1
      
      for (const sl of slOptions) {
        const scheme = { tp1, tp2, sl, direction };
        const trades = simulateTrades(data, scheme, direction);
        const stats = summarizeResults(trades);
        
        // Scoring formula per README
        const total = stats.total || 1;
        const winRate = parseFloat(stats.winRate) / 100 || 0;
        const tp2Pct = (stats.tp2 / total) * 50;
        const lossPct = (stats.losses / total) * 30;
        const score = (winRate * 2) + tp2Pct - lossPct;
        
        if (score > bestScore) {
          bestScore = score;
          bestScheme = { ...scheme, score, pf: parseFloat(stats.pf) || 0 };
        }
      }
    }
  }

  // Fallback if no valid
  if (!bestScheme) {
    bestScheme = { tp1: 0.006, tp2: 0.010, sl: 0.001, direction, score: 0, pf: 0 };
  }
  return bestScheme;
}

// --- simulateTrades: Forward-Walk Simulation ---
function simulateTrades(data, scheme, direction) {
  const { tp1, tp2, sl } = scheme;
  const trades = [];
  let inTrade = false;
  let entryPrice = 0;
  let entryTs = 0;

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const price = row.c;
    const ts = row.ts;

    if (!inTrade) {
      // Enter on signal (each row qualifies)
      entryPrice = price;
      entryTs = ts;
      inTrade = true;
      continue;
    }

    // Check % change from entry
    const pctChange = direction === 'Long' 
      ? (price - entryPrice) / entryPrice 
      : (entryPrice - price) / entryPrice;

    let exitType = null;
    let pnl = 0;

    if (pctChange >= tp1) {
      pnl += tp1 * 0.5;  // 50% at TP1
      if (pctChange >= tp2) {
        pnl += tp2 * 0.5;  // Rest at TP2
        exitType = 'TP2';
      } else {
        exitType = 'TP1';
      }
    } else if (pctChange <= -sl) {
      exitType = 'SL';
      pnl = -sl;
    }

    if (exitType) {
      inTrade = false;
      const finalPnl = direction === 'Long' ? pnl : -pnl;  // Invert for Short
      trades.push({
        entryTs, exitTs: ts, entryPrice, exitPrice: price,
        pnl: finalPnl, exitType, duration: ts - entryTs
      });
    }
  }

  // Close open trade at end
  if (inTrade && data.length > 0) {
    const lastPrice = data[data.length - 1].c;
    const pctChange = direction === 'Long' 
      ? (lastPrice - entryPrice) / entryPrice 
      : (entryPrice - lastPrice) / entryPrice;
    trades.push({
      entryTs, exitTs: data[data.length - 1].ts, entryPrice, exitPrice: lastPrice,
      pnl: direction === 'Long' ? pctChange : -pctChange,
      exitType: 'END', duration: data[data.length - 1].ts - entryTs
    });
  }

  return trades;
}

// --- summarizeResults: Stats Calculation (PF, Win Rate) ---
function summarizeResults(trades) {
  if (trades.length === 0) {
    return { total: 0, wins: 0, losses: 0, tp1: 0, tp2: 0, winRate: '0%', pf: 0, avgPnl: 0 };
  }

  const total = trades.length;
  const wins = trades.filter(t => t.pnl > 0).length;
  const losses = trades.filter(t => t.pnl < 0).length;
  const tp1 = trades.filter(t => t.exitType === 'TP1').length;
  const tp2 = trades.filter(t => t.exitType === 'TP2').length;

  const winRate = ((wins / total) * 100).toFixed(1) + '%';
  const grossProfit = trades.filter(t => t.pnl > 0).reduce((sum, t) => sum + t.pnl, 0);
  const grossLoss = Math.abs(trades.filter(t => t.pnl < 0).reduce((sum, t) => sum + t.pnl, 0) || 0);
  const pf = grossLoss > 0 ? (grossProfit / grossLoss).toFixed(2) : (grossProfit > 0 ? '∞' : '0.00');

  return {
    total, wins, losses, tp1, tp2, winRate,
    pf, avgPnl: (trades.reduce((sum, t) => sum + t.pnl, 0) / total).toFixed(4)
  };
}

// ===== EXPORTS (For app.js) =====
module.exports = { 
  runBacktest, 
  buildQuery, 
  findBestScheme, 
  simulateTrades, 
  summarizeResults 
};