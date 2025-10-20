// backtester/backtester.js
const fs = require('fs');
const db = require('../db/dbsetup');

async function runBacktest() {
  const strategy = JSON.parse(fs.readFileSync(__dirname + '/strategy.json', 'utf8'));
  console.log(`🚀 Running Backtest: ${strategy.name}`);

  // Build complex query with multi-exchange support
  const query = await buildQuery(strategy);
  console.log('📝 Query:', query);

  const { rows } = await db.pool.query(query);
  console.log(`📊 Found ${rows.length} qualifying data points`);

  if(rows.length === 0) {
    console.log('⚠️  No data matched conditions');
    fs.writeFileSync(__dirname + '/results.json', JSON.stringify({ 
      stats: { total: 0, tp1: 0, tp2: 0, loss: 0, winRate: '0.0' }, 
      trades: [],
      bestScheme: null
    }, null, 2));
    return;
  }

  // Find optimal trade scheme
  const bestScheme = findBestScheme(rows, strategy.direction);
  console.log('✨ Best scheme:', bestScheme);

  // Run trades with best scheme
  const trades = simulateTrades(rows, bestScheme, strategy.direction);
  const stats = summarizeResults(trades);

  fs.writeFileSync(__dirname + '/results.json', JSON.stringify({ 
    stats, 
    trades, 
    bestScheme 
  }, null, 2));
  
  console.log(`✅ Backtest complete. ${trades.length} trades evaluated.`);
  console.log(stats);
  
  return { stats, trades, bestScheme };
}

async function buildQuery(strategy) {
  const symbolList = strategy.symbols.map(s => `'${s}'`).join(',');
  
  // Group conditions by exchange
  const exchangeConditions = {};
  strategy.conditions.forEach(c => {
    if(!exchangeConditions[c.exchange]) {
      exchangeConditions[c.exchange] = [];
    }
    exchangeConditions[c.exchange].push(`${c.param} ${c.operator} ${c.value}`);
  });

  // Build WHERE clause for multi-exchange
  const whereParts = [];
  for(const [exch, conds] of Object.entries(exchangeConditions)) {
    whereParts.push(`(exchange = '${exch}' AND ${conds.join(' AND ')})`);
  }
  
  const whereClause = whereParts.join(' OR ');

  // Add MT token filtering if specified
  let mtJoin = '';
  let mtWhere = '';
  if(strategy.mtTokens && strategy.mtTokens.length > 0) {
    const mtList = strategy.mtTokens.map(s => `'${s}'`).join(',');
    mtJoin = `
      LEFT JOIN perp_metrics mt 
      ON pm.ts = mt.ts 
      AND mt.symbol IN (${mtList})
    `;
    mtWhere = 'AND mt.symbol IS NOT NULL';
  }

  const query = `
    SELECT pm.ts, pm.symbol, pm.exchange, pm.c, pm.o, pm.h, pm.l,
           pm.c_chg_1m, pm.c_chg_5m, pm.c_chg_10m,
           pm.v_chg_1m, pm.oi_chg_1m, pm.pfr_chg_1m
    FROM perp_metrics pm
    ${mtJoin}
    WHERE pm.symbol IN (${symbolList})
      AND (${whereClause})
      ${mtWhere}
    ORDER BY pm.ts ASC
    LIMIT 10000;
  `;

  return query;
}

function findBestScheme(data, direction) {
  // Test multiple TP/SL combinations
  const tp1Options = [0.3, 0.45, 0.6, 0.75, 1.0];
  const tp2Options = [0.6, 0.75, 1.0, 1.25, 1.5];
  const slOptions = [0.05, 0.085, 0.12, 0.15, 0.2];
  
  let bestScheme = null;
  let bestScore = -Infinity;

  for(const tp1 of tp1Options) {
    for(const tp2 of tp2Options) {
      if(tp2 <= tp1) continue; // TP2 must be > TP1
      
      for(const sl of slOptions) {
        const scheme = { tp1, tp2, sl, direction };
        const trades = simulateTrades(data, scheme, direction);
        const stats = summarizeResults(trades);
        
        // Score = winRate * 2 + (tp2Count/total * 50) - (lossCount/total * 30)
        const winRate = parseFloat(stats.winRate);