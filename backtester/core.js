/**
 * backtester/core.js
 * Simplified deterministic backtester core (developer-oriented).
 *
 * Controls at top of file:
 *  - RUN_LOOKBACK_MIN: minutes to backtest
 *  - OUTPUT_DIR: where JSON runs are saved
 *  - WINRATE_PROMOTE_THRESHOLD: short-list threshold (e.g., 0.6 => 60%)
 *
 * Notes:
 *  - This file expects a local `db/dbsetup.js` export (dbManager) with a `pool` to query.
 *  - It reads algos from algos_seed.json and writes run artifacts under OUTPUT_DIR.
 *  - The rule evaluator uses a lightweight function builder. This is developer tooling; avoid running untrusted algos.
 *
 * AI integration points (commented below) — where you would call an LLM to suggest new algos or tweak params:
 *  - After run summary generation: send summary JSON to LLM to request suggestions (rankings / param deltas).
 *  - Before tuner starts: send top-N candidates for LLM scoring to prioritize tuning.
 *
 * DB note (commented): Suggested table `perp_algos`:
 *  CREATE TABLE perp_algos (
 *    algo_id TEXT PRIMARY KEY,
 *    title TEXT,
 *    algo_def JSONB,
 *    created_at TIMESTAMP DEFAULT NOW()
 *  );
 *
 */

const fs = require('fs');
const path = require('path');
const dbManager = require('../db/dbsetup'); // your existing db manager
const algosSeedPath = path.join(__dirname, 'algos_seed.json');

const RUN_LOOKBACK_MIN = 60 * 24 * 3; // minutes (default: last 3 days)
const OUTPUT_DIR = path.join(__dirname, 'algos'); // will create weekly folders
const WINRATE_PROMOTE_THRESHOLD = 0.60; // 60% default

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function timestampIso(ts){ return (new Date(ts)).toISOString().replace(/[:.]/g,'-'); }

// Basic safe-ish evaluator: builds a function that receives `row` and returns boolean
// WARNING: This uses Function() and is developer tooling. Do NOT evaluate untrusted code.
// Expected algo.expr uses metric names as variables, e.g. "rsi1_chg_1m > 1.14 && c_chg_5m > 1.5"
function buildEvaluator(expr, allowedVars) {
  // create a list of var declarations to pick values from row and default to null
  const decls = allowedVars.map(v => `const ${v} = (row['${v}'] !== undefined ? row['${v}'] : null);`).join('\n');
  const fnSrc = `${decls}\nreturn (function(){ try { return Boolean(${expr}); } catch(e) { return false; } })();`;
  return new Function('row', fnSrc);
}

// Simple trade simulation:
// - On expr true, open position at that row.c price (or next row.c if null).
// - Monitor following rows until TP or SL hit (using price series c).
// - TP/SL are relative (e.g. 0.0043 => +0.43%). If multiple TPs, take earliest hit and record which.
async function simulateTrades(rows, algo, paramsOverride = {}) {
  const trades = [];
  const evalExpr = algo.expr;
  // prepare allowed vars from first row
  const allowedVars = rows.length>0 ? Object.keys(rows[0]).filter(k=>typeof rows[0][k] === 'number') : [];
  const evaluator = buildEvaluator(evalExpr, allowedVars);

  const tpArr = (algo.tradeSpec && algo.tradeSpec.take_profits) ? algo.tradeSpec.take_profits.slice() : [];
  const sl = (algo.tradeSpec && algo.tradeSpec.stop_loss) ? algo.tradeSpec.stop_loss : null;
  const side = (algo.tradeSpec && algo.tradeSpec.side) ? algo.tradeSpec.side.toLowerCase() : 'buy';

  for (let i=0;i<rows.length;i++){
    const row = rows[i];
    let trigger = false;
    try { trigger = evaluator(row); } catch(e){ trigger=false; }

    if (!trigger) continue;

    // entry price: use row.c, or next available c
    let entryPrice = row.c;
    if (entryPrice === null || entryPrice === undefined) {
      // look ahead up to 3 rows
      for (let j=i+1;j<Math.min(rows.length, i+4); j++){
        if (rows[j].c) { entryPrice=rows[j].c; break; }
      }
    }
    if (!entryPrice) continue; // can't simulate without entry price

    let exitPrice = null;
    let exitIndex = null;
    let result = null;
    // search forward for TP/SL
    for (let j=i+1;j<rows.length;j++){
      const p = rows[j].c;
      if (!p) continue;
      const ret = side === 'buy' ? (p - entryPrice) / entryPrice : (entryPrice - p) / entryPrice;
      // check TPs first (earliest TP hit)
      let tpHit = null;
      for (let k=0;k<tpArr.length;k++){
        if (ret >= tpArr[k]) { tpHit = {tp: tpArr[k], tpIndex:k}; break; }
      }
      if (tpHit) {
        exitPrice = p;
        exitIndex = j;
        result = { type: 'TP', tp: tpHit.tp, tpIndex: tpHit.tpIndex };
        break;
      }
      if (sl !== null && ret <= sl) {
        exitPrice = p;
        exitIndex = j;
        result = { type: 'SL', sl: sl };
        break;
      }
    }
    // if neither hit, close at last row (simulate timeout)
    if (!exitPrice) {
      exitPrice = rows[Math.min(rows.length-1, i+Math.floor(60))].c || entryPrice; // close after up to 60 rows
      exitIndex = Math.min(rows.length-1, i+Math.floor(60));
      result = { type: 'TIMEOUT' };
    }

    const pnl = side === 'buy' ? (exitPrice - entryPrice) / entryPrice : (entryPrice - exitPrice) / entryPrice;
    trades.push({
      algo_id: algo.algo_id,
      ts_entry: rows[i].ts,
      ts_exit: rows[exitIndex].ts,
      entry_price: entryPrice,
      exit_price: exitPrice,
      pnl,
      result
    });

    // advance i past exitIndex to avoid overlapping trades
    i = exitIndex;
  }

  return trades;
}

// Query perp_metrics for given time window and perpspec/symbol (or full table)
async function fetchMetricRows({ perpspec=null, symbol=null, startTs, endTs }) {
  const where = [];
  const params = [];
  let idx = 1;
  if (perpspec) { where.push(`perpspec = $${idx++}`); params.push(perpspec); }
  if (symbol) { where.push(`symbol = $${idx++}`); params.push(symbol); }
  where.push(`ts >= $${idx++}`); params.push(BigInt(startTs));
  where.push(`ts <= $${idx++}`); params.push(BigInt(endTs));
  const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
  const q = `
    SELECT * FROM perp_metrics
    ${whereClause}
    ORDER BY ts ASC
  `;
  const res = await dbManager.pool.query(q, params);
  // normalize numeric strings to JS numbers where possible
  return res.rows.map(r => {
    const out = {};
    for (const k of Object.keys(r)) {
      out[k] = (r[k] !== null && typeof r[k] === 'bigint') ? Number(r[k]) : r[k];
      if (typeof out[k] === 'string' && !isNaN(out[k])) {
        // # keep strings like '123.45' as numbers where appropriate
        const n = Number(out[k]);
        if (!isNaN(n)) out[k] = n;
      }
      if (k === 'ts' && typeof out[k] === 'bigint') out[k] = Number(out[k]);
    }
    return out;
  });
}

function summarizeTrades(trades) {
  if (!trades || trades.length === 0) return { trades_count:0, winrate:0, avg_pnl:0, total_pnl:0 };
  const wins = trades.filter(t => t.pnl > 0);
  const avg = trades.reduce((s,t)=>s + t.pnl,0) / trades.length;
  const total = trades.reduce((s,t)=>s + t.pnl,0);
  return { trades_count: trades.length, winrate: wins.length / trades.length, avg_pnl: avg, total_pnl: total };
}

// Public run function for single algo and params
async function runBacktest(algo, params = {}, opts = {}) {
  const now = Date.now();
  const endTs = now;
  const startTs = now - (RUN_LOOKBACK_MIN * 60 * 1000);
  // fetch rows (optionally filter by symbol/perpspec)
  const rows = await fetchMetricRows({ perpspec: algo.perpspec || null, symbol: algo.symbol || null, startTs, endTs });
  const trades = await simulateTrades(rows, algo, params);
  const summary = summarizeTrades(trades);
  // prepare output file
  const isoWeek = (new Date()).toISOString().slice(0,10);
  const weekFolder = path.join(OUTPUT_DIR, `week-${isoWeek}`);
  ensureDir(weekFolder);
  const filename = path.join(weekFolder, `${algo.algo_id}--${timestampIso(Date.now())}.json`);
  const out = { meta: { algo_id: algo.algo_id, title: algo.title, params, startTs, endTs, created_at: new Date().toISOString() }, summary, trades };
  fs.writeFileSync(filename, JSON.stringify(out, null, 2));
  // if hot candidates exist, add recommendations
  const recs = [];
  if (summary.winrate >= WINRATE_PROMOTE_THRESHOLD && summary.trades_count >= 5) {
    recs.push({ reason: `winrate>=${WINRATE_PROMOTE_THRESHOLD}`, algo_id: algo.algo_id, filename });
  }
  return { summary, trades, filename, recommendations: recs };
}

// Run all algos from seed
async function runAllSeed() {
  const seed = JSON.parse(fs.readFileSync(algosSeedPath,'utf8'));
  const results = [];
  for (const algo of seed) {
    try {
      console.log(`Running algo ${algo.algo_id} - ${algo.title}`);
      const res = await runBacktest(algo);
      console.log(`  -> summary: ${JSON.stringify(res.summary)}`);
      if (res.recommendations.length) console.log('  -> Recommendations:', res.recommendations);
      results.push(res);
    } catch (e) {
      console.error('Error running algo', algo.algo_id, e);
    }
  }
  return results;
}

module.exports = { runBacktest, runAllSeed };
