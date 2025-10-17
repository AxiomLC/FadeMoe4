/**
 * backtester/tuner.js
 * Lightweight tuner that performs grid search over parameter ranges declared in algos_seed.json
 *
 * Expected algo entry to contain `param_ranges` object:
 *   "param_ranges": { "rsi1_chg_1m": [0.5,1.0,1.5], "c_chg_5m": [0.5,1.0,1.5] }
 *
 * It will call core.runBacktest for each candidate and persist results (core already writes files).
 *
 * AI integration point (commented):
 *  - After top-K candidates are found, call LLM to suggest refined ranges or hill-climb directions.
 */
const core = require('./core');
const fs = require('fs');
const path = require('path');

async function generateGrid(paramRanges) {
  const keys = Object.keys(paramRanges);
  if (keys.length === 0) return [{}];
  // cartesian product
  const combos = [];
  function helper(idx, cur) {
    if (idx === keys.length) { combos.push(Object.assign({}, cur)); return; }
    const k = keys[idx];
    for (const v of paramRanges[k]) {
      cur[k] = v;
      helper(idx+1, cur);
    }
  }
  helper(0, {});
  return combos;
}

async function tuneAlgorithm(algo) {
  if (!algo.param_ranges) { console.log('No param_ranges for algo', algo.algo_id); return []; }
  const grid = await generateGrid(algo.param_ranges);
  console.log(`Tuning ${algo.algo_id} - ${grid.length} candidates`);
  const results = [];
  for (const params of grid) {
    // merge params into algo.expr by substituting if desired, but simplest is to set algo.params and let evaluator use them
    const tunedAlgo = Object.assign({}, algo);
    // note: evaluator in core expects fields in rows; tuning might require altering expr or running with param placeholders.
    // For simplicity, assume algos use numeric literals from params via algo.params placeholders before tuning; advanced substitution can be added.
    const res = await core.runBacktest(tunedAlgo, params);
    results.push({ params, summary: res.summary, filename: res.filename });
  }
  // sort by winrate descending
  results.sort((a,b) => (b.summary.winrate || 0) - (a.summary.winrate || 0));
  // recommend top 3
  return results.slice(0,3);
}

module.exports = { tuneAlgorithm };
