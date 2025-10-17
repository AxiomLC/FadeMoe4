/**
 * backtester/tuner2.js
 * Enhanced tuner with parallel execution and improved parameter handling.
 *
 * Features:
 * - Parallel execution of parameter combinations
 * - Better parameter grid generation
 * - Enhanced result analysis
 * - Configurable parallelism
 */

const Backtester = require('./core2');

class AlgorithmTuner {
  constructor(config = {}) {
    this.backtester = new Backtester(config);
    this.defaultOptions = {
      parallelism: 4,
      topCount: 5,
      minTrades: 5,
      minWinRate: 0.5,
      visualize: true
    };
  }

  async _generateGrid(paramRanges) {
    const keys = Object.keys(paramRanges);
    if (keys.length === 0) return [{}];

    // Cartesian product
    const combos = [];
    function helper(idx, cur) {
      if (idx === keys.length) {
        combos.push(Object.assign({}, cur));
        return;
      }
      const k = keys[idx];
      for (const v of paramRanges[k]) {
        cur[k] = v;
        helper(idx + 1, cur);
      }
    }
    helper(0, {});
    return combos;
  }

  async tuneAlgorithm(algo, options = {}) {
    options = { ...this.defaultOptions, ...options };

    if (!algo.param_ranges || Object.keys(algo.param_ranges).length === 0) {
      console.log('No param_ranges for algo', algo.algo_id);
      return [];
    }

    const grid = await this._generateGrid(algo.param_ranges);
    console.log(`Tuning ${algo.algo_id} - ${grid.length} candidates`);

    // Process in parallel batches
    const results = [];
    for (let i = 0; i < grid.length; i += options.parallelism) {
      const batch = grid.slice(i, i + options.parallelism);
      const batchResults = await Promise.all(batch.map(async params => {
        try {
          const res = await this.backtester.runBacktest(algo, params, { visualize: false });
          return { params, summary: res.summary, filename: res.filename };
        } catch (e) {
          console.error('Error in batch:', e);
          return null;
        }
      }));

      results.push(...batchResults.filter(r => r !== null));
    }

    // Filter results based on criteria
    const filteredResults = results.filter(r =>
      r.summary.trades_count >= options.minTrades &&
      r.summary.winrate >= options.minWinRate
    );

    // Sort by winrate descending, then by total_pnl
    filteredResults.sort((a, b) => {
      if (b.summary.winrate !== a.summary.winrate) {
        return b.summary.winrate - a.summary.winrate;
      }
      return (b.summary.total_pnl || 0) - (a.summary.total_pnl || 0);
    });

    // Print top results
    console.log('\nTop Tuning Results:');
    console.log('--------------------------------------------------------------------------------');
    console.log('Rank | Win Rate | Total PnL | Avg PnL | Params');
    console.log('--------------------------------------------------------------------------------');

    const topResults = filteredResults.slice(0, options.topCount);
    topResults.forEach((result, index) => {
      const paramsStr = Object.entries(result.params)
        .map(([k, v]) => `${k}=${v}`)
        .join(', ');

      console.log(
        `${(index + 1).toString().padStart(4)} | ` +
        `${(result.summary.winrate * 100).toFixed(2).padStart(8)}% | ` +
        `${result.summary.total_pnl.toFixed(4).padStart(9)} | ` +
        `${result.summary.avg_pnl.toFixed(4).padStart(7)} | ` +
        `${paramsStr}`
      );
    });

    // Visualize best result if requested
    if (options.visualize && topResults.length > 0) {
      console.log('\nBest Result Visualization:');
      const bestResult = topResults[0];
      const bestTrades = JSON.parse(fs.readFileSync(bestResult.filename, 'utf8')).trades;
      this.backtester._visualizeResults(bestTrades);
    }

    return topResults;
  }

  async tuneMultipleAlgorithms(algos, options = {}) {
    const allResults = [];

    for (const algo of algos) {
      try {
        console.log(`\nStarting tuning for ${algo.algo_id}`);
        const results = await this.tuneAlgorithm(algo, options);
        allResults.push({
          algo_id: algo.algo_id,
          title: algo.title,
          top_results: results
        });
      } catch (e) {
        console.error(`Error tuning algo ${algo.algo_id}:`, e);
      }
    }

    // Sort algorithms by best result
    allResults.sort((a, b) => {
      if (a.top_results.length === 0 && b.top_results.length === 0) return 0;
      if (a.top_results.length === 0) return 1;
      if (b.top_results.length === 0) return -1;

      const aBest = a.top_results[0].summary;
      const bBest = b.top_results[0].summary;

      if (aBest.winrate !== bBest.winrate) {
        return bBest.winrate - aBest.winrate;
      }
      return (bBest.total_pnl || 0) - (aBest.total_pnl || 0);
    });

    // Print overall results
    console.log('\nOverall Tuning Results:');
    console.log('--------------------------------------------------------------------------------');
    console.log('Rank | Algo ID               | Title                     | Best Win Rate | Best Total PnL');
    console.log('--------------------------------------------------------------------------------');

    allResults.forEach((result, index) => {
      const bestResult = result.top_results[0] || {};
      const bestSummary = bestResult.summary || {};

      console.log(
        `${(index + 1).toString().padStart(4)} | ` +
        `${result.algo_id.padEnd(22)} | ` +
        `${(result.title || '').padEnd(25)} | ` +
        `${bestSummary.winrate ? (bestSummary.winrate * 100).toFixed(2).padStart(12) + '%' : 'N/A'.padStart(12)} | ` +
        `${bestSummary.total_pnl ? bestSummary.total_pnl.toFixed(4).padStart(14) : 'N/A'.padStart(14)}`
      );
    });

    return allResults;
  }
}

module.exports = AlgorithmTuner;