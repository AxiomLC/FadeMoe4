/**
 * backtester/index.js
 * Main entry point for the backtesting system.
 */

const Backtester = require('./core2');
const AlgorithmTuner = require('./tuner2');
const fs = require('fs');
const path = require('path');

class BacktestingSystem {
  constructor(config = {}) {
    this.backtester = new Backtester(config);
    this.tuner = new AlgorithmTuner(config);
    this.algosSeedPath = path.join(__dirname, 'algos_seed.json');
  }

  async loadAlgorithms() {
    try {
      const seedData = fs.readFileSync(this.algosSeedPath, 'utf8');
      return JSON.parse(seedData);
    } catch (e) {
      console.error('Error loading algorithms:', e);
      return [];
    }
  }

  async runAllSeed(options = {}) {
    const algos = await this.loadAlgorithms();
    if (algos.length === 0) {
      console.log('No algorithms found in seed file');
      return [];
    }

    const results = [];
    for (const algo of algos) {
      try {
        console.log(`Running algo ${algo.algo_id} - ${algo.title}`);
        const res = await this.backtester.runBacktest(algo, {}, options);
        console.log(`  -> summary: ${JSON.stringify(res.summary)}`);
        if (res.recommendations.length) console.log('  -> Recommendations:', res.recommendations);
        results.push(res);
      } catch (e) {
        console.error('Error running algo', algo.algo_id, e);
      }
    }

    return results;
  }

  async tuneAllSeed(options = {}) {
    const algos = await this.loadAlgorithms();
    if (algos.length === 0) {
      console.log('No algorithms found in seed file');
      return [];
    }

    return this.tuner.tuneMultipleAlgorithms(algos, options);
  }

  async compareAlgorithms(options = {}) {
    const algos = await this.loadAlgorithms();
    if (algos.length === 0) {
      console.log('No algorithms found in seed file');
      return [];
    }

    return this.backtester.compareAlgorithms(algos, options);
  }
}

module.exports = BacktestingSystem;