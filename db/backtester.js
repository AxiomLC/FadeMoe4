// SCRIPT: backtester.js
// Example backtester that tests trading strategies using pre-calculated metrics
// Tests strategies like: IF oi_chg_5m > 1.12% AND lsr_10m < 0.08% THEN price drops 1.16% in 5min

const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');

const SCRIPT_NAME = 'backtester.js';

// ============================================================================
// EXAMPLE STRATEGY DEFINITION
// ============================================================================
const EXAMPLE_STRATEGY = {
  name: "OI Spike + Low LSR → Price Drop",
  description: "When OI increases sharply and LSR is low, price tends to drop",
  
  // Conditions that must be TRUE to trigger signal
  conditions: [
    { param: 'oi_chg_5m', operator: '>', threshold: 1.12 },
    { param: 'lsr_chg_10m', operator: '<', threshold: 0.08 },
    { param: 'v_chg_5m', operator: '>', threshold: 1.0 }
  ],
  
  // What we're predicting
  prediction: {
    param: 'c_chg_5m',      // Price change
    direction: 'down',       // 'up' or 'down'
    magnitude: 1.16,         // Expected % move
    timeframe: '5m'          // How many minutes ahead
  },
  
  // Configuration
  symbols: ['BTC', 'ETH', 'SOL'],  // Which symbols to test
  perpspec: 'bin-ohlcv',            // Which exchange data to use
  minSamples: 10                    // Minimum trades needed for valid backtest
};

// ============================================================================
// STRATEGY EVALUATION FUNCTIONS
// ============================================================================

// Check if a row meets all strategy conditions
function meetsConditions(row, conditions) {
  for (const condition of conditions) {
    const value = row[condition.param];
    
    // Skip if value is null
    if (value === null || value === undefined) {
      return false;
    }

    // Evaluate condition
    switch (condition.operator) {
      case '>':
        if (!(value > condition.threshold)) return false;
        break;
      case '<':
        if (!(value < condition.threshold)) return false;
        break;
      case '>=':
        if (!(value >= condition.threshold)) return false;
        break;
      case '<=':
        if (!(value <= condition.threshold)) return false;
        break;
      case '==':
        if (!(Math.abs(value - condition.threshold) < 0.001)) return false;
        break;
      default:
        return false;
    }
  }
  
  return true;
}

// Check if prediction was correct
function checkPrediction(currentRow, futureRow, prediction) {
  if (!futureRow) return null; // Can't verify if no future data
  
  const actualChange = futureRow[prediction.param];
  if (actualChange === null || actualChange === undefined) return null;

  // Check if move was in predicted direction and magnitude
  if (prediction.direction === 'down') {
    // Predicted drop of at least prediction.magnitude
    return actualChange <= -prediction.magnitude;
  } else if (prediction.direction === 'up') {
    // Predicted rise of at least prediction.magnitude
    return actualChange >= prediction.magnitude;
  }

  return null;
}

// ============================================================================
// BACKTESTER CORE
// ============================================================================
async function runBacktest(strategy, startTs, endTs) {
  console.log(`\n🔬 Running backtest: ${strategy.name}`);
  console.log(`📅 Period: ${new Date(startTs).toISOString()} to ${new Date(endTs).toISOString()}`);
  console.log(`📊 Symbols: ${strategy.symbols.join(', ')}`);
  console.log(`📍 Perpspec: ${strategy.perpspec}`);

  // Fetch metrics data for the backtest period
  console.log('\n📥 Fetching metrics data...');
  const metricsData = await dbManager.queryMetrics(
    strategy.symbols,
    [strategy.perpspec],
    startTs,
    endTs
  );

  if (metricsData.length === 0) {
    console.log('❌ No data found for backtest period');
    return null;
  }

  console.log(`✅ Loaded ${metricsData.length} data points`);

  // Organize data by symbol
  const dataBySymbol = {};
  for (const row of metricsData) {
    if (!dataBySymbol[row.symbol]) {
      dataBySymbol[row.symbol] = [];
    }
    dataBySymbol[row.symbol].push(row);
  }

  // Sort each symbol's data by timestamp
  for (const symbol in dataBySymbol) {
    dataBySymbol[symbol].sort((a, b) => a.ts - b.ts);
  }

  // Run backtest for each symbol
  const results = {
    strategy: strategy.name,
    symbols: {},
    overall: {
      totalSignals: 0,
      totalTrades: 0,
      wins: 0,
      losses: 0,
      accuracy: 0,
      avgWin: 0,
      avgLoss: 0,
      sharpeRatio: 0
    }
  };

  console.log('\n🧪 Testing strategy conditions...\n');

  for (const symbol of strategy.symbols) {
    const symbolData = dataBySymbol[symbol] || [];
    
    if (symbolData.length === 0) {
      console.log(`⚠️  ${symbol}: No data available`);
      continue;
    }

    const trades = [];
    let wins = 0;
    let losses = 0;
    let totalWinPct = 0;
    let totalLossPct = 0;

    // Iterate through data points
    for (let i = 0; i < symbolData.length; i++) {
      const currentRow = symbolData[i];

      // Check if conditions are met
      if (meetsConditions(currentRow, strategy.conditions)) {
        // Look ahead to verify prediction
        const timeframeMinutes = parseInt(strategy.prediction.timeframe);
        const futureRow = symbolData[i + timeframeMinutes];

        const predictionCorrect = checkPrediction(currentRow, futureRow, strategy.prediction);

        if (predictionCorrect !== null) {
          const actualChange = futureRow[strategy.prediction.param];
          
          trades.push({
            ts: currentRow.ts,
            predictedDirection: strategy.prediction.direction,
            actualChange: actualChange,
            correct: predictionCorrect
          });

          if (predictionCorrect) {
            wins++;
            totalWinPct += Math.abs(actualChange);
          } else {
            losses++;
            totalLossPct += Math.abs(actualChange);
          }
        }
      }
    }

    const totalTrades = wins + losses;
    const accuracy = totalTrades > 0 ? (wins / totalTrades * 100) : 0;
    const avgWin = wins > 0 ? (totalWinPct / wins) : 0;
    const avgLoss = losses > 0 ? (totalLossPct / losses) : 0;

    results.symbols[symbol] = {
      totalTrades,
      wins,
      losses,
      accuracy: accuracy.toFixed(2),
      avgWin: avgWin.toFixed(3),
      avgLoss: avgLoss.toFixed(3),
      trades: trades
    };

    results.overall.totalTrades += totalTrades;
    results.overall.wins += wins;
    results.overall.losses += losses;

    // Display results for this symbol
    if (totalTrades >= strategy.minSamples) {
      console.log(`✅ ${symbol}: ${wins}/${totalTrades} (${accuracy.toFixed(1)}%) | Avg Win: ${avgWin.toFixed(2)}% | Avg Loss: ${avgLoss.toFixed(2)}%`);
    } else {
      console.log(`⚠️  ${symbol}: ${totalTrades} trades (min ${strategy.minSamples} required)`);
    }
  }

  // Calculate overall statistics
  if (results.overall.totalTrades > 0) {
    results.overall.accuracy = (results.overall.wins / results.overall.totalTrades * 100).toFixed(2);
    
    // Calculate Sharpe-like ratio (simplified)
    const winRate = results.overall.wins / results.overall.totalTrades;
    const avgWinOverall = results.overall.wins > 0 ? 
      (Object.values(results.symbols).reduce((sum, s) => sum + parseFloat(s.avgWin) * s.wins, 0) / results.overall.wins) : 0;
    const avgLossOverall = results.overall.losses > 0 ?
      (Object.values(results.symbols).reduce((sum, s) => sum + parseFloat(s.avgLoss) * s.losses, 0) / results.overall.losses) : 0;
    
    results.overall.avgWin = avgWinOverall.toFixed(3);
    results.overall.avgLoss = avgLossOverall.toFixed(3);
    results.overall.sharpeRatio = avgLossOverall > 0 ? (avgWinOverall / avgLossOverall).toFixed(2) : 0;
  }

  console.log('\n' + '='.repeat(70));
  console.log('📊 OVERALL RESULTS');
  console.log('='.repeat(70));
  console.log(`Total Trades: ${results.overall.totalTrades}`);
  console.log(`Wins: ${results.overall.wins} | Losses: ${results.overall.losses}`);
  console.log(`Accuracy: ${results.overall.accuracy}%`);
  console.log(`Avg Win: ${results.overall.avgWin}% | Avg Loss: ${results.overall.avgLoss}%`);
  console.log(`Win/Loss Ratio: ${results.overall.sharpeRatio}`);
  console.log('='.repeat(70) + '\n');

  return results;
}

// ============================================================================
// STRATEGY ADJUSTMENT HELPER
// ============================================================================
function adjustStrategy(baseStrategy, adjustments) {
  const newStrategy = JSON.parse(JSON.stringify(baseStrategy)); // Deep clone
  
  // Apply adjustments
  for (const adj of adjustments) {
    if (adj.type === 'condition') {
      const condition = newStrategy.conditions.find(c => c.param === adj.param);
      if (condition) {
        if (adj.threshold !== undefined) condition.threshold = adj.threshold;
        if (adj.operator !== undefined) condition.operator = adj.operator;
      }
    } else if (adj.type === 'prediction') {
      if (adj.magnitude !== undefined) newStrategy.prediction.magnitude = adj.magnitude;
      if (adj.direction !== undefined) newStrategy.prediction.direction = adj.direction;
    }
  }
  
  return newStrategy;
}

// ============================================================================
// PARAMETER SWEEP (Test multiple threshold combinations)
// ============================================================================
async function parameterSweep(baseStrategy, startTs, endTs) {
  console.log('\n🔄 Running parameter sweep...\n');

  const oiThresholds = [0.8, 1.0, 1.12, 1.5, 2.0];
  const lsrThresholds = [0.05, 0.08, 0.10, 0.12];
  const volThresholds = [0.5, 1.0, 1.5, 2.0];

  const sweepResults = [];

  for (const oiThreshold of oiThresholds) {
    for (const lsrThreshold of lsrThresholds) {
      for (const volThreshold of volThresholds) {
        const strategy = adjustStrategy(baseStrategy, [
          { type: 'condition', param: 'oi_chg_5m', threshold: oiThreshold },
          { type: 'condition', param: 'lsr_chg_10m', threshold: lsrThreshold },
          { type: 'condition', param: 'v_chg_5m', threshold: volThreshold }
        ]);

        strategy.name = `OI>${oiThreshold}% LSR<${lsrThreshold}% VOL>${volThreshold}%`;

        const results = await runBacktest(strategy, startTs, endTs);
        
        if (results && results.overall.totalTrades >= baseStrategy.minSamples) {
          sweepResults.push({
            params: { oiThreshold, lsrThreshold, volThreshold },
            accuracy: parseFloat(results.overall.accuracy),
            totalTrades: results.overall.totalTrades,
            sharpe: parseFloat(results.overall.sharpeRatio)
          });
        }
      }
    }
  }

  // Sort by accuracy
  sweepResults.sort((a, b) => b.accuracy - a.accuracy);

  console.log('\n📈 TOP 10 PARAMETER COMBINATIONS:\n');
  for (let i = 0; i < Math.min(10, sweepResults.length); i++) {
    const result = sweepResults[i];
    console.log(`${i + 1}. OI>${result.params.oiThreshold}% LSR<${result.params.lsrThreshold}% VOL>${result.params.volThreshold}% → ${result.accuracy}% (${result.totalTrades} trades, Sharpe: ${result.sharpe})`);
  }

  return sweepResults;
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================
if (require.main === module) {
  async function main() {
    try {
      // Backtest period: last 7 days
      const endTs = Date.now();
      const startTs = endTs - (7 * 24 * 60 * 60 * 1000);

      // Run single backtest with example strategy
      console.log('🚀 Starting backtester...\n');
      const results = await runBacktest(EXAMPLE_STRATEGY, startTs, endTs);

      // Optionally run parameter sweep (uncomment to enable)
      // const sweepResults = await parameterSweep(EXAMPLE_STRATEGY, startTs, endTs);

      console.log('\n✅ Backtest complete!');
      process.exit(0);

    } catch (error) {
      console.error('💥 Backtest failed:', error);
      process.exit(1);
    }
  }

  main();
}

/*  ======= SECTION to connect for jsonb / UI ==============
// Already returns this structure - just needs:
await dbManager.pool.query(
  `INSERT INTO backtest_strategies (name, conditions, prediction, results)
   VALUES ($1, $2, $3, $4)`,
  [strategy.name, strategy.conditions, strategy.prediction, results]
);
*/

module.exports = { runBacktest, adjustStrategy, parameterSweep };