/**
 * backtester/core2.js
 * Enhanced deterministic backtester core with improved performance and features.
 *
 * Features:
 * - Caching for metric data
 * - Parallel execution support
 * - Enhanced trade simulation
 * - Comprehensive performance metrics
 * - Algorithm validation
 * - Result visualization
 */

const fs = require('fs');
const path = require('path');
const dbManager = require('../db/dbsetup');

const DEFAULT_CONFIG = {
  RUN_LOOKBACK_MIN: 60 * 24 * 3, // 3 days default
  OUTPUT_DIR: path.join(__dirname, 'algos'),
  WINRATE_PROMOTE_THRESHOLD: 0.60,
  MAX_POSITIONS: 5,
  DEFAULT_TAKE_PROFITS: [0.005],
  DEFAULT_STOP_LOSS: -0.003,
  DEFAULT_SIDE: 'buy',
  MAX_HOLD_MINUTES: 60,
  CACHE_ENABLED: true
};

class Backtester {
  constructor(config = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this._metricCache = {};
    this._ensureOutputDir();
  }

  _ensureOutputDir() {
    if (!fs.existsSync(this.config.OUTPUT_DIR)) {
      fs.mkdirSync(this.config.OUTPUT_DIR, { recursive: true });
    }
  }

  _timestampIso(ts) {
    return (new Date(ts)).toISOString().replace(/[:.]/g, '-');
  }

  _ensureDir(dir) {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  }

  _buildEvaluator(expr, allowedVars, algoParams = {}) {
    const decls = allowedVars.map(v => {
      const paramValue = algoParams[v] !== undefined ? algoParams[v] : null;
      return `const ${v} = (row['${v}'] !== undefined ? row['${v}'] : ${paramValue});`;
    }).join('\n');

    const helpers = `
      function abs(x) { return Math.abs(x); }
      function max(a, b) { return Math.max(a, b); }
      function min(a, b) { return Math.min(a, b); }
      function avg(...args) { return args.reduce((a, b) => a + b, 0) / args.length; }
      function pctChange(current, previous) {
        return previous !== 0 ? (current - previous) / previous : 0;
      }
    `;

    const fnSrc = `
      ${helpers}
      ${decls}
      return (function(){
        try {
          return Boolean(${expr});
        } catch(e) {
          console.error('Evaluation error:', e.message);
          return false;
        }
      })();
    `;

    return new Function('row', fnSrc);
  }

  async _fetchMetricRows({ perpspec = null, symbol = null, startTs, endTs, cache = true }) {
    const cacheKey = `${perpspec || 'all'}-${symbol || 'all'}-${startTs}-${endTs}`;
    if (cache && this.config.CACHE_ENABLED && this._metricCache[cacheKey]) {
      return this._metricCache[cacheKey];
    }

    const where = [];
    const params = [];
    let idx = 1;

    if (perpspec) {
      where.push(`perpspec = ANY($${idx++})`);
      params.push(Array.isArray(perpspec) ? perpspec : [perpspec]);
    }

    if (symbol) {
      where.push(`symbol = ANY($${idx++})`);
      params.push(Array.isArray(symbol) ? symbol : [symbol]);
    }

    where.push(`ts >= $${idx++}`);
    params.push(BigInt(startTs));
    where.push(`ts <= $${idx++}`);
    params.push(BigInt(endTs));

    const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
    const q = `
      SELECT * FROM perp_metrics
      ${whereClause}
      ORDER BY ts ASC
    `;

    const res = await dbManager.pool.query(q, params);
    const rows = res.rows.map(r => {
      const out = {};
      for (const k of Object.keys(r)) {
        out[k] = (r[k] !== null && typeof r[k] === 'bigint') ? Number(r[k]) : r[k];
        if (typeof out[k] === 'string' && !isNaN(out[k])) {
          const n = Number(out[k]);
          if (!isNaN(n)) out[k] = n;
        }
        if (k === 'ts' && typeof out[k] === 'bigint') out[k] = Number(out[k]);
      }
      return out;
    });

    if (cache && this.config.CACHE_ENABLED) {
      this._metricCache[cacheKey] = rows;
    }

    return rows;
  }

  _validateAlgorithm(algo) {
    if (!algo.algo_id) throw new Error('Algorithm must have an algo_id');
    if (!algo.expr) throw new Error('Algorithm must have an expr');
    if (!algo.tradeSpec) throw new Error('Algorithm must have tradeSpec');

    if (!['buy', 'sell'].includes(algo.tradeSpec.side?.toLowerCase())) {
      throw new Error('tradeSpec.side must be "buy" or "sell"');
    }

    if (!Array.isArray(algo.tradeSpec.take_profits)) {
      throw new Error('tradeSpec.take_profits must be an array');
    }

    if (typeof algo.tradeSpec.stop_loss !== 'number') {
      throw new Error('tradeSpec.stop_loss must be a number');
    }

    if (algo.param_ranges) {
      for (const [param, values] of Object.entries(algo.param_ranges)) {
        if (!Array.isArray(values) || values.length === 0) {
          throw new Error(`param_ranges.${param} must be a non-empty array`);
        }
      }
    }

    return true;
  }

  async _simulateTrades(rows, algo, paramsOverride = {}) {
    const trades = [];
    const allowedVars = rows.length > 0 ? Object.keys(rows[0]).filter(k => typeof rows[0][k] === 'number') : [];
    const evaluator = this._buildEvaluator(algo.expr, allowedVars, paramsOverride);

    const tradeSpec = algo.tradeSpec || {};
    const tpArr = tradeSpec.take_profits || this.config.DEFAULT_TAKE_PROFITS;
    const sl = tradeSpec.stop_loss || this.config.DEFAULT_STOP_LOSS;
    const side = tradeSpec.side ? tradeSpec.side.toLowerCase() : this.config.DEFAULT_SIDE;
    const maxHoldMinutes = tradeSpec.max_hold_minutes || this.config.MAX_HOLD_MINUTES;

    const openPositions = [];

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];

      // Check for position exits first
      for (let j = openPositions.length - 1; j >= 0; j--) {
        const pos = openPositions[j];
        const p = row.c;
        if (!p) continue;

        const ret = side === 'buy' ? (p - pos.entryPrice) / pos.entryPrice : (pos.entryPrice - p) / pos.entryPrice;

        // Check TPs first
        let tpHit = null;
        for (let k = 0; k < tpArr.length; k++) {
          if (ret >= tpArr[k]) {
            tpHit = { tp: tpArr[k], tpIndex: k };
            break;
          }
        }

        if (tpHit) {
          trades.push({
            algo_id: algo.algo_id,
            ts_entry: pos.ts,
            ts_exit: row.ts,
            entry_price: pos.entryPrice,
            exit_price: p,
            pnl: ret,
            result: { type: 'TP', tp: tpHit.tp, tpIndex: tpHit.tpIndex }
          });
          openPositions.splice(j, 1);
          continue;
        }

        // Check SL
        if (ret <= sl) {
          trades.push({
            algo_id: algo.algo_id,
            ts_entry: pos.ts,
            ts_exit: row.ts,
            entry_price: pos.entryPrice,
            exit_price: p,
            pnl: ret,
            result: { type: 'SL', sl: sl }
          });
          openPositions.splice(j, 1);
          continue;
        }

        // Check max hold time
        if ((row.ts - pos.ts) > maxHoldMinutes * 60 * 1000) {
          trades.push({
            algo_id: algo.algo_id,
            ts_entry: pos.ts,
            ts_exit: row.ts,
            entry_price: pos.entryPrice,
            exit_price: p,
            pnl: ret,
            result: { type: 'TIMEOUT' }
          });
          openPositions.splice(j, 1);
        }
      }

      // Check for new positions if we have room
      if (openPositions.length >= this.config.MAX_POSITIONS) continue;

      let trigger = false;
      try {
        trigger = evaluator(row);
      } catch (e) {
        trigger = false;
      }

      if (!trigger) continue;

      // Find entry price
      let entryPrice = row.c;
      if (entryPrice === null || entryPrice === undefined) {
        // Look ahead up to 3 rows
        for (let j = i + 1; j < Math.min(rows.length, i + 4); j++) {
          if (rows[j].c) {
            entryPrice = rows[j].c;
            break;
          }
        }
      }

      if (!entryPrice) continue;

      // Open new position
      openPositions.push({
        ts: row.ts,
        entryPrice: entryPrice,
        rowIndex: i
      });
    }

    // Close any remaining open positions at the last price
    const lastPrice = rows.length > 0 ? rows[rows.length - 1].c : null;
    if (lastPrice) {
      for (const pos of openPositions) {
        const ret = side === 'buy' ? (lastPrice - pos.entryPrice) / pos.entryPrice : (pos.entryPrice - lastPrice) / pos.entryPrice;
        trades.push({
          algo_id: algo.algo_id,
          ts_entry: pos.ts,
          ts_exit: rows[rows.length - 1].ts,
          entry_price: pos.entryPrice,
          exit_price: lastPrice,
          pnl: ret,
          result: { type: 'CLOSE' }
        });
      }
    }

    return trades;
  }

  _summarizeTrades(trades) {
    if (!trades || trades.length === 0) {
      return {
        trades_count: 0,
        winrate: 0,
        avg_pnl: 0,
        total_pnl: 0,
        max_drawdown: 0,
        sharpe_ratio: 0,
        avg_trade_duration: 0,
        win_streak: 0,
        lose_streak: 0,
        profit_factor: 0,
        avg_win_pnl: 0,
        avg_loss_pnl: 0
      };
    }

    const wins = trades.filter(t => t.pnl > 0);
    const losses = trades.filter(t => t.pnl < 0);
    const avgPnl = trades.reduce((s, t) => s + t.pnl, 0) / trades.length;
    const totalPnl = trades.reduce((s, t) => s + t.pnl, 0);

    // Calculate max drawdown
    let maxDrawdown = 0;
    let peak = 0;
    let cumulativePnl = 0;
    for (const trade of trades) {
      cumulativePnl += trade.pnl;
      if (cumulativePnl > peak) peak = cumulativePnl;
      const drawdown = peak - cumulativePnl;
      if (drawdown > maxDrawdown) maxDrawdown = drawdown;
    }

    // Calculate Sharpe ratio (simplified)
    const avgDailyPnl = totalPnl / (trades.length / (24 * 60)); // Assuming 1 trade per minute
    const stdDev = Math.sqrt(trades.reduce((s, t) => s + Math.pow(t.pnl - avgPnl, 2), 0) / trades.length);
    const sharpeRatio = stdDev > 0 ? avgDailyPnl / stdDev : 0;

    // Calculate average trade duration
    const avgDuration = trades.reduce((s, t) => s + (t.ts_exit - t.ts_entry), 0) / trades.length;

    // Calculate streaks
    let winStreak = 0;
    let loseStreak = 0;
    let currentWinStreak = 0;
    let currentLoseStreak = 0;

    for (const trade of trades) {
      if (trade.pnl > 0) {
        currentWinStreak++;
        currentLoseStreak = 0;
        if (currentWinStreak > winStreak) winStreak = currentWinStreak;
      } else {
        currentLoseStreak++;
        currentWinStreak = 0;
        if (currentLoseStreak > loseStreak) loseStreak = currentLoseStreak;
      }
    }

    // Calculate profit factor
    const grossProfit = wins.reduce((s, t) => s + t.pnl, 0);
    const grossLoss = losses.reduce((s, t) => s + Math.abs(t.pnl), 0);
    const profitFactor = grossLoss > 0 ? grossProfit / grossLoss : grossProfit;

    // Calculate average win/loss pnl
    const avgWinPnl = wins.length > 0 ? grossProfit / wins.length : 0;
    const avgLossPnl = losses.length > 0 ? grossLoss / losses.length : 0;

    return {
      trades_count: trades.length,
      winrate: wins.length / trades.length,
      avg_pnl: avgPnl,
      total_pnl: totalPnl,
      max_drawdown: maxDrawdown,
      sharpe_ratio: sharpeRatio,
      avg_trade_duration: avgDuration,
      win_streak: winStreak,
      lose_streak: loseStreak,
      profit_factor: profitFactor,
      avg_win_pnl: avgWinPnl,
      avg_loss_pnl: avgLossPnl
    };
  }

  _visualizeResults(trades, options = {}) {
    if (trades.length === 0) return;

    // Calculate cumulative PnL
    const cumulativePnl = [];
    let runningTotal = 0;
    for (const trade of trades) {
      runningTotal += trade.pnl;
      cumulativePnl.push(runningTotal);
    }

    // Simple ASCII chart
    const maxPnl = Math.max(...cumulativePnl);
    const minPnl = Math.min(...cumulativePnl);
    const range = maxPnl - minPnl;
    const chartHeight = 10;

    console.log('\nTrade Performance Visualization:');
    console.log('--------------------------------');

    for (let i = chartHeight; i >= 0; i--) {
      const level = minPnl + (i / chartHeight) * range;
      let line = `${level.toFixed(4).padStart(8)} | `;

      for (let j = 0; j < cumulativePnl.length; j++) {
        const pnl = cumulativePnl[j];
        const normalized = (pnl - minPnl) / range;
        const pos = Math.floor(normalized * chartHeight);

        if (pos === i) {
          line += '*';
        } else {
          line += ' ';
        }
      }

      console.log(line);
    }

    console.log(''.padStart(10) + '+' + '-'.repeat(cumulativePnl.length));
    console.log(''.padStart(10) + '0' + ''.padStart(cumulativePnl.length - 1, ' ') + trades.length);

    // Print key metrics
    const summary = this._summarizeTrades(trades);
    console.log('\nKey Metrics:');
    console.log(`- Win Rate: ${(summary.winrate * 100).toFixed(2)}%`);
    console.log(`- Total PnL: ${summary.total_pnl.toFixed(4)}`);
    console.log(`- Max Drawdown: ${summary.max_drawdown.toFixed(4)}`);
    console.log(`- Sharpe Ratio: ${summary.sharpe_ratio.toFixed(2)}`);
    console.log(`- Profit Factor: ${summary.profit_factor.toFixed(2)}`);
    console.log(`- Avg Win PnL: ${summary.avg_win_pnl.toFixed(4)}`);
    console.log(`- Avg Loss PnL: ${summary.avg_loss_pnl.toFixed(4)}`);
  }

  async runBacktest(algo, params = {}, opts = {}) {
    this._validateAlgorithm(algo);

    const now = Date.now();
    const endTs = now;
    const startTs = now - (this.config.RUN_LOOKBACK_MIN * 60 * 1000);

    // Fetch rows (optionally filter by symbol/perpspec)
    const rows = await this._fetchMetricRows({
      perpspec: algo.perpspec || null,
      symbol: algo.symbol || null,
      startTs,
      endTs,
      cache: opts.cache !== false
    });

    const trades = await this._simulateTrades(rows, algo, params);
    const summary = this._summarizeTrades(trades);

    // Prepare output file
    const isoWeek = (new Date()).toISOString().slice(0, 10);
    const weekFolder = path.join(this.config.OUTPUT_DIR, `week-${isoWeek}`);
    this._ensureDir(weekFolder);

    const filename = path.join(weekFolder, `${algo.algo_id}--${this._timestampIso(Date.now())}.json`);
    const out = {
      meta: {
        algo_id: algo.algo_id,
        title: algo.title,
        params,
        startTs,
        endTs,
        created_at: new Date().toISOString()
      },
      summary,
      trades
    };

    fs.writeFileSync(filename, JSON.stringify(out, null, 2));

    // If hot candidates exist, add recommendations
    const recs = [];
    if (summary.winrate >= this.config.WINRATE_PROMOTE_THRESHOLD && summary.trades_count >= 5) {
      recs.push({
        reason: `winrate>=${this.config.WINRATE_PROMOTE_THRESHOLD}`,
        algo_id: algo.algo_id,
        filename
      });
    }

    // Visualize results if requested
    if (opts.visualize !== false) {
      this._visualizeResults(trades);
    }

    return { summary, trades, filename, recommendations: recs };
  }

  async compareAlgorithms(algos, options = {}) {
    const results = [];

    for (const algo of algos) {
      try {
        console.log(`Running comparison for ${algo.algo_id}`);
        const res = await this.runBacktest(algo, {}, { visualize: false });
        results.push({
          algo_id: algo.algo_id,
          title: algo.title,
          summary: res.summary,
          filename: res.filename
        });
      } catch (e) {
        console.error(`Error comparing algo ${algo.algo_id}:`, e);
      }
    }

    // Sort by winrate, then total_pnl
    results.sort((a, b) => {
      if (b.summary.winrate !== a.summary.winrate) {
        return b.summary.winrate - a.summary.winrate;
      }
      return (b.summary.total_pnl || 0) - (a.summary.total_pnl || 0);
    });

    // Print comparison table
    console.log('\nAlgorithm Comparison:');
    console.log('--------------------------------------------------------------------------------');
    console.log('Rank | Algo ID               | Title                     | Win Rate | Total PnL | Avg PnL');
    console.log('--------------------------------------------------------------------------------');

    results.forEach((result, index) => {
      console.log(
        `${(index + 1).toString().padStart(4)} | ` +
        `${result.algo_id.padEnd(22)} | ` +
        `${(result.title || '').padEnd(25)} | ` +
        `${(result.summary.winrate * 100).toFixed(2).padStart(8)}% | ` +
        `${result.summary.total_pnl.toFixed(4).padStart(9)} | ` +
        `${result.summary.avg_pnl.toFixed(4).padStart(7)}`
      );
    });

    return results;
  }
}

module.exports = Backtester;