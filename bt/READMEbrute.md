# brute15.js - Core Pattern Discovery Scanner

## Overview
Brute-force scanner that tests single-parameter conditions across all exchanges, timeframes, and thresholds to discover profitable trading patterns. Unlike tune15.js (which tests combinations of multiple algos), brute15 tests individual parameters exhaustively to find the best standalone signals.

## Core Workflow
1. **Generate candidates** - Create all possible combinations of: direction × exchange × param × threshold
2. **Batch time ranges** - Split historical data into manageable time chunks
3. **Filter data** - For each candidate, filter data where condition is met
4. **Early validation** - Skip if filtered data fails min/maxTrades count
5. **Simulate trades** - Test remaining candidates with conservative TP/SL first
6. **Calculate stats** - Track PF, NET$, win rate, timeout rate for each result
7. **Output results** - Display top performers, export JSON with all profitable patterns

## Key Settings

### Detection
- `minTrades` - Minimum number of trades required (e.g., 100)
- `maxTrades` - Maximum number of trades allowed (e.g., 1500)
- `minPF` - Minimum Profit Factor threshold to consider algo profitable (e.g., 1.0)

### Trade Execution
- `tradeWindow` - Exit window in minutes (timeout if TP/SL not hit)
- `posVal` - Position size in dollars per trade
- `tpPerc` - Array of take profit percentages to test (e.g., [1, 1.5, 1.9])
- `slPerc` - Array of stop loss percentages to test (e.g., [0.2, 0.4, 0.6])

### Target Symbols
- `useAll` - If true, tests all symbols from `perp_metrics` except MT
- `list` - If useAll false, tests only symbols in this array

### Core Settings
- `corePercent` - Threshold values to test (e.g., [0.5, 1.5, 2.5, 5, 10, 30, 60, 100])
- `params` - List of metric parameters to test (e.g., v_chg_5m, rsi1_chg_1m, oi_chg_10m)
- `exchanges` - Exchanges to test: bin (Binance), byb (Bybit), okx (OKX)
- `coreDir` - 'Long', 'Short', or 'Both' directions

### Output
- `topResults` - Number of results to display in console (e.g., 20)
- `listResults` - Number of results to write to JSON file (e.g., 20)
- `sortByPF` - If true sorts by Profit Factor; if false sorts by NET$

### Performance
- `concurrencyLimit` - Parallel candidate tests within each batch
- `batchSizeMinutes` - Time range chunk size (default 1440 = 1 day)
- `chunkConcurrency` - Number of batches processed in parallel
- `enablePriceCache` - Cache price data to reduce DB queries

## Key Functions

### `generateCandidates()`
Creates exhaustive list of single-param algos to test:
- For each direction (Long/Short or both)
- For each exchange (bin, byb, okx)
- For each param (v_chg_5m, rsi1_chg_1m, etc.)
- For each threshold in corePercent (positive for `>`, negative for `<`)

Returns array of candidate objects: `{direction, exchange, param, operator, threshold, algoString}`

Example candidates:
- `bin_v_chg_5m>5` (Binance volume 5min change > 5%)
- `okx_rsi1_chg_1m<-20` (OKX RSI 1min change < -20)

### `fetchBatchData(symbols, exchanges, startTs, endTs)`
Fetches all metric data for specified symbols/exchanges within time range. Returns array of row objects with timestamp, symbol, exchange, close price, and all param values.

Single query fetches all data for batch - highly efficient for bulk processing.

### `testCandidates(data, candidates)`
Main testing engine that processes all candidates against batch data:

1. **Filters data** for each candidate's condition (param > threshold or param < threshold)
2. **Early exit** if filtered count fails min/maxTrades
3. **Conservative-first TP/SL testing**:
   - Sorts TP/SL pairs by conservativeness (SL/TP ratio descending)
   - Tests most conservative first (highest SL, lowest TP)
   - Skips remaining if conservative fails minPF threshold
4. **Calculates score** for each valid TP/SL combo:
   - `Score = (WinRate/100 × 30) + (PF × 40) + (min(trades/maxTrades, 1) × 30)`
5. **Tracks best scheme** for each candidate

Returns array of profitable results (PF > minPF) with best TP/SL for each.

### `filterData(data, candidate)`
Filters batch data to rows where candidate condition is met:
- Matches candidate's exchange
- Checks param value exists and is valid
- Evaluates operator condition (> or <)

Returns subset of data array matching condition.

### `simulateTrades(entries, tp, sl, direction, exchange, cache)`
Simulates realistic trade execution for each entry signal:

**Entry Logic:**
- Signal detected at `entryTs`
- Trade enters at `nextTs = entryTs + 60000` (next minute after signal)
- Avoids look-ahead bias - realistic delay between signal and execution

**Exit Logic:**
- Scans price data from `nextTs` to `nextTs + tradeWindow`
- Exits on first occurrence of:
  - **TP hit** - Price moves favorably by `tpPerc`
  - **SL hit** - Price moves unfavorably by `slPerc`
  - **Timeout** - Window expires, exit at last available price

**Direction Handling:**
- **Long**: Profit when price rises, loss when price falls
- **Short**: Profit when price falls, loss when price rises

**Performance Optimization:**
- Groups entries by symbol
- Batch fetches price data per symbol (one query per symbol per batch)
- Caches price data by symbol-exchange-timerange key
- Reuses cached data across multiple candidates trading same symbol

Returns array of trade objects: `{symbol, entryTs, exitTs, pnl, exitType}`

### `calculateStats(trades)`
Computes performance metrics from trade array:
- **winRate** - Percentage of profitable trades
- **timeoutRate** - Percentage of trades that timed out (neither TP nor SL hit)
- **pf** (Profit Factor) - Gross profit / Gross loss (999 if no losses)
- **netPnl** - Total profit minus total loss in dollars
- **avgPnl** - Average PnL per trade
- **wins** - Count of winning trades
- **losses** - Count of losing trades
- **timeouts** - Count of timeout exits

Returns stats object with all metrics.

### `generateOutput(topResults, allResults, startTime, totalCandidates)`
Creates JSON output structure with:
- **metadata**: Script info, timestamp, runtime, full config
- **summary**: Overview text, score formula, sorting method, top 15 formatted results
- **results**: Detailed array of profitable patterns (up to `listResults` count)

Each result includes:
- Formatted algo string (e.g., "ALL;Long;bin_v_chg_5m>5")
- Direction, exchange, param, threshold
- Trade scheme (TP%, SL%, tradeWindow, posVal)
- Full stats (trades, winRate, timeoutRate, PF, NET$, etc.)
- Calculated score

### `formatAlgo(result)`
Formats candidate into human-readable string:
`{symbols};{direction};{exchange}_{param}{operator}{threshold}`

Examples:
- `ALL;Short;bin_rsi1_chg_1m<-20`
- `ETH,BTC;Long;okx_v_chg_5m>10`

## How Scoring Works

Each profitable pattern receives a composite score balancing three factors:

```
Score = (WinRate/100 × 30) + (PF × 40) + (min(trades/maxTrades, 1) × 30)
```

- **Win Rate Component (30%)**: Rewards consistency (higher win rate)
- **Profit Factor Component (40%)**: Rewards efficiency (higher profit/loss ratio)  
- **Trade Count Component (30%)**: Rewards sufficient sample size (more trades up to max)

This prevents over-optimization on rare patterns with few trades while favoring reliable, well-tested signals.

## Optimization Strategies

### Conservative-First Testing
TP/SL combinations tested in order of conservativeness (SL/TP ratio descending):
- Tests highest SL / lowest TP first (most likely to pass minPF)
- If conservative params fail minPF, skips remaining TP/SL for that candidate
- Dramatically reduces simulation time when many patterns fail threshold

### Batch Processing
Time range split into chunks (default 1440 min = 1 day):
- Fetches data once per batch, tests all candidates against it
- Parallelizes batch processing (controlled by `chunkConcurrency`)
- Reduces DB round-trips compared to per-candidate fetching

### Early Exit on Trade Count
Before running expensive trade simulation:
- Filters data by candidate condition
- Checks if filtered count is within min/maxTrades range
- Skips simulation if count invalid (saves DB queries and computation)

### Price Data Caching
Caches fetched price data by `symbol-exchange-minTs-maxTs` key:
- Multiple candidates trading same symbol reuse cached prices
- Eliminates redundant DB queries
- Especially effective with many candidates per symbol

### Parallel Execution
Uses `p-limit` for controlled concurrency:
- Batch-level parallelism (`chunkConcurrency`)
- Candidate-level parallelism within batch (`concurrencyLimit`)
- Prevents DB connection exhaustion while maximizing throughput

## Database Schema Requirements
Expects `perp_metrics` table with columns:
- `ts` (bigint) - Unix timestamp in milliseconds
- `symbol` (text) - Trading pair symbol (BTC, ETH, etc.)
- `exchange` (text) - Exchange identifier (bin, byb, okx)
- `c` (numeric) - Close price
- Metric columns: `v_chg_5m`, `rsi1_chg_1m`, `oi_chg_10m`, `pfr_chg_5m`, etc.

**Note:** MT symbol is excluded when `targetSymbols.useAll = true` (MT is a market index, not tradeable).

## Output Example

### Console Output
```
🏆 TOP 15 BY PROFIT FACTOR:
1. ALL;Long;bin_v_chg_5m>10|TP1.5%|SL0.4%|PF3.25|WR58%|NET$4521|TO22%|Tr342
2. ALL;Short;okx_rsi1_chg_1m<-20|TP1.9%|SL0.2%|PF2.87|WR52%|NET$3890|TO18%|Tr298
```

Reads as: Trade all symbols Long when Binance volume 5min change >10%, using TP 1.5% / SL 0.4%, resulted in 342 trades with 22% timeouts, net $4521 profit, 58% win rate, profit factor 3.25.

### JSON Output Structure
```json
{
  "metadata": {
    "script": "brute15.js",
    "timestamp": "2025-11-21T...",
    "runtime": "12.3 minutes",
    "config": { /* full config */ }
  },
  "summary": {
    "overview": "Tested X thresholds × Y params × Z exchanges × N directions = M total tests, K profitable patterns found",
    "scoreFormula": "Score = (WinRate/100 * 30) + (PF * 40) + (min(trades/maxTrades, 1) * 30)",
    "sortedBy": "PF",
    "topResults": [ /* top 15 formatted */ ]
  },
  "results": [
    {
      "algo": "ALL;Long;bin_v_chg_5m>10",
      "direction": "Long",
      "exchange": "bin",
      "param": "v_chg_5m",
      "threshold": 10,
      "tradeScheme": { "tp": 1.5, "sl": 0.4, "tradeWindow": 30, "posVal": 1000 },
      "stats": {
        "trades": 342,
        "winRate": 58.2,
        "timeoutRate": 22.1,
        "pf": 3.25,
        "netPnl": 4521.50,
        "avgPnl": 13.22,
        "wins": 199,
        "losses": 143,
        "timeouts": 76
      },
      "score": 149.35
    }
  ]
}
```

## Use Cases

### Pattern Discovery
Find best-performing individual indicators across all markets:
- Which RSI thresholds work best for each exchange?
- Which volume spikes predict price movement?
- Which open interest changes signal reversals?

### Parameter Optimization
Test ranges of thresholds to find optimal values:
- Is v_chg_5m > 5% better than > 10%?
- Does rsi1_chg_1m < -20 outperform < -30?

### Exchange Comparison
Compare same indicator across different exchanges:
- Does Binance volume predict better than OKX?
- Which exchange has most reliable liquidation signals?

### Direction Testing
Determine if indicators work better Long vs Short:
- Some patterns may be profitable only in one direction
- `coreDir: 'Both'` tests both to find best fit

### Building Block for tune15
Brute15 finds individual signals. Best performers can be combined in tune15:
1. Run brute15 to discover top individual patterns
2. Select complementary patterns (different params/exchanges/timeframes)
3. Feed into tune15 as ComboAlgos to test combinations
4. Find synergistic multi-algo strategies

## Dependencies
- `p-limit` - For parallel execution control
- `../db/dbsetup` - Database manager with pool.query() method
- Node.js `fs` and `path` - For JSON file writing

## Performance Notes
- **Typical runtime**: 5-20 minutes for 3-day data range with ~1000 candidates
- **Bottleneck**: Database queries (price fetching during simulation)
- **Speedup tips**: 
  - Enable `enablePriceCache: true`
  - Increase `chunkConcurrency` if DB can handle it
  - Reduce `batchSizeMinutes` for more granular parallelism
  - Limit `corePercent` array to most relevant thresholds