# tune15.js - ComboAlgo Backtester

## Overview
Backtests combinations of 2-4 trading algorithms against historical perpetual futures data. Tests all algo signals must fire within a time window (cascade), then simulates trades with multiple TP/SL configurations. Outputs top performers by Profit Factor or NET$ to console and JSON file.

## Core Workflow
1. **Fetch algo triggers** - Query DB for timestamps when each algo condition met
2. **Cascade algos** - Find timestamps where ALL algos fired within `algoWindow` 
3. **Filter by trade count** - Keep combos with `minTrades` to `maxTrades` triggers
4. **Simulate trades** - Test each combo with all TP/SL combinations
5. **Output results** - Display top performers, export JSON with up to `listAlgos` results

## Key Settings

### TradeSettings
- `minPF` - Minimum Profit Factor threshold (e.g., 1.5 = must win $1.50 for every $1.00 lost)
- `tradeDir` - 'Long', 'Short', or 'Both'
- `tradeSymbol.useAll` - If true, trades all symbols from DB except MT; if false uses `list`
- `trade.tradeWindow` - Exit window in minutes (timeout if TP/SL not hit)
- `trade.posVal` - Position size in dollars per trade
- `trade.tpPerc` - Array of take profit percentages to test
- `trade.slPerc` - Array of stop loss percentages to test
- `minTrades` / `maxTrades` - Filter combos by trigger count range

### ComboAlgos
Define 2-4 algorithms using format: `Symbol; Exchange; Param; Operator; Value`
- **Symbol**: Specific (BTC), list ([BTC,ETH]), or All
- **Exchange**: bin, byb, okx, list, or All  
- **Param**: Metric column name or `[params]` to expand full list
- **Operator**: `<`, `>`, or `<>` (both directions)
- **Value**: Single value, list ([20,40]), or `[corePerc]` to expand

Example: `BTC; bin; v_chg_5m; >; [20]` = BTC volume change 5min > 20%

### AlgoSettings
- `algoWindow` - Time window in minutes where ALL algos must fire (cascade window)
- `algoSymbol.useAll` - If true, expands algo symbols from DB; if false uses `list`
- `corePerc` - Percentage thresholds for `[corePerc]` expansion
- `params` - Full list of available metric parameters for `[params]` expansion

### Output
- `topAlgos` - Number of results to display in console
- `listAlgos` - Number of results to write to JSON file (≥ topAlgos)
- `sortByPF` - If true sorts by Profit Factor; if false sorts by NET$
- `tradeTS` - (Future) Include trade timestamps in output

### SpeedConfig
- `fetchParallel` - Concurrent algo DB queries
- `cascadeParallel` - Concurrent cascade operations
- `simulateParallel` - Concurrent trade simulations
- `batchPriceFetch` - Single query for all symbol prices (faster than per-symbol)

## Key Functions

### `parseAlgo(str)`
Splits algo string into components: symbol, exchange, param, operator, value

### `getAllSymbolsExceptMT()`
Queries `perp_metrics` table for all unique symbols except 'MT' (market index). Returns array of symbol strings for dynamic symbol expansion.

### `expandAlgo(algoStr, algoSymbol, params, corePerc)`
Expands algo definition into all possible combinations. Handles:
- Symbol expansion (All, lists, single)
- Exchange expansion  
- Param expansion (`[params]` keyword)
- Value expansion (`[corePerc]` keyword, lists)
- Operator logic (negates values for `<`, converts `<>` to both + and -)

Returns array of combo objects with normalized operators (`>` or `<`).

### `fetchAlgoTimestamps(combo, startTs, endTs)`
Queries DB for all timestamps where a single algo combo condition is met. Returns array of `{ts, symbol, exchange}` objects.

### `cascadeAlgos(algoTimestamps, algoWindowMs)`
Binary-search based cascade that finds timestamps where ALL algos fired within the window. Takes array of timestamp arrays (one per algo), returns final merged timestamps where all conditions met sequentially within `algoWindowMs`.

Algorithm: Start with algo1 timestamps, for each timestamp find next algo's first match within window, repeat for all algos.

### `simulateTrades(triggers, tradeSymbols, tpPerc, slPerc, tradeWindowMs, tradeDir, posVal)`
Simulates trades for each trigger timestamp:
1. Batch fetches price data for all symbols/triggers
2. For each trigger, finds entry price at trigger time
3. Scans forward until TP hit, SL hit, or window timeout
4. Calculates PnL based on position direction (Long/Short)

Returns stats object: `{trades[], count, netPnL, winRate, timeoutRate, profitFactor}`

### `formatComboAlgo(algoComboArray, stats, tpPerc, slPerc, tradeSymbols, tradeDir)`
Formats result as human-readable string:
`Symbols;Direction;Algo1 + Algo2 + Algo3|TP%|SL%|TradeCount|TimeoutRate|NET$|WinRate|PF`

### `writeJsonOutput(results, metadata)`
Writes JSON file to `tune/` directory with filename format: `tune_YYYY-MM-DD_HH-MM_utc.json`

Structure:
```json
{
  "metadata": {
    "timestamp": "ISO string",
    "runtimeMinutes": "X.XX",
    "settings": {...},
    "algos": [...],
    "totalCombinationsTested": N,
    "totalPassedFilters": N,
    "sortedBy": "PF|NET$"
  },
  "results": [
    {
      "algoCombo": [{symbol, exchange, param, operator, value}, ...],
      "stats": {tradeCount, netPnL, winRate, timeoutRate, profitFactor},
      "tpPercent": X,
      "slPercent": X,
      "formattedString": "..."
    }
  ]
}
```

### `runTune()` - Main Execution
1. Validates ≥2 algos defined
2. Expands each algo into combinations
3. Fetches timestamps for all algo combos (parallel)
4. Generates cartesian product of all algo combinations
5. Cascades each combination to find valid trigger timestamps
6. Filters by minTrades/maxTrades
7. Simulates trades with conservative-first optimization:
   - Sorts TP/SL by conservativeness (highest SL/TP ratio first)
   - Tests conservative params first as fast-fail filter
8. Sorts results by PF or NET$ per user setting
9. Displays top N to console, writes up to listAlgos to JSON

## Optimization Notes

### Conservative-First Testing
TP/SL combinations tested in order of conservativeness (SL/TP ratio descending). If most conservative params fail minPF threshold, remaining combinations skipped for that algo combo. Significantly reduces simulation time when many combos fail.

### Batch Price Fetching
When `batchPriceFetch: true`, fetches all symbol prices in single DB query rather than one query per symbol. Builds in-memory cache for all simulations. Much faster for combos trading multiple symbols.

### Parallel Execution
Uses `p-limit` for controlled concurrency:
- Algo fetching (DB queries)
- Cascade operations (CPU-bound)
- Trade simulations (DB + CPU bound)

## Database Schema Requirements
Expects `perp_metrics` table with columns:
- `ts` (bigint) - Unix timestamp in milliseconds
- `symbol` (text) - Trading pair symbol
- `exchange` (text) - Exchange identifier
- `c` (numeric) - Close price
- Plus metric columns like `v_chg_5m`, `rsi1_chg_1m`, `oi_chg_10m`, etc.

## Output Example
```
1. All;Short;MT_bin_rsi1_chg_1m<20 + BTC_bin_v_chg_5m>20 + BTC_bin_oi_chg_10m<0.7|TP1.5%|SL0.3%|Tr90|TO31%|NET$283|WR40%|PF2.98
```

Reads as: Trade all symbols on Short direction when MT RSI 1min change <20 AND BTC volume 5min change >20 AND BTC open interest 10min change <0.7%, using TP 1.5% / SL 0.3%, resulted in 90 trades with 31% timeouts, net $283 profit, 40% win rate, profit factor 2.98.

## Dependencies
- `p-limit` - For parallel execution control
- `../db/dbsetup` - Database manager with query/close methods
- Node.js `fs/promises` - For JSON file writing