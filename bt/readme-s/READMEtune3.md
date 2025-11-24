# 24 Nov 25 READMEtune3.md

## Overview

`tune3.js` is an optimized combo algorithm backtester for perpetual futures data. It tests combinations of 2-4 trading algorithms firing within a specified time window and simulates trades with multiple take profit (TP) and stop loss (SL) configurations.

## Key Features

- Supports **Aggregate** and **Coupling** modes with separate toggles for algo1 and other algos.
- Simulates both Long and Short directions when `tradeDir` is set to `Both`, outputs only the best performing direction.
- Efficient parallel execution using `p-limit` for DB queries, cascading, and simulation.
- Batch price fetching optimized for multiple symbols.
- Outputs results to console and JSON file with detailed trade statistics.

## User Settings

- `TradeSettings` controls:
  - `algo1Aggregate` and `otherAlgosAggregate` toggles.
  - `tradeDir`: 'Long', 'Short', or 'Both'.
  - `tradeSymbol`: symbols to trade.
  - Trade window, position value, TP and SL percentages.
  - Minimum and maximum trades filters.

- `ComboAlgos` defines up to 4 algos with flexible symbol, exchange, param, operator, and value inputs.

- `AlgoSettings` controls algo window, symbol expansions, core percentages, and params.

## Core Workflow

1. **Expand Algos:** Parse and expand each algo into all symbol/exchange/param/value combos.
2. **Fetch Timestamps:** Query DB for trigger timestamps per combo.
3. **Aggregate Timestamps:** In aggregate mode, merge and deduplicate timestamps per algo.
4. **Cascade:** Find timestamps where all algos fired within the algo window.
5. **Filter Combos:** Apply min/max trades filters.
6. **Simulate Trades:** For each combo and TP/SL pair, simulate trades in Long and Short directions if applicable, keeping the best result.
7. **Output:** Display top combos with stats and write JSON output.

## Functions

- `parseAlgo(str)`: Parses algo definition string.
- `expandAlgo(...)`: Expands algo definitions into combos.
- `fetchAlgoTimestamps(...)`: Queries DB for trigger timestamps.
- `aggregateTimestamps(...)`: Deduplicates timestamps per algo.
- `cascadeAlgos(...)`: Cascades timestamps across algos within time window.
- `simulateTrades(...)`: Simulates trades for triggers and TP/SL settings.
- `formatComboAlgo(...)`: Formats combo results for display.
- `writeJsonOutput(...)`: Writes results to JSON file.
- `getMinoritySymbols(...)`: Finds intersection of symbols across algos.

## Notes

- The script enforces consistent aggregate/coupled mode settings.
- Trade entry timestamp can be adjusted to the next available timestamp after cascade trigger (planned enhancement).
- Designed for extensibility and efficient parallelism.

---

This README provides a concise summary of `tune3.js` for future AI or developer reference.