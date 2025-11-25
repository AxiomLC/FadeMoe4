# 25 Nov 25 READMEtune3.md

## Overview

`tune3.js` is an optimized combo algorithm backtester for perpetual futures data. It tests combinations of 2-4 trading algorithms firing within a specified time window and simulates trades with multiple take profit (TP) and stop loss (SL) configurations.

## Key Features

- Supports **Aggregate** and **Coupling** modes with separate toggles for algo1 and other algos.
- Handles mixed mode where some algos are aggregate and others coupled, allowing cascading of non-matching symbols accordingly.
- Simulates both Long and Short directions when `tradeDir` is set to `Both`, outputs only the best performing direction.
- Efficient parallel execution using `p-limit` for DB queries, cascading, and simulation.
- Batch price fetching optimized for multiple symbols.
- Outputs results to console and JSON file with detailed trade statistics.

## User Settings

- `TradeSettings` controls:
  - `algo1Aggregate` and `otherAlgosAggregate` toggles.
  - `tradeDir`: 'Long', 'Short', or 'Both'.
  - `tradeSymbol`: symbols to trade (supports `useAll` and manual lists).
  - Trade window, position value, TP and SL percentages.
  - Minimum and maximum trades filters.

- `ComboAlgos` defines up to 4 algos with flexible symbol, exchange, param, operator, and value inputs.

- `AlgoSettings` controls algo window, symbol expansions, core percentages, and params.

## Core Workflow

1. **Expand Algos:** Parse and expand each algo into all symbol/exchange/param/value combos, supporting manual symbol lists and special placeholders like `[params]` and `[corePerc]`.

2. **Fetch Timestamps:** Query DB for trigger timestamps per combo.

3. **Aggregate Timestamps:** In aggregate mode, merge and deduplicate timestamps per algo.

4. **Minority Symbols Calculation:**  
   - Computes symbol sets per algo considering aggregate or coupled mode.  
   - In mixed mode (aggregate + coupled), minority symbols are relaxed to allow cascading of coupled symbols against aggregate timestamps.

5. **Cascade:**  
   - In aggregate mode, cascades timestamps across algos allowing cross-symbol matches.  
   - In coupled mode, cascades per symbol independently.  
   - In mixed mode, allows coupled algos to cascade against aggregate timestamps regardless of symbol mismatch.

6. **Filter Combos:** Apply min/max trades filters.

7. **Simulate Trades:** For each combo and TP/SL pair, simulate trades in Long and Short directions if applicable, keeping the best result.  
   - Uses all trade symbols in aggregate mode.  
   - Uses specific symbol in coupled mode.

8. **Output:**  
   - Formats and displays top combos with stats, respecting mode and symbol lists.  
   - Writes detailed JSON output for further analysis.

## Functions

- `parseAlgo(str)`: Parses algo definition string into components.

- `expandAlgo(algoStr, algoSymbol, params, corePerc)`: Expands algo definitions into all symbol/exchange/param/value combos, supporting manual arrays and special placeholders.

- `fetchAlgoTimestamps(combo, startTs, endTs)`: Queries DB for trigger timestamps for a combo.

- `aggregateTimestamps(algoResults)`: Deduplicates and merges timestamps per algo in aggregate mode.

- `getMinoritySymbols(allAlgoResults, algoAggregateFlags)`: Computes intersection of symbols across algos, considering aggregate and coupled modes, with special handling for mixed mode.

- `cascadeAlgos(algoTimestamps, algoWindowMs)`: Cascades timestamps across algos within the algo window, supporting aggregate, coupled, and mixed modes.

- `simulateTrades(triggers, tradeSymbols, tpPerc, slPerc, tradeWindowMs, tradeDir, posVal)`: Simulates trades for triggers with given TP/SL and trade settings.

- `formatComboAlgo(algoComboArray, stats, tpPerc, slPerc, tradeSymbols, algoSymbols, tradeDir, mode)`: Formats combo results for display, showing correct symbols per mode and user input.

- `writeJsonOutput(results, metadata)`: Writes results and metadata to a JSON file with timestamped filename.

## Notes

- The script enforces consistent aggregate/coupled mode settings and validates user inputs.

- Mixed mode cascading allows aggregate algos to cascade with coupled algos without strict symbol matching.

- Trade entry timestamp adjustment (to next available timestamp after cascade trigger) is planned for future enhancement.

- Designed for extensibility, efficient parallelism, and flexible user configuration.

---

This README provides a concise summary of `tune3.js` for future AI or developer reference.