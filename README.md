# Perpetual Futures Data System - Technical Architecture
a mini cross-exchange analytics warehouse specialized for perpetual futures scalping/backtesting.
a Domain-Specific Language (DSL) 

## System Overview
PostgreSQL 17.6 + TimescaleDB 2.22.0 perpetual futures analytics database with pre-calculated rolling metrics for high-speed backtesting. Data retention: **10 days** (auto-pruned via TimescaleDB retention policies).

---

## Core Tables

### 1. `perp_data` (Hypertable)
**Raw 1-minute interval perpetual futures data from 3 exchanges**

**Primary Key:** `(ts, symbol, exchange)`
- `ts`: BIGINT timestamp (milliseconds since epoch)
- `symbol`: TEXT (e.g., 'SOL', 'BTC', 'ETH')
- `perpspec`: TEXT schema identifier (e.g., 'bin-ohlcv', 'okx-pfr')

**Columns:** 20 fields total
- Metadata: `source`, `interval`
- OHLCV: `o`, `h`, `l`, `c`, `v`
- Derivatives: `oi` (open interest), `pfr` (funding rate), `lsr` (long/short ratio)
- Technical: `rsi1`, `rsi60`, `tbv` (taker buy vol), `tsv` (taker sell vol)
- Liquidations: `lqside`, `lqprice`, `lqqty`

**Perpspec Design:**
- Each exchange-metric pair is a separate perpspec (e.g., `bin-ohlcv`, `byb-pfr`)
- 19 active perpspecs: 3 exchanges × 6 metric types + 1 cross-exchange RSI
- Perpspecs use JSONB schema registry (`perpspec_schema` table) for field validation
- Each perpspec row contains only relevant fields (sparse data via NULLs)

---

### 2. `perp_metrics` (Hypertable)
**Pre-calculated rolling % changes for backtester optimization**

**Primary Key:** `(ts, symbol, perpspec)`
- One row per perpspec per timestamp (not unified by exchange)
- Example: SOL at 16:37 has 18 separate rows (bin-ohlcv, bin-oi, okx-pfr, etc.)

**Columns:** 57 fields total
- Raw values: `o`, `h`, `l`, `c`, `v`, `oi`, `pfr`, `lsr`, `rsi1`, `rsi60`, `tbv`, `tsv`, `lqside`, `lqprice`, `lqqty`
- % changes for 12 params × 3 windows (1m, 5m, 10m) = 36 change columns
  - Params: `c`, `v`, `oi`, `pfr`, `lsr`, `rsi1`, `rsi60`, `tbv`, `tsv`, `lqprice`, `lqqty`
  - Windows: `_chg_1m`, `_chg_5m`, `_chg_10m`
  - Example: `c_chg_5m` = % change in close price vs 5 minutes ago
- Special: `lqside_chg_*` stores current side if changed (categorical, not % change)

**Why o/h/l included:** Wick rejection analysis for liquidity patterns (e.g., stop hunts, resistance levels)

**Calculation frequency:** Every 1 minute via `calc-metrics.js`

---

### 3. `perpspec_schema` (Registry)
**JSONB field definitions for each perpspec**

Columns: `perpspec_name` (PK), `fields` (JSONB array), `last_updated`

Example entry:
```json
{
  "perpspec_name": "bin-ohlcv",
  "fields": ["ts", "symbol", "source", "perpspec", "interval", "o", "h", "l", "c", "v"]
}
```

---

### 4. `perp_status` & `perp_errors`
**Monitoring tables for script execution**

- `perp_status`: Logs script lifecycle (`started`, `running`, `stopped`)
- `perp_errors`: Detailed error logging with JSONB stack traces

---

## Scripts

### `dbsetup.js`
**Database initialization and schema manager**

**Run once:** `node db/dbsetup.js`
- Drops existing tables (destructive!)
- Creates hypertables with TimescaleDB
- Registers all 19 perpspecs
- Sets 10-day retention policy

**Key exports:**
- `insertData(perpspecName, dataArray)`: Bulk insert to perp_data
- `insertMetrics(metricsArray)`: Bulk upsert to perp_metrics
- `queryMetrics(symbols, perpspecs, startTs, endTs)`: Backtester fast query
- `queryWickRejections(...)`: Example wick analysis query

**Configuration:** `DB_RETENTION_DAYS = 10` (line 13)

---

### `calc-metrics.js`
**Rolling % change calculator (1-minute intervals)**

**Dev mode:** `node db/calc-metrics.js` (single run)
**Production:** `node db/calc-metrics.js --continuous` (infinite loop)

**Startup trigger:**
- Currently: Manual launch for development
- Production: Should be called by `master-api.js` on startup
- To change: Modify `master-api.js` to import and call `runContinuously()`

**Configuration:**
- `DB_RETENTION_DAYS = 10` (line 21) - must match dbsetup.js
- `LOOKBACK_MINUTES = 15` (line 22) - safety buffer for gap detection
- `WINDOW_SIZES = [1, 5, 10]` (line 23) - change windows
- `PERPSPECS = [...]` (line 27) - add new perpspecs here

**Process flow:**
1. Every 60 seconds, fetch last 15 min of perp_data for all symbols/perpspecs
2. Calculate % changes for 1m, 5m, 10m windows
3. Bulk upsert to perp_metrics (ON CONFLICT UPDATE)
4. Log status to perp_status table

**Gap handling:** Fetches 15min window to ensure previous data exists for window calculations

---

### `add-perpspec.js`
**Add new perpspec without database rebuild**

**Usage:** `node db/add-perpspec.js <name> <field1,field2,...>`

**Example:** `node db/add-perpspec.js bin-vwap ts,symbol,source,perpspec,interval,vwap`

**Steps after registration:**
1. Script adds entry to `perpspec_schema` table
2. User manually adds perpspec to `PERPSPECS` array in calc-metrics.js (line 27)
3. Ensure data fetcher populates perp_data with new perpspec
4. Restart calc-metrics.js

**Validation:** Only allows fields that exist in perp_data table schema

---

## Backtester Query Patterns

### Pattern 1: Single perpspec time-series
```javascript
// Get Binance OHLCV data for SOL over 1 hour
const data = await dbManager.queryPerpData(
  'bin-ohlcv', 
  'SOL', 
  startTs, 
  endTs
);
```

### Pattern 2: Cross-exchange comparison
```javascript
// Compare volume spikes across all 3 exchanges
const metrics = await dbManager.queryMetrics(
  ['SOL', 'BTC'], 
  ['bin-ohlcv', 'byb-ohlcv', 'okx-ohlcv'],
  startTs,
  endTs
);
// Returns all rows, filter by perpspec to separate exchanges
```

### Pattern 3: Multi-perpspec strategy
```javascript
// Strategy: Binance volume spike + OKX funding rate divergence
const metrics = await dbManager.queryMetrics(
  ['SOL'],
  ['bin-ohlcv', 'okx-pfr', 'bin-lsr'],
  startTs,
  endTs
);
// Each perpspec has separate row at each timestamp
// Join by (ts, symbol) to correlate metrics
```

### Pattern 4: Wick rejection analysis
```javascript
// Find candles with upper wick > 2% (resistance levels)
const wicks = await dbManager.queryWickRejections(
  'SOL',
  'bin-ohlcv',
  startTs,
  endTs,
  2.0 // 2% threshold
);
```

### Pattern 5: Point-in-time cross-exchange
```javascript
// Exact timestamp comparison across exchanges
const snapshot = await dbManager.queryMetricsAtTimestamp(
  'SOL',
  ['bin-ohlcv', 'okx-ohlcv'],
  1760546220000
);
// Returns one row per perpspec at exact timestamp
```

---

## Data Flow Architecture

```
Exchange APIs → Data Fetchers → perp_data (raw 1min data)
                                      ↓
                              calc-metrics.js (every 1min)
                                      ↓
                              perp_metrics (pre-calculated % changes)
                                      ↓
                              Backtester (fast queries)
```

**Key insight:** perp_metrics avoids on-the-fly LAG() calculations during backtesting. All % changes pre-computed and indexed.

---

## Index Strategy (Fast Queries)

**perp_metrics indexes:**
```sql
idx_metrics_symbol_ts: (symbol, ts DESC)
idx_metrics_perpspec_ts: (perpspec, ts DESC)  
idx_metrics_symbol_perpspec: (symbol, perpspec, ts DESC)
```

**Query optimization:**
- Always include `perpspec` in WHERE clause to leverage indexes
- Use `ts BETWEEN` for time ranges (TimescaleDB chunk pruning)
- Avoid `SELECT *` - specify only needed columns

**Performance target:** 10 days × 1440 min/day × 100 symbols × 18 perpspecs = ~26M rows
- Expected query time: <100ms for 1-symbol, 1-day, multi-perpspec query

---

## Retention & Auto-Pruning

**TimescaleDB retention policy:** Automatically deletes data older than 10 days
- Runs in background (no manual intervention)
- Configurable via `DB_RETENTION_DAYS` in both scripts

**Manual prune (if needed):**
```sql
CALL drop_chunks('perp_data', INTERVAL '10 days');
CALL drop_chunks('perp_metrics', INTERVAL '10 days');
```

---

## Adding New Perpspecs (Complete Workflow)

**Option 1: During setup (recommended)**
1. Edit `dbsetup.js` line ~270: Add to `fixedSchemas` array
2. Edit `calc-metrics.js` line 27: Add to `PERPSPECS` array
3. Run `node db/dbsetup.js` (rebuilds database)

**Option 2: Without rebuild (live system)**
1. Run `node db/add-perpspec.js <name> <fields>`
2. Edit `calc-metrics.js` line 27: Add to `PERPSPECS` array
3. Restart `calc-metrics.js`

**Important:** All perpspecs share same perp_data columns. Cannot add new columns without altering table schema.

---

## Troubleshooting

**Metrics not calculating:**
- Check `perp_status` table for errors
- Verify data exists in perp_data for symbol/perpspec
- Ensure 15min lookback window has data (LOOKBACK_MINUTES)

**Gaps in metrics:**
- calc-metrics.js detects gaps automatically via 15min window
- Force recalculation: Delete metrics, restart calc-metrics.js

**Compression errors:**
- TimescaleDB 2.22.0 compression syntax changed
- Current setup: No compression (lines commented out in dbsetup.js)
- To enable: Research `timescaledb.compress` parameters for 2.22.0

**Performance degradation:**
- Check index usage: `EXPLAIN ANALYZE <query>`
- Verify retention policy active: `SELECT * FROM timescaledb_information.jobs`
- Consider adding composite indexes for specific query patterns

---

## System Requirements

- PostgreSQL 17.6
- TimescaleDB 2.22.0
- Node.js (pg, pg-format, dotenv)
- Disk: ~5GB for 10 days @ 100 symbols (estimate)

---

## Future AI Context Notes

**For future LLM assistants debugging this system:**
1. perpspec = schema identifier, NOT a separate table
2. Primary key `(ts, symbol, perpspec)` means each perpspec gets its own row
3. Metrics are NOT cross-exchange unified - query by perpspec for exchange isolation
4. o/h/l in perp_metrics are intentional (wick analysis, not redundant)
5. 15min lookback in calc-metrics ensures window data exists (not wasteful)
6. Retention policy is automatic - do NOT manually DELETE old data
7. Status logs in purple (Sky Blue #87CEEB) for visibility in console
8. calc-metrics should be triggered by master-api.js in production (currently manual)

**Common misconceptions to avoid:**
- ❌ "Perpspec should be a separate table" → No, it's a schema identifier within shared tables
- ❌ "One unified metrics row per symbol/timestamp" → No, each perpspec gets separate row for timing precision
- ❌ "Remove o/h/l from metrics to avoid duplication" → No, needed for wick analysis
- ❌ "Fetch exactly 10min of data for 10min windows" → No, need 15min buffer for safe calculations