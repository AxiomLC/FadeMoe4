# Unified Perp Data Schema & Upsert System (dbsetup.js - Rev 22 Oct 2025)

## Overview
The `perp_data` table uses a **unified schema** to consolidate all perpetual futures data (OHLCV, OI, PFR, LSR, RSI, TV, LQ) into a single hypertable (TimescaleDB) with composite primary key `(ts BIGINT, symbol TEXT, exchange TEXT)`. This avoids data duplication, enabling efficient 1min aggregation across sources. All data is standardized to **1-minute intervals** (ts floored to minute boundaries via `apiUtils.toMillis(BigInt(ts))` in scripts). Deprecated fields (`source`, `interval`) are dropped; `perpspec` (JSONB array, default `[]`) tracks populated data types (e.g., `["bin-ohlcv", "bin-pfr"]`) for optional filtering.

Key benefits:
- **Additive Upserts**: Scripts insert only their fields (e.g., OHLCV sets `o/h/l/c/v`; PFR sets `pfr`)—COALESCE preserves others (e.g., LQ adds to existing OHLCV rows without overwrites).
- **Storage Efficiency**: ~93% reduction vs. per-type tables; single row per (ts/symbol/exchange).
- **Query Simplicity**: Use PK + field checks (e.g., `WHERE oi IS NOT NULL`) or `perpspec @> '["bin-oi"]'` for JSONB containment.
- **Retention**: Auto-purged after 10 days (matches `calc-metrics.js`).
- **Notes Field**: Optional TEXT for metadata (e.g., 'manual-override'); null by default in API scripts.

## Table Structure
```sql
CREATE TABLE perp_data (
  ts BIGINT NOT NULL,                -- 1min ms timestamp (BigInt, floored)
  symbol TEXT NOT NULL,              -- e.g., 'BTC', 'ETH'
  exchange TEXT NOT NULL,            -- 'bin', 'byb', 'okx'
  perpspec JSONB DEFAULT '[]',       -- Array of perpspec strings (e.g., ["bin-ohlcv", "bin-pfr"]); appended uniquely
  o NUMERIC(20,8), h NUMERIC(20,8), l NUMERIC(20,8), c NUMERIC(20,8), v NUMERIC(20,8),  -- OHLCV
  oi NUMERIC(20,8),                  -- Open Interest (USD-normalized)
  pfr NUMERIC(20,8),                 -- Premium Funding Rate
  lsr NUMERIC(20,8),                 -- Long/Short Ratio
  rsi1 NUMERIC(10,4), rsi60 NUMERIC(10,4),  -- RSI (1min/60min)
  tbv NUMERIC(20,8), tsv NUMERIC(20,8),      -- True Buy/Sell Volume
  lqside VARCHAR(10), lqprice NUMERIC(20,8), lqqty NUMERIC(20,8),  -- Liquidation (majority side, avg price, total qty)
  notes TEXT,                        -- Optional metadata (null by default)
  PRIMARY KEY (ts, symbol, exchange)
);
-- Hypertable on 'ts'; indexes on symbol/exchange for queries.
```

Special cases:
- **MT Token**: Aggregated market trend (symbol='MT', exchange='bin', perpspec='["bin-ohlcv"]'); averages OHLCV from ['ETH', 'BTC', 'XRP', 'SOL'].
- **Perpspec Variants**: `bin-rsi` (only); `byb-tv`, `okx-tv` (no historical backfill); `bin-lq/byb-lq/okx-lq` (WS-only, no historical); `okx-oi/okx-lsr` (API historical limited to ~5 days).

## Upsert Mechanism (insertData & insertBackfillData)
Both methods (in `dbsetup.js`) use JS-based merging (`_mergeRawData`) to combine records by PK, then bulk SQL `INSERT ... VALUES %L ON CONFLICT (ts, symbol, exchange) DO UPDATE SET` with field-specific logic:

- **Shared Merge Logic**:
  - Input: Array of records (e.g., `{ts: BigInt, symbol, exchange, perpspec: string, oi: value}`).
  - JS Map: Key=`${ts}_${symbol}_${exchange}`; populates only matching fields (e.g., 'bin-oi' sets `oi`; ignores others).
  - perpspec: String → array (e.g., 'bin-oi' → `["bin-oi"]`); appended uniquely to existing (e.g., `["bin-ohlcv"] || ["bin-oi"]` → `["bin-ohlcv", "bin-oi"]`).
  - Output: Formatted array for pg-format (ts as string, perpspec as JSON string, nulls for unset fields).

- **insertData (Real-Time "-c.js" Scripts, e.g., 1z-web-ohlcv-c.js, 2-all-pfr-c.js)**:
  - **appendPerpspec=true**: Unique append to perpspec array (e.g., adds 'byb-pfr' to existing row).
  - **DO UPDATE Clause**: `perpspec = COALESCE(perp_data.perpspec, '[]'::jsonb) || EXCLUDED.perpspec,` + `field = COALESCE(EXCLUDED.field, perp_data.field)` for all (preserves existing, e.g., keeps OHLCV if PFR arrives first).
  - Use: Continuous streaming/WS (e.g., OHLCV every 1min confirmed; LQ bucketed; PFR/LSR cached from 5min APIs).
  - Behavior: Additive refreshes; idempotent (no duplicates).

- **insertBackfillData (Historical "-h.js" Scripts, e.g., 1-ohlcv-h.js)**:
  - **appendPerpspec=false**: Sets perpspec to batch's value (e.g., `["bin-ohlcv"]` for OHLCV backfill; no append—assumes clean historical batches).
  - **DO UPDATE Clause**: Same COALESCE as above (additive; fills gaps without overwrites).
  - Use: Bulk historical fetches (e.g., 10 days OHLCV; limited for okx-oi/lsr).
  - Behavior: Fills missing data; skips if PK exists (but updates fields if partial).

- **Edge Cases**:
  - Invalid/NaN values: Scripts validate before insert (e.g., `oi > 0`); nulls propagate safely.
  - MT Insert: Uses `insertData` (perpspec='bin-ohlcv', symbol='MT'); treated as special OHLCV aggregate.
  - Concurrency: Scripts use `p-limit` for API calls; DB pool handles bulk inserts (50k chunks in backfill).

## Data Ingestion Patterns
- **1min Standardization**: All ts floored to minute (e.g., Bybit 5min OI/LSR expanded/cached to 5x 1min records; WS LQ bucketed by minute).
- **Exchange Mapping**: Scripts map symbols (e.g., Bybit meme: 'BONK' → '1000BONKUSDT'); explicit `exchange` ('bin'/'byb'/'okx').
- **No Historical for Some**: TV (byb/okx-tv), LQ (all-lq) are real-time only (WS/API limits); backfill skips them.
- **Script Examples**:
  - **-c.js (Real-Time)**: `dbManager.insertData([{ts: BigInt, symbol, exchange, perpspec: 'bin-pfr', pfr: value}]);` → Appends to perpspec, updates pfr only.
  - **-h.js (Backfill)**: Bulk array to `insertBackfillData` → Sets perpspec for batch, fills fields.
  - **WS Bucketing (LQ)**: Aggregates events to 1min (majority side via counts; tie-breaker uses OHLCV h/l distance).

## Querying Best Practices
Avoid legacy perpspec equality (`perpspec = 'bin-ohlcv'`); use:
- **By Fields**: `SELECT * FROM perp_data WHERE symbol = 'BTC' AND exchange = 'bin' AND ts >= 1728000000000 AND oi IS NOT NULL ORDER BY ts ASC;` (fetches rows with OI).
- **By Perpspec Containment**: `WHERE perpspec @> '["bin-oi"]'::jsonb` (JSONB array contains; efficient with GIN index if added).
- **Full Row**: `WHERE ts = 1728000000000n AND symbol = 'BTC' AND exchange = 'bin';` (PK exact match).
- **Aggregation**: Leverage PK ordering; join with `perp_metrics` on PK for derived metrics.
- **Notes**: `WHERE notes IS NOT NULL;` (manual flags); set via SQL: `UPDATE perp_data SET notes = 'manual-override' WHERE ts = 1728000000000n AND symbol = 'ETH' AND exchange = 'bin';`.

## Limitations & Notes
- **Historical Gaps**: okx-oi/lsr (~5 days API limit); tv/lq (no backfill—real-time only).
- **Perpspec Updates**: bin-rsi (unified); MT uses 'bin-ohlcv' (symbol='MT').
- **Performance**: Bulk inserts (chunks in -h.js); hypertable compression for old data.
- **Future Scripts**: Always provide string `perpspec`, BigInt-compatible `ts`, and only relevant fields—merge handles the rest.

For schema changes: Run `dbManager.initialize()` (drops/recreates tables). Questions? Check script logs or query `perp_status`/`perp_errors`.