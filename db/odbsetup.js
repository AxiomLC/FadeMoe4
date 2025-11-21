// db/dbsetup.js 24 Oct 2025
// ============================================================================
// DATABASE SETUP & MANAGER  pool max=50 supports parallel without queueing. adds a insertWithRetry helper for 
// deadlock resilience (retry 3x PG code 40P01 with backoff) optimizes insertBackfillData with 100k-row chunking,
// Unified upsert: insertData for -c.js (partial DO UPDATE with COALESCE/append perpspec)
// insertBackfillData for -h.js (partial DO UPDATE with COALESCE; additive for historical fills)
// ============================================================================
const { Pool } = require('pg');
const format = require('pg-format');
require('dotenv').config();

const DB_RETENTION_DAYS = 10; // Must match calc-metrics.js

// ============================================================================
// DATABASE MANAGER CLASS
// ============================================================================
class DatabaseManager {
  constructor() {
    // UPDATED: Validate env before Pool (throw if missing - fixes undefined pool)
    const requiredEnv = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME'];
    for (const key of requiredEnv) {
      if (!process.env[key]) {
        throw new Error(`Missing env var: ${key} (check .env file)`);
      }
    }
    console.log('DB env validated'); // NEW: Log for debug (remove if noisy)

    this.pool = new Pool({
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      max: parseInt(process.env.DB_POOL_MAX) || 50, // Tunable via env
      connectionTimeoutMillis: 5000,
      idleTimeoutMillis: 30000
    });

    // NEW: Error handler for pool (logs issues without crashing)
    this.pool.on('error', (err) => console.error('Pool error:', err));
  }

  // Generic query method for custom SQL (reusable by scripts) (UPDATED: Pool guard)
  async query(sql, params = []) {
    if (!this.pool) {
      throw new Error('DB pool not initialized - run initialize() or check env');
    }
    return await this.pool.query(sql, params);
  }

  // Graceful shutdown
  async close() {
    if (this.pool) await this.pool.end();
  }

  // --------------------------------------------------------------------------
  // INITIALIZATION (UPDATED: Optional noDrop param for production/backfill)
  // --------------------------------------------------------------------------
  async initialize(noDrop = false) { // NEW: Param to skip drops (default false for CLI setup)
    console.log('⚙️ Setting up database...');
    if (!noDrop) { // UPDATED: Conditional drop (skip for backfill/master-api)
      await this.dropExistingTables();
    } else {
      console.log('  - Skipping drops (noDrop mode)'); // NEW: Log for backfill
    }
    await this.enableExtensions();
    await this.createCoreTables();
    await this.insertFixedPerpspecSchemas();
    await this.setupRetentionPolicies();
    console.log('✅ Database setup complete.');
  }

  async dropExistingTables() {
    const tables = [
      'perp_data',
      'perp_metrics',
      'perp_status',
      'perp_errors',
      'perpspec_schema',
    ];
    for (const table of tables) {
      try {
        await this.pool.query(`DROP TABLE IF EXISTS ${table} CASCADE`);
        console.log(`  - Dropped table: ${table}`);
      } catch (err) {
        console.error(`  - Error dropping ${table}: ${err.message}`);
      }
    }
  }

  async enableExtensions() {
    try {
      await this.pool.query('CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE');
      console.log('  - TimescaleDB extension enabled.');
    } catch (err) {
      console.log(`  - TimescaleDB warning: ${err.message}`);
    }
  }

  // --------------------------------------------------------------------------
  // CORE TABLES
  // --------------------------------------------------------------------------
  async createCoreTables() {
    try {
      // perp_data: Unified schema with perpspec as JSONB (non-key for frontend/UI queries), notes added, PK on (ts, symbol, exchange)
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS perp_data (
          ts BIGINT NOT NULL,
          symbol TEXT NOT NULL,
          exchange TEXT NOT NULL,
          perpspec JSONB DEFAULT '[]',
          o NUMERIC(20,8),
          h NUMERIC(20,8),
          l NUMERIC(20,8),
          c NUMERIC(20,8),
          v NUMERIC(20,8),
          oi NUMERIC(20,8),
          pfr NUMERIC(20,8),
          lsr NUMERIC(20,8),
          rsi1 NUMERIC(10,4),
          rsi60 NUMERIC(10,4),
          tbv NUMERIC(20,8),
          tsv NUMERIC(20,8),
          lql NUMERIC(20,8),
          lqs NUMERIC(20,8),
          notes TEXT,
          PRIMARY KEY (ts, symbol, exchange)
        )`
      );
      await this.pool.query(`SELECT create_hypertable('perp_data', 'ts', if_not_exists => TRUE)`);
      console.log('  - Created table: perp_data');

      // Indexes for filtering by symbol and exchange
      await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_perp_data_symbol ON perp_data (symbol)`);
      await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_perp_data_exchange ON perp_data (exchange)`);
      console.log('  - Created indexes on perp_data (symbol, exchange)');


// perp_metrics: Includes exchange in PK; % change columns for 1m/5m/10m windows
await this.pool.query(
  `CREATE TABLE IF NOT EXISTS perp_metrics (
    ts BIGINT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    o NUMERIC(20,8), h NUMERIC(20,8), l NUMERIC(20,8), c NUMERIC(20,8),
    v NUMERIC(20,8), oi NUMERIC(20,8), pfr NUMERIC(20,8), lsr NUMERIC(20,8),
    rsi1 NUMERIC(10,4), rsi60 NUMERIC(10,4),
    tbv NUMERIC(20,8), tsv NUMERIC(20,8),
    lql NUMERIC(20,8), lqs NUMERIC(20,8),

    -- % change columns (1m/5m/10m) - NUMERIC(7,3) for range ±9999.999%
    c_chg_1m NUMERIC(7,3), v_chg_1m NUMERIC(7,3), oi_chg_1m NUMERIC(7,3),
    pfr_chg_1m NUMERIC(7,3), lsr_chg_1m NUMERIC(7,3),
    rsi1_chg_1m NUMERIC(7,3), rsi60_chg_1m NUMERIC(7,3),
    tbv_chg_1m NUMERIC(7,3), tsv_chg_1m NUMERIC(7,3),
    lql_chg_1m NUMERIC(7,3), lqs_chg_1m NUMERIC(7,3),

    c_chg_5m NUMERIC(7,3), v_chg_5m NUMERIC(7,3), oi_chg_5m NUMERIC(7,3),
    pfr_chg_5m NUMERIC(7,3), lsr_chg_5m NUMERIC(7,3),
    rsi1_chg_5m NUMERIC(7,3), rsi60_chg_5m NUMERIC(7,3),
    tbv_chg_5m NUMERIC(7,3), tsv_chg_5m NUMERIC(7,3),
    lql_chg_5m NUMERIC(7,3), lqs_chg_5m NUMERIC(7,3),
    
    c_chg_10m NUMERIC(7,3), v_chg_10m NUMERIC(7,3), oi_chg_10m NUMERIC(7,3),
    pfr_chg_10m NUMERIC(7,3), lsr_chg_10m NUMERIC(7,3), 
    rsi1_chg_10m NUMERIC(7,3), rsi60_chg_10m NUMERIC(7,3),
    tbv_chg_10m NUMERIC(7,3), tsv_chg_10m NUMERIC(7,3),
    lql_chg_10m NUMERIC(7,3), lqs_chg_10m NUMERIC(7,3),
    PRIMARY KEY (ts, symbol, exchange)
  )`
);
      await this.pool.query(`SELECT create_hypertable('perp_metrics', 'ts', if_not_exists => TRUE)`);
      console.log('  - Created table: perp_metrics');

      // Indexes for performance
      const indexQueries = [
        'c_chg_1m', 'c_chg_5m', 'c_chg_10m',
        'v_chg_1m', 'v_chg_5m', 'v_chg_10m',
        'oi_chg_1m', 'oi_chg_5m', 'oi_chg_10m',
        'pfr_chg_1m', 'pfr_chg_5m', 'pfr_chg_10m',
        'lsr_chg_1m', 'lsr_chg_5m', 'lsr_chg_10m',
        'rsi1_chg_1m', 'rsi1_chg_5m', 'rsi1_chg_10m',
        'rsi60_chg_1m', 'rsi60_chg_5m', 'rsi60_chg_10m',
        'tbv_chg_1m', 'tbv_chg_5m', 'tbv_chg_10m',
        'tsv_chg_1m', 'tsv_chg_5m', 'tsv_chg_10m',
        'lql_chg_1m', 'lql_chg_5m', 'lql_chg_10m',
        'lqs_chg_1m', 'lqs_chg_5m', 'lqs_chg_10m'
        

      ].map(param => `CREATE INDEX IF NOT EXISTS idx_perp_metrics_${param} ON perp_metrics (${param})`);
      for (const query of indexQueries) {
        await this.pool.query(query);
        console.log(`  - Created index for ${query.split('ON perp_metrics (')[1].split(')')[0]}`);
      }
      await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_perp_metrics_symbol_exchange ON perp_metrics (symbol, exchange)`);
      console.log('  - Created index for symbol, exchange on perp_metrics');

      // perp_status: Unchanged
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS perp_status (
          task_id SERIAL PRIMARY KEY,
          script_name TEXT NOT NULL,
          status TEXT NOT NULL,
          message TEXT,
          details JSONB,
          ts TIMESTAMP DEFAULT NOW()
        )
      `);
      console.log('  - Created table: perp_status');

      // perp_errors: Unchanged
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS perp_errors (
          error_id SERIAL PRIMARY KEY,
          script_name TEXT,
          perpspec TEXT,
          error_type TEXT,
          error_code TEXT,
          error_message TEXT,
          details JSONB,
          ts TIMESTAMP DEFAULT NOW()
        )
      `);
      console.log('  - Created table: perp_errors');

      // perpspec_schema: Legacy for UI (updated to unified fields; no source/interval)
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS perpspec_schema (
          perpspec_name TEXT PRIMARY KEY,
          fields JSONB NOT NULL,
          last_updated TIMESTAMP DEFAULT NOW()
        )
      `);
      console.log('  - Created table: perpspec_schema');
    } catch (error) {
      console.error('  - Error in createCoreTables:', error.message);
      throw error;
    }
  }

  // --------------------------------------------------------------------------
  // SCHEMA REGISTRY (Updated: Removed deprecated source/interval fields)
  // --------------------------------------------------------------------------
  async insertFixedPerpspecSchemas() {
    const perpspecs = ['bin', 'byb', 'okx'];
    const types = ['ohlcv', 'oi', 'pfr', 'lsr', 'tv', 'lq'];

    const schemas = [];
    for (const p of perpspecs) {
      schemas.push({ name: `${p}-ohlcv`, fields: ['ts', 'symbol', 'exchange', 'o', 'h', 'l', 'c', 'v'] });
      schemas.push({ name: `${p}-oi`, fields: ['ts', 'symbol', 'exchange', 'oi'] });
      schemas.push({ name: `${p}-pfr`, fields: ['ts', 'symbol', 'exchange', 'pfr'] });
      schemas.push({ name: `${p}-lsr`, fields: ['ts', 'symbol', 'exchange', 'lsr'] });
      schemas.push({ name: `${p}-tv`, fields: ['ts', 'symbol', 'exchange', 'tbv', 'tsv'] });
      schemas.push({ name: `${p}-lq`, fields: ['ts', 'symbol', 'exchange', 'lql', 'lqs'] });
    }
    schemas.push({ name: 'bin-rsi', fields: ['ts', 'symbol', 'exchange', 'rsi1', 'rsi60'] }); // Unified: no source/interval

    for (const schema of schemas) {
      await this.pool.query(
        `INSERT INTO perpspec_schema (perpspec_name, fields)
         VALUES ($1, $2)
         ON CONFLICT (perpspec_name) DO UPDATE SET
           fields = EXCLUDED.fields, last_updated = NOW()`,
        [schema.name, JSON.stringify(schema.fields)]
      );
      console.log(`  - Registered perpspec_schema for '${schema.name}'`);
    }
  }

  // --------------------------------------------------------------------------
  // RETENTION POLICIES
  // --------------------------------------------------------------------------
async setupRetentionPolicies() {
  try {
    // Create integer_now function for BIGINT timestamps (milliseconds)
    await this.pool.query(`
      CREATE OR REPLACE FUNCTION integer_now_ms() RETURNS BIGINT LANGUAGE SQL STABLE AS $$
        SELECT CAST(EXTRACT(EPOCH FROM NOW()) * 1000 AS BIGINT);
      $$
    `);
    console.log('  - Created integer_now_ms() function');

    // Set integer_now function for hypertables
    await this.pool.query(`SELECT set_integer_now_func('perp_data', 'integer_now_ms')`);
    await this.pool.query(`SELECT set_integer_now_func('perp_metrics', 'integer_now_ms')`);
    console.log('  - Set integer_now function for hypertables');

    // Remove existing policies (ignore if not found)
    try { await this.pool.query(`SELECT remove_retention_policy('perp_data')`); } catch (e) { /* ignore */ }
    try { await this.pool.query(`SELECT remove_retention_policy('perp_metrics')`); } catch (e) { /* ignore */ }
    
    // Add retention policies (10 days in milliseconds)
    const retentionMs = DB_RETENTION_DAYS * 24 * 60 * 60 * 1000;
    await this.pool.query(`SELECT add_retention_policy('perp_data', drop_after => ${retentionMs})`);
    await this.pool.query(`SELECT add_retention_policy('perp_metrics', drop_after => ${retentionMs})`);
    console.log(`  - Retention set to ${DB_RETENTION_DAYS} days (${retentionMs}ms)`);
  } catch (err) {
    console.error(`  - Error setting retention: ${err.message}`);
  }
}
  // --------------------------------------------------------------------------
  // DATA OPERATIONS
  // --------------------------------------------------------------------------
  // Shared merge logic: Merges allRawData into formatted values array
  // appendPerpspec: If true (for -c.js), unique-append to perpspec; else set to batch's perpspec (for -h.js)
  async _mergeRawData(allRawData, appendPerpspec = false) {
    if (!Array.isArray(allRawData) || allRawData.length === 0) return [];

    function getExchangeFromPerpspec(perpspec) {
      if (perpspec.startsWith('bin-')) return 'bin';
      if (perpspec.startsWith('byb-')) return 'byb';
      if (perpspec.startsWith('okx-')) return 'okx';
      return null;
    }

    const merged = new Map(); // Key: `${ts}_${symbol}_${exchange}`

    for (const record of allRawData) {
      const ts = record.ts;
      const symbol = record.symbol;
      let perpspec = record.perpspec;
      const exchange = getExchangeFromPerpspec(perpspec);
      if (!exchange) continue;
      const key = `${ts}_${symbol}_${exchange}`;

      if (typeof perpspec === 'string') perpspec = [perpspec];
      else if (!Array.isArray(perpspec)) perpspec = ['unknown'];

      if (!merged.has(key)) {
        merged.set(key, {
          ts, symbol, exchange, perpspec,
          o: null, h: null, l: null, c: null, v: null,
          oi: null, pfr: null, lsr: null,
          rsi1: null, rsi60: null,
          tbv: null, tsv: null,
          lql: null, lqs: null,
          notes: record.notes || null
        });
      }

      const row = merged.get(key);
      // Populate fields based on perpspec
      if (record.perpspec?.includes('ohlcv')) {
        row.o = record.o; row.h = record.h; row.l = record.l;
        row.c = record.c; row.v = record.v;
        if (!appendPerpspec) row.perpspec = perpspec; // For backfill: Set to this batch
      }
      if (record.perpspec?.includes('oi')) row.oi = record.oi;
      if (record.perpspec?.includes('pfr')) row.pfr = record.pfr;
      if (record.perpspec?.includes('lsr')) row.lsr = record.lsr;
      if (record.perpspec?.includes('rsi')) { row.rsi1 = record.rsi1; row.rsi60 = record.rsi60; }
      if (record.perpspec?.includes('tv')) { row.tbv = record.tbv; row.tsv = record.tsv; }
      if (record.perpspec?.includes('lq')) {
        row.lql = record.lql; row.lqs = record.lqs;
      }
      if (record.notes) row.notes = record.notes;
      // For continuous: Unique append to perpspec
      if (appendPerpspec) {
        row.perpspec = [...new Set([...(row.perpspec || []), ...perpspec])];
      }
    }

    const mergedArray = Array.from(merged.values());
    if (mergedArray.length === 0) return [];

    const fields = ['ts', 'symbol', 'exchange', 'perpspec', 'o', 'h', 'l', 'c', 'v', 'oi', 'pfr', 'lsr', 'rsi1', 'rsi60', 'tbv', 'tsv', 'lql', 'lqs', 'notes'];

    return mergedArray.map(row => fields.map(f => {
      let val = row[f] ?? null;
      if (f === 'ts' && typeof val === 'bigint') val = val.toString();
      else if (f === 'perpspec' && Array.isArray(val)) val = JSON.stringify(val);
      if (['o','h','l','c','v','oi','pfr','lsr','rsi1','rsi60','tbv','tsv','lql','lqs'].includes(f) && typeof val === 'number' && isNaN(val)) val = null;
      return val;
    }));
  }

  // New: Private helper for deadlock retries (PG code 40P01) on any query
  async insertWithRetry(query, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await this.pool.query(query);
      } catch (error) {
        if (error.code === '40P01' && attempt < maxRetries) {  // Deadlock
          console.warn(`Deadlock on query (attempt ${attempt}/${maxRetries}); retrying in ${attempt}s...`);
          await new Promise(r => setTimeout(r, 1000 * attempt));  // Backoff
          continue;
        }
        throw error;  // Re-throw others
      }
    }
  }

  // For -c.js continuous/real-time: Partial update (COALESCE preserves existing; append perpspec)
  // PK fields (ts/symbol/exchange) immutable
  async insertData(allRawData) {
    const values = await this._mergeRawData(allRawData, true); // Append perpspec
    if (values.length === 0) return { rowCount: 0 };

    const fields = ['ts', 'symbol', 'exchange', 'perpspec', 'o', 'h', 'l', 'c', 'v', 'oi', 'pfr', 'lsr', 'rsi1', 'rsi60', 'tbv', 'tsv', 'lql', 'lqs', 'notes'];

    const updateClause = `
      perpspec = COALESCE(perp_data.perpspec, '[]'::jsonb) || EXCLUDED.perpspec,
      o = COALESCE(EXCLUDED.o, perp_data.o),
      h = COALESCE(EXCLUDED.h, perp_data.h),
      l = COALESCE(EXCLUDED.l, perp_data.l),
      c = COALESCE(EXCLUDED.c, perp_data.c),
      v = COALESCE(EXCLUDED.v, perp_data.v),
      oi = COALESCE(EXCLUDED.oi, perp_data.oi),
      pfr = COALESCE(EXCLUDED.pfr, perp_data.pfr),
      lsr = COALESCE(EXCLUDED.lsr, perp_data.lsr),
      rsi1 = COALESCE(EXCLUDED.rsi1, perp_data.rsi1),
      rsi60 = COALESCE(EXCLUDED.rsi60, perp_data.rsi60),
      tbv = COALESCE(EXCLUDED.tbv, perp_data.tbv),
      tsv = COALESCE(EXCLUDED.tsv, perp_data.tsv),
      lql = COALESCE(EXCLUDED.lql, perp_data.lql),
      lqs = COALESCE(EXCLUDED.lqs, perp_data.lqs),
      notes = COALESCE(EXCLUDED.notes, perp_data.notes)
    `;

    const query = format(
      `INSERT INTO perp_data (${fields.join(', ')})
       VALUES %L
       ON CONFLICT (ts, symbol, exchange) DO UPDATE SET ${updateClause}`,
      values
    );

    const result = await this.insertWithRetry(query);
    return result;
  }

  // For -h.js backfill: Partial update (COALESCE preserves existing; set perpspec to batch)
  // Additive: Adds fields (e.g., LSR adds lsr to OHLCV rows) without skipping
  async insertBackfillData(allRawData) {
    const values = await this._mergeRawData(allRawData, false); // No append for backfill
    if (values.length === 0) return { rowCount: 0 };

    const fields = ['ts', 'symbol', 'exchange', 'perpspec', 'o', 'h', 'l', 'c', 'v', 'oi', 'pfr', 'lsr', 'rsi1', 'rsi60', 'tbv', 'tsv', 'lql', 'lqs', 'notes'];

    // Partial update with COALESCE (additive)
    const updateClause = `
      perpspec = COALESCE(perp_data.perpspec, '[]'::jsonb) || EXCLUDED.perpspec,
      o = COALESCE(EXCLUDED.o, perp_data.o),
      h = COALESCE(EXCLUDED.h, perp_data.h),
      l = COALESCE(EXCLUDED.l, perp_data.l),
      c = COALESCE(EXCLUDED.c, perp_data.c),
      v = COALESCE(EXCLUDED.v, perp_data.v),
      oi = COALESCE(EXCLUDED.oi, perp_data.oi),
      pfr = COALESCE(EXCLUDED.pfr, perp_data.pfr),
      lsr = COALESCE(EXCLUDED.lsr, perp_data.lsr),
      rsi1 = COALESCE(EXCLUDED.rsi1, perp_data.rsi1),
      rsi60 = COALESCE(EXCLUDED.rsi60, perp_data.rsi60),
      tbv = COALESCE(EXCLUDED.tbv, perp_data.tbv),
      tsv = COALESCE(EXCLUDED.tsv, perp_data.tsv),
      lql = COALESCE(EXCLUDED.lql, perp_data.lql),
      lqs = COALESCE(EXCLUDED.lqs, perp_data.lqs),
      notes = COALESCE(EXCLUDED.notes, perp_data.notes)
    `;

    let totalRowCount = 0;
    // Insert in chunks of 100k (big loop for high flow; retry per chunk)
    const chunkSize = 50000;
    for (let i = 0; i < values.length; i += chunkSize) {
      const chunk = values.slice(i, i + chunkSize);
      const chunkQuery = format(
        `INSERT INTO perp_data (${fields.join(', ')})
         VALUES %L
         ON CONFLICT (ts, symbol, exchange) DO UPDATE SET ${updateClause}`,
        chunk
      );
      try {
        const chunkResult = await this.insertWithRetry(chunkQuery);
        totalRowCount += chunkResult.rowCount || chunk.length;
        await new Promise(r => setTimeout(r, 50));  // Tiny pause (reduces burst, no slowdown)
      } catch (error) {
        // Per-chunk error catch: Log for perpspec/symbol (no full abort, continue other chunks/symbols)
        console.error(`Insert failed for chunk [${i}-${i + chunk.length}] (perpspec impact): ${error.message}`);
        await this.logError('dbsetup', 'DB', 'INSERT_CHUNK_FAIL', `Chunk insert failed: ${error.message}`, {
          chunkStart: i,
          chunkSize: chunk.length,
          errorCode: error.code || 'UNKNOWN'
        });
        // Continue to next chunk (doesn't halt script/backfill)
      }
    }

    return { rowCount: totalRowCount };
  }

  // insertMetrics: Unchanged (full update for metrics)
  async insertMetrics(metricsArray) {
    if (!Array.isArray(metricsArray) || metricsArray.length === 0) return { rowCount: 0 };
    const cols = Object.keys(metricsArray[0]);
    const values = metricsArray.map(m => cols.map(c =>
      c === 'window_sizes' ? `{${m[c].join(',')}}` : m[c] ?? null
    ));
    const query = format(
      `INSERT INTO perp_metrics (${cols.join(', ')})
       VALUES %L
       ON CONFLICT (ts, symbol, exchange) DO UPDATE SET
       ${cols.filter(c => !['ts','symbol','exchange'].includes(c)).map(c => `${c}=EXCLUDED.${c}`).join(', ')}`,
      values
    );
    return await this.pool.query(query);
  }

  // queryPerpData: Unified schema query (no interval/source filter)
  async queryPerpData(exchange, symbol, startTs, endTs) {
    if (!exchange || !symbol || startTs == null || endTs == null) {
      console.warn(`Invalid query parameters for queryPerpData: exchange=${exchange}, symbol=${symbol}, startTs=${startTs}, endTs=${endTs}`);
      return [];
    }

    const query = `
      SELECT ts, symbol, exchange, perpspec, o, h, l, c, v, oi, pfr, lsr, rsi1, rsi60, tbv, tsv, lql, lqs, notes
      FROM perp_data
      WHERE exchange = $1 AND symbol = $2 AND ts >= $3 AND ts < $4
      ORDER BY ts ASC
    `;

    try {
      const result = await this.pool.query(query, [exchange, symbol, BigInt(startTs), BigInt(endTs)]);
      return result.rows.map(row => ({
        ...row,
        ts: Number(row.ts),
        v: row.v ? Number(row.v) : null,
        c: row.c ? Number(row.c) : null,
        o: row.o ? Number(row.o) : null,
        h: row.h ? Number(row.h) : null,
        l: row.l ? Number(row.l) : null,
        oi: row.oi ? Number(row.oi) : null,
        pfr: row.pfr ? Number(row.pfr) : null,
        lsr: row.lsr ? Number(row.lsr) : null,
        rsi1: row.rsi1 ? Number(row.rsi1) : null,
        rsi60: row.rsi60 ? Number(row.rsi60) : null,
        tbv: row.tbv ? Number(row.tbv) : null,
        tsv: row.tsv ? Number(row.tsv) : null,
        lql: row.lql ? Number(row.lql) : null,
        lqs: row.lqs ? Number(row.lqs) : null,
        notes: row.notes || null,
        perpspec: row.perpspec || null
      }));
    } catch (error) {
      console.error(`Error querying perp_data for exchange='${exchange}', symbol='${symbol}':`, error.message);
      return [];
    }
  }

  // --------------------------------------------------------------------------
  // LOGGING
  // --------------------------------------------------------------------------
  async logStatus(script, status, msg, details = null) {
    await this.pool.query(
      `INSERT INTO perp_status (script_name, status, message, details, ts)
       VALUES ($1, $2, $3, $4, NOW())`,
      [script, status, msg, details]
    );
  }

  async logError(script, type, code, msg, details = null) {
    await this.pool.query(
      `INSERT INTO perp_errors (script_name, error_type, error_code, error_message, details, ts)
       VALUES ($1, $2, $3, $4, $5, NOW())`,
      [script, type, code, msg, details]
    );
  }
}

// ============================================================================
// EXPORT SINGLETON
// ============================================================================
const dbManager = new DatabaseManager();
module.exports = dbManager;

// ============================================================================
// CLI EXECUTION (UPDATED: Explicit noDrop=false for setup)
if (require.main === module) {
  (async () => {
    try {
      await dbManager.initialize(false); // UPDATED: Explicit for CLI (full setup)
      await dbManager.close(); // Graceful exit
    } catch (err) {
      console.error('❌ Database setup failed:', err);
      process.exit(1);
    }
  })();
}
