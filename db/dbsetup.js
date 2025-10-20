// db/dbsetup.js 15 Oct 2025
// ============================================================================
// DATABASE SETUP & MANAGER
// Handles all PostgreSQL + TimescaleDB operations for perpetual futures data
// ============================================================================
const { Pool } = require('pg');
const format = require('pg-format');
require('dotenv').config();

// ============================================================================
// USER CONFIGURATION
// ============================================================================
const DB_RETENTION_DAYS = 10; // Must match calc-metrics.js

// ============================================================================
// DATABASE MANAGER CLASS
// ============================================================================
class DatabaseManager {
  constructor() {
    this.pool = new Pool({
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
    });
  }

  // --------------------------------------------------------------------------
  // INITIALIZATION
  // --------------------------------------------------------------------------
  async initialize() {
    console.log('⚙️ Setting up database...');
    await this.dropExistingTables();
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
          lqside VARCHAR(10),
          lqprice NUMERIC(20,8),
          lqqty NUMERIC(20,8),
          notes TEXT,
          PRIMARY KEY (ts, symbol, exchange)
        )`
      );
      await this.pool.query(`SELECT create_hypertable('perp_data', 'ts', if_not_exists => TRUE)`);
      console.log('  - Created table: perp_data');

      // ===== 19 OCt Add indexes for filtering by symbol and exchange
      await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_perp_data_symbol ON perp_data (symbol)`);
      await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_perp_data_exchange ON perp_data (exchange)`);
      console.log('  - Created indexes on perp_data (symbol, exchange)');

      // perp_metrics: Includes exchange column in PK; % change columns for 1m/5m/10m windows
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS perp_metrics (
          ts BIGINT NOT NULL,
          symbol TEXT NOT NULL,
          exchange TEXT NOT NULL,
          window_sizes SMALLINT[] DEFAULT '{1,5,10}',
          o NUMERIC(20,8), h NUMERIC(20,8), l NUMERIC(20,8), c NUMERIC(20,8),
          v NUMERIC(20,8), oi NUMERIC(20,8), pfr NUMERIC(20,8), lsr NUMERIC(20,8),
          rsi1 NUMERIC(10,4), rsi60 NUMERIC(10,4),
          tbv NUMERIC(20,8), tsv NUMERIC(20,8),
          lqside VARCHAR(10), lqprice NUMERIC(20,8), lqqty NUMERIC(20,8),

          -- % change columns (1m/5m/10m)
          c_chg_1m NUMERIC(6,4), v_chg_1m NUMERIC(6,4), oi_chg_1m NUMERIC(6,4),
          pfr_chg_1m NUMERIC(6,4), lsr_chg_1m NUMERIC(6,4),
          rsi1_chg_1m NUMERIC(6,4), rsi60_chg_1m NUMERIC(6,4),
          tbv_chg_1m NUMERIC(6,4), tsv_chg_1m NUMERIC(6,4),
          lqside_chg_1m VARCHAR(10), lqprice_chg_1m NUMERIC(6,4), lqqty_chg_1m NUMERIC(6,4),

          c_chg_5m NUMERIC(6,4), v_chg_5m NUMERIC(6,4), oi_chg_5m NUMERIC(6,4),
          pfr_chg_5m NUMERIC(6,4), lsr_chg_5m NUMERIC(6,4),
          rsi1_chg_5m NUMERIC(6,4), rsi60_chg_5m NUMERIC(6,4),
          tbv_chg_5m NUMERIC(6,4), tsv_chg_5m NUMERIC(6,4),
          lqside_chg_5m VARCHAR(10), lqprice_chg_5m NUMERIC(6,4), lqqty_chg_5m NUMERIC(6,4),
          
          c_chg_10m NUMERIC(6,4), v_chg_10m NUMERIC(6,4), oi_chg_10m NUMERIC(6,4),
          pfr_chg_10m NUMERIC(6,4),
          lsr_chg_10m NUMERIC(6,4), rsi1_chg_10m NUMERIC(6,4), rsi60_chg_10m NUMERIC(6,4),
          tbv_chg_10m NUMERIC(6,4), tsv_chg_10m NUMERIC(6,4),
          lqside_chg_10m VARCHAR(10), lqprice_chg_10m NUMERIC(6,4), lqqty_chg_10m NUMERIC(6,4),
          PRIMARY KEY (ts, symbol, exchange)
        )`
      );
      await this.pool.query(`SELECT create_hypertable('perp_metrics', 'ts', if_not_exists => TRUE)`);
      console.log('  - Created table: perp_metrics');

      //************************************************************************* */
      // Indexes for performance (added 18 Oct)
      await this.pool.query(`
        CREATE INDEX IF NOT EXISTS idx_pfr_chg_5m 
        ON perp_metrics(pfr_chg_5m);
      `);
      await this.pool.query(`
        CREATE INDEX IF NOT EXISTS idx_oi_chg_5m 
        ON perp_metrics(oi_chg_5m);
      `);
      await this.pool.query(`
        CREATE INDEX IF NOT EXISTS idx_v_chg_10m 
        ON perp_metrics(v_chg_10m);
      `);
      console.log('  - Created indexes on perp_metrics');


      //=======================================================

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

      // perpspec_schema: Unchanged (legacy for UI if needed)
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
      throw error;  // Re-throw to fail initialization
    }
  }

  // --------------------------------------------------------------------------
  // SCHEMA REGISTRY
  // --------------------------------------------------------------------------
  async insertFixedPerpspecSchemas() {
    const perpspecs = [
      'bin', 'byb', 'okx'
    ];
    const types = ['ohlcv', 'oi', 'pfr', 'lsr', 'tv', 'lq'];

    const schemas = [];
    for (const p of perpspecs) {
      schemas.push({ name: `${p}-ohlcv`, fields: ['ts','symbol','source','perpspec','interval','o','h','l','c','v'] });
      schemas.push({ name: `${p}-oi`, fields: ['ts','symbol','source','perpspec','interval','oi'] });
      schemas.push({ name: `${p}-pfr`, fields: ['ts','symbol','source','perpspec','interval','pfr'] });
      schemas.push({ name: `${p}-lsr`, fields: ['ts','symbol','source','perpspec','interval','lsr'] });
      schemas.push({ name: `${p}-tv`, fields: ['ts','symbol','source','perpspec','interval','tbv','tsv'] });
      schemas.push({ name: `${p}-lq`, fields: ['ts','symbol','source','perpspec','interval','lqside','lqprice','lqqty'] });
    }
    schemas.push({ name: 'bin-rsi', fields: ['ts','symbol','source','perpspec','interval','rsi1','rsi60'] }); // RSI changed to bin-rsi 18 Oct

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
      await this.pool.query(`SELECT remove_retention_policy('perp_data')`);
      await this.pool.query(`SELECT remove_retention_policy('perp_metrics')`);
      await this.pool.query(`SELECT add_retention_policy('perp_data', INTERVAL '${DB_RETENTION_DAYS} days')`);
      await this.pool.query(`SELECT add_retention_policy('perp_metrics', INTERVAL '${DB_RETENTION_DAYS} days')`);
      console.log(`  - Retention set to ${DB_RETENTION_DAYS} days`);
    } catch (err) {
      console.error(`  - Error setting retention: ${err.message}`);
    }
  }

  // --------------------------------------------------------------------------
  // DATA OPERATIONS
  // --------------------------------------------------------------------------
  // Updated to merge perpspec data into unified rows per (ts, symbol, exchange); keep perpspec for frontend
  async insertData(allRawData) {  // Now takes all perpspec data at once (e.g., from API batch)
  if (!Array.isArray(allRawData) || allRawData.length === 0) return { rowCount: 0 };

  // Helper to extract exchange from perpspec (add if not present)
  function getExchangeFromPerpspec(perpspec) {
    if (perpspec.startsWith('bin-')) return 'bin';
    if (perpspec.startsWith('byb-')) return 'byb';
    if (perpspec.startsWith('okx-')) return 'okx';
    return null;
  }

  // Merge into unified rows
  const merged = new Map();  // Key: `${ts}_${symbol}_${exchange}`

  for (const record of allRawData) {
    const ts = record.ts;
    const symbol = record.symbol;
    let perpspec = record.perpspec;  // Keep for frontend
    const exchange = getExchangeFromPerpspec(perpspec);
    if (!exchange) continue;  // Skip invalid
    const key = `${ts}_${symbol}_${exchange}`;

    // Ensure perpspec is a JSONB array (e.g., if single string like 'bin-ohlcv', wrap as ['bin-ohlcv'])
    if (typeof perpspec === 'string') {
      perpspec = [perpspec];  // Align with README: array of full perpspec names
    }

    if (!merged.has(key)) {
      merged.set(key, {
        ts,
        symbol,
        exchange,
        perpspec: perpspec || ['unknown'],  // Populate perpspec (e.g., default to input or 'unified')
        o: null, h: null, l: null, c: null, v: null,
        oi: null, pfr: null, lsr: null,
        rsi1: null, rsi60: null,
        tbv: null, tsv: null,
        lqside: null, lqprice: null, lqqty: null,
        notes: record.notes || null  // Optional notes from API (e.g., 'farcaster')
      });
    }
//=============================================================
    const row = merged.get(key);
    // Populate based on perpspec (use original record.perpspec string for substring checks)
    if (record.perpspec && record.perpspec.includes('ohlcv')) {
      row.o = record.o; row.h = record.h; row.l = record.l;
      row.c = record.c; row.v = record.v;
      row.perpspec = perpspec;  // Prioritize ohlcv as default perpspec for unified row (wrapped array)
    }
    if (record.perpspec && record.perpspec.includes('oi')) row.oi = record.oi;
    if (record.perpspec && record.perpspec.includes('pfr')) row.pfr = record.pfr;
    if (record.perpspec && record.perpspec.includes('lsr')) row.lsr = record.lsr;
    if (record.perpspec && record.perpspec.includes('rsi')) { row.rsi1 = record.rsi1; row.rsi60 = record.rsi60; }
    if (record.perpspec && record.perpspec.includes('tv')) { row.tbv = record.tbv; row.tsv = record.tsv; }
    if (record.perpspec && record.perpspec.includes('lq')) {
      row.lqside = record.lqside; row.lqprice = record.lqprice; row.lqqty = record.lqqty;
    }
    // Update notes if provided
    if (record.notes) row.notes = record.notes;
  }
//=================================================================
  // Upsert merged rows (include perpspec in fields/values)
  const mergedArray = Array.from(merged.values());
  if (mergedArray.length === 0) return { rowCount: 0 };

  const fields = ['ts', 'symbol', 'exchange', 'perpspec', 'o', 'h', 'l', 'c', 'v', 'oi', 'pfr', 'lsr', 'rsi1', 'rsi60', 'tbv', 'tsv', 'lqside', 'lqprice', 'lqqty', 'notes'];

  // FIX: Explicitly format values for pg-format %L: ts as string, perpspec as JSON string, others as-is/null
  const values = mergedArray.map(row => {
    return fields.map(f => {
      let val = row[f] ?? null;
      if (f === 'ts' && typeof val === 'bigint') {
        val = val.toString();  // BigInt → string for BIGINT column (pg-format quotes it)
      } else if (f === 'perpspec' && Array.isArray(val)) {
        val = JSON.stringify(val);  // Array → '"[\"bin-ohlcv\"]" ' for JSONB (pg-format quotes outer)
      }
      // NUMERIC fields: Ensure numbers are not NaN/null
      if (['o','h','l','c','v','oi','pfr','lsr','rsi1','rsi60','tbv','tsv','lqprice','lqqty'].includes(f) && typeof val === 'number' && isNaN(val)) {
        val = null;
      }
      return val;
    });
  });

  const query = format(
    `INSERT INTO perp_data (${fields.join(', ')})
     VALUES %L
     ON CONFLICT (ts, symbol, exchange) DO UPDATE SET
       perpspec = EXCLUDED.perpspec, o = EXCLUDED.o, h = EXCLUDED.h, l = EXCLUDED.l, c = EXCLUDED.c, v = EXCLUDED.v,
       oi = EXCLUDED.oi, pfr = EXCLUDED.pfr, lsr = EXCLUDED.lsr,
       rsi1 = EXCLUDED.rsi1, rsi60 = EXCLUDED.rsi60,
       tbv = EXCLUDED.tbv, tsv = EXCLUDED.tsv,
       lqside = EXCLUDED.lqside, lqprice = EXCLUDED.lqprice, lqqty = EXCLUDED.lqqty,
       notes = COALESCE(EXCLUDED.notes, perp_data.notes)`,  // Preserve notes
    values
  );

  // TEMP DEBUG: Log first row's formatted values (remove after testing)
  if (values.length > 0) {
    console.log(`[DB INSERT DEBUG] First row: ts='${values[0][0]}', perpspec='${values[0][3]}', o=${values[0][4]}, exchange='${values[0][2]}'`);
  }

  const result = await this.pool.query(query);
  console.log(`[DB INSERT] Successfully inserted/updated ${result.rowCount} rows from ${mergedArray.length} prepared`);
  return result;
}

  //++++++++++++++++++++++++++++++==================================================
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

  // Updated query for unified schema (include perpspec for frontend, no interval/source)
  async queryPerpData(exchange, symbol, startTs, endTs) {
    if (!exchange || !symbol || startTs == null || endTs == null) {
        console.warn(`Invalid query parameters for queryPerpData: exchange=${exchange}, symbol=${symbol}, startTs=${startTs}, endTs=${endTs}`);
        return [];
    }

    const query = `
        SELECT ts, symbol, exchange, perpspec, o, h, l, c, v, oi, pfr, lsr, rsi1, rsi60, tbv, tsv, lqside, lqprice, lqqty, notes
        FROM perp_data
        WHERE exchange = $1
          AND symbol = $2
          AND ts >= $3
          AND ts < $4
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
            lqside: row.lqside ? String(row.lqside) : null,
            lqprice: row.lqprice ? Number(row.lqprice) : null,
            lqqty: row.lqqty ? Number(row.lqqty) : null,
            notes: row.notes || null,
            perpspec: row.perpspec || null  // Include for frontend
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
// CLI EXECUTION
// ============================================================================
if (require.main === module) {
  (async () => {
    try {
      await dbManager.initialize();
    } catch (err) {
      console.error('❌ Database setup failed:', err);
      process.exit(1);
    }
  })();
}