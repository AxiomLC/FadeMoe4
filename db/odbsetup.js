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
    // perp_data
    await this.pool.query(
      `CREATE TABLE IF NOT EXISTS perp_data (
        ts BIGINT NOT NULL,
        symbol TEXT NOT NULL,
        perpspec TEXT NOT NULL,
        source TEXT,
        interval TEXT DEFAULT '1min',
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
        PRIMARY KEY (ts, symbol, perpspec)
      )`
    );
    await this.pool.query(`SELECT create_hypertable('perp_data', 'ts', if_not_exists => TRUE)`);

// ...  EDITED 17 Oct  ...
    // perp_metrics
    await this.pool.query(
      `CREATE TABLE IF NOT EXISTS perp_metrics (
        ts BIGINT NOT NULL,
        symbol TEXT NOT NULL,
        exchange TEXT NOT NULL,  -- ADDED exchange column
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
        PRIMARY KEY (ts, symbol, exchange)  -- MODIFIED primary key
      )`
    );
    await this.pool.query(`SELECT create_hypertable('perp_metrics', 'ts', if_not_exists => TRUE)`);

    //+++++++++++++INDEXES added 18 Oct +++++++++++++++++++++++++++++++++
    // Just create basic indexes without WHERE clause
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
//+++++++++++++INDEXES above +++++++++++++++++++++++++++++++++

// ... 
    // perp_status
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

    // perp_errors
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

    // perpspec_schema
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS perpspec_schema (
        perpspec_name TEXT PRIMARY KEY,
        fields JSONB NOT NULL,
        last_updated TIMESTAMP DEFAULT NOW()
      )
    `);
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
    schemas.push({ name: 'bin-rsi', fields: ['ts','symbol','source','perpspec','interval','rsi1','rsi60'] }); //RSI changed to bin-rsi 18 Oct

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
  async insertData(perpspecName, dataArray) {
    if (!Array.isArray(dataArray) || dataArray.length === 0) return { rowCount: 0 };
    const fields = Object.keys(dataArray[0]);
    const values = dataArray.map(item => fields.map(f => item[f]));
    const query = format(
      `INSERT INTO perp_data (${fields.join(', ')})
       VALUES %L
       ON CONFLICT (ts, symbol, perpspec) DO NOTHING`,
      values
    );
    return await this.pool.query(query);
  }

// --------------------------------------------------------------------------
// DATA OPERATIONS
// --------------------EDIT 17 OCT by Grok ------------------------------------------------------
async insertData(perpspecName, dataArray) {
  if (!Array.isArray(dataArray) || dataArray.length === 0) return { rowCount: 0 };
  const fields = Object.keys(dataArray[0]);
  const values = dataArray.map(item => fields.map(f => item[f]));
  const query = format(
    `INSERT INTO perp_data (${fields.join(', ')})
     VALUES %L
     ON CONFLICT (ts, symbol, perpspec) DO NOTHING`,
    values
  );
  return await this.pool.query(query);
}

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

// ... rest of the file unchanged (queryPerpData, logStatus, etc.)

  //====================Query Perp Data added below by Ai 15 Oct for tv files=====================================================

  // Add this method to the DatabaseManager class in dbsetup.js
async queryPerpData(perpspec, symbol, startTs, endTs) {
    if (!perpspec || !symbol || startTs == null || endTs == null) {
        console.warn(`Invalid query parameters for queryPerpData: perpspec=${perpspec}, symbol=${symbol}, startTs=${startTs}, endTs=${endTs}`);
        return [];
    }

    const query = `
        SELECT ts, interval, v, c, o, h, l, oi, pfr, lsr, rsi1, rsi60, tbv, tsv, lqside, lqprice, lqqty
        FROM perp_data
        WHERE perpspec = $1
          AND symbol = $2
          AND ts >= $3
          AND ts < $4
        ORDER BY ts ASC
    `;

    try {
        const result = await this.pool.query(query, [perpspec, symbol, BigInt(startTs), BigInt(endTs)]);
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
        }));
    } catch (error) {
        console.error(`Error querying perp_data for perpspec='${perpspec}', symbol='${symbol}':`, error.message);
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


