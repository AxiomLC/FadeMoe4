// db/dbsetup2.js 22 Oct 2025
// ============================================================================
// SIMPLE ADDENDUM SCRIPT: Hardcoded Partial Unique Index Creation with Hypertable Support
// Run with: node db/dbsetup2.js
//
// This script creates a partial unique index on perp_metrics for skipping rows
// where c_chg_1m is populated, improving backfill efficiency.
//
// Supports TimescaleDB hypertables by avoiding CONCURRENTLY keyword.
//
// Additional example code for adding columns or full indexes is provided as
// commented sections for future use.
// ============================================================================

const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

// === HARD-CODED CONFIGURATION ===
const TABLE_NAME = 'perp_metrics';
const PARTIAL_INDEX_NAME = 'uniq_perp_metrics_null_cchg1m';
const PARTIAL_INDEX_COLUMNS = ['ts', 'symbol', 'exchange'];
const PARTIAL_INDEX_WHERE = 'c_chg_1m IS NULL';

// === MAIN FUNCTION ===
async function main() {
  try {
    console.log(`🔧 Creating partial unique index '${PARTIAL_INDEX_NAME}' on table '${TABLE_NAME}' with columns (${PARTIAL_INDEX_COLUMNS.join(', ')}) WHERE ${PARTIAL_INDEX_WHERE}...`);

    // Check if index exists
    const checkIndexQuery = `
      SELECT 1 FROM pg_indexes WHERE tablename = $1 AND indexname = $2
    `;
    const existsResult = await pool.query(checkIndexQuery, [TABLE_NAME, PARTIAL_INDEX_NAME]);
    if (existsResult.rowCount > 0) {
      console.log(`  - Index '${PARTIAL_INDEX_NAME}' already exists. Skipping creation.`);
      return;
    }

    // Check if table is hypertable (TimescaleDB)
    const hypertableCheckQuery = `
      SELECT hypertable_name FROM timescaledb_information.hypertables WHERE hypertable_name = $1
    `;
    const hypertableResult = await pool.query(hypertableCheckQuery, [TABLE_NAME]);
    const isHypertable = hypertableResult.rowCount > 0;

    // Build create index query
    let createIndexQuery = `CREATE UNIQUE INDEX ${PARTIAL_INDEX_NAME} ON ${TABLE_NAME} (${PARTIAL_INDEX_COLUMNS.join(', ')}) WHERE ${PARTIAL_INDEX_WHERE}`;
    if (!isHypertable) {
      // Use CONCURRENTLY if not hypertable
      createIndexQuery = `CREATE UNIQUE INDEX CONCURRENTLY ${PARTIAL_INDEX_NAME} ON ${TABLE_NAME} (${PARTIAL_INDEX_COLUMNS.join(', ')}) WHERE ${PARTIAL_INDEX_WHERE}`;
    } else {
      console.log('  - Detected hypertable, creating index WITHOUT CONCURRENTLY (required by TimescaleDB).');
    }

    console.log(`  - Executing: ${createIndexQuery}`);
    await pool.query(createIndexQuery);
    console.log(`  - Partial unique index '${PARTIAL_INDEX_NAME}' created successfully.`);

    // ===========================
    // FUTURE: Add Column Example
    // ===========================
    /*
    // To add a new column, uncomment and edit below:
    const NEW_COLUMN_NAME = 'new_metric';
    const NEW_COLUMN_TYPE = 'NUMERIC(20,8)';
    const ADD_INDEX_ON_NEW_COLUMN = true;

    console.log(`🔧 Adding field '${NEW_COLUMN_NAME}' (${NEW_COLUMN_TYPE}) to table '${TABLE_NAME}'...`);

    const checkColQuery = `
      SELECT column_name FROM information_schema.columns 
      WHERE table_name = $1 AND column_name = $2
    `;
    const colExists = await pool.query(checkColQuery, [TABLE_NAME, NEW_COLUMN_NAME]);
    if (colExists.rows.length > 0) {
      console.log(`  - Column '${NEW_COLUMN_NAME}' already exists in '${TABLE_NAME}'. Skipping.`);
    } else {
      const alterQuery = `ALTER TABLE ${TABLE_NAME} ADD COLUMN IF NOT EXISTS ${NEW_COLUMN_NAME} ${NEW_COLUMN_TYPE}`;
      await pool.query(alterQuery);
      console.log(`  - Added column '${NEW_COLUMN_NAME}' (${NEW_COLUMN_TYPE}) to '${TABLE_NAME}' (defaults to NULL).`);

      if (ADD_INDEX_ON_NEW_COLUMN) {
        const indexName = `idx_${TABLE_NAME}_${NEW_COLUMN_NAME}`;
        const indexQuery = `CREATE INDEX IF NOT EXISTS ${indexName} ON ${TABLE_NAME} (${NEW_COLUMN_NAME})`;
        await pool.query(indexQuery);
        console.log(`  - Added index '${indexName}' on '${NEW_COLUMN_NAME}'.`);
      }
    }
    */

    // ===========================
    // FUTURE: Add Full Index Example
    // ===========================
    /*
    // To add a full index, uncomment and edit below:
    const FULL_INDEX_NAME = 'idx_perp_metrics_example';
    const FULL_INDEX_COLUMNS = ['symbol', 'exchange'];

    console.log(`🔧 Creating index '${FULL_INDEX_NAME}' on table '${TABLE_NAME}' with columns (${FULL_INDEX_COLUMNS.join(', ')})...`);

    const checkFullIndexQuery = `
      SELECT 1 FROM pg_indexes WHERE tablename = $1 AND indexname = $2
    `;
    const fullIndexExists = await pool.query(checkFullIndexQuery, [TABLE_NAME, FULL_INDEX_NAME]);
    if (fullIndexExists.rowCount > 0) {
      console.log(`  - Index '${FULL_INDEX_NAME}' already exists. Skipping creation.`);
    } else {
      const createFullIndexQuery = `CREATE INDEX CONCURRENTLY ${FULL_INDEX_NAME} ON ${TABLE_NAME} (${FULL_INDEX_COLUMNS.join(', ')})`;
      console.log(`  - Executing: ${createFullIndexQuery}`);
      await pool.query(createFullIndexQuery);
      console.log(`  - Index '${FULL_INDEX_NAME}' created successfully.`);
    }
    */

  } catch (error) {
    console.error(`❌ Error: ${error.message}`);
    if (error.code === '42P01') console.log(`  - Table '${TABLE_NAME}' does not exist. Run dbsetup.js first.`);
    else if (error.code === '42703') console.log(`  - Invalid type or field name. Check SQL syntax.`);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

// Run if direct
if (require.main === module) {
  main();
}

module.exports = { main };