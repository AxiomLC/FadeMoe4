// db/dbsetup2.js 22 Oct 2025
// ============================================================================
// ADDENDUM SCRIPT: Safely Add New Fields to Existing Tables (No Drop/Re-create)
// Usage: node db/dbsetup2.js [options]
// Examples:
//   - Add field: node dbsetup2.js --table=perp_data --field=new_metric --type=NUMERIC(20,8)
//   - With perpspec: node dbsetup2.js --table=perp_data --field=new_metric --type=NUMERIC(20,8) --perpspec=bin-new --fields='["ts","symbol","exchange","new_metric"]'
// Options:
//   --table: Table name (default: perp_data)
//   --field: New column name (required)
//   --type: SQL type (e.g., NUMERIC(20,8), TEXT; required)
//   --perpspec: New perpspec name (optional; updates perpspec_schema)
//   --fields: JSON array of fields for perpspec_schema (optional; e.g., '["ts","new_field"]')
//   --index: Add index on new field? (true/false; default: true for NUMERIC/TEXT)
// ============================================================================
const { Pool } = require('pg');
require('dotenv').config();
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const argv = yargs(hideBin(process.argv))
  .option('table', { default: 'perp_data', describe: 'Target table (e.g., perp_data, perp_metrics)' })
  .option('field', { demandOption: true, describe: 'New column name' })
  .option('type', { demandOption: true, describe: 'SQL column type (e.g., NUMERIC(20,8))' })
  .option('perpspec', { describe: 'New perpspec name for schema (optional)' })
  .option('fields', { describe: 'JSON array of fields for perpspec_schema (optional)' })
  .option('index', { default: true, type: 'boolean', describe: 'Add index on new field?' })
  .help()
  .argv;

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

// ============================================================================
// MAIN ADDENDUM LOGIC
// ============================================================================
async function addField() {
  const { table, field, type, perpspec, fields: fieldsStr, index } = argv;
  console.log(`🔧 Adding field '${field}' (${type}) to table '${table}'...`);

  try {
    // Step 1: Check if column already exists
    const checkColQuery = `
      SELECT column_name FROM information_schema.columns 
      WHERE table_name = $1 AND column_name = $2
    `;
    const colExists = await pool.query(checkColQuery, [table, field]);
    if (colExists.rows.length > 0) {
      console.log(`  - Column '${field}' already exists in '${table}'. Skipping.`);
      return;
    }

    // Step 2: Add the column (safe ALTER; defaults to NULL)
    const alterQuery = `ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS ${field} ${type}`;
    await pool.query(alterQuery);
    console.log(`  - Added column '${field}' (${type}) to '${table}' (defaults to NULL).`);

    // Step 3: Add index if requested (for queryable fields like NUMERIC/TEXT)
    if (index) {
      const indexName = `idx_${table}_${field}`;
      const indexQuery = `CREATE INDEX IF NOT EXISTS ${indexName} ON ${table} (${field})`;
      await pool.query(indexQuery);
      console.log(`  - Added index '${indexName}' on '${field}'.`);
    }

    // Step 4: Update perpspec_schema if perpspec provided (legacy UI support)
    if (perpspec) {
      let schemaFields = fieldsStr ? JSON.parse(fieldsStr) : ['ts', 'symbol', 'exchange', field];
      const schemaQuery = `
        INSERT INTO perpspec_schema (perpspec_name, fields)
        VALUES ($1, $2)
        ON CONFLICT (perpspec_name) DO UPDATE SET
          fields = EXCLUDED.fields, last_updated = NOW()
      `;
      await pool.query(schemaQuery, [perpspec, JSON.stringify(schemaFields)]);
      console.log(`  - Registered/updated perpspec_schema for '${perpspec}' with fields: ${JSON.stringify(schemaFields)}.`);
    }

    // Step 5: Log schema backup (simple dump of table structure for records)
    const descQuery = `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`;
    const desc = await pool.query(descQuery, [table]);
    console.log(`  - Updated schema for '${table}':\n${JSON.stringify(desc.rows, null, 2)}`);

    console.log(`✅ Field addition complete for '${table}'. No data loss; existing rows have NULL in new column.`);
    console.log(`💡 Next: Update insertData/insertBackfillData merge logic to populate '${field}' based on perpspec (e.g., if (perpspec.includes('new')) row.${field} = record.${field};`);
    console.log(`   Re-run backfill scripts if needed to populate historical data.`);

  } catch (error) {
    console.error(`❌ Error adding field: ${error.message}`);
    if (error.code === '42P01') console.log(`  - Table '${table}' does not exist. Run dbsetup.js first.`);
    else if (error.code === '42703') console.log(`  - Invalid type or field name. Check SQL syntax.`);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

// Run if direct
if (require.main === module) {
  addField();
}

module.exports = { addField };