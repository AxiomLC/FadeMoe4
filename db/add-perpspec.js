// db/add-perpspec.js
// ============================================================================
// ADD NEW PERPSPEC WITHOUT FULL DATABASE REBUILD
// Registers a new perpspec schema in the perpspec_schema table
// calc-metrics.js will automatically pick it up on next run
// ============================================================================
// USAGE: node db/add-perpspec.js <perpspec_name> <field1,field2,field3,...>
// EXAMPLE: node db/add-perpspec.js bin-vwap ts,symbol,source,perpspec,interval,vwap
// ============================================================================

const dbManager = require('./dbsetup');

// ============================================================================
// PERPSPEC REGISTRATION
// ============================================================================
async function addPerpspec(perpspecName, fieldsArray) {
  console.log(`\n📝 Registering new perpspec: ${perpspecName}`);
  console.log(`   Fields: ${fieldsArray.join(', ')}`);

  try {
    // Check if perpspec already exists
    const checkQuery = `SELECT perpspec_name FROM perpspec_schema WHERE perpspec_name = $1`;
    const existing = await dbManager.pool.query(checkQuery, [perpspecName]);

    if (existing.rows.length > 0) {
      console.log(`⚠️  Perpspec '${perpspecName}' already exists. Updating fields...`);
    }

    // Insert or update perpspec schema
    const upsertQuery = `
      INSERT INTO perpspec_schema (perpspec_name, fields)
      VALUES ($1, $2)
      ON CONFLICT (perpspec_name) DO UPDATE SET
        fields = EXCLUDED.fields,
        last_updated = NOW()
    `;

    await dbManager.pool.query(upsertQuery, [perpspecName, JSON.stringify(fieldsArray)]);

    console.log(`✅ Perpspec '${perpspecName}' registered successfully!`);
    console.log(`\n📋 NEXT STEPS:`);
    console.log(`   1. Add '${perpspecName}' to PERPSPECS array in calc-metrics.js`);
    console.log(`   2. Ensure data fetcher is populating perp_data with this perpspec`);
    console.log(`   3. Restart calc-metrics.js to begin calculating metrics`);
    console.log(`\n✨ Done!\n`);

  } catch (error) {
    console.error(`❌ Error registering perpspec:`, error.message);
    process.exit(1);
  }
}

// ============================================================================
// CLI EXECUTION
// ============================================================================
if (require.main === module) {
  const args = process.argv.slice(2);

  if (args.length !== 2) {
    console.log(`
❌ Invalid usage!

USAGE:
  node db/add-perpspec.js <perpspec_name> <field1,field2,field3,...>

EXAMPLES:
  node db/add-perpspec.js bin-vwap ts,symbol,source,perpspec,interval,vwap
  node db/add-perpspec.js okx-basis ts,symbol,source,perpspec,interval,basis

NOTES:
  - All perpspecs share the same perp_data table columns
  - Only specify fields that will be populated for this perpspec
  - Standard fields (ts, symbol, source, perpspec, interval) should always be included
  - After registration, add perpspec to PERPSPECS array in calc-metrics.js
    `);
    process.exit(1);
  }

  const perpspecName = args[0];
  const fieldsArray = args[1].split(',').map(f => f.trim());

  // Validate field names match perp_data columns
  const validColumns = [
    'ts', 'symbol', 'perpspec', 'source', 'interval',
    'o', 'h', 'l', 'c', 'v', 'oi', 'pfr', 'lsr',
    'rsi1', 'rsi60', 'tbv', 'tsv', 'lqside', 'lqprice', 'lqqty'
  ];

  const invalidFields = fieldsArray.filter(f => !validColumns.includes(f));
  if (invalidFields.length > 0) {
    console.error(`\n❌ Invalid field names: ${invalidFields.join(', ')}`);
    console.error(`\nValid columns in perp_data table:`);
    console.error(`  ${validColumns.join(', ')}\n`);
    process.exit(1);
  }

  addPerpspec(perpspecName, fieldsArray)
    .then(() => process.exit(0))
    .catch(err => {
      console.error('❌ Failed to add perpspec:', err);
      process.exit(1);
    });
}

module.exports = { addPerpspec };