// db_manual_migration.js

const { Pool } = require('pg');
require('dotenv').config();

class MigrationManager {
    constructor() {
        this.pool = new Pool({
            host: process.env.DB_HOST,
            port: process.env.DB_PORT,
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
            database: process.env.DB_NAME
        });
    }

    /**
     * **MANUAL ENTRY ZONE: ADD NEW COLUMNS TO perp_data TABLE**
     *
     * This function checks if a column exists and only adds it if it doesn't.
     * This makes the script safe to re-run without errors.
     */
    async addColumnToPerpData(columnName, columnType) {
        // Step 1: Check if the column already exists
        const checkQuery = `
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'perp_data'
            AND column_name = $1;
        `;
        const checkResult = await this.pool.query(checkQuery, [columnName]);

        if (checkResult.rows.length === 0) {
            // Step 2: Add the column if it does not exist
            const alterQuery = `
                ALTER TABLE perp_data
                ADD COLUMN ${columnName} ${columnType};
            `;
            try {
                await this.pool.query(alterQuery);
                console.log(`✅ Added new column to perp_data: ${columnName} ${columnType}`);
            } catch (error) {
                console.error(`❌ Error adding column ${columnName}: ${error.message}`);
                // Throw error to stop migration if ALTER fails
                throw error;
            }
        } else {
            console.log(`➡️ Column already exists in perp_data: ${columnName}`);
        }
    }

    /**
     * **MANUAL ENTRY ZONE: INSERT/UPDATE NEW PERPSPEC SCHEMAS**
     *
     * This function uses ON CONFLICT DO UPDATE, which means:
     * - If the 'perpspec_name' doesn't exist, it is inserted (NEW SCHEMA).
     * - If the 'perpspec_name' exists, its 'fields' and 'last_updated' are updated (UPDATE SCHEMA).
     */
    async upsertPerpspecSchema(perpspecName, fieldsArray) {
        const query = `
            INSERT INTO perpspec_schema (perpspec_name, fields)
            VALUES ($1, $2)
            ON CONFLICT (perpspec_name) DO UPDATE SET
               fields = EXCLUDED.fields,
               last_updated = NOW()
        `;
        try {
            await this.pool.query(query, [perpspecName, JSON.stringify(fieldsArray)]);
            console.log(`✅ Upserted perpspec_schema for '${perpspecName}'.`);
        } catch (error) {
            console.error(`❌ Error upserting perpspec_schema for '${perpspecName}': ${error.message}`);
            // Throw error to stop migration
            throw error;
        }
    }

    /**
     * Run all manual migrations.
     */
    async runMigrations() {
        console.log('--- 🚀 Starting Manual Migrations ---');

        // =========================================================================
        // **1. MANUAL COLUMN ADDITIONS FOR perp_data**
        // =========================================================================

        // **EXAMPLE 1: Add Funding Rate**
        // **Column Name**: funding_rate
        // **SQL Type**: NUMERIC(20, 8) - good for precise decimal values

        // await this.addColumnToPerpData('funding_rate', 'NUMERIC(20, 8)');

        // **EXAMPLE 2: Add Open Interest (Total number of contracts)**
        // **Column Name**: open_interest
        // **SQL Type**: ---- NUMERIC
        await this.addColumnToPerpData('oi', 'NUMERIC(20, 8)');
        await this.addColumnToPerpData('pfr', 'NUMERIC(20, 8)');
        await this.addColumnToPerpData('lq', 'NUMERIC(20, 8)');
        await this.addColumnToPerpData('lsr', 'NUMERIC(20, 8)');

        // **EXAMPLE 3: Add an Exchange-Specific JSON Field (for non-standard data)**
        // **Column Name**: exchange_data
        // **SQL Type**: JSONB - for storing arbitrary structured data

        // await this.addColumnToPerpData('exchange_data', 'JSONB');

        // **-- ADD NEW COLUMNS ABOVE THIS LINE --**
        // =========================================================================


        // =========================================================================
        // **2. MANUAL SCHEMAS FOR perpspec_schema**
        // =========================================================================

        // **NOTE:** Core OHLCV schemas ('bin-ohlcv', 'byb-ohlcv', 'okx-ohlcv')
        // should remain in your main dbsetup.js file.

        // **EXAMPLE 1: -----**
        // **Perpspec Name**: OI
        // **Fields**: Must include all mandatory fields + the new field
        await this.upsertPerpspecSchema('bin-oi', [
            'ts', 'symbol', 'source', 'perpspec', 'interval',
            'oi' // <-- **NEW FIELD HERE**
        ]);
        // **EXAMPLE 2: -------**
        await this.upsertPerpspecSchema('byb-oi', [
            'ts', 'symbol', 'source', 'perpspec', 'interval',
            'oi'
        ]);
        // **EXAMPLE 3: -------**
        await this.upsertPerpspecSchema('okx-oi', [
            'ts', 'symbol', 'source', 'perpspec', 'interval',
            'oi'
        ]);
        //-----------------------------------------------------------------------------------

        // PFR
        await this.upsertPerpspecSchema('bin-pfr', [
            'ts', 'symbol', 'source', 'perpspec', 'interval',
            'pfr' // <-- **NEW FIELD HERE**
        ]);
        // **EXAMPLE 2: -------**
        await this.upsertPerpspecSchema('byb-pfr', [
            'ts', 'symbol', 'source', 'perpspec', 'interval',
            'pfr'
        ]);
        // **EXAMPLE 3: -------**
        await this.upsertPerpspecSchema('okx-pfr', [
            'ts', 'symbol', 'source', 'perpspec', 'interval',
            'pfr'
        ]);
        //-----------------------------------------------------------------------------------

        // ** LQ **
        await this.upsertPerpspecSchema('bin-lq', [
            'ts', 'symbol', 'source', 'perpspec', 'interval',
            'lq' // <-- **NEW FIELD HERE**
        ]);
        // **EXAMPLE 2: -------**
        await this.upsertPerpspecSchema('byb-lq', [
            'ts', 'symbol', 'source', 'perpspec', 'interval',
            'lq'
        ]);
        // **EXAMPLE 3: -------**
        await this.upsertPerpspecSchema('okx-lq', [
            'ts', 'symbol', 'source', 'perpspec', 'interval',
            'lq'
        ]);

        //-----------------------------------------------------------------------------------

        // ** LSR **
        await this.upsertPerpspecSchema('bin-lsr', [
            'ts', 'symbol', 'source', 'perpspec', 'interval',
            'lsr' // <-- **NEW FIELD HERE**
        ]);
        // **EXAMPLE 2: -------**
        await this.upsertPerpspecSchema('byb-lsr', [
            'ts', 'symbol', 'source', 'perpspec', 'interval',
            'lsr'
        ]);
        // **EXAMPLE 3: -------**
        await this.upsertPerpspecSchema('okx-lsr', [
            'ts', 'symbol', 'source', 'perpspec', 'interval',
            'lsr'
        ]);

        // **-- ADD NEW SCHEMAS ABOVE THIS LINE --**
        // =========================================================================

        console.log('--- ✅ Manual Migrations Complete ---');
    }

    async close() {
        await this.pool.end();
    }
}

async function runSetup() {
    const migrationManager = new MigrationManager();
    try {
        await migrationManager.runMigrations();
    } catch (error) {
        console.error('❌ Manual migration failed:', error);
        process.exit(1);
    } finally {
        await migrationManager.close();
    }
}

if (require.main === module) {
    runSetup();
}

// NEW ASYNC function per AI -- test debug error/logging problem ----------------
async function logStatus(scriptName, status, message, details) {
  try {
    console.log(`dbManager.logStatus called: script=${scriptName}, status=${status}, message=${message}`);
    // Example insert query - replace with your actual query
    const query = `INSERT INTO perp_status (script_name, status, message, details) VALUES ($1, $2, $3, $4)`;
    await pool.query(query, [scriptName, status, message, details]);
    console.log(`dbManager.logStatus insert succeeded`);
  } catch (error) {
    console.error(`dbManager.logStatus insert failed:`, error);
    throw error;
  }
}

async function logError(scriptName, errorType, errorCode, errorMessage, details) {
  try {
    console.log(`dbManager.logError called: script=${scriptName}, type=${errorType}, code=${errorCode}, message=${errorMessage}`);
    // Example insert query - replace with your actual query
    const query = `INSERT INTO perp_errors (script_name, error_type, error_code, error_message, details) VALUES ($1, $2, $3, $4, $5)`;
    await pool.query(query, [scriptName, errorType, errorCode, errorMessage, details]);
    console.log(`dbManager.logError insert succeeded`);
  } catch (error) {
    console.error(`dbManager.logError insert failed:`, error);
    throw error;
  }
}