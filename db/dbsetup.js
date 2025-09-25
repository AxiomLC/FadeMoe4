const { Pool } = require('pg');
const format = require('pg-format');
require('dotenv').config();

class DatabaseManager {
    constructor() {
        this.pool = new Pool({
            host: process.env.DB_HOST,
            port: process.env.DB_PORT,
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
            database: process.env.DB_NAME
        });
    }

    // ------------------------------------------------------------------------
    //  CORE DATABASE INITIALIZATION (RUN ONCE)
    // ------------------------------------------------------------------------

    async initialize() {
        console.log('⚙️ Setting up database...');
        await this.dropExistingTables();
        await this.enableExtensions();
        await this.createCoreTables();
        console.log('✅ Database setup complete. Success.');
    }

    async dropExistingTables() {
        const tablesToDrop = ['perp_data', 'perp_status', 'perp_errors', 'perpspec_schema'];
        for (const table of tablesToDrop) {
            try {
                await this.pool.query(`DROP TABLE IF EXISTS ${table}`);
                console.log(`  - Dropped table: ${table}`);
            } catch (error) {
                console.error(`  - Error dropping table ${table}: ${error.message}`);
            }
        }
    }

    async enableExtensions() {
        try {
            await this.pool.query('CREATE EXTENSION IF NOT EXISTS timescaledb');
            console.log('  - TimescaleDB extension enabled.');
        } catch (error) {
            console.log(`  - TimescaleDB extension already exists or warning: ${error.message}`);
        }
    }

    async createCoreTables() {
        // Core time-series table
        try {
            await this.pool.query(`
                CREATE TABLE IF NOT EXISTS perp_data (
                    ts TIMESTAMP NOT NULL,
                    symbol TEXT NOT NULL,
                    source TEXT NOT NULL,
                    interval TEXT DEFAULT '1min',
                    PRIMARY KEY (ts, symbol, source)
                )
            `);
            console.log('  - Created/updated perp_data table.');
        } catch (error) {
            console.error(`  - Error creating/updating perp_data table: ${error.message}`);
        }

        // Hypertable creation
        try {
            await this.pool.query(`SELECT create_hypertable('perp_data', 'ts', if_not_exists => TRUE)`);
            console.log('  - Created perp_data hypertable.');
        } catch (error) {
            console.error(`  - Error creating hypertable: ${error.message}`);
        }

        // Status tracking table
        try {
            await this.pool.query(`
                CREATE TABLE IF NOT EXISTS perp_status (
                    task_id SERIAL PRIMARY KEY,
                    script_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    ts_completed TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            `);
            console.log('  - Created perp_status table.');
        } catch (error) {
            console.error(`  - Error creating perp_status table: ${error.message}`);
        }

        // Error logging table
        try {
            await this.pool.query(`
                CREATE TABLE IF NOT EXISTS perp_errors (
                    error_id SERIAL PRIMARY KEY,
                    script_name TEXT,
                    error_type TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    details JSONB,
                    ts TIMESTAMP DEFAULT NOW()
                )
            `);
            console.log('  - Created perp_errors table.');
        } catch (error) {
            console.error(`  - Error creating perp_errors table: ${error.message}`);
        }

        // perpspec_schema table
        try {
            await this.pool.query(`
                CREATE TABLE IF NOT EXISTS perpspec_schema (
                    perpspec_name TEXT PRIMARY KEY,
                    fields JSONB NOT NULL,
                    last_updated TIMESTAMP DEFAULT NOW()
                )
            `);
            console.log('  - Created perpspec_schema table.');
        } catch (error) {
            console.error(`  - Error creating perpspec_schema table: ${error.message}`);
        }
    }

    // Helper to get column type based on value (simplified for this context)
    getColumnType(value) {
        if (value === null || value === undefined) return 'TEXT';
        if (typeof value === 'number') return Number.isInteger(value) ? 'BIGINT' : 'NUMERIC(20, 8)';
        if (typeof value === 'boolean') return 'BOOLEAN';
        if (typeof value === 'object') return 'JSONB';
        return 'TEXT';
    }

    // ------------------------------------------------------------------------
    //  UNIVERSAL DATA INSERTION (SHARED BY ALL SCRIPTS)
    // ------------------------------------------------------------------------

    async insertData(perpspecName, dataArray) {
        if (!Array.isArray(dataArray) || dataArray.length === 0) {
            console.log('  - ⚠️ No data to insert.');
            return;
        }

        const firstItem = dataArray[0];
        const expectedFields = Object.keys(firstItem);

        try {
            const insertColumnNames = expectedFields.join(', ');
            const values = dataArray.map(item => expectedFields.map(field => item[field]));

            const query = format(`
                INSERT INTO perp_data (${insertColumnNames})
                VALUES %L
                ON CONFLICT (ts, symbol, source) DO NOTHING
            `, values);

            await this.pool.query(query);
            console.log(`  - Inserted ${dataArray.length} records for perpspec '${perpspecName}'.`);
        } catch (error) {
            console.error(`  - Error inserting data for perpspec '${perpspecName}': ${error.message}`);
            throw error;
        }
    }

    // Logging methods remain simple console logs
    async logStatus(scriptName, status, message, details = null) {
        console.log(`  - Status: ${scriptName} - ${status} - ${message}`);
    }

    async logError(scriptName, errorType, errorCode, errorMessage, details = null) {
        console.error(`  - Error: ${scriptName} - ${errorCode} - ${errorMessage}`);
        if (details) {
            console.error(`    Details: ${JSON.stringify(details)}`);
        }
    }
}

const dbManager = new DatabaseManager();
module.exports = dbManager;

// --- MODIFIED PART ---
// Only call initialize() if this script is run directly (i.e., not required by another module)
if (require.main === module) {
    async function runSetup() {
        try {
            await dbManager.initialize();
        } catch (error) {
            console.error('❌ Database setup failed:', error);
            process.exit(1); // Exit with an error code
        }
    }
    runSetup();
}