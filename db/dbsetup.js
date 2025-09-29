const { Pool } = require('pg');
const format = require('pg-format');
require('dotenv').config();

/**
 * DatabaseManager handles all database setup, schema management,
 * and data insertion for the FadeMoe4 platform.
 * 
 * Core tables:
 * - perp_data: unified time-series table keyed by (ts, symbol, source)
 * - perpspec_schema: metadata describing dynamic schema fields per perpspec
 * - perp_status, perp_errors: logging and error tracking
 * 
 * Uses TimescaleDB hypertables for efficient time-series data storage.
 */
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

    /**
     * Initialize the database by dropping existing tables,
     * enabling extensions, creating core tables, and inserting fixed schemas.
     */
    async initialize() {
        console.log('⚙️ Setting up database...');
        await this.dropExistingTables();
        await this.enableExtensions();
        await this.createCoreTables();
        await this.insertFixedPerpspecSchemas();
        console.log('✅ Database setup complete. Success.');
    }

    /**
     * Drop existing tables to reset the database schema.
     */
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

    /**
     * Enable required PostgreSQL extensions, such as TimescaleDB.
     */
    async enableExtensions() {
        try {
            await this.pool.query('CREATE EXTENSION IF NOT EXISTS timescaledb');
            console.log('  - TimescaleDB extension enabled.');
        } catch (error) {
            console.log(`  - TimescaleDB extension already exists or warning: ${error.message}`);
        }
    }

    /**
     * Create core tables including perp_data, perp_status, perp_errors, and perpspec_schema.
     * The perp_data table stores all time-series data with core columns:
     * ts (timestamp), symbol, source (perpspec name), interval, and OHLCV columns.
     */
    async createCoreTables() {
        try {
            await this.pool.query(`
                CREATE TABLE IF NOT EXISTS perp_data (
                    ts BIGINT NOT NULL,
                    symbol TEXT NOT NULL,
                    source TEXT NOT NULL,
                    perpspec TEXT NOT NULL,
                    interval TEXT DEFAULT '1min',
                    o NUMERIC(20,8),
                    h NUMERIC(20,8),
                    l NUMERIC(20,8),
                    c NUMERIC(20,8),
                    v NUMERIC(20,8),
                    PRIMARY KEY (ts, symbol, source)
                )
            `);
            console.log('  - Created/updated perp_data table with OHLCV columns.');
        } catch (error) {
            console.error(`  - Error creating/updating perp_data table: ${error.message}`);
        }

        try {
            await this.pool.query(`SELECT create_hypertable('perp_data', 'ts', if_not_exists => TRUE)`);
            console.log('  - Created perp_data hypertable.');
        } catch (error) {
            console.error(`  - Error creating hypertable: ${error.message}`);
        }

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

    /**
     * Insert fixed perpspec schemas for core OHLCV data sources.
     * Each schema includes mandatory fields: ts, symbol, source, interval,
     * plus OHLCV columns.
     */
    async insertFixedPerpspecSchemas() {
        const fixedSchemas = [
            { name: 'bin-ohlcv', fields: ['ts', 'symbol', 'source', 'perpspec', 'interval', 'o', 'h', 'l', 'c', 'v'] },
            { name: 'byb-ohlcv', fields: ['ts', 'symbol', 'source', 'perpspec', 'interval', 'o', 'h', 'l', 'c', 'v'] },
            { name: 'okx-ohlcv', fields: ['ts', 'symbol', 'source', 'perpspec', 'interval', 'o', 'h', 'l', 'c', 'v'] }
        ];

        for (const schema of fixedSchemas) {
            try {
                await this.pool.query(
                    `INSERT INTO perpspec_schema (perpspec_name, fields)
                     VALUES ($1, $2)
                     ON CONFLICT (perpspec_name) DO UPDATE SET
                        fields = EXCLUDED.fields,
                        last_updated = NOW()`,
                    [schema.name, JSON.stringify(schema.fields)]
                );
                console.log(`  - Registered perpspec_schema for '${schema.name}'`);
            } catch (error) {
                console.error(`  - Error registering perpspec_schema for '${schema.name}': ${error.message}`);
            }
        }
    }

    /**
     * Determine the appropriate column type for a given value.
     * Used for dynamic column creation.
     * @param {*} value
     * @returns {string} SQL column type
     */
    getColumnType(value) {
        if (value === null || value === undefined) return 'TEXT';
        if (typeof value === 'number') return Number.isInteger(value) ? 'BIGINT' : 'NUMERIC(20, 8)';
        if (typeof value === 'boolean') return 'BOOLEAN';
        if (typeof value === 'object') return 'JSONB';
        return 'TEXT';
    }

    /**
     * Insert an array of data objects into the perp_data table.
     * Uses bulk insert with ON CONFLICT DO NOTHING to avoid duplicates.
     * @param {string} perpspecName - The source/perpspec name for the data.
     * @param {Array<Object>} dataArray - Array of data records to insert.
     */
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

    /**
     * Log status messages for scripts.
     * @param {string} scriptName
     * @param {string} status
     * @param {string} message
     * @param {object|null} details
     */
    async logStatus(scriptName, status, message, details = null) {
        console.log(`  - Status: ${scriptName} - ${status} - ${message}`);
    }

    /**
     * Log errors for scripts.
     * @param {string} scriptName
     * @param {string} errorType
     * @param {string} errorCode
     * @param {string} errorMessage
     * @param {object|null} details
     */
    async logError(scriptName, errorType, errorCode, errorMessage, details = null) {
        console.error(`  - Error: ${scriptName} - ${errorCode} - ${errorMessage}`);
        if (details) {
            console.error(`    Details: ${JSON.stringify(details)}`);
        }
    }
}

const dbManager = new DatabaseManager();
module.exports = dbManager;

if (require.main === module) {
    async function runSetup() {
        try {
            await dbManager.initialize();
        } catch (error) {
            console.error('❌ Database setup failed:', error);
            process.exit(1);
        }
    }
    runSetup();
}