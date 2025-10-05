const { Pool } = require('pg');
const format = require('pg-format');
require('dotenv').config();

// ============================================================================
// DATABASE MANAGER CLASS
// Handles all PostgreSQL operations for the perpetual futures data system
// ============================================================================
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

    // ========================================================================
    // INITIALIZATION - Sets up database schema from scratch
    // ========================================================================
    async initialize() {
        console.log('⚙️ Setting up database...');
        await this.dropExistingTables();
        await this.enableExtensions();
        await this.createCoreTables();
        await this.insertFixedPerpspecSchemas();
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
        // Main data table - stores all OHLCV and OI data
        try {
            await this.pool.query(
                `CREATE TABLE IF NOT EXISTS perp_data (
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
                    oi NUMERIC(20,8),
                    PRIMARY KEY (ts, symbol, source)
                )`
            );
            console.log('  - Created/updated perp_data table with OHLCV + OI columns.');
        } catch (error) {
            console.error(`  - Error creating/updating perp_data table: ${error.message}`);
        }

        // Convert to TimescaleDB hypertable for efficient time-series queries
        try {
            await this.pool.query(`SELECT create_hypertable('perp_data', 'ts', if_not_exists => TRUE)`);
            console.log('  - Created perp_data hypertable.');
        } catch (error) {
            console.error(`  - Error creating hypertable: ${error.message}`);
        }

        // Status logging table - tracks script execution
        try {
            await this.pool.query(
                `CREATE TABLE IF NOT EXISTS perp_status (
                    task_id SERIAL PRIMARY KEY,
                    script_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    details JSONB,
                    ts TIMESTAMP DEFAULT NOW()
                )`
            );
            console.log('  - Created perp_status table.');
        } catch (error) {
            console.error(`  - Error creating perp_status table: ${error.message}`);
        }

        // Error logging table - tracks failures
        try {
            await this.pool.query(
                `CREATE TABLE IF NOT EXISTS perp_errors (
                    error_id SERIAL PRIMARY KEY,
                    script_name TEXT,
                    error_type TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    details JSONB,
                    ts TIMESTAMP DEFAULT NOW()
                )`
            );
            console.log('  - Created perp_errors table.');
        } catch (error) {
            console.error(`  - Error creating perp_errors table: ${error.message}`);
        }

        // Schema registry - defines fields for each perpspec
        try {
            await this.pool.query(
                `CREATE TABLE IF NOT EXISTS perpspec_schema (
                    perpspec_name TEXT PRIMARY KEY,
                    fields JSONB NOT NULL,
                    last_updated TIMESTAMP DEFAULT NOW()
                )`
            );
            console.log('  - Created perpspec_schema table.');
        } catch (error) {
            console.error(`  - Error creating perpspec_schema table: ${error.message}`);
        }
    }

    async insertFixedPerpspecSchemas() {
        const fixedSchemas = [
            { name: 'bin-ohlcv', fields: ['ts', 'symbol', 'source', 'perpspec', 'interval', 'o', 'h', 'l', 'c', 'v'] },
            { name: 'byb-ohlcv', fields: ['ts', 'symbol', 'source', 'perpspec', 'interval', 'o', 'h', 'l', 'c', 'v'] },
            { name: 'okx-ohlcv', fields: ['ts', 'symbol', 'source', 'perpspec', 'interval', 'o', 'h', 'l', 'c', 'v'] },
            { name: 'bin-oi', fields: ['ts', 'symbol', 'source', 'perpspec', 'interval', 'oi'] },
            { name: 'byb-oi', fields: ['ts', 'symbol', 'source', 'perpspec', 'interval', 'oi'] },
            { name: 'okx-oi', fields: ['ts', 'symbol', 'source', 'perpspec', 'interval', 'oi'] }
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

    // ========================================================================
    // DATA OPERATIONS - Insert and query data
    // ========================================================================
    
    // Bulk insert with conflict handling (upserts avoided, duplicates ignored)
    async insertData(perpspecName, dataArray) {
        if (!Array.isArray(dataArray) || dataArray.length === 0) {
            // console.log('  - ⚠️ No data to insert.'); // Suppressed detailed log
            return { rowCount: 0 };
        }

        const firstItem = dataArray[0];
        const expectedFields = Object.keys(firstItem);

        try {
            const insertColumnNames = expectedFields.join(', ');
            const values = dataArray.map(item => expectedFields.map(field => item[field]));

            const query = format(
                `INSERT INTO perp_data (${insertColumnNames})
                VALUES %L
                ON CONFLICT (ts, symbol, source) DO NOTHING`,
                values
            );

            const result = await this.pool.query(query);
            // console.log(`  - Inserted ${result.rowCount} records for perpspec '${perpspecName}'.`); // Suppressed detailed log
            return result;
        } catch (error) {
            console.error(`  - Error inserting data for perpspec '${perpspecName}': ${error.message}`);
            throw error;
        }
    }

    // ========================================================================
    // FILL SIZE DETECTION - Fast gap detection for backfilling
    // Single query using MIN/MAX/COUNT - optimized for speed
    // ========================================================================
    async getFillSize(perpspec, symbol) {
        const queryStats = `
            SELECT 
                MIN(ts) as earliest,
                MAX(ts) as latest,
                COUNT(*) as record_count
            FROM perp_data
            WHERE perpspec = $1 AND symbol = $2
        `;

        const queryGaps = `
            SELECT ts, lead_ts, lead_ts - ts AS gap
            FROM (
                SELECT ts,
                       LEAD(ts) OVER (ORDER BY ts) AS lead_ts
                FROM perp_data
                WHERE perpspec = $1 AND symbol = $2
            ) sub
            WHERE lead_ts IS NOT NULL AND lead_ts - ts > 60000  -- gap > 1 minute
            ORDER BY ts
        `;
        try {
            const statsResult = await this.pool.query(queryStats, [perpspec, symbol]);
            const gapsResult = await this.pool.query(queryGaps, [perpspec, symbol]);

            if (!statsResult.rows[0] || statsResult.rows[0].record_count === '0') {
                return {
                    earliest: null,
                    latest: null,
                    recordCount: 0,
                    isEmpty: true,
                    isFull: false,
                    gaps: []
                };
            }

            const { earliest, latest, record_count } = statsResult.rows[0];
            const gaps = gapsResult.rows.map(row => ({
                start: parseInt(row.ts),
                end: parseInt(row.lead_ts),
                gapSize: parseInt(row.gap)
            }));

            const expectedRecords = 10 * 24 * 60; // 10 days * 1440 minutes/day
            const isFull = parseInt(record_count) >= expectedRecords && gaps.length === 0;

            return {
                earliest: parseInt(earliest),
                latest: parseInt(latest),
                recordCount: parseInt(record_count),
                isEmpty: false,
                isFull,
                gaps
            };
        } catch (error) {
            console.error(`  - Error getting fill size for perpspec='${perpspec}', symbol='${symbol}':`, error.message);
            throw error;
        }
    }

    // Calculate exact time ranges that need backfilling
    calculateBackfill(fillSize, targetStart, targetEnd) {
        if (fillSize.isEmpty) {
            return [{
                missingStart: targetStart,
                missingEnd: targetEnd,
                estimatedRecords: Math.floor((targetEnd - targetStart) / (60 * 1000))
            }];
        }

        const intervals = [];

        // Add gap intervals with slight overlap (e.g., 1 minute before gap start)
        for (const gap of fillSize.gaps) {
            intervals.push({
                missingStart: Math.max(gap.start - 60000, targetStart),
                missingEnd: Math.min(gap.end + 60000, targetEnd),
                estimatedRecords: Math.floor((gap.end - gap.start) / (60 * 1000))
            });
        }

        // Check for missing data before earliest
        if (fillSize.earliest > targetStart) {
            intervals.push({
                missingStart: targetStart,
                missingEnd: fillSize.earliest - 60000,
                estimatedRecords: Math.floor((fillSize.earliest - targetStart) / (60 * 1000))
            });
        }

        // Check for missing data after latest
        if (fillSize.latest < targetEnd) {
            intervals.push({
                missingStart: fillSize.latest + 60000,
                missingEnd: targetEnd,
                estimatedRecords: Math.floor((targetEnd - fillSize.latest) / (60 * 1000))
            });
        }

        if (intervals.length === 0) {
            return [];
        }

        return intervals;
    }

    // ========================================================================
    // LOGGING - Status and error tracking
    // ========================================================================
    async logStatus(scriptName, status, message, details = null) {
        try {
            const query = `
                INSERT INTO perp_status (script_name, status, message, details, ts)
                VALUES ($1, $2, $3, $4, NOW())
            `;
            await this.pool.query(query, [scriptName, status, message, details]);
        } catch (error) {
            console.error(`  - Failed to log status:`, error);
        }
    }

    async logError(scriptName, errorType, errorCode, errorMessage, details = null) {
        try {
            const query = `
                INSERT INTO perp_errors (script_name, error_type, error_code, error_message, details, ts)
                VALUES ($1, $2, $3, $4, $5, NOW())
            `;
            await this.pool.query(query, [scriptName, errorType, errorCode, errorMessage, details]);
            console.log(`  - Error logged to DB: ${scriptName} - ${errorCode}`);
        } catch (error) {
            console.error(`  - Failed to log error:`, error);
        }
    }
}

// ============================================================================
// EXPORT SINGLE INSTANCE
// ============================================================================
const dbManager = new DatabaseManager();
module.exports = dbManager;

// ============================================================================
// CLI EXECUTION - Run setup when called directly
// ============================================================================
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
