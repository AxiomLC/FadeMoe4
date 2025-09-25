// apis/c-pfr-h.js
const fs = require('fs');
const axios = require('axios');
const dbManager = require('../db/dbsetup'); // Assuming dbsetup.js is in a 'db' subfolder
require('dotenv').config();

// ============================================================================
//  SCRIPT CONFIGURATION & HELPERS
// ============================================================================

const SCRIPT_NAME = 'c-pfr-h.js';
const PERPSPEC_NAME = 'pfr'; // Corresponds to the 'source' column value and perpspec_schema entry

// Coinalyze API configuration
const API_CONFIG = {
    baseUrl: 'https://api.coinalyze.net',
    endpoint: '/v1/predicted-funding-rate-history',
    apiKey: process.env.COINALYZE_API_KEY,
    interval: '1min', // Coinalyze PFR API interval (check docs if this changes)
    timeout: 30000,
    userAgent: 'FadeMoe4/1.0'
};

// ============================================================================
//  UNIVERSAL FUNCTIONS (SHARED BY ALL SCRIPTS)
//  Standardized: Status logging, Error handling, Data insertion
// ============================================================================

async function logScriptStatus(status, message, details = null) {
    await dbManager.logStatus(SCRIPT_NAME, status, message, details);
}

async function logScriptError(errorType, errorCode, errorMessage, details = null) {
    await dbManager.logError(SCRIPT_NAME, errorType, errorCode, errorMessage, details);
}

// ============================================================================
//  SYMBOL HANDLING (CUSTOM SECTION - CLEARLY MARKED)
//  Extract correct Coinalyze symbol format from dynamic-symbols.json
// ============================================================================

function getApiSymbol(exchangeSymbols) {
    // Use the 'c-bybit' key for Coinalyze Bybit PFR data
    // This value ("AVAXUSDT.6") is what gets passed to the API
    return exchangeSymbols['c-bybit'] || null;
}

// ============================================================================
//  DATA PROCESSING (CUSTOM SECTION - CLEARLY MARKED)
//  Transform Coinalyze API response to standardized format
//  Use 'c' (close) value as the predicted funding rate
// ============================================================================

function processApiData(apiResponseData, baseSymbol, apiSymbol) {
    // Coinalyze PFR API returns: [{symbol: "...", history: [{t: timestamp, o: open, h: high, l: low, c: close/funding_rate}, ...]}, ...]
    if (!apiResponseData || !Array.isArray(apiResponseData) || apiResponseData.length === 0) {
        console.log(`⚠️ No data array returned from API for ${baseSymbol}`);
        return [];
    }

    // Extract history data from the first symbol result (assuming only one symbol requested at a time)
    const symbolResult = apiResponseData[0];
    const historyData = symbolResult.history || [];

    if (historyData.length === 0) {
        console.log(`⚠️ No history data found in API response for ${baseSymbol}`);
        return [];
    }

    const processedRecords = historyData.map(item => {
        const timestamp = parseInt(item.t);
        const fundingRate = parseFloat(item.c);

        // Basic validation for critical fields
        if (isNaN(timestamp) || isNaN(fundingRate)) {
            console.log(`⚠️ Invalid data found in history item for ${baseSymbol}:`, item);
            return null; // Mark as invalid
        }

        return {
            ts: new Date(timestamp * 1000),   // Unified timestamp
            symbol: baseSymbol,               // Unified base symbol (from dynamic-symbols.json key)
            source: 'c-bybit',                // *** Explicitly set source based on the key used ***
            interval: API_CONFIG.interval,    // Unified interval
            pfr: fundingRate                  // The 'pfr' column value
        };
    }).filter(item => item !== null); // Filter out any nulls from invalid data

    return processedRecords;
}

// ============================================================================
//  DYNAMIC SCHEMA MANAGEMENT (NEW SECTION FOR EACH SCRIPT)
//  Ensures columns exist and updates perpspec_schema table
// ============================================================================

async function ensureColumnsExist(expectedFields) {
    try {
        const existingColumns = await dbManager.pool.query(`
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'perp_data'
        `);
        const existingColumnNames = existingColumns.rows.map(row => row.column_name);

        for (const field of expectedFields) {
            if (!existingColumnNames.includes(field)) {
                // Determine column type (simplified - improve this logic if needed)
                // For PFR, it's a floating-point number.
                const columnType = 'NUMERIC(20, 8)'; // Defaulting to NUMERIC for funding rates

                const addColumnQuery = `ALTER TABLE perp_data ADD COLUMN ${field} ${columnType}`;
                try {
                    await dbManager.pool.query(addColumnQuery);
                    console.log(`  - ✅ Added column '${field}' to perp_data`);
                } catch (addColumnError) {
                    console.error(`  - ❌ Error adding column '${field}' to perp_data:`, addColumnError.message);
                    // Decide if this should be a fatal error or just logged
                }
            }
        }
    } catch (error) {
        console.error('❌ Error checking/adding columns:', error.message);
        throw error; // Re-throw to stop execution if column management fails
    }
}

async function updatePerpspecSchema(perpspecName, fields) {
    try {
        await dbManager.pool.query(
            `INSERT INTO perpspec_schema (perpspec_name, fields)
             VALUES ($1, $2)
             ON CONFLICT (perpspec_name) DO UPDATE SET
                fields = $2,
                last_updated = NOW()`,
            [perpspecName, JSON.stringify(fields)]
        );
        console.log(`  - ✅ Updated perpspec_schema for '${perpspecName}' with fields: ${fields.join(', ')}`);
    } catch (error) {
        console.error(`❌ Error updating perpspec_schema for '${perpspecName}':`, error.message);
        throw error;
    }
}

// ============================================================================
//  CORE EXECUTION LOGIC (UNIVERSAL SECTION)
//  Standardized flow: Read symbols -> Fetch data -> Process -> Ensure Schema -> Insert
// ============================================================================

async function execute() { // Renamed from executeBackfill for consistency
    console.log(`🚀 Starting ${SCRIPT_NAME} backfill...`);

    // Validate API key
    if (!API_CONFIG.apiKey) {
        const errorMsg = 'COINALYZE_API_KEY not found in .env file!';
        console.error(`❌ ${errorMsg}`);
        await logScriptError('CONFIG', 'MISSING_API_KEY', errorMsg);
        return;
    }

    // Read dynamic symbols
    let dynamicSymbols;
    try {
        dynamicSymbols = JSON.parse(fs.readFileSync('dynamic-symbols.json', 'utf8'));
        console.log(`📊 Found ${Object.keys(dynamicSymbols).length} symbols to process`);
    } catch (error) {
        const errorMsg = 'Could not read dynamic-symbols.json. Run g-symbols.js first!';
        console.error(`❌ ${errorMsg}`);
        await logScriptError('FILE', 'MISSING_SYMBOLS', errorMsg);
        return;
    }

    // Calculate time range (10 days maximum for history scripts)
    const now = Math.floor(Date.now() / 1000);
    const tenDaysAgo = now - (10 * 24 * 3600); // 10 days in seconds
    const to = now;
    const from = tenDaysAgo;

    console.log(`📅 Backfill range: ${new Date(from * 1000).toISOString()} → ${new Date(to * 1000).toISOString()}`);

    // Process each symbol
    for (const [baseSymbol, exchangeSymbols] of Object.entries(dynamicSymbols)) {
        console.log(`\n💰 Processing ${baseSymbol}...`);

        const apiSymbol = getApiSymbol(exchangeSymbols); // Get the symbol format for the API call
        if (!apiSymbol) {
            console.log(`  - ⚠️ No API symbol found for '${PERPSPEC_NAME}' on Bybit for ${baseSymbol}, skipping...`);
            continue;
        }

        console.log(`  - ⏳ Fetching Coinalyze PFR for ${apiSymbol}...`);

        try {
            // Log start status
            await logScriptStatus('running', `Backfill started for ${baseSymbol}`, {
                api_symbol: apiSymbol,
                from: from,
                to: to,
                interval: API_CONFIG.interval
            });

            // Fetch funding rate data from Coinalyze API
            const url = `${API_CONFIG.baseUrl}${API_CONFIG.endpoint}`;
            console.log(`  - 📡 Requesting: ${url}`);

            const response = await axios.get(url, {
                params: {
                    symbols: apiSymbol, // Use the specific API symbol format
                    interval: API_CONFIG.interval,
                    from: from,
                    to: to
                },
                headers: {
                    'Authorization': `Bearer ${API_CONFIG.apiKey}`
                },
                timeout: API_CONFIG.timeout
            });

            const apiData = response.data;
            console.log(`  - 📥 Received response for ${apiSymbol}.`);

            // Process and insert data
            const processedData = processApiData(apiData, baseSymbol, apiSymbol); // Pass apiSymbol for context if needed
            console.log(`  - 🧹 Processed ${processedData.length} valid records`);

            if (processedData.length > 0) {
                // Dynamically ensure columns exist and update perpspec_schema
                const expectedFields = Object.keys(processedData[0]); // Get columns from the first processed record
                await ensureColumnsExist(expectedFields);
                await updatePerpspecSchema(PERPSPEC_NAME, expectedFields); // Use PERPSPEC_NAME

                // Insert into database using universal function
                await dbManager.insertData(PERPSPEC_NAME, processedData); // Use PERPSPEC_NAME

                // Log success status
                await logScriptStatus('success', `Backfill completed for ${baseSymbol}`, {
                    records_inserted: processedData.length,
                    api_symbol: apiSymbol
                });

                console.log(`  - ✅ ${baseSymbol} backfill completed successfully`);
            } else {
                const warningMsg = `No valid data processed for ${baseSymbol}`;
                console.log(`  - ⚠️ ${warningMsg}`);
                await logScriptStatus('warning', warningMsg, { api_symbol: apiSymbol });
            }

        } catch (error) {
            console.error(`  - ❌ Error processing ${baseSymbol}:`, error.message);
            // Log detailed error information
            await logScriptError('API', error.code || 'UNKNOWN_API_ERROR', error.message, {
                script: SCRIPT_NAME,
                base_symbol: baseSymbol,
                api_symbol: apiSymbol,
                response_status: error.response?.status,
                response_data: error.response?.data,
                error_details: error.details
            });

            // Continue with next symbol even if one fails
            continue;
        }
    }

    console.log('🎉 Coinalyze PFR History backfill completed!');
}

// ============================================================================
//  EXPORT FOR MASTER API (UNIVERSAL SECTION)
//  Standardized export name for easy discovery
// ============================================================================

module.exports = { execute }; // Export the main function

// Self-executing when run directly
if (require.main === module) {
    execute()
        .then(() => {
            console.log('🏁 Script completed successfully');
            process.exit(0);
        })
        .catch(error => {
            console.error('💥 Script failed with error:', error);
            process.exit(1);
        });
}