// api-utils.js
const axios = require('axios');

// ============================================================================
//  API Configuration Defaults
// ============================================================================
// Default configurations that can be spread into script-specific API_CONFIG objects.

const API_CONFIG_DEFAULTS = {
    timeout: 30000, // Default request timeout in milliseconds
    userAgent: 'FadeMoe4/1.0' // Default User-Agent string
};

// ============================================================================
//  UNIVERSAL FUNCTIONS (SHARED BY ALL SCRIPTS)
//  These functions provide standardized logging and database interaction.
// ============================================================================

/**
 * Logs the status of a script execution to the database.
 * @param {object} dbManager - The database manager instance.
 * @param {string} scriptName - The name of the script being logged.
 * @param {string} status - The status of the operation (e.g., 'running', 'success', 'warning', 'error').
 * @param {string} message - A descriptive message about the status.
 * @param {object|null} [details=null] - Optional additional details about the status.
 */
async function logScriptStatus(dbManager, scriptName, status, message, details = null) {
  try {
    await dbManager.logStatus(scriptName, status, message, details);
    } catch (error) {
    console.error(`apiUtils.logScriptStatus failed:`, error);
        throw error;
    }
}

/**
 * Logs an error encountered during script execution to the database.
 * @param {object} dbManager - The database manager instance.
 * @param {string} scriptName - The name of the script where the error occurred.
 * @param {string} errorType - The type of error (e.g., 'CONFIG', 'FILE', 'API').
 * @param {string} errorCode - A specific code for the error.
 * @param {string} errorMessage - A description of the error.
 * @param {object|null} [details=null] - Optional additional details about the error.
 */
async function logScriptError(dbManager, scriptName, errorType, errorCode, errorMessage, details = null) {
    try {
    await dbManager.logError(scriptName, errorType, errorCode, errorMessage, details);
    } catch (error) {
    console.error(`apiUtils.logScriptError failed:`, error);
        throw error;
    }
}

// ============================================================================
//  TIMESTAMP NORMALIZATION UTILITY
//  Handles various timestamp formats and converts them to BigInt milliseconds (UTC0).
// ============================================================================

/**
 * Normalizes various timestamp formats to epoch milliseconds (UTC0) as BigInt.
 * Handles numbers (seconds or milliseconds), strings (ISO 8601 or numeric), and BigInt.
 * @param {number|string|BigInt|null} ts - The timestamp to normalize.
 * @returns {BigInt|null} The timestamp in BigInt milliseconds (UTC0), or null if input is null.
 * @throws {Error} If the timestamp format is unsupported.
 */
function toMillis(ts) {
    if (ts == null) return null;

    let millis;

    // Handle BigInt input
    if (typeof ts === 'bigint') {
        // Heuristic: If it's less than 10^12, assume it's seconds and convert to ms.
        // Otherwise, assume it's already milliseconds.
        millis = ts < BigInt(1e12) ? ts * BigInt(1000) : ts;
    }
    // Handle Number input
    else if (typeof ts === 'number') {
        // Heuristic: If it's less than 10^12, assume it's seconds and convert to ms.
        // Otherwise, assume it's already milliseconds.
        millis = ts < 1e12 ? ts * 1000 : ts;
    }
    // Handle String input
    else if (typeof ts === 'string') {
        // Try parsing as ISO string first (e.g., "2025-09-25T06:34:39.000Z")
        const parsedMillisFromISO = Date.parse(ts);
        if (!isNaN(parsedMillisFromISO)) {
            millis = BigInt(parsedMillisFromISO);
        } else {
            // If ISO parse fails, try parsing as a numeric string
            const numericTs = Number(ts);
            if (!isNaN(numericTs)) {
                // Apply the same heuristic for numeric strings
                millis = BigInt(numericTs < 1e12 ? numericTs * 1000 : numericTs);
            } else {
                throw new Error("Unsupported timestamp string format: " + ts);
            }
        }
    } else {
        throw new Error("Unsupported timestamp type: " + typeof ts + " - " + ts);
    }

    // Ensure the final result is a BigInt
    return BigInt(millis);
}

// ============================================================================
//  DYNAMIC SCHEMA MANAGEMENT
//  Functions to ensure database columns exist and to update the perpspec_schema table.
// ============================================================================

/**
 * Ensures that all expected database columns exist in the 'perp_data' table.
 * If a column does not exist, it attempts to add it with a default type.
 * @param {object} dbManager - The database manager instance.
 * @param {string[]} expectedFields - An array of column names that should exist.
 */
async function ensureColumnsExist(dbManager, expectedFields) {
    try {
        const existingColumns = await dbManager.pool.query(
            `
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'perp_data'
        `);
        const existingColumnNames = existingColumns.rows.map(row => row.column_name);

        for (const field of expectedFields) {
            if (!existingColumnNames.includes(field)) {
                // Defaulting to NUMERIC for potential floating-point values like funding rates or prices.
                // Adjust if specific columns require different types (e.g., BIGINT for ts if not already handled).
                const columnType = 'NUMERIC(20, 8)';

                const addColumnQuery = `ALTER TABLE perp_data ADD COLUMN ${field} ${columnType}`;
                try {
                    await dbManager.pool.query(addColumnQuery);
                    console.log(`  - ✅ Added column '${field}' to perp_data`);
                } catch (addColumnError) {
                    console.error(`  - ❌ Error adding column '${field}' to perp_data:`, addColumnError.message);
                }
            }
        }
    } catch (error) {
        console.error('❌ Error checking/adding columns:', error.message);
        throw error;
    }
}

/**
 * Updates the 'perpspec_schema' table with the fields for a given perpspec.
 * Uses INSERT ON CONFLICT to either insert a new record or update an existing one.
 * @param {object} dbManager - The database manager instance.
 * @param {string} perpspecName - The name of the perpspec (e.g., 'pfr').
 * @param {string[]} fields - An array of field names for this perpspec.
 */
async function updatePerpspecSchema(dbManager, perpspecName, fields) {
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

// Export all utility functions and configurations
module.exports = {
    API_CONFIG_DEFAULTS,
    logScriptStatus,
    logScriptError,
    ensureColumnsExist,
    updatePerpspecSchema,
    toMillis
};