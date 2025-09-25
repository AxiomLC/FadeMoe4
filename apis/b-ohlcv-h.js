// apis/b-ohlcv-h.js
const fs = require('fs');
const axios = require('axios');
const dbManager = require('../db/dbsetup');
require('dotenv').config();

// ============================================================================
//  UNIVERSAL FUNCTIONS (SHARED BY ALL SCRIPTS)
//  Standardized: Status logging, Error handling, Data insertion
// ============================================================================

// Standardized status logging for all scripts
async function logScriptStatus(status, message, details = null) {
    await dbManager.logStatus('b-ohlcv-h.js', status, message, details);
}

// Standardized error logging for all scripts
async function logScriptError(errorType, errorCode, errorMessage, details = null) {
    await dbManager.logError('b-ohlcv-h.js', errorType, errorCode, errorMessage, details);
}

// ============================================================================
//  API-SPECIFIC CONFIGURATION (CUSTOM SECTION - CLEARLY MARKED)
//  Each script customizes: Endpoint, Authentication, Symbol format, Parsing
// ============================================================================

// Binance Futures API configuration
const API_CONFIG = {
    baseUrl: 'https://fapi.binance.com',
    endpoint: '/fapi/v1/klines',
    interval: '1m',
    maxLimit: 1500,  // Max candles per request
    timeout: 30000,
    userAgent: 'FadeMoe4/1.0'
};

// ============================================================================
//  SYMBOL HANDLING (CUSTOM SECTION - CLEARLY MARKED)
//  Extract correct symbol format from dynamic-symbols.json
// ============================================================================

function getBinanceSymbol(exchangeSymbols) {
    // Convert Coinalyze format to actual Binance symbol
    // e.g., "BTCUSDT_PERP.A" -> "BTCUSDT"
    return exchangeSymbols.binance_ohlcv || 
           exchangeSymbols.binance?.replace('_PERP.A', '').replace('.A', '') ||
           null;
}

// ============================================================================
//  DATA PROCESSING (CUSTOM SECTION - CLEARLY MARKED)
//  Transform API response to standardized format
// ============================================================================

function processCandleData(candles, baseSymbol) {
    return candles.map(candle => ({
        ts: new Date(candle[0]),      // Unified timestamp
        symbol: baseSymbol,           // Unified base symbol
        exchange: 'binance',          // Unified exchange
        source: 'binance',            // Unified data source
        interval: '1min',             // Unified interval
        o: parseFloat(candle[1]),     // Open
        h: parseFloat(candle[2]),     // High
        l: parseFloat(candle[3]),     // Low
        c: parseFloat(candle[4]),     // Close (main value for backtester)
        v: parseFloat(candle[5]),     // Volume (standardized for backtester)
        created_at: new Date(),
        updated_at: new Date()
    })).filter(item => 
        !isNaN(item.o) && !isNaN(item.h) && !isNaN(item.l) && 
        !isNaN(item.c) && !isNaN(item.v)
    );
}

// ============================================================================
//  CORE EXECUTION LOGIC (UNIVERSAL SECTION)
//  Standardized flow: Read symbols -> Fetch data -> Process -> Insert
// ============================================================================

async function executeBackfill() {
    console.log('🚀 Starting Binance OHLCV History backfill...');

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

    // Calculate time range (10 days rolling)
    const now = new Date();
    const tenDaysAgo = new Date(now.getTime() - (10 * 24 * 60 * 60 * 1000));
    const endTime = now.getTime();
    const startTime = tenDaysAgo.getTime();

    console.log(`📅 Backfill range: ${tenDaysAgo.toISOString()} → ${now.toISOString()}`);

    // Process each symbol
    for (const [baseSymbol, exchangeSymbols] of Object.entries(dynamicSymbols)) {
        console.log(`\n💰 Processing ${baseSymbol}...`);
        
        const binanceSymbol = getBinanceSymbol(exchangeSymbols);
        if (!binanceSymbol) {
            console.log(`⚠️ No Binance symbol for ${baseSymbol}, skipping...`);
            continue;
        }
        
        console.log(`⏳ Fetching Binance: ${binanceSymbol}...`);
        
        try {
            // Log start status
            await logScriptStatus('running', `Backfill started for ${baseSymbol}`, {
                symbol_format: binanceSymbol,
                from: startTime,
                to: endTime,
                interval: API_CONFIG.interval
            });

            // Fetch data with pagination
            let allCandles = [];
            let currentStartTime = startTime;
            let requestCount = 0;
            const maxRequests = 15;
            
            while (currentStartTime < endTime && requestCount < maxRequests) {
                try {
                    console.log(`📡 Request ${requestCount + 1}: ${new Date(currentStartTime).toISOString()}`);
                    
                    const response = await axios.get(`${API_CONFIG.baseUrl}${API_CONFIG.endpoint}`, {
                        params: {
                            symbol: binanceSymbol,
                            interval: API_CONFIG.interval,
                            startTime: currentStartTime,
                            endTime: endTime,
                            limit: API_CONFIG.maxLimit
                        },
                        timeout: API_CONFIG.timeout,
                        headers: {
                            'User-Agent': API_CONFIG.userAgent
                        }
                    });

                    const candles = response.data;
                    console.log(`📥 Received ${candles.length} candles`);
                    
                    if (!candles || candles.length === 0) {
                        console.log(`⚠️ No more data available for ${binanceSymbol}`);
                        break;
                    }
                    
                    allCandles = allCandles.concat(candles);
                    
                    // Update start time for next request
                    const lastCandle = candles[candles.length - 1];
                    currentStartTime = lastCandle[0] + 1;
                    
                    requestCount++;
                    
                    // Rate limiting
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    
                } catch (requestError) {
                    console.error(`❌ Request error for ${binanceSymbol}:`, requestError.message);
                    break;
                }
            }

            console.log(`📈 Total candles fetched: ${allCandles.length}`);

            if (allCandles.length > 0) {
                // Process and insert data
                const processedData = processCandleData(allCandles, baseSymbol);
                console.log(`🧹 Processed ${processedData.length} valid records`);

                if (processedData.length > 0) {
                    // Insert into database using universal function
                    await dbManager.insertData('ohlcv', processedData);
                    
                    // Log success status
                    await logScriptStatus('success', `Backfill completed for ${baseSymbol}`, {
                        records_inserted: processedData.length,
                        symbol_format: binanceSymbol
                    });

                    console.log(`✅ ${baseSymbol} backfill completed successfully`);
                } else {
                    const warningMsg = `No valid data for ${baseSymbol} after filtering`;
                    console.log(`⚠️ ${warningMsg}`);
                    await logScriptStatus('warning', warningMsg, { symbol_format: binanceSymbol });
                }
            } else {
                const warningMsg = `No data returned for ${baseSymbol}`;
                console.log(`⚠️ ${warningMsg}`);
                await logScriptStatus('warning', warningMsg, { symbol_format: binanceSymbol });
            }

        } catch (error) {
            console.error(`❌ Error processing ${baseSymbol}:`, error.message);
            
            // Categorize and log error
            let errorCategory = 'unknown';
            let errorCode = 'UNKNOWN_ERROR';
            
            if (error.response?.status === 400) {
                errorCategory = 'bad_parameters';
                errorCode = 'BINANCE_400';
            } else if (error.response?.status === 401) {
                errorCategory = 'invalid_api_key';
                errorCode = 'BINANCE_401';
            } else if (error.response?.status === 429) {
                errorCategory = 'rate_limited';
                errorCode = 'BINANCE_429';
            } else if (error.response?.status === 500) {
                errorCategory = 'server_error';
                errorCode = 'BINANCE_500';
            } else if (error.code === 'ECONNABORTED') {
                errorCategory = 'timeout';
                errorCode = 'NETWORK_TIMEOUT';
            }

            await logScriptError('API', errorCode, `${errorCategory}: ${error.message}`, {
                symbol: baseSymbol,
                symbol_format: binanceSymbol,
                exchange: 'binance',
                response_status: error.response?.status,
                error_code_detail: error.code
            });

            continue;
        }
    }

    console.log('🎉 Binance OHLCV History backfill completed!');
}

// ============================================================================
//  EXPORT FOR MASTER API (UNIVERSAL SECTION)
//  Standardized export name for easy discovery
// ============================================================================

module.exports = { execute: executeBackfill };

// Self-executing when run directly
if (require.main === module) {
    executeBackfill()
        .then(() => {
            console.log('🏁 Script completed successfully');
            process.exit(0);
        })
        .catch(error => {
            console.error('💥 Script failed with error:', error);
            process.exit(1);
        });
}