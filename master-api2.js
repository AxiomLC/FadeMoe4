/* =======================================================
 * MASTER-API2.JS - PRODUCTION ORCHESTRATOR
 * **apis\ folder: Runs -h backfill scripts parallel then -c files for real-time
 * Integrated status logging via apiUtils to dbsetup.js tables.
 * ======================================================= */

const fs = require('fs');
const path = require('path');
const pLimit = require('p-limit'); // From samples like all-lsr-c.js for concurrency
const apiUtils = require('./api-utils'); // For status logging (wraps dbManager)
const dbManager = require('./db/dbsetup'); // Provides dbManager for logging

 /* Edit here to set last -h or -c script (null = all parallel).
 * ======================================================= */
const LAST_H_SCRIPT = 'null'; // e.g. 'all-pfr-h.js'
const LAST_C_SCRIPT = 'null'; // e.g. 'rsi-c2.js'

/* =======================================================
 * MASTER API CLASS - CORE ORCHESTRATION
 * ======================================================= */
class MasterAPI {
    constructor() {
        this.scripts = [];
        this.running = false;
        this.intervals = {}; // Stores setInterval IDs for -c scripts
        this.heartbeatInterval = null; // Master -c heartbeat
    }

    /* =======================================================
     * INITIALIZATION - DISCOVER SCRIPTS FROM apis/ FOLDER
     * Scans .js files, parses filename for -h/-c mode, sorts alphabetically.
     * Logs "started" status via apiUtils.
     * ======================================================= */
    async initialize() {
        console.log('\x1b[35m' + '#### 🚀 MASTER API INITIALIZING ####' + '\x1b[0m');
        await this.discoverScripts();
        console.log('\x1b[35m' + `#### ✅ FOUND ${this.scripts.length} SCRIPTS IN apis/ ####` + '\x1b[0m');
        // Master Status: started
        await apiUtils.logScriptStatus(dbManager, 'master-api', 'started', 'Master API initialized');
    }

    /* =======================================================
     * SCRIPT DISCOVERY - DYNAMIC SCAN OF apis/ FOLDER
     * Filters .js files, determines type (history/current) from filename.
     * ======================================================= */
    async discoverScripts() {
        const apisDir = path.join(__dirname, 'apis');
        try {
            const files = fs.readdirSync(apisDir);
            this.scripts = files
                .filter(file => file.endsWith('.js'))
                .map(file => {
                    const fullPath = path.join(apisDir, file);
                    const stats = fs.statSync(fullPath);
                    return {
                        name: file,
                        path: fullPath,
                        mtype: stats.mtime,
                        type: this.determineScriptType(file)
                    };
                })
                .sort((a, b) => a.name.localeCompare(b.name));
        } catch (error) {
            console.error('❌ Error discovering scripts:', error.message);
            this.scripts = [];
        }
    }

    /* =======================================================
     * TYPE DETERMINATION - PARSE FILENAME FOR MODE (-h/-c)
     * Extracts source, metric, mode from filename (e.g., all-ohlcv-h.js → history).
     * ======================================================= */
    determineScriptType(filename) {
        const parts = filename.replace('.js', '').split('-');
        if (parts.length < 3) return 'unknown';
        const source = parts[0];
        const metric = parts[1];
        const mode = parts[2];
        return {
            source: source,
            metric: metric,
            mode: mode === 'h' ? 'history' : mode === 'c' ? 'current' : 'unknown',
            fullName: filename
        };
    }

    /* =======================================================
     * BACKFILL MODE - RUN ALL -h SCRIPTS IN PARALLEL (EXCEPT LAST IF SPECIFIED)
     * Uses pLimit(5) for concurrency; calls backfill()/execute() or main.
     * Promise.allSettled for non-blocking (isolates failures).
     * Logs "running -h" status before/after.
     * ======================================================= */
    async runBackfill() {
        console.log('\x1b[35m' + '#### 🔄 STARTING BACKFILL MODE (-h FILES) ####' + '\x1b[0m');
        // Master Status: running -h
        await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Master API running -h backfill');
        const historyScripts = this.scripts.filter(script => script.type.mode === 'history');
        if (historyScripts.length === 0) {
            console.log('\x1b[35m' + '#### ✅ NO -h SCRIPTS, SKIPPING BACKFILL ####' + '\x1b[0m');
            return;
        }

        const lastHScript = historyScripts.find(s => s.name === LAST_H_SCRIPT);
        const parallelScripts = lastHScript ? historyScripts.filter(s => s.name !== LAST_H_SCRIPT) : historyScripts;

        console.log(`📊 Running ${parallelScripts.length} -h scripts in parallel...`);
        const limit = pLimit(5); // Parallel concurrency (safe for different APIs)
        const parallelPromises = parallelScripts.map(script =>
            limit(async () => {
                try {
                    console.log(`\n🚀 Executing ${script.name} (parallel)...`);
                    const scriptModule = require(script.path);
                    if (typeof scriptModule.backfill === 'function') {
                        await scriptModule.backfill(); // From all-ohlcv-h.js sample
                    } else if (typeof scriptModule.execute === 'function') {
                        await scriptModule.execute();
                    } else {
                        await require(script.path); // Fallback to main if no export
                    }
                    console.log(`✅ Completed ${script.name} (parallel)`);
                } catch (error) {
                    console.error(`❌ Error in ${script.name}:`, error.message);
                    // Isolate error—don't stop others
                }
            })
        );

        await Promise.allSettled(parallelPromises); // Parallel, non-blocking

        // Run last script sequentially if specified
        if (lastHScript) {
            console.log(`\n🚀 Executing last -h script: ${lastHScript.name} (sequential)...`);
            try {
                const scriptModule = require(lastHScript.path);
                if (typeof scriptModule.backfill === 'function') {
                    await scriptModule.backfill();
                } else if (typeof scriptModule.execute === 'function') {
                    await scriptModule.execute();
                } else {
                    await require(lastHScript.path);
                }
                console.log(`✅ Completed ${lastHScript.name} (last)`);
            } catch (error) {
                console.error(`❌ Error in ${lastHScript.name}:`, error.message);
            }
        }

        console.log('\x1b[35m' + '#### 🎉 BACKFILL MODE COMPLETED ####' + '\x1b[0m');
        // Master Status: running -h complete
        await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Master API -h backfill complete');
    }

    /* =======================================================
     * LIVE MODE - LAUNCH ALL -c SCRIPTS WITH INTERVALS (START LAST IF SPECIFIED) unlesss LAST_C_SCRIPT
     * setInterval(60s) per script; calls execute()/pollAllSymbols() or main.
     * 3min heartbeat logs "running -c" for monitoring.
     * Logs "running -c" status on start.
     * ======================================================= */
    startLiveMode() {
        console.log('\x1b[35m' + '#### ⚡ STARTING LIVE MODE (-c FILES) ####' + '\x1b[0m');
        // Master Status: running -c
        apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Master API running -c continuous');
        const currentScripts = this.scripts.filter(script => script.type.mode === 'current');
        if (currentScripts.length === 0) {
            console.log('\x1b[35m' + '#### ⚠️ NO -c SCRIPTS FOUND ####' + '\x1b[0m');
            return;
        }

        const lastCScript = currentScripts.find(s => s.name === LAST_C_SCRIPT);
        const parallelScripts = lastCScript ? currentScripts.filter(s => s.name !== LAST_C_SCRIPT) : currentScripts;

        console.log(`📡 Starting ${parallelScripts.length} -c scripts with 60s intervals...`);
        parallelScripts.forEach(script => {
            try {
                const interval = 60000; // 60s default (from samples like all-oi-c.js)
                console.log(`🚀 Starting ${script.name} with ${interval/1000}s interval`);
                this.intervals[script.name] = setInterval(async () => {
                    try {
                        console.log(`📈 Running ${script.name}...`);
                        const scriptModule = require(script.path);
                        if (typeof scriptModule.execute === 'function') {
                            await scriptModule.execute();
                        } else if (typeof scriptModule.pollAllSymbols === 'function') { // From all-lsr-c.js sample
                            await scriptModule.pollAllSymbols();
                        } else if (typeof scriptModule.run === 'function') {
                            await scriptModule.run();
                        } else {
                            await require(script.path); // Fallback
                        }
                        console.log(`✅ ${script.name} completed`);
                    } catch (error) {
                        console.error(`❌ Error in ${script.name}:`, error.message);
                        // Isolate—continue others
                    }
                }, interval);
            } catch (error) {
                console.error(`❌ Failed to start ${script.name}:`, error.message);
            }
        });

        // Start last script's interval after a short delay (all concurrent, but last)
        if (lastCScript) {
            setTimeout(() => {
                try {
                    const interval = 60000;
                    console.log(`🚀 Starting last -c script: ${lastCScript.name} with ${interval/1000}s interval`);
                    this.intervals[lastCScript.name] = setInterval(async () => {
                        try {
                            console.log(`📈 Running ${lastCScript.name}...`);
                            const scriptModule = require(lastCScript.path);
                            if (typeof scriptModule.execute === 'function') {
                                await scriptModule.execute();
                            } else if (typeof scriptModule.pollAllSymbols === 'function') {
                                await scriptModule.pollAllSymbols();
                            } else if (typeof scriptModule.run === 'function') {
                                await scriptModule.run();
                            } else {
                                await require(lastCScript.path);
                            }
                            console.log(`✅ ${lastCScript.name} completed`);
                        } catch (error) {
                            console.error(`❌ Error in ${lastCScript.name}:`, error.message);
                            // Isolate—continue others
                        }
                    }, interval);
                } catch (error) {
                    console.error(`❌ Failed to start ${lastCScript.name}:`, error.message);
                }
            }, 1000); // 1s delay for "last"
        }

        this.running = true;
        console.log('\x1b[35m' + '#### ✅ LIVE MODE ACTIVE - ALL -c SCRIPTS RUNNING ####' + '\x1b[0m');

        // 3min heartbeat for Master -c status
        this.heartbeatInterval = setInterval(async () => {
            if (this.running) {
                await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Master API -c continuous active');
            }
        }, 180000); // 3min
    }

    /* =======================================================
     * SHUTDOWN MODE - GRACEFUL STOP OF -c SCRIPTS
     * Clears all intervals; logs "stopped smoothly".
     * Called on SIGINT/SIGTERM.
     * ======================================================= */
    stopLiveMode() {
        console.log('\x1b[35m' + '#### 🛑 STOPPING LIVE MODE ####' + '\x1b[0m');
        Object.keys(this.intervals).forEach(scriptName => {
            clearInterval(this.intervals[scriptName]);
            console.log(`⏹️ Stopped ${scriptName}`);
        });
        if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
        this.intervals = {};
        this.running = false;
        console.log('\x1b[35m' + '#### ✅ LIVE MODE STOPPED ####' + '\x1b[0m');
        // Master Status: stopped smoothly
        apiUtils.logScriptStatus(dbManager, 'master-api', 'stopped', 'Master API stopped smoothly');
    }

    /* =======================================================
     * START SEQUENCE - INIT → BACKFILL → LIVE MODE
     * Calls initialize(), runBackfill(), startLiveMode().
     * Catches startup errors with "stopped (error)".
     * ======================================================= */
    async start() {
        await this.initialize();
        await this.runBackfill();
        this.startLiveMode();
        console.log('\x1b[35m' + '#### 🎯 MASTER API FULLY OPERATIONAL! ####' + '\x1b[0m');
        console.log('\x1b[35m' + '#### 📊 BACKFILL COMPLETE, LIVE DATA ACTIVE ####' + '\x1b[0m');
    }

    /* =======================================================
     * STOP SEQUENCE - GRACEFUL SHUTDOWN
     * Calls stopLiveMode(); logs "stopped (error)" if needed.
     * ======================================================= */
    async stop() {
        console.log('\x1b[35m' + '#### 🛑 SHUTTING DOWN MASTER API ####' + '\x1b[0m');
        this.stopLiveMode();
        console.log('\x1b[35m' + '#### 👋 MASTER API SHUTDOWN COMPLETE ####' + '\x1b[0m');
        // Master Status: stopped (error if needed, but smooth here)
        if (this.running) {
            await apiUtils.logScriptStatus(dbManager, 'master-api', 'stopped', 'Master API stopped (error)');
        }
    }
}

/* =======================================================
 * MODULE ENTRY POINT - CREATE INSTANCE & HANDLE SIGNALS
 * Runs start() on direct call; catches errors with "stopped".
 * Exports MasterAPI class.
 * ======================================================= */
if (require.main === module) {
    const master = new MasterAPI();
    
    process.on('SIGINT', async () => {
        console.log('\n\x1b[35m' + '#### ⚠️ SHUTDOWN SIGNAL RECEIVED ####' + '\x1b[0m');
        await master.stop();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log('\n\x1b[35m' + '#### ⚠️ TERMINATION SIGNAL RECEIVED ####' + '\x1b[0m');
        await master.stop();
        process.exit(0);
    });

    master.start().catch(err => {
        console.error('\x1b[35m' + '#### 💥 MASTER API START FAILED ####' + '\x1b[0m', err);
        apiUtils.logScriptStatus(dbManager, 'master-api', 'stopped', 'Master API start failed (error)');
        process.exit(1);
    });
}

module.exports = MasterAPI;