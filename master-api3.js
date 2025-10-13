/* =======================================================
 * MASTER-API3.JS - PRODUCTION ORCHESTRATOR
 * **apis\ folder: Runs -h backfill scripts parallel then -c files for real-time
 * Integrated status logging via apiUtils to dbsetup.js tables.
 * ======================================================= */

const fs = require('fs');
const path = require('path');
const pLimit = require('p-limit'); // For concurrency control
const apiUtils = require('./api-utils'); // For status logging (wraps dbManager)
const dbManager = require('./db/dbsetup'); // Provides dbManager for logging

/*
 * Configuration: Specify scripts to run first or last in backfill (-h) and live (-c) modes
 * Empty arrays mean no special ordering.
 */
const FIRST_H_SCRIPTS = ['all-ohlcv-h.js']; // e.g. 'all-pfr-h.js'
const LAST_H_SCRIPTS = ['rsi-h.js', 'bin-tv-h.js'];
const FIRST_C_SCRIPTS = ['web-ohlcv-c.js'];
const LAST_C_SCRIPTS = ['web-ttv-c.js'];

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
        await apiUtils.logScriptStatus(dbManager, 'master-api', 'started', 'Master-Api initialize -h scripts, followed by -c scripts.');
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
                        mtime: stats.mtime,
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
  if (filename.endsWith('-h.js')) {
    return { mode: 'history', fullName: filename };
  } else if (filename.endsWith('-c.js')) {
    return { mode: 'current', fullName: filename };
  } else {
    return { mode: 'unknown', fullName: filename };
  }
}

    /* =======================================================
     * BACKFILL MODE - RUN ALL -h SCRIPTS
     * Runs FIRST_H_SCRIPTS sequentially, then parallel all others except FIRST/LAST, then LAST_H_SCRIPTS sequentially.
     * Logs start and completion with concise console and DB messages.
     * ======================================================= */
    async runBackfill() {
        const historyScripts = this.scripts.filter(s => s.type.mode === 'history');
        if (historyScripts.length === 0) {
            console.log('\x1b[35m' + '#### ✅ NO -h SCRIPTS, SKIPPING BACKFILL ####' + '\x1b[0m');
            return;
        }

        console.log('\x1b[35m' + `#### 🔄 STARTING BACKFILL MODE (-h FILES) ${historyScripts.map(s => s.name).join(', ')} ####` + '\x1b[0m');

        // Log DB status for backfill start
        await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Master-Api backfill -h scripts started');

        // Run FIRST_H_SCRIPTS sequentially
        for (const firstName of FIRST_H_SCRIPTS) {
            const script = historyScripts.find(s => s.name === firstName);
            if (script) await this.runScript(script);
        }

        // Filter out FIRST and LAST scripts
        const middleScripts = historyScripts.filter(s => !FIRST_H_SCRIPTS.includes(s.name) && !LAST_H_SCRIPTS.includes(s.name));

        // Run middle scripts in parallel with concurrency limit
        const limit = pLimit(5);
        const promises = middleScripts.map(script => limit(() => this.runScript(script)));
        await Promise.allSettled(promises);

        // Run LAST_H_SCRIPTS sequentially
        for (const lastName of LAST_H_SCRIPTS) {
            const script = historyScripts.find(s => s.name === lastName);
            if (script) await this.runScript(script);
        }

        console.log('\x1b[35m' + '#### 📊 BACKFILL MODE COMPLETE ####' + '\x1b[0m');

        // Log DB status for backfill complete
        await apiUtils.logScriptStatus(dbManager, 'master-api', 'backfill complete', 'Master-Api backfill -h scripts finished.');
    }

    /* =======================================================
     * LIVE MODE - RUN ALL -c SCRIPTS
     * Runs FIRST_C_SCRIPTS sequentially, then parallel all others except FIRST/LAST, then LAST_C_SCRIPTS sequentially.
     * Uses setInterval for continuous polling.
     * Logs start and heartbeat with concise console and DB messages.
     * ======================================================= */
    startLiveMode() {
        const currentScripts = this.scripts.filter(s => s.type.mode === 'current');
        if (currentScripts.length === 0) {
            console.log('\x1b[35m' + '#### ⚠️ NO -c SCRIPTS FOUND ####' + '\x1b[0m');
            return;
        }

        console.log('\x1b[35m' + `#### ⚡ STARTING LIVE MODE (-c FILES) ${currentScripts.map(s => s.name).join(', ')} ####` + '\x1b[0m');

        // Log DB status for live mode start
        apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Master-Api live.');

        // Run FIRST_C_SCRIPTS sequentially
        const runSequential = async (names) => {
            for (const name of names) {
                const script = currentScripts.find(s => s.name === name);
                if (script) await this.startInterval(script);
            }
        };

        // Run middle scripts in parallel
        const middleScripts = currentScripts.filter(s => !FIRST_C_SCRIPTS.includes(s.name) && !LAST_C_SCRIPTS.includes(s.name));

        runSequential(FIRST_C_SCRIPTS).then(() => {
            middleScripts.forEach(script => {
                this.startInterval(script);
            });
            runSequential(LAST_C_SCRIPTS);
        });

        this.running = true;

        console.log('\x1b[35m' + '#### ✅ LIVE MODE ACTIVE - ALL -c SCRIPTS RUNNING ####' + '\x1b[0m');

        // 3min heartbeat for Master -c status
        this.heartbeatInterval = setInterval(async () => {
            if (this.running) {
                await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Master API running -c continuous active');
                console.log('\x1b[35m' + '#### 🎯 MASTER API RUNNING -c SCRIPTS ####' + '\x1b[0m');
            }
        }, 180000); // 3min
    }

    /* =======================================================
     * START INTERVAL FOR A -c SCRIPT
     * Calls execute()/pollAllSymbols()/run() or require fallback every 60s
     * ======================================================= */
    startInterval(script) {
        const interval = 60000;
        // Removed per-script start log to reduce noise
        this.intervals[script.name] = setInterval(async () => {
            try {
                // Removed per-script running/completed logs to reduce noise
                const scriptModule = require(script.path);
                if (typeof scriptModule.execute === 'function') {
                    await scriptModule.execute();
                } else if (typeof scriptModule.pollAllSymbols === 'function') {
                    await scriptModule.pollAllSymbols();
                } else if (typeof scriptModule.run === 'function') {
                    await scriptModule.run();
                } else {
                    await require(script.path);
                }
            } catch (error) {
                console.error(`❌ Error in ${script.name}:`, error.message);
            }
        }, interval);
    }

    /* =======================================================
     * RUN A SCRIPT (used in backfill)
     * Calls backfill()/execute() or require fallback
     * ======================================================= */
    async runScript(script) {
        try {
            // Removed per-script execution start/completion logs to reduce noise
            const scriptModule = require(script.path);
            if (typeof scriptModule.backfill === 'function') {
                await scriptModule.backfill();
            } else if (typeof scriptModule.execute === 'function') {
                await scriptModule.execute();
            } else {
                await require(script.path);
            }
        } catch (error) {
            console.error(`❌ Error in ${script.name}:`, error.message);
        }
    }

    /* =======================================================
     * SHUTDOWN MODE - GRACEFUL STOP OF -c SCRIPTS
     * Clears all intervals; logs "stopped smoothly".
     * ======================================================= */
    stopLiveMode() {
        console.log('\x1b[35m' + '#### 🛑 STOPPING LIVE MODE ####' + '\x1b[0m');
        Object.keys(this.intervals).forEach(scriptName => {
            clearInterval(this.intervals[scriptName]);
            // Removed per-script stop logs to reduce noise
        });
        if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
        this.intervals = {};
        this.running = false;
        console.log('\x1b[35m' + '#### ✅ LIVE MODE STOPPED ####' + '\x1b[0m');
        // Master Status: stopped smoothly
        apiUtils.logScriptStatus(dbManager, 'master-api', 'stopped', 'Master-Api smoothly stopped.');
    }

    /* =======================================================
     * START SEQUENCE - INIT → BACKFILL → LIVE MODE
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
     * ======================================================= */
    async stop() {
        console.log('\x1b[35m' + '#### 🛑 SHUTTING DOWN MASTER API ####' + '\x1b[0m');
        this.stopLiveMode();
        console.log('\x1b[35m' + '#### 👋 MASTER API SHUTDOWN COMPLETE ####' + '\x1b[0m');
        // Master Status: stopped (error if needed, but smooth here)
        if (this.running) {
            await apiUtils.logScriptStatus(dbManager, 'master-api', 'stopped', 'Master-Api stopped (error)');
        }
    }
}

/* =======================================================
 * MODULE ENTRY POINT - CREATE INSTANCE & HANDLE SIGNALS
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
        apiUtils.logScriptStatus(dbManager, 'master-api', 'stopped', 'Master-Api start failed (error)');
        process.exit(1);
    });
}

module.exports = MasterAPI;