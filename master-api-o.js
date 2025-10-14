/* =======================================================
 * MASTER-API.JS - PRODUCTION ORCHESTRATOR (REVISED)
 * **apis\ folder: Block-based parallel execution with conditional triggers
 * Integrated status logging via apiUtils to dbsetup.js tables.
 * ======================================================= */

const fs = require('fs');
const path = require('path');
const pLimit = require('p-limit');
const apiUtils = require('./api-utils');
const dbManager = require('./db/dbsetup');

/* =======================================================
 * USER CONTROLS - ADJUST SCRIPT BLOCK PREFIXES & TRIGGERS
 * ======================================================= */

// Block 1 runs first in parallel; completion triggers Block 2 to run
const BLOCK_1_PREFIX = '1-';

// Block 2 runs after Block 1 completes
const BLOCK_2_PREFIX = '2-';

// Trigger: This file's completion triggers all 1z scripts to run
const TRIGGER_FILE = '1-all-ohlcv-h.js';
const TRIGGER_1Z_PREFIX = '1z-';

// Parallel execution concurrency limit
const CONCURRENCY_LIMIT = 5;

/* =======================================================
 * MASTER API CLASS - CORE ORCHESTRATION
 * ======================================================= */
class MasterAPI {
    constructor() {
        this.scripts = [];
        this.running = false;
        this.intervals = {}; // Stores setInterval IDs for -c scripts
        this.heartbeatInterval = null;
    }

    /* =======================================================
     * INITIALIZATION - DISCOVER SCRIPTS FROM apis/ FOLDER
     * ======================================================= */
    async initialize() {
        console.log('\x1b[35m\x1b[1m' + '#### 🚀 MASTER API INITIALIZING ####' + '\x1b[0m');
        await this.discoverScripts();
        console.log('\x1b[35m\x1b[1m' + `#### ✅ FOUND ${this.scripts.length} SCRIPTS IN apis/ ####` + '\x1b[0m');
        
        await apiUtils.logScriptStatus(
            dbManager, 
            'master-api', 
            'started', 
            'Master-Api initialize: Block 1 → 1z (trigger) → Block 2'
        );
    }

    /* =======================================================
     * SCRIPT DISCOVERY - DYNAMIC SCAN OF apis/ FOLDER
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
                        block: this.determineBlock(file),
                        mode: this.determineMode(file)
                    };
                })
                .sort((a, b) => a.name.localeCompare(b.name));
        } catch (error) {
            console.error('❌ Error discovering scripts:', error.message);
            this.scripts = [];
        }
    }

    /* =======================================================
     * BLOCK DETERMINATION - CLASSIFY SCRIPT BY FILENAME PREFIX
     * ======================================================= */
    determineBlock(filename) {
        if (filename.startsWith(BLOCK_1_PREFIX)) return 'block1';
        if (filename.startsWith(TRIGGER_1Z_PREFIX)) return '1z';
        if (filename.startsWith(BLOCK_2_PREFIX)) return 'block2';
        return 'unknown';
    }

    /* =======================================================
     * MODE DETERMINATION - PARSE FILENAME FOR -h/-c
     * ======================================================= */
    determineMode(filename) {
        if (filename.endsWith('-h.js')) return 'history';
        if (filename.endsWith('-c.js')) return 'current';
        return 'unknown';
    }

    /* =======================================================
     * BLOCK 1 EXECUTION - RUN ALL 1-* SCRIPTS IN PARALLEL
     * Watches for TRIGGER_FILE completion to start 1z scripts
     * ======================================================= */
    async runBlock1() {
        const block1Scripts = this.scripts.filter(s => s.block === 'block1');
        
        if (block1Scripts.length === 0) {
            console.log('\x1b[35m\x1b[1m' + '#### ⚠️ NO BLOCK 1 SCRIPTS FOUND ####' + '\x1b[0m');
            return;
        }

        console.log('\x1b[35m\x1b[1m' + `#### 🔄 STARTING BLOCK 1: ${block1Scripts.map(s => s.name).join(', ')} ####` + '\x1b[0m');

        await apiUtils.logScriptStatus(
            dbManager, 
            'master-api', 
            'running', 
            'Block 1 scripts started'
        );

        const limit = pLimit(CONCURRENCY_LIMIT);
        
        const promises = block1Scripts.map(script => 
            limit(async () => {
                const result = await this.runScript(script);
                
                // Check if this is the trigger file
                if (script.name === TRIGGER_FILE && result.success) {
                    console.log('\x1b[35m\x1b[1m' + `#### ⚡ TRIGGER: ${TRIGGER_FILE} COMPLETE - STARTING 1z SCRIPTS ####` + '\x1b[0m');
                    this.runBlock1z(); // Start 1z scripts immediately (non-blocking)
                } else if (script.name === TRIGGER_FILE && !result.success) {
                    console.error('\x1b[35m\x1b[1m' + `#### 🚨🚨🚨 CRITICAL: ${TRIGGER_FILE} FAILED - 1z SCRIPTS BLOCKED 🚨🚨🚨 ####` + '\x1b[0m');
                }
                
                return result;
            })
        );

        await Promise.allSettled(promises);
        
        console.log('\x1b[35m\x1b[1m' + '#### ✅ BLOCK 1 COMPLETE ####' + '\x1b[0m');
        
        await apiUtils.logScriptStatus(
            dbManager, 
            'master-api', 
            'running', 
            'Block 1 complete'
        );
    }

    /* =======================================================
     * 1Z EXECUTION - RUN ALL 1z-* SCRIPTS IN PARALLEL
     * Triggered by TRIGGER_FILE completion
     * ======================================================= */
    async runBlock1z() {
        const block1zScripts = this.scripts.filter(s => s.block === '1z');
        
        if (block1zScripts.length === 0) {
            console.log('\x1b[35m\x1b[1m' + '#### ⚠️ NO 1z SCRIPTS FOUND ####' + '\x1b[0m');
            return;
        }

        console.log('\x1b[35m\x1b[1m' + `#### ⚡ STARTING 1z SCRIPTS: ${block1zScripts.map(s => s.name).join(', ')} ####` + '\x1b[0m');

        const limit = pLimit(CONCURRENCY_LIMIT);
        
        const promises = block1zScripts.map(script => 
            limit(async () => {
                const result = await this.runScript(script);
                
                // Start interval for -c scripts immediately after backfill
                if (script.mode === 'current') {
                    this.startInterval(script);
                }
                
                return result;
            })
        );

        Promise.allSettled(promises); // Non-blocking - don't wait for 1z to finish
    }

    /* =======================================================
     * BLOCK 2 EXECUTION - RUN ALL 2-* SCRIPTS IN PARALLEL
     * Starts after Block 1 completes (regardless of 1z status)
     * ======================================================= */
    async runBlock2() {
        const block2Scripts = this.scripts.filter(s => s.block === 'block2');
        
        if (block2Scripts.length === 0) {
            console.log('\x1b[35m\x1b[1m' + '#### ⚠️ NO BLOCK 2 SCRIPTS FOUND ####' + '\x1b[0m');
            return;
        }

        console.log('\x1b[35m\x1b[1m' + `#### ⚡ STARTING BLOCK 2: ${block2Scripts.map(s => s.name).join(', ')} ####` + '\x1b[0m');

        await apiUtils.logScriptStatus(
            dbManager, 
            'master-api', 
            'running', 
            'Block 2 scripts started'
        );

        const limit = pLimit(CONCURRENCY_LIMIT);
        
        const promises = block2Scripts.map(script => 
            limit(async () => {
                const result = await this.runScript(script);
                
                // Start interval for -c scripts immediately after backfill
                if (script.mode === 'current') {
                    this.startInterval(script);
                }
                
                return result;
            })
        );

        await Promise.allSettled(promises);
        
        console.log('\x1b[35m\x1b[1m' + '#### ✅ BLOCK 2 COMPLETE ####' + '\x1b[0m');
        
        await apiUtils.logScriptStatus(
            dbManager, 
            'master-api', 
            'running', 
            'Block 2 complete'
        );
    }

    /* =======================================================
     * START INTERVAL FOR A -c SCRIPT
     * Calls execute()/pollAllSymbols()/run() or require fallback every 60s
     * ======================================================= */
    startInterval(script) {
        const interval = 60000; // 1 minute
        
        this.intervals[script.name] = setInterval(async () => {
            try {
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
     * Returns success status for trigger monitoring
     * ======================================================= */
    async runScript(script) {
        try {
            const scriptModule = require(script.path);
            if (typeof scriptModule.backfill === 'function') {
                await scriptModule.backfill();
            } else if (typeof scriptModule.execute === 'function') {
                await scriptModule.execute();
            } else {
                await require(script.path);
            }
            return { success: true, script: script.name };
        } catch (error) {
            console.error(`❌ Error in ${script.name}:`, error.message);
            return { success: false, script: script.name, error };
        }
    }

    /* =======================================================
     * START CONTINUOUS MODE HEARTBEAT
     * Logs running -c scripts every 1 minute
     * ======================================================= */
    startHeartbeat() {
        this.running = true;
        
        this.heartbeatInterval = setInterval(async () => {
            if (this.running) {
                const runningScripts = Object.keys(this.intervals).join(', ');
                const message = `master-api is running real-time: ${runningScripts}`;
                
                console.log('\x1b[35m\x1b[1m' + `#### 💓 ${message} ####` + '\x1b[0m');
                
                await apiUtils.logScriptStatus(
                    dbManager, 
                    'master-api', 
                    'running', 
                    message
                );
            }
        }, 60000); // 1 minute
    }

    /* =======================================================
     * SHUTDOWN MODE - GRACEFUL STOP OF -c SCRIPTS
     * ======================================================= */
    stopLiveMode() {
        console.log('\x1b[35m\x1b[1m' + '#### 🛑 STOPPING LIVE MODE ####' + '\x1b[0m');
        
        Object.keys(this.intervals).forEach(scriptName => {
            clearInterval(this.intervals[scriptName]);
        });
        
        if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
        
        this.intervals = {};
        this.running = false;
        
        console.log('\x1b[35m\x1b[1m' + '#### ✅ LIVE MODE STOPPED ####' + '\x1b[0m');
    }

    /* =======================================================
     * START SEQUENCE - INIT → BLOCK 1 → BLOCK 2 → HEARTBEAT
     * ======================================================= */
    async start() {
        await this.initialize();
        await this.runBlock1();
        await this.runBlock2();
        this.startHeartbeat();
        
        console.log('\x1b[35m\x1b[1m' + '#### 🎯 MASTER API FULLY OPERATIONAL! ####' + '\x1b[0m');
        console.log('\x1b[35m\x1b[1m' + '#### 📊 ALL BLOCKS COMPLETE, LIVE DATA ACTIVE ####' + '\x1b[0m');
    }

    /* =======================================================
     * STOP SEQUENCE - GRACEFUL SHUTDOWN
     * ======================================================= */
    async stop() {
        console.log('\x1b[35m\x1b[1m' + '#### 🛑 SHUTTING DOWN MASTER API ####' + '\x1b[0m');
        
        this.stopLiveMode();
        
        await apiUtils.logScriptStatus(
            dbManager, 
            'master-api', 
            'stopped', 
            'Master-Api stopped smoothly'
        );
        
        console.log('\x1b[35m\x1b[1m' + '#### 👋 MASTER API SHUTDOWN COMPLETE ####' + '\x1b[0m');
    }
}

/* =======================================================
 * MODULE ENTRY POINT - CREATE INSTANCE & HANDLE SIGNALS
 * ======================================================= */
if (require.main === module) {
    const master = new MasterAPI();

    process.on('SIGINT', async () => {
        console.log('\n\x1b[35m\x1b[1m' + '#### ⚠️ SHUTDOWN SIGNAL RECEIVED ####' + '\x1b[0m');
        await master.stop();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log('\n\x1b[35m\x1b[1m' + '#### ⚠️ TERMINATION SIGNAL RECEIVED ####' + '\x1b[0m');
        await master.stop();
        process.exit(0);
    });

    master.start().catch(async err => {
        console.error('\x1b[35m\x1b[1m' + '#### 💥 MASTER API START FAILED ####' + '\x1b[0m', err);
        await apiUtils.logScriptStatus(
            dbManager, 
            'master-api', 
            'stopped', 
            'Master-Api start failed (error)'
        );
        process.exit(1);
    });
}

module.exports = MasterAPI;