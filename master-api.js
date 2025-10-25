/* =======================================================
 * MASTER-API.JS - 25 Oct PRODUCTION ORCHESTRATOR (REVISED)
 * **apis\ folder: Block-based parallel execution with conditional triggers
 * Integrated status logging via apiUtils to dbsetup.js tables.
 * full parallel execution per block, DB-driven heartbeat after Block 2 start,
 * =======run only perp_data backfil = "node master-api.js -only"  =============== */

const fs = require('fs');
const path = require('path');
const pLimit = require('p-limit');
const apiUtils = require('./api-utils');
const dbManager = require('./db/dbsetup');
const calcMetrics = require('./db/calc-metrics'); // Connect calc-metrics

// User Controls - Adjust script block prefixes & triggers
const BLOCK_1_PREFIX = '1-';
const BLOCK_2_PREFIX = '2-';
const TRIGGER_FILE = '1-ohlcv-h.js';
const TRIGGER_1Z_PREFIX = '1z-';

// User-configurable heartbeat frequency in milliseconds
const HEARTBEAT_INTERVAL_MS = 60000; // Default 1 minute

const STATUS_COLOR = '\x1b[35m\x1b[1m'; // Light purple for key status logs
const RESET = '\x1b[0m';

class MasterAPI {
  constructor() {
    this.scripts = [];
    this.running = false;
    this.heartbeatInterval = null;
    this.runningScripts = new Map(); // Map script name => { module, stopFunction? }
    this.metricsStarted = false; // NEW: Track if calc-metrics was started (for conditional stop)
  }

  // Initialization - Discover scripts from apis/ folder
  async initialize() {
    console.log(`${STATUS_COLOR}#### 🚀 MASTER API INITIALIZING ####${RESET}`);
    await this.discoverScripts();
    console.log(`${STATUS_COLOR}#### ✅ FOUND ${this.scripts.length} SCRIPTS IN apis/ ####${RESET}`);

    await apiUtils.logScriptStatus(
      dbManager,
      'master-api',
      'started',
      'Master-Api initialize: Block 1 → 1z (trigger) → Block 2'
    );
  }

  // Script discovery - Dynamic scan of apis/ folder
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
            isTrigger: file === TRIGGER_FILE,
            is1z: file.startsWith(TRIGGER_1Z_PREFIX)
          };
        });
    } catch (error) {
      console.error('❌ Error discovering scripts:', error.message);
      this.scripts = [];
    }
  }

  // Block determination - classify script by filename prefix only
  determineBlock(filename) {
    if (filename.startsWith(BLOCK_1_PREFIX)) return 'block1';
    if (filename.startsWith(TRIGGER_1Z_PREFIX)) return '1z';
    if (filename.startsWith(BLOCK_2_PREFIX)) return 'block2';
    return 'unknown';
  }

  // Run all Block 1 scripts fully in parallel
  async runBlock1() {
    const block1Scripts = this.scripts.filter(s => s.block === 'block1');

    if (block1Scripts.length === 0) {
      console.log(`${STATUS_COLOR}#### ⚠️ NO BLOCK 1 SCRIPTS FOUND ####${RESET}`);
      return;
    }

    console.log(`${STATUS_COLOR}#### 🔄 STARTING BLOCK 1: ${block1Scripts.map(s => s.name).join(', ')} ####${RESET}`);

    await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Block 1 scripts started');

    const promises = block1Scripts.map(script => this.runScript(script));

    const results = await Promise.allSettled(promises);

    // Check trigger file success to start 1z scripts
    const triggerResult = results.find(r => r.value?.script === TRIGGER_FILE);
    if (triggerResult && triggerResult.status === 'fulfilled' && triggerResult.value.success) {
      console.log(`${STATUS_COLOR}#### ⚡ TRIGGER: ${TRIGGER_FILE} COMPLETE - STARTING 1z SCRIPTS ####${RESET}`);
      this.runBlock1z(); // Start 1z scripts (non-blocking)
    } else {
      console.error(`${STATUS_COLOR}#### 🚨🚨🚨 CRITICAL: ${TRIGGER_FILE} FAILED OR NOT FOUND - 1z SCRIPTS BLOCKED 🚨🚨🚨 ####${RESET}`);
    }

    console.log(`${STATUS_COLOR}#### ✅ BLOCK 1 COMPLETE ####${RESET}`);

    await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Block 1 complete');
  }

  // Run all 1z scripts fully parallel, non-blocking
  async runBlock1z() {
    const block1zScripts = this.scripts.filter(s => s.is1z);

    if (block1zScripts.length === 0) {
      console.log(`${STATUS_COLOR}#### ⚠️ NO 1z SCRIPTS FOUND ####${RESET}`);
      return;
    }

    console.log(`${STATUS_COLOR}#### ⚡ STARTING 1z SCRIPTS: ${block1zScripts.map(s => s.name).join(', ')} ####${RESET}`);

    const promises = block1zScripts.map(script => this.runScript(script));

    Promise.allSettled(promises); // Non-blocking
  }

  // Run all Block 2 scripts fully parallel
  async runBlock2() {
    const block2Scripts = this.scripts.filter(s => s.block === 'block2');

    if (block2Scripts.length === 0) {
      console.log(`${STATUS_COLOR}#### ⚠️ NO BLOCK 2 SCRIPTS FOUND ####${RESET}`);
      return;
    }

    console.log(`${STATUS_COLOR}#### ⚡ STARTING BLOCK 2: ${block2Scripts.map(s => s.name).join(', ')} ####${RESET}`);

    await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Block 2 scripts started');

    const promises = block2Scripts.map(script => this.runScript(script));

    await Promise.allSettled(promises);

    console.log(`${STATUS_COLOR}#### 🎯 MASTER API FULLY OPERATIONAL! LIVE DATA ACTIVE ####${RESET}`);

    await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Master API fully operational, live data active');

    // Start heartbeat after Block 2 begins
    this.startHeartbeat();
  }

  // Run a script and return success status
  async runScript(script) {
    try {
      // Verify the file exists
      if (!fs.existsSync(script.path)) {
        console.error(`🚨 FILE NOT FOUND: ${script.path}`);
        return { success: false, script: script.name, error: new Error('File not found') };
      }

      // Clear the require cache for this script to ensure we get a fresh copy
      delete require.cache[require.resolve(script.path)];

      const scriptModule = require(script.path);

      // Execute the script based on available methods
      if (typeof scriptModule.calculateRSIForAllSymbols === 'function') {
        await scriptModule.calculateRSIForAllSymbols();
      } else if (typeof scriptModule.backfill === 'function') {
        await scriptModule.backfill();
      } else if (typeof scriptModule.execute === 'function') {
        await scriptModule.execute();
      } else if (typeof scriptModule.run === 'function') {
        await scriptModule.run();
      } else {
        await require(script.path);
      }

      this.runningScripts.set(script.name, scriptModule);

      return { success: true, script: script.name };
    } catch (error) {
      console.error(`❌ Error in ${script.name}:`, error.message);

      // Log error to database
      await apiUtils.logScriptStatus(
        dbManager,
        script.name,
        'error',
        `Error in ${script.name}: ${error.message}`
      );

      return { success: false, script: script.name, error };
    }
  }

  // Heartbeat: log running scripts based on DB status logs of recent 'running' entries
  startHeartbeat() {
    if (this.heartbeatInterval) return; // Prevent multiple intervals
    this.running = true;

    this.heartbeatInterval = setInterval(async () => {
      if (!this.running) return;

      try {
        const message = `master-api running real-time scripts`;
        console.log(`\x1b[35m\x1b[1m#### ⏱️ ${message} ####${RESET}`);
        await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', message);
      } catch (error) {
        console.error('❌ Error during master-api heartbeat:', error.message);
      }
    }, HEARTBEAT_INTERVAL_MS);
  }

  // Stop heartbeat and signal running scripts to stop
  async stop() {
    console.log(`${STATUS_COLOR}#### 🛑 STOPPING MASTER API ####${RESET}`);

    this.running = false;

    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }

    // Signal all running scripts to stop if they export a stop function
    for (const [scriptName, scriptModule] of this.runningScripts.entries()) {
      if (typeof scriptModule.stop === 'function') {
        try {
          console.log(`Stopping script ${scriptName}...`);
          await scriptModule.stop();
          console.log(`Script ${scriptName} stopped smoothly.`);
        } catch (error) {
          console.error(`Error stopping script ${scriptName}:`, error.message);
        }
      }
    }

    // Conditionally stop calc-metrics if it was started (UPDATED: Check tracker)
    if (this.metricsStarted && calcMetrics && typeof calcMetrics.stopContinuously === 'function') {
      try {
        console.log('Stopping calc-metrics.js...');
        await calcMetrics.stopContinuously();
        console.log('calc-metrics.js stopped smoothly.');
      } catch (error) {
        console.error('Error stopping calc-metrics.js:', error.message);
      }
    }

    await apiUtils.logScriptStatus(dbManager, 'master-api', 'stopped', 'Master-Api stopped smoothly');

    console.log(`${STATUS_COLOR}#### ✅ MASTER API STOPPED ####${RESET}`);
  }

  // Start sequence: initialize → Block 1 → Block 2 → heartbeat (UPDATED: Conditional metrics)
  async start(noMetrics) { // FIXED: No default param (avoids lint E0151; check inside)
    await this.initialize();
    await this.runBlock1();
    await this.runBlock2();

    // Conditionally start calc-metrics in continuous mode (UPDATED: Skip if noMetrics)
    const skipMetrics = noMetrics === true; // NEW: Internal check (true for -only; false/undefined = full mode)
    if (!skipMetrics) {
      this.metricsStarted = true; // NEW: Track for conditional stop in stop()
      calcMetrics.runContinuously()
        .then(() => {
          console.log("✅ calc-metrics.js triggered");
        })
        .catch(err => {
          console.error('❌ calc-metrics failed:', err);
          // Log the error to the database
          apiUtils.logScriptStatus(dbManager, 'master-api', 'error', `calc-metrics failed: ${err.message}`);
        });
    } else {
      console.log(`${STATUS_COLOR}#### 📊 SKIPPING calc-metrics.js (-only data mode) ####${RESET}`); // NEW: Log skip for data-only
      await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Master API in data-only mode (no metrics)'); // NEW: DB log for mode
    }
  }
}

/* =======================================================
 * MODULE ENTRY POINT - CREATE INSTANCE & HANDLE SIGNALS
 * ======================================================= */
if (require.main === module) {
  const args = process.argv.slice(2); // NEW: Parse CLI args
  const noMetrics = args.includes('-only'); // NEW: Detect -only flag for data-only mode

  const master = new MasterAPI();

  process.on('SIGINT', async () => {
    console.log('\n\x1b[35m\x1b[1m#### ⚠️ SHUTDOWN SIGNAL RECEIVED ####\x1b[0m');
    await master.stop();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    console.log('\n\x1b[35m\x1b[1m#### ⚠️ TERMINATION SIGNAL RECEIVED ####\x1b[0m');
    await master.stop();
    process.exit(0);
  });

  process.on('uncaughtException', async (err) => {
    console.error('\n\x1b[35m\x1b[1m#### 💥 UNCAUGHT EXCEPTION ####\x1b[0m', err);
    await apiUtils.logScriptStatus(dbManager, 'master-api', 'error', `Uncaught exception: ${err.message}`);
    await master.stop();
    process.exit(1);
  });

  process.on('unhandledRejection', async (reason) => {
    console.error('\n\x1b[35m\x1b[1m#### 💥 UNHANDLED REJECTION ####\x1b[0m', reason);
    await apiUtils.logScriptStatus(dbManager, 'master-api', 'error', `Unhandled rejection: ${reason.message}`);
    await master.stop();
    process.exit(1);
  });

  master.start(noMetrics) // UPDATED: Pass boolean (true for -only; false for full—matches internal check)
    .catch(async err => {
      console.error('\x1b[35m\x1b[1m#### 💥 MASTER API START FAILED ####\x1b[0m', err);
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
