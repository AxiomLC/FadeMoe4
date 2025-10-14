/* =======================================================
 * MASTER-API.JS - PRODUCTION ORCHESTRATOR (REVISED)
 * **apis\ folder: Block-based parallel execution with conditional triggers
 * Integrated status logging via apiUtils to dbsetup.js tables.
 * Revised per user instructions: no interval-based repeats, no suffix detection,
 * full parallel execution per block, DB-driven heartbeat after Block 2 start,
 * streamlined stopping logic, user stoppage stops all scripts,
 * error logging on unexpected shutdown, and heartbeat frequency control.
 * ======================================================= */

const fs = require('fs');
const path = require('path');
const pLimit = require('p-limit');
const apiUtils = require('./api-utils');
const dbManager = require('./db/dbsetup');

// User Controls - Adjust script block prefixes & triggers
const BLOCK_1_PREFIX = '1-';
const BLOCK_2_PREFIX = '2-';
const TRIGGER_FILE = '1-all-ohlcv-h.js';
const TRIGGER_1Z_PREFIX = '1z-';

// User-configurable heartbeat frequency in milliseconds
const HEARTBEAT_INTERVAL_MS = 60000; // Default 1 minute

class MasterAPI {
  constructor() {
    this.scripts = [];
    this.running = false;
    this.heartbeatInterval = null;
    this.runningScripts = new Map(); // Map script name => { module, stopFunction? }
  }

  // Initialization - Discover scripts from apis/ folder
  async initialize() {
    console.log('\x1b[35m\x1b[1m#### 🚀 MASTER API INITIALIZING ####\x1b[0m');
    await this.discoverScripts();
    console.log(`\x1b[35m\x1b[1m#### ✅ FOUND ${this.scripts.length} SCRIPTS IN apis/ ####\x1b[0m`);

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
            block: this.determineBlock(file)
          };
        })
        .sort((a, b) => a.name.localeCompare(b.name));
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
  // Trigger 1z scripts only after trigger file completes successfully
  async runBlock1() {
    const block1Scripts = this.scripts.filter(s => s.block === 'block1');

    if (block1Scripts.length === 0) {
      console.log('\x1b[35m\x1b[1m#### ⚠️ NO BLOCK 1 SCRIPTS FOUND ####\x1b[0m');
      return;
    }

    console.log(`\x1b[35m\x1b[1m#### 🔄 STARTING BLOCK 1: ${block1Scripts.map(s => s.name).join(', ')} ####\x1b[0m`);

    await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Block 1 scripts started');

    const promises = block1Scripts.map(script => this.runScript(script));

    const results = await Promise.allSettled(promises);

    // Check trigger file success to start 1z scripts
    const triggerResult = results.find(r => r.value?.script === TRIGGER_FILE);
    if (triggerResult && triggerResult.status === 'fulfilled' && triggerResult.value.success) {
      console.log(`\x1b[35m\x1b[1m#### ⚡ TRIGGER: ${TRIGGER_FILE} COMPLETE - STARTING 1z SCRIPTS ####\x1b[0m`);
      this.runBlock1z(); // Start 1z scripts (non-blocking)
    } else {
      console.error(`\x1b[35m\x1b[1m#### 🚨🚨🚨 CRITICAL: ${TRIGGER_FILE} FAILED OR NOT FOUND - 1z SCRIPTS BLOCKED 🚨🚨🚨 ####\x1b[0m`);
    }

    console.log('\x1b[35m\x1b[1m#### ✅ BLOCK 1 COMPLETE ####\x1b[0m');

    await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Block 1 complete');
  }

  // Run all 1z scripts fully parallel, non-blocking
  async runBlock1z() {
    const block1zScripts = this.scripts.filter(s => s.block === '1z');

    if (block1zScripts.length === 0) {
      console.log('\x1b[35m\x1b[1m#### ⚠️ NO 1z SCRIPTS FOUND ####\x1b[0m');
      return;
    }

    console.log(`\x1b[35m\x1b[1m#### ⚡ STARTING 1z SCRIPTS: ${block1zScripts.map(s => s.name).join(', ')} ####\x1b[0m`);

    const promises = block1zScripts.map(script => this.runScript(script));

    Promise.allSettled(promises); // Non-blocking
  }

  // Run all Block 2 scripts fully parallel
  // Start only after all Block 1 scripts complete
  async runBlock2() {
    const block2Scripts = this.scripts.filter(s => s.block === 'block2');

    if (block2Scripts.length === 0) {
      console.log('\x1b[35m\x1b[1m#### ⚠️ NO BLOCK 2 SCRIPTS FOUND ####\x1b[0m');
      return;
    }

    console.log(`\x1b[35m\x1b[1m#### ⚡ STARTING BLOCK 2: ${block2Scripts.map(s => s.name).join(', ')} ####\x1b[0m`);

    await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Block 2 scripts started');

    const promises = block2Scripts.map(script => this.runScript(script));

    await Promise.allSettled(promises);

    console.log('\x1b[35m\x1b[1m#### 🎯 MASTER API FULLY OPERATIONAL! LIVE DATA ACTIVE ####\x1b[0m');

    await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', 'Master API fully operational, live data active');

    // Start heartbeat after Block 2 begins
    this.startHeartbeat();
  }

  // Run a script and return success status
  async runScript(script) {
    try {
      const scriptModule = require(script.path);
      if (typeof scriptModule.backfill === 'function') {
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
        // Query DB for scripts with 'running' status updated in last 3 minutes
        const result = await dbManager.pool.query(`
          SELECT DISTINCT perpspec AS script_name
          FROM perp_status
          WHERE status = 'running' AND last_updated >= NOW() - INTERVAL '3 minutes'
        `);

        const runningScripts = result.rows.map(row => row.script_name).join(', ') || 'none';

        const message = `master-api is running real-time: ${runningScripts}`;

        console.log(`\x1b[35m\x1b[1m#### 💓 ${message} ####\x1b[0m`);

        await apiUtils.logScriptStatus(dbManager, 'master-api', 'running', message);

      } catch (error) {
        console.error('❌ Error during master-api heartbeat:', error.message);
      }
    }, HEARTBEAT_INTERVAL_MS);
  }

  // Stop heartbeat and signal running scripts to stop
  async stop() {
    console.log('\x1b[35m\x1b[1m#### 🛑 STOPPING MASTER API ####\x1b[0m');

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

    await apiUtils.logScriptStatus(dbManager, 'master-api', 'stopped', 'Master-Api stopped smoothly');

    console.log('\x1b[35m\x1b[1m#### ✅ MASTER API STOPPED ####\x1b[0m');
  }

  // Start sequence: initialize → Block 1 → Block 2 → heartbeat
  async start() {
    await this.initialize();
    await this.runBlock1();
    await this.runBlock2();
  }
}

/* =======================================================
 * MODULE ENTRY POINT - CREATE INSTANCE & HANDLE SIGNALS
 * ======================================================= */
if (require.main === module) {
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
    await apiUtils.logScriptStatus(dbManager, 'master-api', 'error', `Unhandled rejection: ${reason}`);
    await master.stop();
    process.exit(1);
  });

  master.start().catch(async err => {
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