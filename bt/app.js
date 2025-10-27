/*
  bt/app.js - Express Server for FadeAI Backtester (bt/ Mode)
  Description: REST API for UI (index.html), strategy management, backtesting via bt.js, AI suggestions via server.js.
               Compatible with new dbsetup.js (uses dbManager.pool, logs to perp_status/errors). Serves static files
               from bt/ (e.g., index.html). Endpoints per README: /symbols, /columns, /strategy, /backtest, /suggest, /health.
               Auto-prunes results/ to 100 files (per README Phase 5).
  Date: 24 Oct 2025
  Version: 2.0 (Renamed for bt/, enhanced logging/error handling, schema-aligned queries)
*/

const express = require('express');
const fs = require('fs');
const path = require('path');

// ===== IMPORTS (Updated for bt/ and new dbsetup.js) =====
const dbManager = require('../db/dbsetup');  // NEW: Instance from class-based dbsetup (pool, query, logging)
const { runBacktest } = require('./bt');  // UPDATED: ./bt for renamed bt.js (exports runBacktest)
const { suggestStrategy } = require('./ai-agent');  // Unchanged: server.js in bt/

const app = express();
const PORT = process.env.PORT || 8000;

// ===== MIDDLEWARE =====
app.use(express.json());
app.use(express.static(__dirname));  // Serves bt/ files (index.html, etc.)

// --- Logging Middleware (for requests) ---
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.url}`);
  next();
});

// ===== ENDPOINTS =====

// --- Serve UI (index.html) ---
app.get('/', (req, res) => {
  const htmlPath = path.join(__dirname, 'index.html');
  if (fs.existsSync(htmlPath)) {
    console.log('Serving bt/index.html');
    res.sendFile(htmlPath);
  } else {
    // Fallback if missing (basic HTML with links)
    console.log('bt/index.html not found - serving fallback');
    res.send(`
      <html><head><title>bt/ Backtester</title></head><body>
        <h1>🚀 bt/ Backtester Ready (24 Oct 2025)</h1>
        <p>Server OK. Test APIs:</p>
        <ul>
          <li><a href="/symbols">/symbols</a> (from perp_metrics)</li>
          <li><a href="/columns">/columns</a> (% chg fields)</li>
          <li>POST /strategy (save JSON)</li>
          <li>POST /backtest (run via bt.js)</li>
          <li>POST /suggest (AI via server.js)</li>
          <li><a href="/health">/health</a> (DB check)</li>
        </ul>
        <script>console.log('Fallback UI - Create index.html for full UI');</script>
      </body></html>
    `);
  }
});

// --- Get Symbols (from perp_metrics, new schema) ---
app.get('/symbols', async (req, res) => {
  try {
    console.log('Fetching symbols from perp_metrics...');
    const result = await dbManager.pool.query('SELECT DISTINCT symbol FROM perp_metrics ORDER BY symbol');
    const symbols = result.rows.map(row => row.symbol).filter(s => s);  // Clean, no nulls
    res.json({ symbols });
  } catch (error) {
    console.error('Error fetching symbols:', error.message);
    await dbManager.logError('app.js', 'DB', 'SYMBOLS_FAIL', error.message);
    res.status(500).json({ error: 'Failed to fetch symbols - check perp_metrics data' });
  }
});

// --- Get Columns (% chg from perp_metrics) ---
app.get('/columns', async (req, res) => {
  try {
    console.log('Fetching chg columns from perp_metrics...');
    const sql = `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = 'perp_metrics'
        AND column_name LIKE '%_chg_%'
        AND data_type IN ('numeric', 'double precision')
      ORDER BY column_name;
    `;
    const { rows } = await dbManager.pool.query(sql);
    const columns = rows.map(r => r.column_name);
    res.json({ columns });
  } catch (error) {
    console.error('Error fetching columns:', error.message);
    await dbManager.logError('app.js', 'DB', 'COLUMNS_FAIL', error.message);
    res.status(500).json({ error: 'Failed to fetch columns' });
  }
});

// --- Save Strategy (to strategy.json) ---
app.post('/strategy', (req, res) => {
  try {
    const strategyPath = path.join(__dirname, 'strategy.json');
    fs.writeFileSync(strategyPath, JSON.stringify(req.body, null, 2));
    console.log('Strategy saved:', req.body.name || 'Unnamed');
    res.json({ success: true, path: 'bt/strategy.json' });
  } catch (error) {
    console.error('Save strategy error:', error.message);
    res.status(500).json({ error: 'Failed to save strategy.json' });
  }
});

// --- Run Backtest (via bt.js) ---
app.post('/backtest', async (req, res) => {
  try {
    const strategyPath = path.join(__dirname, 'strategy.json');
    if (!fs.existsSync(strategyPath)) {
      return res.status(400).json({ error: 'strategy.json missing - POST to /strategy first' });
    }
    console.log('Running backtest via bt/bt.js...');
    const results = await runBacktest();  // Calls bt.js (updated for schema)
    await dbManager.logStatus('app.js', 'success', 'BACKTEST_RUN', { totalTrades: results.stats?.total || 0 });
    res.json(results);
  } catch (error) {
    console.error('Backtest error:', error.message);
    await dbManager.logError('app.js', 'BACKTEST', 'RUN_FAIL', error.message);
    res.status(500).json({ error: 'Backtest failed - check bt.js and DB' });
  }
});

// --- AI Suggestions (via server.js) ---
app.post('/suggest', async (req, res) => {
  try {
    const { prompt = '' } = req.body;
    console.log('Generating AI suggestions for:', prompt);
    const suggestions = await suggestStrategy(prompt);
    res.json({ suggestions });
  } catch (error) {
    console.error('Suggest error:', error.message);
    await dbManager.logError('app.js', 'AI', 'SUGGEST_FAIL', error.message);
    res.status(500).json({ error: 'AI suggestion failed - check server.js' });
  }
});

// --- Health Check (DB + Server) ---
app.get('/health', async (req, res) => {
  try {
    await dbManager.pool.query('SELECT 1 as health_check');  // Test query
    res.json({ 
      status: 'OK', 
      db: 'Connected (new schema: perp_metrics chg columns)', 
      timestamp: new Date().toISOString(),
      port: PORT,
      folder: 'bt/'  // Confirm rename
    });
  } catch (error) {
    console.error('Health check failed:', error.message);
    res.status(500).json({ status: 'Error', db: 'Failed', error: error.message });
  }
});

// ===== FILE PRUNING (Per README Phase 5) =====
// --- Auto-prune results/ to max 100 files (run after backtest) ---
function pruneResultsFolder() {
  const resultsDir = path.join(__dirname, 'results');
  if (!fs.existsSync(resultsDir)) {
    fs.mkdirSync(resultsDir);
    return;
  }
  const files = fs.readdirSync(resultsDir)
    .filter(f => f.endsWith('.json'))
    .map(f => ({ name: f, mtime: fs.statSync(path.join(resultsDir, f)).mtime }))
    .sort((a, b) => b.mtime - a.mtime);  // Newest first
  
  if (files.length > 100) {
    files.slice(100).forEach(f => {
      fs.unlinkSync(path.join(resultsDir, f.name));
      console.log(`Pruned old result: ${f.name}`);
    });
  }
}

// Call prune after /backtest (add to endpoint if needed)
app.post('/backtest', async (req, res) => {
  // ... existing code ...
  pruneResultsFolder();  // NEW: Prune after run
  // ...
});

// ===== SERVER STARTUP & SHUTDOWN =====
app.listen(PORT, () => {
  console.log(`🚀 bt/ Backtester Server running at http://localhost:${PORT} (24 Oct 2025)`);
  console.log(`📁 Serving from: ${__dirname} (bt/)`);
  console.log('🔍 Test: /health, /symbols, POST /backtest');
});

// --- Graceful Shutdown ---
process.on('SIGINT', async () => {
  console.log('Shutting down bt/ server...');
  await dbManager.close();  // Close DB pool
  process.exit(0);
});