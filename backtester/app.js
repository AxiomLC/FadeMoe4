const express = require('express');
const fs = require('fs');
const path = require('path');
const db = require('../db/dbsetup');
const runBacktest = require('./backtester');
const { suggestStrategy } = require('./server');

const app = express();
const PORT = 8000;

app.use(express.json());
app.use(express.static(__dirname));

// Serve the HTML interface
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Get available symbols
app.get('/symbols', async (req, res) => {
  const query = 'SELECT DISTINCT symbol FROM perp_metrics ORDER BY symbol';
  const { rows } = await db.pool.query(query);
  res.json(rows);
});

// Get metric columns
app.get('/columns', async (req, res) => {
  const sql = `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'perp_metrics'
      AND column_name LIKE '%_chg_%'
    ORDER BY column_name;
  `;
  const { rows } = await db.pool.query(sql);
  res.json(rows.map(r => r.column_name));
});

// Save strategy
app.post('/strategy', (req, res) => {
  fs.writeFileSync(__dirname + '/strategy.json', JSON.stringify(req.body, null, 2));
  res.json({ success: true });
});

// Run backtest
app.post('/backtest', async (req, res) => {
  await runBacktest();
  const results = JSON.parse(fs.readFileSync(__dirname + '/results.json', 'utf8'));
  res.json(results);
});

// AI suggest strategies
app.post('/suggest', async (req, res) => {
  const suggestions = await suggestStrategy();
  res.json({ suggestions });
});

app.listen(PORT, () => {
  console.log(`🚀 FadeAI Backtester running at http://localhost:${PORT}`);
});