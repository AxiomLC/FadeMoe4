/*
  bt/ai-agent.js - AI Strategy Suggestions Module (Renamed from server.js)
  Description: Generates AlgoQL strategies using OpenAI (gpt-4o-mini) based on trending tokens, recent metrics from perp_metrics,
               and available chg columns. Exports suggestStrategy() for app.js /suggest endpoint. CLI mode with --run.
               Compatible with new dbsetup.js (uses dbManager.pool.query()). Writes to bt/ai_suggestions.json.
               Fallback to stub if OpenAI fails/missing key. Logs errors to perp_errors.
  Date: 24 Oct 2025
  Version: 1.2 (Renamed for clarity, fixed DB/path/model, enhanced prompt)
*/

const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');
const OpenAI = require('openai');

// ===== IMPORTS (Updated for new dbsetup.js) =====
const dbManager = require('../db/dbsetup');  // UPDATED: dbManager instance (pool, logError)
const fetch = require('node-fetch');

dotenv.config();

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY || 'YOUR_API_KEY_HERE',  // Set in .env
});

const MODEL = 'gpt-4o-mini';  // UPDATED: Valid model
const MAX_TOKENS = parseInt(process.env.MAX_TOKENS) || 1500;

// ===== HELPER FUNCTIONS =====

// --- getTrendingTokens: Fetch from CoinGecko ---
async function getTrendingTokens() {
  try {
    const res = await fetch('https://api.coingecko.com/api/v3/search/trending');
    if (!res.ok) throw new Error('CoinGecko API failed');
    const data = await res.json();
    return data.coins.map(c => c.item.symbol.toUpperCase()).slice(0, 10);  // Top 10
  } catch (error) {
    console.warn('Trending fetch failed:', error.message);
    await dbManager.logError('ai-agent.js', 'API', 'TRENDS_FAIL', error.message);
    return ['BTC', 'ETH', 'SOL'];  // Fallback
  }
}

// --- getRecentMetricsSummary: Query perp_metrics (1h avg, new schema) ---
async function getRecentMetricsSummary() {
  const query = `
    SELECT symbol,
           AVG(c_chg_1m) AS price_1m,
           AVG(v_chg_1m) AS vol_1m,
           AVG(oi_chg_1m) AS oi_1m,
           AVG(pfr_chg_1m) AS fund_1m,
           AVG(rsi1) AS rsi_avg  -- Raw RSI (not chg)
    FROM perp_metrics
    WHERE ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '1 hour') * 1000
      AND symbol IS NOT NULL
    GROUP BY symbol
    HAVING COUNT(*) > 5  -- Min samples
    ORDER BY ABS(AVG(c_chg_1m)) DESC
    LIMIT 10;
  `;
  try {
    const { rows } = await dbManager.pool.query(query);  // UPDATED: dbManager.pool
    return rows.map(row => ({
      ...row,
      price_1m: parseFloat(row.price_1m || 0),
      vol_1m: parseFloat(row.vol_1m || 0),
      oi_1m: parseFloat(row.oi_1m || 0),
      fund_1m: parseFloat(row.fund_1m || 0),
      rsi_avg: parseFloat(row.rsi_avg || 50)
    }));
  } catch (error) {
    console.error('Metrics query failed:', error.message);
    await dbManager.logError('ai-agent.js', 'DB', 'METRICS_FAIL', error.message);
    return [];  // Empty fallback
  }
}

// --- getMetricColumns: List % chg columns from perp_metrics ---
async function getMetricColumns() {
  const sql = `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'perp_metrics'
      AND column_name LIKE '%_chg_%'
      AND data_type IN ('numeric', 'double precision')
    ORDER BY column_name;
  `;
  try {
    const { rows } = await dbManager.pool.query(sql);  // UPDATED: dbManager.pool
    return rows.map(r => r.column_name);
  } catch (error) {
    console.error('Columns query failed:', error.message);
    await dbManager.logError('ai-agent.js', 'DB', 'COLUMNS_FAIL', error.message);
    return ['pfr_chg_5m', 'oi_chg_5m', 'v_chg_1m'];  // Fallback
  }
}

// ===== MAIN EXPORT: suggestStrategy (For app.js /suggest) =====
async function suggestStrategy(prompt = '') {
  console.log('🤖 AI Agent: Generating suggestions (prompt:', prompt, ')');

  const trending = await getTrendingTokens();
  const metrics = await getRecentMetricsSummary();
  const columns = await getMetricColumns();

  const trendStr = trending.join(', ') || 'none';
  const metricSummary = metrics
    .map(r => `${r.symbol}: price=${r.price_1m.toFixed(2)}%, vol=${r.vol_1m.toFixed(2)}%, fund=${r.fund_1m.toFixed(2)}%, rsi=${r.rsi_avg.toFixed(1)}`)
    .join('\n') || 'No recent data';

  // System prompt per README (AlgoQL format, warnings)
  const systemPrompt = `You are FadeAI, a crypto perp strategy advisor. Suggest 2-3 profitable AlgoQL strategies based on data.
Available: 10 days 1m data, symbols (BTC,ETH,...), exchanges (bin,byb,okx), metrics (${columns.join(', ')}).
Timeframes: chg_1m/5m/10m. Format: SYMBOLS;DIRECTION;CONDITIONS (e.g., BTC;Long;bin_pfr_chg_5m>0.31).
Warn on risks (overfitting). Output ONLY valid JSON array of objects: [{algoql: "...", pf: 1.2, winRate: "55%", explanation: "..."}].`;

  const userPrompt = `${systemPrompt}

Trending: ${trendStr}
Recent metrics (1h avg):
${metricSummary}

User request: ${prompt || 'Suggest long strategies for top movers.'}`;

  // Stub fallback if no OpenAI key
  if (!process.env.OPENAI_API_KEY || process.env.OPENAI_API_KEY === 'YOUR_API_KEY_HERE') {
    console.warn('No OpenAI key - using stub suggestions');
    const stub = [
      {
        algoql: 'BTC,ETH;Long;bin_pfr_chg_5m>0.31 AND byb_oi_chg_5m>0.25',
        pf: 1.34,
        winRate: '56.2%',
        explanation: 'Funding/OI divergence for longs (per README example). Risk: Low volume.',
        confidence: 'medium'
      },
      {
        algoql: 'SOL;Short;okx_lsr_chg_10m<-0.5 OR bin_v_chg_5m>2.1',
        pf: 1.12,
        winRate: '48%',
        explanation: 'LSR drop or volume spike for shorts. Test on recent data.',
        confidence: 'low'
      }
    ];
    // Write stub to file
    fs.writeFileSync(path.join(__dirname, 'ai_suggestions.json'), JSON.stringify(stub, null, 2));
    return stub;
  }

  try {
    const res = await openai.chat.completions.create({
      model: MODEL,
      messages: [
        { role: 'system', content: 'Output ONLY a valid JSON array of strategy objects. No extra text.' },
        { role: 'user', content: userPrompt }
      ],
      temperature: 0.7,
      max_tokens: MAX_TOKENS,
    });

    const text = res.choices[0].message.content.trim();
    let strategies;
    try {
      strategies = JSON.parse(text);  // Parse AI output
    } catch (parseErr) {
      throw new Error(`Invalid JSON from AI: ${parseErr.message}`);
    }

    // Write to bt/ai_suggestions.json (per README)
    fs.writeFileSync(path.join(__dirname, 'ai_suggestions.json'), JSON.stringify(strategies, null, 2));

    console.log(`✅ AI generated ${strategies.length} strategies.`);
    await dbManager.logStatus('ai-agent.js', 'success', 'AI_SUGGEST', { count: strategies.length, prompt });
    return strategies;
  } catch (error) {
    console.error('AI generation failed:', error.message);
    await dbManager.logError('ai-agent.js', 'AI', 'GENERATE_FAIL', error.message, { prompt, model: MODEL });
    // Fallback to stub
    const fallback = [
      {
        algoql: 'BTC,ETH;Long;bin_pfr_chg_5m>0.31 AND byb_oi_chg_5m>0.25',
        pf: 1.34,
        winRate: '56.2%',
        explanation: 'Fallback: Funding/OI divergence.',
        confidence: 'medium'
      }
    ];
    fs.writeFileSync(path.join(__dirname, 'ai_suggestions.json'), JSON.stringify(fallback, null, 2));
    return fallback;
  }
}

// ===== CLI MODE (Optional: node bt/ai-agent.js --run) =====
if (require.main === module && process.argv.includes('--run')) {
  suggestStrategy(process.argv[2] || '').then(strats => {
    console.log('AI Suggestions:', JSON.stringify(strats, null, 2));
    process.exit(0);
  }).catch(err => {
    console.error('CLI failed:', err);
    process.exit(1);
  });
}

// ===== EXPORTS =====
module.exports = { suggestStrategy };