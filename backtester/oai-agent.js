/**
 * ============================================================================
 * AI STRATEGY ADVISOR — ai-agent.js  (FadeAI Backtester)
 * ============================================================================
 * Role: Suggest candidate strategies (JSON format) for backtesting.
 * Uses:
 *   - Postgres/TimescaleDB metrics summary
 *   - Optional trending token APIs
 *   - OpenAI API (or compatible model)
 *
 * Usage:
 *   node backtester/ai-agent.js --run
 *
 * Exports:
 *   suggestStrategy() — for use by server.js endpoint (/suggest)
 *
 * ============================================================================
 */

import fs from "fs";
import dotenv from "dotenv";
import OpenAI from "openai";
import db from "../db/dbsetup.js";
import fetch from "node-fetch"; // for external trend APIs

dotenv.config();

/* ============================================================================
 * 1️⃣  ENVIRONMENT CONFIGURATION
 * ============================================================================
 * Add to your .env file:
 *
 *   OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxx
 *   MODEL_NAME=gpt-5           # or gpt-4o, gpt-4.1, etc.
 *   MAX_TOKENS=1500
 * ============================================================================
 */

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY || "YOUR_API_KEY_HERE",
});

const MODEL = process.env.MODEL_NAME || "gpt-5";
const MAX_TOKENS = parseInt(process.env.MAX_TOKENS || "1500");

/* ============================================================================
 * 2️⃣  HELPER — Fetch trending tokens from Coingecko
 * ============================================================================
 * (Optional — comment out if offline)
 */
async function getTrendingTokens() {
  try {
    const res = await fetch("https://api.coingecko.com/api/v3/search/trending");
    const data = await res.json();
    const coins = data.coins.map((c) => c.item.symbol.toUpperCase());
    return coins;
  } catch (err) {
    console.warn("⚠️  Trending fetch failed:", err.message);
    return [];
  }
}

/* ============================================================================
 * 3️⃣  HELPER — Get a sample of metrics from your DB
 * ============================================================================
 * Summarizes recent behavior for the AI’s context.
 */
async function getRecentMetricsSummary() {
  const query = `
    SELECT symbol,
           AVG(c_chg_1m) AS price_1m,
           AVG(v_chg_1m) AS vol_1m,
           AVG(oi_chg_1m) AS oi_1m,
           AVG(pfr_chg_1m) AS fund_1m,
           AVG(rsi1) AS rsi_avg
    FROM perp_metrics
    WHERE ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '1 hour')*1000
    GROUP BY symbol
    ORDER BY ABS(AVG(c_chg_1m)) DESC
    LIMIT 10;
  `;
  try {
    const { rows } = await db.pool.query(query);
    return rows;
  } catch (e) {
    console.error("DB summary failed:", e.message);
    return [];
  }
}

/* ============================================================================
 * 4️⃣  HELPER — Get dynamic column names from DB
 * ============================================================================
 * This makes the AI aware of your actual available parameters.
 */
async function getMetricColumns() {
  const sql = `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'perp_metrics'
      AND column_name LIKE '%_chg_%'
    ORDER BY column_name;
  `;
  const { rows } = await db.pool.query(sql);
  return rows.map((r) => r.column_name);
}

/* ============================================================================
 * 5️⃣  MAIN FUNCTION — Suggest Strategy
 * ============================================================================
 */
export async function suggestStrategy() {
  console.log("🤖 AI-Agent: Generating strategy suggestions...");

  const trending = await getTrendingTokens();
  const metrics = await getRecentMetricsSummary();
  const columns = await getMetricColumns();

  const trendStr = trending.length ? trending.join(", ") : "none";
  const metricSummary = metrics
    .map(
      (r) =>
        `${r.symbol}: price=${r.price_1m?.toFixed(2)}%, vol=${r.vol_1m?.toFixed(
          2
        )}%, rsi=${r.rsi_avg?.toFixed(1)}`
    )
    .join("\n");

  const prompt = `
You are FadeAI Strategy Advisor. You assist with generating short-term perpetual futures strategies for backtesting.

### Available metric columns:
${columns.join(", ")}

### Latest average metrics (1h sample):
${metricSummary}

### Trending tokens on the web:
${trendStr}

Generate 2–3 JSON strategy suggestions for quick backtesting.
Each must include:
- "name"
- "exchange"
- "symbols"
- "conditions": [{ "param", "operator", "value" }]
- "trade_scheme": { "direction", "tp1", "tp2", "sl" }

Keep thresholds realistic (e.g., 0.1–5.0% changes).
Use JSON array only, no prose.
`;

  try {
    const res = await openai.chat.completions.create({
      model: MODEL,
      messages: [
        { role: "system", content: "You output only valid JSON arrays of strategies." },
        { role: "user", content: prompt },
      ],
      temperature: 0.7,
      max_tokens: MAX_TOKENS,
    });

    const text = res.choices[0].message.content.trim();
    const strategies = JSON.parse(text);

    fs.writeFileSync(
      "./backtester/ai_suggestions.json",
      JSON.stringify(strategies, null, 2)
    );

    console.log(`✅ Generated ${strategies.length} candidate strategies.`);
    return strategies;
  } catch (err) {
    console.error("💥 AI generation failed:", err.message);
    return [];
  }
}

/* ============================================================================
 * 6️⃣  OPTIONAL EXPANSION ZONE (COMMENTED)
 * ============================================================================
 * Below sections are placeholders for future duties.
 * They should remain commented until explicitly authorized.
 * ============================================================================
 */

// // 🔹 (Future) Auto-run Backtest immediately after AI suggestion
// import { runBacktest } from './backtester.js';
// async function autoEvaluate() {
//   const suggestions = await suggestStrategy();
//   for (const strat of suggestions) {
//     fs.writeFileSync('./backtester/strategy.json', JSON.stringify(strat, null, 2));
//     await runBacktest();
//   }
// }

// // 🔹 (Future) Auto-approve & store best-performing algo to DB
// import { storeAlgoToDB } from './algo-storage.js';
// async function autoApproveBest() {
//   const results = JSON.parse(fs.readFileSync('./backtester/results.json', 'utf8'));
//   if (results.stats.winRate > 70) await storeAlgoToDB(results.strategy);
// }

// // 🔹 (Future) Live-trade mode (EXTREME CAUTION)
// // Connect to Orderly or Drift trading endpoints using same JSON structure
// // async function executeLiveTrade(strategy) {
// //   if (strategy.trade_scheme.direction === 'mBuy') { ... }
// // }

/* ============================================================================
 * 7️⃣  CLI EXECUTION
 * ============================================================================
 */
if (process.argv.includes("--run")) {
  suggestStrategy()
    .then((r) => console.log(JSON.stringify(r, null, 2)))
    .catch((e) => console.error(e));
}
