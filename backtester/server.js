/**
 * AI STRATEGY ADVISOR — CommonJS version
 */

const fs = require("fs");
const dotenv = require("dotenv");
const OpenAI = require("openai");
const db = require("../db/dbsetup.js");
const fetch = require("node-fetch");

dotenv.config();

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY || "YOUR_API_KEY_HERE",
});

const MODEL = "gpt-4.1-mini";
const MAX_TOKENS = parseInt(process.env.MAX_TOKENS || "1500");

async function getTrendingTokens() {
  try {
    const res = await fetch("https://api.coingecko.com/api/v3/search/trending");
    const data = await res.json();
    return data.coins.map(c => c.item.symbol.toUpperCase());
  } catch {
    return [];
  }
}

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
  const { rows } = await db.pool.query(query);
  return rows;
}

async function getMetricColumns() {
  const sql = `
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'perp_metrics'
      AND column_name LIKE '%_chg_%'
    ORDER BY column_name;
  `;
  const { rows } = await db.pool.query(sql);
  return rows.map(r => r.column_name);
}

async function suggestStrategy() {
  console.log("🤖 AI-Agent: Generating strategy suggestions...");

  const trending = await getTrendingTokens();
  const metrics = await getRecentMetricsSummary();
  const columns = await getMetricColumns();

  const trendStr = trending.length ? trending.join(", ") : "none";
  const metricSummary = metrics
  .map(
    r =>
      `${r.symbol}: price=${parseFloat(r.price_1m || 0).toFixed(2)}%, vol=${parseFloat(r.vol_1m || 0).toFixed(
        2
      )}%, rsi=${parseFloat(r.rsi_avg || 0).toFixed(1)}`
  )
  .join("\n");

  const prompt = `
You are FadeAI Strategy Advisor. You assist with generating short-term perpetual futures strategies for backtesting.

### Available metric columns:
${columns.join(", ")}

### Latest average metrics (1h sample):
${metricSummary}

### Trending tokens:
${trendStr}

Generate 2–3 JSON strategy suggestions.
`;

  try {
    const res = await openai.chat.completions.create({
      model: MODEL,
      messages: [
        { role: "system", content: "Output only valid JSON arrays." },
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

    console.log(`✅ Generated ${strategies.length} strategies.`);
    return strategies;
  } catch (err) {
    console.error("AI generation failed:", err.message);
    return [];
  }
}

module.exports = { suggestStrategy };

if (process.argv.includes("--run")) {
  suggestStrategy().then(r => console.log(r)).catch(console.error);
}
