# FadeAI Backtester - Technical Implementation Guide

## Overview
The FadeAI Backtester is a sophisticated query-driven strategy testing system for cryptocurrency perpetual futures. It uses a custom Domain-Specific Language (AlgoQL) to define trading strategies and tests them against 10 days of historical data across 40+ symbols and 3 exchanges.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        USER INTERFACE                        │
│  (index.html - Horizontal param selectors + Algo builder)   │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      EXPRESS SERVER                          │
│  (app.js - REST API endpoints for backtest/AI/storage)      │
└──────────┬─────────────┬─────────────┬─────────────┬────────┘
           │             │             │             │
           ▼             ▼             ▼             ▼
    ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
    │ AlgoQL   │  │ Backtest │  │Statistical│  │ AI Agent │
    │ Parser   │  │ Engine   │  │ Analyzer │  │ (OpenAI) │
    └──────────┘  └──────────┘  └──────────┘  └──────────┘
           │             │             │             │
           └─────────────┴─────────────┴─────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  PostgreSQL/TimescaleDB │
              │  (perp_metrics table)   │
              └────────────────────────┘
```

---

## File Structure

```
backtester/
│
├── app.js                      # Express server with REST API
├── index.html                  # Frontend UI (param builders + Algo box)
│
├── backtester.js               # Core backtesting engine
├── algoql-parser.js            # AlgoQL → SQL translator
├── statistical-analyzer.js     # AI discovery via correlation analysis
├── ai-agent.js                 # OpenAI integration for suggestions
├── server.js                   # Legacy AI helper (can merge into ai-agent.js)
│
├── results/                    # Auto-generated backtest results (pruned >100)
├── ai-suggestions/             # AI-generated algos (persistent)
│
├── strategy.json               # Current active strategy (temporary)
└── README_SETUP.md             # This file
```

---

## AlgoQL Language Specification

### Syntax
```
<SYMBOLS>;<DIRECTION>;<CONDITIONS>
```

### Examples
```
BTC,ETH;Long;bin_pfr_chg_5m>0.31 AND byb_oi_chg_5m>0.25
ALL;Short;okx_lsr_chg_10m<-0.5 OR bin_v_chg_5m>2.1
BTC;Long;bin_pfr_chg_5m>0.5 AND symbolMT_v_chg_1m>1.5
```

### Grammar
```
SYMBOLS     ::= "ALL" | symbol ("," symbol)*
DIRECTION   ::= "Long" | "Short"
CONDITIONS  ::= expression (logic_op expression)*
expression  ::= condition | "(" CONDITIONS ")"
condition   ::= [exchange "_"] param "_chg_" timeframe operator value
              | "symbol" SYMBOL "_" param "_chg_" timeframe operator value

exchange    ::= "bin" | "byb" | "okx"
param       ::= "o" | "h" | "l" | "c" | "v" | "oi" | "pfr" | "lsr" 
              | "rsi1" | "rsi60" | "tbv" | "tsv" | "lqprice" | "lqqty"
timeframe   ::= "1m" | "5m" | "10m"
operator    ::= ">" | "<" | ">=" | "<=" | "="
value       ::= NUMERIC(6,4)  # e.g., 0.31, 2.145, -0.5
logic_op    ::= "AND" | "OR"
```

---

## Module Specifications

### 1. `algoql-parser.js`

**Purpose:** Translate AlgoQL strings into SQL WHERE clauses

**Key Functions:**

```javascript
/**
 * Parse AlgoQL string and return structured query components
 * @param {string} algoql - e.g., "BTC,ETH;Long;bin_pfr_chg_5m>0.31 AND byb_oi_chg_5m>0.25"
 * @returns {object} { symbols, direction, conditions, mtSymbols, sqlWhere }
 */
function parseAlgoQL(algoql)

/**
 * Convert conditions array to SQL WHERE clause
 * @param {Array} conditions - Parsed condition objects
 * @returns {string} SQL WHERE clause with proper grouping
 */
function conditionsToSQL(conditions)

/**
 * Handle multi-exchange logic (each exchange in separate OR group)
 * @param {Array} conditions - Conditions grouped by exchange
 * @returns {string} SQL with (exchange='bin' AND ...) OR (exchange='byb' AND ...)
 */
function buildMultiExchangeSQL(conditions)
```

**Algorithm:**
1. Split AlgoQL on `;` to extract symbols, direction, conditions
2. Parse condition string using regex: `/(bin|byb|okx|symbol[A-Z]+)_([a-z0-9]+)_chg_(1m|5m|10m)([><]=?|=)(\d+\.\d+)/g`
3. Group conditions by exchange
4. Build SQL with proper parentheses for AND/OR precedence
5. Handle `symbolMT` references via LEFT JOIN

**Edge Cases:**
- Missing exchange prefix defaults to 'bin'
- `symbolMT_chg_5m>1.0` requires JOIN to perp_metrics WHERE symbol='MT'
- NOT logic: `NOT (condition)` translates to SQL `NOT (...)`

---

### 2. `backtester.js` (Enhanced)

**Purpose:** Execute backtest simulations on historical data

**Key Functions:**

```javascript
/**
 * Run full backtest from strategy.json
 * @returns {object} { stats, trades, bestScheme }
 */
async function runBacktest()

/**
 * Build SQL query from parsed AlgoQL
 * @param {object} parsedAlgo - From algoql-parser
 * @returns {string} Complete SQL SELECT statement
 */
function buildQuery(parsedAlgo)

/**
 * Find optimal TP/SL scheme via grid search
 * @param {Array} data - Matched data points
 * @param {string} direction - 'Long' or 'Short'
 * @returns {object} { tp1, tp2, sl, pf }
 */
function findBestScheme(data, direction)

/**
 * Simulate trades with given scheme
 * @param {Array} data - Price data
 * @param {object} scheme - { tp1, tp2, sl }
 * @param {string} direction - 'Long' or 'Short'
 * @returns {Array} Trade outcomes
 */
function simulateTrades(data, scheme, direction)
```

**Optimization Grid:**
- TP1: [0.3, 0.45, 0.6, 0.75, 1.0]%
- TP2: [0.6, 0.75, 1.0, 1.25, 1.5]%
- SL: [0.05, 0.085, 0.12, 0.15, 0.2]%
- Total combinations: 5×5×5 = 125 tests
- Scoring: `PF = (tp1*1 + tp2*2 - loss*1.5) / total`

**Query Pattern:**
```sql
SELECT pm.ts, pm.symbol, pm.exchange, pm.c, pm.o, pm.h, pm.l
FROM perp_metrics pm
LEFT JOIN perp_metrics mt ON pm.ts = mt.ts AND mt.symbol = 'MT'
WHERE pm.symbol IN ('BTC','ETH','SOL')
  AND (
    (pm.exchange = 'bin' AND pm.pfr_chg_5m > 0.31)
    OR (pm.exchange = 'byb' AND pm.oi_chg_5m > 0.25)
  )
  AND mt.v_chg_1m > 1.5
ORDER BY pm.ts ASC
LIMIT 10000;
```

---

### 3. `statistical-analyzer.js`

**Purpose:** Discover profitable param combinations via correlation analysis

**Key Functions:**

```javascript
/**
 * Run statistical analysis to find predictive params
 * @param {object} options - { minCorrelation: 0.3, topN: 20 }
 * @returns {Array} Top param combinations sorted by predictive power
 */
async function discoverProfitableParams(options)

/**
 * Calculate correlation between param and future price movement
 * @param {string} param - e.g., 'pfr_chg_5m'
 * @param {string} exchange - 'bin', 'byb', or 'okx'
 * @returns {number} Correlation coefficient (-1 to 1)
 */
async function calculateParamCorrelation(param, exchange)

/**
 * Test candidate algo and return profit factor
 * @param {object} candidate - { param, threshold, exchange }
 * @returns {object} { pf, winRate, totalTrades }
 */
async function quickTestAlgo(candidate)
```

**SQL Query for Correlation:**
```sql
WITH param_signals AS (
  SELECT 
    ts, symbol, exchange,
    pfr_chg_5m,
    LEAD(c_chg_1m, 3) OVER (PARTITION BY symbol, exchange ORDER BY ts) as future_move
  FROM perp_metrics
  WHERE ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '10 days')*1000
    AND pfr_chg_5m IS NOT NULL
)
SELECT 
  exchange,
  CORR(pfr_chg_5m, future_move) as correlation,
  STDDEV(pfr_chg_5m) as param_volatility,
  COUNT(*) as sample_size
FROM param_signals
WHERE future_move IS NOT NULL
GROUP BY exchange
HAVING COUNT(*) > 500
ORDER BY ABS(correlation) DESC;
```

**Discovery Algorithm:**
1. Query all params for correlation with 3-minute future price movement
2. Filter: |correlation| > 0.25, sample_size > 500
3. For top 20 params, test with threshold = mean + 0.5*stddev
4. Run quick backtest (last 7 days only for speed)
5. Return top 3 algos by profit factor

**Output Format:**
```json
[
  {
    "algoql": "ALL;Long;bin_pfr_chg_5m>0.42 AND byb_oi_chg_5m>0.31",
    "correlation": 0.38,
    "pf": 1.34,
    "winRate": "56.2%",
    "totalTrades": 847,
    "confidence": "high"
  }
]
```

---

### 4. `ai-agent.js`

**Purpose:** OpenAI integration for strategy suggestions and chat

**Key Functions:**

```javascript
/**
 * Generate strategy suggestions from AI
 * @param {string} userPrompt - Optional custom prompt
 * @returns {Array} Array of AlgoQL strategies
 */
async function suggestStrategies(userPrompt = null)

/**
 * Interactive AI chat for algo ideas
 * @param {string} message - User's question/request
 * @param {Array} chatHistory - Previous conversation
 * @returns {object} { response, suggestedAlgoql }
 */
async function chatWithAI(message, chatHistory)

/**
 * AI analysis of backtest results with improvement suggestions
 * @param {object} backtestResults - { stats, trades, bestScheme }
 * @param {string} originalAlgoql - The tested algo
 * @returns {object} { analysis, suggestions }
 */
async function analyzeAndImprove(backtestResults, originalAlgoql)
```

**System Prompt:**
```
You are FadeAI, a crypto perpetual futures strategy advisor.

Available data:
- 10 days of 1-minute data
- 40+ symbols (BTC, ETH, SOL, meme coins, etc.)
- 3 exchanges (bin=Binance, byb=Bybit, okx=OKX)
- Metrics: o,h,l,c,v,oi,pfr,lsr,rsi1,rsi60,tbv,tsv,lqprice,lqqty
- Timeframes: chg_1m, chg_5m, chg_10m

Your role:
1. Suggest profitable AlgoQL strategies
2. Explain market conditions that trigger each strategy
3. Warn about risks (overfitting, low sample size, etc.)
4. Improve existing algos based on backtest results

AlgoQL format: SYMBOLS;DIRECTION;CONDITIONS
Example: BTC,ETH;Long;bin_pfr_chg_5m>0.31 AND byb_oi_chg_5m>0.25
```

**Chat Flow:**
```
User: "Find divergence between Binance funding and Bybit OI"
AI: "I'll look for cases where Binance funding spikes while Bybit OI drops..."
   → Generates AlgoQL
   → Runs backtest
   → Returns: "Found 127 matches, 47% win rate. Try tightening thresholds."
```

---

### 5. `app.js` (REST API)

**Endpoints:**

```javascript
// Serve UI
GET /                          // Returns index.html

// Symbol/column metadata
GET /symbols                   // Returns list of all symbols
GET /columns                   // Returns all param names with _chg suffixes

// Strategy management
POST /strategy                 // Save current strategy.json
GET /saved-algos               // List all saved algos from DB
POST /save-algo                // Save algo to saved_algos table
DELETE /algo/:id               // Delete saved algo

// Backtesting
POST /backtest                 // Run backtest on current strategy.json
POST /backtest/quick-tune      // Run Quick Tune (±10% variations)

// AI features
POST /ai-discover              // Statistical discovery (Approach B)
POST /ai-chat                  // Interactive AI chat
POST /ai-improve               // Analyze results + suggest improvements

// File management
GET /results                   // List recent result files
GET /results/:filename         // Load specific result
GET /ai-suggestions            // List AI-generated algos
```

**File Pruning Logic:**
```javascript
// Auto-prune results/ folder to max 100 files
async function pruneResultsFolder() {
  const files = fs.readdirSync('./backtester/results')
    .map(f => ({ name: f, time: fs.statSync(`./backtester/results/${f}`).mtime }))
    .sort((a, b) => b.time - a.time);
  
  if (files.length > 100) {
    files.slice(100).forEach(f => fs.unlinkSync(`./backtester/results/${f.name}`));
  }
}
```

---

## UI Interaction Flow

### Step 1: Configure Params
```
┌─────────────────────────────────────────────────────────────┐
│ TOKEN Selection: [BTC ▼] [ETH ▼] [ALL ☐]  Direction: ●Long ○Short│
├─────────────────────────────────────────────────────────────┤
│ PARAMS: [o▼] [h▼] [l▼] [c▼] [v▼] [oi▼] [pfr▼] [lsr▼] ...  │
│                                                              │
│ Exchange: [bin▼] Param: [pfr▼] Time: [chg_5m▼] Op: [>▼]    │
│ Value: [0.31] Logic: [AND▼] [✓] [Submit →]                  │
└─────────────────────────────────────────────────────────────┘
```

### Step 2: Build Algo
```
┌─────────────────────────────────────────────────────────────┐
│ ALGO BOX:                                                    │
│ BTC,ETH;Long;bin_pfr_chg_5m>0.31 [x] AND byb_oi_chg_5m>0.25[x]│
│                                                              │
│ [🚀 Backtest] [⚡ Quick Tune] [delete]                       │
└─────────────────────────────────────────────────────────────┘
```

### Step 3: View Results
```
┌─────────────────────────────────────────────────────────────┐
│ RESULTS:                                                     │
│ Total: 3094 | Win Rate: 34.13% | PF: 0.54                   │
│ Best Scheme: TP1=0.45% TP2=0.69% SL=0.085%                  │
│                                                              │
│ [Adjust] [Re-Run] [💾 Save to DB]                           │
└─────────────────────────────────────────────────────────────┘
```

### Step 4: AI Assistance
```
┌─────────────────────────────────────────────────────────────┐
│ AI CHAT:                                                     │
│ 💬: "Look for divergence between bin and byb funding"       │
│ 🤖: "Found pattern: bin_pfr_chg_5m>0.5 when byb_pfr<0.2..." │
│     [Setup ALGO]                                             │
│                                                              │
│ [🔍 AI Discover] [Send]                                      │
└─────────────────────────────────────────────────────────────┘
```

---

## Database Schema

### `perp_metrics` (Existing - with new indexes)
```sql
CREATE TABLE perp_metrics (
  ts BIGINT NOT NULL,
  symbol TEXT NOT NULL,
  exchange TEXT NOT NULL,
  -- Raw values
  o, h, l, c, v, oi, pfr, lsr, rsi1, rsi60, tbv, tsv, lqside, lqprice, lqqty,
  -- % changes (1m/5m/10m)
  c_chg_1m, v_chg_1m, oi_chg_1m, pfr_chg_1m, ... (36 change columns),
  PRIMARY KEY (ts, symbol, exchange)
);

-- NEW INDEXES
CREATE INDEX idx_metrics_symbol_ts ON perp_metrics(symbol, ts DESC);
CREATE INDEX idx_metrics_exchange_ts ON perp_metrics(exchange, ts DESC);
CREATE INDEX idx_metrics_composite ON perp_metrics(exchange, symbol, ts DESC);
CREATE INDEX idx_pfr_chg_5m ON perp_metrics(pfr_chg_5m) WHERE pfr_chg_5m IS NOT NULL;
CREATE INDEX idx_oi_chg_5m ON perp_metrics(oi_chg_5m) WHERE oi_chg_5m IS NOT NULL;
CREATE INDEX idx_v_chg_10m ON perp_metrics(v_chg_10m) WHERE v_chg_10m IS NOT NULL;
CREATE INDEX idx_c_chg_1m ON perp_metrics(c_chg_1m) WHERE c_chg_1m IS NOT NULL;
```

### `saved_algos` (New)
```sql
CREATE TABLE saved_algos (
  algo_id SERIAL PRIMARY KEY,
  algo_name TEXT NOT NULL,
  algoql TEXT NOT NULL,
  symbols TEXT[],
  direction TEXT,
  conditions JSONB,
  backtest_results JSONB,  -- { stats, trades, bestScheme }
  best_scheme JSONB,        -- { tp1, tp2, sl, pf }
  created_at TIMESTAMP DEFAULT NOW(),
  notes TEXT
);
```

---

## Performance Benchmarks

| Operation | Without Indexes | With Indexes | Target |
|-----------|----------------|--------------|--------|
| Simple backtest (40 symbols, 10 days, 4 params) | 15-30s | 3-8s | < 10s |
| Quick Tune (12 variations) | 4 min | 60s | < 90s |
| AI Discovery (correlation analysis) | N/A | 90s | < 2 min |
| AI Chat response | N/A | 5-10s | < 15s |

---

## Implementation Checklist

### Phase 1: Foundation
- [ ] Update dbsetup.js with indexes
- [ ] Create algoql-parser.js
- [ ] Update backtester.js to use parser
- [ ] Test basic query: `BTC;Long;bin_pfr_chg_5m>0.31`

### Phase 2: UI
- [ ] Rebuild index.html with horizontal params
- [ ] Implement submit/delete logic
- [ ] Test Algo box population
- [ ] Wire up backtest button

### Phase 3: AI Discovery
- [ ] Create statistical-analyzer.js
- [ ] Implement correlation SQL queries
- [ ] Add /ai-discover endpoint
- [ ] Test discovery on real data

### Phase 4: AI Chat
- [ ] Update ai-agent.js for chat
- [ ] Add /ai-chat endpoint
- [ ] Implement chat history
- [ ] Test prompt → AlgoQL generation

### Phase 5: File Management
- [ ] Create results/ and ai-suggestions/ folders
- [ ] Implement auto-pruning logic
- [ ] Add file listing endpoints
- [ ] Test save/load workflow

---

## Testing Strategy

1. **Unit Tests** (algoql-parser.js):
   - Parse: `BTC;Long;bin_pfr_chg_5m>0.31`
   - Parse: `ALL;Short;(bin_pfr>0.5 OR byb_pfr>0.5) AND symbolMT_v>1.0`
   - Edge case: Missing exchange prefix

2. **Integration Tests** (backtester.js):
   - Query returns expected row count
   - Trade simulation calculates correct outcomes
   - Best scheme optimization finds valid TP/SL

3. **Performance Tests**:
   - 40 symbols, 10 days, 4 params < 10 seconds
   - Quick Tune < 90 seconds
   - AI Discovery < 2 minutes

---

## Future Enhancements

1. **Parallel Backtesting**: Test multiple algos concurrently
2. **Walk-Forward Analysis**: Test algo on rolling time windows
3. **Monte Carlo Simulation**: Randomize entry times to test robustness
4. **Live Trading Integration**: Auto-execute winning algos (EXTREME CAUTION)
5. **Multi-Asset Class**: Extend to spot, options, futures basis

---

## Troubleshooting

### Query too slow (>30s)
- Check if indexes exist: `\d perp_metrics` in psql
- Verify ANALYZE has run: `ANALYZE perp_metrics;`
- Reduce time window or symbol count

### AI generates invalid AlgoQL
- Check system prompt includes grammar rules
- Validate with algoql-parser before backtesting
- Log failed parses for retraining

### Results folder fills up
- Pruning should auto-run after each backtest
- Manual cleanup: `rm backtester/results/run_2025*.json`

---

## Contact & Support

For questions about implementation, contact the FadeAI development team.

**Last Updated:** October 18, 2025