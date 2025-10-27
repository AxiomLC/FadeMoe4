// backtester/algoql-parser.js
// ============================================================================
// ALGOQL PARSER - Translates AlgoQL DSL to SQL
// ============================================================================
// Parses: "BTC,ETH;Long;bin_pfr_chg_5m>0.31 AND byb_oi_chg_5m>0.25"
// Into structured SQL WHERE clauses with multi-exchange support
// ============================================================================

/**
 * Main parser function
 * @param {string} algoql - Full AlgoQL string
 * @returns {object} Parsed components
 */
function parseAlgoQL(algoql) {
  if (!algoql || typeof algoql !== 'string') {
    throw new Error('Invalid AlgoQL: must be a non-empty string');
  }

  const parts = algoql.split(';');
  if (parts.length !== 3) {
    throw new Error('Invalid AlgoQL format. Expected: SYMBOLS;DIRECTION;CONDITIONS');
  }

  const [symbolsStr, direction, conditionsStr] = parts.map(p => p.trim());

  // Parse symbols
  const symbols = parseSymbols(symbolsStr);
  
  // Validate direction
  if (!['Long', 'Short'].includes(direction)) {
    throw new Error(`Invalid direction: ${direction}. Must be 'Long' or 'Short'`);
  }

  // Parse conditions
  const { conditions, mtSymbols } = parseConditions(conditionsStr);

  // Build SQL
  const sqlWhere = buildSQLWhere(conditions, mtSymbols);

  return {
    symbols,
    direction,
    conditions,
    mtSymbols,
    sqlWhere,
    originalAlgoql: algoql
  };
}

/**
 * Parse symbols section
 * @param {string} symbolsStr - "BTC,ETH,SOL" or "ALL"
 * @returns {Array|string} Array of symbols or "ALL"
 */
function parseSymbols(symbolsStr) {
  if (symbolsStr.toUpperCase() === 'ALL') {
    return 'ALL';
  }
  
  const symbols = symbolsStr.split(',').map(s => s.trim()).filter(s => s.length > 0);
  if (symbols.length === 0) {
    throw new Error('No symbols specified');
  }
  
  return symbols;
}

/**
 * Parse conditions string into structured array
 * @param {string} conditionsStr - "bin_pfr_chg_5m>0.31 AND byb_oi_chg_5m>0.25"
 * @returns {object} { conditions: Array, mtSymbols: Array }
 */
function parseConditions(conditionsStr) {
  const conditions = [];
  const mtSymbols = new Set();

  // Regex to match conditions
  // Matches: bin_pfr_chg_5m>0.31 OR symbolBTC_v_chg_1m>2.0
  const conditionRegex = /(bin|byb|okx|symbol[A-Z0-9]+)_([a-z0-9]+)_chg_(1m|5m|10m)\s*([><]=?|=)\s*(-?\d+\.?\d*)/gi;
  
  // Track logic operators between conditions
  const logicOps = extractLogicOperators(conditionsStr);
  
  let match;
  let condIndex = 0;
  
  while ((match = conditionRegex.exec(conditionsStr)) !== null) {
    const [fullMatch, exchangeOrSymbol, param, timeframe, operator, value] = match;
    
    // Check if it's a symbol reference (e.g., symbolBTC)
    const isSymbolRef = exchangeOrSymbol.startsWith('symbol');
    
    if (isSymbolRef) {
      const refSymbol = exchangeOrSymbol.replace('symbol', '');
      mtSymbols.add(refSymbol);
      
      conditions.push({
        type: 'symbol_reference',
        symbol: refSymbol,
        param,
        timeframe,
        operator,
        value: parseFloat(value),
        logic: logicOps[condIndex] || 'AND'
      });
    } else {
      conditions.push({
        type: 'exchange_param',
        exchange: exchangeOrSymbol.toLowerCase(),
        param,
        timeframe,
        operator,
        value: parseFloat(value),
        logic: logicOps[condIndex] || 'AND'
      });
    }
    
    condIndex++;
  }

  if (conditions.length === 0) {
    throw new Error('No valid conditions found');
  }

  return { conditions, mtSymbols: Array.from(mtSymbols) };
}

/**
 * Extract AND/OR operators between conditions
 * @param {string} conditionsStr
 * @returns {Array} Array of logic operators
 */
function extractLogicOperators(conditionsStr) {
  const operators = [];
  const tokens = conditionsStr.split(/\s+/);
  
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i].toUpperCase();
    if (token === 'AND' || token === 'OR') {
      operators.push(token);
    }
  }
  
  return operators;
}

/**
 * Build SQL WHERE clause from parsed conditions
 * @param {Array} conditions - Parsed conditions
 * @param {Array} mtSymbols - Symbol references for JOINs
 * @returns {string} SQL WHERE clause
 */
function buildSQLWhere(conditions, mtSymbols) {
  // Group conditions by exchange
  const exchangeGroups = {};
  const symbolRefConditions = [];
  
  conditions.forEach(cond => {
    if (cond.type === 'exchange_param') {
      if (!exchangeGroups[cond.exchange]) {
        exchangeGroups[cond.exchange] = [];
      }
      exchangeGroups[cond.exchange].push(cond);
    } else if (cond.type === 'symbol_reference') {
      symbolRefConditions.push(cond);
    }
  });

  const whereParts = [];

  // Build exchange-based conditions (each exchange gets OR group)
  const exchangeSQL = [];
  for (const [exchange, conds] of Object.entries(exchangeGroups)) {
    const conditions = conds.map(c => 
      `pm.${c.param}_chg_${c.timeframe} ${c.operator} ${c.value}`
    ).join(' AND ');
    
    exchangeSQL.push(`(pm.exchange = '${exchange}' AND ${conditions})`);
  }

  if (exchangeSQL.length > 0) {
    whereParts.push(`(${exchangeSQL.join(' OR ')})`);
  }

  // Add symbol reference conditions
  symbolRefConditions.forEach((cond, idx) => {
    const alias = `mt${idx}`;
    whereParts.push(
      `${alias}.${cond.param}_chg_${cond.timeframe} ${cond.operator} ${cond.value}`
    );
  });

  return whereParts.join(' AND ');
}

/**
 * Build complete SQL query from parsed AlgoQL
 * @param {object} parsed - From parseAlgoQL()
 * @param {number} limitRows - Max rows to return
 * @returns {string} Complete SQL SELECT statement
 */
function buildFullQuery(parsed, limitRows = 10000) {
  const { symbols, sqlWhere, mtSymbols } = parsed;

  // Symbol filter
  let symbolFilter = '';
  if (symbols !== 'ALL') {
    const symbolList = symbols.map(s => `'${s}'`).join(',');
    symbolFilter = `AND pm.symbol IN (${symbolList})`;
  }

  // Build JOINs for symbol references
  let joins = '';
  if (mtSymbols.length > 0) {
    mtSymbols.forEach((sym, idx) => {
      joins += `
      LEFT JOIN perp_metrics mt${idx}
        ON pm.ts = mt${idx}.ts 
        AND mt${idx}.symbol = '${sym}'
        AND mt${idx}.exchange = pm.exchange`;
    });
  }

  const query = `
    SELECT 
      pm.ts, pm.symbol, pm.exchange, 
      pm.o, pm.h, pm.l, pm.c, pm.v,
      pm.oi, pm.pfr, pm.lsr, pm.rsi1, pm.rsi60,
      pm.c_chg_1m, pm.c_chg_5m, pm.c_chg_10m,
      pm.v_chg_1m, pm.oi_chg_1m, pm.pfr_chg_1m
    FROM perp_metrics pm
    ${joins}
    WHERE ${sqlWhere}
      ${symbolFilter}
      AND pm.ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '10 days')*1000
    ORDER BY pm.ts ASC
    LIMIT ${limitRows};
  `;

  return query.trim();
}

/**
 * Validate AlgoQL syntax without executing
 * @param {string} algoql
 * @returns {object} { valid: boolean, error: string|null }
 */
function validateAlgoQL(algoql) {
  try {
    parseAlgoQL(algoql);
    return { valid: true, error: null };
  } catch (err) {
    return { valid: false, error: err.message };
  }
}

/**
 * Pretty print parsed AlgoQL for debugging
 * @param {object} parsed
 * @returns {string} Human-readable format
 */
function prettyPrint(parsed) {
  const { symbols, direction, conditions, mtSymbols } = parsed;
  
  let output = `Symbols: ${Array.isArray(symbols) ? symbols.join(', ') : symbols}\n`;
  output += `Direction: ${direction}\n`;
  output += `Conditions:\n`;
  
  conditions.forEach((cond, idx) => {
    if (cond.type === 'exchange_param') {
      output += `  ${idx + 1}. ${cond.exchange}_${cond.param}_chg_${cond.timeframe} ${cond.operator} ${cond.value}`;
    } else {
      output += `  ${idx + 1}. symbol${cond.symbol}_${cond.param}_chg_${cond.timeframe} ${cond.operator} ${cond.value}`;
    }
    if (idx < conditions.length - 1) {
      output += ` ${cond.logic}\n`;
    }
  });
  
  if (mtSymbols.length > 0) {
    output += `\nSymbol References: ${mtSymbols.join(', ')}`;
  }
  
  return output;
}

// ============================================================================
// EXPORTS
// ============================================================================
module.exports = {
  parseAlgoQL,
  buildFullQuery,
  validateAlgoQL,
  prettyPrint
};

// ============================================================================
// CLI TESTING
// ============================================================================
if (require.main === module) {
  const testCases = [
    "BTC,ETH;Long;bin_pfr_chg_5m>0.31 AND byb_oi_chg_5m>0.25",
    "ALL;Short;okx_lsr_chg_10m<-0.5 OR bin_v_chg_5m>2.1",
    "BTC;Long;bin_pfr_chg_5m>0.5 AND symbolMT_v_chg_1m>1.5",
    "DOGE,SHIB;Long;bin_pfr_chg_5m>0.5 AND byb_oi_chg_5m<-0.3 AND symbolBTC_c_chg_1m>1.5"
  ];

  console.log('🧪 AlgoQL Parser Test Suite\n');
  console.log('='.repeat(70));

  testCases.forEach((algoql, idx) => {
    console.log(`\nTest ${idx + 1}: ${algoql}`);
    console.log('-'.repeat(70));
    
    try {
      const parsed = parseAlgoQL(algoql);
      console.log(prettyPrint(parsed));
      console.log('\n📝 Generated SQL:');
      console.log(buildFullQuery(parsed, 100));
    } catch (err) {
      console.error('❌ Error:', err.message);
    }
    
    console.log('='.repeat(70));
  });
}