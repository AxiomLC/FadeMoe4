// db/cards-back.js
// Backend logic for alert cards: polling perp_metrics, evaluating combos, managing alert delays

const dbManager = require('../db/dbsetup');

// In-memory store for last alert timestamps per symbol+combo
const lastAlertTimestamps = new Map();

/**
 * Fetch latest perp_metrics rows for given symbol+exchange pairs
 * @param {Array<{symbol:string, exchange:string}>} pairs
 * @returns {Promise<Array>} rows
 */
async function fetchLatestMetrics(pairs) {
  if (!pairs || pairs.length === 0) return [];

  const values = [];
  const conditions = pairs.map(({ symbol, exchange }, i) => {
    values.push(symbol, exchange);
    return `(symbol = $${values.length - 1} AND exchange = $${values.length})`;
  }).join(' OR ');

  const query = `
    SELECT DISTINCT ON (symbol, exchange) *
    FROM perp_metrics
    WHERE ${conditions}
    ORDER BY symbol, exchange, ts DESC
  `;

  try {
    const result = await dbManager.pool.query(query, values);
    return result.rows;
  } catch (error) {
    console.error('Error fetching latest perp_metrics:', error.message);
    return [];
  }
}

/**
 * Parse a comboAlgo string into structured conditions
 * @param {string} comboStr
 * @returns {object} { tradeSymbols, direction, conditions: [{symbol, exchange, param, operator, value}] }
 */
function parseComboAlgo(comboStr) {
  // Format: SYMBOLS;DIRECTION;COND + COND + ...
  const parts = comboStr.split(';');
  if (parts.length < 3) throw new Error('Invalid comboAlgo format');

  const tradeSymbols = parts[0].toUpperCase().split(',').map(s => s.trim()).filter(Boolean);
  const direction = parts[1].trim();
  const condStr = parts.slice(2).join(';'); // In case conditions contain ';'

  const condParts = condStr.split('+').map(c => c.trim()).filter(Boolean);

  const conditions = condParts.map(cond => {
    // cond like SYMBOL_EXCHANGE_PARAMOPVALUE
    // e.g. MT_bin_rsi1_chg_1m>30
    const match = cond.match(/^([A-Z]+)_([a-z]+)_([a-z0-9_]+)([<>])(\d+(\.\d+)?)$/i);
    if (!match) throw new Error(`Invalid condition format: ${cond}`);
    const [, symbol, exchange, param, operator, value] = match;
    return {
      symbol: symbol.toUpperCase(),
      exchange: exchange.toLowerCase(),
      param,
      operator,
      value: parseFloat(value)
    };
  });

  return { tradeSymbols, direction, conditions };
}

/**
 * Evaluate if conditions are met for given perp_metrics rows
 * @param {Array} rows - latest perp_metrics rows
 * @param {object} combo - parsed combo { tradeSymbols, direction, conditions }
 * @param {number} alertDelay - minutes
 * @param {Map} lastAlerts - map for last alert timestamps
 * @returns {Array} alerts
 */
function evaluateAlerts(rows, combo, alertDelay, lastAlerts) {
  const alerts = [];
  const now = Date.now();

  // Organize rows by symbol+exchange for quick lookup
  const rowMap = new Map();
  for (const row of rows) {
    const key = `${row.symbol.toUpperCase()}_${row.exchange.toLowerCase()}`;
    rowMap.set(key, row);
  }

  // For each tradeSymbol, check conditions
  for (const tradeSymbol of combo.tradeSymbols) {
    if (tradeSymbol === 'MT') continue; // skip MT as tradeSymbol

    // For each condition, evaluate on corresponding symbol+exchange row
    let allTrue = true;
    let latestTs = 0;

    for (const cond of combo.conditions) {
      // If algoSymbol is 'ALL', replace with tradeSymbol for alerting but condition checked on algoSymbol
      const condSymbol = cond.symbol === 'ALL' ? tradeSymbol : cond.symbol;
      const key = `${condSymbol.toUpperCase()}_${cond.exchange.toLowerCase()}`;
      const row = rowMap.get(key);
      if (!row) {
        allTrue = false;
        break;
      }
      const val = row[cond.param];
      if (val === null || val === undefined) {
        allTrue = false;
        break;
      }
      if (cond.operator === '>') {
        if (!(val > cond.value)) {
          allTrue = false;
          break;
        }
      } else if (cond.operator === '<') {
        if (!(val < cond.value)) {
          allTrue = false;
          break;
        }
      }
      if (row.ts > latestTs) latestTs = row.ts;
    }

    if (allTrue) {
      // Check alert delay
      const alertKey = `${tradeSymbol}_${combo.conditions.map(c => `${c.symbol}_${c.exchange}_${c.param}${c.operator}${c.value}`).join('_')}`;
      const lastTs = lastAlerts.get(alertKey) || 0;
      if (now - lastTs >= alertDelay * 60000) {
        lastAlerts.set(alertKey, now);
        alerts.push({
          tradeSymbol,
          direction: combo.direction,
          comboStr: combo.tradeSymbols.join(',') + ';' + combo.direction + ';' + combo.conditions.map(c => `${c.symbol}_${c.exchange}_${c.param}${c.operator}${c.value}`).join(' + '),
          ts: new Date(latestTs).toISOString(),
          alertId: alertKey
        });
      }
    }
  }

  return alerts;
}

module.exports = {
  fetchLatestMetrics,
  parseComboAlgo,
  evaluateAlerts,
  lastAlertTimestamps
};
