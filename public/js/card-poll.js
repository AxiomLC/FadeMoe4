// public/js/card-poll.js
// Alert Cards polling, parsing, evaluation, and rendering

const alertPoll = (function() {
  let pollingInterval = null;
  let alertHistory = [];
  let alertLastTriggers = {};
  let pollingRunning = false;

  // Helper to get localStorage data
  function getStored(key, def) {
    try {
      const val = localStorage.getItem(key);
      return val ? JSON.parse(val) : def;
    } catch {
      return def;
    }
  }

  // Helper to set localStorage data
  function setStored(key, val) {
    try {
      localStorage.setItem(key, JSON.stringify(val));
    } catch {}
  }

  // Parse comboAlgo string - simplified validation and parsing
  function parseComboAlgo(str) {
    if (!str) return null;
    const parts = str.split(';').map(p => p.trim());
    if (parts.length !== 3) return null;
    const tradeSymbols = parts[0].split(',').map(s => s.trim().toUpperCase());
    const direction = parts[1];
    const conditionsStr = parts[2];

    const conditionParts = conditionsStr.split('+').map(p => p.trim());
    const conditions = [];
    for (const condStr of conditionParts) {
      const match = condStr.match(/^([A-Z]+)_([a-z]+)_([a-z0-9_]+)([<>])([\d.]+)$/i);
      if (!match) return null;
      const [, algoSymbol, exchange, param, op, valStr] = match;
      const val = parseFloat(valStr);
      if (isNaN(val)) return null;
      conditions.push({
        algoSymbol: algoSymbol.toUpperCase(),
        exchange,
        param,
        op,
        val
      });
    }

    return { tradeSymbols, direction, conditions };
  }

  // Validate all combo inputs
  function isValidInputs() {
    const inputs = document.querySelectorAll('.combo-input');
    return Array.from(inputs).every(input => parseComboAlgo(input.value));
  }

  let cachedTradeSymbols = null;

  async function getAllTradeSymbols() {
    if (cachedTradeSymbols) return cachedTradeSymbols;
    try {
      const res = await fetch('/api/symbols');
      if (!res.ok) throw new Error('Failed to fetch symbols');
      const symbols = await res.json();
      cachedTradeSymbols = symbols.filter(s => s.toUpperCase() !== 'MT');
      return cachedTradeSymbols;
    } catch (e) {
      console.error('Error fetching all trade symbols:', e);
      return [];
    }
  }

  function startPolling() {
    if (pollingInterval) return;
    console.log('AlertPoll: Starting polling...');
    pollingRunning = true;
    setStored('alertPollingRunning', true);
    pollOnce();
    pollingInterval = setInterval(pollOnce, 60000);
  }

  function stopPolling() {
    if (pollingInterval) {
      clearInterval(pollingInterval);
      pollingInterval = null;
      console.log('AlertPoll: Stopped polling.');
      pollingRunning = false;
      setStored('alertPollingRunning', false);
    }
  }

  function isRunning() {
    return !!pollingInterval;
  }

  async function pollOnce() {
    try {
      const comboAlgos = getActiveComboAlgos();
      if (comboAlgos.length === 0) return;

      const allTradeSymbols = await getAllTradeSymbols();

      const symbols = new Set();
      const exchanges = new Set();

      comboAlgos.forEach(algoStr => {
        const parsed = parseComboAlgo(algoStr);
        if (!parsed) return;

        let tradeSyms = parsed.tradeSymbols.map(s => s.toUpperCase());
        if (tradeSyms.includes('ALL')) {
          tradeSyms = allTradeSymbols;
        }

        tradeSyms.forEach(sym => {
          if (sym !== 'MT') symbols.add(sym);
        });

        exchanges.add('bin');
      });

      if (symbols.size === 0 || exchanges.size === 0) {
        console.warn('Skipping poll: symbols or exchanges empty');
        return;
      }

      const params = new URLSearchParams();
      params.append('symbols', Array.from(symbols).join(','));
      params.append('exchanges', Array.from(exchanges).join(','));

      const res = await fetch(`/api/latest-metrics?${params.toString()}`);
      if (!res.ok) throw new Error('API fetch failed');
      const data = await res.json();

      evaluateTriggers(comboAlgos, data.data);
      renderCards();
    } catch (e) {
      console.error('Polling error:', e);
    }
  }

  function getActiveComboAlgos() {
    const inputs = document.querySelectorAll('.combo-input');
    return Array.from(inputs).map(input => input.value.trim()).filter(v => v);
  }

  function evaluateTriggers(comboAlgos, metricsData) {
    const maxAlerts = parseInt(localStorage.getItem('alertMaxAlerts')) || 20;
    const delayMin = parseInt(localStorage.getItem('alertDelay')) || 5;
    const delayMs = delayMin * 60 * 1000;
    const currentTs = Date.now();

    comboAlgos.forEach((algoStr, comboIndex) => {
      const parsed = parseComboAlgo(algoStr);
      if (!parsed) return;

      let tradeSymbols = parsed.tradeSymbols;
      if (tradeSymbols.includes('ALL')) {
        tradeSymbols = cachedTradeSymbols || [];
      }

      tradeSymbols.forEach(tradeSym => {
        if (tradeSym === 'MT') return;

        const key = `${tradeSym}_${comboIndex}`;
        const lastTriggerTs = alertLastTriggers[key] || 0;
        if (currentTs - lastTriggerTs < delayMs) return;

        let allTrue = true;
        for (const cond of parsed.conditions) {
          let evalRow;
          let symToUse = cond.algoSymbol;

          if (cond.algoSymbol === 'MT') {
            evalRow = metricsData.find(d => d.symbol === 'MT' && d.exchange === cond.exchange);
          } else if (cond.algoSymbol === 'ALL') {
            symToUse = tradeSym;
            evalRow = metricsData.find(d => d.symbol === symToUse && d.exchange === cond.exchange);
          } else {
            evalRow = metricsData.find(d => d.symbol === cond.algoSymbol && d.exchange === cond.exchange);
          }

          if (!evalRow) {
            allTrue = false;
            break;
          }

          const val = evalRow[cond.param];
          if (val === null || val === undefined || isNaN(val)) {
            allTrue = false;
            break;
          }

          let conditionTrue;
          if (cond.op === '>') conditionTrue = val > cond.val;
          else conditionTrue = val < cond.val;

          if (!conditionTrue) {
            allTrue = false;
            break;
          }
        }

        if (allTrue) {
          const triggerTs = currentTs;
          alertHistory.unshift({
            ts: triggerTs,
            symbol: tradeSym,
            direction: parsed.direction,
            comboAlgo: algoStr,
            comboIndex
          });
          alertLastTriggers[key] = triggerTs;
        }
      });
    });

    alertHistory = alertHistory.slice(0, maxAlerts);

    setStored('alertHistory', alertHistory);
    setStored('alertLastTriggers', alertLastTriggers);
  }

  function renderCards() {
    const grid = document.getElementById('alert-grid');
    if (!grid) return;

    grid.innerHTML = '';

    if (alertHistory.length === 0) {
      const placeholder = document.createElement('div');
      placeholder.id = 'no-alerts-placeholder';
      placeholder.className = 'loading';
      placeholder.style.width = '100%';
      placeholder.style.textAlign = 'center';
      placeholder.style.padding = '40px';
      placeholder.style.color = '#aaa';
      placeholder.textContent = pollingRunning ? 'No alerts yet.' : 'Start polling to monitor triggers.';
      grid.appendChild(placeholder);
      return;
    }

    alertHistory.forEach(alert => {
      const card = document.createElement('div');
      card.className = 'alert-card';
      const date = new Date(alert.ts);
      const timeStr = `${date.getUTCHours().toString().padStart(2, '0')}:${date.getUTCMinutes().toString().padStart(2, '0')}:${date.getUTCSeconds().toString().padStart(2, '0')} UTC ${date.getUTCFullYear()}-${(date.getUTCMonth() + 1).toString().padStart(2, '0')}-${date.getUTCDate().toString().padStart(2, '0')}`;
      const dirColor = alert.direction === 'Long' ? '#4ade80' : '#f87171';
      const dirClass = alert.direction === 'Long' ? 'btn-glow-green' : 'btn-glow-red';

      const comboText = alert.comboAlgo.replace(/\s*\+\s*/g, '+<br>');
      card.innerHTML = `
      <div class="card-time" style="font-size: 1rem; color: #aaa; margin-bottom: 5px;">${timeStr}</div>
      <div class="card-direction ${dirClass}" style="font-size: 1.5rem; font-weight: bold; color: ${dirColor}; margin-bottom: 10px; text-align: center;">${alert.direction}</div>
      <div class="card-symbol" style="font-size: 1.2rem; color: #d1d5db; margin-bottom: 5px; text-align: center;">${alert.symbol}</div>
      <div class="card-combo" style="font-size: 1rem; color: #9f59ff; white-space: normal;">#${alert.comboIndex + 1} <br> ${comboText}</div>
      `;
      grid.appendChild(card);
    });
  }

  function clearAlerts() {
    alertHistory = [];
    alertLastTriggers = {};
    setStored('alertHistory', alertHistory);
    setStored('alertLastTriggers', alertLastTriggers);
    renderCards();
  }

  function restoreAlerts() {
    alertHistory = getStored('alertHistory', []);
    alertLastTriggers = getStored('alertLastTriggers', {});
    renderCards();
  }

  async function initialize() {
    restoreAlerts();
    pollingRunning = getStored('alertPollingRunning', false);
    if (pollingRunning) {
      startPolling();
    }
    console.log(`AlertPoll: Initialized. Polling running: ${pollingRunning}`);
  }

  // Expose pollingRunning state for UI
  function isPollingRunning() {
    return pollingRunning;
  }

  return {
    startPolling,
    stopPolling,
    isRunning,
    isValidInputs,
    renderCards,
    clearAlerts,
    initialize
  };
})();

window.alertPoll = alertPoll;