// public/js/card-controls.js
// UI Controls Logic for Alert Cards
// Handles Start/Stop buttons, comboAlgo inputs, delay/max alerts, wipe button, validation, and UI updates

(function() {
  const STORAGE_KEYS = {
    comboAlgos: 'alertComboAlgos',
    delay: 'alertDelay',
    maxAlerts: 'alertMaxAlerts'
  };

  function saveSettings() {
    const comboInputs = document.querySelectorAll('.combo-input');
    const comboAlgos = Array.from(comboInputs).map(input => input.value.trim()).filter(v => v);
    localStorage.setItem(STORAGE_KEYS.comboAlgos, JSON.stringify(comboAlgos));

    const delay = parseInt(document.getElementById('delay-input').value) || 5;
    localStorage.setItem(STORAGE_KEYS.delay, delay.toString());

    const maxAlerts = parseInt(document.getElementById('max-alerts-input').value) || 20;
    localStorage.setItem(STORAGE_KEYS.maxAlerts, maxAlerts.toString());
  }

  function restoreSettings() {
    const savedAlgos = localStorage.getItem(STORAGE_KEYS.comboAlgos);
    if (savedAlgos) {
      const algos = JSON.parse(savedAlgos);
      const container = document.getElementById('combo-inputs-container');
      container.querySelectorAll('.combo-input-row').forEach(row => row.remove());
      algos.forEach((algo, index) => addComboInput(algo, index));
      if (algos.length === 0) addComboInput('', 0);
    } else {
      addComboInput('', 0);
    }

    const savedDelay = localStorage.getItem(STORAGE_KEYS.delay) || '5';
    document.getElementById('delay-input').value = savedDelay;

    const savedMax = localStorage.getItem(STORAGE_KEYS.maxAlerts) || '20';
    document.getElementById('max-alerts-input').value = savedMax;
  }

  function addComboInput(value = '', index) {
    const container = document.getElementById('combo-inputs-container');
    const row = document.createElement('div');
    row.className = 'combo-input-row';
    row.dataset.index = index;
    row.style.display = 'flex';
    row.style.alignItems = 'center';
    row.style.gap = '10px';
    row.innerHTML = `
      <span class="input-number" style="font-weight: bold; color: #b569ff;">#${index + 1}</span>
      <input type="text" class="combo-input" placeholder="e.g. All;Short;MT_bin_rsi1_chg_10m<30 + All_bin_v_chg_5m>20" value="${value}" style="flex: 1; padding: 9px; font-size: 1.1rem; border-radius: 5px; border: 1px solid #5a4090; background: #382356; color: lightgrey;" />
      <button class="${index === 0 ? 'add-btn btn btn-small' : 'remove-btn btn btn-small btn-glow-red'}" style="margin-left: 10px;">${index === 0 ? '+' : '-'}</button>
      <span class="error-msg" style="color: #f87171; font-size: 0.8rem; margin-left: 10px;"></span>
    `;
    container.appendChild(row);
  }

  function renumberInputs() {
    document.querySelectorAll('.combo-input-row').forEach((row, index) => {
      row.dataset.index = index;
      row.querySelector('.input-number').textContent = `#${index + 1}`;
      const btn = row.querySelector('button');
      if (index === 0) {
        btn.textContent = '+';
        btn.className = 'add-btn btn btn-small';
      } else {
        btn.textContent = '-';
        btn.className = 'remove-btn btn btn-small btn-glow-red';
      }
    });
  }

  function validateComboInput(input) {
    const errorSpan = input.parentElement.querySelector('.error-msg');
    if (!input.value.trim()) {
      input.style.borderColor = '#5a4090';
      errorSpan.textContent = '';
      return true;
    }
    const valid = parseComboAlgo(input.value.trim());
    if (!valid) {
      input.style.borderColor = '#f87171';
      errorSpan.textContent = 'Invalid comboAlgo format.';
      return false;
    } else {
      input.style.borderColor = '#5a4090';
      errorSpan.textContent = '';
      return true;
    }
  }

  // Dummy parser placeholder - replace with real parser or import
  function parseComboAlgo(str) {
    const parts = str.split(';');
    if (parts.length !== 3) return false;
    return true;
  }

  function updateStartButton() {
    const btn = document.getElementById('start-polling-btn');
    const inputs = document.querySelectorAll('.combo-input');
    const allValid = Array.from(inputs).every(input => validateComboInput(input));
    btn.disabled = !allValid || inputs.length === 0;
    btn.classList.toggle('btn-glow-green', !btn.disabled);
    btn.classList.toggle('btn-glow-red', btn.disabled);
  }
//=================================================
  function setupListeners() {
    document.getElementById('start-polling-btn').addEventListener('click', () => {
      if (window.alertPoll && window.alertPoll.isRunning()) {
        window.alertPoll.stopPolling();
        updatePollingStatus(false);
      } else {
        if (!window.alertPoll || !window.alertPoll.isValidInputs()) return;
        window.alertPoll.startPolling();
        updatePollingStatus(true);
      }
    });

    document.getElementById('delay-input').addEventListener('input', () => {
      saveSettings();
    });

    document.getElementById('max-alerts-input').addEventListener('input', () => {
      saveSettings();
      if (window.alertPoll) window.alertPoll.renderCards();
    });

    document.getElementById('wipe-btn').addEventListener('click', () => {
      if (confirm('Clear all alerts, settings, and history? This cannot be undone.')) {
        localStorage.clear();
        if (window.alertPoll) {
          window.alertPoll.stopPolling();
          window.alertPoll.clearAlerts();
        }
        restoreSettings();
        updatePollingStatus(false);
      }
    });

    document.getElementById('combo-inputs-container').addEventListener('click', (e) => {
      if (e.target.classList.contains('add-btn')) {
        const count = document.querySelectorAll('.combo-input-row').length;
        if (count < 10) {
          addComboInput('', count);
          renumberInputs();
          updateStartButton();
        }
      } else if (e.target.classList.contains('remove-btn')) {
        if (document.querySelectorAll('.combo-input-row').length > 1) {
          e.target.closest('.combo-input-row').remove();
          renumberInputs();
          updateStartButton();
        }
      }
    });

    document.getElementById('combo-inputs-container').addEventListener('input', (e) => {
      if (e.target.classList.contains('combo-input')) {
        validateComboInput(e.target);
        updateStartButton();
        saveSettings();
      }
    });
  //============  POLL-TEST  ================================
    document.getElementById('test-poll-btn').addEventListener('click', () => {
      console.log('Poll Test Initiated.');
      if (window.pollTest) {
      window.pollTest.runTest();
      }
    });
  }
  //=================================================
  function updatePollingStatus(isRunning) {
    const btn = document.getElementById('start-polling-btn');
    const status = document.getElementById('polling-status');
    if (isRunning) {
      btn.textContent = 'Stop';
      btn.classList.remove('btn-glow-red');
      btn.classList.add('btn-glow-green');
      status.textContent = 'live';
      status.style.color = '#4ade80';
      console.log('AlertControls: Polling started.');
    } else {
      btn.textContent = 'Start';
      btn.classList.remove('btn-glow-green');
      btn.classList.add('btn-glow-red');
      status.textContent = 'stopped';
      status.style.color = '#777';
      console.log('AlertControls: Polling stopped.');
    }
  }

  function initializeControls() {
    restoreSettings();
    setupListeners();
    updateStartButton();
    const pollingRunning = localStorage.getItem('alertPollingRunning') === 'true';
    updatePollingStatus(pollingRunning);

    // If polling was running, ensure alertPoll is started
    if (pollingRunning && window.alertPoll && !window.alertPoll.isRunning()) {
      window.alertPoll.startPolling();
    }
  }

  window.initializeCardControls = initializeControls;
})();
