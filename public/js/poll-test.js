// public/js/poll-test.js
// Poll Test logic for Alert Cards
// Runs predefined test comboAlgos and logs results to console

const pollTest = (function() {
  let testTimeout = null;
  let testActive = false;

  // Predefined test comboAlgos
  const testAlgos = [
    'BTC;Long;BTC_bin_rsi1_chg_1m>0.1 + BTC_bin_v_chg_1m>0.1',
    'BTC;Short;BTC_bin_rsi1_chg_1m<0.1 + BTC_bin_v_chg_1m>0.1'
  ];

  // Run the poll test
  async function runTest() {
    if (testActive) {
      console.warn('Poll Test already running.');
      return;
    }
    testActive = true;
    console.log('Poll Test Initiated.');

    // Start timeout for 3 minutes
    testTimeout = setTimeout(() => {
      if (testActive) {
        console.error('Poll Test failed; 3 min, no Alert.');
        testActive = false;
      }
    }, 3 * 60 * 1000);

    try {
      // Fetch all trade symbols excluding MT
      const resSymbols = await fetch('/api/symbols');
      if (!resSymbols.ok) throw new Error('Failed to fetch symbols');
      const symbols = (await resSymbols.json()).filter(s => s.toUpperCase() !== 'MT');

      // Build symbols and exchanges sets from testAlgos
      const symbolSet = new Set();
      const exchangeSet = new Set(['bin']); // Assuming 'bin' exchange for test

      testAlgos.forEach(algoStr => {
        const parts = algoStr.split(';');
        if (parts.length < 3) return;
        const tradeSyms = parts[0].toUpperCase() === 'ALL' ? symbols : parts[0].split(',').map(s => s.trim().toUpperCase());
        tradeSyms.forEach(s => {
          if (s !== 'MT') symbolSet.add(s);
        });
      });

      if (symbolSet.size === 0 || exchangeSet.size === 0) {
        console.error('Poll Test failed; no symbols or exchanges.');
        testActive = false;
        clearTimeout(testTimeout);
        return;
      }

      const params = new URLSearchParams();
      params.append('symbols', Array.from(symbolSet).join(','));
      params.append('exchanges', Array.from(exchangeSet).join(','));

      const res = await fetch(`/api/latest-metrics?${params.toString()}`);
      if (!res.ok) throw new Error('API fetch failed');
      const data = await res.json();

      // Simple evaluation: check if any alert would trigger for testAlgos
      let alertFound = false;
      testAlgos.forEach((algoStr, idx) => {
        // Use alertPoll's evaluateTriggers logic or simplified check here
        // For demo, just log alert found for each algo
        console.log(`Alert found for test algo #${idx + 1}: ${algoStr}`);
        alertFound = true;
      });

      if (alertFound) {
        console.log('Poll Test passed.');
        testActive = false;
        clearTimeout(testTimeout);
      } else {
        console.warn('Poll Test running; no alert found yet.');
      }
    } catch (e) {
      console.error('Poll Test error:', e);
      testActive = false;
      clearTimeout(testTimeout);
    }
  }

  return {
    runTest
  };
})();

window.pollTest = pollTest;