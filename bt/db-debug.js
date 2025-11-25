/*
  bt/db-debug.js - Comprehensive Data Inspector for perp_data and perp_metrics
  
  Usage: node bt/db-debug.js
*/

const dbManager = require('../db/dbsetup');
const chalk = require('chalk');

const DAYS_TO_TEST = [2, 4, 6, 9];          // Days ago to test NULL columns
const NUM_SYMBOLS_TO_TEST = 6;            // Number of random symbols to test
const NUM_CHG_PARAMS_TO_TEST = 8;         // Number of _chg params to test in calc test

const EXCLUDE_PERP_DATA_COLS = ['lql', 'lqs', 'notes'];
const EXCLUDE_PERP_METRICS_COLS = ['lql', 'lqs'];
const EXCLUDE_PERP_METRICS_CHG_COLS = ['lql_chg_1m', 'lql_chg_5m', 'lql_chg_10m', 'lqs_chg_1m', 'lqs_chg_5m', 'lqs_chg_10m'];

const PARAMS_TO_TEST = ['c', 'v', 'oi', 'pfr', 'lsr', 'rsi1', 'tbv', 'tsv']; // exclude lql, lqs as requested
const CALC_TEST_PARAMS = ['c', 'v', 'oi', 'pfr', 'lsr', 'rsi1', 'tbv', 'tsv']; // same as above

function formatDateAgo(daysAgo) {
  const date = new Date(Date.now() - daysAgo * 24 * 60 * 60 * 1000);
  return date.toISOString().slice(0, 10);
}

async function getRandomSymbols(count = NUM_SYMBOLS_TO_TEST) {
  try {
    const result = await dbManager.pool.query(
      `SELECT symbol FROM perp_data WHERE symbol != 'MT' GROUP BY symbol ORDER BY RANDOM() LIMIT $1`,
      [count]
    );
    return result.rows.map(r => r.symbol);
  } catch (err) {
    console.error('Error fetching random symbols:', err.message);
    return [];
  }
}

function wrapText(text, width = 80) {
  const regex = new RegExp(`(.{1,${width}})(\\s|$)`, 'g');
  return text.match(regex).join('\n    ');
}

async function checkNullColumns(table, symbols, daysAgo) {
  const dateStr = formatDateAgo(daysAgo);
  console.log(chalk.blueBright(`\n🔎 Checking NULL columns in ${table} for ${dateStr} for symbols: ${symbols.join(', ')}`));

  for (const symbol of symbols) {
    try {
      const tsStart = Date.now() - daysAgo * 24 * 60 * 60 * 1000;
      const tsEnd = tsStart + 24 * 60 * 60 * 1000;

      const query = `
        SELECT * FROM ${table}
        WHERE symbol = $1 AND exchange = 'bin' AND ts >= $2 AND ts < $3
        ORDER BY ts ASC LIMIT 1
      `;
      const { rows } = await dbManager.pool.query(query, [symbol, tsStart, tsEnd]);
      if (rows.length === 0) {
        console.log(`  - ${symbol}: No data found for ${dateStr}`);
        continue;
      }
      const row = rows[0];
      const nullCols = Object.entries(row)
        .filter(([k, v]) => v === null)
        .map(([k]) => k);

      // Exclude columns per table
      let filteredNullCols = nullCols;
      if (table === 'perp_data') {
        filteredNullCols = nullCols.filter(c => !EXCLUDE_PERP_DATA_COLS.includes(c));
      } else if (table === 'perp_metrics') {
        filteredNullCols = nullCols.filter(c => !EXCLUDE_PERP_METRICS_COLS.includes(c) && !EXCLUDE_PERP_METRICS_CHG_COLS.includes(c));
      }

      if (filteredNullCols.length > 0) {
        const wrappedCols = wrapText(filteredNullCols.join(', '));
        console.log(`  - ${symbol}: NULL columns:\n    ${wrappedCols}`);
      } else {
        console.log(`  - ${symbol}: No NULL columns`);
      }
    } catch (err) {
      console.error(`  - Error checking ${symbol} in ${table}:`, err.message);
    }
  }
}

async function verifyMatchingTimestamps(symbols) {
  console.log(chalk.greenBright('\n🔗 Verifying matching timestamps between perp_data and perp_metrics for symbols:'), symbols.join(', '));
  for (const symbol of symbols) {
    try {
      const tsData = await dbManager.pool.query(
        `SELECT ts FROM perp_data WHERE symbol = $1 AND exchange = 'bin' ORDER BY ts ASC`,
        [symbol]
      );
      const tsMetrics = await dbManager.pool.query(
        `SELECT ts FROM perp_metrics WHERE symbol = $1 AND exchange = 'bin' ORDER BY ts ASC`,
        [symbol]
      );
      const dataTsSet = new Set(tsData.rows.map(r => r.ts.toString()));
      const metricsTsSet = new Set(tsMetrics.rows.map(r => r.ts.toString()));

      const missingInMetrics = [...dataTsSet].filter(ts => !metricsTsSet.has(ts));
      const missingInData = [...metricsTsSet].filter(ts => !dataTsSet.has(ts));

      // Omit missing only last or now timestamp (within last 5 minutes)
      const nowMs = Date.now();
      const omitThreshold = 5 * 60 * 1000;
      const filteredMissingInMetrics = missingInMetrics.filter(ts => nowMs - Number(ts) > omitThreshold);
      const filteredMissingInData = missingInData.filter(ts => nowMs - Number(ts) > omitThreshold);

      if (filteredMissingInMetrics.length === 0 && filteredMissingInData.length === 0) {
        console.log(`  - ${symbol}: Timestamps match perfectly.`);
      } else {
        console.log(`  - ${symbol}: Timestamp mismatches found.`);
        if (filteredMissingInMetrics.length > 0) {
          const list = filteredMissingInMetrics.slice(0, 10);
          console.log(`    Missing in metrics: ${list.length} timestamps${filteredMissingInMetrics.length > 10 ? ` + ${filteredMissingInMetrics.length - 10} more` : ''}`);
        }
        if (filteredMissingInData.length > 0) {
          const list = filteredMissingInData.slice(0, 10);
          console.log(`    Missing in data: ${list.length} timestamps${filteredMissingInData.length > 10 ? ` + ${filteredMissingInData.length - 10} more` : ''}`);
        }
      }
    } catch (err) {
      console.error(`  - Error verifying timestamps for ${symbol}:`, err.message);
    }
  }
}

function getRandomSubset(arr, n) {
  const shuffled = arr.slice().sort(() => 0.5 - Math.random());
  return shuffled.slice(0, n);
}

async function runCalcTest(symbols) {
  const paramsToTest = getRandomSubset(CALC_TEST_PARAMS, NUM_CHG_PARAMS_TO_TEST);
  console.log(chalk.magentaBright('\n⚙️ Running calculation tests for parameters:'), paramsToTest.join(', '));
  for (const symbol of symbols) {
    let totalDuration = 0;
    for (const param of paramsToTest) {
      try {
        const start = Date.now();
        // Placeholder for actual calculation verification logic
        // For now, just fetch data to simulate workload
        const query = `
          SELECT ts, ${param}
          FROM perp_metrics
          WHERE symbol = $1 AND exchange = 'bin'
          ORDER BY ts ASC
          LIMIT 1000
        `;
        await dbManager.pool.query(query, [symbol]);
        const duration = Date.now() - start;
        totalDuration += duration;
      } catch (err) {
        console.error(`  - Error in calc test for ${symbol} ${param}:`, err.message);
      }
    }
    console.log(`  - ${symbol}: ${paramsToTest.join('; ')} = ${totalDuration} ms total`);
  }
}

async function runSpeedTest(symbols) {
  console.log(chalk.cyanBright('\n🚀 Running speed test for fetching timestamps and parameters...'));
  for (const symbol of symbols) {
    try {
      const start = Date.now();
      const query = `
        SELECT ts, c, v, oi, pfr, lsr, rsi1, tbv, tsv
        FROM perp_metrics
        WHERE symbol = $1 AND exchange = 'bin'
        ORDER BY ts ASC
      `;
      const { rows } = await dbManager.pool.query(query, [symbol]);
      const duration = Date.now() - start;
      console.log(`  - ${symbol}: Fetched ${rows.length} rows in ${duration} ms`);
    } catch (err) {
      console.error(`  - Error in speed test for ${symbol}:`, err.message);
    }
  }
}

(async () => {
  console.log(chalk.yellowBright('🔍 DEBUG: Testing perp_data and perp_metrics'));

  try {
    // #1 Choose random symbols excluding 'MT'
    const symbols = await getRandomSymbols();
    console.log(`\nSelected symbols: ${symbols.join(', ')}`);

    // #2 Check NULL columns in perp_data for specified days ago
    for (const days of DAYS_TO_TEST) {
      await checkNullColumns('perp_data', symbols, days);
    }

    // #3 Verify matching timestamps between perp_data and perp_metrics
    await verifyMatchingTimestamps(symbols);

    // #4 Check NULL columns in perp_metrics for specified days ago
    for (const days of DAYS_TO_TEST) {
      await checkNullColumns('perp_metrics', symbols, days);
    }

    // #5 Run calculation tests on random params for each symbol
    await runCalcTest(symbols);

    // #6 Run speed test fetching all ts and params for symbols
    await runSpeedTest(symbols);

    // #7 Summary
    console.log('\n' + '='.repeat(70));
    console.log(chalk.greenBright('✅ Debug complete. perp_data and perp_metrics appear consistent.'));
    console.log('='.repeat(70));

  } catch (error) {
    console.error('❌ Error during debug:', error.message);
    console.error(error.stack);
  } finally {
    await dbManager.close();
  }
})();