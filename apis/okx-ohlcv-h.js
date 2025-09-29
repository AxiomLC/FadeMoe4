const fs = require('fs');
const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
require('dotenv').config();

const SCRIPT_NAME = 'okx-ohlcv-h.js';
const INTERVAL = '1m';
const DAYS_TO_FETCH = 10;
const LIMIT_PER_REQUEST = 100;
const RATE_LIMIT_DELAY = 100;
const PERPSPEC_PREFIX = {
  okx: 'okx-ohlcv'
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchOKXHistorical(symbol, days = DAYS_TO_FETCH) {
  const limit = LIMIT_PER_REQUEST;
  const targetCandles = days * 1440;
  const totalRequests = Math.ceil(targetCandles / limit);

  console.log(`[okx] Backfill: ${targetCandles} candles, ${totalRequests} requests for ${symbol}`);

  let allCandles = [];
  let after = null;
  for (let i = 0; i < totalRequests; i++) {
    let url = `https://www.okx.com/api/v5/market/history-candles?instId=${symbol}&bar=${INTERVAL}&limit=${limit}`;
    if (after) url += `&after=${after}`;
    try {
      const response = await axios.get(url, { timeout: 10000 });
      const data = response.data;

      if (data.code === "0" && data.data && data.data.length > 0) {
      allCandles.push(...data.data);

        if (i === 0) {
          console.log(`[okx] Example candle:`, data.data[0]);
        }
        after = data.data[data.data.length - 1][0];

        console.log(`[okx] Request ${i + 1}/${totalRequests}: +${data.data.length} candles (Total: ${allCandles.length})`);

        if (allCandles.length >= targetCandles) {
          console.log(`[okx] 🎯 Target reached!`);
          break;
    }
      } else {
        console.log(`[okx] Request ${i + 1} failed or no data:`, data.msg || 'No data');
        break;
  }

      await sleep(RATE_LIMIT_DELAY);
  } catch (error) {
      console.error(`[okx] Request ${i + 1} error:`, error.message);
      break;
  }
}

  return allCandles;
}

function processCandles(candles, baseSymbol, perpspec) {
  return candles.map(candle => {
  try {
      const ts = apiUtils.toMillis(BigInt(candle[0]));
      return {
        ts,
        symbol: baseSymbol,
        source: perpspec,
        perpspec,
        interval: INTERVAL,
        o: parseFloat(candle[1]),
        h: parseFloat(candle[2]),
        l: parseFloat(candle[3]),
        c: parseFloat(candle[4]),
        v: parseFloat(candle[5])
      };
    } catch (e) {
      console.warn(`[okx] Skipping invalid candle for ${baseSymbol}:`, candle);
      return null;
  }
  }).filter(item => item !== null);
}

async function backfillSymbol(baseSymbol, since) {
  const perpspec = PERPSPEC_PREFIX.okx;
  const dynamicSymbols = JSON.parse(fs.readFileSync('dynamic-symbols.json', 'utf8'));
  const symbol = dynamicSymbols[baseSymbol]?.[perpspec];
  if (!symbol) {
    console.warn(`[okx] No symbol mapping for ${baseSymbol} (${perpspec}), skipping.`);
    return;
  }

  console.log(`[okx] Starting backfill for ${baseSymbol} (${symbol})`);

  try {
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `Backfill started for ${baseSymbol} on okx`, {
      symbol,
      from: since,
      interval: INTERVAL
    });

    const rawCandles = await fetchOKXHistorical(symbol, DAYS_TO_FETCH);
    console.log(`[okx] Fetched ${rawCandles.length} raw candles for ${symbol}`);

    if (rawCandles.length === 0) {
      const warningMsg = `No OHLCV data returned for ${symbol}`;
      console.warn(`[okx] ${warningMsg}`);
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'warning', warningMsg, { symbol });
      return;
    }

    const processedData = processCandles(rawCandles, baseSymbol, perpspec);

    if (processedData.length === 0) {
      const warningMsg = `No valid OHLCV data after processing for ${symbol}`;
      console.warn(`[okx] ${warningMsg}`);
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'warning', warningMsg, { symbol });
      return;
    }

    const expectedFields = Object.keys(processedData[0]);
    await apiUtils.ensureColumnsExist(dbManager, expectedFields);
    await apiUtils.updatePerpspecSchema(dbManager, perpspec, expectedFields);
    await dbManager.insertData(perpspec, processedData);

    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'success', `Backfill completed for ${baseSymbol} on okx`, {
      records_inserted: processedData.length,
      symbol
    });

    console.log(`[okx] Backfill completed for ${baseSymbol}`);

  } catch (error) {
    console.error(`[okx] Error during backfill for ${baseSymbol}:`, error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'FETCH_INSERT_ERROR', error.message, {
      exchange: 'okx',
      symbol: baseSymbol
    });
  }
}

async function execute() {
  console.log(`🚀 Starting ${SCRIPT_NAME} backfill...`);

  let dynamicSymbols;
  try {
    dynamicSymbols = JSON.parse(fs.readFileSync('dynamic-symbols.json', 'utf8'));
    console.log(`📊 Found ${Object.keys(dynamicSymbols).length} symbols to process`);
  } catch (error) {
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'FILE', 'MISSING_SYMBOLS', 'Could not read dynamic-symbols.json. Run g-symbols.js first!');
    return;
  }

  const since = Date.now() - DAYS_TO_FETCH * 24 * 60 * 60 * 1000;

  const pLimit = (await import('p-limit')).default;
  const limit = pLimit(3);
  const promises = [];

  for (const baseSymbol of Object.keys(dynamicSymbols)) {
    promises.push(limit(() => backfillSymbol(baseSymbol, since)));
  }

  await Promise.all(promises);

  console.log('🎉 OKX OHLCV backfill completed!');
}

if (require.main === module) {
  execute()
    .then(() => {
      console.log('🏁 OKX OHLCV backfill script completed successfully');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 OKX OHLCV backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { execute };