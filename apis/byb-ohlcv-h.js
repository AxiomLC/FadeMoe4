const fs = require('fs');
const axios = require('axios');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
require('dotenv').config();

const SCRIPT_NAME = 'byb-ohlcv-h.js';
const INTERVAL = '1m';
const DAYS_TO_FETCH = 10;
const LIMIT_PER_REQUEST = 1000;
const RATE_LIMIT_DELAY = 200;
const PERPSPEC_PREFIX = {
  bybit: 'byb-ohlcv'
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchBybitHistorical(symbol, days = DAYS_TO_FETCH) {
  const totalCandles = days * 1440;
  const totalRequests = Math.ceil(totalCandles / LIMIT_PER_REQUEST);

  console.log(`[bybit] Backfill: ${totalCandles} candles, ${totalRequests} requests for ${symbol}`);

  let allCandles = [];
  let end = Date.now();

  for (let i = 0; i < totalRequests; i++) {
    console.log(`[bybit] Request ${i + 1}/${totalRequests} for ${symbol}`);

    const url = 'https://api.bybit.com/v5/market/kline';
    const params = {
      category: 'linear',
      symbol: symbol,
      interval: '1',
      limit: LIMIT_PER_REQUEST,
      end: end
    };

    try {
      const response = await axios.get(url, { params, timeout: 10000 });
      const data = response.data;

      if (!data.result?.list || data.result.list.length === 0) {
        console.log('[bybit] No more data available');
        break;
      }

      allCandles.push(...data.result.list);

      end = data.result.list[data.result.list.length - 1][0] - 1;

      await sleep(RATE_LIMIT_DELAY);
    } catch (error) {
      console.error(`[bybit] Request failed:`, error.message);
      await sleep(1000);
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
      console.warn(`[bybit] Skipping invalid candle for ${baseSymbol}:`, candle);
      return null;
    }
  }).filter(item => item !== null);
}

async function backfillSymbol(baseSymbol, since) {
  const perpspec = PERPSPEC_PREFIX.bybit;
  const dynamicSymbols = JSON.parse(fs.readFileSync('dynamic-symbols.json', 'utf8'));
  const symbol = dynamicSymbols[baseSymbol]?.[perpspec];
  if (!symbol) {
    console.warn(`[bybit] No symbol mapping for ${baseSymbol} (${perpspec}), skipping.`);
    return;
  }

  console.log(`[bybit] Starting backfill for ${baseSymbol} (${symbol})`);

  try {
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `Backfill started for ${baseSymbol} on bybit`, {
      symbol,
      from: since,
      interval: INTERVAL
    });

    const rawCandles = await fetchBybitHistorical(symbol, DAYS_TO_FETCH);
    console.log(`[bybit] Fetched ${rawCandles.length} raw candles for ${symbol}`);

    if (rawCandles.length === 0) {
      const warningMsg = `No OHLCV data returned for ${symbol}`;
      console.warn(`[bybit] ${warningMsg}`);
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'warning', warningMsg, { symbol });
      return;
    }

    const processedData = processCandles(rawCandles, baseSymbol, perpspec);

    if (processedData.length === 0) {
      const warningMsg = `No valid OHLCV data after processing for ${symbol}`;
      console.warn(`[bybit] ${warningMsg}`);
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'warning', warningMsg, { symbol });
      return;
    }

    const expectedFields = Object.keys(processedData[0]);
    await apiUtils.ensureColumnsExist(dbManager, expectedFields);
    await apiUtils.updatePerpspecSchema(dbManager, perpspec, expectedFields);
    await dbManager.insertData(perpspec, processedData);

    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'success', `Backfill completed for ${baseSymbol} on bybit`, {
      records_inserted: processedData.length,
      symbol
    });

    console.log(`[bybit] Backfill completed for ${baseSymbol}`);

  } catch (error) {
    console.error(`[bybit] Error during backfill for ${baseSymbol}:`, error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'FETCH_INSERT_ERROR', error.message, {
      exchange: 'bybit',
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

  const limit = (await import('p-limit')).default;
  const limiter = limit(3);
  const promises = [];

  for (const baseSymbol of Object.keys(dynamicSymbols)) {
    promises.push(limiter(() => backfillSymbol(baseSymbol, since)));
  }

  await Promise.all(promises);

  console.log('🎉 Bybit OHLCV backfill completed!');
}

if (require.main === module) {
  execute()
    .then(() => {
      console.log('🏁 Bybit OHLCV backfill script completed successfully');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 Bybit OHLCV backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { execute };