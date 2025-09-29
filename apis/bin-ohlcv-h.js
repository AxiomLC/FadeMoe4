const fs = require('fs');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const axios = require('axios');
require('dotenv').config();

const SCRIPT_NAME = 'bin-ohlcv-h.js';
const INTERVAL = '1m';
const DAYS_TO_FETCH = 10;
const LIMIT_PER_REQUEST = 1500;
const RATE_LIMIT_DELAY = 200;
const PERPSPEC_PREFIX = {
  binance: 'bin-ohlcv'
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchBinanceHistorical(symbol, days = DAYS_TO_FETCH) {
  const totalCandles = days * 1440;
  const totalRequests = Math.ceil(totalCandles / LIMIT_PER_REQUEST);

  console.log(`[binance] Backfill: ${totalCandles} candles, ${totalRequests} requests for ${symbol}`);

  let allCandles = [];
  let startTime = null;
  for (let i = 0; i < totalRequests; i++) {
    console.log(`[binance] Request ${i + 1}/${totalRequests} for ${symbol}`);

    const url = 'https://fapi.binance.com/fapi/v1/klines';
    const params = {
      symbol: symbol,
      interval: INTERVAL,
      limit: LIMIT_PER_REQUEST
    };
    if (startTime) {
      params.startTime = startTime;
    }

    try {
      const response = await axios.get(url, { params, timeout: 10000 });
      const data = response.data;

      if (!data || data.length === 0) {
        console.log('[binance] No more data available');
        break;
      }

      allCandles.push(...data);

      startTime = data[data.length - 1][0] + 1;

      await sleep(RATE_LIMIT_DELAY);
    } catch (error) {
      console.error(`[binance] Request failed:`, error.message);
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
      console.warn(`[binance] Skipping invalid candle for ${baseSymbol}:`, candle);
      return null;
    }
  }).filter(item => item !== null);
}

async function backfillSymbol(baseSymbol, since) {
  const perpspec = PERPSPEC_PREFIX.binance;
  const dynamicSymbols = JSON.parse(fs.readFileSync('dynamic-symbols.json', 'utf8'));
  const symbol = dynamicSymbols[baseSymbol]?.[perpspec];
  if (!symbol) {
    console.warn(`[binance] No symbol mapping for ${baseSymbol} (${perpspec}), skipping.`);
    return;
  }

  console.log(`[binance] Starting backfill for ${baseSymbol} (${symbol})`);

  try {
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', `Backfill started for ${baseSymbol} on binance`, {
      symbol,
      from: since,
      interval: INTERVAL
    });

    const rawCandles = await fetchBinanceHistorical(symbol, DAYS_TO_FETCH);
    console.log(`[binance] Fetched ${rawCandles.length} raw candles for ${symbol}`);

    if (rawCandles.length === 0) {
      const warningMsg = `No OHLCV data returned for ${symbol}`;
      console.warn(`[binance] ${warningMsg}`);
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'warning', warningMsg, { symbol });
      return;
    }

    const processedData = processCandles(rawCandles, baseSymbol, perpspec);

    if (processedData.length === 0) {
      const warningMsg = `No valid OHLCV data after processing for ${symbol}`;
      console.warn(`[binance] ${warningMsg}`);
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'warning', warningMsg, { symbol });
      return;
    }

    const expectedFields = Object.keys(processedData[0]);
    await apiUtils.ensureColumnsExist(dbManager, expectedFields);
    await apiUtils.updatePerpspecSchema(dbManager, perpspec, expectedFields);
    await dbManager.insertData(perpspec, processedData);

    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'success', `Backfill completed for ${baseSymbol} on binance`, {
      records_inserted: processedData.length,
      symbol
    });

    console.log(`[binance] Backfill completed for ${baseSymbol}`);

  } catch (error) {
    console.error(`[binance] Error during backfill for ${baseSymbol}:`, error.message);
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'API', 'FETCH_INSERT_ERROR', error.message, {
      exchange: 'binance',
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

  console.log('🎉 Binance OHLCV backfill completed!');
}

if (require.main === module) {
  execute()
    .then(() => {
      console.log('🏁 Binance OHLCV backfill script completed successfully');
      process.exit(0);
    })
    .catch(err => {
      console.error('💥 Binance OHLCV backfill script failed:', err);
      process.exit(1);
    });
}

module.exports = { execute };