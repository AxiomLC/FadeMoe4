/* ==========================================
 * o1z-web-ohlcv-c.js   (Revised 14 Oct 2025)
 * Continuous WebSocket OHLCV Collector
 * ========================================== */

const WebSocket = require('ws');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
const perpList = require('../perp-list');
require('dotenv').config();

const SCRIPT_NAME = 'o1z-web-ohlcv-c.js';

/* ==========================================
 * USER LOG COLOR CONTROLS
 * ========================================== */
const STATUS_COLOR = '\x1b[92m'; // Light green
const RESET = '\x1b[0m';
const ERROR_COLOR = '\x1b[91m'; // Red

/* ==========================================
 * MARKET TREND (MT) CONFIGURATION
 * ========================================== */
const MT_SYMBOLS = ['ETH', 'BTC', 'XRP', 'SOL'];
const MT_SYMBOL = 'MT';

/* ==========================================
 * EXCHANGE CONFIGURATION
 * ========================================== */
const EXCHANGE_CONFIG = {
  BINANCE: {
    PERPSPEC: 'bin-ohlcv',
    DB_INTERVAL: '1m',
    mapSymbol: sym => `${sym.toLowerCase()}usdt`,
    getWsUrl: sym => `wss://fstream.binance.com/ws/${sym.toLowerCase()}usdt@kline_1m`
  },
  BYBIT: {
    PERPSPEC: 'byb-ohlcv',
    WS_URL: 'wss://stream.bybit.com/v5/public/linear',
    DB_INTERVAL: '1m',
    mapSymbol: sym => {
      const memeCoins = ['BONK', 'PEPE', 'FLOKI', 'TOSHI'];
      return memeCoins.includes(sym) ? `1000${sym}USDT` : `${sym}USDT`;
    },
    unmapSymbol: topic => {
      const memeCoins = ['BONK', 'PEPE', 'FLOKI', 'TOSHI'];
      for (const coin of memeCoins) {
        if (topic === `1000${coin}USDT`) return coin;
      }
      return topic.replace('USDT', '');
    }
  },
  OKX: {
    PERPSPEC: 'okx-ohlcv',
    WS_URL: 'wss://ws.okx.com:8443/ws/v5/business',
    DB_INTERVAL: '1m',
    mapSymbol: sym => `${sym}-USDT-SWAP`
  }
};

/* ==========================================
 * STATE TRACKERS
 * ========================================== */
const completedSymbols = {
  'bin-ohlcv': new Set(),
  'byb-ohlcv': new Set(),
  'okx-ohlcv': new Set()
};
const bybitActiveSymbols = new Set();

let mtLatestData = new Map();
MT_SYMBOLS.forEach(sym => mtLatestData.set(sym, null));

/* ==========================================
 * CONNECTION STATUS FLAGS
 * ========================================== */
let connectedFlags = {
  BINANCE: false,
  BYBIT: false,
  OKX: false
};
let connectedLogged = false;

/* ==========================================
 * Helper: check if all connected
 * ========================================== */
async function checkAllConnected() {
  if (!connectedLogged && connectedFlags.BINANCE && connectedFlags.BYBIT && connectedFlags.OKX) {
    connectedLogged = true;
    const perpspecs = 'bin-ohlcv, byb-ohlcv, okx-ohlcv';
    const message = `🛫 ${perpspecs} websockets connected, fetching started.`;
    console.log(`${STATUS_COLOR} ${message}${RESET}`);
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'connected', message);
  }
}

/* ==========================================
 * Compute and Insert MT
 * ========================================== */
function computeMTRecord(currentTs) {
  let totalO = 0, totalH = 0, totalL = 0, totalC = 0, totalV = 0;
  let count = 0;
  for (const sym of MT_SYMBOLS) {
    const latest = mtLatestData.get(sym);
    if (latest && latest.ts <= currentTs) {
      totalO += latest.o; totalH += latest.h; totalL += latest.l;
      totalC += latest.c; totalV += latest.v; count++;
    }
  }
  if (count === 0) return null;
  return {
    ts: currentTs,
    symbol: MT_SYMBOL,
    source: 'bin-ohlcv',
    perpspec: 'bin-ohlcv',
    interval: '1m',
    o: totalO / count,
    h: totalH / count,
    l: totalL / count,
    c: totalC / count,
    v: totalV
  };
}

async function insertMT(perpspec, mtRecord) {
  try {
    if (!mtRecord) return;
    await dbManager.insertData(perpspec, [mtRecord]);
    console.log(`${STATUS_COLOR}✈️ MT Market Trend token 1m${RESET}`);
  } catch (error) {
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'INTERNAL', 'MT_INSERT_FAILED', error.message);
    console.error(`${ERROR_COLOR}❌ MT insert failed: ${error.message}${RESET}`);
  }
}

/* ==========================================
 * PROCESS + INSERT
 * ========================================== */
async function processAndInsert(exchange, baseSymbol, rawData) {
  const config = EXCHANGE_CONFIG[exchange];
  if (!config) return;
  const perpspec = config.PERPSPEC;
  let record = null;

  try {
    // BINANCE
    if (exchange === 'BINANCE') {
      const k = rawData.k;
      const ts = apiUtils.toMillis(BigInt(k.t));
      record = { ts, symbol: baseSymbol, source: perpspec, perpspec,
        interval: config.DB_INTERVAL, o: +k.o, h: +k.h, l: +k.l, c: +k.c, v: +k.q };
      if (MT_SYMBOLS.includes(baseSymbol)) mtLatestData.set(baseSymbol, { ts, o: +k.o, h: +k.h, l: +k.l, c: +k.c, v: +k.q });
    }
    // BYBIT
    else if (exchange === 'BYBIT') {
      const k = rawData.data && rawData.data[0];
      if (!k) return;
      record = { ts: BigInt(k.start), symbol: baseSymbol, source: perpspec, perpspec,
        interval: config.DB_INTERVAL, o: +k.open, h: +k.high, l: +k.low, c: +k.close, v: +(k.turnover || 0) };
    }
    // OKX
    else if (exchange === 'OKX') {
      const c = rawData.data && rawData.data[0];
      if (!c) return;
      const ts = apiUtils.toMillis(BigInt(c[0]));
      record = { ts, symbol: baseSymbol, source: perpspec, perpspec,
        interval: config.DB_INTERVAL, o: +c[1], h: +c[2], l: +c[3], c: +c[4], v: +(c[7] || 0) };
    }

    if (!record) return;
    await dbManager.insertData(perpspec, [record]);
    completedSymbols[perpspec].add(baseSymbol);

    const expectedCount = exchange === 'BYBIT' ? bybitActiveSymbols.size : perpList.length;
    if (completedSymbols[perpspec].size === expectedCount) {
      const msg = `${STATUS_COLOR}${perpspec} 1min ohlcv for ${expectedCount} symbols${RESET}`;
      console.log(msg);
      await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'running', msg.replace(STATUS_COLOR, '').replace(RESET, ''));

      if (perpspec === 'bin-ohlcv') {
        const latestTs = Array.from(mtLatestData.values()).find(d => d && d.ts)?.ts;
        if (latestTs) await insertMT('bin-ohlcv', computeMTRecord(latestTs));
      }
      completedSymbols[perpspec].clear();
    }
  } catch (error) {
    await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'INTERNAL', 'INSERT_FAILED', error.message, { perpspec, symbol: baseSymbol });
  }
}

/* ==========================================
 * WEBSOCKETS
 * ========================================== */
async function binanceWebSocket() {
  const config = EXCHANGE_CONFIG.BINANCE;
  for (const baseSymbol of perpList) {
    const ws = new WebSocket(config.getWsUrl(baseSymbol));
    ws.on('open', async () => {
      connectedFlags.BINANCE = true;
      await checkAllConnected();
    });
    ws.on('message', async (d) => {
      const m = JSON.parse(d);
      if (m.k && m.k.x) await processAndInsert('BINANCE', baseSymbol, m);
    });
    ws.on('error', e => console.error(`${ERROR_COLOR}Binance WS error:${RESET}`, e.message));
    ws.on('close', () => setTimeout(() => binanceWebSocket(), 5000));
  }
}

async function bybitWebSocket() {
  const config = EXCHANGE_CONFIG.BYBIT;
  const ws = new WebSocket(config.WS_URL);
  let isConnected = false;

  ws.on('open', async () => {
    isConnected = true;
    connectedFlags.BYBIT = true;
    await checkAllConnected();
    perpList.forEach((sym, i) => {
      const bybitSymbol = config.mapSymbol(sym);
      const msg = { op: 'subscribe', args: [`kline.1.${bybitSymbol}`] };
      setTimeout(() => isConnected && ws.send(JSON.stringify(msg)), i * 50);
    });
  });

  ws.on('message', async (data) => {
    const m = JSON.parse(data);
    if (m.op === 'ping') return ws.send(JSON.stringify({ op: 'pong' }));
    if (!m.data?.length) return;
    if (!m.topic?.startsWith('kline.1.')) return;
    const bybitSymbol = m.topic.split('.')[2];
    const baseSymbol = config.unmapSymbol(bybitSymbol);
    bybitActiveSymbols.add(baseSymbol);
    const k = m.data[0];
    if (k.confirm) await processAndInsert('BYBIT', baseSymbol, m);
  });

  ws.on('error', e => console.error(`${ERROR_COLOR}Bybit WS error:${RESET}`, e.message));
  ws.on('close', () => { isConnected = false; bybitActiveSymbols.clear(); setTimeout(() => bybitWebSocket(), 5000); });
}

async function okxWebSocket() {
  const config = EXCHANGE_CONFIG.OKX;
  const ws = new WebSocket(config.WS_URL);

  ws.on('open', async () => {
    connectedFlags.OKX = true;
    await checkAllConnected();
    const args = perpList.map(sym => ({ channel: 'candle1m', instId: config.mapSymbol(sym) }));
    ws.send(JSON.stringify({ op: 'subscribe', args }));
  });

  ws.on('message', async (data) => {
    const m = JSON.parse(data);
    if (!m.data?.length) return;
    const instId = m.arg?.instId;
    const baseSymbol = perpList.find(sym => config.mapSymbol(sym) === instId);
    if (!baseSymbol) return;
    const c = m.data[0];
    if (c[8] === "1" || c[8] === true) await processAndInsert('OKX', baseSymbol, m);
  });

  ws.on('error', e => console.error(`${ERROR_COLOR}OKX WS error:${RESET}`, e.message));
  ws.on('close', () => setTimeout(() => okxWebSocket(), 5000));
}

/* ==========================================
 * MAIN EXECUTION
 * ========================================== */
async function execute() {
  console.log(`${STATUS_COLOR}✈️ Starting ${SCRIPT_NAME} - WebSocket OHLCV streaming${RESET}`);
  await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'started', `${SCRIPT_NAME} connected`);

  binanceWebSocket();
  bybitWebSocket();
  okxWebSocket();

  process.on('SIGINT', async () => {
    console.log(`\n${STATUS_COLOR}✈️ ${SCRIPT_NAME} received SIGINT, stopping...${RESET}`);
    await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'stopped', `${SCRIPT_NAME} stopped smoothly`);
    process.exit(0);
  });
}

if (require.main === module) {
  execute().catch(err => {
    console.error(`${ERROR_COLOR}💥 WebSocket OHLCV streaming failed:${RESET}`, err);
    apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'SYSTEM', 'INITIALIZATION_FAILED', err.message);
    process.exit(1);
  });
}

module.exports = { execute };