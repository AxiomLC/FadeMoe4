// web-ohlcv-c.js
// ------------------------------------------------------------
// Websocket OHLCV collector for Binance (works), Bybit, OKX
// - Resilient parsing for each exchange's WS payload shape
// - Ensures ts is BigInt for DB insertion (record.ts.toString())
// - Keeps perpspec/source mapping from dynamicSymbols.json
// - Defensive: ignores subscribe/pong messages; logs parse errors
// ------------------------------------------------------------

const WebSocket = require('ws');
const fs = require('fs');
const apiUtils = require('../api-utils');
const dbManager = require('../db/dbsetup');
require('dotenv').config();

const SCRIPT_NAME = 'web-ohlcv-c.js';

// Load dynamic symbols mapping base symbols to exchange-specific symbols
const dynamicSymbols = JSON.parse(fs.readFileSync('dynamic-symbols.json', 'utf8'));

// Exchange config: maps exchange to DB schema, dynamicSymbols key for API source, and perpspec name
const EXCHANGES = {
    binance: { sourceKey: 'bin-ohlcv', perpspec: 'bin-ohlcv' },
    bybit: { sourceKey: 'byb-ohlcv', perpspec: 'byb-ohlcv' },
    okx: { sourceKey: 'okx-ohlcv', perpspec: 'okx-ohlcv' }
};

/**
 * Processes raw candle data from WebSocket and inserts into DB.
 * Sets source and perpspec based on exchange config.
 * Uses baseSymbol as canonical symbol in DB.
 */
async function processAndInsert(exchange, baseSymbol, rawData) {
    const config = EXCHANGES[exchange];
    if (!config) {
        console.error(`Unsupported exchange: ${exchange}`);
        return;
    }

    const symbolValue = dynamicSymbols[baseSymbol]?.[config.sourceKey];
    if (!symbolValue) {
        console.warn(`⚠️ No symbol value for ${baseSymbol} ${config.sourceKey}`);
        return;
    }

    const apiSource = config.sourceKey;
    const perpspec = config.perpspec;
    let record = null;
    let ts = null;

    try {
        if (exchange === 'binance') {
            // existing working code kept intact
            const k = rawData.k;
            ts = normalizeTimestamp(k.t);
            if (!ts) return;

            // ensure BigInt ts
            ts = BigInt(String(ts));

            record = {
                ts,
                symbol: baseSymbol,
                source: apiSource,
                perpspec,
                interval: '1min',
                o: parseFloat(k.o),
                h: parseFloat(k.h),
                l: parseFloat(k.l),
                c: parseFloat(k.c),
                v: parseFloat(k.v)
            };
        } else if (exchange === 'bybit') {
            // Bybit V5 kline payload: { topic: "kline.1.BTCUSDT", data: [ { start, end, interval, open, close, high, low, volume, turnover, confirm, timestamp } ] }
            const k = rawData.data && rawData.data[0];
            if (!k) {
                // nothing to do
                return;
            }

            // Bybit `start` is already ms per Bybit docs -> do not multiply by 1000
            ts = normalizeTimestamp(k.start);
            if (!ts) return;
            ts = BigInt(String(ts));

            record = {
                ts,
                symbol: baseSymbol,
                source: apiSource,
                perpspec,
                interval: '1min',
                o: parseFloat(k.open),
                h: parseFloat(k.high),
                l: parseFloat(k.low),
                c: parseFloat(k.close),
                v: parseFloat(k.volume || k.turnover || 0)
            };
        } else if (exchange === 'okx') {
            // OKX WS candle payload: message.arg.instId is instrument; data[0] is an array [ts, o, h, l, c, vol, confirm?]
            const c = rawData.data && rawData.data[0];
            if (!c) return;

            // c[0] is timestamp in ms (string or number)
            ts = normalizeTimestamp(c[0]);
            if (!ts) return;
            ts = BigInt(String(ts));

            // Volume index typically at c[5]; confirm flag sometimes at c[6]
            const vol = (c.length > 5 && c[5] !== undefined) ? c[5] : 0;
            record = {
                ts,
                symbol: baseSymbol,
                source: apiSource,
                perpspec,
                interval: '1min',
                o: parseFloat(c[1]),
                h: parseFloat(c[2]),
                l: parseFloat(c[3]),
                c: parseFloat(c[4]),
                v: parseFloat(vol)
            };
        } else {
            console.error(`Unsupported exchange: ${exchange}`);
            return;
        }

        await insertRecord(record, exchange, baseSymbol, ts);
    } catch (e) {
        console.error(`Error processing candle for ${exchange} ${baseSymbol}:`, e && e.message ? e.message : e);
    }
}

/**
 * Normalize timestamp using apiUtils.toMillis.
 * Returns null if error occurs.
 */
function normalizeTimestamp(ts) {
    try {
        return apiUtils.toMillis(ts);
    } catch (e) {
        console.error(`⚠️ Timestamp normalization error:`, e && e.message ? e.message : e);
        return null;
    }
}

/**
 * Inserts or updates OHLCV record in the database.
 * Uses UPSERT on (ts, symbol, source) to avoid duplicates.
 * Logs success or error via apiUtils.
 */
async function insertRecord(record, exchange, baseSymbol, ts) {
    try {
        const query = `
            INSERT INTO perp_data (ts, symbol, source, perpspec, interval, o, h, l, c, v)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (ts, symbol, source) DO UPDATE SET
                o = EXCLUDED.o,
                h = EXCLUDED.h,
                l = EXCLUDED.l,
                c = EXCLUDED.c,
                v = EXCLUDED.v
        `;

        await dbManager.pool.query(query, [
            record.ts.toString(),
            record.symbol,
            record.source,
            record.perpspec,
            record.interval,
            record.o,
            record.h,
            record.l,
            record.c,
            record.v
        ]);

        await apiUtils.logScriptStatus(dbManager, SCRIPT_NAME, 'success', `Inserted OHLCV for ${baseSymbol} on ${exchange}`, {
            ts: record.ts.toString()
        });

        console.log(`✅ ${exchange} ${baseSymbol} OHLCV inserted at ${new Date(Number(record.ts)).toISOString()}`);
    } catch (error) {
        console.error(`❌ DB insert error for ${exchange} ${baseSymbol}:`, error && error.message ? error.message : error);
        await apiUtils.logScriptError(dbManager, SCRIPT_NAME, 'DB', 'INSERT_ERROR', error && error.message ? error.message : error, {
            exchange,
            symbol: baseSymbol
        });
    }
}

/**
 * Starts Binance WebSocket connections for all symbols with 'bin-ohlcv' key.
 * Subscribes to 1m kline streams.
 */
async function binanceWebSocket() {
    try {
        const symbols = Object.keys(dynamicSymbols).filter(sym => dynamicSymbols[sym].hasOwnProperty('bin-ohlcv'));
        symbols.forEach(baseSymbol => {
            const wsSymbol = dynamicSymbols[baseSymbol]['bin-ohlcv'];
            const ws = new WebSocket(`wss://fstream.binance.com/ws/${wsSymbol.toLowerCase()}@kline_1m`);

            ws.on('open', () => {
                console.log(`Binance websocket opened for ${baseSymbol}`);
            });

            ws.on('message', async (data) => {
                try {
                    const message = JSON.parse(data);
                    if (message.k && message.k.x) {
                        await processAndInsert('binance', baseSymbol, message);
                        console.log(`✅ Binance ${baseSymbol} candle closed at ${new Date(Number(message.k.t)).toISOString()}`);
                    }
                } catch (e) {
                    console.error(`Binance message parse error for ${baseSymbol}:`, e && e.message ? e.message : e);
                }
            });

            ws.on('error', (error) => {
                console.error(`Binance WebSocket error for ${baseSymbol}:`, error && error.message ? error.message : error);
                setTimeout(binanceWebSocket, 5000);
            });

            ws.on('close', () => {
                console.log(`Binance websocket closed for ${baseSymbol}`);
            });
        });
    } catch (error) {
        console.error('Binance WebSocket connection error:', error && error.message ? error.message : error);
        setTimeout(binanceWebSocket, 10000);
    }
}

/**
 * Starts Bybit WebSocket connection subscribing to all symbols with 'byb-ohlcv' key.
 */
async function bybitWebSocket() {
    try {
        const symbols = Object.keys(dynamicSymbols).filter(sym => dynamicSymbols[sym].hasOwnProperty('byb-ohlcv'));
        if (symbols.length === 0) {
            console.log('No Bybit symbols defined in dynamic-symbols.json');
            return;
        }

        const ws = new WebSocket('wss://stream.bybit.com/v5/public/linear');

        ws.on('open', () => {
            const args = symbols.map(sym => `kline.1.${dynamicSymbols[sym]['byb-ohlcv']}`);
            ws.send(JSON.stringify({ op: 'subscribe', args }));
            console.log(`Bybit websocket subscribed to: ${args.join(', ')}`);
        });

        ws.on('message', async (data) => {
            try {
                const message = JSON.parse(data);

                // ignore non-data messages (subscribe ack, heartbeats)
                if (!message.data || !Array.isArray(message.data) || message.data.length === 0) return;

                // topic contains the symbol: "kline.1.BTCUSDT"
                const topic = message.topic || '';
                const topicParts = topic.split('.');
                const symbolFromTopic = topicParts[2]; // BTCUSDT

                // find baseSymbol mapping from dynamicSymbols
                const baseSymbol = Object.keys(dynamicSymbols).find(sym => dynamicSymbols[sym]['byb-ohlcv'] === symbolFromTopic);
                if (!baseSymbol) return;

                // Bybit data is an array, take the first kline object
                const kline = message.data[0];
                // confirm === true means candle closed (per Bybit docs)
                if (kline && (kline.confirm === true)) {
                    // pass the whole message so processAndInsert sees the expected structure
                    await processAndInsert('bybit', baseSymbol, message);
                    // kline.start is ms (no multiply)
                    console.log(`✅ Bybit ${baseSymbol} candle closed at ${new Date(Number(kline.start)).toISOString()}`);
                }
            } catch (e) {
                console.error('Bybit message parse error:', e && e.message ? e.message : e);
            }
        });

        ws.on('error', (error) => {
            console.error('Bybit WebSocket error:', error && error.message ? error.message : error);
            setTimeout(bybitWebSocket, 5000);
        });

        ws.on('close', () => {
            console.log('Bybit websocket connection closed');
        });
    } catch (error) {
        console.error('Bybit WebSocket connection error:', error && error.message ? error.message : error);
        setTimeout(bybitWebSocket, 10000);
    }
}

/**
 * Starts OKX WebSocket connection subscribing to all symbols with 'okx-ohlcv' key.
 * Now uses 'candle1m' channel for trade candles (includes volume).
 */
async function okxWebSocket() {
    try {
        const symbols = Object.keys(dynamicSymbols).filter(sym => dynamicSymbols[sym].hasOwnProperty('okx-ohlcv'));
        if (symbols.length === 0) {
            console.log('No OKX symbols defined in dynamic-symbols.json');
            return;
        }

        // Using public endpoint for trade candles (candle1m)
        const ws = new WebSocket('wss://ws.okx.com:8443/ws/v5/business');

        ws.on('open', () => {
            // Subscribe to candle1m channel (trade candles with volume)
            const args = symbols.map(sym => ({ 
                channel: 'candle1m', 
                instId: dynamicSymbols[sym]['okx-ohlcv'] 
            }));
            ws.send(JSON.stringify({ op: 'subscribe', args }));
            console.log(`OKX websocket subscribed to candle1m: ${args.map(a => a.instId).join(', ')}`);
        });

        ws.on('message', async (data) => {
            try {
                const message = JSON.parse(data);

                // Ignore non-data messages (subscribe confirmations, etc.)
                if (!message.data || !Array.isArray(message.data) || message.data.length === 0) return;

                // message.arg.instId is the instrument id (e.g. BTC-USDT-SWAP)
                const instId = message.arg && message.arg.instId;
                if (!instId) return;

                // Locate baseSymbol by matching dynamicSymbols[*]['okx-ohlcv'] === instId
                const baseSymbol = Object.keys(dynamicSymbols).find(sym => dynamicSymbols[sym]['okx-ohlcv'] === instId);
                if (!baseSymbol) return;

                const c = message.data[0];
                // OKX candle1m array format: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
                // c[5] = volume in contracts
                // c[8] = confirm boolean (true = candle closed)
                const confirm = (c.length > 8) ? c[8] : undefined;
                
                // Only process closed candles (confirm === "1" as string or true as boolean)
                if (confirm === "1" || confirm === true) {
                    await processAndInsert('okx', baseSymbol, message);
                    console.log(`✅ OKX ${baseSymbol} candle closed at ${new Date(Number(c[0])).toISOString()}`);
                }
            } catch (e) {
                console.error('OKX message parse error:', e && e.message ? e.message : e);
            }
        });

        ws.on('error', (error) => {
            console.error('OKX WebSocket error:', error && error.message ? error.message : error);
            setTimeout(okxWebSocket, 5000);
        });

        ws.on('close', () => {
            console.log('OKX websocket connection closed');
        });
    } catch (error) {
        console.error('OKX WebSocket connection error:', error && error.message ? error.message : error);
        setTimeout(okxWebSocket, 10000);
    }
}

/**
 * Starts all WebSocket connections for Binance, Bybit, and OKX.
 */
async function startAllConnections() {
    console.log('🚀 Starting all websocket OHLCV connections...');
    binanceWebSocket();
    bybitWebSocket();
    okxWebSocket();
}

module.exports = { execute: startAllConnections };

if (require.main === module) {
    startAllConnections()
        .then(() => {
            console.log('🏁 web-ohlcv-c.js started successfully, websockets running');
        })
        .catch(error => {
            console.error('💥 web-ohlcv-c.js failed:', error && error.message ? error.message : error);
            process.exit(1);
        });
}
