// g-symbols.js
const fs = require('fs');
const perpList = require('./perp-list');

function generateSymbols() {
    const dynamicSymbols = {};

    perpList.forEach(symbol => {
        dynamicSymbols[symbol] = {
            "c-binance": `${symbol}USDT_PERP.A`,
            "c-bybit": `${symbol}USDT.6`,
            "c-hyperliquid": `${symbol}.H`,
            "c-okx": `${symbol}USDT_PERP.3`,
            "bin-ohlcv": `${symbol}USDT`,
            "byb-ohlcv": `${symbol}USDT`,
            "okx-ohlcv": `${symbol}-USDT-SWAP`,
            "bin-oi": `${symbol}USDT`,
            "byb-oi": `${symbol}USDT`,
            "okx-oi": `${symbol}-USDT-SWAP`,
            "bin-pfr": `${symbol}USDT`,
            "byb-pfr": `${symbol}USDT`,
            "okx-pfr": `${symbol}-USDT-SWAP`,
            "bin-lq": `${symbol}USDT`,
            "byb-lq": `${symbol}USDT`,
            "okx-lq": `${symbol}-USDT-SWAP`,
            "bin-lsr": `${symbol}USDT`,
            "byb-lsr": `${symbol}USDT`,
            "okx-lsr": `${symbol}-USDT-SWAP`,

        };
    });

    fs.writeFileSync('dynamic-symbols.json', JSON.stringify(dynamicSymbols, null, 2));
    console.log('Dynamic symbols generated successfully');
    console.log(`✅ Generated symbols for ${Object.keys(dynamicSymbols).length} tokens`);
    console.log('📋 Format: c-* = Coinalyze symbols, *-ohlcv = unified OHLCV symbols');
}

generateSymbols();