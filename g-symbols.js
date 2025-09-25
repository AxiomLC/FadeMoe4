// g-symbols.js
const fs = require('fs');
const perpList = require('./perp-list');

function generateSymbols() {
    const dynamicSymbols = {};

    perpList.forEach(symbol => {
        dynamicSymbols[symbol] = {
            "c-binance": `${symbol}USDT_PERP.A`,    // Coinalyze Binance
            "c-bybit": `${symbol}USDT.6`,           // Coinalyze Bybit
            "c-hyperliquid": `${symbol}.H`,         // Coinalyze Hyperliquid
            "c-okx": `${symbol}USDT_PERP.3`,        // Coinalyze OKX
            "binance_ohlcv": `${symbol}USDT`        // Binance OHLCV (actual Binance format)
        };
    });

    fs.writeFileSync('dynamic-symbols.json', JSON.stringify(dynamicSymbols, null, 2));
    console.log('Dynamic symbols generated successfully');
    console.log(`✅ Generated symbols for ${Object.keys(dynamicSymbols).length} tokens`);
    console.log('📋 Format: c-* = Coinalyze symbols, binance_ohlcv = Binance OHLCV symbols');
}

generateSymbols();