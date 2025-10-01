// Binance Liquidations REST API Test
// Test script to fetch historical liquidation data from Binance USDT Futures

const https = require('https');

// Configuration
const BASE_URL = 'fapi.binance.com';
const ENDPOINT = '/fapi/v1/allForceOrders';

// Parameters
const params = {
  symbol: 'BTCUSDT',  // Trading pair
  limit: 10           // Number of liquidations to fetch (max 1000)
  // startTime: Date.now() - 24*60*60*1000,  // Optional: 24 hours ago
  // endTime: Date.now()                      // Optional: now
};

// Build query string
const queryString = Object.entries(params)
  .map(([key, val]) => `${key}=${val}`)
  .join('&');

// Request options
const options = {
  hostname: BASE_URL,
  path: `${ENDPOINT}?${queryString}`,
  method: 'GET',
  headers: {
    'Content-Type': 'application/json'
  }
};

console.log('\n=== BINANCE LIQUIDATIONS REST API TEST ===\n');
console.log(`Endpoint: https://${BASE_URL}${ENDPOINT}`);
console.log(`Parameters: ${queryString}\n`);

// Make request
const req = https.request(options, (res) => {
  let data = '';

  res.on('data', (chunk) => {
    data += chunk;
  });

  res.on('end', () => {
    try {
      const liquidations = JSON.parse(data);
      
      if (liquidations.code) {
        console.error('Error:', liquidations.msg);
        return;
      }

      console.log(`Total liquidations fetched: ${liquidations.length}\n`);
      
      if (liquidations.length > 0) {
        console.log('Sample Liquidation Data:\n');
        
        // Display first liquidation with detailed info
        const sample = liquidations[0];
        console.log('First Liquidation:');
        console.log('─────────────────────────────────────');
        console.log(`Symbol:           ${sample.symbol}`);
        console.log(`Side:             ${sample.side}`);
        console.log(`Order Type:       ${sample.orderType || sample.type}`);
        console.log(`Time In Force:    ${sample.timeInForce}`);
        console.log(`Original Qty:     ${sample.origQty}`);
        console.log(`Price:            ${sample.price}`);
        console.log(`Avg Price:        ${sample.avgPrice}`);
        console.log(`Order Status:     ${sample.orderStatus || sample.status}`);
        console.log(`Last Filled Qty:  ${sample.lastFilledQty || sample.executedQty}`);
        console.log(`Executed Qty:     ${sample.executedQty}`);
        console.log(`Time:             ${sample.time} (${new Date(sample.time).toISOString()})`);
        console.log(`Update Time:      ${sample.updateTime} (${new Date(sample.updateTime).toISOString()})`);
        console.log('─────────────────────────────────────\n');

        // Show numeric field summary
        console.log('NUMERIC FIELDS SUMMARY (for database storage):');
        console.log('─────────────────────────────────────');
        console.log(`origQty (string):     "${sample.origQty}"`);
        console.log(`price (string):       "${sample.price}"`);
        console.log(`avgPrice (string):    "${sample.avgPrice}"`);
        console.log(`executedQty (string): "${sample.executedQty}"`);
        console.log(`time (number):        ${sample.time}`);
        console.log(`updateTime (number):  ${sample.updateTime}`);
        console.log('─────────────────────────────────────\n');

        // Show all liquidations in compact format
        console.log('All Liquidations (compact view):');
        liquidations.forEach((liq, idx) => {
          console.log(`${idx + 1}. ${liq.symbol} ${liq.side} | Qty: ${liq.origQty} @ ${liq.price} | Time: ${new Date(liq.time).toISOString()}`);
        });
      } else {
        console.log('No liquidations found for the specified parameters.');
      }

    } catch (err) {
      console.error('Error parsing response:', err.message);
      console.log('Raw response:', data);
    }
  });
});

req.on('error', (err) => {
  console.error('Request error:', err.message);
});

req.end();