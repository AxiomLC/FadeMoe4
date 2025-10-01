// Binance Liquidations WebSocket Test
// Real-time liquidation stream from Binance USDT Futures

const WebSocket = require('ws');

// Configuration
const WS_URL = 'wss://fstream.binance.com/ws/btcusdt@forceOrder';
// Alternative for all symbols: 'wss://fstream.binance.com/ws/!forceOrder@arr'

console.log('\n=== BINANCE LIQUIDATIONS WEBSOCKET TEST ===\n');
console.log(`WebSocket URL: ${WS_URL}`);
console.log('Listening for liquidation orders...\n');
console.log('Note: This will wait for real-time liquidations to occur.');
console.log('Press Ctrl+C to stop.\n');

let liquidationCount = 0;

const ws = new WebSocket(WS_URL);

ws.on('open', () => {
  console.log('✓ WebSocket connection established');
  console.log('Waiting for liquidation events...\n');
});

ws.on('message', (data) => {
  try {
    const message = JSON.parse(data);
    
    // Check if this is a forceOrder event
    if (message.e === 'forceOrder') {
      liquidationCount++;
      const order = message.o;
      
      console.log(`\n╔═══════════════════════════════════════════════════════╗`);
      console.log(`║  LIQUIDATION #${liquidationCount} DETECTED`);
      console.log(`╚═══════════════════════════════════════════════════════╝`);
      console.log('Event Type:          ', message.e);
      console.log('Event Time:          ', message.E, `(${new Date(message.E).toISOString()})`);
      console.log('─────────────────────────────────────────');
      console.log('Symbol:              ', order.s);
      console.log('Side:                ', order.S);
      console.log('Order Type:          ', order.o);
      console.log('Time In Force:       ', order.f);
      console.log('Original Quantity:   ', order.q);
      console.log('Price:               ', order.p);
      console.log('Average Price:       ', order.ap);
      console.log('Order Status:        ', order.X);
      console.log('Last Filled Quantity:', order.l);
      console.log('Accumulated Filled:  ', order.z);
      console.log('Order Time:          ', order.T, `(${new Date(order.T).toISOString()})`);
      console.log('─────────────────────────────────────────\n');

      // Show numeric fields for database comparison
      console.log('NUMERIC FIELDS (for database comparison with REST):');
      console.log('─────────────────────────────────────────');
      console.log(`q (origQty) - string:     "${order.q}"`);
      console.log(`p (price) - string:       "${order.p}"`);
      console.log(`ap (avgPrice) - string:   "${order.ap}"`);
      console.log(`z (executedQty) - string: "${order.z}"`);
      console.log(`T (time) - number:        ${order.T}`);
      console.log(`E (eventTime) - number:   ${message.E}`);
      console.log('─────────────────────────────────────────\n');

      // Field mapping between WebSocket and REST
      console.log('WEBSOCKET → REST FIELD MAPPING:');
      console.log('─────────────────────────────────────────');
      console.log('WS: order.s  → REST: symbol');
      console.log('WS: order.S  → REST: side');
      console.log('WS: order.o  → REST: orderType');
      console.log('WS: order.q  → REST: origQty');
      console.log('WS: order.p  → REST: price');
      console.log('WS: order.ap → REST: avgPrice');
      console.log('WS: order.z  → REST: executedQty');
      console.log('WS: order.T  → REST: time');
      console.log('─────────────────────────────────────────\n');

    } else {
      console.log('Received non-liquidation message:', message);
    }
  } catch (err) {
    console.error('Error parsing message:', err.message);
    console.log('Raw data:', data.toString());
  }
});

ws.on('error', (err) => {
  console.error('WebSocket error:', err.message);
});

ws.on('close', () => {
  console.log('\n✗ WebSocket connection closed');
  console.log(`Total liquidations received: ${liquidationCount}`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n\nShutting down...');
  ws.close();
  process.exit(0);
});