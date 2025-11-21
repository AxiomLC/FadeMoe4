/*
  bt/debug.js - Minimal Data Inspector
  
  Checks what's actually in perp_metrics for a symbol/exchange/param
  
  Usage: node bt/debug.js
*/

const dbManager = require('../db/dbsetup');

const CONFIG = {
  symbol: 'ETH',
  exchange: 'bin',
  param: 'c_chg_5m'
};

(async () => {
  console.log('🔍 DEBUG: Inspecting perp_metrics');
  console.log('='.repeat(70));
  
  try {
    // Query 1: Total rows for this symbol/exchange
    console.log(`\n1️⃣ Total rows for ${CONFIG.symbol}_${CONFIG.exchange}:`);
    const countQuery = `
      SELECT COUNT(*) as total
      FROM perp_metrics
      WHERE symbol = $1 AND exchange = $2
    `;
    const { rows: countRows } = await dbManager.pool.query(countQuery, [CONFIG.symbol, CONFIG.exchange]);
    console.log(`   Total: ${countRows[0].total}`);
    
    // Query 2: Rows with valid c and param
    console.log(`\n2️⃣ Rows with valid c AND ${CONFIG.param}:`);
    const validQuery = `
      SELECT COUNT(*) as total
      FROM perp_metrics
      WHERE symbol = $1 
        AND exchange = $2
        AND c IS NOT NULL 
        AND c > 0
        AND ${CONFIG.param} IS NOT NULL
    `;
    const { rows: validRows } = await dbManager.pool.query(validQuery, [CONFIG.symbol, CONFIG.exchange]);
    console.log(`   Valid: ${validRows[0].total}`);
    
    // Query 3: Sample recent rows (DESC order - newest first)
    console.log(`\n3️⃣ Most recent 10 rows (DESC):`);
    const recentQuery = `
      SELECT ts, c, ${CONFIG.param}
      FROM perp_metrics
      WHERE symbol = $1 AND exchange = $2
      ORDER BY ts DESC
      LIMIT 10
    `;
    const { rows: recentRows } = await dbManager.pool.query(recentQuery, [CONFIG.symbol, CONFIG.exchange]);
    recentRows.forEach((r, i) => {
      const date = new Date(Number(r.ts)).toISOString().slice(0, 19);
      const paramVal = r[CONFIG.param] !== null ? parseFloat(r[CONFIG.param]).toFixed(2) : 'NULL';
      console.log(`   ${i+1}. ${date} | c=${r.c} | ${CONFIG.param}=${paramVal}`);
    });
    
    // Query 4: Sample oldest rows (ASC order - oldest first)
    console.log(`\n4️⃣ Oldest 10 rows (ASC):`);
    const oldestQuery = `
      SELECT ts, c, ${CONFIG.param}
      FROM perp_metrics
      WHERE symbol = $1 AND exchange = $2
      ORDER BY ts ASC
      LIMIT 10
    `;
    const { rows: oldestRows } = await dbManager.pool.query(oldestQuery, [CONFIG.symbol, CONFIG.exchange]);
    oldestRows.forEach((r, i) => {
      const date = new Date(Number(r.ts)).toISOString().slice(0, 19);
      const paramVal = r[CONFIG.param] !== null ? parseFloat(r[CONFIG.param]).toFixed(2) : 'NULL';
      console.log(`   ${i+1}. ${date} | c=${r.c} | ${CONFIG.param}=${paramVal}`);
    });
    
    // Query 5: Param value distribution
    console.log(`\n5️⃣ ${CONFIG.param} value distribution (where NOT NULL):`);
    const distQuery = `
      SELECT 
        MIN(${CONFIG.param}) as min,
        MAX(${CONFIG.param}) as max,
        AVG(${CONFIG.param}) as avg,
        COUNT(*) as count
      FROM perp_metrics
      WHERE symbol = $1 
        AND exchange = $2
        AND ${CONFIG.param} IS NOT NULL
    `;
    const { rows: distRows } = await dbManager.pool.query(distQuery, [CONFIG.symbol, CONFIG.exchange]);
    const dist = distRows[0];
    console.log(`   Min: ${dist.min}`);
    console.log(`   Max: ${dist.max}`);
    console.log(`   Avg: ${parseFloat(dist.avg).toFixed(2)}`);
    console.log(`   Count: ${dist.count}`);
    
    // Query 6: Count rows where param > 0.1
    console.log(`\n6️⃣ Rows where ${CONFIG.param} > 0.1:`);
    const thresholdQuery = `
      SELECT COUNT(*) as count
      FROM perp_metrics
      WHERE symbol = $1 
        AND exchange = $2
        AND ${CONFIG.param} > 0.1
    `;
    const { rows: thresholdRows } = await dbManager.pool.query(thresholdQuery, [CONFIG.symbol, CONFIG.exchange]);
    console.log(`   Count: ${thresholdRows[0].count}`);
    
    // Query 7: Sample 10 rows where param > 0.1
    if (parseInt(thresholdRows[0].count) > 0) {
      console.log(`\n7️⃣ Sample 10 rows where ${CONFIG.param} > 0.1:`);
      const sampleQuery = `
        SELECT ts, c, ${CONFIG.param}
        FROM perp_metrics
        WHERE symbol = $1 
          AND exchange = $2
          AND ${CONFIG.param} > 0.1
        ORDER BY ts DESC
        LIMIT 10
      `;
      const { rows: sampleRows } = await dbManager.pool.query(sampleQuery, [CONFIG.symbol, CONFIG.exchange]);
      sampleRows.forEach((r, i) => {
        const date = new Date(Number(r.ts)).toISOString().slice(0, 19);
        console.log(`   ${i+1}. ${date} | c=${r.c} | ${CONFIG.param}=${parseFloat(r[CONFIG.param]).toFixed(2)}`);
      });
    }
    
    console.log('\n' + '='.repeat(70));
    console.log('✅ Debug complete');
    console.log('='.repeat(70));
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    console.error(error.stack);
  } finally {
    await dbManager.close();
  }
})();