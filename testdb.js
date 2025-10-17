const fs = require('fs');
const dbManager = require('./db/dbsetup');

const SYMBOL = 'SOL';
const LOOKBACK_MINUTES = 10; // Fetch last 10 minutes to ensure full 5 records per perpspec
const PERPSPECS = [
  'bin-ohlcv', 'byb-ohlcv', 'okx-ohlcv',
  'bin-oi', 'byb-oi', 'okx-oi',
  'bin-lsr', 'byb-lsr', 'okx-lsr',
  'bin-pfr', 'byb-pfr', 'okx-pfr',
  'bin-tv', 'byb-tv', 'okx-tv',
  'rsi',
  'bin-lq', 'byb-lq', 'okx-lq' // Included liquidity perpspecs
];

// Fetch data for a perpspec and symbol within time range
async function fetchDataForPerpspec(symbol, perpspec, startTs, endTs) {
  const query = `
    SELECT *
    FROM perp_data
    WHERE symbol = $1
      AND perpspec = $2
      AND ts >= $3
      AND ts <= $4
    ORDER BY ts ASC
  `;
  try {
    const result = await dbManager.pool.query(query, [symbol, perpspec, BigInt(startTs), BigInt(endTs)]);
    return result.rows.map(row => ({
      ...row,
      ts: Number(row.ts),
      utc_time: new Date(Number(row.ts)).toISOString()
    }));
  } catch (error) {
    console.error(`Error fetching data for ${symbol} ${perpspec}:`, error.message);
    return [];
  }
}

// Generate HTML table from data
function generateHtmlTable(data) {
  if (data.length === 0) {
    return '<p>No data available.</p>';
  }

  const excludeCols = new Set(['source', 'interval']);

  // Collect all unique columns except excluded
  const columnsSet = new Set();
  data.forEach(row => {
    Object.keys(row).forEach(key => {
      if (!excludeCols.has(key)) columnsSet.add(key);
    });
  });
  let columns = Array.from(columnsSet);

  // Ensure ts and utc_time are first columns
  columns = columns.filter(c => c !== 'ts' && c !== 'utc_time');
  columns.unshift('utc_time');
  columns.unshift('ts');

  // Build table header
  let html = '<table id="perpspecTable" class="display" style="width:100%">\n<thead>\n<tr>';
  columns.forEach(col => {
    html += `<th>${col}</th>`;
  });
  html += '</tr>\n</thead>\n<tbody>\n';

  // Build rows
  data.forEach(row => {
    html += '<tr>';
    columns.forEach(col => {
      let cell = row[col];
      if (cell === null || cell === undefined) cell = '';
      html += `<td>${cell}</td>`;
    });
    html += '</tr>\n';
  });

  html += '</tbody>\n</table>';
  return html;
}

async function main() {
  const now = Date.now();
  const startTs = now - LOOKBACK_MINUTES * 60 * 1000;
  const endTs = now;

  let allData = [];

  for (const perpspec of PERPSPECS) {
    const data = await fetchDataForPerpspec(SYMBOL, perpspec, startTs, endTs);
    // Keep only last 5 records per perpspec to ensure full 5 records
    const lastFive = data.slice(-5);
    allData = allData.concat(lastFive);
  }

  // Sort by perpspec ascending, then ts ascending
  allData.sort((a, b) => {
    if (a.perpspec < b.perpspec) return -1;
    if (a.perpspec > b.perpspec) return 1;
    return a.ts - b.ts;
  });

  // Technical summary paragraph
  const techSummary = `
    <p><strong>HTML example table of 5min window to DB. Technical Summary:</strong> This database stores perpetual futures data from 3 exchanges (currently) organized by "schema" <em>perpspec</em> (e.g., bin-ohlcv, byb-lsr). 
    All data timestamps are normalized to 1-minute intervals per perpspec, making the <code>interval</code> and <code>source</code> fields redundant for analysis and thus excluded here.
    The <code>perp_metrics</code> table currently stores pre-calculated metrics of perp_data columns only, not organized by schema/perpspecs. Other perpspecs besides ohlcv, could be stored as <code>NUMERIC(10,4)</code> for efficiency.</p>
  `;

  const htmlTable = generateHtmlTable(allData);

  const htmlContent = `
  <!DOCTYPE html>
  <html lang="en">
  <head>
    <meta charset="UTF-8" />
    <title>Perpspec Data for ${SYMBOL} Last 5 Records Each</title>
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.5/css/jquery.dataTables.min.css"/>
    <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.5/js/jquery.dataTables.min.js"></script>
    <style>
      body { font-family: Arial, sans-serif; padding: 20px; }
      h1 { margin-bottom: 10px; }
      p { max-width: 900px; }
    </style>
  </head>
  <body>
    <h1>Perpspec Data for ${SYMBOL} - Last 5 Records Each</h1>
    ${techSummary}
    ${htmlTable}
    <script>
      $(document).ready(function() {
        $('#perpspecTable').DataTable({
          order: [[0, 'asc']], // sort by ts ascending
          paging: false,
          scrollX: true
        });
      });
    </script>
  </body>
  </html>
  `;

  const outputPath = `./perpspecs_${SYMBOL}_last5records_each_sortable.html`;
  fs.writeFileSync(outputPath, htmlContent);
  console.log(`Sortable HTML table saved to ${outputPath}`);
}

main().catch(err => {
  console.error('Error in test fetch:', err);
  process.exit(1);
});