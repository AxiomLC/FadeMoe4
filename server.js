const express = require('express');
const dbManager = require('./db/dbsetup');
const path = require('path');
const app = express();
const PORT = process.env.PORT || 3000;

// ============================================================================
// Middleware
// ============================================================================
app.use(express.json());
app.use(express.static('public'));

// ============================================================================
// API Endpoints
// ============================================================================

// Get all unique symbols from perp_data table
app.get('/api/symbols', async (req, res) => {
    try {
        const result = await dbManager.pool.query(
            'SELECT DISTINCT symbol FROM perp_data ORDER BY symbol'
        );
        res.json(result.rows.map(row => row.symbol));
    } catch (error) {
        console.error('Error fetching symbols:', error.message);
        res.status(500).json({ error: 'Failed to fetch symbols' });
    }
});

// Get all unique exchanges from perp_data table
app.get('/api/exchanges', async (req, res) => {
    try {
        const result = await dbManager.pool.query(
            'SELECT DISTINCT exchange FROM perp_data ORDER BY exchange'
        );
        res.json(result.rows.map(row => row.exchange));
    } catch (error) {
        console.error('Error fetching exchanges:', error.message);
        res.status(500).json({ error: 'Failed to fetch exchanges' });
    }
});

// Get all available columns (params) from perp_data table
app.get('/api/params', async (req, res) => {
    try {
        const params = [
            'ts', 'symbol', 'exchange',
            'o', 'h', 'l', 'c', 'v', 'oi', 'pfr', 'lsr', 
            'rsi1', 'rsi60', 'tbv', 'tsv', 'lqside', 'lqprice', 'lqqty'
        ];
        res.json(params);
    } catch (error) {
        console.error('Error fetching params:', error.message);
        res.status(500).json({ error: 'Failed to fetch params' });
    }
});

// Get paginated perp_data records based on filters
app.get('/api/perp_data', async (req, res) => {
    try {
        const {
            page = 1,
            limit = 100,
            symbol = '',
            exchange = '',
            params = ''
        } = req.query;

        console.log('Received /api/perp_data query params:', req.query);

        const pageNum = parseInt(page) || 1;
        const pageSize = parseInt(limit) || 100;
        const offset = (pageNum - 1) * pageSize;

        // Build WHERE clause
        let whereClause = '';
        const values = [];
//==========================================================================
        if (symbol && symbol !== '' && symbol !== 'all') {
    const symbols = symbol.split(',').map(s => s.trim()).filter(s => s.length > 0);
    if (symbols.length > 0) {
        const placeholders = symbols.map((_, i) => `$${values.length + i + 1}`).join(', ');
        whereClause += ` AND symbol IN (${placeholders})`;
        values.push(...symbols);
    }
}

if (exchange && exchange !== '' && exchange !== 'all') {
    const exchanges = exchange.split(',').map(e => e.trim()).filter(e => e.length > 0);
    if (exchanges.length > 0) {
        const placeholders = exchanges.map((_, i) => `$${values.length + i + 1}`).join(', ');
        whereClause += ` AND exchange IN (${placeholders})`;
        values.push(...exchanges);
    }
}


        // Valid parameters
        const validParams = ['ts', 'symbol', 'exchange', 'o', 'h', 'l', 'c', 'v', 'oi', 'pfr', 'lsr', 'rsi1', 'rsi60', 'tbv', 'tsv', 'lqside', 'lqprice', 'lqqty'];

        // Build SELECT clause
        let selectFields = 'ts, symbol, exchange, o, h, l, c, v, oi, pfr, lsr, rsi1, rsi60, tbv, tsv, lqside, lqprice, lqqty';
        let visibleColumns = validParams;
        
        if (params && params !== '' && params !== 'all') {
            const selectedParams = params.split(',');
            const filteredParams = selectedParams.filter(param => validParams.includes(param.trim()));
            if (filteredParams.length > 0) {
                selectFields = filteredParams.join(', ');
                visibleColumns = filteredParams;
            }
        }

        // Count total records
        let countQuery = `SELECT COUNT(*) FROM perp_data WHERE 1=1 ${whereClause}`;
        const countResult = await dbManager.pool.query(countQuery, values);
        const totalRecords = parseInt(countResult.rows[0].count);
        const totalPages = Math.ceil(totalRecords / pageSize);

        // Get paginated data - SORTED BY ts DESC, symbol, exchange
        // Add pagination placeholders correctly
values.push(pageSize, offset);
let limitIndex = values.length - 1;     // pageSize
let offsetIndex = values.length;        // offset

let dataQuery = `SELECT ${selectFields} FROM perp_data WHERE 1=1 ${whereClause}
ORDER BY ts DESC, symbol, exchange
LIMIT $${limitIndex} OFFSET $${offsetIndex}`;

        
        console.log('Executing SQL:', dataQuery, 'with values:', values);
        const dataResult = await dbManager.pool.query(dataQuery, values);
        
        // Process data - convert BigInt timestamps to numbers
        const processedData = dataResult.rows.map(row => {
            const processedRow = { ...row };
            // Convert BigInt timestamp to number (milliseconds)
            if (processedRow.ts !== null && processedRow.ts !== undefined) {
                processedRow.ts = Number(processedRow.ts);
            }
            return processedRow;
        });
        
        res.json({
            data: processedData,
            pagination: {
                currentPage: pageNum,
                totalPages: totalPages,
                totalRecords: totalRecords,
                pageSize: pageSize
            },
            visibleColumns: visibleColumns
        });
    } catch (error) {
        console.error('Error fetching perp_data:', error.message);
        res.status(500).json({ error: 'Failed to fetch perp_data' });
    }
});

// System Summary Endpoint
app.get('/api/system-summary', async (req, res) => {
    try {
        const latestStatusResult = await dbManager.pool.query(
            `SELECT script_name, status, message, ts FROM perp_status ORDER BY ts DESC LIMIT 10`
        );
        
        const rows = latestStatusResult.rows || [];
        const runningScripts = rows.filter(r => r && r.status === 'running').map(r => r.script_name);
        const recentStatus = rows.slice(0, 5);

        const recentErrors = await dbManager.pool.query(
            `SELECT script_name, error_type, error_message, ts FROM perp_errors ORDER BY ts DESC LIMIT 10`
        );

        const errorRows = recentErrors.rows || [];

        let statusText = `<div id="status-flex-container" style="display:flex; gap:20px; max-height:250px; background-color:#1e1e2f; padding:0; border-radius:8px; color:#d1d5db; overflow:hidden;">`;

        statusText += `<div id="status-left" style="flex:1 1 60%; background-color:#000; padding:5px; border:none; overflow-y:auto; max-height:200px;">`;
        statusText += `<div style="margin-bottom: 15px;">`;
        statusText += `<h3 style="color: #9f59ff; margin: 0 0 10px 0;">📊 Current Operations</h3>`;
        if (runningScripts && runningScripts.length > 0) {
            runningScripts.forEach(script => {
                statusText += `<div style="padding-left: 15px; margin-top: 3px; color: #fbbf24; font-size: 14px;">• ${script}</div>`;
            });
        } else {
            statusText += `<div style="color: #d1d5db;">No scripts currently running.</div>`;
        }
        statusText += `</div>`;

        if (recentStatus && recentStatus.length > 0) {
            statusText += `<div style="margin-bottom: 15px;">`;
            statusText += `<strong style="color: #b569ff;">Latest Activity:</strong>`;
            recentStatus.slice(0, 5).forEach(op => {
                let statusColor = '#d1d5db';
                if (op && op.status === 'success') statusColor = '#4ade80';
                else if (op && op.status === 'running') statusColor = '#fbbf24';
                else if (op && op.status === 'error') statusColor = '#f87171';

                if (op && op.ts) {
                    const ts = new Date(op.ts);
                    const timeStr = ts.toISOString().substr(11, 8) + ' UTC';

                    statusText += `<div style="padding-left: 15px; margin-top: 3px; color: ${statusColor}; font-size: 14px;">`;
                    statusText += `• ${op.script_name || 'Unknown'} [${timeStr}]: ${op.message || op.status || 'No message'}`;
                    statusText += `</div>`;
                }
            });
            statusText += `</div>`;
        }

        statusText += `</div>`;

        statusText += `<div id="status-right" style="flex:1 1 40%; background-color:#000; border:none; border-left: 1px solid rgba(159, 89, 255, 0.3); padding:5px; overflow-y:auto; max-height:200px;">`;

        if (errorRows && errorRows.length > 0) {
            statusText += `<h4 style="color: #f87171; margin: 0 0 10px 0;">⚠️ Recent Issues</h4>`;

            errorRows.slice(0, 3).forEach(error => {
                if (error && error.ts) {
                    const ts = new Date(error.ts);
                    const timeStr = ts.toISOString().substr(11, 8) + ' UTC';

                    statusText += `<div style="margin-bottom: 8px; padding-left: 10px;">`;
                    statusText += `<strong style="color: #fbbf24;">${error.script_name || 'Unknown'} [${timeStr}]:</strong> ${error.error_message || 'Unknown error'}<br>`;
                    statusText += `</div>`;
                }
            });
        } else {
            statusText += `<div style="color: #d1d5db;">No recent issues.</div>`;
        }

        statusText += `</div>`;
        statusText += `</div>`;

        if ((!runningScripts || runningScripts.length === 0) && (!recentStatus || recentStatus.length === 0) && (!errorRows || errorRows.length === 0)) {
            statusText = `<div style="text-align: center; padding: 20px;">`;
            statusText += `<h3 style="color: #9f59ff; margin: 0 0 10px 0;">🌟 System Ready</h3>`;
            statusText += `<p style="color: #d1d5db; margin: 0;">No recent activity. Data collection scripts are ready to run.</p>`;
            statusText += `</div>`;
        }

        res.json({
            statusText: statusText,
            errors: errorRows,
            operations: recentStatus
        });

    } catch (error) {
        console.error('API Error: Failed to fetch system summary:', error.message);
        res.status(500).json({ error: 'Failed to fetch system summary' });
    }
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// ============================================================================
// Server Startup
// ============================================================================
app.listen(PORT, () => {
    console.log(`🚀 FadeMoe4 Server running on http://localhost:${PORT}`);
    console.log(`📊 Database viewer available at http://localhost:${PORT}`);
});