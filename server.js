const express = require('express');
const dbManager = require('./db/dbsetup');
const path = require('path');
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.static('public'));
app.use(express.json());

app.get('/api/perpspecs', async (req, res) => {
    try {
        const result = await dbManager.pool.query(`
            SELECT perpspec_name, fields FROM perpspec_schema
        `);
        res.json(result.rows);
    } catch (error) {
        console.error('Error fetching perpspecs:', error);
        res.status(500).json({ error: 'Failed to fetch perpspecs' });
    }
});

app.get('/api/data/:perpspec', async (req, res) => {
    try {
        const { perpspec } = req.params;
        const limit = parseInt(req.query.limit) || 100;
        const offset = parseInt(req.query.offset) || 0;
        const schemaInfo = await dbManager.pool.query(`
            SELECT fields FROM perpspec_schema WHERE perpspec_name = $1
        `, [perpspec]);

        if (schemaInfo.rows.length === 0) {
            return res.status(400).json({ error: `Perpspec '${perpspec}' not found.` });
        }

        const fields = schemaInfo.rows[0].fields;
        if (!fields || fields.length === 0) {
            return res.status(400).json({ error: `No fields defined for perpspec '${perpspec}'.` });
        }

        const coreColumns = ['ts', 'symbol', 'source', 'interval'];
        const selectColumns = [...new Set([...coreColumns, ...fields])].join(', ');

        const dataQuery = `
            SELECT ${selectColumns} FROM perp_data
            WHERE perpspec = $3
            ORDER BY ts DESC
            LIMIT $1 OFFSET $2
        `;

        const result = await dbManager.pool.query(dataQuery, [limit, offset, perpspec]);

        const countQuery = `
            SELECT COUNT(*) FROM perp_data WHERE perpspec = $1
        `;
        const countResult = await dbManager.pool.query(countQuery, [perpspec]);
        const totalRecords = parseInt(countResult.rows[0].count);
        const totalPages = Math.ceil(totalRecords / limit);

        res.json({
            data: result.rows,
            total: totalRecords,
            totalPages: totalPages,
            limit,
            offset,
            perpspec,
            fields
        });
    } catch (error) {
        console.error(`Error fetching data for perpspec '${req.params.perpspec}':`, error);
        res.status(500).json({ error: `Failed to fetch data for perpspec '${req.params.perpspec}'` });
    }
});

app.get('/api/system-summary', async (req, res) => {
    try {
        const latestStatusResult = await dbManager.pool.query(`
            SELECT DISTINCT ON (script_name) script_name, status, ts
            FROM perp_status
            ORDER BY script_name, ts DESC
        `);

        const runningScripts = latestStatusResult.rows.filter(r => r.status === 'running').map(r => r.script_name);

        const recentStatus = await dbManager.pool.query(`
            SELECT
                script_name,
                status,
                message,
                ts
            FROM perp_status
            ORDER BY ts DESC
            LIMIT 20
        `);

        const recentErrors = await dbManager.pool.query(`
            SELECT
                script_name,
                error_type,
                error_code,
                error_message,
                ts
            FROM perp_errors
            WHERE ts > NOW() - INTERVAL '2 hours'
            ORDER BY ts DESC
            LIMIT 10
        `);

        let statusText = '';

        statusText += `<div id="status-flex-container" style="display:flex; gap:20px; max-height:250px; background-color:#1e1e2f; padding:0; border-radius:8px; color:#d1d5db; overflow:hidden;">`;

        statusText += `<div id="status-left" style="flex:1 1 60%; background-color:#000; padding:5px; border:none; overflow-y:auto; max-height:200px;">`;
        statusText += `<div style="margin-bottom: 15px;">`;
        statusText += `<h3 style="color: #9f59ff; margin: 0 0 10px 0;">📊 Current Operations</h3>`;
        if (runningScripts.length > 0) {
            runningScripts.forEach(script => {
                statusText += `<div style="padding-left: 15px; margin-top: 3px; color: #fbbf24; font-size: 14px;">• ${script}</div>`;
            });
        } else {
            statusText += `<div style="color: #d1d5db;">No scripts currently running.</div>`;
        }
        statusText += `</div>`;

        if (recentStatus.rows.length > 0) {
            statusText += `<div style="margin-bottom: 15px;">`;
            statusText += `<strong style="color: #b569ff;">Latest Activity:</strong>`;
            recentStatus.rows.slice(0, 5).forEach(op => {
                let statusColor = '#d1d5db';
                if (op.status === 'success') statusColor = '#4ade80';
                else if (op.status === 'running') statusColor = '#fbbf24';
                else if (op.status === 'error') statusColor = '#f87171';

                const ts = new Date(op.ts);
                const timeStr = ts.toISOString().substr(11, 8) + ' UTC';

                statusText += `<div style="padding-left: 15px; margin-top: 3px; color: ${statusColor}; font-size: 14px;">`;
                statusText += `• ${op.script_name} [${timeStr}]: ${op.message || op.status}`;
                statusText += `</div>`;
            });
            statusText += `</div>`;
        }

        statusText += `</div>`;

        statusText += `<div id="status-right" style="flex:1 1 40%; background-color:#000; border:none; border-left: 1px solid rgba(159, 89, 255, 0.3); padding:5px; overflow-y:auto; max-height:200px;">`;

        if (recentErrors.rows.length > 0) {
            statusText += `<h4 style="color: #f87171; margin: 0 0 10px 0;">⚠️ Recent Issues</h4>`;

            recentErrors.rows.slice(0, 3).forEach(error => {
                const ts = new Date(error.ts);
                const timeStr = ts.toISOString().substr(11, 8) + ' UTC';

                let errorType = 'Other';
                if (error.error_code === '400') errorType = 'Bad Parameters';
                else if (error.error_code === '401') errorType = 'API Key Issues';
                else if (error.error_code === '429') errorType = 'Rate Limited';
                else if (error.error_code === '500') errorType = 'Server Errors';

                statusText += `<div style="margin-bottom: 8px; padding-left: 10px;">`;
                statusText += `<strong style="color: #fbbf24;">${error.script_name} [${timeStr}]:</strong> ${error.error_message}<br>`;
                statusText += `<small style="color: #d1d5db;">${errorType} (${error.error_code})</small>`;
                statusText += `</div>`;
            });
        } else {
            statusText += `<div style="color: #d1d5db;">No recent issues.</div>`;
        }

        statusText += `</div>`;

        statusText += `</div>`;

        if (runningScripts.length === 0 && recentStatus.rows.length === 0 && recentErrors.rows.length === 0) {
            statusText = `<div style="text-align: center; padding: 20px;">`;
            statusText += `<h3 style="color: #9f59ff; margin: 0 0 10px 0;">🏁 System Ready</h3>`;
            statusText += `<p style="color: #d1d5db; margin: 0;">No recent activity. Data collection scripts are ready to run.</p>`;
            statusText += `</div>`;
        }

        res.json({ statusText, errors: recentErrors.rows, operations: recentStatus.rows });
    } catch (error) {
        console.error('Error fetching system summary:', error);
        res.status(500).json({ error: 'Failed to fetch system summary' });
    }
});

app.get('/api/alert-cards', async (req, res) => {
    try {
        res.json({
            message: 'Alert cards endpoint reserved for future implementation',
            status: 'not_implemented'
        });
    } catch (error) {
        console.error('Error accessing alert-cards endpoint:', error);
        res.status(500).json({ error: 'Failed to access alert-cards endpoint' });
    }
});

app.get('/health', (req, res) => {
    res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

app.listen(PORT, () => {
    console.log(`🚀 FadeMoe4 Server running on http://localhost:${PORT}`);
    console.log(`📊 Database viewer available at http://localhost:${PORT}`);
});

