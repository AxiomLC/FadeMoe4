let currentView = 'db';
let currentPerpspec = '';
let currentOffset = 0;
let currentLimit = 100;
let totalRecords = 0;

document.addEventListener('DOMContentLoaded', () => {
    loadComponents();
    loadSystemStatus();
    setInterval(loadSystemStatus, 30000);

    const modal = document.getElementById('schemaModal');
    const closeBtn = document.getElementById('modalCloseBtn');
    const testOpenBtn = document.getElementById('testOpenModalBtn');

    if (modal && closeBtn) {
        closeBtn.addEventListener('click', () => {
            console.log('Modal close button clicked');
            modal.classList.add('hidden');
});
        modal.addEventListener('click', (event) => {
            if (event.target === modal) {
                console.log('Modal background clicked');
                modal.classList.add('hidden');
    }
        });
    }

    loadThreeButtons();
});
async function loadSystemStatus() {
    const statusBox = document.getElementById('statusBox');
    if (!statusBox) return;

    try {
        const response = await fetch('/api/system-summary');
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const data = await response.json();
        if (data.statusText) {
            statusBox.innerHTML = data.statusText;
        }
    } catch (error) {
        console.error('Failed to load system status:', error);
        if (statusBox) statusBox.innerHTML = '<div class="error">Failed to load system status</div>';
    }
}

async function loadComponents() {
    try {
        const controlsResponse = await fetch('components/controls.html');
        const controlsHtml = await controlsResponse.text();
        document.getElementById('controls-container').innerHTML = controlsHtml;

        setupEventListeners();

        const dbviewControlResponse = await fetch('components/dbviewcontrol.html');
        const dbviewControlHtml = await dbviewControlResponse.text();
        document.getElementById('dbviewcontrol-container').innerHTML = dbviewControlHtml;

        initDbviewControlListeners();
    } catch (error) {
        console.error('Failed to load controls or dbviewcontrol:', error);
    }
    try {
        const statusResponse = await fetch('components/statusbox.html');
        const statusHtml = await statusResponse.text();
        document.getElementById('status-box-container').innerHTML = statusHtml;
    } catch (error) {
        console.error('Failed to load status box:', error);
    }
    try {
        const dbViewResponse = await fetch('components/dbview.html');
        const dbViewHtml = await dbViewResponse.text();
        document.getElementById('db-view-container').innerHTML = dbViewHtml;
    } catch (error) {
        console.error('Failed to load DB view:', error);
    }

    try {
        const alertViewResponse = await fetch('components/alertview.html');
        const alertViewHtml = await alertViewResponse.text();
        document.getElementById('alert-view-container').innerHTML = alertViewHtml;
    } catch (error) {
        console.error('Failed to load Alert view:', error);
    }
}

function setupEventListeners() {
    const viewDbBtn = document.getElementById('view-db-btn');
    const viewAlertsBtn = document.getElementById('view-alerts-btn');
    if (viewDbBtn && viewAlertsBtn) {
        viewDbBtn.addEventListener('click', () => switchView('db'));
        viewAlertsBtn.addEventListener('click', () => switchView('alerts'));
    }
}

function initDbviewControlListeners() {
    const perpspecDropdown = document.getElementById('perpspecDropdown');
    const selectBtn = document.getElementById('selectPerpspecBtn');
    const viewSchemaBtn = document.getElementById('viewSchemaBtn');

    if (!perpspecDropdown || !selectBtn || !viewSchemaBtn) {
        console.warn('DB view control buttons or dropdown not found');
        return;
    }

    loadPerpspecsDbviewControl();

    selectBtn.addEventListener('click', () => {
        const selected = perpspecDropdown.value;
        if (!selected) {
            alert('Please select a Perpspec');
            return;
        }
        fetchAndDisplayDataDbviewControl(selected);
    });

    viewSchemaBtn.addEventListener('click', async () => {
        const selected = perpspecDropdown.value;
        if (!selected) {
            alert('Please select a Perpspec');
            return;
        }
        await fetchAndDisplaySchema(selected);
    });
}

function switchView(view) {
    currentView = view;

    const viewDbBtn = document.getElementById('view-db-btn');
    const viewAlertsBtn = document.getElementById('view-alerts-btn');
    const dbViewContainer = document.getElementById('db-view-container');
    const alertViewContainer = document.getElementById('alert-view-container');

    if (viewDbBtn && viewAlertsBtn && dbViewContainer && alertViewContainer) {
        viewDbBtn.classList.toggle('active', view === 'db');
        viewDbBtn.classList.toggle('glow-purple', view === 'db');
        viewDbBtn.classList.remove('glow-pink');

        viewAlertsBtn.classList.toggle('active', view === 'alerts');
        viewAlertsBtn.classList.toggle('glow-pink', view === 'alerts');
        viewAlertsBtn.classList.remove('glow-purple');

        dbViewContainer.classList.toggle('hidden', view !== 'db');
        alertViewContainer.classList.toggle('hidden', view !== 'alerts');
    }
}

function initDbviewControl() {
    const perpspecDropdown = document.getElementById('perpspecDropdown');
    const selectBtn = document.getElementById('selectPerpspecBtn');

    if (!perpspecDropdown || !selectBtn) return;

    loadPerpspecsDbviewControl();

    selectBtn.addEventListener('click', () => {
        const selected = perpspecDropdown.value;
        if (!selected) {
            alert('Please select a Perpspec');
            return;
        }
        fetchAndDisplayDataDbviewControl(selected);
    });
}

async function loadPerpspecsDbviewControl() {
    const dropdown = document.getElementById('perpspecDropdown');
    dropdown.innerHTML = '<option value="">Loading Perpspecs...</option>';
    try {
        const response = await fetch('/api/perpspecs');
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const perpspecs = await response.json();
        dropdown.innerHTML = '<option value="">Select Perpspec...</option>';
        perpspecs.forEach(ps => {
            const option = document.createElement('option');
            option.value = ps.perpspec_name;
            option.textContent = ps.perpspec_name.toUpperCase();
            dropdown.appendChild(option);
        });
    } catch (error) {
        console.error('Failed to load perpspecs:', error);
        dropdown.innerHTML = '<option value="">Failed to load Perpspecs</option>';
    }
}

async function fetchAndDisplayDataDbviewControl(perpspec) {
    const container = document.getElementById('db-view-container');
    container.innerHTML = '<div class="loading">Loading data...</div>';

    try {
        const schemaResponse = await fetch('/api/perpspecs');
        if (!schemaResponse.ok) throw new Error(`HTTP error! status: ${schemaResponse.status}`);
        const perpspecs = await schemaResponse.json();
        const schema = perpspecs.find(ps => ps.perpspec_name === perpspec);
        if (!schema) throw new Error(`Schema for ${perpspec} not found`);

        const fields = schema.fields;

        const dataResponse = await fetch(`/api/data/${perpspec}?limit=100&offset=0`);
        if (!dataResponse.ok) throw new Error(`HTTP error! status: ${dataResponse.status}`);
        const dataResult = await dataResponse.json();

        if (!dataResult.data || dataResult.data.length === 0) {
            container.innerHTML = '<div class="loading">No data found for selected Perpspec.</div>';
            return;
        }

        let html = '<table><thead><tr>';
        const coreColumns = ['ts', 'symbol', 'source', 'interval'];
        const allColumns = [...new Set([...coreColumns, ...fields])];
        allColumns.forEach(col => {
            html += `<th>${col}</th>`;
        });
        html += '</tr></thead><tbody>';

        dataResult.data.forEach(row => {
            html += '<tr>';
            allColumns.forEach(col => {
                let val = row[col];
                if (val === null || val === undefined) val = '<em>null</em>';
                else if (col === 'ts') val = new Date(val).toLocaleString();
                else val = val.toString();
                html += `<td>${val}</td>`;
            });
            html += '</tr>';
        });

        html += '</tbody></table>';
        container.innerHTML = html;
    } catch (error) {
        console.error('Error fetching/displaying data:', error);
        container.innerHTML = `<div class="error">Error: ${error.message}</div>`;
    }
}

async function fetchAndDisplaySchema(perpspec) {
    const modal = document.getElementById('schemaModal');
    const modalContent = document.getElementById('modalSchemaContent');
    if (!modal || !modalContent) {
        console.error('Schema modal elements not found');
        return;
    }

    modalContent.innerHTML = '<div class="loading">Loading schema...</div>';
    try {
        const response = await fetch('/api/schema/perp_data');
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const columns = await response.json();

        if (!columns || columns.length === 0) {
            modalContent.innerHTML = '<div>No schema columns found.</div>';
            return;
        }

        let html = '<strong>Detailed Schema for perp_data Table:</strong>';
        html += '<table><thead><tr><th>Column Name</th><th>Data Type</th><th>Nullable</th><th>Default</th></tr></thead><tbody>';

        columns.forEach(col => {
            html += `<tr><td>${col.column_name}</td><td>${col.data_type}</td><td>${col.is_nullable}</td><td>${col.column_default || ''}</td></tr>`;
        });

        html += '</tbody></table>';

        modalContent.innerHTML = html;
        modal.classList.remove('hidden');
        console.log('Detailed schema modal displayed');
    } catch (error) {
        console.error('Error fetching detailed schema:', error);
        modalContent.innerHTML = `<div class="error">Error: ${error.message}</div>`;
    }
}

function toggleButton(btn, onText, offText) {
    btn.addEventListener('click', () => {
        if (btn.classList.contains('on')) {
            btn.classList.remove('on');
            btn.classList.add('off');
            btn.textContent = offText;
        } else {
            btn.classList.remove('off');
            btn.classList.add('on');
            btn.textContent = onText;
        }
    });
}

function attachToggleButtonsListeners() {
    const btnApis = document.getElementById('btnApis');
    const btnTrader = document.getElementById('btnTrader');
    const btnBacktester = document.getElementById('btnBacktester');

    if (btnApis) toggleButton(btnApis, 'APIs Pull On', 'APIs Pull Off');
    if (btnTrader) toggleButton(btnTrader, 'Core Trader On', 'Core Trader Off');
    if (btnBacktester) toggleButton(btnBacktester, 'Backtester On', 'Backtester Off');
}

async function loadThreeButtons() {
    try {
        const response = await fetch('components/3buttons.html');
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const html = await response.text();
        document.getElementById('three-buttons-container').innerHTML = html;
        attachToggleButtonsListeners();
    } catch (error) {
        console.error('Failed to load 3buttons.html:', error);
    }
}

