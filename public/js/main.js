/**
 * Main JavaScript for FadeMoe4 UI.
 * Handles loading UI components, system status, perpspec dropdown,
 * schema display, and view toggling.
 */

let currentView = 'db';
let currentPerpspec = '';
let currentOffset = 0;
let currentLimit = 100;
let totalRecords = 0;

document.addEventListener('DOMContentLoaded', () => {
    loadComponents();
    loadSystemStatus();
    setInterval(loadSystemStatus, 4000);

    const modal = document.getElementById('schemaModal');
    const closeBtn = document.getElementById('modalCloseBtn');
    if (modal && closeBtn) {
        closeBtn.addEventListener('click', () => {
            modal.classList.add('hidden');
        });
        modal.addEventListener('click', (event) => {
            if (event.target === modal) {
                modal.classList.add('hidden');
            }
        });
    }

    loadThreeButtons();
});

/**
 * Load and update system status box from API.
 */
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

/**
 * Load UI components HTML fragments and initialize event listeners.
 */
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

/**
 * Setup event listeners for view toggle buttons.
 */
function setupEventListeners() {
    const viewDbBtn = document.getElementById('view-db-btn');
    const viewAlertsBtn = document.getElementById('view-alerts-btn');
    if (viewDbBtn && viewAlertsBtn) {
        viewDbBtn.addEventListener('click', () => switchView('db'));
        viewAlertsBtn.addEventListener('click', () => switchView('alerts'));
    }
}

/**
 * Initialize listeners for DB view controls (dropdown, buttons).
 */
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
        await fetchAndDisplaySchemaFields(selected);
    });
}

/**
 * Switch between DB view and Alerts view.
 * @param {string} view - 'db' or 'alerts'
 */
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

/**
 * Load perpspecs from API and populate dropdown.
 */
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

/**
 * Fetch and display schema fields for selected perpspec.
 * @param {string} perpspec
 */
async function fetchAndDisplaySchemaFields(perpspec) {
    const container = document.getElementById('db-view-container');
    container.innerHTML = '<div class="loading">Loading schema fields...</div>';

    try {
        const response = await fetch('/api/perpspecs');
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const perpspecs = await response.json();
        const schema = perpspecs.find(ps => ps.perpspec_name === perpspec);
        if (!schema) throw new Error(`Schema for ${perpspec} not found`);

        const fields = schema.fields;
        let html = `<h3>Schema Fields for ${perpspec.toUpperCase()}</h3><ul>`;
        fields.forEach(field => {
            html += `<li>${field}</li>`;
        });
        html += '</ul>';

        container.innerHTML = html;
    } catch (error) {
        console.error('Error fetching/displaying schema fields:', error);
        container.innerHTML = `<div class="error">Error: ${error.message}</div>`;
    }
}

/**
 * Fetch and display data for selected perpspec.
 * @param {string} perpspec
 */
async function fetchAndDisplayDataDbviewControl(perpspec) {
    const container = document.getElementById('db-view-container');
    container.innerHTML = '<div class="loading">Loading data...</div>';

    try {
        const schemaResponse = await fetch('/api/perpspecs');
        if (!schemaResponse.ok) throw new Error(`HTTP error! status: ${schemaResponse.status}`);
        const perpspecs = await schemaResponse.json();
        const schema = perpspecs.find(ps => ps.perpspec_name === perpspec);
        if (!schema) throw new Error(`Schema for ${perpspec} not found`);

        // Filter out 'perpspec' from fields to display
        const fields = schema.fields.filter(f => f !== 'perpspec');

        const dataResponse = await fetch(`/api/data/${perpspec}?limit=100&offset=0`);
        if (!dataResponse.ok) throw new Error(`HTTP error! status: ${dataResponse.status}`);
        const dataResult = await dataResponse.json();

        if (!dataResult.data || dataResult.data.length === 0) {
            container.innerHTML = '<div class="loading">No data found for selected Perpspec.</div>';
            return;
        }

        const coreColumns = ['ts', 'symbol', 'source', 'interval'];
        // Exclude 'perpspec' from core columns as well if present
        const filteredCoreColumns = coreColumns.filter(c => c !== 'perpspec');
        const allColumns = [...new Set([...filteredCoreColumns, ...fields])];

        let html = '<table><thead><tr>';
        allColumns.forEach(col => {
            html += `<th>${col}</th>`;
        });
        html += '</tr></thead><tbody>';

        dataResult.data.forEach(row => {
            html += '<tr>';
            allColumns.forEach(col => {
                let val = row[col];
                if (val === null || val === undefined) val = '<em>null</em>';
                else if (col === 'ts') val = formatTimestampForUI(val);
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

/**
 * Placeholder for loading additional UI buttons if needed.
 */
function loadThreeButtons() {
    // Implement if needed
}

/**
 * Format timestamp for display.
 * @param {number|string|bigint} timestamp
 * @returns {string}
 */
function formatTimestampForUI(ts) {
    const date = new Date(Number(ts));
    const day = String(date.getUTCDate()).padStart(2, '0');
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const year = date.getUTCFullYear();
    const hours = String(date.getUTCHours()).padStart(2, '0');
    const minutes = String(date.getUTCMinutes()).padStart(2, '0');
    const seconds = String(date.getUTCSeconds()).padStart(2, '0');
    return `${day}/${month}/${year} ${hours}:${minutes}:${seconds} UTC`;
}

