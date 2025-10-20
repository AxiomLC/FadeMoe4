// public/js/main.js
// ============================================================================
// Main Application Logic
// Manages app state, fetches data from API, and updates UI accordingly.
// ============================================================================
// ============================================================================
// Main Application Logic
// Manages app state, fetches data from API, and updates UI accordingly.
// ============================================================================

// ✅ Only create appState if it doesn’t already exist
if (!window.appState) {
    window.appState = {
        currentPage: 1,
        limit: 100,
        selectedSymbols: [],
        selectedExchanges: [],
        visibleColumns: [],
        totalPages: 1,
        totalRecords: 0,
        defaultColumns: ['ts', 'symbol', 'exchange', 'o', 'h', 'l', 'c', 'v', 'oi', 'pfr', 'lsr', 'rsi1', 'rsi60', 'tbv', 'tsv', 'lqside', 'lqprice', 'lqqty'],
        mandatoryColumns: ['ts', 'symbol', 'exchange']
    };
}

document.addEventListener('DOMContentLoaded', () => {
    initializeUI();
});

/**
 * Initializes the UI by fetching selectors data and system status.
 */
async function initializeUI() {
    try {
        const state = window.appState; // use global shared state

        // Fetch and populate selectors
        const symbols = await fetchJson('/api/symbols');
        populateMultiSelect('symbol-dropdown', symbols, state.selectedSymbols);

        const exchanges = await fetchJson('/api/exchanges');
        populateMultiSelect('exchange-dropdown', exchanges, state.selectedExchanges);

        const params = await fetchJson('/api/params');
        // ✅ Preserve user column selections if already set
        const currentColumns = state.visibleColumns.length
            ? state.visibleColumns
            : state.defaultColumns;
        populateMultiSelect('params-dropdown', params, currentColumns);

        // Fetch system status (non-blocking)
        fetchSystemSummary();

        // Initial data fetch
        await fetchData();
    } catch (error) {
        console.error('Error initializing UI:', error);
    }
}


/**
 * Fetches JSON data from given URL with error handling.
 * @param {string} url
 * @returns {Promise<any>}
 */
async function fetchJson(url) {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    return response.json();
}

/**
 * Fetches system summary and updates status box.
 */
async function fetchSystemSummary() {
    try {
        const summary = await fetchJson('/api/system-summary');
        const statusBox = document.getElementById('system-status-box');
        if (statusBox && summary.statusText) {
            statusBox.innerHTML = summary.statusText;
        }
    } catch (error) {
        console.error('Failed to fetch system summary:', error);
    }
}

/**
 * Fetches perp_data from API based on current appState and updates table and pagination.
 */
async function fetchData() {
    try {
        const params = new URLSearchParams();
        params.append('page', window.appState.currentPage);
        params.append('limit', window.appState.limit);

        // Send filters only if selected, else omit to fetch all
        if (window.appState.selectedSymbols.length > 0) {
            params.append('symbol', window.appState.selectedSymbols.join(','));
        }
        if (window.appState.selectedExchanges.length > 0) {
            params.append('exchange', window.appState.selectedExchanges.join(','));
        }
        // Send selected columns or default columns
        const cols = window.appState.visibleColumns.length ? window.appState.visibleColumns : window.appState.defaultColumns;
        params.append('params', cols.join(','));

        const url = `/api/perp_data?${params.toString()}`;
        console.log('Fetching data with URL:', url);

        const result = await fetchJson(url);
        console.log('Received data:', result);

        window.appState.totalPages = result.pagination.totalPages;
        window.appState.totalRecords = result.pagination.totalRecords;
        window.appState.currentPage = result.pagination.currentPage;
        window.appState.visibleColumns = result.visibleColumns || window.appState.defaultColumns;

        displayTableData(result.data, window.appState.visibleColumns);
        updatePaginationControls(window.appState.currentPage, window.appState.totalPages, window.appState.totalRecords);
    } catch (error) {
        console.error('Failed to fetch data:', error);
        const tableMessage = document.getElementById('table-message');
        if (tableMessage) {
            tableMessage.textContent = `Error loading data: ${error.message}`;
        }
    }
}

// Expose fetchData globally for controls.js
window.fetchData = fetchData;