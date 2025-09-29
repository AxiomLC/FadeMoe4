/**
 * UI Utility Functions for FadeMoe4 frontend.
 * Includes status updates, error display, table rendering, pagination,
 * and HTML escaping to prevent XSS.
 */

/**
 * Update status bar message with current time.
 * @param {string} message
 */
function updateStatus(message) {
    const statusBar = document.getElementById('statusBar');
    if (statusBar) {
        statusBar.textContent = `${new Date().toLocaleTimeString()}: ${message}`;
    }
}

/**
 * Display an error message in the main table area.
 * @param {string} message
 */
function showError(message) {
    const tableContent = document.getElementById('tableContent');
    if (tableContent) {
        tableContent.innerHTML = `<div class="error">❌ ${message}</div>`;
        updateStatus(`Error: ${message}`);
    }
}

/**
 * Render data as an HTML table.
 * @param {Array} data - Array of data objects.
 * @param {Array|null} fields - Optional array of fields to display.
 */
function displayTableData(data, fields = null) {
    const tableContent = document.getElementById('tableContent');

    if (!data || data.length === 0) {
        tableContent.innerHTML = '<div class="loading">No data found</div>';
        return;
    }

    const columns = fields ? fields : Object.keys(data[0]);

    let html = '<table><thead><tr>';
    columns.forEach(col => {
        html += `<th style="color: #b569ff;">${col}</th>`;
    });
    html += '</tr></thead><tbody>';

    data.forEach(row => {
        html += '<tr>';
        columns.forEach(col => {
            let value = row[col];

            if (value === null || value === undefined) {
                value = '<em>null</em>';
            } else if (col === 'ts' && typeof value === 'string' && value.includes('T')) {
                value = new Date(value).toLocaleString();
            } else if (typeof value === 'object' && value !== null) {
                value = `<span class="json-cell" title="${escapeHtml(JSON.stringify(value, null, 2))}">${escapeHtml(JSON.stringify(value))}</span>`;
            } else if (typeof value === 'string' && value.length > 50) {
                value = `<span title="${escapeHtml(value)}">${escapeHtml(value.substring(0, 50))}...</span>`;
            } else {
                value = escapeHtml(String(value));
            }

            html += `<td>${value}</td>`;
        });
        html += '</tr>';
    });

    html += '</tbody></table>';
    tableContent.innerHTML = html;
}

/**
 * Update pagination controls based on total records and current page.
 * @param {number} totalRecords
 * @param {number} currentOffset
 * @param {number} currentLimit
 */
function updatePagination(totalRecords, currentOffset, currentLimit) {
    const pagination = document.getElementById('pagination');
    const pageInfo = document.getElementById('pageInfo');

    if (totalRecords > currentLimit) {
        pagination.style.display = 'flex';
        const currentPage = Math.floor(currentOffset / currentLimit) + 1;
        const totalPages = Math.ceil(totalRecords / currentLimit);
        pageInfo.textContent = `Page ${currentPage} of ${totalPages} (${totalRecords} total)`;

        const prevBtn = document.getElementById('prev-page-btn');
        const nextBtn = document.getElementById('next-page-btn');
        prevBtn.disabled = currentOffset === 0;
        nextBtn.disabled = currentOffset + currentLimit >= totalRecords;
    } else {
        pagination.style.display = 'none';
    }
}

/**
 * Escape HTML special characters to prevent XSS attacks.
 * @param {string} unsafe
 * @returns {string}
 */
function escapeHtml(unsafe) {
    if (typeof unsafe !== 'string') return unsafe;
    return unsafe
         .replace(/&/g, "&amp;")
         .replace(/</g, "&lt;")
         .replace(/>/g, "&gt;")
         .replace(/"/g, "&quot;")
         .replace(/'/g, "&#039;");
}