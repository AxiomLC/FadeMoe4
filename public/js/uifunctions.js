// public/js/uifunctions.js
// ============================================================================
// UI Utility Functions
// Handles rendering data, managing UI elements, and utility tasks.
// ============================================================================

/**
 * Escapes HTML special characters to prevent XSS attacks.
 * @param {string} str - The string to escape.
 * @returns {string} The escaped string.
 */
function escapeHtml(str) {
    if (typeof str !== 'string') return str;
    return str.replace(/[&<>"']/g, function(match) {
        return {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#39;'
        }[match];
    });
}

/**
 * Formats a Unix timestamp (milliseconds) into human-readable UTC string.
 * Example output: "2025-10-19 13:34:00 UTC"
 * @param {number} timestamp - Unix timestamp in milliseconds.
 * @returns {string} Formatted date string or 'Invalid Date' if invalid.
 */
function formatTimestampUTC(timestamp) {
    if (timestamp == null || timestamp === '') return '';
    
    try {
        // Ensure it's a number
        let numericTimestamp = Number(timestamp);
        
        if (isNaN(numericTimestamp)) return 'Invalid Date';

        const date = new Date(numericTimestamp);
        if (isNaN(date.getTime())) return 'Invalid Date';

        // Format: "2025-10-19 13:34:00 UTC"
        return date.toISOString().replace('T', ' ').substring(0, 19) + ' UTC';
    } catch {
        return 'Invalid Date';
    }
}
//==============================================================
/**
 * Formats a number string to keep only meaningful decimals.
 * Rules:
 * - Leave one zero if all decimals are zeros (e.g. 3.000 → 3.0)
 * - Trim trailing zeros if any non-zero digit exists after decimal
 * - Preserve leading zeros for small decimals (e.g. 0.00032)
 */
function smartTrimDecimal(value) {
    if (value == null || value === '') return '';
    const num = Number(value);
    if (isNaN(num)) return value.toString();

    // Convert to string with full precision but prevent scientific notation
    let str = num.toString();

    // If there is no decimal point, return as-is
    if (!str.includes('.')) return str;

    const [intPart, decPart] = str.split('.');

    // If all decimal digits are zeros -> keep only one zero
    if (/^0+$/.test(decPart)) {
        return `${intPart}.0`;
    }

    // Otherwise trim only the trailing zeros after the last non-zero
    const trimmed = decPart.replace(/0+$/, '');
    return `${intPart}.${trimmed}`;
}

/**
 * Displays data in a table.
 * @param {Array<Object>} data - Array of data objects.
 * @param {Array<string>} columns - Columns to display.
 */
function displayTableData(data, columns) {
    const tableHeaderRow = document.getElementById('table-header-row');
    const tableBody = document.getElementById('table-body');
    const tableMessage = document.getElementById('table-message');

    if (!tableHeaderRow || !tableBody || !tableMessage) {
        console.error('Table elements not found');
        return;
    }

    if (!data || data.length === 0) {
        tableHeaderRow.innerHTML = '';
        tableBody.innerHTML = '';
        tableMessage.textContent = 'No data found';
        return;
    }

    tableMessage.textContent = '';

    // Build header
    tableHeaderRow.innerHTML = '';
    columns.forEach(col => {
        let headerText = col.charAt(0).toUpperCase() + col.slice(1);
        if (col === 'exchange') headerText = 'Exch';
        const th = document.createElement('th');
        th.textContent = headerText;
        tableHeaderRow.appendChild(th);
    });

    // Build body
    tableBody.innerHTML = '';
    data.forEach(row => {
        const tr = document.createElement('tr');
        columns.forEach(col => {
            const td = document.createElement('td');
            let val = row[col];
            if (val === null || val === undefined) {
                td.innerHTML = '<em>null</em>';
            } else if (col === 'ts') {
                td.textContent = formatTimestampUTC(val);
            } else if (typeof val === 'object') {
                const jsonStr = JSON.stringify(val);
                td.textContent = jsonStr.length > 50 ? jsonStr.substring(0, 50) + '...' : jsonStr;
                td.title = jsonStr;
            } else if (typeof val === 'string' && val.length > 50) {
                td.textContent = val.substring(0, 50) + '...';
                td.title = val;
            } else if (typeof val === 'number' || (!isNaN(val) && val !== '')) {
                td.textContent = smartTrimDecimal(val);
            } else {
                td.textContent = val.toString();
            }

            tr.appendChild(td);
        });
        tableBody.appendChild(tr);
    });
}

/**
 * Populates a multi-select dropdown with checkboxes.
 * @param {string} containerId - ID of the dropdown container div.
 * @param {Array<string>} items - List of items to populate.
 * @param {Array<string>} selectedItems - Items to mark as selected.
 */
function populateMultiSelect(containerId, items, selectedItems = []) {
    const container = document.getElementById(containerId);
    if (!container) {
        console.error(`Multi-select container ${containerId} not found`);
        return;
    }
    container.innerHTML = '';

    // Add "All" checkbox
    const allLabel = document.createElement('label');
    allLabel.className = 'multi-select-label';
    const allCheckbox = document.createElement('input');
    allCheckbox.type = 'checkbox';
    allCheckbox.value = '__all__';
    allCheckbox.checked = selectedItems.length === 0 || selectedItems.length === items.length;
    allLabel.appendChild(allCheckbox);
    allLabel.appendChild(document.createTextNode('All'));
    container.appendChild(allLabel);

    allCheckbox.addEventListener('change', () => {
        const checkboxes = container.querySelectorAll('input[type="checkbox"]:not([value="__all__"])');
        checkboxes.forEach(cb => cb.checked = allCheckbox.checked);
    });

    // Add individual items
    items.forEach(item => {
        const label = document.createElement('label');
        label.className = 'multi-select-label';
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.value = item;
        checkbox.checked = selectedItems.includes(item);
        
        // STOP PROPAGATION to keep dropdown open
        checkbox.addEventListener('click', (e) => {
            e.stopPropagation();
        });
        
        label.appendChild(checkbox);
        label.appendChild(document.createTextNode(item));
        container.appendChild(label);

        checkbox.addEventListener('change', () => {
            if (!checkbox.checked) {
                allCheckbox.checked = false;
            } else {
                const allChecked = Array.from(container.querySelectorAll('input[type="checkbox"]:not([value="__all__"])'))
                    .every(cb => cb.checked);
                allCheckbox.checked = allChecked;
            }
        });
    });
}

/**
 * Updates pagination controls.
 */
function updatePaginationControls(currentPage, totalPages, totalRecords) {
    const pageInfo = document.getElementById('page-info');
    const totalPagesDisplay = document.getElementById('total-pages-display');
    const prevBtn = document.getElementById('prev-page-btn');
    const nextBtn = document.getElementById('next-page-btn');
    const pageInput = document.getElementById('page-input');

    if (pageInfo) pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;
    if (totalPagesDisplay) totalPagesDisplay.textContent = `Total Pages: ${totalPages}`;

    if (prevBtn) prevBtn.disabled = currentPage <= 1;
    if (nextBtn) nextBtn.disabled = currentPage >= totalPages;
    if (pageInput) pageInput.value = currentPage;
}