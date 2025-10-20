// public/js/controls.js
// ============================================================================
// UI Controls Logic
// Handles multi-select dropdown toggles, refresh, pagination, and view toggles.
// ============================================================================

document.addEventListener('DOMContentLoaded', () => {
    // Ensure a single global state object
    if (!window.appState) {
        window.appState = {
            currentPage: 1,
            limit: 100,
            selectedSymbols: [],
            selectedExchanges: [],
            visibleColumns: [],
            totalPages: 1,
            totalRecords: 0,
            defaultColumns: ['ts','symbol','exchange','o','h','l','c','v','oi','pfr','lsr','rsi1','rsi60','tbv','tsv','lqside','lqprice','lqqty'],
            mandatoryColumns: ['ts','symbol','exchange']
        };
    }

    const state = window.appState;

    // ------------------------------
    // Dropdown & View Controls (unchanged)
    // ------------------------------
    const multiSelects = [
        { btnId: 'symbol-selector', dropdownId: 'symbol-dropdown' },
        { btnId: 'exchange-selector', dropdownId: 'exchange-dropdown' },
        { btnId: 'params-selector', dropdownId: 'params-dropdown' }
    ];

    function closeAllDropdowns() {
        multiSelects.forEach(({ dropdownId }) => {
            const dropdown = document.getElementById(dropdownId);
            if (dropdown) dropdown.style.display = 'none';
        });
    }

    multiSelects.forEach(({ btnId, dropdownId }) => {
        const container = document.getElementById(btnId);
        if (!container) return;
        const btn = container.querySelector('.multi-select-btn');
        const dropdown = document.getElementById(dropdownId);
        if (!btn || !dropdown) return;

        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            const isVisible = dropdown.style.display === 'block';
            closeAllDropdowns();
            dropdown.style.display = isVisible ? 'none' : 'block';
        });
    });

    document.addEventListener('click', (e) => {
        if (!e.target.closest('.multi-select-dropdown')) closeAllDropdowns();
    });

    // ------------------------------
    // Refresh Button
    // ------------------------------
    const refreshBtn = document.getElementById('refresh-btn');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', () => {
            // Always update global state
            state.selectedSymbols = getSelectedFromDropdown('symbol-dropdown');
            state.selectedExchanges = getSelectedFromDropdown('exchange-dropdown');
            state.visibleColumns = getSelectedFromDropdown('params-dropdown');

            // Keep current page! (do not reset to 1)
            // state.currentPage = 1; // only if you want reset each refresh

            closeAllDropdowns();

            if (typeof window.fetchData === 'function') {
                window.fetchData(); // this will use window.appState
            }
        });
    }

    // ------------------------------
    // Pagination Controls
    // ------------------------------
    const prevPageBtn = document.getElementById('prev-page-btn');
    const nextPageBtn = document.getElementById('next-page-btn');
    const goToPageBtn = document.getElementById('go-to-page-btn');
    const pageInput = document.getElementById('page-input');

    if (prevPageBtn) {
        prevPageBtn.addEventListener('click', () => {
            if (state.currentPage > 1) {
                state.currentPage--;
                if (typeof window.fetchData === 'function') window.fetchData();
            }
        });
    }

    if (nextPageBtn) {
        nextPageBtn.addEventListener('click', () => {
            if (state.currentPage < state.totalPages) {
                state.currentPage++;
                if (typeof window.fetchData === 'function') window.fetchData();
            }
        });
    }

    if (goToPageBtn && pageInput) {
        goToPageBtn.addEventListener('click', () => {
            const pageNum = parseInt(pageInput.value);
            if (pageNum >= 1 && pageNum <= state.totalPages) {
                state.currentPage = pageNum;
                if (typeof window.fetchData === 'function') window.fetchData();
            } else {
                alert(`Please enter a valid page number between 1 and ${state.totalPages}`);
                pageInput.value = state.currentPage;
            }
        });
    }

    // ------------------------------
    // Helper
    // ------------------------------
    function getSelectedFromDropdown(dropdownId) {
        const container = document.getElementById(dropdownId);
        if (!container) return [];
        const allCheckbox = container.querySelector('input[value="__all__"]');
        if (allCheckbox && allCheckbox.checked) return [];
        const selected = [];
        container.querySelectorAll('input[type="checkbox"]:not([value="__all__"])').forEach(cb => {
            if (cb.checked) selected.push(cb.value);
        });
        return selected;
    }
});
