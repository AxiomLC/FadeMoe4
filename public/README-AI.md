# 22 Nov 25, Alert Cards UI - README

## Overview

The Alert Cards UI provides a live, dynamic dashboard for monitoring trading alerts based on user-defined "comboAlgos". It polls backend data every minute, evaluates alert conditions, and displays alert cards in a responsive grid.

---

## Files

### `cardsview.html`

- The main HTML page for the Alert Cards view.
- Contains UI controls: Start/Stop polling buttons, comboAlgo input boxes, delay and max alerts inputs, and a wipe button.
- Displays alert cards in a flex grid below the controls.
- Loads `card-controls.js` and `card-poll.js` scripts.

### `js/card-controls.js`

- Manages UI controls logic:
  - Handles Start/Stop button toggling and status display.
  - Manages dynamic comboAlgo input rows with add/remove buttons.
  - Validates user input and shows inline error messages.
  - Saves and restores user settings (comboAlgos, delay, max alerts) via localStorage.
  - Handles the wipe button to clear all alerts and settings.
- Exposes `initializeCardControls()` for setup.

### `js/card-poll.js`

- Handles polling backend API every 1 minute to fetch latest perp_metrics data.
- Parses and evaluates user-defined comboAlgos against the data.
- Manages alert state, including alert history and last trigger timestamps.
- Renders alert cards dynamically in the UI.
- Exposes `alertPoll` object with methods to start/stop polling, initialize, and render alerts.

### `server.js` (new API endpoint)

- Adds `/api/latest-metrics` endpoint.
- Accepts `symbols` and `exchanges` query parameters.
- Returns latest perp_metrics rows per symbol and exchange for efficient polling.

### `index.html` (edited)

- Dynamically loads and initializes Alert Cards view and scripts when "Alert Cards" button is pressed.
- Maintains seamless toggling between DB view, Alert Cards, and Backtester views.

---

## Flow

1. User clicks "Alert Cards" button on main dashboard.

2. `cardsview.html` is loaded dynamically into the page.

3. `card-controls.js` initializes UI controls and restores saved settings.

4. User enters comboAlgos, sets delay and max alerts, and clicks Start.

5. `card-poll.js` starts polling the backend `/api/latest-metrics` every minute.

6. Polling data is evaluated against comboAlgos; matching alerts are added to alert history.

7. Alert cards are rendered dynamically in a responsive grid.

8. User can stop polling, add/remove comboAlgos, adjust settings, or wipe all alerts.

---

## Notes

- User inputs are validated with inline feedback.

- Alert history and settings persist across sessions using localStorage.

- Polling and evaluation logic is optimized for performance and scalability.

- The UI uses consistent styling with the main dashboard.

---

For detailed implementation, see respective files in `public/` and `public/js/`.