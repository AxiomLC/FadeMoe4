25 Sep Summary 

### Current Project Status

The FadeMoe4 project has successfully integrated a single history (`h`) data collection script (`c-pfr-h.js`) that fetches predicted funding rate (PFR) data, dynamically creates and updates the corresponding schema in the database (`perpspec_schema`), and inserts time-series data into the core `perp_data` table. The Express-based server correctly pulls the schema information from the database and exposes RESTful endpoints (`/api/perpspecs` and `/api/data/:perpspec`) that serve the available perpspecs and their associated data.

The frontend files located in the `public/` directory, including the main JavaScript (`main.js`), UI helper functions (`uifunctions.js`), and HTML components (`controls.html`, `dbviewcontrol.html`, `dbview.html`, and `statusbox.html`), successfully fetch and display the perpspec list and data. The UI features a dropdown to select available perpspecs, a refresh button to reload data, and buttons to toggle between the database view and alert cards (future feature). Data is dynamically rendered in a responsive table based on the selected schema, with pagination support and real-time system status updates.

This stable foundation enables easy addition of new `h` and `c` scripts for other data sources and metrics, with automatic schema management and seamless UI integration.

---

24 SEP SUMMARY - Addtls
App Flow Summary:

The FadeMoe4 application operates on a dynamic, metric-driven architecture where -h and -c data collection scripts are the sole creators of Metrics and their Fields. 
dbsetup.js
 provides the foundational database structure (perp_data with core ts, symbol, source, interval columns), enabling dynamic schema extension. Each -h/-c script defines its METRIC_NAME (e.g., "pfr", "ohlcv") and dynamically creates its associated Fields (columns) in perp_data as needed, ensuring data uniformity. The server.js API layer exposes /metric to list available metrics (derived from perp_data columns) and /metric/:metricName to fetch data for a specific metric, dynamically selecting all associated fields. The UI interacts with these endpoints to display metrics and their data, adapting to the evolving database schema. A metric_schema table (to be implemented) will govern field management and deletion for schema consistency.

---

6:12pm addtl summary:

# Current Project Status

**As of September 23, 2025**: The FadeMoe4 system is fully operational with a robust, production-ready architecture. The core infrastructure is stable and requires minimal maintenance, while the modular design allows for easy extension.

**Stable Components** (`dbsetup.js`, `server.js`, `master-api.js`): These foundational elements are complete and functioning correctly - they establish the database schema, serve the web interface, and orchestrate data collection respectively. These files should remain untouched except for critical bug fixes.

**Flexible Components** (`g-symbols.js`): The symbol generation system can be easily modified to accommodate new API sources or token additions without affecting the core system.

**Modular Data Collection** (`apis/*.js`): The heart of the system lies in the historical (`-h.js`) and current (`-c.js`) data collection scripts. These files control everything - from API interactions to database storage. Maintaining consistency in their structure and naming conventions is crucial for system reliability.

**Immediate Roadmap**: Next to be implemented are the Coinalyze historical data collectors for Predicted Funding Rate (PFR), Open Interest (OI), Long/Short Ratio (LSR), and Liquidations (LQ), followed by their real-time counterparts.

**Future Development**: The system is architected to seamlessly integrate advanced features including a sophisticated backtesting engine and real-time alert cards for trading signals, all while maintaining the unified data structure that enables powerful AI-assisted analysis.

---

6:10pm new Summary for README
Database Architecture Philosophy: The 
dbsetup.js
 is a universal, one-time initialization tool that creates a flexible schema for any data collection script. Each historical (-h.js) or current (-c.js) script follows a standardized pattern: it dynamically creates tables/columns based on API response data, using unified ts (timestamp), symbol, and source as core identifiers. Scripts share common functions for status logging (running/done/error) and data insertion, with only API-specific details (endpoint, authentication, symbol format, special parsing) being customized per script. This ensures maximum code reuse while maintaining flexibility for diverse data sources.

---
# FadeMoe4 - Condensed README (23 Sep 2025)

## 🏗️ Application Architecture Overview

### Core Components
1. **Database Layer** (`db/dbsetup.js`)
   - TimescaleDB/PostgreSQL with hypertables for time-series data
   - Tables: `perp_data` (main), `perp_status`, `perp_errors`, `metric_catalog`, `perp_metadata`
   - Dynamic schema creation for new data types

2. **Data Collection Layer** (`apis/` folder)
   - Plugin-based architecture - any `.js` file in `apis/` is automatically executed
   - Naming convention: `[source]-[metric]-[type].js` (e.g., `c-oi-h.js` = Coinalyze OpenInterest History)
   - Scripts handle their own API calls, data processing, and database insertion

3. **Orchestration Layer** (`master-api.js`)
   - Dynamically discovers and manages all scripts in `apis/` folder
   - Coordinates backfill → live transition
   - Respects rate limits and handles errors

4. **Web Interface** (`public/`)
   - Single-page application with dark/purple theme
   - Dynamic status box always visible at top
   - Database viewer with data type selectors
   - Future alert cards grid

### Data Flow
```
Symbol List (perp-list.js) 
    ↓
Dynamic Symbol Generator (g-symbols.js)
    ↓
Master API Orchestrator (master-api.js)
    ↓
Individual API Scripts (apis/*.js)
    ↓
Database (perp_data table with hypertables)
    ↓
Web UI (Database Viewer)
```

### Database Structure
- **`perp_data`**: Main time-series table with universal 1-min timestamps
  - Token prices: Full OHLCV data
  - Perp specs: Only `c` (close) column used, rest NULL
- **`perp_status`**: Lightweight job tracking
- **`perp_errors`**: Critical error logging
- **`metric_catalog`**: Tracks available metrics
- **`perp_metadata`**: Symbol/exchange configuration

### UI Controls
- **Top Controls**: View DB/Alerts toggle, DB APIs On/Off, Core Trader On/Off, Backtester
- **Dynamic Status Box**: Always visible system status and recent errors
- **Main View**: Database viewer with data type selectors (OHLCV, Funding Rates, etc.)
- **Future**: Alert cards grid view

### Master API Structure
- **Dynamic Discovery**: Scans `apis/` folder for all `.js` scripts
- **Execution Modes**: 
  - Backfill: Run all history scripts once
  - Live: Run all current scripts continuously (1min intervals)
- **Plugin System**: Drop new script in `apis/` → auto-executed
- **Error Handling**: Isolated script failures don't stop others

## 🚀 Getting Started
1. `node g-symbols.js` - Generate dynamic symbols
2. `node master-api.js` - Start data collection (backfill → live)
3. `node server.js` - Start web interface
4. Visit `http://localhost:3000` - View data and status

## 📁 File Naming Convention (apis/ folder)
- `c-*` = Coinalyze source
- `b-*` = Binance source  
- `ccxt-*` = CCXT library
- `*-h` = History (backfill)
- `*-c` = Current (real-time)
- Examples: `c-oi-h.js`, `b-ohlcv-c.js`, `ccxt-fr-c.js`