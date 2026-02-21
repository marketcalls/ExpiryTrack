# ExpiryTrack Feature Roadmap

Generated: 2026-02-16

---

## Top 10 Recommendations (Prioritized)

| # | Feature | Effort | Impact |
|---|---------|--------|--------|
| 1 | Fix Settings page port mismatch (says 5000, should be 5005) | 15 min | Critical bug |
| 2 | Support more instruments (FinNifty, Midcap, BANKEX) + dynamic UI | 1-2 days | High |
| 3 | Fix N+1 export queries (CSV/JSON/ZIP use per-contract queries vs Parquet's single JOIN) | 1 day | Massive perf gain |
| 4 | Option Chain Explorer page (traditional CE/PE table by strike) | 2-3 days | Highest value for F&O researchers |
| 5 | OI Analysis & Max Pain calculator (OI build-up, PCR, max pain strike) | 2-3 days | Most requested by traders |
| 6 | Candlestick Chart Viewer (view OHLCV data as interactive charts in-app) | 2 days | Makes data immediately usable |
| 7 | Token expiry handling (show time remaining, warn before collection) | 1 day | Prevents frustrating failures |
| 8 | One-click Smart Collect (auto-detect new expiries, fetch only missing) | 1 day | Simplifies daily workflow |
| 9 | Concurrent task protection (prevent multiple collections at once) | 2 hours | Prevents API abuse |
| 10 | Read/write connection separation for DuckDB | 1 day | Unlocks concurrent ops |

---

## Category 1: Data Collection Improvements

### 1.1 Support for More Instruments
**Priority: HIGH | Complexity: LOW**

The instrument mapper (`src/utils/instrument_mapper.py`) is hardcoded to only 3 instruments: Nifty 50, Bank Nifty, and Sensex. The collect wizard (`templates/collect_wizard.html`) has these same 3 hardcoded as HTML checkboxes.

Add support for Nifty Financial Services (FinNifty), Nifty Midcap Select, Nifty Next 50, and BSE instruments like BANKEX. The `OpenAlgoSymbolGenerator.SYMBOL_MAPPING` in `src/utils/openalgo_symbol.py` already has mappings for FINNIFTY, NIFTYNXT50, MIDCPNIFTY, BANKEX -- but these are not wired up to the instrument mapper or UI. Both wizard templates need to dynamically render instruments from the database `default_instruments` table rather than hardcoding HTML.

### 1.2 Custom Instrument Management via UI
**Priority: HIGH | Complexity: MEDIUM**

No UI for managing instruments. The `default_instruments` table exists and `setup_default_instruments()` in `src/database/manager.py` seeds 3 defaults, but users cannot add/remove/reorder instruments through the web interface.

Add an "Instruments" management section to the Settings page where users can add custom Upstox instrument keys, toggle active/inactive, set priority order, and validate them against the API before saving.

### 1.3 Selective Strike Range Collection
**Priority: MEDIUM | Complexity: MEDIUM**

When collecting options data, the system fetches ALL strikes for each expiry -- including deep OTM strikes that have zero volume. This wastes API calls and storage.

Add a strike range filter to Step 2 of the collection wizard. Options: "All Strikes", "ATM +/- N strikes", "Strikes with volume > 0", or a custom min/max range.

### 1.4 Incremental/Delta Collection (Smart Collect)
**Priority: HIGH | Complexity: MEDIUM**

The collection wizard asks users to manually select expiry dates every time. No "Collect everything new since last run" button. The scheduler's `_run_daily_collection` auto-detects expiries but the web wizard does not.

Add a "Smart Collect" button to the home page that automatically detects new expiries, identifies unfetched contracts, and starts collection. The Download Status page already shows missing contracts -- bridge that data into a one-click action.

### 1.5 Collection Estimation
**Priority: MEDIUM | Complexity: LOW**

Before starting a download, the user has no idea how many API calls it will take, how long it will run, or how much storage it will consume.

After the user selects expiries in Step 3, display: estimated API calls, estimated time, estimated storage in MB, and whether it will hit the rate cap.

### 1.6 Retry Logic for Failed Contracts
**Priority: HIGH | Complexity: LOW**

In `src/collectors/task_manager.py`, `_fetch_contract_data` has a bare `raise` on failure. The `job_status` table has a `retry_count` column but there is no actual retry mechanism with exponential backoff.

Implement automatic retry (up to 3 attempts with exponential backoff). If a contract fails after all retries, mark it in a `failed_contracts` table with the error reason, and show on Download Status page with a "Retry Failed" button.

---

## Category 2: Analytics and Visualization

### 2.1 Option Chain Explorer
**Priority: HIGH | Complexity: MEDIUM**

The database already has `get_option_chain()` in `src/database/manager.py`, but there is no UI to visualize an option chain.

Add an "Option Chain Explorer" page showing a traditional option chain table: strike prices down the left, CE data on the left half, PE data on the right half. Show OI, volume, OHLC for each strike. This is the single most valuable feature for F&O researchers.

### 2.2 Open Interest Analysis Charts
**Priority: HIGH | Complexity: MEDIUM**

The `historical_data` table stores an `oi` column, but the analytics engine has no OI-related queries. None of the analytics charts use OI data.

Add: (a) OI build-up chart across strikes, (b) Put-Call ratio by OI, (c) Max Pain calculator, (d) OI change heatmap across strikes and dates.

### 2.3 Historical Price Chart Viewer
**Priority: HIGH | Complexity: MEDIUM**

The system collects millions of OHLCV candles but has no way to view them as charts. Users must export data to external tools.

Add a "Data Browser" page with a Chart.js or lightweight-charts candlestick chart. Let users select instrument, expiry, and contract, then render interactive OHLC with volume overlay. Add timeframe aggregation (1min, 5min, 15min, 1hr, daily).

### 2.4 Volume Profile / Volume-at-Price
**Priority: MEDIUM | Complexity: MEDIUM**

Volume data is stored per candle but never aggregated by price level.

Add a volume profile chart showing distribution of traded volume across price levels. Valuable for identifying support/resistance zones.

### 2.5 Comparative Expiry Analysis
**Priority: MEDIUM | Complexity: MEDIUM**

No way to compare metrics across expiries (e.g., "Was December expiry more volatile than November?").

Add a comparison dashboard: select 2-3 expiry dates and overlay metrics: total volume, average spread, active strikes, OI concentration, day-wise volume patterns.

### 2.6 Data Freshness Indicator
**Priority: LOW | Complexity: LOW**

Home page shows total candles but not when data was last collected.

Show "Last collection: X hours ago" and "Newest data date: YYYY-MM-DD" on the home page and analytics dashboard.

---

## Category 3: Export Enhancements

### 3.1 Excel (XLSX) Export Format
**Priority: MEDIUM | Complexity: LOW**

`openpyxl` is already in requirements.txt but never used. No Excel export option.

Add XLSX export using openpyxl or pandas `to_excel()`. Include formatted header row, auto-column-width, and optionally separate worksheets per expiry or contract type.

### 3.2 Amibroker/MetaTrader Export Format
**Priority: MEDIUM | Complexity: MEDIUM**

Many F&O traders use Amibroker or MetaTrader for backtesting. Current export formats don't match these platforms.

Add export templates for Amibroker (specific CSV format) and MetaTrader (HST/CSV format). Pre-format dates and symbols per platform requirements.

### 3.3 Streaming / Chunked Export for Large Datasets
**Priority: MEDIUM | Complexity: MEDIUM**

CSV and JSON exports build the entire dataset in memory (`all_data = []`). Could exhaust memory for large datasets.

Use Flask's streaming response with generators for CSV, and DuckDB's native COPY TO for CSV (similar to Parquet export).

### 3.4 Export History and Re-download
**Priority: LOW | Complexity: LOW**

Export tasks stored in memory dict, cleaned up after 1 hour, lost on server restart.

Save export metadata to a `export_history` database table. Show past exports with re-download links.

### 3.5 Export Filtering by Contract Type
**Priority: LOW | Complexity: LOW**

Export wizard lets users select instruments and expiries, but not filter by contract type (CE only, PE only, FUT only).

Add contract type filter checkboxes to export options.

---

## Category 4: UX and UI Improvements

### 4.1 Responsive Mobile Navigation
**Priority: MEDIUM | Complexity: LOW**

Navbar has 8+ links in horizontal flex. Overflows on mobile.

Add a mobile hamburger menu using DaisyUI's drawer component.

### 4.2 Dark Mode Support
**Priority: LOW | Complexity: LOW**

`data-theme="light"` is hardcoded. DaisyUI supports dark themes natively.

Add dark/light mode toggle in navbar. Store preference in localStorage.

### 4.3 Keyboard Shortcuts
**Priority: LOW | Complexity: LOW**

No keyboard shortcuts exist.

Add: `Ctrl+K` for command palette, `G C` for Collect, `G E` for Export, `G A` for Analytics, `G S` for Status.

### 4.4 Toast Notifications Instead of Alerts
**Priority: MEDIUM | Complexity: LOW**

Several UI interactions use `alert()` which blocks the page.

Replace all `alert()` calls with DaisyUI toast components that auto-dismiss.

### 4.5 Progress Persistence Across Page Navigation
**Priority: MEDIUM | Complexity: MEDIUM**

If user starts collection then navigates away, they lose progress visibility.

Add persistent progress indicator in navbar during active collection. Use polling from `base.html`.

### 4.6 Breadcrumb Navigation
**Priority: LOW | Complexity: LOW**

No breadcrumbs. Add to secondary pages (e.g., "Home > Analytics").

### 4.7 Settings Page Redirect URL Fix
**Priority: HIGH | Complexity: LOW**

Settings template says redirect URL is `http://127.0.0.1:5000/upstox/callback` but actual default port is 5005. This mismatch causes OAuth failures for new users.

Fix hardcoded port references to dynamically use `{{ config.PORT }}` or change to 5005.

---

## Category 5: Data Quality and Integrity

### 5.1 Auto-Fix for Quality Violations
**Priority: MEDIUM | Complexity: MEDIUM**

Quality checker only reports violations, cannot fix them.

Add "Fix Issues" button: (a) re-mark orphan contracts as `data_fetched=FALSE`, (b) deduplicate timestamps, (c) flag/remove OHLC integrity violations.

### 5.2 Market Hours Validation
**Priority: LOW | Complexity: LOW**

`MARKET_OPEN`, `MARKET_CLOSE`, `EXPECTED_CANDLES_PER_DAY` constants are defined but never used in any check.

Add check for candles outside market hours and days with suspiciously low candle counts.

### 5.3 Data Completeness Scoring
**Priority: MEDIUM | Complexity: LOW**

Download Status shows per-expiry completion but not per-contract data completeness (e.g., contract marked fetched but has only 50 candles).

Add completeness score: compare actual vs expected trading days per contract.

### 5.4 Scheduled Quality Checks
**Priority: LOW | Complexity: LOW**

Quality checks must be triggered manually. Scheduler doesn't run them.

Add weekly scheduled quality check job, store results in `quality_reports` table.

---

## Category 6: Performance Improvements

### 6.1 Connection Pooling for DuckDB
**Priority: HIGH | Complexity: MEDIUM**

`get_connection()` creates a new connection per operation with a class-level write lock. All operations (including reads) are serialized.

Separate read and write paths. Allow concurrent reads with read-only connection pool while keeping write lock for insertions.

### 6.2 Analytics Query Caching
**Priority: MEDIUM | Complexity: LOW**

Every analytics dashboard load fires 6+ expensive queries scanning the entire `historical_data` table.

Add short-lived cache (TTL 60-300 seconds). Invalidate when collection task completes.

### 6.3 Pagination for Download Status
**Priority: MEDIUM | Complexity: LOW**

Download status API returns ALL rows with no pagination. Could be hundreds of rows.

Add server-side pagination (limit/offset) and lazy-loading.

### 6.4 Materialized Summary Table
**Priority: MEDIUM | Complexity: MEDIUM**

`get_summary_stats()` runs 6 separate COUNT(*) queries on every home/status page load.

Create a `summary_stats` table updated after each collection batch.

### 6.5 Fix Export N+1 Query Problem
**Priority: HIGH | Complexity: MEDIUM**

CSV, JSON, ZIP exports have N+1 queries: for each instrument, for each expiry, query contracts, then for each contract query historical data. Parquet export correctly uses a single JOIN.

Rewrite CSV/JSON/ZIP exports to use a single JOIN query like Parquet. Could reduce export time from minutes to seconds.

---

## Category 7: Reliability and Operations

### 7.1 Health Check Endpoint
**Priority: MEDIUM | Complexity: LOW**

No `/health` endpoint for monitoring.

Add endpoint returning: app status, DB connectivity, disk space, token validity, scheduler status, last collection timestamp.

### 7.2 Graceful Token Expiry Handling
**Priority: HIGH | Complexity: MEDIUM**

Upstox tokens expire after ~24 hours. No proactive warning. Long collections can fail mid-run.

(a) Show token expiry time in navbar, (b) Warn before starting if token will expire mid-collection, (c) Fail fast if < 1 hour remains on token.

### 7.3 Database Backup and Restore
**Priority: MEDIUM | Complexity: LOW**

No backup mechanism.

Add `main.py backup` CLI command and Settings page button. Also add `main.py restore`.

### 7.4 Export File Cleanup
**Priority: LOW | Complexity: LOW**

Exported files accumulate in `exports/` with no cleanup.

Add scheduled job to delete exports older than N days. Show disk usage on Status page.

### 7.5 Concurrent Task Protection
**Priority: MEDIUM | Complexity: LOW**

Nothing prevents starting multiple collection tasks simultaneously. Could overwhelm rate limits and cause write contention.

Check before starting: if any task is RUNNING, refuse or queue the new task.

### 7.6 Persistent Task History
**Priority: MEDIUM | Complexity: MEDIUM**

Collection tasks stored only in memory, lost on server restart. `job_status` table exists but unused by web-initiated tasks.

Persist task metadata to `job_status` table. Load recent tasks on restart.

---

## Category 8: New Feature Modules

### 8.1 REST API for External Access
**Priority: MEDIUM | Complexity: MEDIUM**

No way for external tools (Python scripts, Jupyter notebooks) to query historical data without accessing the DB file directly.

Add `/api/v1/data` endpoint with query parameters and API key auth. Turns ExpiryTrack into a data serving platform.

### 8.2 Notifications System
**Priority: LOW | Complexity: MEDIUM**

No notifications when collection completes/fails, scheduler fails, quality issues detected, or token about to expire.

Add pluggable notification system: in-app bell, optional email via SMTP, optional webhook (Telegram/Discord/Slack).

### 8.3 Data Deletion / Cleanup UI
**Priority: MEDIUM | Complexity: LOW**

No way to delete data for a specific expiry/instrument through the UI.

Add "Data Management" page: delete by expiry, by instrument, by date range, with disk space estimate.

### 8.4 Import from External Sources
**Priority: LOW | Complexity: MEDIUM**

Only collects from Upstox API. Cannot import from CSV or other sources.

Add CSV/Parquet import feature that maps columns to ExpiryTrack schema.

### 8.5 Data Comparison Tool
**Priority: LOW | Complexity: MEDIUM**

No way to compare collected data against a reference dataset for accuracy verification.

Add tool that takes reference data and shows OHLCV discrepancies.

### 8.6 Collection Calendar View
**Priority: LOW | Complexity: MEDIUM**

Download Status is a flat table. No visual calendar of data coverage.

Add calendar heatmap (like GitHub contribution graph): green = full coverage, yellow = partial, red = missing, gray = non-trading day.

---

## Risks and Considerations

- **API Rate Limits:** Features adding API calls must respect 50/sec, 500/min, 2000/30min limits in `src/utils/rate_limiter.py`
- **Database Size:** More instruments and OI analysis will grow DuckDB file. Consider data retention policies and periodic VACUUM
- **Upstox API Changes:** Expired instruments API endpoints are specialized. API versioning changes could break collection
- **Memory Usage:** Analytics caching and persistent task history will increase memory footprint
