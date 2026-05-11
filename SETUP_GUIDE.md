# ExpiryTrack Setup Guide

## Quick Setup

Follow these steps to get ExpiryTrack running on your system.

## Prerequisites

1. Python 3.10 or higher
2. Upstox Developer Account (with the **Plus Plan** active — required for
   expired-contract historical data)
3. Windows / Linux / macOS

## Step 1: Install Dependencies

```bash
# Recommended: uv (handles the venv automatically)
uv sync

# Alternative: pip
pip install -r requirements.txt
```

ExpiryTrack uses two databases:
* `data/expirytrack.db` — SQLite (config, instruments, contracts, jobs)
* `data/market_data.duckdb` — DuckDB (all OHLCV+OI bars)

Both are auto-created on first run; you don't need to provision anything.

## Step 2: Configure Environment

The app has sensible defaults, so a `.env` file is **optional**. If you want
to override paths or other knobs, copy the template and edit:

```bash
cp .env.example .env
```

Key settings:

```env
# SQLite metadata DB (small)
DATABASE_PATH=./data/expirytrack.db

# DuckDB market-data store (grows with collected candles)
MARKET_DATA_DB_PATH=./data/market_data.duckdb
```

Credentials are stored encrypted in SQLite via the web Settings page —
you do **not** need to put `UPSTOX_API_KEY` / `UPSTOX_API_SECRET` in `.env`.

## Step 3: Initialize (optional)

```bash
python scripts/init_database.py
```

This pre-creates the SQLite schema. Skipping the step is fine — the
DatabaseManager runs the same DDL the first time the app starts.

## Step 4: Test Connection

```bash
python main.py test
```

## Usage Options

### Option 1: Command Line Interface (CLI)

The CLI provides full control over data collection:

```bash
# Authenticate with Upstox
python main.py authenticate

# Get expiries for an instrument
python main.py get-expiries --instrument "NSE_INDEX|Nifty 50"

# Get contracts for specific expiry
python main.py get-contracts --instrument "NSE_INDEX|Nifty 50" --expiry "2025-08-28"

# Collect all data (auto mode)
python main.py collect --instrument "NSE_INDEX|Nifty 50" --months 6

# Resume incomplete collection
python main.py resume

# Check database status
python main.py status

# Optimize database
python main.py optimize
```

### Option 2: Web Interface

Run the Flask application for a graphical interface:

```bash
python expirytrack_app.py
```

Then open your browser to: http://127.0.0.1:5000

Web interface features:
- Configure API credentials via UI
- OAuth authentication flow
- Real-time collection monitoring
- Database statistics dashboard

### Option 3: Quick Collection Script

For testing with the latest expiry:

```bash
python scripts/quick_collect.py
```

## Common Commands

### Check Database Status
```bash
python main.py status
```

Output:
```
ExpiryTrack Database Status
==================================================
Instruments: 1
Expiries: 52
Contracts: 2,450
Historical Candles: 1,234,567   (DuckDB / market_data.duckdb)
--------------------------------------------------
Pending Expiries: 0
Pending Contracts: 45
==================================================
SQLite metadata: data/expirytrack.db
DuckDB market data: data/market_data.duckdb
```

### Monitor Rate Limits

The application automatically manages rate limits, but you can monitor usage:

```python
# In Python script
from src.api.client import UpstoxAPIClient
client = UpstoxAPIClient()
client.print_rate_limit_dashboard()
```

Output:
```
==================================================
Rate Limit Status Dashboard
==================================================
Second    : [████████████░░░░░░░░] 30/45 (66.7%)
Minute    : [██████░░░░░░░░░░░░░░] 150/450 (33.3%)
Half Hour : [████░░░░░░░░░░░░░░░░] 400/1800 (22.2%)
--------------------------------------------------
Total Requests: 12,345
Errors: 0
==================================================
```

## Troubleshooting

### Authentication Issues

If authentication fails:
1. Check API credentials in `.env`
2. Ensure redirect URL matches exactly in Upstox app settings
3. Clear tokens and retry:
```bash
python main.py clear-auth
python main.py authenticate
```

### Rate Limit Errors

If you hit rate limits:
1. The application automatically backs off
2. Reduce worker count in `.env`:
```env
MAX_WORKERS=5  # Reduce from 10
```

### Database Lock Errors

ExpiryTrack uses two databases:

* SQLite (`data/expirytrack.db`) — WAL mode, multiple readers + one writer.
* DuckDB (`data/market_data.duckdb`) — DuckDB allows only one read-write
  process per file across the OS.

If you see *"Cannot open file ... process cannot access"* on the DuckDB
file, the Flask app is already running and another script (a test, a
notebook) is trying to open the same file. **Stop the Flask app first**
(or open the DuckDB file read-only in your script: `duckdb.connect(path,
read_only=True)`).

Within the Flask process, MarketDataStore uses a single shared connection
across collector, exporter, and `/query` so they never deadlock.

### Missing Data

If data appears incomplete:
1. Check job status in database
2. Resume collection:
```bash
python main.py resume
```
3. Verify specific contracts:
```bash
python main.py get-contracts --instrument "NSE_INDEX|Nifty 50" --expiry "2025-08-28"
```

## Advanced Configuration

### Re-clustering for fast reads after large loads

After ingesting a lot of new contracts (e.g., a fresh 6-month collection),
the rows in `market_data.duckdb` are in insertion order — not in the order
queries filter on. Running `MarketDataStore.compact()` once rewrites the
table sorted by `(base_symbol, expiry_date, contract_type, strike_price,
openalgo_symbol, ts)`. After that, zone-maps line up with the columns
queries filter on and big scans get noticeably faster.

```python
from src.database import MarketDataStore
MarketDataStore.instance().compact()
```

`compact()` is safe to call repeatedly. It's expensive (rewrites the whole
table), so run it after a big batch and not after every contract.

### Custom Rate Limits

Adjust rate limits for safety:
```env
MAX_REQUESTS_SEC=40  # More conservative
MAX_REQUESTS_MIN=400
MAX_REQUESTS_30MIN=1600
```

### Batch Processing

Configure batch sizes:
```env
BATCH_SIZE=5000  # Records per insert
CHECKPOINT_INTERVAL=100  # Contracts per checkpoint
```

## Data Access

### Direct SQL Queries

Connect to the database:
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('./data/expirytrack.db')

# Example: Get all NIFTY 23000 CE options
query = """
    SELECT c.trading_symbol, c.expiry_date, h.*
    FROM contracts c
    JOIN historical_data h ON c.expired_instrument_key = h.expired_instrument_key
    WHERE c.strike_price = 23000
    AND c.contract_type = 'CE'
    ORDER BY h.timestamp
"""

df = pd.read_sql_query(query, conn)
```

### Export Data

Export to CSV:
```python
# Export specific contract data
df.to_csv('nifty_23000_ce.csv', index=False)
```

## Performance Tips

1. **Run during off-market hours** for faster collection
2. **Use SSD storage** for database performance
3. **Enable HTTP/2** (already configured with httpx)
4. **Monitor memory usage** with large datasets
5. **Schedule regular VACUUM** for SQLite optimization

## Support

- Check logs in `./logs/` directory
- Enable debug logging:
```env
LOG_LEVEL=DEBUG
```
- For issues, check the [Issue Tracker](https://github.com/yourusername/expirytrack/issues)

## Next Steps

1. ✅ Complete setup
2. ✅ Test with one instrument
3. 📊 Start collecting historical data
4. 🔄 Schedule regular updates
5. 📈 Analyze your data!

---

Happy data collecting! 🚀