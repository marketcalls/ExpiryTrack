# ExpiryTrack - Developer Quick Start Guide

## 🚀 Quick Overview

ExpiryTrack is a Python application that systematically collects and stores historical 1-minute trading data for expired Futures & Options contracts from Upstox APIs into a time-series database.

## 📋 Prerequisites

- Python 3.9+
- Upstox Developer Account
- API Credentials (App ID, Secret)
- 50GB+ disk space
- Stable internet connection

## 🔧 Setup in 5 Minutes

### 1. Clone & Install
```bash
# Clone repository
git clone https://github.com/yourusername/expirytrack.git
cd expirytrack

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies (includes httpx for API requests)
pip install -r requirements.txt
```

### 2. Configure Environment
Create `.env` file:
```env
# Upstox API Configuration
UPSTOX_API_KEY=your_api_key
UPSTOX_API_SECRET=your_secret
UPSTOX_REDIRECT_URI=http://localhost:8000/callback

# Database Configuration
DB_TYPE=sqlite  # or 'duckdb'
DB_PATH=./data/expirytrack.db

# Collection Settings
DEFAULT_INSTRUMENT=NSE_INDEX|Nifty 50
HISTORICAL_MONTHS=6
DATA_INTERVAL=1minute

# Performance Settings
MAX_WORKERS=5
BATCH_SIZE=5000
API_RATE_LIMIT=3
```

### 3. Initialize Database
```bash
python scripts/init_database.py
```

## 🎯 Core Usage Examples

### Basic Data Collection Flow

```python
from expirytrack import ExpiryTracker

# Initialize tracker
tracker = ExpiryTracker()

# Authenticate (opens browser for OAuth)
tracker.authenticate()

# Fetch expiries for NIFTY (always from API, never hardcoded)
expiries = tracker.get_expiries("NSE_INDEX|Nifty 50")
print(f"Found {len(expiries)} expiry dates")
# Note: These are dynamically fetched expired dates, not predictions

# Fetch contracts for specific expiry
contracts = tracker.get_contracts(
    instrument="NSE_INDEX|Nifty 50",
    expiry_date="2025-08-28"
)
print(f"Found {len(contracts)} contracts")

# Collect historical data
tracker.collect_historical_data(
    contracts=contracts,
    from_date="2025-08-01",
    to_date="2025-08-28"
)
```

### Automated Collection

```python
# Collect all data for an instrument
tracker.auto_collect(
    instrument="NSE_INDEX|Nifty 50",
    months_back=6
)
```

## 📊 Data Access Examples

### Query Historical Data
```python
import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('./data/expirytrack.db')

# Get 1-min data for specific contract
query = """
    SELECT timestamp, open, high, low, close, volume, open_interest
    FROM historical_data
    WHERE expired_instrument_key = 'NSE_FO|71706|28-08-2025'
    AND DATE(timestamp) = '2025-08-15'
    ORDER BY timestamp
"""

df = pd.read_sql_query(query, conn)
print(df.head())
```

### Find All Contracts for an Expiry
```python
query = """
    SELECT trading_symbol, contract_type, strike_price
    FROM contracts
    WHERE instrument_key = 'NSE_INDEX|Nifty 50'
    AND expiry_date = '2025-08-28'
    ORDER BY strike_price
"""

contracts_df = pd.read_sql_query(query, conn)
print(contracts_df)
```

## 🏗️ Project Structure

```
expirytrack/
├── src/
│   ├── auth/           # Authentication module
│   ├── api/            # API client wrapper
│   ├── collectors/     # Data collection logic
│   ├── database/       # Database operations
│   └── utils/          # Helper functions
├── scripts/
│   ├── init_database.py
│   ├── collect_data.py
│   └── validate_data.py
├── tests/              # Test suite
├── docs/               # API documentation
├── data/               # Database files
└── logs/               # Application logs
```

## 🔄 Common Workflows

### 1. Daily Update Workflow
```bash
# Run daily at market close
python scripts/daily_update.py --instrument "NSE_INDEX|Nifty 50"
```

### 2. Bulk Historical Collection
```bash
# Collect 6 months of data
python scripts/bulk_collect.py \
    --instrument "NSE_INDEX|Nifty 50" \
    --months 6 \
    --workers 5
```

### 3. Data Validation
```bash
# Validate data integrity
python scripts/validate_data.py --check-all
```

## 🐛 Debugging Tips

### Enable Debug Logging
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Check API Response
```python
# Test API connection
from expirytrack.api import test_connection
test_connection()
```

### Monitor Rate Limits
```python
# Check current rate limit status
tracker.api_client.get_rate_limit_status()
```

## 📈 Performance Tips

1. **Batch Operations**: Process contracts in batches of 10-20
2. **Parallel Workers**: Use 10-15 workers max (50 req/sec API limit)
3. **Rate Limit Awareness**: Stay within 50/sec, 500/min, 2000/30min limits
4. **Database Optimization**: Run `VACUUM` weekly on SQLite
5. **Checkpoint Frequently**: Save progress every 100 contracts
6. **Dynamic Scheduling**: Let data availability guide collection timing, not fixed schedules
7. **HTTP/2 with httpx**: Enable for better connection multiplexing
8. **Request Pooling**: Use httpx connection pooling to reduce overhead

## 🚨 Common Issues & Solutions

### Issue: Authentication Failed
```python
# Clear cached tokens
tracker.auth_manager.clear_tokens()
# Re-authenticate
tracker.authenticate()
```

### Issue: Rate Limit Exceeded
```python
# Check current usage against limits
tracker.api_client.check_rate_limits()
# Returns: {'per_second': 45/50, 'per_minute': 450/500, 'per_30min': 1500/2000}

# Throttle if needed
tracker.api_client.set_max_rate(40)  # Max 40 req/second (safety margin)
```

### Issue: Database Lock
```python
# Use WAL mode for SQLite
conn.execute("PRAGMA journal_mode=WAL")
```

### Issue: Incomplete Data
```python
# Resume from checkpoint
tracker.resume_collection(checkpoint_file="checkpoint.json")
```

## 🧪 Testing

```bash
# Run all tests
pytest

# Run specific test module
pytest tests/test_api_client.py

# Run with coverage
pytest --cov=expirytrack
```

## 📝 Key API Endpoints Reference

| Operation | Endpoint | Rate Limits |
|-----------|----------|-------------|
| Get Expiries | `/v2/expired-instruments/expiries` | 50/sec, 500/min, 2000/30min |
| Get Options | `/v2/expired-instruments/option/contract` | 50/sec, 500/min, 2000/30min |
| Get Futures | `/v2/expired-instruments/future/contract` | 50/sec, 500/min, 2000/30min |
| Get Historical | `/v2/expired-instruments/historical-candle/...` | 50/sec, 500/min, 2000/30min |

## 🔍 Useful SQL Queries

### Find Most Traded Contracts
```sql
SELECT
    c.trading_symbol,
    SUM(h.volume) as total_volume
FROM contracts c
JOIN historical_data h ON c.expired_instrument_key = h.expired_instrument_key
WHERE c.expiry_date = '2025-08-28'
GROUP BY c.trading_symbol
ORDER BY total_volume DESC
LIMIT 10;
```

### Get Daily OHLC
```sql
SELECT
    DATE(timestamp) as date,
    MIN(low) as day_low,
    MAX(high) as day_high,
    FIRST_VALUE(open) as day_open,
    LAST_VALUE(close) as day_close,
    SUM(volume) as day_volume
FROM historical_data
WHERE expired_instrument_key = 'NSE_FO|71706|28-08-2025'
GROUP BY DATE(timestamp);
```

## 🔗 Resources

- [Upstox API Documentation](https://upstox.com/developer/api-documentation)
- [Project GitHub](https://github.com/yourusername/expirytrack)
- [Issue Tracker](https://github.com/yourusername/expirytrack/issues)
- [Discord Community](https://discord.gg/expirytrack)

## 💡 Pro Tips

1. **Start Small**: Test with one expiry before bulk collection
2. **Monitor Resources**: Watch memory usage during large collections
3. **Backup Regularly**: Schedule daily database backups
4. **Use DuckDB**: For large datasets (>10GB), switch to DuckDB
5. **Cache Responses**: Enable response caching for repeated queries

## 🎉 Quick Win

Get your first data in 3 commands:
```bash
# 1. Setup
python scripts/init_database.py

# 2. Authenticate
python scripts/authenticate.py

# 3. Collect latest expiry
python scripts/quick_collect.py --latest
```

---

**Need Help?** Check the [full documentation](./PRODUCT_DOCUMENTATION.md) or raise an issue on GitHub!