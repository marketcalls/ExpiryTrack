# ExpiryTrack User Guide

## 🎯 User-Friendly Interface

ExpiryTrack now features a simplified, user-friendly interface for collecting expired contract data. No more complex instrument codes - just simple, recognizable names!

## 📊 Supported Instruments

The application supports the following major indices with easy-to-remember names:

| User-Friendly Name | Upstox Instrument Key | Exchange |
|-------------------|----------------------|----------|
| **Nifty 50** | NSE_INDEX\|Nifty 50 | NSE |
| **Bank Nifty** | NSE_INDEX\|Nifty Bank | NSE |
| **Sensex** | BSE_INDEX\|SENSEX | BSE |

## 🖥️ Command Line Interface

### Setup Credentials (One-time)
```bash
python main.py setup
```
Enter your Upstox API Key and Secret when prompted. These are stored encrypted in the database.

### Authenticate
```bash
python main.py authenticate
```
This opens your browser for Upstox OAuth login.

### Collect Data

#### Interactive Mode (Recommended for beginners)
```bash
python main.py collect
```
The system will show you available instruments and let you choose:
```
1. Nifty 50
2. Bank Nifty
3. Sensex
4. All instruments

Enter your choice: 1,2  # Select multiple with comma-separated numbers
```

#### Collect Specific Instruments
```bash
# Single instrument
python main.py collect -i "Nifty 50"

# Multiple instruments
python main.py collect -i "Nifty 50" -i "Bank Nifty"

# All instruments
python main.py collect --all
```

#### With Custom Options
```bash
python main.py collect -i "Nifty 50" --months 3 --concurrent 10
```

### Get Expiry Dates
```bash
# Check available expiry dates for an instrument
python main.py get-expiries --instrument "Nifty 50"
```

### Get Contracts for Expiry
```bash
# Get all contracts for a specific expiry
python main.py get-contracts --instrument "Bank Nifty" --expiry 2025-08-28
```

### Check Status
```bash
# View database statistics
python main.py status
```

## 🌐 Web Interface

### Start the Web Application
```bash
python expirytrack_app.py
```
Open your browser to: http://127.0.0.1:5000

### Web Features

#### 1. Dashboard
- View collection statistics
- Monitor database size
- Check recent collections

#### 2. Collection Page
- **Checkbox Selection**: Simply check the instruments you want
- **Select All Option**: One-click to select all instruments
- **Visual Interface**: Clean, modern interface with progress tracking
- **Collection Options**:
  - Historical Months (1-6 months)
  - Data Interval (1min, 3min, 5min, 15min, 30min)
  - Concurrent Workers (1-20)

#### 3. Settings Page
- Configure API credentials
- View current configuration
- Update settings as needed

## 💡 Tips for Best Results

### 1. Start Small
Begin with one instrument and 1 month of data to test the setup:
```bash
python main.py collect -i "Nifty 50" --months 1
```

### 2. Use Interactive Mode
If unsure, use the interactive mode which guides you through selection:
```bash
python main.py collect  # Without any options
```

### 3. Monitor Progress
The collection process shows real-time progress:
```
Starting data collection
Instruments: Nifty 50, Bank Nifty
Months back: 6
----------------------------------------
Collecting: Nifty 50
Progress: [████████████████████] 100%
----------------------------------------
Collecting: Bank Nifty
Progress: [████████░░░░░░░░░░░░] 40%
```

### 4. Resume on Failure
If collection fails, you can resume from where it stopped:
```bash
python main.py resume
```

## 📊 Data Access

Once data is collected, you can query it using SQL:

```python
import pandas as pd
import sqlite3

# Connect to database
conn = sqlite3.connect('./data/expirytrack.db')

# Example: Get all Nifty 50 option data
query = """
    SELECT
        c.trading_symbol,
        c.strike_price,
        c.contract_type,
        h.timestamp,
        h.close,
        h.volume
    FROM contracts c
    JOIN historical_data h ON c.expired_instrument_key = h.expired_instrument_key
    WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
    AND c.expiry_date = '2025-08-28'
    ORDER BY h.timestamp
"""

df = pd.read_sql_query(query, conn)
print(df.head())
```

## 🔧 Troubleshooting

### Issue: "No API credentials found!"
**Solution**: Run `python main.py setup` and enter your Upstox API credentials.

### Issue: "Please authenticate first"
**Solution**: Run `python main.py authenticate` to login to Upstox.

### Issue: "Rate limit exceeded"
**Solution**: The application automatically handles rate limits. If you see this error frequently, reduce the concurrent workers:
```bash
python main.py collect --concurrent 5
```

### Issue: Collection seems slow
**Solution**: This is normal for large data collections. The application respects Upstox rate limits (50 req/sec). For faster collection:
- Use maximum workers: `--concurrent 15`
- Collect specific instruments instead of all
- Reduce the time period: `--months 1`

## 📞 Support

For issues or questions:
1. Check the logs in `logs/expirytrack.log`
2. Run `python main.py status` to verify database health
3. Report issues on GitHub with error messages

## 🎉 Quick Start Example

Here's a complete example from setup to data collection:

```bash
# 1. Setup credentials (one-time)
python main.py setup
# Enter API Key: your_key_here
# Enter API Secret: your_secret_here

# 2. Authenticate
python main.py authenticate
# Browser opens for login

# 3. Collect Nifty 50 data for last 3 months
python main.py collect -i "Nifty 50" --months 3

# 4. Check status
python main.py status
# Shows: Contracts: 15,000, Historical Candles: 1,500,000

# 5. Start web interface
python expirytrack_app.py
# Open http://127.0.0.1:5000 in browser
```

That's it! You're now collecting expired contract data with ExpiryTrack's user-friendly interface.