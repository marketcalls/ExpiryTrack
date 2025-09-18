# ExpiryTrack

**Zero-Config Automated Historical Data Collection for Expired Derivatives Contracts**

ExpiryTrack is a specialized Python application that systematically collects, stores, and manages historical trading data for expired Futures and Options contracts from the Upstox platform, building a comprehensive time-series database for quantitative analysis and backtesting.

## 🎯 Why ExpiryTrack?

Expired contract data is the backbone of serious derivatives trading research, yet it's notoriously difficult to obtain and manage. ExpiryTrack solves this by automating the entire data pipeline with a zero-configuration approach.

### Key Benefits
- **Zero Configuration**: No .env files needed - encrypted credential storage in database
- **Multi-Instrument Default**: Supports NSE_INDEX|Nifty 50, NSE_INDEX|Nifty Bank, BSE_INDEX|SENSEX out of the box
- **Comprehensive Coverage**: Collect up to 6 months of historical expired contracts
- **High Granularity**: 1-minute interval OHLCV data with open interest
- **Efficient Storage**: Optimized time-series database using SQLite/DuckDB
- **Automated Pipeline**: Set it and forget it - fully automated collection
- **Research Ready**: Data structured for immediate analysis and backtesting

## 🚀 Features

- ✅ **Zero-Config Design**: Encrypted database storage for credentials
- ✅ **Multi-Instrument Support**: NSE, BSE indices and stocks
- ✅ **Complete Contract Coverage**: All strikes for Options (CE/PE) and Futures
- ✅ **Historical Depth**: 6 months of expired contract data
- ✅ **1-Minute Resolution**: Fine-grained time series data
- ✅ **Smart Recovery**: Checkpoint-based resume on failures
- ✅ **Rate Limit Management**: Handles 50/sec, 500/min, 2000/30min limits
- ✅ **Data Validation**: Built-in integrity checks
- ✅ **Parallel Processing**: Up to 50 requests/second throughput
- ✅ **Modern HTTP**: Uses httpx with HTTP/2 support

## 📚 Documentation

- **[Product Documentation](./design/PRODUCT_DOCUMENTATION.md)** - Complete product overview, architecture, and roadmap
- **[Technical Implementation Guide](./design/TECHNICAL_IMPLEMENTATION_GUIDE.md)** - Detailed technical specifications and design patterns
- **[Developer Quick Start](./design/DEVELOPER_QUICKSTART.md)** - Get up and running in 5 minutes
- **[API Rate Limits Guide](./design/API_RATE_LIMITS.md)** - Understanding and managing Upstox API rate limits
- **[Application Summary](./design/APPLICATION_SUMMARY.md)** - Overview of built components and features

## 🏗️ System Architecture

```
User → Setup Credentials → Authentication → Data Orchestrator → Parallel Collectors → Database
                                                    ↓
                                           [Expiry Fetcher]
                                           [Contract Fetcher]
                                           [Historical Data Fetcher]
```

## 💾 Data Pipeline

1. **Discovery**: Dynamically fetch all available expiry dates from API
2. **Mapping**: Get all contracts for each expiry
3. **Collection**: Fetch 1-min historical data
4. **Storage**: Store in optimized time-series format with encryption

## 🔧 Quick Start

```bash
# Install
pip install -r requirements.txt

# Setup credentials (one-time, stored encrypted in database)
python main.py setup

# Authenticate with Upstox
python main.py authenticate

# Start collecting for all default instruments
python main.py collect

# Or collect for specific instrument
python main.py collect --instrument "NSE_INDEX|Nifty 50"
```

## 🔐 Zero-Config Features

ExpiryTrack eliminates the need for .env configuration files:

- **Encrypted Credentials**: API keys stored securely in database using machine-specific encryption
- **Default Instruments**: Pre-configured for major indices (Nifty 50, Nifty Bank, SENSEX)
- **Sensible Defaults**: All settings have optimal defaults - just run and collect
- **Optional Overrides**: Environment variables can still override performance settings if needed

## 📊 Sample Data Query

```python
import pandas as pd
import sqlite3

# Connect to database
conn = sqlite3.connect('./data/expirytrack.db')

# Get option chain data
query = """
    SELECT
        strike_price,
        contract_type,
        MAX(close) as max_price,
        SUM(volume) as total_volume
    FROM contracts c
    JOIN historical_data h ON c.expired_instrument_key = h.expired_instrument_key
    WHERE c.expiry_date = '2025-08-28'
    GROUP BY strike_price, contract_type
"""

df = pd.read_sql_query(query, conn)
```

## 📈 Use Cases

- **Backtesting**: Test strategies on actual historical data
- **Greeks Analysis**: Calculate historical option Greeks
- **Volatility Studies**: Analyze implied vs realized volatility
- **Market Microstructure**: Study intraday patterns
- **Risk Management**: Historical scenario analysis
- **ML Training**: Build predictive models on quality data

## 🗂️ Database Schema

### Core Tables
- `instruments` - Underlying instruments
- `expiries` - Available expiry dates
- `contracts` - Contract specifications
- `historical_data` - 1-minute OHLCV data
- `job_status` - Collection job tracking
- `credentials` - Encrypted API credentials (NEW)
- `default_instruments` - Pre-configured instruments (NEW)

## ⚙️ Configuration

Optional settings via environment variables (see `.env.example`):
```env
# Performance tuning (optional - defaults work well)
MAX_WORKERS=10          # Parallel workers
MAX_REQUESTS_SEC=45     # Safety margin below 50/sec limit
BATCH_SIZE=5000         # Insert batch size
HISTORICAL_MONTHS=6     # Months to collect
LOG_LEVEL=INFO          # Logging level
```

## 🔒 Security

- OAuth 2.0 authentication
- Machine-specific encrypted credential storage
- HTTPS API communication
- No plaintext credentials in files
- Database-level encryption support

## 📋 Requirements

- Python 3.9+
- 4GB RAM (8GB recommended)
- 50GB+ storage space
- Stable internet connection
- Upstox Developer Account

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](./CONTRIBUTING.md) for details.

## 📄 License

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)** - see [LICENSE](./LICENSE.md) file for details.

### What this means:
- ✅ You can use, modify, and distribute this software
- ✅ You must disclose source code when distributing
- ✅ You must license modifications under AGPL-3.0
- ✅ You must provide source code to users of network services
- ✅ Copyright and license notices must be preserved

## ⚠️ Disclaimer

This software is for educational and research purposes only. Users are responsible for complying with all applicable laws and regulations regarding financial data usage. Always respect Upstox API terms of service and rate limits.

## 🆘 Support

- 📖 [Full Documentation](./design/)
- 🐛 [Issue Tracker](https://github.com/yourusername/expirytrack/issues)

## 🌟 Roadmap

- [x] Core data collection pipeline
- [x] Zero-config credential management
- [x] Multi-instrument support
- [x] SQLite storage backend
- [x] Encrypted credential storage
- [ ] DuckDB integration
- [ ] Web dashboard
- [ ] Cloud storage support
- [ ] Real-time streaming
- [ ] Advanced analytics module
- [ ] Docker containerization

## 🎉 Key Principles

1. **Never Hardcode**: All expiry dates and market data fetched dynamically from API
2. **Zero Config**: Works out of the box with sensible defaults
3. **Security First**: Encrypted storage for all sensitive data
4. **Rate Compliant**: Respects all Upstox API rate limits
5. **Resume Ready**: Checkpoint-based recovery from failures

---

**Built for the quantitative trading community**

*Transform expired contracts into trading insights with ExpiryTrack*