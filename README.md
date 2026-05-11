# ExpiryTrack

**Zero-Config Web-Based Historical Data Collection for Expired F&O Contracts**

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Flask](https://img.shields.io/badge/flask-2.0+-green.svg)](https://flask.palletsprojects.com/)

ExpiryTrack is a modern web application that systematically collects, stores, and manages historical trading data for expired Futures and Options contracts from the Upstox platform. Built with a user-friendly interface and zero-configuration philosophy, it makes historical data collection effortless.

## 🌟 Key Features

- **Web-Based Interface**: Clean, intuitive UI with step-by-step wizard
- **Zero Configuration**: Encrypted credential storage - no .env files needed
- **Multi-Instrument Support**: Pre-configured for Nifty 50, Bank Nifty, and Sensex
- **3-Month Historical Data**: Automatically downloads last 3 months before expiry
- **Real-Time Progress**: Live monitoring with detailed logs and statistics
- **Async Processing**: Efficient background task management
- **Secure**: OAuth 2.0 authentication with encrypted storage
- **Easy Data Export**: Web-based export wizard and CLI tool for CSV / JSON / ZIP / Parquet
- **DuckDB analytics**: Market data stored in DuckDB for fast columnar scans, vectorized backtesting, and ad-hoc SQL
- **Built-in Query / Scan page**: run read-only DuckDB SQL right from the browser, with CSV / Parquet export
- **Separate Date/Time Columns**: Exports include individual date and time columns for easy analysis
- **Open Interest Data**: Full OI (Open Interest) data included in exports

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- **Upstox Plus Plan** (Required for expired contract data access)
- Upstox Developer Account ([Get it here](https://api.upstox.com/))
- 4GB RAM (8GB recommended)
- 10GB+ free disk space

### ⚠️ Important: Upstox Plus Plan Required

**ExpiryTrack requires the Upstox Plus Plan to access expired contract data.** The Basic Plan does not provide access to historical data for expired derivatives contracts.

#### About Upstox Plus Plan:
- **Free activation** initially (may become chargeable in future with advance notice)
- Access to advanced features and enhanced API capabilities
- Priority access to historical data for expired contracts
- Can switch between Plus and Basic plans anytime (24-hour cooling period applies)

#### How to Activate:
1. Log into your Upstox account
2. Navigate to Settings → Plans
3. Activate the Plus Plan (currently free)
4. Wait for plan activation confirmation

For complete details, see the [Upstox Plus Plan Terms](https://upstox.com/files/terms-and-condition/plus-pack.pdf)

### Installation

#### 1. Clone the Repository

```bash
git clone https://github.com/marketcalls/ExpiryTrack.git
cd ExpiryTrack
```

#### 2. Choose Your Installation Method

##### Option A: Using uv (Recommended)

[uv](https://docs.astral.sh/uv/) is a fast Python package manager that handles virtual environments automatically.

```bash
# Install uv if not already installed
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# Run the application (uv handles everything automatically)
uv run app.py
```

##### Option B: Using pip (Traditional)

```bash
# Create virtual environment
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

Note: This installs all dependencies including `openpyxl` for Excel export functionality.

#### 3. Run the Application

```bash
# If using uv
uv run app.py

# If using pip/venv
python app.py
```

The application will start on `http://localhost:5000`

## 📱 UI Usage Guide

### 1️⃣ Initial Setup

1. **Open Browser**: Navigate to `http://localhost:5000`
2. **Go to Settings**: Click on "Settings" in the navigation menu
3. **Enter API Credentials**:
   - API Key: Your Upstox API key
   - API Secret: Your Upstox API secret
   - Redirect URL: `http://127.0.0.1:5000/callback` (default)
4. **Save Credentials**: Click "Save Credentials"
5. **Authenticate**: Click "Login with Upstox" and complete OAuth flow

### 2️⃣ Data Collection Wizard

Navigate to "Collect Data" to start the 4-step collection wizard:

#### Step 1: Select Instruments
- Choose from pre-configured instruments:
  - ✅ Nifty 50
  - ✅ Bank Nifty
  - ✅ Sensex
- Use checkboxes for individual selection
- "Select All" option available

#### Step 2: Choose Contract Types
- Select the type of contracts to download:
  - 📈 **Options**: Call and Put options for all strikes
  - 📊 **Futures**: Futures contracts
  - 🎯 **Both**: Options and Futures

#### Step 3: Pick Expiry Dates
- View all available expiries for selected instruments
- Individual checkbox selection for specific expiries
- "Select All" button for each instrument
- Shows expiry count for each instrument

#### Step 4: Configure & Download
- **Review Summary**: See selected instruments, types, and expiries
- **Set Interval**: Choose data granularity (1-minute default)
- **Configure Workers**: Set concurrent workers (1-10, default: 5)
- **Start Download**: Click "🚀 Start Download" to begin

### 3️⃣ Monitor Progress

During collection, you'll see:
- **Real-time Progress Bar**: Visual progress indicator
- **Live Statistics**:
  - Expiries processed
  - Contracts downloaded
  - Candles collected
  - Errors (if any)
- **Scrollable Log Window**: Detailed logs with timestamps
- **Color-coded Status**: Success (green), Warning (yellow), Error (red)

### 4️⃣ View Status

Navigate to "Status" page to:
- View database statistics
- See recent collection tasks
- Check task history
- Monitor system health

## Project Structure

```
ExpiryTrack/
├── app.py                  # Flask application
├── main.py                 # CLI entry point
├── requirements.txt        # Python dependencies (latest pinned versions)
├── pyproject.toml          # Project metadata for uv
├── .env / .env.example     # Environment overrides (SQLite + DuckDB paths)
├── src/
│   ├── api/                # Upstox API client (httpx + rate limiter)
│   ├── auth/               # OAuth + encrypted credential storage
│   ├── backtest/           # Backtesting / scanning helpers on top of DuckDB
│   ├── collectors/         # Async collection orchestration + task manager
│   ├── database/
│   │   ├── manager.py      # SQLite metadata DB (config tables)
│   │   └── market_data.py  # DuckDB-backed MarketDataStore (OHLCV+OI)
│   ├── export/             # DuckDB-native CSV / JSON / ZIP / Parquet exporter
│   └── utils/              # Encryption, logging, OpenAlgo symbology, rate limiter
├── templates/
│   ├── base.html, index.html, settings.html, status.html
│   ├── collect_wizard.html, export_wizard.html
│   └── query.html          # New: DuckDB SQL console
├── test/                   # Smoke / integration tests
├── data/
│   ├── expirytrack.db      # SQLite metadata
│   └── market_data.duckdb  # DuckDB market data
├── exports/                # CSV / JSON / ZIP / Parquet outputs
├── logs/                   # Application logs
└── design/                 # Architectural docs
```

## Data Storage

ExpiryTrack uses a **two-database split** so OLTP and analytical workloads
don't compete for the same engine:

| Path | Engine | Holds | Why |
|---|---|---|---|
| `data/expirytrack.db` | **SQLite** | credentials, instruments, expiries, contracts, job status | Small, frequent point reads/writes; encrypted credentials |
| `data/market_data.duckdb` | **DuckDB** | All OHLCV+OI bars (1-minute candles) | Columnar storage, zone-skipping, vectorized analytics, native Parquet export |

Both paths are configurable via `.env`:

```
DATABASE_PATH=./data/expirytrack.db
MARKET_DATA_DB_PATH=./data/market_data.duckdb
```

### DuckDB schema (`market_data`)

A single wide, denormalized table — repeated category columns
(`base_symbol`, `expiry_date`, `contract_type`) cost almost nothing to store
thanks to DuckDB's RLE / dictionary encoding, and let nearly every analytical
query skip joins entirely:

```sql
CREATE TABLE market_data (
    expired_instrument_key  VARCHAR  NOT NULL,
    openalgo_symbol         VARCHAR  NOT NULL,
    base_symbol             VARCHAR  NOT NULL,
    instrument_key          VARCHAR  NOT NULL,
    expiry_date             DATE     NOT NULL,
    contract_type           VARCHAR  NOT NULL,   -- 'CE' | 'PE' | 'FUT'
    strike_price            DOUBLE,
    trading_symbol          VARCHAR,
    ts                      TIMESTAMPTZ NOT NULL,
    open, high, low, close  DOUBLE   NOT NULL,
    volume                  BIGINT   NOT NULL,
    oi                      BIGINT,
    PRIMARY KEY (expired_instrument_key, ts)
);
```

Pre-built resampling views (`market_data_5m`, `_15m`, `_30m`, `_1h`, `_1d`)
are computed on demand from the 1-minute base using DuckDB `time_bucket`.

### Cross-database joins

The SQLite metadata DB is `ATTACH`ed read-only as schema `meta`, so cross-DB
joins are one statement away:

```sql
SELECT m.openalgo_symbol, m.ts, m.close, c.lot_size, c.tick_size
FROM market_data m
JOIN meta.contracts c USING (expired_instrument_key)
WHERE m.base_symbol = 'NIFTY';
```

## 🔤 OpenAlgo Symbology

ExpiryTrack includes **OpenAlgo symbology** - a standardized, user-friendly format for F&O symbols that makes querying the database intuitive and efficient.

### Symbol Format

#### Futures
Format: `[BaseSymbol][DDMMMYY]FUT`
- Example: `BANKNIFTY28MAR24FUT` (Bank Nifty futures expiring March 28, 2024)

#### Options
Format: `[BaseSymbol][DDMMMYY][Strike][CE/PE]`
- Example: `NIFTY28MAR2420800CE` (Nifty 20800 Call expiring March 28, 2024)
- Example: `BANKNIFTY25APR2447500PE` (Bank Nifty 47500 Put expiring April 25, 2024)

### Supported Base Symbols

**NSE Index:**
- `NIFTY` - Nifty 50
- `BANKNIFTY` - Bank Nifty
- `FINNIFTY` - Fin Nifty
- `MIDCPNIFTY` - Midcap Nifty

**BSE Index:**
- `SENSEX` - Sensex
- `BANKEX` - Bankex
- `SENSEX50` - Sensex 50

### Database Queries

Query contract metadata (SQLite) using OpenAlgo symbols:

```python
# Get specific contract
contract = db.get_contract_by_openalgo_symbol('NIFTY28MAR2420800CE')

# Get all BANKNIFTY contracts
contracts = db.get_contracts_by_base_symbol('BANKNIFTY')

# Get option chain
chain = db.get_option_chain('NIFTY', '2024-03-28')

# Get futures
futures = db.get_futures_by_symbol('BANKNIFTY')

# Search symbols
results = db.search_openalgo_symbols('MAR24')
```

### Backtesting / scanning over DuckDB

For OHLCV+OI access, use the vectorized helpers in `src.backtest`. All
functions return pandas DataFrames (zero-copy through Arrow), suitable for
plugging directly into your strategy code:

```python
from src.backtest import bars, chain, scan, BacktestData

# 5-minute resampled bars for one contract
df = bars("NIFTY28APR2624000CE", timeframe="5m")

# Option chain snapshot at a given timestamp
ch = chain("NIFTY", "2026-04-28", at="2026-04-25 14:30")

# Top-volume CE strikes on a given day
hot = scan(base_symbol="NIFTY", contract_type="CE", date="2026-04-25") \
        .nlargest(20, "volume")

# Iterate the chain forward in time for an event-driven backtest
bt = BacktestData("NIFTY", "2026-04-28", timeframe="5m")
for ts, snap in bt.iter_bars():
    # snap is a DataFrame: one row per contract for that timestamp
    ...
```

### Interactive Query / Scan page

The web UI ships with an in-browser DuckDB SQL console at `/query` for
ad-hoc analysis: write `SELECT` / `WITH` queries, view results in a sortable
table, and download the full result set as CSV or Parquet. Read-only by
construction — DDL/DML tokens are rejected at the API boundary.

### SQL Examples

```sql
-- Get specific option
SELECT * FROM contracts
WHERE openalgo_symbol = 'NIFTY28MAR2420800CE';

-- Get all BANKNIFTY options for March
SELECT * FROM contracts
WHERE openalgo_symbol LIKE 'BANKNIFTY%MAR24%'
AND (openalgo_symbol LIKE '%CE' OR openalgo_symbol LIKE '%PE');

-- Get futures expiring in April
SELECT * FROM contracts
WHERE openalgo_symbol LIKE '%APR24FUT';
```

## 📤 Exporting Data

ExpiryTrack provides two powerful ways to export historical data using OpenAlgo symbols:

### Web-Based Export Wizard

Navigate to "Export Data" in the web interface to use the intuitive 4-step export wizard:

#### Step 1: Select Instruments
- Choose from configured instruments (Nifty 50, Bank Nifty, Sensex)
- Select multiple instruments for batch export

#### Step 2: Choose Expiry Dates
- View available expiries for selected instruments
- Select specific expiries or use "Select All"
- Shows expiry count per instrument

#### Step 3: Export Options
- **Format**: CSV, JSON, or ZIP archive
- **Include OpenAlgo Symbols**: Add standardized symbology
- **Include Metadata**: Add contract details (strike, option type)
- **Time Range**: All data or specific periods
- **Separate Files**: Export each contract individually

#### Step 4: Review & Export
- Review summary of selections
- Click "Start Export" to begin
- Real-time progress tracking
- Download link provided when complete

### Command-Line Export Tool

ExpiryTrack also includes a powerful command-line export tool for automation and scripting.

### Quick Export Examples

```bash
# Export single symbol to CSV (default)
python export_openalgo_data.py NIFTY28AUG25FUT

# Export to Excel format with two sheets (data + metadata)
python export_openalgo_data.py NIFTY28AUG2522600CE --format excel

# Export to JSON format
python export_openalgo_data.py BANKNIFTY28AUG2547500PE --format json

# Export to custom directory
python export_openalgo_data.py NIFTY28AUG25FUT --output my_exports
```

### Search and Batch Export

```bash
# Search for all NIFTY August 2025 contracts
python export_openalgo_data.py --search NIFTY28AUG25

# Export all matching contracts with auto-confirmation
python export_openalgo_data.py --search NIFTY28AUG25 --auto --format excel

# Export all 22600 strike options
python export_openalgo_data.py --search 22600 --auto
```

### Export Output

Files are saved in the `exports` directory with timestamps:
- **CSV**: Contains columns in order: `openalgo_symbol, date, time, timestamp, open, high, low, close, volume, oi`
- **Excel**: Two sheets - Historical Data and Contract Info
- **JSON**: Structured format with contract metadata and historical data
- **ZIP**: Archive containing multiple CSV files (when separate files option is selected)

Example output:
```
Exporting data for: NIFTY28AUG25FUT
Trading Symbol: NIFTY FUT 28 AUG 25
Contract Type: FUT
Expiry Date: 2025-08-28
Total Data Points: 23250

Exported to: exports/NIFTY28AUG25FUT_20250918_224910.csv
```

For detailed export documentation, see [EXPORT_GUIDE.md](EXPORT_GUIDE.md)

## 🔍 Export Features

### Data Columns in Exports

All exports include the following columns:
- **openalgo_symbol**: Standardized F&O symbol (e.g., NIFTY16SEP25C22700)
- **date**: Trading date (YYYY-MM-DD)
- **time**: Trading time (HH:MM:SS)
- **timestamp**: Full ISO timestamp
- **open**: Opening price
- **high**: High price
- **low**: Low price
- **close**: Closing price
- **volume**: Trading volume
- **oi**: Open Interest

### Metadata Columns (Optional)

When metadata is included:
- **instrument**: Instrument name
- **expiry**: Expiry date
- **strike**: Strike price
- **option_type**: CE (Call) or PE (Put) or FUT (Futures)
- **trading_symbol**: Original trading symbol

## 🔧 Configuration (Optional)

While ExpiryTrack works with zero configuration, you can customize settings:

1. Copy `.env.example` to `.env`
2. Modify settings as needed:
   ```env
   # Flask settings
   FLASK_ENV=development
   SECRET_KEY=your-secret-key

   # Data collection
   HISTORICAL_DAYS=90  # 3 months
   MAX_WORKERS=5

   # Rate limiting
   MAX_REQUESTS_PER_SECOND=45
   ```

## 🛠️ Troubleshooting

### Common Issues

#### Application won't start
```bash
# Check Python version
python --version  # Should be 3.8+

# Reinstall dependencies
pip install -r requirements.txt --upgrade
```

#### Authentication fails
- Verify API credentials in Settings
- Check redirect URL matches Upstox app settings
- Ensure `http://127.0.0.1:5000/callback` is whitelisted

#### Data not downloading
- Check logs in the progress window
- Verify internet connection
- Ensure market hours for historical data availability

#### Template errors
- Clear browser cache
- Restart the application
- Check `templates/` folder exists

## 📈 Usage Tips

1. **Best Collection Times**: Run during market hours for latest data
2. **Optimal Workers**: Use 3-5 workers for stable performance
3. **Data Range**: 3 months historical data is optimal balance
4. **Regular Updates**: Schedule weekly collections for latest expiries
5. **Monitor Logs**: Check progress window for any issues

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the GNU Affero General Public License v3.0 - see [LICENSE](LICENSE.md) file for details.

## 🙏 Acknowledgments

- [Upstox API](https://upstox.com/developer/api-documentation) for providing market data access
- Flask community for the excellent web framework
- Contributors and users of ExpiryTrack

## 📧 Support

- **Documentation**: [Full docs](./design/)
- **Issues**: [GitHub Issues](https://github.com/marketcalls/ExpiryTrack/issues)
- **Discussions**: [GitHub Discussions](https://github.com/marketcalls/ExpiryTrack/discussions)

## 🔗 Links

- **GitHub Repository**: [https://github.com/marketcalls/ExpiryTrack](https://github.com/marketcalls/ExpiryTrack)
- **Upstox Developer**: [https://api.upstox.com/](https://api.upstox.com/)
- **Documentation**: [Design Docs](./design/)

---

## 📄 Disclaimer

**Disclaimer:** ExpiryTrack is an independent, open-source application developed by individual developers. We are not affiliated with, endorsed by, or associated with Upstox or any of its brands, subsidiaries, or related entities. This application uses publicly available Upstox APIs for educational and research purposes. All trademarks, logos, and brand names are the property of their respective owners. Users are responsible for compliance with Upstox's terms of service and API usage policies.

---

**Built with ❤️ for the Quantitative Trading Community**

*Transform expired contracts into actionable trading insights with ExpiryTrack*