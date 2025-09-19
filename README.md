# ExpiryTrack

**Zero-Config Web-Based Historical Data Collection for Expired F&O Contracts**

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Flask](https://img.shields.io/badge/flask-2.0+-green.svg)](https://flask.palletsprojects.com/)

ExpiryTrack is a modern web application that systematically collects, stores, and manages historical trading data for expired Futures and Options contracts from the Upstox platform. Built with a user-friendly interface and zero-configuration philosophy, it makes historical data collection effortless.

## 🌟 Key Features

- **🎯 Web-Based Interface**: Clean, intuitive UI with step-by-step wizard
- **🔐 Zero Configuration**: Encrypted credential storage - no .env files needed
- **📊 Multi-Instrument Support**: Pre-configured for Nifty 50, Bank Nifty, and Sensex
- **📈 3-Month Historical Data**: Automatically downloads last 3 months before expiry
- **⚡ Real-Time Progress**: Live monitoring with detailed logs and statistics
- **🔄 Async Processing**: Efficient background task management
- **🛡️ Secure**: OAuth 2.0 authentication with encrypted storage
- **📤 Easy Data Export**: Web-based export wizard and CLI tool for CSV, JSON, and ZIP formats
- **📅 Separate Date/Time Columns**: Exports include individual date and time columns for easy analysis
- **💹 Open Interest Data**: Full OI (Open Interest) data included in exports

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

#### 2. Create Virtual Environment (Recommended)

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

#### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

Note: This installs all dependencies including `openpyxl` for Excel export functionality.

#### 4. Run the Application

```bash
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

## 🗂️ Project Structure

```
ExpiryTrack/
├── app.py                  # Main Flask application
├── main.py                 # CLI interface (optional)
├── requirements.txt        # Python dependencies
├── .env.example           # Configuration template
├── src/                   # Source code
│   ├── api/              # Upstox API client
│   ├── auth/             # Authentication manager
│   ├── collectors/       # Data collection logic
│   ├── database/         # Database operations
│   └── utils/            # Utilities
├── templates/            # HTML templates
│   ├── base.html        # Base template
│   ├── index.html       # Home page
│   ├── settings.html    # Settings page
│   ├── dashboard.html   # Dashboard
│   ├── collect_wizard.html # Collection wizard
│   ├── export_wizard.html  # Export wizard
│   └── status.html      # Status page
├── src/export/          # Export functionality
│   └── exporter.py      # Data export logic
├── exports/             # Exported data files
├── data/                 # Database storage
├── logs/                 # Application logs
└── design/              # Documentation
```

## 📊 Data Storage

### Database Location
- SQLite database: `data/expirytrack.db`
- Encrypted credentials stored in database
- No sensitive data in plain text files

### Data Structure
- **Historical Data**: 1-minute OHLCV candles with timestamps
- **Contract Info**: Strike prices, expiry dates, instrument keys
- **OpenAlgo Symbols**: User-friendly symbology for easy querying
- **Collection Metadata**: Task status, progress, logs

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

Query contracts using OpenAlgo symbols:

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