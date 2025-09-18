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

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- Upstox Developer Account ([Get it here](https://api.upstox.com/))
- 4GB RAM (8GB recommended)
- 10GB+ free disk space

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
│   └── status.html      # Status page
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
- **Collection Metadata**: Task status, progress, logs

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

**Built with ❤️ for the Quantitative Trading Community**

*Transform expired contracts into actionable trading insights with ExpiryTrack*