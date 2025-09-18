# ExpiryTrack - Product Documentation

## Executive Summary

ExpiryTrack is a modern web-based financial data management application designed to capture, store, and manage historical trading data for expired derivative contracts (Futures and Options) from the Upstox trading platform. The application features an intuitive web interface with a step-by-step collection wizard, real-time progress monitoring, and zero-configuration setup. It focuses on building a comprehensive time-series database of 1-minute interval historical data for expired contracts, collecting the last 3 months of data before each contract's expiry.

## Product Vision

### Problem Statement
Expired contract data is crucial for:
- Historical strategy backtesting
- Market behavior analysis
- Risk assessment across different expiry periods
- Pattern recognition in derivative markets
- Building machine learning models for trading

However, this data is:
- Not readily available in organized formats
- Difficult to collect systematically
- Resource-intensive to maintain
- Critical for serious quantitative analysis

### Solution
ExpiryTrack automates the entire workflow of:
1. Discovering available expired contracts
2. Fetching historical data systematically
3. Storing data in an efficient time-series database
4. Providing easy access for analysis

## Core Features

### 1. Automated Expiry Discovery
- **Dynamically fetches** all available expiry dates from Upstox API
- **No hardcoded dates** - always queries live API for accurate expiry information
- Supports up to 6 months of historical expiries (API limitation)
- Handles both weekly and monthly expiry cycles
- Automatically identifies expired contracts vs active contracts

### 2. Contract Data Retrieval
- **Options Contracts**: Retrieves all Call (CE) and Put (PE) options for each expiry
- **Futures Contracts**: Fetches futures contract details for each expiry date
- Captures complete contract metadata including:
  - Strike prices
  - Lot sizes
  - Trading symbols
  - Instrument keys
  - Contract specifications

### 3. Historical Data Collection
- Retrieves 1-minute interval OHLCV data
- **3-Month Historical Range**: Downloads last 3 months of data before expiry
- Includes Open Interest tracking
- Supports multiple timeframes (1min, 3min, 5min, 15min, 30min)
- Real-time progress monitoring with detailed logs
- Handles large-scale data efficiently with async processing

### 4. Web Interface
- **4-Step Collection Wizard**: Intuitive guided process
- **Individual Selection**: Checkbox-based selection for instruments and expiries
- **Real-Time Progress**: Live updates with progress bars and statistics
- **Status Dashboard**: Monitor active and completed tasks
- **Settings Management**: Secure credential configuration

### 5. Database Management
- Efficient time-series storage using SQLite
- Encrypted credential storage with Fernet
- Optimized for analytical queries
- Data compression and indexing
- Explicit transaction commits for data integrity

## System Architecture

### Components Overview

```
┌─────────────────────┐
│   Web Interface     │
│  (Flask + DaisyUI)  │
└──────────┬──────────┘
           │
┌──────────▼──────────┐
│   Auth Manager      │
│ (Upstox OAuth 2.0)  │
└──────────┬──────────┘
           │
┌──────────▼──────────┐
│   Task Manager      │
│  (Async Processing) │
└──────────┬──────────┘
           │
     ┌─────┴─────┬────────────┬─────────────┐
     │           │            │             │
┌────▼───┐ ┌────▼────┐ ┌─────▼────┐ ┌──────▼─────┐
│Expiry  │ │Contract │ │Historical│ │  Database  │
│Fetcher │ │Fetcher  │ │  Data    │ │   Writer   │
│        │ │         │ │ Fetcher  │ │            │
└────┬───┘ └────┬────┘ └─────┬────┘ └──────┬─────┘
     │          │            │              │
     └──────────┴────────────┴──────────────┘
                        │
                ┌───────▼────────┐
                │  SQLite/DuckDB │
                │   (Storage)    │
                └────────────────┘
```

### Data Flow

1. **Authentication Phase**
   - User logs in via Upstox OAuth
   - Access token stored securely
   - Session management initialized

2. **Discovery Phase**
   - Fetch available underlying instruments
   - Retrieve expiry dates for each instrument
   - Build work queue of contracts to process

3. **Collection Phase**
   - For each expiry date:
     - Fetch all option contracts (CE & PE)
     - Fetch futures contracts
     - Retrieve historical 1-min data
   - Handle rate limiting and retries

4. **Storage Phase**
   - Transform data to optimized format
   - Insert into time-series tables
   - Update metadata and indices
   - Maintain data integrity

## Database Schema

### Core Tables

#### 1. instruments
```sql
CREATE TABLE instruments (
    instrument_key TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT,
    exchange TEXT,
    segment TEXT,
    underlying_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 2. expiries
```sql
CREATE TABLE expiries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_key TEXT,
    expiry_date DATE,
    is_weekly BOOLEAN,
    contracts_fetched BOOLEAN DEFAULT FALSE,
    data_fetched BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrument_key) REFERENCES instruments(instrument_key)
);
```

#### 3. contracts
```sql
CREATE TABLE contracts (
    expired_instrument_key TEXT PRIMARY KEY,
    instrument_key TEXT,
    expiry_date DATE,
    contract_type TEXT, -- 'FUT', 'CE', 'PE'
    strike_price DECIMAL(10,2),
    trading_symbol TEXT,
    lot_size INTEGER,
    tick_size DECIMAL(10,2),
    exchange_token TEXT,
    metadata JSON,
    FOREIGN KEY (instrument_key) REFERENCES instruments(instrument_key)
);
```

#### 4. historical_data (Time-series optimized)
```sql
CREATE TABLE historical_data (
    expired_instrument_key TEXT,
    timestamp TIMESTAMP,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    volume BIGINT,
    open_interest BIGINT,
    PRIMARY KEY (expired_instrument_key, timestamp),
    FOREIGN KEY (expired_instrument_key) REFERENCES contracts(expired_instrument_key)
);

-- Partitioning by month for better query performance
CREATE INDEX idx_historical_date ON historical_data(DATE(timestamp));
CREATE INDEX idx_historical_instrument ON historical_data(expired_instrument_key);
```

#### 5. job_status
```sql
CREATE TABLE job_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type TEXT,
    instrument_key TEXT,
    expiry_date DATE,
    status TEXT, -- 'pending', 'running', 'completed', 'failed'
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0
);
```

## API Integration

### Upstox API Endpoints Used

1. **Get Expiries**
   - Endpoint: `/v2/expired-instruments/expiries`
   - Returns: List of available expiry dates
   - Rate Limit: Standard tier

2. **Get Option Contracts**
   - Endpoint: `/v2/expired-instruments/option/contract`
   - Parameters: instrument_key, expiry_date
   - Returns: All option contracts for the expiry

3. **Get Future Contracts**
   - Endpoint: `/v2/expired-instruments/future/contract`
   - Parameters: instrument_key, expiry_date
   - Returns: Future contract details

4. **Get Historical Candle Data**
   - Endpoint: `/v2/expired-instruments/historical-candle/{key}/{interval}/{to}/{from}`
   - Returns: OHLCV + Open Interest data

### Rate Limiting Strategy
**Upstox API Rate Limits (per user):**
- **50 requests per second**
- **500 requests per minute**
- **2000 requests per 30 minutes**

Implementation approach:
- Implement exponential backoff on rate limit errors
- Use token bucket algorithm for request throttling
- Queue management for bulk operations
- Parallel processing with intelligent throttling to stay within limits

### HTTP Client Implementation
- **Use httpx library** for all API requests (async support, connection pooling, HTTP/2)
- Built-in retry mechanisms with httpx
- Better timeout handling compared to requests
- Support for concurrent requests with httpx.AsyncClient
- Automatic rate limit tracking and compliance

## User Workflows

### Initial Setup
1. Launch ExpiryTrack application
2. Authenticate with Upstox credentials
3. Grant necessary permissions
4. Configure database location
5. Select instruments to track

### Data Collection Workflow
1. **Manual Mode**
   - Select specific instruments
   - Choose date ranges
   - Initiate collection
   - Monitor progress

2. **Automated Mode**
   - Configure scheduled jobs
   - Set collection intervals
   - Auto-retry failed jobs
   - Email notifications on completion

### Data Access
1. **Direct Database Access**
   - SQLite/DuckDB connection
   - SQL queries for analysis
   - Export capabilities

2. **API Access** (Future Enhancement)
   - REST endpoints for data retrieval
   - Pagination support
   - Filter and aggregation options

## Performance Considerations

### Data Volume Estimates
- Per contract per day: Variable based on actual trading hours
- **No hardcoded market timings** - data collection based on actual available data
- Per expiry (100 strikes): ~37,500 records (approximate)
- Monthly volume (4 expiries): ~150,000 records (approximate)
- Annual volume: ~1.8 million records per instrument (approximate)
- Actual volumes depend on market holidays and trading sessions

### Optimization Strategies
1. **Storage**
   - Data compression
   - Columnar storage (DuckDB)
   - Partition by time periods
   - Archive old data

2. **Processing**
   - Batch inserts
   - Connection pooling
   - Async I/O operations
   - Progress checkpointing

3. **Querying**
   - Proper indexing
   - Materialized views for common queries
   - Query result caching
   - Read replicas for analysis

## Security & Compliance

### Data Security
- Encrypted token storage
- Secure API communication (HTTPS)
- Local database encryption option
- No sensitive trading data exposure

### Compliance
- Adheres to Upstox API terms
- Respects rate limiting
- Data usage for analysis only
- No redistribution of raw data

## Roadmap & Future Enhancements

### Phase 1 (Current)
- ✅ Basic authentication
- ✅ Core API integration
- 🔄 Database schema implementation
- 🔄 Basic data collection

### Phase 2 (Next Quarter)
- Advanced scheduling system
- Multi-instrument parallel processing
- Data quality checks
- Basic analytics dashboard

### Phase 3 (Future)
- Cloud storage integration
- Distributed processing
- Real-time data streaming
- Advanced analytics features
- ML model integration support

## Technical Requirements

### System Requirements
- **OS**: Windows 10+, macOS 10.15+, Linux (Ubuntu 20.04+)
- **Memory**: Minimum 4GB RAM (8GB recommended)
- **Storage**: 50GB+ available space
- **Network**: Stable internet connection

### Software Dependencies
- Python 3.9+
- SQLite 3.35+ or DuckDB 0.8+
- Required Python packages (see requirements.txt)

## Support & Maintenance

### Error Handling
- Comprehensive logging system
- Error recovery mechanisms
- Data integrity checks
- Rollback capabilities

### Monitoring
- Job status tracking
- Data collection metrics
- API usage statistics
- Performance monitoring

## Conclusion

ExpiryTrack transforms the complex task of managing expired contract historical data into an automated, reliable process. By systematically collecting and organizing this valuable data, it enables traders, researchers, and analysts to perform sophisticated backtesting and analysis that would otherwise be impossible or extremely time-consuming.

The application serves as a critical infrastructure component for serious quantitative trading operations, providing the historical depth necessary for developing and validating trading strategies in the derivatives market.