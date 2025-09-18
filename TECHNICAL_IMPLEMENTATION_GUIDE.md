# ExpiryTrack - Technical Implementation Guide

## Overview
This guide provides detailed technical specifications for implementing ExpiryTrack's core functionality. It covers the complete data pipeline from API integration to database storage.

## Core Data Pipeline Implementation

### 1. Instrument Discovery Pipeline

```python
Pipeline: Instrument Discovery
├── Input: User-selected underlying symbols
├── Process:
│   ├── Validate instrument keys against pattern
│   ├── Fetch available expiry dates
│   ├── Filter for date range (6 months historical)
│   └── Queue expiries for processing
└── Output: List of expiry dates per instrument
```

**Key Considerations:**
- Instrument key format: `{SEGMENT}|{SYMBOL}` (e.g., `NSE_INDEX|Nifty 50`)
- **Always fetch expiry dates from API** - never hardcode or predict
- Maximum 6 months of historical expiries available (API constraint)
- Handle both weekly and monthly expiries dynamically
- No assumptions about market timings or trading hours

### 2. Contract Collection Pipeline

```python
Pipeline: Contract Collection
├── Input: Instrument key + Expiry date
├── Process:
│   ├── Fetch option contracts (parallel)
│   │   ├── Call options (CE)
│   │   └── Put options (PE)
│   ├── Fetch futures contracts
│   ├── Parse contract metadata
│   └── Generate expired_instrument_keys
└── Output: Contract details with unique identifiers
```

**Expired Instrument Key Format:**
- Pattern: `{SEGMENT}|{TOKEN}|{DD-MM-YYYY}`
- Example: `NSE_FO|71706|28-08-2025`

### 3. Historical Data Pipeline

```python
Pipeline: Historical Data Collection
├── Input: List of expired_instrument_keys
├── Process:
│   ├── Batch contracts for efficiency
│   ├── For each contract:
│   │   ├── Calculate date ranges
│   │   ├── Fetch 1-minute candles
│   │   ├── Validate data completeness
│   │   └── Handle missing data
│   ├── Transform to database format
│   └── Bulk insert with checkpointing
└── Output: Time-series data in database
```

## Implementation Modules

### Module 1: Authentication Manager
```python
class AuthManager:
    """Handles Upstox OAuth 2.0 authentication flow"""

    Components:
    - OAuth flow handler
    - Token storage (encrypted)
    - Token refresh mechanism
    - Session management

    Key Methods:
    - initiate_auth()
    - handle_callback()
    - refresh_token()
    - get_valid_token()
```

### Module 2: API Client
```python
class UpstoxAPIClient:
    """Wrapper for Upstox Expired Instruments APIs using httpx"""

    Endpoints:
    - get_expiries(instrument_key)  # Always fetch from API, no hardcoding
    - get_option_contracts(instrument_key, expiry_date)
    - get_future_contracts(instrument_key, expiry_date)
    - get_historical_data(expired_key, interval, from_date, to_date)

    HTTP Client:
    - **Uses httpx library** for all requests
    - Async support with httpx.AsyncClient
    - HTTP/2 support for better performance
    - Connection pooling and keep-alive

    Features:
    - Rate limiting compliance:
      * 50 requests/second maximum
      * 500 requests/minute maximum
      * 2000 requests/30 minutes maximum
    - Token bucket algorithm for rate control
    - Retry logic with exponential backoff
    - Built-in timeout handling
    - Response caching for identical requests
```

### Module 3: Data Processor
```python
class DataProcessor:
    """Processes and transforms API responses"""

    Responsibilities:
    - Parse JSON responses
    - Validate data integrity
    - Handle missing/null values
    - Convert timestamps to UTC
    - Calculate derived metrics

    Data Validations:
    - OHLC relationship (High >= Low, etc.)
    - Volume non-negative
    - Timestamp continuity
    - Contract specification consistency
```

### Module 4: Database Manager
```python
class DatabaseManager:
    """Manages SQLite/DuckDB operations"""

    Operations:
    - Schema creation and migration
    - Bulk insert optimization
    - Transaction management
    - Index maintenance
    - Query optimization

    Performance Features:
    - Prepared statements
    - Batch size optimization (5000 records)
    - WAL mode for SQLite
    - Columnar storage for DuckDB
```

### Module 5: Job Scheduler
```python
class JobScheduler:
    """Orchestrates data collection jobs"""

    Features:
    - Priority queue for jobs
    - Parallel execution (configurable workers)
    - Progress tracking
    - Failure recovery
    - Checkpoint management

    Job Types:
    - EXPIRY_FETCH
    - CONTRACT_FETCH
    - HISTORICAL_DATA_FETCH
    - DATA_VALIDATION
```

## Data Collection Strategy

### Optimal Collection Sequence

1. **Phase 1: Discovery (1 API call per instrument)**
   ```
   For each instrument:
     → Fetch all expiry dates
     → Store in expiries table
     → Mark as pending
   ```

2. **Phase 2: Contract Mapping (2 API calls per expiry)**
   ```
   For each expiry:
     → Fetch option contracts (1 call)
     → Fetch futures contracts (1 call)
     → Extract all expired_instrument_keys
     → Store in contracts table
   ```

3. **Phase 3: Historical Data (1 call per contract per date range)**
   ```
   For each contract:
     → Calculate optimal date ranges
     → Fetch 1-minute data in chunks
     → Validate and store
     → Update progress
   ```

### Rate Limiting Strategy

```python
Rate Limit Configuration (Upstox API):
├── Per-second limit: 50 requests
├── Per-minute limit: 500 requests
├── Per-30-minutes limit: 2000 requests
├── Rate limiter implementation:
│   ├── Token bucket for per-second control
│   ├── Sliding window for minute/30-min tracking
│   └── Request queue with priority ordering
├── Backoff strategy:
│   ├── Initial wait: 1 second
│   ├── Max wait: 60 seconds
│   └── Multiplier: 2
└── Circuit breaker: 5 consecutive failures

# Example rate limiter setup
from typing import deque
import time

class UpstoxRateLimiter:
    def __init__(self):
        self.per_second_limit = 50
        self.per_minute_limit = 500
        self.per_30min_limit = 2000

        self.second_window = deque(maxlen=50)
        self.minute_window = deque(maxlen=500)
        self.thirty_min_window = deque(maxlen=2000)

    def can_make_request(self) -> bool:
        now = time.time()

        # Check per-second limit
        if len(self.second_window) >= self.per_second_limit:
            if now - self.second_window[0] < 1.0:
                return False

        # Check per-minute limit
        if len(self.minute_window) >= self.per_minute_limit:
            if now - self.minute_window[0] < 60.0:
                return False

        # Check per-30-minutes limit
        if len(self.thirty_min_window) >= self.per_30min_limit:
            if now - self.thirty_min_window[0] < 1800.0:
                return False

        return True
```

### Parallel Processing Design

```python
Worker Pool Design (Optimized for 50 req/sec limit):
├── Discovery Worker (1 thread)
│   └── Sequential instrument processing (low frequency)
├── Contract Workers (5 threads)
│   └── Parallel expiry processing (up to 10 req/sec per worker)
├── Data Workers (10 threads)
│   └── Parallel contract data fetching (up to 5 req/sec per worker)
├── Rate Limit Coordinator (1 thread)
│   └── Manages request tokens across all workers
└── Database Writer (2 threads)
    └── Batch insert queue with parallel writes

# With 50 req/sec limit, we can efficiently run:
# - 10-15 parallel workers maximum
# - Each worker throttled to 3-5 req/sec
# - Coordinated through central rate limiter
```

## Database Optimization

### SQLite Configuration
```sql
-- Performance optimizations
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -64000; -- 64MB cache
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 30000000000; -- 30GB mmap

-- Data integrity
PRAGMA foreign_keys = ON;
PRAGMA ignore_check_constraints = OFF;
```

### DuckDB Configuration
```sql
-- Memory management
SET memory_limit = '4GB';
SET threads = 4;

-- Storage optimization
SET enable_compression = true;
SET compression = 'snappy';

-- Query optimization
SET enable_optimizer = true;
SET optimizer_search_depth = 25;
```

### Indexing Strategy
```sql
-- Primary indices for query performance
CREATE INDEX idx_expiry_date ON contracts(expiry_date);
CREATE INDEX idx_contract_type ON contracts(contract_type);
CREATE INDEX idx_strike_price ON contracts(strike_price);

-- Composite indices for common queries
CREATE INDEX idx_instrument_expiry ON contracts(instrument_key, expiry_date);
CREATE INDEX idx_historical_composite ON historical_data(
    expired_instrument_key,
    DATE(timestamp)
);

-- Covering index for analytics
CREATE INDEX idx_analytics ON historical_data(
    expired_instrument_key,
    timestamp,
    close,
    volume,
    open_interest
);
```

## Error Handling & Recovery

### Error Classification
```python
Error Hierarchy:
├── Recoverable Errors
│   ├── Network timeout
│   ├── Rate limit exceeded
│   ├── Temporary API unavailability
│   └── Database lock
├── Partial Failures
│   ├── Missing data for date range
│   ├── Incomplete contract list
│   └── Data validation failures
└── Critical Errors
    ├── Authentication failure
    ├── Invalid API response format
    ├── Database corruption
    └── Disk space exhausted
```

### Recovery Strategies
1. **Automatic Retry**
   - Network errors: 3 retries with exponential backoff
   - Rate limits: Wait and retry after cooldown
   - Database locks: Retry with jitter

2. **Checkpoint Recovery**
   - Save progress after each successful batch
   - Resume from last checkpoint on failure
   - Validate checkpoint integrity

3. **Data Validation Recovery**
   - Mark invalid data for review
   - Continue with valid data
   - Generate validation report

## Performance Metrics

### Key Performance Indicators
```python
Metrics to Track:
├── Throughput Metrics
│   ├── Contracts processed/hour
│   ├── Data points inserted/second
│   └── API calls/minute
├── Latency Metrics
│   ├── API response time
│   ├── Database insert time
│   └── End-to-end processing time
├── Quality Metrics
│   ├── Data completeness %
│   ├── Validation failure rate
│   └── Duplicate detection count
└── Resource Metrics
    ├── Memory usage
    ├── CPU utilization
    ├── Disk I/O
    └── Network bandwidth
```

### Monitoring Implementation
```python
class MetricsCollector:
    """Collects and reports performance metrics"""

    Metrics:
    - Counter: Total API calls
    - Gauge: Active workers
    - Histogram: Response times
    - Summary: Data processing rates

    Reporting:
    - Console dashboard
    - Log file metrics
    - Database metrics table
    - Alert thresholds
```

## Implementation Best Practices

### Dynamic Data Handling
- **Never hardcode expiry dates** - always fetch from API
- **No market timing assumptions** - let data availability drive collection
- **Dynamic contract discovery** - don't assume strike ranges
- **Flexible date ranges** - adapt to actual data availability

### HTTP Client Setup (httpx)
```python
import httpx

# Basic client setup
client = httpx.Client(
    base_url="https://api.upstox.com/v2",
    headers={"Accept": "application/json"},
    timeout=30.0,
    limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
)

# Async client for parallel requests
async_client = httpx.AsyncClient(
    base_url="https://api.upstox.com/v2",
    http2=True,  # Enable HTTP/2
    timeout=httpx.Timeout(30.0),
    limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
)
```

## Testing Strategy

### Unit Tests
- API client methods (mock httpx responses)
- Data validation functions
- Database operations
- Authentication flow

### Integration Tests
- End-to-end data pipeline
- Error recovery mechanisms
- Rate limiting behavior
- Database transactions

### Performance Tests
- Load testing with concurrent requests
- Database query optimization
- Memory leak detection
- Stress testing with large datasets

### Data Quality Tests
- OHLC data validation
- Timestamp continuity
- Contract specification consistency
- Duplicate detection

## Deployment Considerations

### Environment Configuration
```yaml
Development:
  database: sqlite
  api_rate_limit: 1/sec
  workers: 2
  log_level: DEBUG

Production:
  database: duckdb
  api_rate_limit: 3/sec
  workers: 10
  log_level: INFO

Database:
  connection_pool_size: 20
  batch_insert_size: 5000
  checkpoint_interval: 1000

API:
  timeout: 30
  max_retries: 3
  backoff_factor: 2
```

### Resource Requirements
```yaml
Minimum:
  cpu: 2 cores
  memory: 4GB
  disk: 50GB SSD
  network: 10 Mbps

Recommended:
  cpu: 4 cores
  memory: 8GB
  disk: 200GB NVMe
  network: 100 Mbps

Production:
  cpu: 8 cores
  memory: 16GB
  disk: 1TB NVMe
  network: 1 Gbps
```

## Security Implementation

### Secure Token Storage
```python
Token Security:
├── Encryption at rest (AES-256)
├── Key derivation (PBKDF2)
├── Secure key storage (OS keyring)
└── Token rotation schedule
```

### API Security
- HTTPS only communication
- Certificate pinning
- Request signing
- Response validation

### Database Security
- Encrypted database files
- Access control lists
- Audit logging
- Backup encryption

## Conclusion

This technical implementation guide provides the blueprint for building a robust, scalable, and efficient ExpiryTrack system. Following these specifications ensures reliable data collection, optimal performance, and maintainable code architecture.