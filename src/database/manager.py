"""
Database Manager for ExpiryTrack
Handles SQLite/DuckDB operations with optimized time-series storage
"""
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date
import logging
from contextlib import contextmanager

from ..config import config

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Database manager for time-series expired contract data
    """

    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize database manager

        Args:
            db_path: Path to database file
        """
        self.db_path = db_path or config.DB_PATH
        self.db_type = config.DB_TYPE

        # Create database directory if needed
        self.db_path.parent.mkdir(exist_ok=True, parents=True)

        # Initialize database
        self._init_database()

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            if self.db_type == 'sqlite':
                conn = sqlite3.connect(str(self.db_path))
                conn.row_factory = sqlite3.Row
                # Enable optimizations
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("PRAGMA synchronous = NORMAL")
                conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
                conn.execute("PRAGMA temp_store = MEMORY")
                conn.execute("PRAGMA foreign_keys = ON")
            else:
                # DuckDB support can be added here
                raise NotImplementedError("DuckDB support coming soon")

            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def _init_database(self) -> None:
        """Initialize database schema"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Check if contracts table exists and needs migration
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='contracts'")
            if cursor.fetchone():
                # Table exists, check for openalgo_symbol column
                cursor.execute("PRAGMA table_info(contracts)")
                columns = [col[1] for col in cursor.fetchall()]

                if 'openalgo_symbol' not in columns:
                    # Add the new column to existing table
                    cursor.execute("ALTER TABLE contracts ADD COLUMN openalgo_symbol TEXT")
                    conn.commit()
                    logger.info("Added openalgo_symbol column to contracts table")

                    # Create index for the new column
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_openalgo_symbol ON contracts(openalgo_symbol)")
                    conn.commit()
                    logger.info("Created index for openalgo_symbol column")

            # Create credentials table for encrypted storage
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS credentials (
                    id INTEGER PRIMARY KEY,
                    api_key TEXT NOT NULL,
                    api_secret TEXT NOT NULL,
                    redirect_uri TEXT DEFAULT 'http://127.0.0.1:5000/upstox/callback',
                    access_token TEXT,
                    token_expiry REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create default instruments table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS default_instruments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instrument_key TEXT UNIQUE NOT NULL,
                    symbol TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    priority INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create instruments table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS instruments (
                    instrument_key TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    name TEXT,
                    exchange TEXT,
                    segment TEXT,
                    underlying_type TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create expiries table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS expiries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instrument_key TEXT NOT NULL,
                    expiry_date DATE NOT NULL,
                    is_weekly BOOLEAN,
                    contracts_fetched BOOLEAN DEFAULT FALSE,
                    data_fetched BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (instrument_key) REFERENCES instruments(instrument_key),
                    UNIQUE(instrument_key, expiry_date)
                )
            """)

            # Create contracts table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS contracts (
                    expired_instrument_key TEXT PRIMARY KEY,
                    instrument_key TEXT NOT NULL,
                    expiry_date DATE NOT NULL,
                    contract_type TEXT NOT NULL,
                    strike_price DECIMAL(10,2),
                    trading_symbol TEXT NOT NULL,
                    openalgo_symbol TEXT,  -- OpenAlgo symbology for easy querying
                    lot_size INTEGER,
                    tick_size DECIMAL(10,2),
                    exchange_token TEXT,
                    freeze_quantity INTEGER,
                    minimum_lot INTEGER,
                    metadata JSON,
                    data_fetched BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (instrument_key) REFERENCES instruments(instrument_key)
                )
            """)

            # Create historical_data table (optimized for time-series)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS historical_data (
                    expired_instrument_key TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open DECIMAL(10,2) NOT NULL,
                    high DECIMAL(10,2) NOT NULL,
                    low DECIMAL(10,2) NOT NULL,
                    close DECIMAL(10,2) NOT NULL,
                    volume BIGINT NOT NULL,
                    open_interest BIGINT,
                    PRIMARY KEY (expired_instrument_key, timestamp),
                    FOREIGN KEY (expired_instrument_key) REFERENCES contracts(expired_instrument_key)
                )
            """)

            # Create job_status table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_type TEXT NOT NULL,
                    instrument_key TEXT,
                    expiry_date DATE,
                    contract_key TEXT,
                    status TEXT NOT NULL,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    checkpoint JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indices for performance
            indices = [
                "CREATE INDEX IF NOT EXISTS idx_expiry_date ON contracts(expiry_date)",
                "CREATE INDEX IF NOT EXISTS idx_contract_type ON contracts(contract_type)",
                "CREATE INDEX IF NOT EXISTS idx_strike_price ON contracts(strike_price)",
                "CREATE INDEX IF NOT EXISTS idx_openalgo_symbol ON contracts(openalgo_symbol)",  # Index for OpenAlgo symbols
                "CREATE INDEX IF NOT EXISTS idx_instrument_expiry ON contracts(instrument_key, expiry_date)",
                "CREATE INDEX IF NOT EXISTS idx_historical_date ON historical_data(DATE(timestamp))",
                "CREATE INDEX IF NOT EXISTS idx_historical_instrument ON historical_data(expired_instrument_key)",
                "CREATE INDEX IF NOT EXISTS idx_job_status ON job_status(status, job_type)"
            ]

            for index in indices:
                cursor.execute(index)

            logger.info("Database schema initialized successfully")

    # Credentials operations
    def save_credentials(self, api_key: str, api_secret: str, redirect_uri: str = None) -> bool:
        """Save encrypted credentials to database"""
        from ..utils.encryption import encryption

        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Encrypt sensitive data
            encrypted_key = encryption.encrypt(api_key)
            encrypted_secret = encryption.encrypt(api_secret)

            # Check if credentials exist
            cursor.execute("SELECT COUNT(*) FROM credentials")
            exists = cursor.fetchone()[0] > 0

            if exists:
                cursor.execute("""
                    UPDATE credentials
                    SET api_key = ?, api_secret = ?, redirect_uri = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                """, (encrypted_key, encrypted_secret, redirect_uri or 'http://127.0.0.1:5000/upstox/callback'))
            else:
                cursor.execute("""
                    INSERT INTO credentials (api_key, api_secret, redirect_uri)
                    VALUES (?, ?, ?)
                """, (encrypted_key, encrypted_secret, redirect_uri or 'http://127.0.0.1:5000/upstox/callback'))

            return True

    def get_credentials(self) -> Optional[Dict]:
        """Get decrypted credentials from database"""
        from ..utils.encryption import encryption

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM credentials WHERE id = 1")
            row = cursor.fetchone()

            if row:
                return {
                    'api_key': encryption.decrypt(row['api_key']),
                    'api_secret': encryption.decrypt(row['api_secret']),
                    'redirect_uri': row['redirect_uri'],
                    'access_token': encryption.decrypt(row['access_token']) if row['access_token'] else None,
                    'token_expiry': row['token_expiry']
                }
            return None

    def save_token(self, access_token: str, expiry: float) -> bool:
        """Save encrypted access token"""
        from ..utils.encryption import encryption

        with self.get_connection() as conn:
            cursor = conn.cursor()
            encrypted_token = encryption.encrypt(access_token)

            cursor.execute("""
                UPDATE credentials
                SET access_token = ?, token_expiry = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            """, (encrypted_token, expiry))

            return True

    # Default instruments operations
    def setup_default_instruments(self) -> bool:
        """Setup default instruments for collection"""
        default_instruments = [
            {'key': 'NSE_INDEX|Nifty 50', 'symbol': 'Nifty 50', 'priority': 1},
            {'key': 'NSE_INDEX|Nifty Bank', 'symbol': 'Nifty Bank', 'priority': 2},
            {'key': 'BSE_INDEX|SENSEX', 'symbol': 'SENSEX', 'priority': 3},
        ]

        with self.get_connection() as conn:
            cursor = conn.cursor()

            for inst in default_instruments:
                cursor.execute("""
                    INSERT OR IGNORE INTO default_instruments (instrument_key, symbol, priority)
                    VALUES (?, ?, ?)
                """, (inst['key'], inst['symbol'], inst['priority']))

            logger.info("Default instruments configured")
            return True

    def get_default_instruments(self) -> List[str]:
        """Get list of default instruments to collect"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT instrument_key FROM default_instruments
                WHERE is_active = TRUE
                ORDER BY priority
            """)
            return [row[0] for row in cursor.fetchall()]

    # Instrument operations
    def insert_instrument(self, instrument_data: Dict) -> bool:
        """Insert or update instrument"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO instruments
                (instrument_key, symbol, name, exchange, segment, underlying_type)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                instrument_data['instrument_key'],
                instrument_data['symbol'],
                instrument_data.get('name'),
                instrument_data.get('exchange'),
                instrument_data.get('segment'),
                instrument_data.get('underlying_type')
            ))
            return True

    # Expiry operations
    def insert_expiries(self, instrument_key: str, expiry_dates: List[str]) -> int:
        """Insert multiple expiry dates"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            count = 0
            for expiry_date in expiry_dates:
                try:
                    # Determine if weekly (simplified logic)
                    date_obj = datetime.strptime(expiry_date, '%Y-%m-%d')
                    is_weekly = date_obj.weekday() == 3  # Thursday

                    cursor.execute("""
                        INSERT OR IGNORE INTO expiries
                        (instrument_key, expiry_date, is_weekly)
                        VALUES (?, ?, ?)
                    """, (instrument_key, expiry_date, is_weekly))

                    if cursor.rowcount > 0:
                        count += 1
                except Exception as e:
                    logger.error(f"Failed to insert expiry {expiry_date}: {e}")

            logger.info(f"Inserted {count} new expiries for {instrument_key}")
            return count

    def get_pending_expiries(self, instrument_key: str) -> List[Dict]:
        """Get expiries that haven't been processed"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM expiries
                WHERE instrument_key = ? AND contracts_fetched = FALSE
                ORDER BY expiry_date
            """, (instrument_key,))
            return [dict(row) for row in cursor.fetchall()]

    # Contract operations
    def insert_contracts(self, contracts: List[Dict]) -> int:
        """Insert multiple contracts"""
        from ..utils.openalgo_symbol import to_openalgo_symbol

        with self.get_connection() as conn:
            cursor = conn.cursor()
            count = 0

            for contract in contracts:
                try:
                    # Extract expired instrument key
                    expired_key = contract.get('instrument_key', '')

                    # Generate OpenAlgo symbol
                    openalgo_symbol = to_openalgo_symbol(contract)

                    cursor.execute("""
                        INSERT OR REPLACE INTO contracts
                        (expired_instrument_key, instrument_key, expiry_date,
                         contract_type, strike_price, trading_symbol, openalgo_symbol,
                         lot_size, tick_size, exchange_token, freeze_quantity, minimum_lot, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        expired_key,
                        contract.get('underlying_key', ''),
                        contract.get('expiry', ''),
                        contract.get('instrument_type', ''),  # CE, PE, FUT
                        contract.get('strike_price'),
                        contract.get('trading_symbol', ''),
                        openalgo_symbol,  # Add OpenAlgo symbol
                        contract.get('lot_size'),
                        contract.get('tick_size'),
                        contract.get('exchange_token', ''),
                        contract.get('freeze_quantity'),
                        contract.get('minimum_lot'),
                        json.dumps(contract)  # Store full contract as metadata
                    ))

                    if cursor.rowcount > 0:
                        count += 1
                except Exception as e:
                    logger.error(f"Failed to insert contract {contract.get('trading_symbol')}: {e}")

            logger.info(f"Inserted {count} contracts")
            return count

    def get_pending_contracts(self, limit: int = 100) -> List[Dict]:
        """Get contracts that need historical data fetched"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM contracts
                WHERE data_fetched = FALSE
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    # Historical data operations
    def insert_historical_data(self, expired_instrument_key: str, candles: List[List]) -> int:
        """
        Insert historical OHLCV data

        Args:
            expired_instrument_key: Contract identifier
            candles: List of [timestamp, open, high, low, close, volume, oi]

        Returns:
            Number of records inserted
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            count = 0

            # Prepare batch insert
            data_to_insert = []
            for candle in candles:
                try:
                    # Parse candle data
                    timestamp = candle[0]
                    open_price = float(candle[1])
                    high = float(candle[2])
                    low = float(candle[3])
                    close = float(candle[4])
                    volume = int(candle[5])
                    open_interest = int(candle[6]) if len(candle) > 6 else None

                    data_to_insert.append((
                        expired_instrument_key,
                        timestamp,
                        open_price,
                        high,
                        low,
                        close,
                        volume,
                        open_interest
                    ))
                except Exception as e:
                    logger.error(f"Failed to parse candle: {e}")

            # Batch insert
            if data_to_insert:
                try:
                    cursor.executemany("""
                        INSERT OR REPLACE INTO historical_data
                        (expired_instrument_key, timestamp, open, high, low, close, volume, open_interest)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, data_to_insert)

                    conn.commit()  # Explicitly commit the transaction
                    count = len(data_to_insert)  # Use len instead of rowcount
                    logger.info(f"Successfully inserted {count} candles for {expired_instrument_key}")

                    # Mark contract as data fetched
                    cursor.execute("""
                        UPDATE contracts
                        SET data_fetched = TRUE
                        WHERE expired_instrument_key = ?
                    """, (expired_instrument_key,))
                    conn.commit()

                except Exception as e:
                    logger.error(f"Failed to insert historical data for {expired_instrument_key}: {e}")
                    conn.rollback()
                    raise e
            else:
                logger.warning(f"No data to insert for {expired_instrument_key}")

            return count

    # Job management
    def create_job(self, job_type: str, **kwargs) -> int:
        """Create a new job entry"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO job_status
                (job_type, instrument_key, expiry_date, contract_key, status, started_at)
                VALUES (?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
            """, (
                job_type,
                kwargs.get('instrument_key'),
                kwargs.get('expiry_date'),
                kwargs.get('contract_key')
            ))
            return cursor.lastrowid

    def update_job_status(self, job_id: int, status: str, error: Optional[str] = None) -> None:
        """Update job status"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if status == 'completed':
                cursor.execute("""
                    UPDATE job_status
                    SET status = ?, completed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (status, job_id))
            elif status == 'failed':
                cursor.execute("""
                    UPDATE job_status
                    SET status = ?, error_message = ?, retry_count = retry_count + 1
                    WHERE id = ?
                """, (status, error, job_id))
            else:
                cursor.execute("""
                    UPDATE job_status
                    SET status = ?
                    WHERE id = ?
                """, (status, job_id))

    def save_checkpoint(self, job_id: int, checkpoint_data: Dict) -> None:
        """Save job checkpoint for resume"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE job_status
                SET checkpoint = ?
                WHERE id = ?
            """, (json.dumps(checkpoint_data), job_id))

    # Query operations
    def get_historical_data_count(self, expired_instrument_key: str = None) -> int:
        """Get count of historical data records"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if expired_instrument_key:
                cursor.execute("""
                    SELECT COUNT(*) FROM historical_data
                    WHERE expired_instrument_key = ?
                """, (expired_instrument_key,))
            else:
                cursor.execute("SELECT COUNT(*) FROM historical_data")
            return cursor.fetchone()[0]

    def get_summary_stats(self) -> Dict:
        """Get database summary statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            stats = {}

            # Count instruments
            cursor.execute("SELECT COUNT(*) FROM instruments")
            stats['total_instruments'] = cursor.fetchone()[0]

            # Count expiries
            cursor.execute("SELECT COUNT(*) FROM expiries")
            stats['total_expiries'] = cursor.fetchone()[0]

            # Count contracts
            cursor.execute("SELECT COUNT(*) FROM contracts")
            stats['total_contracts'] = cursor.fetchone()[0]

            # Count historical data
            cursor.execute("SELECT COUNT(*) FROM historical_data")
            stats['total_candles'] = cursor.fetchone()[0]

            # Pending work
            cursor.execute("SELECT COUNT(*) FROM expiries WHERE contracts_fetched = FALSE")
            stats['pending_expiries'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM contracts WHERE data_fetched = FALSE")
            stats['pending_contracts'] = cursor.fetchone()[0]

            return stats

    # OpenAlgo symbol queries
    def get_contract_by_openalgo_symbol(self, openalgo_symbol: str) -> Optional[Dict]:
        """Get contract by OpenAlgo symbol"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM contracts
                WHERE openalgo_symbol = ?
            """, (openalgo_symbol,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_contracts_by_base_symbol(self, base_symbol: str) -> List[Dict]:
        """Get all contracts for a base symbol (e.g., 'NIFTY', 'BANKNIFTY')"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM contracts
                WHERE openalgo_symbol LIKE ?
                ORDER BY expiry_date, strike_price
            """, (f"{base_symbol}%",))
            return [dict(row) for row in cursor.fetchall()]

    def get_option_chain(self, base_symbol: str, expiry_date: str) -> Dict[str, List[Dict]]:
        """Get option chain for a symbol and expiry"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Format expiry date for OpenAlgo format (DDMMMYY)
            from ..utils.openalgo_symbol import OpenAlgoSymbolGenerator
            formatted_date = OpenAlgoSymbolGenerator.format_expiry_date(expiry_date)

            # Get calls
            cursor.execute("""
                SELECT * FROM contracts
                WHERE openalgo_symbol LIKE ? AND openalgo_symbol LIKE '%CE'
                ORDER BY strike_price
            """, (f"{base_symbol}{formatted_date}%",))
            calls = [dict(row) for row in cursor.fetchall()]

            # Get puts
            cursor.execute("""
                SELECT * FROM contracts
                WHERE openalgo_symbol LIKE ? AND openalgo_symbol LIKE '%PE'
                ORDER BY strike_price
            """, (f"{base_symbol}{formatted_date}%",))
            puts = [dict(row) for row in cursor.fetchall()]

            return {"calls": calls, "puts": puts}

    def get_futures_by_symbol(self, base_symbol: str) -> List[Dict]:
        """Get all futures contracts for a symbol"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM contracts
                WHERE openalgo_symbol LIKE ? AND openalgo_symbol LIKE '%FUT'
                ORDER BY expiry_date
            """, (f"{base_symbol}%",))
            return [dict(row) for row in cursor.fetchall()]

    def search_openalgo_symbols(self, pattern: str) -> List[Dict]:
        """Search for contracts by OpenAlgo symbol pattern"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT openalgo_symbol, trading_symbol, expiry_date,
                       contract_type, strike_price
                FROM contracts
                WHERE openalgo_symbol LIKE ?
                ORDER BY openalgo_symbol
                LIMIT 100
            """, (f"%{pattern}%",))
            return [dict(row) for row in cursor.fetchall()]

    def vacuum(self) -> None:
        """Optimize database (SQLite)"""
        if self.db_type == 'sqlite':
            with self.get_connection() as conn:
                conn.execute("VACUUM")
                logger.info("Database optimized (VACUUM completed)")

    def __str__(self) -> str:
        """String representation"""
        stats = self.get_summary_stats()
        return (f"DatabaseManager(type={self.db_type}, "
                f"instruments={stats['total_instruments']}, "
                f"contracts={stats['total_contracts']}, "
                f"candles={stats['total_candles']:,})")