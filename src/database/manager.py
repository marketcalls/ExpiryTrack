"""
Database Manager for ExpiryTrack

Stores configuration / metadata in SQLite:
    credentials, default_instruments, instruments, expiries, contracts,
    job_status.

Historical OHLCV+OI data lives in DuckDB and is accessed transparently
through MarketDataStore; the historical_data SQLite table has been removed.
"""
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date
import logging
from contextlib import contextmanager

from ..config import config
from .market_data import MarketDataStore

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Database manager for time-series expired contract data
    """

    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize database manager.

        Args:
            db_path: Path to SQLite metadata DB. Defaults to config.DB_PATH.
        """
        self.db_path = db_path or config.DB_PATH
        self.db_type = 'sqlite'

        # Create database directory if needed
        self.db_path.parent.mkdir(exist_ok=True, parents=True)

        # Initialize database
        self._init_database()

        # DuckDB-backed market data store (lazy: created on first use).
        # Hosts everything that used to be in the historical_data table.
        self._market_data: Optional[MarketDataStore] = None

    @property
    def market_data(self) -> MarketDataStore:
        """Singleton MarketDataStore for OHLCV+OI access."""
        if self._market_data is None:
            self._market_data = MarketDataStore.instance()
        return self._market_data

    @contextmanager
    def get_connection(self):
        """Context manager for SQLite metadata connections."""
        conn = None
        try:
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            # Enable optimizations
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA foreign_keys = ON")

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

            # historical_data has moved to DuckDB.  Drop the legacy SQLite
            # table on first run so the two stores never go out of sync.
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='historical_data'"
            )
            if cursor.fetchone():
                logger.info("Dropping legacy SQLite historical_data table (now in DuckDB)")
                cursor.execute("DROP TABLE historical_data")
                # Also reset data_fetched so the user can re-collect into DuckDB.
                try:
                    cursor.execute(
                        "UPDATE contracts SET data_fetched = FALSE WHERE data_fetched = TRUE"
                    )
                except sqlite3.OperationalError:
                    pass
                conn.commit()

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

            # historical_data is intentionally NOT created here.
            # All OHLCV+OI bars live in DuckDB; see MarketDataStore.

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
                "CREATE INDEX IF NOT EXISTS idx_openalgo_symbol ON contracts(openalgo_symbol)",
                "CREATE INDEX IF NOT EXISTS idx_instrument_expiry ON contracts(instrument_key, expiry_date)",
                "CREATE INDEX IF NOT EXISTS idx_job_status ON job_status(status, job_type)",
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
    # ----------------------------------------------------------------------
    # All OHLCV+OI persistence is delegated to MarketDataStore (DuckDB).
    # Callers should prefer the new signature `insert_candles(contract, ...)`
    # which carries enough context to populate the denormalized columns.
    # ----------------------------------------------------------------------
    def insert_candles(self, contract: Dict, candles: List[List]) -> int:
        """
        Insert OHLCV+OI candles for one contract into the DuckDB store and
        flip its `data_fetched` flag in SQLite.
        """
        if not candles:
            return 0

        # Make sure the contract dict carries the denormalizing fields the
        # store needs.  If only the expired_instrument_key is known, look up
        # the rest from SQLite.
        contract = self._enrich_contract(contract)

        count = self.market_data.insert_candles(contract, candles)

        if count:
            expired_key = (
                contract.get("expired_instrument_key")
                or contract.get("instrument_key", "")
            )
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE contracts SET data_fetched = TRUE WHERE expired_instrument_key = ?",
                    (expired_key,),
                )
            logger.info(f"Inserted {count} candles for {expired_key} (DuckDB)")
        return count

    def insert_historical_data(self, expired_instrument_key: str, candles: List[List]) -> int:
        """
        Backwards-compatible wrapper: looks up the contract row in SQLite and
        delegates to insert_candles().
        """
        contract = self._lookup_contract(expired_instrument_key)
        if not contract:
            logger.error(
                f"insert_historical_data: contract {expired_instrument_key} not found in SQLite"
            )
            return 0
        return self.insert_candles(contract, candles)

    def _lookup_contract(self, expired_instrument_key: str) -> Optional[Dict]:
        """Fetch a contract row by its expired_instrument_key."""
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM contracts WHERE expired_instrument_key = ?",
                (expired_instrument_key,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _enrich_contract(self, contract: Dict) -> Dict:
        """
        Ensure a contract dict has the fields MarketDataStore expects.

        Accepts both the raw API shape (where `instrument_key` is the expired
        contract id) and the SQLite-row shape (where `expired_instrument_key`
        is set explicitly).
        """
        # Already enriched?
        if contract.get("expired_instrument_key") and contract.get("trading_symbol"):
            return contract

        # API shape: instrument_key holds the per-contract expired key.
        expired_key = (
            contract.get("expired_instrument_key")
            or contract.get("instrument_key", "")
        )
        if expired_key:
            row = self._lookup_contract(expired_key)
            if row:
                merged = dict(row)
                merged.update({k: v for k, v in contract.items() if v is not None})
                merged["expired_instrument_key"] = expired_key
                return merged
        return contract

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
        """Get count of historical data records (from DuckDB)."""
        if expired_instrument_key:
            df = self.market_data.sql(
                "SELECT count(*) AS c FROM market_data WHERE expired_instrument_key = ?",
                [expired_instrument_key],
            )
        else:
            df = self.market_data.sql("SELECT count(*) AS c FROM market_data")
        return int(df["c"].iloc[0]) if not df.empty else 0

    def get_summary_stats(self) -> Dict:
        """Get database summary statistics (joins SQLite + DuckDB)."""
        stats: Dict[str, Any] = {}
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM instruments")
            stats['total_instruments'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM expiries")
            stats['total_expiries'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM contracts")
            stats['total_contracts'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM expiries WHERE contracts_fetched = FALSE")
            stats['pending_expiries'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM contracts WHERE data_fetched = FALSE")
            stats['pending_contracts'] = cursor.fetchone()[0]

        # Candle count comes from DuckDB.
        try:
            md = self.market_data.get_summary()
            stats['total_candles'] = int(md.get('total_candles') or 0)
        except Exception as e:
            logger.warning(f"market_data summary unavailable: {e}")
            stats['total_candles'] = 0

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

    def get_expiries_for_instrument(self, instrument: str) -> List[str]:
        """Get all unique expiry dates for an instrument from the database

        Args:
            instrument: Instrument key (e.g., 'NSE_INDEX|Nifty 50')

        Returns:
            List of expiry dates as strings
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT expiry_date FROM contracts
                WHERE instrument_key = ?
                ORDER BY expiry_date DESC
            """, (instrument,))
            return [row[0] for row in cursor.fetchall()]

    def get_contracts_for_expiry(self, instrument: str, expiry_date: str) -> List[Dict]:
        """Get all contracts for an instrument and expiry date

        Args:
            instrument: Instrument key
            expiry_date: Expiry date string

        Returns:
            List of contract dictionaries
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM contracts
                WHERE instrument_key = ?
                AND expiry_date = ?
                ORDER BY strike_price, contract_type
            """, (instrument, expiry_date))
            return [dict(row) for row in cursor.fetchall()]

    def get_historical_data(self, expired_instrument_key: str) -> List[List]:
        """Get historical data for a specific expired instrument.

        Reads from the DuckDB store; returns list-of-lists for backwards
        compatibility with the exporter and demo scripts.
        """
        df = self.market_data.sql(
            """
            SELECT ts, open, high, low, close, volume, oi
            FROM market_data
            WHERE expired_instrument_key = ?
            ORDER BY ts
            """,
            [expired_instrument_key],
        )
        if df.empty:
            return []
        # Render timestamps as ISO strings to match the prior contract.
        df = df.copy()
        df["ts"] = df["ts"].astype(str)
        return df.values.tolist()

    def vacuum(self) -> None:
        """Optimize both databases."""
        with self.get_connection() as conn:
            conn.execute("VACUUM")
        try:
            self.market_data.vacuum()
        except Exception as e:
            logger.warning(f"market_data vacuum skipped: {e}")
        logger.info("Database optimized (SQLite VACUUM + DuckDB CHECKPOINT)")

    def __str__(self) -> str:
        """String representation"""
        stats = self.get_summary_stats()
        return (f"DatabaseManager(type={self.db_type}, "
                f"instruments={stats['total_instruments']}, "
                f"contracts={stats['total_contracts']}, "
                f"candles={stats['total_candles']:,})")