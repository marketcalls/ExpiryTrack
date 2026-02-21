"""
Database Manager for ExpiryTrack
Handles DuckDB operations with optimized time-series storage
"""
import json
import threading
import duckdb
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date
import logging
from contextlib import contextmanager

from ..config import config

logger = logging.getLogger(__name__)

_DEFAULT_REDIRECT_URI = config.UPSTOX_REDIRECT_URI


class DictCursor:
    """Wrapper around DuckDB cursor that supports dict-like row access via row['column']"""

    def __init__(self, cursor):
        self._cursor = cursor
        self._description = None

    def execute(self, sql, params=None):
        if params is not None:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)
        self._description = self._cursor.description
        return self

    def executemany(self, sql, params):
        self._cursor.executemany(sql, params)
        self._description = self._cursor.description
        return self

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        if self._description:
            return DictRow(row, [d[0] for d in self._description])
        return row

    def fetchall(self):
        rows = self._cursor.fetchall()
        if self._description:
            columns = [d[0] for d in self._description]
            return [DictRow(row, columns) for row in rows]
        return rows

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def lastrowid(self):
        # DuckDB doesn't have lastrowid natively; we handle this per-query
        return getattr(self._cursor, 'lastrowid', None)

    @property
    def description(self):
        return self._cursor.description


class DictRow:
    """Row object that supports both index-based and key-based access, like sqlite3.Row"""

    def __init__(self, values, columns):
        self._values = values
        self._columns = columns
        self._col_map = {col: idx for idx, col in enumerate(columns)}

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return self._values[self._col_map[key]]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def keys(self):
        return self._columns

    def __contains__(self, key):
        return key in self._col_map

    def items(self):
        return zip(self._columns, self._values)

    def __repr__(self):
        return f"DictRow({dict(self.items())})"


def dict_from_row(row):
    """Convert a DictRow to a plain dict"""
    if row is None:
        return None
    if isinstance(row, DictRow):
        return {col: row[col] for col in row.keys()}
    return dict(row)


class DatabaseManager:
    """
    Database manager for time-series expired contract data
    """

    # Class-level lock shared across all instances for DuckDB single-writer serialization
    _write_lock = threading.Lock()

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
        """Context manager for database connections.

        Uses a class-level threading.Lock to serialize writes,
        since DuckDB only supports a single concurrent writer.
        """
        with self._write_lock:
            conn = None
            try:
                conn = duckdb.connect(str(self.db_path))
                yield conn
                conn.commit()
            except Exception as e:
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass  # No active transaction to rollback
                logger.error(f"Database error: {e}")
                raise
            finally:
                if conn:
                    conn.close()

    def _table_exists(self, conn, table_name: str) -> bool:
        """Check if a table exists using information_schema"""
        result = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            (table_name,)
        ).fetchone()
        return result[0] > 0

    def _get_columns(self, conn, table_name: str) -> List[str]:
        """Get column names for a table using information_schema"""
        result = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            (table_name,)
        ).fetchall()
        return [row[0] for row in result]

    def _init_database(self) -> None:
        """Initialize database schema"""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())

            # Check if contracts table exists and needs migration
            if self._table_exists(conn, 'contracts'):
                columns = self._get_columns(conn, 'contracts')

                if 'openalgo_symbol' not in columns:
                    cursor.execute("ALTER TABLE contracts ADD COLUMN openalgo_symbol TEXT")
                    conn.commit()
                    logger.info("Added openalgo_symbol column to contracts table")

                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_openalgo_symbol ON contracts(openalgo_symbol)")
                    conn.commit()
                    logger.info("Created index for openalgo_symbol column")

                if 'no_data' not in columns:
                    cursor.execute("ALTER TABLE contracts ADD COLUMN no_data BOOLEAN DEFAULT FALSE")
                    conn.commit()
                    logger.info("Added no_data column to contracts table")

                    # One-time migration: mark existing stuck contracts as no_data
                    # These are contracts with data_fetched=FALSE that have no historical data
                    # and their expiry date has passed (so they've been attempted before)
                    result = cursor.execute("""
                        UPDATE contracts c
                        SET data_fetched = TRUE, no_data = TRUE
                        WHERE c.data_fetched = FALSE
                          AND c.expiry_date < CURRENT_DATE
                          AND NOT EXISTS (
                              SELECT 1 FROM historical_data h
                              WHERE h.expired_instrument_key = c.expired_instrument_key
                          )
                        RETURNING expired_instrument_key
                    """)
                    fixed_count = len(result.fetchall())
                    conn.commit()
                    if fixed_count:
                        logger.info(f"Migration: marked {fixed_count} stuck contracts as no_data")

                if 'fetch_attempts' not in columns:
                    cursor.execute("ALTER TABLE contracts ADD COLUMN fetch_attempts INTEGER DEFAULT 0")
                    conn.commit()
                    logger.info("Added fetch_attempts column to contracts table")

                if 'last_attempted_at' not in columns:
                    cursor.execute("ALTER TABLE contracts ADD COLUMN last_attempted_at TIMESTAMP")
                    conn.commit()
                    logger.info("Added last_attempted_at column to contracts table")

            # Fix: mark expiries as contracts_fetched if they already have contracts
            if self._table_exists(conn, 'expiries'):
                result = cursor.execute("""
                    UPDATE expiries e
                    SET contracts_fetched = TRUE
                    WHERE e.contracts_fetched = FALSE
                      AND EXISTS (
                          SELECT 1 FROM contracts c
                          WHERE c.instrument_key = e.instrument_key
                            AND c.expiry_date = e.expiry_date
                      )
                    RETURNING instrument_key, expiry_date
                """)
                fixed = result.fetchall()
                conn.commit()
                if fixed:
                    logger.info(f"Migration: marked {len(fixed)} expiries as contracts_fetched")

            # Check if historical_data table exists and needs oi column
            if self._table_exists(conn, 'historical_data'):
                columns = self._get_columns(conn, 'historical_data')

                if 'oi' not in columns and 'open_interest' not in columns:
                    cursor.execute("ALTER TABLE historical_data ADD COLUMN oi BIGINT DEFAULT 0")
                    conn.commit()
                    logger.info("Added oi column to historical_data table")

            # Create sequences for auto-increment columns
            for seq in ['default_instruments_id_seq', 'expiries_id_seq', 'job_status_id_seq']:
                cursor.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq}")

            # Create credentials table for encrypted storage
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS credentials (
                    id INTEGER PRIMARY KEY,
                    api_key TEXT NOT NULL,
                    api_secret TEXT NOT NULL,
                    redirect_uri TEXT DEFAULT 'http://127.0.0.1:5005/upstox/callback',
                    access_token TEXT,
                    token_expiry DOUBLE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create default instruments table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS default_instruments (
                    id INTEGER PRIMARY KEY DEFAULT nextval('default_instruments_id_seq'),
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
                    id INTEGER PRIMARY KEY DEFAULT nextval('expiries_id_seq'),
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
                    openalgo_symbol TEXT,
                    lot_size INTEGER,
                    tick_size DECIMAL(10,2),
                    exchange_token TEXT,
                    freeze_quantity INTEGER,
                    minimum_lot INTEGER,
                    metadata JSON,
                    data_fetched BOOLEAN DEFAULT FALSE,
                    no_data BOOLEAN DEFAULT FALSE,
                    fetch_attempts INTEGER DEFAULT 0,
                    last_attempted_at TIMESTAMP,
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
                    oi BIGINT DEFAULT 0,
                    PRIMARY KEY (expired_instrument_key, timestamp),
                    FOREIGN KEY (expired_instrument_key) REFERENCES contracts(expired_instrument_key)
                )
            """)

            # Create job_status table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_status (
                    id INTEGER PRIMARY KEY DEFAULT nextval('job_status_id_seq'),
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
            cursor = DictCursor(conn.cursor())

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
                """, (encrypted_key, encrypted_secret, redirect_uri or _DEFAULT_REDIRECT_URI))
            else:
                cursor.execute("""
                    INSERT INTO credentials (id, api_key, api_secret, redirect_uri)
                    VALUES (1, ?, ?, ?)
                """, (encrypted_key, encrypted_secret, redirect_uri or _DEFAULT_REDIRECT_URI))

            return True

    def get_credentials(self) -> Optional[Dict]:
        """Get decrypted credentials from database"""
        from ..utils.encryption import encryption

        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
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
            cursor = DictCursor(conn.cursor())
            encrypted_token = encryption.encrypt(access_token)

            cursor.execute("""
                UPDATE credentials
                SET access_token = ?, token_expiry = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            """, (encrypted_token, expiry))

            return True

    # Default instruments operations
    def setup_default_instruments(self) -> bool:
        """Setup default instruments for collection (#9 — expanded to 6)"""
        default_instruments = [
            {'key': 'NSE_INDEX|Nifty 50', 'symbol': 'Nifty 50', 'priority': 100},
            {'key': 'NSE_INDEX|Nifty Bank', 'symbol': 'Bank Nifty', 'priority': 90},
            {'key': 'BSE_INDEX|SENSEX', 'symbol': 'Sensex', 'priority': 80},
            {'key': 'NSE_INDEX|Nifty Fin Service', 'symbol': 'FINNIFTY', 'priority': 70},
            {'key': 'NSE_INDEX|NIFTY MID SELECT', 'symbol': 'MIDCPNIFTY', 'priority': 60},
            {'key': 'BSE_INDEX|BANKEX', 'symbol': 'BANKEX', 'priority': 50},
        ]

        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())

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
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT instrument_key FROM default_instruments
                WHERE is_active = TRUE
                ORDER BY priority DESC
            """)
            return [row[0] for row in cursor.fetchall()]

    # ── Active instrument CRUD (#9) ───────────────────────────
    def get_active_instruments(self) -> List[Dict]:
        """Get all active instruments with full details."""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT id, instrument_key, symbol, is_active, priority
                FROM default_instruments
                ORDER BY priority DESC
            """)
            return [dict_from_row(row) for row in cursor.fetchall()]

    def add_instrument(self, instrument_key: str, symbol: str, priority: int = 0) -> Optional[int]:
        """Add a new instrument."""
        with self.get_connection() as conn:
            try:
                result = conn.execute("""
                    INSERT INTO default_instruments (instrument_key, symbol, priority)
                    VALUES (?, ?, ?)
                    RETURNING id
                """, (instrument_key, symbol, priority))
                row = result.fetchone()
                conn.commit()
                return row[0] if row else None
            except Exception as e:
                if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                    return None
                raise

    def toggle_instrument(self, instrument_id: int, is_active: bool) -> bool:
        """Toggle instrument active status."""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE default_instruments SET is_active = ? WHERE id = ?
            """, (is_active, instrument_id))
            conn.commit()
            return True

    def remove_instrument(self, instrument_id: int) -> bool:
        """Remove an instrument by ID."""
        with self.get_connection() as conn:
            conn.execute("DELETE FROM default_instruments WHERE id = ?", (instrument_id,))
            conn.commit()
            return True

    # ── API Keys (#10) ────────────────────────────────────────
    def _ensure_api_keys_table(self, conn):
        """Create api_keys table if it doesn't exist."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY DEFAULT nextval('default_instruments_id_seq'),
                key_name TEXT NOT NULL,
                api_key TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                rate_limit_per_hour INTEGER DEFAULT 1000
            )
        """)

    def create_api_key(self, key_name: str) -> Optional[Dict]:
        """Generate and store a new API key."""
        import secrets as _secrets
        api_key = 'expt_' + _secrets.token_urlsafe(32)
        with self.get_connection() as conn:
            self._ensure_api_keys_table(conn)
            result = conn.execute("""
                INSERT INTO api_keys (key_name, api_key)
                VALUES (?, ?)
                RETURNING id, key_name, api_key, created_at
            """, (key_name, api_key))
            row = result.fetchone()
            conn.commit()
            if row:
                return {'id': row[0], 'key_name': row[1], 'api_key': row[2], 'created_at': str(row[3])}
            return None

    def verify_api_key(self, api_key: str) -> Optional[Dict]:
        """Verify an API key and update last_used_at."""
        with self.get_connection() as conn:
            self._ensure_api_keys_table(conn)
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT id, key_name, rate_limit_per_hour FROM api_keys
                WHERE api_key = ? AND is_active = TRUE
            """, (api_key,))
            row = cursor.fetchone()
            if row:
                conn.execute("""
                    UPDATE api_keys SET last_used_at = CURRENT_TIMESTAMP WHERE api_key = ?
                """, (api_key,))
                conn.commit()
                return dict_from_row(row)
            return None

    def list_api_keys(self) -> List[Dict]:
        """List all API keys (masked)."""
        with self.get_connection() as conn:
            self._ensure_api_keys_table(conn)
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT id, key_name, api_key, created_at, last_used_at, is_active, rate_limit_per_hour
                FROM api_keys ORDER BY created_at DESC
            """)
            keys = []
            for row in cursor.fetchall():
                d = dict_from_row(row)
                # Mask API key for display
                full_key = d['api_key']
                d['api_key_masked'] = full_key[:9] + '...' + full_key[-4:]
                keys.append(d)
            return keys

    def revoke_api_key(self, key_id: int) -> bool:
        """Revoke an API key."""
        with self.get_connection() as conn:
            self._ensure_api_keys_table(conn)
            conn.execute("UPDATE api_keys SET is_active = FALSE WHERE id = ?", (key_id,))
            conn.commit()
            return True

    # ── Retry Logic (#15) ─────────────────────────────────────
    def increment_fetch_attempt(self, expired_key: str) -> None:
        """Increment fetch_attempts and set last_attempted_at."""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE contracts
                SET fetch_attempts = fetch_attempts + 1,
                    last_attempted_at = CURRENT_TIMESTAMP
                WHERE expired_instrument_key = ?
            """, (expired_key,))
            conn.commit()

    def get_failed_contracts(self, instrument_key: Optional[str] = None) -> List[Dict]:
        """Get contracts that failed fetching (fetch_attempts > 0, data_fetched = FALSE)."""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            where = "WHERE c.data_fetched = FALSE AND c.fetch_attempts > 0 AND c.no_data = FALSE"
            params = []
            if instrument_key:
                where += " AND c.instrument_key = ?"
                params.append(instrument_key)
            cursor.execute(f"""
                SELECT c.expired_instrument_key, c.instrument_key, c.expiry_date,
                       c.trading_symbol, c.fetch_attempts, c.last_attempted_at
                FROM contracts c
                {where}
                ORDER BY c.last_attempted_at
            """, params)
            return [dict_from_row(row) for row in cursor.fetchall()]

    def reset_fetch_attempts(self, instrument_key: Optional[str] = None) -> int:
        """Reset fetch_attempts for failed contracts so they can be retried."""
        with self.get_connection() as conn:
            where = "WHERE data_fetched = FALSE AND fetch_attempts > 0 AND no_data = FALSE"
            params = []
            if instrument_key:
                where += " AND instrument_key = ?"
                params.append(instrument_key)
            result = conn.execute(f"""
                UPDATE contracts
                SET fetch_attempts = 0, last_attempted_at = NULL
                {where}
                RETURNING expired_instrument_key
            """, params)
            count = len(result.fetchall())
            conn.commit()
            return count

    # Instrument operations
    def insert_instrument(self, instrument_data: Dict) -> bool:
        """Insert or update instrument"""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
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
            cursor = DictCursor(conn.cursor())

            # Get existing expiries to determine what's new
            cursor.execute(
                "SELECT expiry_date FROM expiries WHERE instrument_key = ?",
                (instrument_key,)
            )
            existing = {str(row[0]) for row in cursor.fetchall()}

            count = 0
            for expiry_date in expiry_dates:
                if expiry_date in existing:
                    continue
                try:
                    date_obj = datetime.strptime(expiry_date, '%Y-%m-%d')
                    is_weekly = date_obj.weekday() == 3  # Thursday

                    cursor.execute("""
                        INSERT OR IGNORE INTO expiries
                        (instrument_key, expiry_date, is_weekly)
                        VALUES (?, ?, ?)
                    """, (instrument_key, expiry_date, is_weekly))
                    count += 1
                except Exception as e:
                    logger.error(f"Failed to insert expiry {expiry_date}: {e}")

            logger.info(f"Inserted {count} new expiries for {instrument_key}")
            return count

    def get_pending_expiries(self, instrument_key: str) -> List[Dict]:
        """Get expiries that haven't been processed"""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT * FROM expiries
                WHERE instrument_key = ? AND contracts_fetched = FALSE
                ORDER BY expiry_date
            """, (instrument_key,))
            return [dict_from_row(row) for row in cursor.fetchall()]

    def mark_expiry_contracts_fetched(self, instrument_key: str, expiry_date: str) -> None:
        """Mark an expiry as having its contracts fetched."""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE expiries
                SET contracts_fetched = TRUE
                WHERE instrument_key = ? AND expiry_date = ?
            """, (instrument_key, expiry_date))
            conn.commit()

    # Contract operations
    def insert_contracts(self, contracts: List[Dict]) -> int:
        """Insert multiple contracts. Skips contracts that already exist to preserve
        data_fetched, no_data, and fetch_attempts state."""
        from ..utils.openalgo_symbol import to_openalgo_symbol

        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            inserted = 0
            skipped = 0

            for contract in contracts:
                try:
                    expired_key = contract.get('instrument_key', '')
                    openalgo_symbol = to_openalgo_symbol(contract)

                    cursor.execute("""
                        INSERT OR IGNORE INTO contracts
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
                        openalgo_symbol,
                        contract.get('lot_size'),
                        contract.get('tick_size'),
                        contract.get('exchange_token', ''),
                        contract.get('freeze_quantity'),
                        contract.get('minimum_lot'),
                        json.dumps(contract)
                    ))
                    # DuckDB returns -1 for rowcount, check if key existed
                    inserted += 1
                except Exception as e:
                    if 'duplicate' in str(e).lower() or 'unique' in str(e).lower():
                        skipped += 1
                    else:
                        logger.error(f"Failed to insert contract {contract.get('trading_symbol')}: {e}")

            if skipped:
                logger.info(f"Inserted {inserted} contracts, skipped {skipped} existing")
            else:
                logger.info(f"Inserted {inserted} contracts")
            return inserted

    def get_pending_contracts(self, limit: int = 100) -> List[Dict]:
        """Get contracts that need historical data fetched"""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT * FROM contracts
                WHERE data_fetched = FALSE
                LIMIT ?
            """, (limit,))
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_fetched_keys(self, expired_keys: list) -> set:
        """Return the subset of expired_instrument_keys that already have data_fetched=TRUE."""
        if not expired_keys:
            return set()
        with self.get_connection() as conn:
            placeholders = ','.join(['?'] * len(expired_keys))
            rows = conn.execute(f"""
                SELECT expired_instrument_key FROM contracts
                WHERE data_fetched = TRUE
                AND expired_instrument_key IN ({placeholders})
            """, expired_keys).fetchall()
            return {row[0] for row in rows}

    # Historical data operations
    def insert_historical_data(self, expired_instrument_key: str, candles: List[List]) -> int:
        """
        Insert historical OHLCV data using DataFrame bulk insert for performance.

        Args:
            expired_instrument_key: Contract identifier
            candles: List of [timestamp, open, high, low, close, volume, oi]

        Returns:
            Number of records inserted
        """
        with self.get_connection() as conn:
            count = 0

            # Prepare batch insert
            data_to_insert = []
            for candle in candles:
                try:
                    timestamp = candle[0]
                    open_price = float(candle[1])
                    high = float(candle[2])
                    low = float(candle[3])
                    close = float(candle[4])
                    volume = int(candle[5])
                    open_interest = int(candle[6]) if len(candle) > 6 else None

                    data_to_insert.append({
                        'expired_instrument_key': expired_instrument_key,
                        'timestamp': timestamp,
                        'open': open_price,
                        'high': high,
                        'low': low,
                        'close': close,
                        'volume': volume,
                        'oi': open_interest
                    })
                except Exception as e:
                    logger.error(f"Failed to parse candle: {e}")

            # DataFrame bulk insert — ~500x faster than executemany in DuckDB
            if data_to_insert:
                try:
                    conn.execute("BEGIN TRANSACTION")
                    df = pd.DataFrame(data_to_insert)
                    conn.execute("""
                        INSERT OR REPLACE INTO historical_data
                        SELECT * FROM df
                    """)

                    # Mark contract as data fetched (atomic with insert)
                    conn.execute("""
                        UPDATE contracts
                        SET data_fetched = TRUE,
                            no_data = FALSE,
                            fetch_attempts = fetch_attempts + 1,
                            last_attempted_at = CURRENT_TIMESTAMP
                        WHERE expired_instrument_key = ?
                    """, (expired_instrument_key,))
                    conn.commit()

                    count = len(data_to_insert)
                    logger.info(f"Successfully inserted {count} candles for {expired_instrument_key}")

                except Exception as e:
                    logger.error(f"Failed to insert historical data for {expired_instrument_key}: {e}")
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    raise
            else:
                logger.warning(f"No data to insert for {expired_instrument_key}")

            return count

    def mark_contract_no_data(self, expired_instrument_key: str) -> None:
        """Mark a contract as fetched but with no data available from the API."""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE contracts
                SET data_fetched = TRUE,
                    no_data = TRUE,
                    fetch_attempts = fetch_attempts + 1,
                    last_attempted_at = CURRENT_TIMESTAMP
                WHERE expired_instrument_key = ?
            """, (expired_instrument_key,))
            conn.commit()
            logger.info(f"Marked contract {expired_instrument_key} as no_data")

    def reset_contracts_for_refetch(self, instrument_key: str, expiry_date: str) -> int:
        """Reset data_fetched flag for all contracts of an instrument+expiry,
        so they will be re-downloaded on the next collection run."""
        with self.get_connection() as conn:
            result = conn.execute("""
                UPDATE contracts
                SET data_fetched = FALSE, no_data = FALSE
                WHERE instrument_key = ? AND expiry_date = ?
                RETURNING expired_instrument_key
            """, (instrument_key, expiry_date))
            count = len(result.fetchall())
            conn.commit()
            logger.info(f"Reset {count} contracts for {instrument_key} expiry {expiry_date}")
            return count

    # Job management
    def create_job(self, job_type: str, **kwargs) -> int:
        """Create a new job entry"""
        with self.get_connection() as conn:
            result = conn.execute("""
                INSERT INTO job_status
                (job_type, instrument_key, expiry_date, contract_key, status, started_at)
                VALUES (?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
                RETURNING id
            """, (
                job_type,
                kwargs.get('instrument_key'),
                kwargs.get('expiry_date'),
                kwargs.get('contract_key')
            )).fetchone()
            return result[0]

    def update_job_status(self, job_id: int, status: str, error: Optional[str] = None) -> None:
        """Update job status"""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())

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
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                UPDATE job_status
                SET checkpoint = ?
                WHERE id = ?
            """, (json.dumps(checkpoint_data), job_id))

    # Query operations
    def get_historical_data_count(self, expired_instrument_key: str = None) -> int:
        """Get count of historical data records"""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
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
            cursor = DictCursor(conn.cursor())

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
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT * FROM contracts
                WHERE openalgo_symbol = ?
            """, (openalgo_symbol,))
            row = cursor.fetchone()
            return dict_from_row(row) if row else None

    def get_contracts_by_base_symbol(self, base_symbol: str) -> List[Dict]:
        """Get all contracts for a base symbol (e.g., 'NIFTY', 'BANKNIFTY')"""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT * FROM contracts
                WHERE openalgo_symbol LIKE ?
                ORDER BY expiry_date, strike_price
            """, (f"{base_symbol}%",))
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_option_chain(self, base_symbol: str, expiry_date: str) -> Dict[str, List[Dict]]:
        """Get option chain for a symbol and expiry"""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())

            # Format expiry date for OpenAlgo format (DDMMMYY)
            from ..utils.openalgo_symbol import OpenAlgoSymbolGenerator
            formatted_date = OpenAlgoSymbolGenerator.format_expiry_date(expiry_date)

            # Get calls
            cursor.execute("""
                SELECT * FROM contracts
                WHERE openalgo_symbol LIKE ? AND openalgo_symbol LIKE '%CE'
                ORDER BY strike_price
            """, (f"{base_symbol}{formatted_date}%",))
            calls = [dict_from_row(row) for row in cursor.fetchall()]

            # Get puts
            cursor.execute("""
                SELECT * FROM contracts
                WHERE openalgo_symbol LIKE ? AND openalgo_symbol LIKE '%PE'
                ORDER BY strike_price
            """, (f"{base_symbol}{formatted_date}%",))
            puts = [dict_from_row(row) for row in cursor.fetchall()]

            return {"calls": calls, "puts": puts}

    def get_futures_by_symbol(self, base_symbol: str) -> List[Dict]:
        """Get all futures contracts for a symbol"""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT * FROM contracts
                WHERE openalgo_symbol LIKE ? AND openalgo_symbol LIKE '%FUT'
                ORDER BY expiry_date
            """, (f"{base_symbol}%",))
            return [dict_from_row(row) for row in cursor.fetchall()]

    def search_openalgo_symbols(self, pattern: str) -> List[Dict]:
        """Search for contracts by OpenAlgo symbol pattern"""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT openalgo_symbol, trading_symbol, expiry_date,
                       contract_type, strike_price
                FROM contracts
                WHERE openalgo_symbol LIKE ?
                ORDER BY openalgo_symbol
                LIMIT 100
            """, (f"%{pattern}%",))
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_expiries_for_instrument(self, instrument: str) -> List[str]:
        """Get all unique expiry dates for an instrument from the database

        Args:
            instrument: Instrument key (e.g., 'NSE_INDEX|Nifty 50')

        Returns:
            List of expiry dates as strings
        """
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT DISTINCT expiry_date FROM contracts
                WHERE instrument_key = ?
                ORDER BY expiry_date DESC
            """, (instrument,))
            return [str(row[0]) for row in cursor.fetchall()]

    def get_contracts_for_expiry(self, instrument: str, expiry_date: str) -> List[Dict]:
        """Get all contracts for an instrument and expiry date

        Args:
            instrument: Instrument key
            expiry_date: Expiry date string

        Returns:
            List of contract dictionaries
        """
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT * FROM contracts
                WHERE instrument_key = ?
                AND expiry_date = ?
                ORDER BY strike_price, contract_type
            """, (instrument, expiry_date))
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_historical_data(self, expired_instrument_key: str) -> List[List]:
        """Get historical data for a specific expired instrument

        Args:
            expired_instrument_key: The expired instrument key

        Returns:
            List of candles [timestamp, open, high, low, close, volume, oi]
        """
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT timestamp, open, high, low, close, volume, oi
                FROM historical_data
                WHERE expired_instrument_key = ?
                ORDER BY timestamp
            """, (expired_instrument_key,))
            return [list(row) for row in cursor.fetchall()]

    def vacuum(self) -> None:
        """Optimize database"""
        with self.get_connection() as conn:
            conn.execute("CHECKPOINT")
            logger.info("Database optimized (CHECKPOINT completed)")

    def __str__(self) -> str:
        """String representation"""
        stats = self.get_summary_stats()
        return (f"DatabaseManager(type={self.db_type}, "
                f"instruments={stats['total_instruments']}, "
                f"contracts={stats['total_contracts']}, "
                f"candles={stats['total_candles']:,})")
