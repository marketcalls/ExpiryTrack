"""
Database Manager for ExpiryTrack
Thin facade over domain-specific repository classes.
Handles DuckDB schema initialization and connection management.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb

from ..config import config

logger = logging.getLogger(__name__)

_DEFAULT_REDIRECT_URI = config.UPSTOX_REDIRECT_URI


class DictCursor:
    """Wrapper around DuckDB cursor that supports dict-like row access via row['column']"""

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor
        self._description: Any = None

    def execute(self, sql: str, params: Any = None) -> DictCursor:
        if params is not None:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)
        self._description = self._cursor.description
        return self

    def executemany(self, sql: str, params: Any) -> DictCursor:
        self._cursor.executemany(sql, params)
        self._description = self._cursor.description
        return self

    def fetchone(self) -> Any:
        row = self._cursor.fetchone()
        if row is None:
            return None
        if self._description:
            return DictRow(row, [d[0] for d in self._description])
        return row

    def fetchall(self) -> Any:
        rows = self._cursor.fetchall()
        if self._description:
            columns = [d[0] for d in self._description]
            return [DictRow(row, columns) for row in rows]
        return rows

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount  # type: ignore[no-any-return]

    @property
    def lastrowid(self) -> int | None:
        # DuckDB doesn't have lastrowid natively; we handle this per-query
        return getattr(self._cursor, "lastrowid", None)

    @property
    def description(self) -> Any:
        return self._cursor.description


class DictRow:
    """Row object that supports both index-based and key-based access, like sqlite3.Row"""

    def __init__(self, values: tuple[Any, ...], columns: list[str]) -> None:
        self._values = values
        self._columns = columns
        self._col_map: dict[str, int] = {col: idx for idx, col in enumerate(columns)}

    def __getitem__(self, key: int | str) -> Any:
        if isinstance(key, int):
            return self._values[key]
        return self._values[self._col_map[key]]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def keys(self) -> list[str]:
        return self._columns

    def __contains__(self, key: object) -> bool:
        return key in self._col_map

    def items(self) -> zip[tuple[str, Any]]:
        return zip(self._columns, self._values, strict=False)

    def __repr__(self) -> str:
        return f"DictRow({dict(self.items())})"


def dict_from_row(row: Any) -> Any:
    """Convert a DictRow to a plain dict"""
    if row is None:
        return None
    if isinstance(row, DictRow):
        return {col: row[col] for col in row.keys()}  # noqa: SIM118
    return dict(row)


class DatabaseManager:
    """
    Database manager facade — delegates domain operations to repository classes.
    Owns connection management, schema initialization, and cross-cutting queries.
    """

    # Class-level lock shared across all instances for DuckDB single-writer serialization
    _write_lock = threading.Lock()
    _read_pool_max = 3

    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or config.DB_PATH
        self.db_type = config.DB_TYPE

        # Instance-level read connection pool (avoids cross-instance contamination in tests)
        self._read_pool: list[Any] = []
        self._read_pool_lock = threading.Lock()

        # Create database directory if needed
        self.db_path.parent.mkdir(exist_ok=True, parents=True)

        # Initialize database schema
        self._init_database()

        # Compose repository instances
        from .repos import (
            BacktestRepository,
            CandleRepository,
            ContractRepository,
            CredentialRepository,
            ExportsRepo,
            HistoricalDataRepository,
            InstrumentMasterRepository,
            InstrumentRepository,
            JobRepository,
            TaskRepository,
            WatchlistRepository,
        )

        self.credentials = CredentialRepository(self)
        self.instruments = InstrumentRepository(self)
        self.contracts = ContractRepository(self)
        self.historical = HistoricalDataRepository(self)
        self.instrument_master = InstrumentMasterRepository(self)
        self.candles = CandleRepository(self)
        self.watchlists = WatchlistRepository(self)
        self.jobs = JobRepository(self)
        self.tasks_repo = TaskRepository(self)
        self.exports_repo = ExportsRepo(self)
        self.backtests = BacktestRepository(self)

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """Context manager for read-write database connections.

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
                    except duckdb.Error:
                        logger.debug("No active transaction to rollback")
                logger.error(f"Database error: {e}")
                raise
            finally:
                if conn:
                    conn.close()

    @contextmanager
    def get_read_connection(self) -> Generator[Any, None, None]:
        """Context manager for read-only database connections.

        Uses a simple connection pool (max 3) to reduce connection overhead.
        No write lock needed — DuckDB allows concurrent reads.
        """
        conn = None
        try:
            with self._read_pool_lock:
                if self._read_pool:
                    conn = self._read_pool.pop()
            if conn is None:
                conn = duckdb.connect(str(self.db_path))
            yield conn
        except Exception:
            # On error, don't return connection to pool
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            conn = None
            raise
        finally:
            if conn is not None:
                with self._read_pool_lock:
                    if len(self._read_pool) < self._read_pool_max:
                        self._read_pool.append(conn)
                    else:
                        conn.close()

    def _table_exists(self, conn: Any, table_name: str) -> bool:
        """Check if a table exists using information_schema"""
        result = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", (table_name,)
        ).fetchone()
        return bool(result[0] > 0)

    def _get_columns(self, conn: Any, table_name: str) -> list[str]:
        """Get column names for a table using information_schema"""
        result = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?", (table_name,)
        ).fetchall()
        return [row[0] for row in result]

    def _init_database(self) -> None:
        """Initialize database schema and run pending migrations."""
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())

            # Create sequences for auto-increment columns
            for seq in ["default_instruments_id_seq", "expiries_id_seq", "job_status_id_seq"]:
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
                    category TEXT DEFAULT 'Index',
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
                    PRIMARY KEY (expired_instrument_key, timestamp)
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

            # Create instrument_master table (for all Upstox instruments)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS instrument_master (
                    instrument_key VARCHAR PRIMARY KEY,
                    trading_symbol VARCHAR,
                    name VARCHAR,
                    exchange VARCHAR,
                    segment VARCHAR NOT NULL,
                    instrument_type VARCHAR,
                    isin VARCHAR,
                    lot_size INTEGER,
                    tick_size DECIMAL(10,4),
                    expiry DATE,
                    strike_price DECIMAL(15,2),
                    option_type VARCHAR,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create candle_data table (for V3 historical candle data)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS candle_data (
                    instrument_key VARCHAR NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open DECIMAL(15,4) NOT NULL,
                    high DECIMAL(15,4) NOT NULL,
                    low DECIMAL(15,4) NOT NULL,
                    close DECIMAL(15,4) NOT NULL,
                    volume BIGINT NOT NULL DEFAULT 0,
                    oi BIGINT DEFAULT 0,
                    interval VARCHAR NOT NULL DEFAULT '1day',
                    PRIMARY KEY (instrument_key, timestamp, interval)
                )
            """)

            # Create watchlists table
            cursor.execute("""
                CREATE SEQUENCE IF NOT EXISTS watchlists_id_seq
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlists (
                    id INTEGER PRIMARY KEY DEFAULT nextval('watchlists_id_seq'),
                    name VARCHAR NOT NULL,
                    segment VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create watchlist_items table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlist_items (
                    watchlist_id INTEGER NOT NULL REFERENCES watchlists(id),
                    instrument_key VARCHAR NOT NULL,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (watchlist_id, instrument_key)
                )
            """)

            # Create candle_collection_status table (track what's been collected)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS candle_collection_status (
                    instrument_key VARCHAR NOT NULL,
                    interval VARCHAR NOT NULL DEFAULT '1day',
                    last_collected_date DATE,
                    earliest_date DATE,
                    candle_count BIGINT DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (instrument_key, interval)
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
                "CREATE INDEX IF NOT EXISTS idx_job_status ON job_status(status, job_type)",
                # Instrument master indices
                "CREATE INDEX IF NOT EXISTS idx_im_segment ON instrument_master(segment)",
                "CREATE INDEX IF NOT EXISTS idx_im_type ON instrument_master(instrument_type)",
                "CREATE INDEX IF NOT EXISTS idx_im_exchange ON instrument_master(exchange)",
                "CREATE INDEX IF NOT EXISTS idx_im_symbol ON instrument_master(trading_symbol)",
                # Candle data indices
                "CREATE INDEX IF NOT EXISTS idx_candle_instrument ON candle_data(instrument_key)",
                "CREATE INDEX IF NOT EXISTS idx_candle_interval ON candle_data(interval)",
                "CREATE INDEX IF NOT EXISTS idx_candle_timestamp ON candle_data(timestamp)",
            ]

            for index in indices:
                cursor.execute(index)

            # Run versioned migrations
            from .migrations.runner import MigrationRunner

            runner = MigrationRunner(conn)
            runner.run_pending()

            logger.info("Database schema initialized successfully")

    # ── Cross-cutting queries (kept in facade) ────────────────

    def get_summary_stats(self) -> dict:
        """Get database summary statistics"""
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            stats = {}
            cursor.execute("SELECT COUNT(*) FROM instruments")
            stats["total_instruments"] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM expiries")
            stats["total_expiries"] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM contracts")
            stats["total_contracts"] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM historical_data")
            stats["total_candles"] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM expiries WHERE contracts_fetched = FALSE")
            stats["pending_expiries"] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM contracts WHERE data_fetched = FALSE")
            stats["pending_contracts"] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM instrument_master")
            stats["master_instruments"] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM candle_data")
            stats["total_candle_data"] = cursor.fetchone()[0]
            return stats

    def vacuum(self) -> None:
        """Optimize database"""
        with self.get_connection() as conn:
            conn.execute("CHECKPOINT")
            logger.info("Database optimized (CHECKPOINT completed)")

    # ── Backward-compatible delegation methods ────────────────
    # These delegate to the appropriate repository so that existing
    # callers (blueprints, CLI, scripts) continue to work unchanged.

    # Credentials
    def save_credentials(self, *a: Any, **kw: Any) -> Any:
        return self.credentials.save_credentials(*a, **kw)

    def get_credentials(self) -> Any:
        return self.credentials.get_credentials()

    def save_token(self, *a: Any, **kw: Any) -> Any:
        return self.credentials.save_token(*a, **kw)

    def _ensure_api_keys_table(self, conn: Any) -> Any:
        return self.credentials._ensure_api_keys_table(conn)

    def create_api_key(self, *a: Any, **kw: Any) -> Any:
        return self.credentials.create_api_key(*a, **kw)

    def verify_api_key(self, *a: Any, **kw: Any) -> Any:
        return self.credentials.verify_api_key(*a, **kw)

    def list_api_keys(self) -> Any:
        return self.credentials.list_api_keys()

    def revoke_api_key(self, *a: Any, **kw: Any) -> Any:
        return self.credentials.revoke_api_key(*a, **kw)

    # Instruments (default instruments CRUD + F&O)
    def setup_default_instruments(self) -> Any:
        return self.instruments.setup_default_instruments()

    def get_default_instruments(self) -> Any:
        return self.instruments.get_default_instruments()

    def get_active_instruments(self) -> Any:
        return self.instruments.get_active_instruments()

    def add_instrument(self, *a: Any, **kw: Any) -> Any:
        return self.instruments.add_instrument(*a, **kw)

    def toggle_instrument(self, *a: Any, **kw: Any) -> Any:
        return self.instruments.toggle_instrument(*a, **kw)

    def remove_instrument(self, *a: Any, **kw: Any) -> Any:
        return self.instruments.remove_instrument(*a, **kw)

    def insert_instrument(self, *a: Any, **kw: Any) -> Any:
        return self.instruments.insert_instrument(*a, **kw)

    def get_fo_underlying_instruments(self, *a: Any, **kw: Any) -> Any:
        return self.instruments.get_fo_underlying_instruments(*a, **kw)

    def get_fo_available_instruments(self, *a: Any, **kw: Any) -> Any:
        return self.instruments.get_fo_available_instruments(*a, **kw)

    def bulk_import_fo_instruments(self, *a: Any, **kw: Any) -> Any:
        return self.instruments.bulk_import_fo_instruments(*a, **kw)

    # Contracts (expiries, contracts, OpenAlgo queries, retry/reset)
    def insert_expiries(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.insert_expiries(*a, **kw)

    def get_pending_expiries(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.get_pending_expiries(*a, **kw)

    def mark_expiry_contracts_fetched(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.mark_expiry_contracts_fetched(*a, **kw)

    def insert_contracts(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.insert_contracts(*a, **kw)

    def get_pending_contracts(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.get_pending_contracts(*a, **kw)

    def get_fetched_keys(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.get_fetched_keys(*a, **kw)

    def get_contract_by_openalgo_symbol(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.get_contract_by_openalgo_symbol(*a, **kw)

    def get_contracts_by_base_symbol(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.get_contracts_by_base_symbol(*a, **kw)

    def get_option_chain(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.get_option_chain(*a, **kw)

    def get_futures_by_symbol(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.get_futures_by_symbol(*a, **kw)

    def search_openalgo_symbols(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.search_openalgo_symbols(*a, **kw)

    def get_expiries_for_instrument(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.get_expiries_for_instrument(*a, **kw)

    def get_contracts_for_expiry(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.get_contracts_for_expiry(*a, **kw)

    def increment_fetch_attempt(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.increment_fetch_attempt(*a, **kw)

    def get_failed_contracts(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.get_failed_contracts(*a, **kw)

    def reset_fetch_attempts(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.reset_fetch_attempts(*a, **kw)

    def reset_contracts_for_refetch(self, *a: Any, **kw: Any) -> Any:
        return self.contracts.reset_contracts_for_refetch(*a, **kw)

    # Historical data
    def insert_historical_data(self, *a: Any, **kw: Any) -> Any:
        return self.historical.insert_historical_data(*a, **kw)

    def mark_contract_no_data(self, *a: Any, **kw: Any) -> Any:
        return self.historical.mark_contract_no_data(*a, **kw)

    def get_historical_data(self, *a: Any, **kw: Any) -> Any:
        return self.historical.get_historical_data(*a, **kw)

    def get_historical_data_count(self, *a: Any, **kw: Any) -> Any:
        return self.historical.get_historical_data_count(*a, **kw)

    # Instrument master
    def bulk_insert_instrument_master(self, *a: Any, **kw: Any) -> Any:
        return self.instrument_master.bulk_insert_instrument_master(*a, **kw)

    def search_instrument_master(self, *a: Any, **kw: Any) -> Any:
        return self.instrument_master.search_instrument_master(*a, **kw)

    def get_instrument_master_segments(self) -> Any:
        return self.instrument_master.get_instrument_master_segments()

    def get_instruments_by_segment(self, *a: Any, **kw: Any) -> Any:
        return self.instrument_master.get_instruments_by_segment(*a, **kw)

    def get_instrument_master_last_sync(self) -> Any:
        return self.instrument_master.get_instrument_master_last_sync()

    def get_instrument_types_by_segment(self, *a: Any, **kw: Any) -> Any:
        return self.instrument_master.get_instrument_types_by_segment(*a, **kw)

    def get_instrument_keys_by_segment(self, *a: Any, **kw: Any) -> Any:
        return self.instrument_master.get_instrument_keys_by_segment(*a, **kw)

    def get_instrument_master_count(self, *a: Any, **kw: Any) -> Any:
        return self.instrument_master.get_instrument_master_count(*a, **kw)

    # Candle data
    def insert_candle_data(self, *a: Any, **kw: Any) -> Any:
        return self.candles.insert_candle_data(*a, **kw)

    def get_candle_data(self, *a: Any, **kw: Any) -> Any:
        return self.candles.get_candle_data(*a, **kw)

    def get_candle_data_count(self, *a: Any, **kw: Any) -> Any:
        return self.candles.get_candle_data_count(*a, **kw)

    def get_candle_collection_status(self, *a: Any, **kw: Any) -> Any:
        return self.candles.get_candle_collection_status(*a, **kw)

    def get_last_candle_timestamps(self, *a: Any, **kw: Any) -> Any:
        return self.candles.get_last_candle_timestamps(*a, **kw)

    def get_candle_analytics_summary(self) -> Any:
        return self.candles.get_candle_analytics_summary()

    # Watchlists
    def create_watchlist(self, *a: Any, **kw: Any) -> Any:
        return self.watchlists.create_watchlist(*a, **kw)

    def get_watchlists(self) -> Any:
        return self.watchlists.get_watchlists()

    def get_watchlist_items(self, *a: Any, **kw: Any) -> Any:
        return self.watchlists.get_watchlist_items(*a, **kw)

    def add_to_watchlist(self, *a: Any, **kw: Any) -> Any:
        return self.watchlists.add_to_watchlist(*a, **kw)

    def remove_from_watchlist(self, *a: Any, **kw: Any) -> Any:
        return self.watchlists.remove_from_watchlist(*a, **kw)

    def delete_watchlist(self, *a: Any, **kw: Any) -> Any:
        return self.watchlists.delete_watchlist(*a, **kw)

    # Jobs
    def create_job(self, *a: Any, **kw: Any) -> Any:
        return self.jobs.create_job(*a, **kw)

    def update_job_status(self, *a: Any, **kw: Any) -> Any:
        return self.jobs.update_job_status(*a, **kw)

    def save_checkpoint(self, *a: Any, **kw: Any) -> Any:
        return self.jobs.save_checkpoint(*a, **kw)

    def __str__(self) -> str:
        stats = self.get_summary_stats()
        return (
            f"DatabaseManager(type={self.db_type}, "
            f"instruments={stats['total_instruments']}, "
            f"contracts={stats['total_contracts']}, "
            f"candles={stats['total_candles']:,})"
        )
