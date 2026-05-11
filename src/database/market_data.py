"""
MarketDataStore - DuckDB-backed columnar store for OHLCV+OI market data.

Design principles
-----------------
* All historical bars live in a single wide, denormalized table.  Repeated
  string / category columns (base_symbol, expiry_date, contract_type) compress
  to near-zero on disk thanks to DuckDB's RLE / dictionary encoding, while
  letting analytical queries skip joins entirely.

* The SQLite metadata DB is ATTACHed read-only, so cross-DB joins
  (e.g. lot_size, tick_size) are one statement away when needed.

* All read APIs return pandas DataFrames (zero-copy via Arrow), making the
  store directly usable for backtesting / scanning loops.

* DuckDB only allows one connection mode (read-write OR read-only) per file
  per process.  We therefore keep a single process-wide read-write connection
  and obtain thread-local cursors via `conn.cursor()` for concurrent reads.
  Writes are serialized through `self._write_lock`.
"""
from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import duckdb
import pandas as pd

from ..config import config
from ..utils.openalgo_symbol import OpenAlgoSymbolGenerator, to_openalgo_symbol

logger = logging.getLogger(__name__)


# Allowed timeframes mapped to time_bucket intervals.
TIMEFRAMES: Dict[str, str] = {
    "1m": "1 minute",
    "3m": "3 minutes",
    "5m": "5 minutes",
    "15m": "15 minutes",
    "30m": "30 minutes",
    "1h": "1 hour",
    "1d": "1 day",
}


class MarketDataStore:
    """
    DuckDB-backed market data store.

    One instance per process.  Internally maintains:
      * a single read-write connection (used by the collector / migration)
      * read-only connections opened on demand for queries

    The SQLite metadata DB is ATTACHed read-only as schema `meta` on each
    connection (so cross-DB joins work in SQL).
    """

    _instance: Optional["MarketDataStore"] = None
    _instance_lock = threading.Lock()

    # --- singleton accessor ------------------------------------------------
    @classmethod
    def instance(cls) -> "MarketDataStore":
        """Process-wide singleton (Flask + collector share one writer)."""
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    # --- lifecycle ---------------------------------------------------------
    def __init__(
        self,
        db_path: Optional[Path] = None,
        sqlite_path: Optional[Path] = None,
    ):
        self.db_path = Path(db_path or config.MARKET_DATA_DB_PATH)
        self.sqlite_path = Path(sqlite_path or config.DB_PATH)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Single read-write connection.  All access goes through cursors
        # (which are thread-safe in DuckDB).  Writes are additionally
        # serialized through self._write_lock.
        self._write_lock = threading.Lock()
        self._conn: duckdb.DuckDBPyConnection = duckdb.connect(
            str(self.db_path), read_only=False
        )
        self._attach_sqlite(self._conn)

        self._init_schema()

    def close(self) -> None:
        """Close the underlying connection."""
        try:
            self._conn.close()
        except Exception:
            pass

    # --- connection helpers ------------------------------------------------
    def _attach_sqlite(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Attach the SQLite metadata DB as schema 'meta' (read-only)."""
        if not self.sqlite_path.exists():
            return
        try:
            conn.execute("INSTALL sqlite")
        except duckdb.Error:
            # extension may already be installed; LOAD will surface real issues
            pass
        conn.execute("LOAD sqlite")
        # Detach first if a stale attachment exists, then attach.
        try:
            conn.execute("DETACH meta")
        except duckdb.Error:
            pass
        conn.execute(
            f"ATTACH '{self.sqlite_path.as_posix()}' AS meta (TYPE sqlite, READ_ONLY)"
        )

    @contextmanager
    def write(self):
        """Yield a write cursor under the writer lock."""
        with self._write_lock:
            cur = self._conn.cursor()
            try:
                yield cur
            finally:
                cur.close()

    @contextmanager
    def read(self):
        """Yield an independent cursor for read queries (no lock)."""
        cur = self._conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    # --- schema bootstrap --------------------------------------------------
    def _init_schema(self) -> None:
        """Create the market_data table, indexes, and resampled views."""
        with self.write() as conn:
            # Threading + memory tuned for analytical workloads on a desktop
            # box.  Safe defaults that still respect a 4 GB-RAM minimum.
            try:
                conn.execute("PRAGMA enable_object_cache")
            except duckdb.Error:
                pass
            try:
                conn.execute("PRAGMA threads=4")
            except duckdb.Error:
                pass

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_data (
                    expired_instrument_key  VARCHAR  NOT NULL,
                    openalgo_symbol         VARCHAR  NOT NULL,
                    base_symbol             VARCHAR  NOT NULL,
                    instrument_key          VARCHAR  NOT NULL,
                    expiry_date             DATE     NOT NULL,
                    contract_type           VARCHAR  NOT NULL,
                    strike_price            DOUBLE,
                    trading_symbol          VARCHAR,
                    ts                      TIMESTAMPTZ NOT NULL,
                    open                    DOUBLE   NOT NULL,
                    high                    DOUBLE   NOT NULL,
                    low                     DOUBLE   NOT NULL,
                    close                   DOUBLE   NOT NULL,
                    volume                  BIGINT   NOT NULL,
                    oi                      BIGINT,
                    PRIMARY KEY (expired_instrument_key, ts)
                )
                """
            )

            # ART indexes on the columns we filter by most.  In DuckDB an
            # index helps point lookups and tight ranges; the columnar
            # zone-map already accelerates broad scans.
            for col in (
                "openalgo_symbol",
                "base_symbol",
                "expiry_date",
                "ts",
            ):
                try:
                    conn.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_md_{col} "
                        f"ON market_data({col})"
                    )
                except duckdb.Error:
                    # CREATE INDEX is best-effort; an older DuckDB build
                    # without index support should still work.
                    pass

            # Resampled views (computed on demand from the 1-min base).
            for tf, interval in TIMEFRAMES.items():
                if tf == "1m":
                    continue
                conn.execute(
                    f"""
                    CREATE OR REPLACE VIEW market_data_{tf} AS
                    SELECT
                        expired_instrument_key,
                        any_value(openalgo_symbol)  AS openalgo_symbol,
                        any_value(base_symbol)      AS base_symbol,
                        any_value(instrument_key)   AS instrument_key,
                        any_value(expiry_date)      AS expiry_date,
                        any_value(contract_type)    AS contract_type,
                        any_value(strike_price)     AS strike_price,
                        any_value(trading_symbol)   AS trading_symbol,
                        time_bucket(INTERVAL '{interval}', ts) AS ts,
                        first(open ORDER BY ts) AS open,
                        max(high)               AS high,
                        min(low)                AS low,
                        last(close ORDER BY ts) AS close,
                        sum(volume)             AS volume,
                        last(oi ORDER BY ts)    AS oi
                    FROM market_data
                    GROUP BY expired_instrument_key,
                             time_bucket(INTERVAL '{interval}', ts)
                    """
                )

        logger.info("MarketDataStore schema ready at %s", self.db_path)

    # =====================================================================
    # WRITE PATH
    # =====================================================================
    # DuckDB's optimal insert path is *bulk*: register a pandas DataFrame
    # and run a single `INSERT ... SELECT FROM df`.  Row-by-row executemany
    # is 10x-100x slower because it pays parsing + constraint-check cost
    # per row.  We build the batch once with vectorized pandas operations
    # and ingest it in a single statement.
    # =====================================================================
    def insert_candles(
        self,
        contract: Dict[str, Any],
        candles: Sequence[Sequence[Any]],
    ) -> int:
        """
        Insert OHLCV+OI candles for one contract.

        Args:
            contract: Contract dict (as stored in SQLite `contracts` table).
                      Must carry enough info to derive the denormalized
                      columns (instrument_key, expiry_date, contract_type,
                      strike_price, trading_symbol, openalgo_symbol).
            candles:  Iterable of rows of the form
                      [timestamp, open, high, low, close, volume, oi]
                      where timestamp may be an ISO-8601 string (Upstox
                      default) or epoch milliseconds.

        Returns the number of rows actually inserted.
        """
        if not candles:
            return 0

        # Derive denormalized columns once per contract.
        meta = self._contract_meta(contract)

        # Build a single DataFrame from the candle batch.  All conversions
        # are vectorized — no per-row Python loop.
        arr = candles if isinstance(candles, list) else list(candles)
        df = pd.DataFrame(arr, columns=[
            "_ts", "open", "high", "low", "close", "volume", "oi"
        ][: len(arr[0])])

        # If oi wasn't supplied, add a NULL column so the schema matches.
        if "oi" not in df.columns:
            df["oi"] = pd.NA

        # Vectorized timestamp parse: handles ISO-8601 strings and epoch ms.
        ts_series = df["_ts"]
        if ts_series.dtype.kind in ("i", "f"):
            df["ts"] = pd.to_datetime(ts_series, unit="ms", utc=True)
        else:
            df["ts"] = pd.to_datetime(ts_series, utc=True, errors="coerce")
        df.drop(columns=["_ts"], inplace=True)

        # Drop rows where the timestamp failed to parse.
        bad = df["ts"].isna()
        if bad.any():
            logger.warning(
                "Dropping %d candles with bad timestamps for %s",
                int(bad.sum()), meta["expired_instrument_key"],
            )
            df = df.loc[~bad]
        if df.empty:
            return 0

        # Coerce numeric types up-front so DuckDB does not have to.
        for col, dtype in (
            ("open", "float64"), ("high", "float64"),
            ("low",  "float64"), ("close","float64"),
            ("volume", "Int64"), ("oi",   "Int64"),
        ):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

        # Constant denormalized columns.
        n = len(df)
        df["expired_instrument_key"] = meta["expired_instrument_key"]
        df["openalgo_symbol"]        = meta["openalgo_symbol"]
        df["base_symbol"]            = meta["base_symbol"]
        df["instrument_key"]         = meta["instrument_key"]
        df["expiry_date"]            = pd.to_datetime(meta["expiry_date"]).date() if meta["expiry_date"] else None
        df["contract_type"]          = meta["contract_type"]
        df["strike_price"]           = meta["strike_price"]
        df["trading_symbol"]         = meta["trading_symbol"]

        # Column order must match the table.
        df = df[[
            "expired_instrument_key", "openalgo_symbol", "base_symbol",
            "instrument_key", "expiry_date", "contract_type", "strike_price",
            "trading_symbol", "ts",
            "open", "high", "low", "close", "volume", "oi",
        ]]

        with self.write() as conn:
            # Register the DataFrame as a temporary view, then INSERT OR
            # REPLACE in one shot.  DuckDB streams the columns directly
            # from pandas/Arrow without serializing each row to SQL.
            conn.register("_incoming_candles", df)
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO market_data "
                    "SELECT * FROM _incoming_candles"
                )
            finally:
                conn.unregister("_incoming_candles")

        return n

    def _contract_meta(self, contract: Dict[str, Any]) -> Dict[str, Any]:
        """Derive denormalized columns for a contract dict.

        Accepts both the raw API contract shape (where `instrument_key` is the
        expired-contract id and `underlying_key` is the underlying) and the
        SQLite row shape (where `instrument_key` is the underlying and the
        contract id lives in `expired_instrument_key`).
        """
        # --- contract id (the per-contract expired key) -------------------
        if contract.get("expired_instrument_key"):
            expired_key = contract["expired_instrument_key"]
            underlying_key = (
                contract.get("underlying_key")
                or contract.get("instrument_key", "")
            )
        else:
            # API shape
            expired_key = contract.get("instrument_key", "")
            underlying_key = contract.get("underlying_key", "")

        # --- contract type -------------------------------------------------
        contract_type = (
            contract.get("contract_type")
            or contract.get("instrument_type")
            or ""
        ).upper()
        if contract_type == "CALL":
            contract_type = "CE"
        elif contract_type == "PUT":
            contract_type = "PE"
        elif contract_type in ("FUTURE", "FUTURES"):
            contract_type = "FUT"

        # --- openalgo symbol (canonical) ----------------------------------
        openalgo_symbol = (
            contract.get("openalgo_symbol")
            or to_openalgo_symbol(contract)
        )

        # --- base_symbol — derive from the underlying_key (authoritative)
        # rather than parsing the openalgo symbol back, because the canonical
        # OpenAlgo format concatenates year and strike (e.g. NIFTY28APR2624000CE
        # where '26' + '24000') and the existing parser confuses the two.
        base_symbol = (contract.get("base_symbol") or "").upper()
        if not base_symbol and underlying_key:
            label = underlying_key.split("|")[-1]
            base_symbol = OpenAlgoSymbolGenerator.SYMBOL_MAPPING.get(
                label, label.replace(" ", "").upper()
            )
        # Final fallback: strip the trailing <expiry><strike><CE|PE|FUT>
        # from the openalgo symbol using the known expiry string.
        if not base_symbol and openalgo_symbol:
            expiry_token = OpenAlgoSymbolGenerator.format_expiry_date(
                contract.get("expiry") or contract.get("expiry_date") or ""
            )
            if expiry_token and expiry_token in openalgo_symbol:
                base_symbol = openalgo_symbol.split(expiry_token, 1)[0]

        # --- strike --------------------------------------------------------
        strike = contract.get("strike_price")
        try:
            strike = float(strike) if strike not in (None, "", 0) else None
        except (TypeError, ValueError):
            strike = None
        # Futures never have a strike.
        if contract_type == "FUT":
            strike = None

        return {
            "expired_instrument_key": expired_key,
            "openalgo_symbol": openalgo_symbol,
            "base_symbol": base_symbol,
            "instrument_key": underlying_key,
            "expiry_date": contract.get("expiry") or contract.get("expiry_date") or "",
            "contract_type": contract_type,
            "strike_price": strike,
            "trading_symbol": contract.get("trading_symbol", ""),
        }

    # =====================================================================
    # READ PATH
    # =====================================================================
    def _table_for(self, timeframe: str) -> str:
        tf = timeframe.lower()
        if tf == "1m":
            return "market_data"
        if tf not in TIMEFRAMES:
            raise ValueError(
                f"Unknown timeframe {timeframe!r}. Valid: {list(TIMEFRAMES)}"
            )
        return f"market_data_{tf}"

    def get_bars(
        self,
        openalgo_symbol: Optional[str] = None,
        expired_instrument_key: Optional[str] = None,
        timeframe: str = "1m",
        from_ts: Optional[str | datetime] = None,
        to_ts: Optional[str | datetime] = None,
    ) -> pd.DataFrame:
        """
        Return OHLCV+OI bars for one contract as a DataFrame, indexed by `ts`.
        """
        if not openalgo_symbol and not expired_instrument_key:
            raise ValueError("Provide either openalgo_symbol or expired_instrument_key")

        table = self._table_for(timeframe)
        clauses, params = [], []
        if openalgo_symbol:
            clauses.append("openalgo_symbol = ?")
            params.append(openalgo_symbol)
        if expired_instrument_key:
            clauses.append("expired_instrument_key = ?")
            params.append(expired_instrument_key)
        if from_ts is not None:
            clauses.append("ts >= ?")
            params.append(pd.Timestamp(from_ts).to_pydatetime())
        if to_ts is not None:
            clauses.append("ts <= ?")
            params.append(pd.Timestamp(to_ts).to_pydatetime())

        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        query = f"""
            SELECT ts, open, high, low, close, volume, oi,
                   openalgo_symbol, trading_symbol, contract_type, strike_price,
                   expiry_date
            FROM {table}
            {where}
            ORDER BY ts
        """
        with self.read() as conn:
            df = conn.execute(query, params).df()
        if not df.empty:
            df = df.set_index("ts")
        return df

    def get_chain(
        self,
        base_symbol: str,
        expiry: str,
        at: Optional[str | datetime] = None,
    ) -> pd.DataFrame:
        """
        Snapshot the option chain for `base_symbol` / `expiry` at time `at`
        (defaults to the latest available timestamp).

        Returns one row per contract with the chosen-timestamp OHLCV+OI.
        """
        params: List[Any] = [base_symbol.upper(), expiry]
        time_filter = ""
        if at is not None:
            params.append(pd.Timestamp(at).to_pydatetime())
            time_filter = "AND ts <= ?"

        query = f"""
            WITH ranked AS (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY expired_instrument_key
                           ORDER BY ts DESC
                       ) AS rn
                FROM market_data
                WHERE base_symbol = ? AND expiry_date = ?
                  {time_filter}
            )
            SELECT openalgo_symbol, trading_symbol, contract_type, strike_price,
                   ts, open, high, low, close, volume, oi, expired_instrument_key
            FROM ranked
            WHERE rn = 1
            ORDER BY contract_type, strike_price
        """
        with self.read() as conn:
            return conn.execute(query, params).df()

    def scan(
        self,
        base_symbol: Optional[str] = None,
        expiry: Optional[str] = None,
        contract_type: Optional[str] = None,
        strike_lt: Optional[float] = None,
        strike_gt: Optional[float] = None,
        strike_eq: Optional[float] = None,
        date: Optional[str] = None,
        from_ts: Optional[str | datetime] = None,
        to_ts: Optional[str | datetime] = None,
        timeframe: str = "1m",
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        General-purpose vectorized scan with common dimension filters.
        """
        table = self._table_for(timeframe)
        clauses, params = [], []
        if base_symbol:
            clauses.append("base_symbol = ?")
            params.append(base_symbol.upper())
        if expiry:
            clauses.append("expiry_date = ?")
            params.append(expiry)
        if contract_type:
            clauses.append("contract_type = ?")
            params.append(contract_type.upper())
        if strike_eq is not None:
            clauses.append("strike_price = ?")
            params.append(strike_eq)
        if strike_lt is not None:
            clauses.append("strike_price < ?")
            params.append(strike_lt)
        if strike_gt is not None:
            clauses.append("strike_price > ?")
            params.append(strike_gt)
        if date:
            clauses.append("CAST(ts AS DATE) = ?")
            params.append(date)
        if from_ts is not None:
            clauses.append("ts >= ?")
            params.append(pd.Timestamp(from_ts).to_pydatetime())
        if to_ts is not None:
            clauses.append("ts <= ?")
            params.append(pd.Timestamp(to_ts).to_pydatetime())

        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        lim = f"LIMIT {int(limit)}" if limit else ""
        query = f"""
            SELECT openalgo_symbol, ts, open, high, low, close, volume, oi,
                   base_symbol, expiry_date, contract_type, strike_price,
                   trading_symbol, expired_instrument_key
            FROM {table}
            {where}
            ORDER BY ts, openalgo_symbol
            {lim}
        """
        with self.read() as conn:
            return conn.execute(query, params).df()

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> pd.DataFrame:
        """
        Run arbitrary read-only SQL.  `meta.<sqlite_table>` is available for
        joining contract metadata (lot_size, tick_size, etc.).
        """
        with self.read() as conn:
            return conn.execute(query, params or []).df()

    # --- export helpers ----------------------------------------------------
    def to_parquet(self, dest: str | Path, query: str, params: Optional[Sequence[Any]] = None) -> Path:
        """Write the result of `query` to a Parquet file."""
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        with self.read() as conn:
            conn.execute(
                f"COPY ({query}) TO '{dest.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)",
                params or [],
            )
        return dest

    def to_csv(self, dest: str | Path, query: str, params: Optional[Sequence[Any]] = None) -> Path:
        """Write the result of `query` to a CSV file."""
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        with self.read() as conn:
            conn.execute(
                f"COPY ({query}) TO '{dest.as_posix()}' (FORMAT CSV, HEADER, DELIMITER ',')",
                params or [],
            )
        return dest

    # --- stats / maintenance ----------------------------------------------
    def get_summary(self) -> Dict[str, Any]:
        """Lightweight stats for the dashboard."""
        with self.read() as conn:
            row = conn.execute(
                """
                SELECT count(*) AS total_candles,
                       count(DISTINCT expired_instrument_key) AS contracts_with_data,
                       count(DISTINCT base_symbol) AS unique_symbols,
                       min(ts) AS min_ts,
                       max(ts) AS max_ts
                FROM market_data
                """
            ).fetchone()
        keys = ["total_candles", "contracts_with_data", "unique_symbols", "min_ts", "max_ts"]
        return dict(zip(keys, row or [0, 0, 0, None, None]))

    def list_symbols(self, base_symbol: Optional[str] = None, limit: int = 500) -> pd.DataFrame:
        """Distinct openalgo_symbols present in market_data."""
        clause = "WHERE base_symbol = ?" if base_symbol else ""
        params = [base_symbol.upper()] if base_symbol else []
        with self.read() as conn:
            return conn.execute(
                f"""
                SELECT openalgo_symbol,
                       any_value(base_symbol)    AS base_symbol,
                       any_value(expiry_date)    AS expiry_date,
                       any_value(contract_type)  AS contract_type,
                       any_value(strike_price)   AS strike_price,
                       count(*) AS bars,
                       min(ts) AS first_ts,
                       max(ts) AS last_ts
                FROM market_data
                {clause}
                GROUP BY openalgo_symbol
                ORDER BY base_symbol, expiry_date, contract_type, strike_price
                LIMIT {int(limit)}
                """,
                params,
            ).df()

    def vacuum(self) -> None:
        """Flush WAL into the main file (lightweight maintenance)."""
        with self.write() as conn:
            conn.execute("CHECKPOINT")
            logger.info("MarketDataStore checkpointed")

    def compact(self) -> None:
        """
        Re-cluster market_data on disk so zone-maps line up with the columns
        we filter on the most (base_symbol, expiry_date, openalgo_symbol, ts).

        This is the DuckDB equivalent of SQLite's CLUSTER + VACUUM.  After a
        large bulk load (a migration or a multi-week collection), running
        this once compacts the row groups and unlocks big read-side wins.
        Safe to call repeatedly; expensive (rewrites the whole table).
        """
        with self.write() as conn:
            logger.info("MarketDataStore: rewriting market_data for clustering...")
            conn.execute("BEGIN")
            try:
                conn.execute(
                    """
                    CREATE OR REPLACE TABLE market_data_tmp AS
                    SELECT *
                    FROM market_data
                    ORDER BY base_symbol, expiry_date, contract_type,
                             strike_price NULLS LAST,
                             openalgo_symbol, ts
                    """
                )
                conn.execute("DROP TABLE market_data")
                conn.execute("ALTER TABLE market_data_tmp RENAME TO market_data")
                # Re-create the indexes after rename.
                for col in ("openalgo_symbol", "base_symbol", "expiry_date", "ts"):
                    try:
                        conn.execute(
                            f"CREATE INDEX IF NOT EXISTS idx_md_{col} "
                            f"ON market_data({col})"
                        )
                    except duckdb.Error:
                        pass
                conn.execute("COMMIT")
                conn.execute("CHECKPOINT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            logger.info("MarketDataStore: compact() complete")
