"""Backtest engine — loads data, runs strategy, returns results."""

from __future__ import annotations

import logging
from typing import Any, Callable

from .metrics import compute_metrics
from .sandbox import compile_strategy
from .strategy import Strategy

logger = logging.getLogger(__name__)

MAX_BARS = 200_000


class BacktestEngine:
    """Loads OHLCV data from DuckDB and runs a strategy against it."""

    def __init__(self, db_manager: Any) -> None:
        self.db = db_manager

    def run(
        self,
        strategy_code: str,
        instrument_key: str,
        data_source: str = "candle_data",
        interval: str = "1day",
        from_date: str | None = None,
        to_date: str | None = None,
        initial_capital: float = 100000.0,
        commission_rate: float = 0.0003,
        progress_callback: Callable[[int, str], None] | None = None,
    ) -> dict:
        """Run a backtest and return results.

        Returns dict with keys: metrics, trades, equity_curve, bars_processed.
        """
        if progress_callback:
            progress_callback(5, "Compiling strategy...")

        # Compile strategy from user code
        strategy = compile_strategy(strategy_code)
        strategy._cash = initial_capital
        strategy._initial_capital = initial_capital
        strategy._commission_rate = commission_rate

        if progress_callback:
            progress_callback(10, "Loading market data...")

        # Load OHLCV data
        bars = self._load_data(instrument_key, data_source, interval, from_date, to_date)

        if not bars:
            return {
                "metrics": {},
                "trades": [],
                "equity_curve": [],
                "bars_processed": 0,
                "error": "No data found for the given parameters.",
            }

        if len(bars) > MAX_BARS:
            bars = bars[:MAX_BARS]

        total_bars = len(bars)

        if progress_callback:
            progress_callback(15, f"Running strategy on {total_bars:,} bars...")

        # Initialize strategy
        strategy.on_init()

        # Run bar-by-bar
        for i, bar in enumerate(bars):
            strategy._bar_index = i
            strategy._current_price = float(bar["close"])
            strategy._current_time = str(bar["timestamp"])

            strategy.on_candle(
                timestamp=str(bar["timestamp"]),
                open=float(bar["open"]),
                high=float(bar["high"]),
                low=float(bar["low"]),
                close=float(bar["close"]),
                volume=int(bar["volume"]),
                oi=int(bar.get("oi", 0)),
            )

            # Snapshot equity every bar (or every N bars for large datasets)
            if total_bars <= 5000 or i % max(1, total_bars // 2000) == 0 or i == total_bars - 1:
                strategy._snapshot_equity()

            # Progress updates every 10%
            if progress_callback and total_bars > 100 and i % (total_bars // 10) == 0:
                pct = 15 + int((i / total_bars) * 75)
                progress_callback(pct, f"Processing bar {i + 1:,}/{total_bars:,}")

        # Finalize
        strategy.on_finish()

        # Force-close any open position at last price
        if strategy._position > 0:
            strategy.sell(strategy._position)
        elif strategy._position < 0:
            strategy.cover(abs(strategy._position))

        # Final equity snapshot
        strategy._snapshot_equity()

        if progress_callback:
            progress_callback(95, "Computing metrics...")

        # Compute metrics
        metrics = compute_metrics(
            strategy._trades,
            strategy._equity_curve,
            initial_capital,
            strategy._total_commission,
        )

        # Serialize trades
        trades_data = [
            {
                "entry_time": t.entry_time,
                "exit_time": t.exit_time,
                "side": t.side,
                "qty": t.qty,
                "entry_price": round(t.entry_price, 2),
                "exit_price": round(t.exit_price, 2),
                "pnl": round(t.pnl, 2),
                "commission": round(t.commission, 4),
            }
            for t in strategy._trades
        ]

        # Downsample equity curve if too large
        equity_data = strategy._equity_curve
        if len(equity_data) > 2000:
            step = len(equity_data) // 2000
            equity_data = equity_data[::step] + [equity_data[-1]]

        if progress_callback:
            progress_callback(100, "Backtest complete!")

        return {
            "metrics": metrics,
            "trades": trades_data,
            "equity_curve": equity_data,
            "bars_processed": total_bars,
        }

    def _load_data(
        self,
        instrument_key: str,
        data_source: str,
        interval: str,
        from_date: str | None,
        to_date: str | None,
    ) -> list[dict]:
        """Load OHLCV bars from DuckDB."""
        with self.db.get_read_connection() as conn:
            if data_source == "candle_data":
                return self._load_candle_data(conn, instrument_key, interval, from_date, to_date)
            elif data_source == "historical_data":
                return self._load_historical_data(conn, instrument_key, from_date, to_date)
            else:
                raise ValueError(f"Unknown data source: {data_source}")

    def _load_candle_data(
        self,
        conn: Any,
        instrument_key: str,
        interval: str,
        from_date: str | None,
        to_date: str | None,
    ) -> list[dict]:
        """Load from candle_data table."""
        sql = """
            SELECT timestamp, open, high, low, close, volume, oi
            FROM candle_data
            WHERE instrument_key = ? AND interval = ?
        """
        params: list[Any] = [instrument_key, interval]

        if from_date:
            sql += " AND timestamp >= ?"
            params.append(from_date)
        if to_date:
            sql += " AND timestamp <= ?"
            params.append(to_date)

        sql += " ORDER BY timestamp ASC"

        rows = conn.execute(sql, params).fetchall()
        columns = [d[0] for d in conn.description] if conn.description else []
        return [dict(zip(columns, row, strict=False)) for row in rows]

    def _load_historical_data(
        self,
        conn: Any,
        instrument_key: str,
        from_date: str | None,
        to_date: str | None,
    ) -> list[dict]:
        """Load from historical_data table (F&O data)."""
        sql = """
            SELECT timestamp, open, high, low, close, volume, oi
            FROM historical_data
            WHERE expired_instrument_key = ?
        """
        params: list[Any] = [instrument_key]

        if from_date:
            sql += " AND timestamp >= ?"
            params.append(from_date)
        if to_date:
            sql += " AND timestamp <= ?"
            params.append(to_date)

        sql += " ORDER BY timestamp ASC"

        rows = conn.execute(sql, params).fetchall()
        columns = [d[0] for d in conn.description] if conn.description else []
        return [dict(zip(columns, row, strict=False)) for row in rows]

    def get_available_instruments(self, data_source: str = "candle_data") -> list[dict]:
        """List instruments that have data available for backtesting."""
        with self.db.get_read_connection() as conn:
            if data_source == "candle_data":
                rows = conn.execute("""
                    SELECT DISTINCT cd.instrument_key,
                           COALESCE(im.name, cd.instrument_key) as display_name,
                           COUNT(*) as bar_count,
                           MIN(cd.timestamp) as first_date,
                           MAX(cd.timestamp) as last_date
                    FROM candle_data cd
                    LEFT JOIN instrument_master im ON cd.instrument_key = im.instrument_key
                    GROUP BY cd.instrument_key, im.name
                    ORDER BY cd.instrument_key
                """).fetchall()
            else:
                rows = conn.execute("""
                    SELECT DISTINCT c.instrument_key,
                           COALESCE(i.symbol, c.instrument_key) as display_name,
                           COUNT(DISTINCT c.expiry_date) as expiry_count,
                           COUNT(DISTINCT c.expired_instrument_key) as contract_count,
                           COUNT(*) as bar_count,
                           MIN(h.timestamp) as first_date,
                           MAX(h.timestamp) as last_date
                    FROM historical_data h
                    JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                    JOIN instruments i ON c.instrument_key = i.instrument_key
                    GROUP BY c.instrument_key, i.symbol
                    ORDER BY i.symbol
                """).fetchall()

            columns = [d[0] for d in conn.description] if conn.description else []
            results = []
            for row in rows:
                d = dict(zip(columns, row, strict=False))
                for k in ("first_date", "last_date"):
                    if hasattr(d.get(k), "isoformat"):
                        d[k] = d[k].isoformat()
                results.append(d)
            return results

    def get_fo_expiries(self, instrument_key: str) -> list[dict]:
        """List distinct expiry dates for an underlying with contract/bar counts."""
        with self.db.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT DISTINCT c.expiry_date,
                       COUNT(DISTINCT c.expired_instrument_key) as contract_count,
                       COUNT(*) as bar_count,
                       MIN(h.timestamp) as first_date,
                       MAX(h.timestamp) as last_date
                FROM historical_data h
                JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                WHERE c.instrument_key = ?
                GROUP BY c.expiry_date
                ORDER BY c.expiry_date
            """, [instrument_key]).fetchall()

            columns = [d[0] for d in conn.description] if conn.description else []
            results = []
            for row in rows:
                d = dict(zip(columns, row, strict=False))
                for k in ("expiry_date", "first_date", "last_date"):
                    if hasattr(d.get(k), "isoformat"):
                        d[k] = d[k].isoformat()
                results.append(d)
            return results

    def get_fo_contracts(self, instrument_key: str, expiry_date: str) -> list[dict]:
        """List individual contracts for an underlying + expiry date."""
        with self.db.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT c.expired_instrument_key,
                       c.trading_symbol,
                       c.contract_type,
                       c.strike_price,
                       COUNT(*) as bar_count,
                       MIN(h.timestamp) as first_date,
                       MAX(h.timestamp) as last_date
                FROM historical_data h
                JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                WHERE c.instrument_key = ? AND c.expiry_date = ?
                GROUP BY c.expired_instrument_key, c.trading_symbol,
                         c.contract_type, c.strike_price
                ORDER BY c.contract_type, c.strike_price
            """, [instrument_key, expiry_date]).fetchall()

            columns = [d[0] for d in conn.description] if conn.description else []
            results = []
            for row in rows:
                d = dict(zip(columns, row, strict=False))
                for k in ("first_date", "last_date"):
                    if hasattr(d.get(k), "isoformat"):
                        d[k] = d[k].isoformat()
                results.append(d)
            return results
