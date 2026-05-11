"""High-level backtesting / scanning API on top of MarketDataStore.

These helpers are intentionally thin — they're vectorized DataFrame
accessors, not a full strategy engine.  Build your own loop or strategy
class on top of them; the store does the heavy lifting.

Typical usage
-------------
>>> from src.backtest import bars, chain, scan, iter_bars
>>> df = bars("NIFTY28APR2624000CE", timeframe="5m")
>>> ch = chain("NIFTY", "2026-04-28", at="2026-04-25 14:30")
>>> hot = scan(base_symbol="NIFTY", contract_type="CE", date="2026-04-25").nlargest(20, "volume")
>>> for ts, snap in iter_bars("NIFTY", "2026-04-28", timeframe="5m"):
...     # ts -> Timestamp; snap -> chain DataFrame at that ts
...     ...
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Iterator, List, Optional, Tuple

import pandas as pd

from ..database.market_data import MarketDataStore


# ---------------------------------------------------------------------------
# Shared store accessor.
# ---------------------------------------------------------------------------
def _store() -> MarketDataStore:
    return MarketDataStore.instance()


# ---------------------------------------------------------------------------
# Stateless functional helpers.
# ---------------------------------------------------------------------------
def bars(
    openalgo_symbol: str,
    timeframe: str = "1m",
    from_ts: Optional[str | datetime] = None,
    to_ts: Optional[str | datetime] = None,
) -> pd.DataFrame:
    """OHLCV+OI bars for one contract, indexed by `ts`."""
    return _store().get_bars(
        openalgo_symbol=openalgo_symbol,
        timeframe=timeframe,
        from_ts=from_ts,
        to_ts=to_ts,
    )


def chain(
    base_symbol: str,
    expiry: str,
    at: Optional[str | datetime] = None,
) -> pd.DataFrame:
    """Option chain snapshot for `base_symbol` / `expiry` at (or before) `at`."""
    return _store().get_chain(base_symbol=base_symbol, expiry=expiry, at=at)


def scan(
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
    """General-purpose dimension-filtered scan."""
    return _store().scan(
        base_symbol=base_symbol,
        expiry=expiry,
        contract_type=contract_type,
        strike_lt=strike_lt,
        strike_gt=strike_gt,
        strike_eq=strike_eq,
        date=date,
        from_ts=from_ts,
        to_ts=to_ts,
        timeframe=timeframe,
        limit=limit,
    )


def snapshot(
    base_symbol: str,
    expiry: str,
    ts: str | datetime,
) -> pd.DataFrame:
    """Alias for `chain(..., at=ts)`."""
    return chain(base_symbol=base_symbol, expiry=expiry, at=ts)


# ---------------------------------------------------------------------------
# Iterators for event-driven backtests.
# ---------------------------------------------------------------------------
def iter_bars(
    base_symbol: str,
    expiry: str,
    timeframe: str = "1m",
    from_ts: Optional[str | datetime] = None,
    to_ts: Optional[str | datetime] = None,
) -> Iterator[Tuple[pd.Timestamp, pd.DataFrame]]:
    """
    Iterate the option chain forward in time.

    Yields (timestamp, snapshot_dataframe) tuples — one per timeframe bucket.
    The snapshot dataframe is one row per contract for that timestamp,
    suitable for strategy logic that needs the full chain at each step.
    """
    store = _store()
    table = store._table_for(timeframe)
    clauses = ["base_symbol = ?", "expiry_date = ?"]
    params: List[Any] = [base_symbol.upper(), expiry]
    if from_ts is not None:
        clauses.append("ts >= ?")
        params.append(pd.Timestamp(from_ts).to_pydatetime())
    if to_ts is not None:
        clauses.append("ts <= ?")
        params.append(pd.Timestamp(to_ts).to_pydatetime())

    where = " AND ".join(clauses)
    query = f"""
        SELECT ts, openalgo_symbol, contract_type, strike_price,
               open, high, low, close, volume, oi, expired_instrument_key
        FROM {table}
        WHERE {where}
        ORDER BY ts, contract_type, strike_price
    """
    df = store.sql(query, params)
    if df.empty:
        return
    for ts, group in df.groupby("ts", sort=True):
        yield pd.Timestamp(ts), group.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Stateful wrapper (handy for notebooks).
# ---------------------------------------------------------------------------
@dataclass
class BacktestData:
    """A small stateful facade that pins a (base_symbol, expiry) context.

    >>> bt = BacktestData("NIFTY", "2026-04-28")
    >>> bt.chain(at="2026-04-25 14:30").head()
    >>> bt.bars("NIFTY28APR2624000CE", timeframe="5m")
    """

    base_symbol: str
    expiry: str
    timeframe: str = "1m"

    def chain(self, at: Optional[str | datetime] = None) -> pd.DataFrame:
        return chain(self.base_symbol, self.expiry, at=at)

    def bars(
        self,
        openalgo_symbol: str,
        timeframe: Optional[str] = None,
        from_ts: Optional[str | datetime] = None,
        to_ts: Optional[str | datetime] = None,
    ) -> pd.DataFrame:
        return bars(
            openalgo_symbol,
            timeframe=timeframe or self.timeframe,
            from_ts=from_ts,
            to_ts=to_ts,
        )

    def iter_bars(
        self,
        timeframe: Optional[str] = None,
        from_ts: Optional[str | datetime] = None,
        to_ts: Optional[str | datetime] = None,
    ) -> Iterator[Tuple[pd.Timestamp, pd.DataFrame]]:
        return iter_bars(
            self.base_symbol,
            self.expiry,
            timeframe=timeframe or self.timeframe,
            from_ts=from_ts,
            to_ts=to_ts,
        )

    def scan(self, **filters: Any) -> pd.DataFrame:
        filters.setdefault("base_symbol", self.base_symbol)
        filters.setdefault("expiry", self.expiry)
        filters.setdefault("timeframe", self.timeframe)
        return scan(**filters)
