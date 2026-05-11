"""Backtesting / scanning helpers for ExpiryTrack.

Wraps the DuckDB MarketDataStore with friendly, vectorized helpers that
return pandas DataFrames suitable for strategy research and signal scanning.
"""

from .api import (
    BacktestData,
    bars,
    chain,
    scan,
    iter_bars,
    snapshot,
)

__all__ = [
    "BacktestData",
    "bars",
    "chain",
    "scan",
    "iter_bars",
    "snapshot",
]
