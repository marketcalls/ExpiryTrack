"""Strategy base class for backtesting."""

from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass
class Trade:
    """Record of a completed trade."""

    entry_time: str
    exit_time: str
    side: str  # "long" or "short"
    qty: int
    entry_price: float
    exit_price: float
    pnl: float
    commission: float
    bar_index_entry: int
    bar_index_exit: int


class Strategy:
    """Base class for user strategies.

    Users subclass this and override on_init(), on_candle(), on_finish().
    Order methods: buy(), sell(), short(), cover().
    """

    def __init__(self) -> None:
        # Internal state — managed by engine
        self._position: int = 0  # +ve = long, -ve = short, 0 = flat
        self._avg_price: float = 0.0
        self._cash: float = 0.0
        self._initial_capital: float = 0.0
        self._commission_rate: float = 0.0003  # 0.03%
        self._total_commission: float = 0.0
        self._bar_index: int = 0
        self._current_price: float = 0.0
        self._current_time: str = ""
        self._trades: list[Trade] = []
        self._equity_curve: list[dict] = []
        self._pending_entry: dict | None = None  # track open position entry

    # ── User-overridable hooks ──

    def on_init(self) -> None:
        """Called once before the first bar. Set parameters here."""

    def on_candle(
        self,
        timestamp: str,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        oi: int,
    ) -> None:
        """Called for each bar. Override to implement strategy logic."""

    def on_finish(self) -> None:
        """Called after the last bar. Optional cleanup."""

    # ── Order methods ──

    def buy(self, qty: int = 1) -> None:
        """Go long or add to long position."""
        if qty <= 0:
            return
        price = self._current_price
        cost = price * qty
        commission = cost * self._commission_rate
        self._cash -= cost + commission
        self._total_commission += commission

        if self._position < 0:
            # Covering short
            cover_qty = min(qty, abs(self._position))
            pnl = (self._avg_price - price) * cover_qty - commission
            self._record_trade("short", cover_qty, price, pnl, commission)
            remaining = qty - cover_qty
            self._position += cover_qty
            if remaining > 0:
                self._avg_price = price
                self._position += remaining
                self._pending_entry = {
                    "time": self._current_time,
                    "price": price,
                    "bar_index": self._bar_index,
                }
            elif self._position == 0:
                self._avg_price = 0.0
                self._pending_entry = None
        else:
            # Adding to long
            if self._position == 0:
                self._avg_price = price
                self._pending_entry = {
                    "time": self._current_time,
                    "price": price,
                    "bar_index": self._bar_index,
                }
            else:
                total_cost = self._avg_price * self._position + price * qty
                self._avg_price = total_cost / (self._position + qty)
            self._position += qty

    def sell(self, qty: int = 1) -> None:
        """Close or reduce long position."""
        if qty <= 0 or self._position <= 0:
            return
        qty = min(qty, self._position)
        price = self._current_price
        proceeds = price * qty
        commission = proceeds * self._commission_rate
        self._cash += proceeds - commission
        self._total_commission += commission

        pnl = (price - self._avg_price) * qty - commission
        self._record_trade("long", qty, price, pnl, commission)
        self._position -= qty
        if self._position == 0:
            self._avg_price = 0.0
            self._pending_entry = None

    def short(self, qty: int = 1) -> None:
        """Go short or add to short position."""
        if qty <= 0:
            return
        price = self._current_price
        proceeds = price * qty
        commission = proceeds * self._commission_rate
        self._cash += proceeds - commission
        self._total_commission += commission

        if self._position > 0:
            # Selling long
            sell_qty = min(qty, self._position)
            pnl = (price - self._avg_price) * sell_qty - commission
            self._record_trade("long", sell_qty, price, pnl, commission)
            remaining = qty - sell_qty
            self._position -= sell_qty
            if remaining > 0:
                self._avg_price = price
                self._position -= remaining
                self._pending_entry = {
                    "time": self._current_time,
                    "price": price,
                    "bar_index": self._bar_index,
                }
            elif self._position == 0:
                self._avg_price = 0.0
                self._pending_entry = None
        else:
            # Adding to short
            if self._position == 0:
                self._avg_price = price
                self._pending_entry = {
                    "time": self._current_time,
                    "price": price,
                    "bar_index": self._bar_index,
                }
            else:
                total_cost = self._avg_price * abs(self._position) + price * qty
                self._avg_price = total_cost / (abs(self._position) + qty)
            self._position -= qty

    def cover(self, qty: int = 1) -> None:
        """Close or reduce short position."""
        if qty <= 0 or self._position >= 0:
            return
        qty = min(qty, abs(self._position))
        price = self._current_price
        cost = price * qty
        commission = cost * self._commission_rate
        self._cash -= cost + commission
        self._total_commission += commission

        pnl = (self._avg_price - price) * qty - commission
        self._record_trade("short", qty, price, pnl, commission)
        self._position += qty
        if self._position == 0:
            self._avg_price = 0.0
            self._pending_entry = None

    # ── Properties ──

    @property
    def position(self) -> int:
        return self._position

    @property
    def cash(self) -> float:
        return self._cash

    @property
    def equity(self) -> float:
        """Cash + market value of open position."""
        if self._position > 0:
            return self._cash + self._position * self._current_price
        elif self._position < 0:
            return self._cash + self._position * self._current_price
        return self._cash

    @property
    def avg_price(self) -> float:
        return self._avg_price

    @property
    def bar_index(self) -> int:
        return self._bar_index

    # ── Internal ──

    def _record_trade(
        self, side: str, qty: int, exit_price: float, pnl: float, commission: float
    ) -> None:
        entry = self._pending_entry or {
            "time": self._current_time,
            "price": self._avg_price,
            "bar_index": self._bar_index,
        }
        self._trades.append(
            Trade(
                entry_time=entry["time"],
                exit_time=self._current_time,
                side=side,
                qty=qty,
                entry_price=entry["price"],
                exit_price=exit_price,
                pnl=pnl,
                commission=commission,
                bar_index_entry=entry["bar_index"],
                bar_index_exit=self._bar_index,
            )
        )

    def _snapshot_equity(self) -> None:
        self._equity_curve.append(
            {
                "bar": self._bar_index,
                "time": self._current_time,
                "equity": round(self.equity, 2),
            }
        )
