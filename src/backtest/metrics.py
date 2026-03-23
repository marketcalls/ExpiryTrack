"""Compute performance metrics from backtest trades and equity curve."""

from __future__ import annotations

import math

from .strategy import Trade


def compute_metrics(
    trades: list[Trade],
    equity_curve: list[dict],
    initial_capital: float,
    total_commission: float,
) -> dict:
    """Compute comprehensive backtest performance metrics."""
    final_equity = equity_curve[-1]["equity"] if equity_curve else initial_capital
    total_pnl = final_equity - initial_capital
    total_return_pct = (total_pnl / initial_capital * 100) if initial_capital else 0.0

    total_trades = len(trades)
    winning = [t for t in trades if t.pnl > 0]
    losing = [t for t in trades if t.pnl <= 0]

    win_rate = (len(winning) / total_trades * 100) if total_trades else 0.0
    avg_win = (sum(t.pnl for t in winning) / len(winning)) if winning else 0.0
    avg_loss = (sum(t.pnl for t in losing) / len(losing)) if losing else 0.0

    gross_profit = sum(t.pnl for t in winning)
    gross_loss = abs(sum(t.pnl for t in losing))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf") if gross_profit > 0 else 0.0

    # Max drawdown
    max_dd, max_dd_pct = _compute_drawdown(equity_curve)

    # Sharpe ratio (annualized, assuming daily bars)
    sharpe = _compute_sharpe(equity_curve, initial_capital)

    return {
        "total_return_pct": round(total_return_pct, 2),
        "total_pnl": round(total_pnl, 2),
        "final_equity": round(final_equity, 2),
        "total_trades": total_trades,
        "win_rate": round(win_rate, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "profit_factor": round(profit_factor, 2) if profit_factor != float("inf") else "Inf",
        "max_drawdown": round(max_dd, 2),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "sharpe_ratio": round(sharpe, 2),
        "total_commission": round(total_commission, 2),
    }


def _compute_drawdown(equity_curve: list[dict]) -> tuple[float, float]:
    """Compute maximum drawdown in absolute and percentage terms."""
    if not equity_curve:
        return 0.0, 0.0

    peak = equity_curve[0]["equity"]
    max_dd = 0.0
    max_dd_pct = 0.0

    for point in equity_curve:
        eq = point["equity"]
        if eq > peak:
            peak = eq
        dd = peak - eq
        dd_pct = (dd / peak * 100) if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
        if dd_pct > max_dd_pct:
            max_dd_pct = dd_pct

    return max_dd, max_dd_pct


def _compute_sharpe(
    equity_curve: list[dict], initial_capital: float, risk_free_rate: float = 0.05
) -> float:
    """Annualized Sharpe ratio from equity curve returns."""
    if len(equity_curve) < 2:
        return 0.0

    returns = []
    for i in range(1, len(equity_curve)):
        prev_eq = equity_curve[i - 1]["equity"]
        curr_eq = equity_curve[i]["equity"]
        if prev_eq > 0:
            returns.append((curr_eq - prev_eq) / prev_eq)

    if not returns:
        return 0.0

    n = len(returns)
    mean_ret = sum(returns) / n
    daily_rf = risk_free_rate / 252

    variance = sum((r - mean_ret) ** 2 for r in returns) / n
    std_ret = math.sqrt(variance) if variance > 0 else 0.0

    if std_ret == 0:
        return 0.0

    sharpe = (mean_ret - daily_rf) / std_ret * math.sqrt(252)
    return sharpe
