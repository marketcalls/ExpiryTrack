#!/usr/bin/env python
"""
Generate Interactive Trade Verification Charts - SuperTrend Credit Spread
==========================================================================
Reads from:
  - trades_ST80_3_6_500pt_5lots.csv  (your actual trade journal)
  - expirytrack.duckdb               (NIFTY spot + option 1-min candles)

For each trading day, creates an HTML chart with:
  - Top:    NIFTY 5-min candlestick + SuperTrend line + entry/exit arrows
  - Middle: SuperTrend direction bar chart (bull/bear)
  - Bottom: Sell-leg option 1-min candles per trade

Usage:
  # All trades
  uv run python generate_charts.py

  # Single day
  uv run python generate_charts.py --date 2026-02-27

  # One month
  uv run python generate_charts.py --month 2025-01

  # Different ST config
  uv run python generate_charts.py --config 80_3.6
"""

import pandas as pd
import numpy as np
import duckdb
import os
import sys
import argparse
from datetime import time, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings("ignore")

# ── PATHS ──────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Update this path to match your machine
EXPIRYTRACK_DB = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"

TIMEFRAME = 5  # 5-min bars

# Default: best config — matches your CSV filename
DEFAULT_PERIOD  = 80
DEFAULT_MULT    = 3.6
DEFAULT_SPREAD  = 500
DEFAULT_LOTS    = 5

# ── CSV MAPPING ────────────────────────────────────────────────────────────────
# Your actual CSV: trades_ST80_3_6_500pt_5lots.csv
# All required columns already present — no renaming needed.


# ══════════════════════════════════════════════════════════════════════════════
# SUPERTREND  (Wilder's RMA — identical to backtest)
# ══════════════════════════════════════════════════════════════════════════════
def compute_supertrend(highs, lows, closes, period, multiplier):
    n = len(closes)
    tr = np.zeros(n)
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i],
                    abs(highs[i] - closes[i - 1]),
                    abs(lows[i] - closes[i - 1]))

    alpha = 1.0 / period
    atr = np.zeros(n)
    if n >= period:
        atr[period - 1] = np.mean(tr[:period])
        for i in range(period, n):
            atr[i] = alpha * tr[i] + (1 - alpha) * atr[i - 1]
    else:
        atr[:] = tr[0]

    hl2         = (highs + lows) / 2.0
    upper_band  = hl2 + multiplier * atr
    lower_band  = hl2 - multiplier * atr
    direction   = np.ones(n, dtype=int)
    final_upper = upper_band.copy()
    final_lower = lower_band.copy()

    for i in range(1, n):
        if upper_band[i] < final_upper[i-1] or closes[i-1] > final_upper[i-1]:
            final_upper[i] = upper_band[i]
        else:
            final_upper[i] = final_upper[i-1]

        if lower_band[i] > final_lower[i-1] or closes[i-1] < final_lower[i-1]:
            final_lower[i] = lower_band[i]
        else:
            final_lower[i] = final_lower[i-1]

        if direction[i-1] == 1:
            direction[i] = 1 if closes[i] >= final_lower[i] else -1
        else:
            direction[i] = -1 if closes[i] <= final_upper[i] else 1

    supertrend = np.where(direction == 1, final_lower, final_upper)
    return direction, supertrend


# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════
def load_spot_data():
    """Load NIFTY 1-min from ExpiryTrack (with warmup for SuperTrend)."""
    print("Loading NIFTY spot data...", flush=True)
    con = duckdb.connect(EXPIRYTRACK_DB, read_only=True)
    df = con.execute("""
        SELECT timestamp, open, high, low, close, volume
        FROM candle_data
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND interval = '1minute'
          AND timestamp >= '2024-08-01'
        ORDER BY timestamp
    """).fetchdf()
    con.close()

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    df = df[~df.index.duplicated(keep="first")]
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    print(f"  {len(df):,} 1-min bars loaded", flush=True)
    return df


def resample_5m(df_1m):
    """Resample 1-min to 5-min bars (per-day to avoid cross-day bars)."""
    parts = []
    for date, g in df_1m.groupby(df_1m.index.date):
        r = g.resample(f"{TIMEFRAME}min").agg(
            {"open": "first", "high": "max", "low": "min", "close": "last"}
        ).dropna()
        parts.append(r)
    return pd.concat(parts).sort_index()


def load_option_candles(contract_key, date, con):
    """Load option 1-min candles for a specific date from ExpiryTrack."""
    date_str  = date.isoformat()
    next_date = (date + timedelta(days=1)).isoformat()
    df = con.execute(f"""
        SELECT timestamp, open, high, low, close, volume
        FROM historical_data
        WHERE TRIM(expired_instrument_key) = '{contract_key}'
          AND timestamp >= '{date_str}'
          AND timestamp < '{next_date}'
        ORDER BY timestamp
    """).fetchdf()
    if df.empty:
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").sort_index()
    df = df[~df.index.duplicated(keep="first")]
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    return df


def load_contracts(con):
    """Load NIFTY option contracts from ExpiryTrack."""
    print("Loading option contracts...", flush=True)
    contracts = con.execute("""
        SELECT expired_instrument_key, expiry_date, contract_type, strike_price
        FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND data_fetched = true AND no_data = false
    """).fetchdf()
    contracts["expiry_date"] = pd.to_datetime(contracts["expiry_date"])
    print(f"  {len(contracts):,} contracts loaded", flush=True)
    return contracts


def find_option_key(contracts, strike, opt_type, expiry):
    """Find expired_instrument_key for a given strike/type/expiry."""
    expiry = pd.Timestamp(expiry)
    match = contracts[
        (contracts["strike_price"].astype(float) == float(strike)) &
        (contracts["contract_type"] == opt_type) &
        (contracts["expiry_date"] == expiry)
    ]
    return None if match.empty else match.iloc[0]["expired_instrument_key"]


# ══════════════════════════════════════════════════════════════════════════════
# CHART GENERATION
# ══════════════════════════════════════════════════════════════════════════════
def generate_day_chart(date, day_trades, nifty_5m, st_direction, st_values,
                       nifty_1m, contracts, opt_con, period, mult):
    """Generate a single day's interactive Plotly chart."""
    day_5m      = nifty_5m[nifty_5m.index.date == date]
    day_st_dir  = st_direction[st_direction.index.date == date]
    day_st_val  = st_values[st_values.index.date == date]

    if day_5m.empty:
        return None

    n_trades = len(day_trades)
    MAX_OPT_TRADES = 5
    show_trades = min(n_trades, MAX_OPT_TRADES)

    # Row layout: Row1=NIFTY+ST, Row2=ST direction, Row3..N=option candles
    n_rows = 2 + show_trades
    row_heights = [0.40, 0.10]
    if show_trades > 0:
        opt_h = 0.50 / show_trades
        row_heights += [opt_h] * show_trades
    else:
        row_heights = [0.7, 0.3]

    subtitles = [
        f"NIFTY 50 ({TIMEFRAME}min) + SuperTrend({period},{mult})",
        "SuperTrend Direction"
    ]
    for i, (_, t) in enumerate(day_trades.head(show_trades).iterrows()):
        opt_type = "PE" if t["type"] == "BULL_PUT" else "CE"
        pnl_str  = f"₹{t['total_pnl']:+,.0f}"
        subtitles.append(
            f"T{i+1} SELL {opt_type} {int(t['sell_strike'])} | {t['type']} | "
            f"{t['exit_reason']} | {pnl_str}"
        )

    v_spacing = min(0.02, 0.9 / max(n_rows - 1, 1))
    fig = make_subplots(
        rows=n_rows, cols=1,
        shared_xaxes=True,
        vertical_spacing=v_spacing,
        row_heights=row_heights,
        subplot_titles=subtitles,
    )

    # ── Row 1: NIFTY candlestick ────────────────────────────────────────────
    fig.add_trace(go.Candlestick(
        x=day_5m.index, open=day_5m["open"], high=day_5m["high"],
        low=day_5m["low"], close=day_5m["close"],
        name="NIFTY",
        increasing_line_color="#26a69a",
        decreasing_line_color="#ef5350",
    ), row=1, col=1)

    # SuperTrend line (green=bull, red=bear)
    if not day_st_val.empty:
        bull_mask = day_st_dir == 1
        bear_mask = day_st_dir == -1

        bull_vals = day_st_val.copy(); bull_vals[bear_mask] = np.nan
        fig.add_trace(go.Scatter(
            x=bull_vals.index, y=bull_vals.values,
            mode="lines", line=dict(color="#26a69a", width=2),
            name="ST Bull", showlegend=True,
        ), row=1, col=1)

        bear_vals = day_st_val.copy(); bear_vals[bull_mask] = np.nan
        fig.add_trace(go.Scatter(
            x=bear_vals.index, y=bear_vals.values,
            mode="lines", line=dict(color="#ef5350", width=2),
            name="ST Bear", showlegend=True,
        ), row=1, col=1)

    # Entry/exit arrows on NIFTY chart
    for i, (_, t) in enumerate(day_trades.iterrows()):
        entry_t  = pd.Timestamp(t["entry_time"])
        exit_t   = pd.Timestamp(t["exit_time"])
        is_bull  = t["type"] == "BULL_PUT"
        color    = "#26a69a" if is_bull else "#ef5350"
        sym      = "triangle-up" if is_bull else "triangle-down"
        label    = "Bull Put" if is_bull else "Bear Call"

        fig.add_trace(go.Scatter(
            x=[entry_t], y=[t["entry_spot"]],
            mode="markers+text",
            marker=dict(symbol=sym, size=16, color=color,
                        line=dict(width=1, color="white")),
            text=[f"T{i+1}"], textposition="top center",
            textfont=dict(size=10, color="white"),
            name=f"T{i+1} {label} Entry", showlegend=False,
            hovertext=(f"Entry: {label}<br>Spot: {t['entry_spot']:.0f}<br>"
                       f"Sell: ₹{t['sell_entry']:.2f}<br>"
                       f"Buy: ₹{t['buy_entry']:.2f}<br>"
                       f"Credit: ₹{t['net_credit']:.2f}<br>"
                       f"Qty: {int(t['qty'])}"),
        ), row=1, col=1)

        # Exit marker (only if exit is on this day)
        if exit_t.date() == date:
            exit_color = "#26a69a" if t["total_pnl"] > 0 else "#ef5350"
            fig.add_trace(go.Scatter(
                x=[exit_t], y=[t["entry_spot"]],
                mode="markers",
                marker=dict(symbol="x", size=14, color=exit_color,
                            line=dict(width=2, color="white")),
                name=f"T{i+1} Exit", showlegend=False,
                hovertext=(f"Exit: {t['exit_reason']}<br>"
                           f"P&L: ₹{t['total_pnl']:+,.0f}<br>"
                           f"Days held: {t['days_held']}"),
            ), row=1, col=1)

    # ── Row 2: SuperTrend direction bar ─────────────────────────────────────
    if not day_st_dir.empty:
        colors = ["#26a69a" if d == 1 else "#ef5350" for d in day_st_dir.values]
        fig.add_trace(go.Bar(
            x=day_st_dir.index, y=day_st_dir.values,
            marker_color=colors, name="ST Direction", showlegend=False,
        ), row=2, col=1)

    # ── Rows 3+: Sell-leg option candles ────────────────────────────────────
    for i, (_, t) in enumerate(day_trades.head(show_trades).iterrows()):
        opt_row  = 3 + i
        opt_type = "PE" if t["type"] == "BULL_PUT" else "CE"
        expiry   = t["expiry"]
        entry_t  = pd.Timestamp(t["entry_time"])
        exit_t   = pd.Timestamp(t["exit_time"])

        sell_key = find_option_key(contracts, t["sell_strike"], opt_type, expiry)
        if sell_key:
            sell_df = load_option_candles(sell_key, date, opt_con)
            if sell_df is not None and not sell_df.empty:
                fig.add_trace(go.Candlestick(
                    x=sell_df.index, open=sell_df["open"], high=sell_df["high"],
                    low=sell_df["low"], close=sell_df["close"],
                    name=f"SELL {opt_type} {int(t['sell_strike'])}",
                    showlegend=False,
                    increasing_line_color="#26a69a",
                    decreasing_line_color="#ef5350",
                ), row=opt_row, col=1)

                # Entry / exit price horizontal lines
                fig.add_hline(y=t["sell_entry"], line_dash="dash",
                              line_color="#2196F3",
                              annotation_text=f"Sell @ {t['sell_entry']:.2f}",
                              row=opt_row, col=1)
                fig.add_hline(y=t["sell_exit"], line_dash="dot",
                              line_color="#FF9800",
                              annotation_text=f"Exit @ {t['sell_exit']:.2f}",
                              row=opt_row, col=1)

                # Entry marker on option chart
                fig.add_trace(go.Scatter(
                    x=[entry_t], y=[t["sell_entry"]],
                    mode="markers",
                    marker=dict(symbol="triangle-down", size=14,
                                color="#ef5350",
                                line=dict(width=1, color="white")),
                    name="Sell Entry", showlegend=False,
                    hovertext=(f"SELL {opt_type} {int(t['sell_strike'])} "
                               f"@ ₹{t['sell_entry']:.2f}<br>"
                               f"BUY {opt_type} {int(t['buy_strike'])} "
                               f"@ ₹{t['buy_entry']:.2f}<br>"
                               f"Net Credit: ₹{t['net_credit']:.2f}"),
                ), row=opt_row, col=1)

                # Exit marker (only if trade closed today)
                if exit_t.date() == date:
                    exit_color = "#26a69a" if t["total_pnl"] > 0 else "#ef5350"
                    fig.add_trace(go.Scatter(
                        x=[exit_t], y=[t["sell_exit"]],
                        mode="markers",
                        marker=dict(symbol="star", size=14,
                                    color=exit_color,
                                    line=dict(width=1, color="white")),
                        name="Sell Exit", showlegend=False,
                        hovertext=(f"Buy-back @ ₹{t['sell_exit']:.2f}<br>"
                                   f"{t['exit_reason']} | "
                                   f"P&L: ₹{t['total_pnl']:+,.0f}"),
                    ), row=opt_row, col=1)

    # ── Layout ──────────────────────────────────────────────────────────────
    day_pnl        = day_trades["total_pnl"].sum()
    wins           = (day_trades["total_pnl"] > 0).sum()
    losses         = (day_trades["total_pnl"] <= 0).sum()
    overnight_cnt  = (day_trades["days_held"] > 0).sum()
    pnl_color      = "green" if day_pnl > 0 else "red"
    ov_tag = f" | {overnight_cnt} overnight" if overnight_cnt > 0 else ""

    fig.update_layout(
        title=dict(
            text=(f"<b>{date.strftime('%A %d %b %Y')}</b> | "
                  f"ST({period},{mult}) | "
                  f"{n_trades} trade{'s' if n_trades!=1 else ''}{ov_tag} | "
                  f"W:{wins} L:{losses} | "
                  f"<span style='color:{pnl_color}'>Net P&L: ₹{day_pnl:,.0f}</span>"),
            font=dict(size=16),
        ),
        height=300 + 200 * n_rows,
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        margin=dict(l=60, r=30, t=80, b=30),
    )

    # Remove rangesliders from all candlestick axes
    for i in range(1, n_rows + 1):
        axis_name = f"xaxis{i}" if i > 1 else "xaxis"
        fig.update_layout(**{axis_name: dict(rangeslider=dict(visible=False))})

    return fig


# ══════════════════════════════════════════════════════════════════════════════
# HTML HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def build_trade_table_html(day_trades):
    rows_html = ""
    for _, t in day_trades.iterrows():
        pnl_class = "profit" if t["total_pnl"] > 0 else "loss"
        opt_type  = "PE" if t["type"] == "BULL_PUT" else "CE"
        hold      = f"{t['days_held']}D" if t["days_held"] > 0 else "Intraday"
        entry_ts  = pd.Timestamp(t["entry_time"])
        exit_ts   = pd.Timestamp(t["exit_time"])
        # Extra fields from your enriched CSV
        qty_str    = str(int(t["qty"])) if "qty" in t else "—"
        margin_str = f"₹{int(t['margin_req']):,}" if "margin_req" in t else "—"
        rows_html += f"""<tr>
            <td>{t.get('trade_no', '')}</td>
            <td>{entry_ts.strftime('%Y-%m-%d %H:%M')}</td>
            <td>{exit_ts.strftime('%Y-%m-%d %H:%M')}</td>
            <td>{t['type']}</td>
            <td>{opt_type} {int(t['sell_strike'])}</td>
            <td>{opt_type} {int(t['buy_strike'])}</td>
            <td>{t['entry_spot']:.0f}</td>
            <td>{t['sell_entry']:.2f}</td>
            <td>{t['buy_entry']:.2f}</td>
            <td>{t['net_credit']:.2f}</td>
            <td>{t['sell_exit']:.2f}</td>
            <td>{t['buy_exit']:.2f}</td>
            <td>{t['exit_spread']:.2f}</td>
            <td>{hold}</td>
            <td>₹{t['gross_pnl']:,.0f}</td>
            <td>₹{t['charges']:,.0f}</td>
            <td class="{pnl_class}">₹{t['total_pnl']:,.0f}</td>
            <td>{t['exit_reason']}</td>
            <td>{qty_str}</td>
            <td>{margin_str}</td>
        </tr>"""
    return rows_html


def generate_index_html(all_days_data, charts_dir, period, mult, total_csv_trades):
    rows = ""
    cum_pnl = 0
    for date, info in sorted(all_days_data.items()):
        cum_pnl += info["pnl"]
        pnl_class = "profit" if info["pnl"] > 0 else "loss"
        cum_class = "profit" if cum_pnl > 0 else "loss"
        fname  = info["filename"]
        on_tag = f" ({info['overnight']}ON)" if info["overnight"] > 0 else ""
        rows += f"""<tr>
            <td><a href="{fname}">{date.strftime('%Y-%m-%d')}</a></td>
            <td>{date.strftime('%A')}</td>
            <td>{info['trades']}{on_tag}</td>
            <td>{info['wins']}</td>
            <td>{info['losses']}</td>
            <td class="{pnl_class}">₹{info['pnl']:,.0f}</td>
            <td class="{cum_class}">₹{cum_pnl:,.0f}</td>
        </tr>"""

    total_trades = sum(d["trades"] for d in all_days_data.values())
    total_pnl    = sum(d["pnl"] for d in all_days_data.values())
    total_wins   = sum(d["wins"] for d in all_days_data.values())

    html = f"""<!DOCTYPE html>
<html><head>
<title>SuperTrend({period},{mult}) Credit Spread — Trade Charts</title>
<style>
    body {{ font-family: 'Segoe UI', sans-serif; background: #1a1a2e; color: #eee; margin: 20px; }}
    h1 {{ color: #00d2ff; }} h2 {{ color: #aaa; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 15px; }}
    th {{ background: #16213e; color: #00d2ff; padding: 10px; text-align: left; position: sticky; top: 0; }}
    td {{ padding: 8px 10px; border-bottom: 1px solid #333; }}
    tr:hover {{ background: #16213e; }}
    a {{ color: #00d2ff; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .profit {{ color: #26a69a; font-weight: bold; }}
    .loss {{ color: #ef5350; font-weight: bold; }}
    .summary {{ background: #16213e; padding: 15px; border-radius: 8px; margin: 15px 0;
                display: flex; gap: 30px; flex-wrap: wrap; }}
    .stat {{ text-align: center; }}
    .stat-value {{ font-size: 24px; font-weight: bold; }}
    .stat-label {{ font-size: 12px; color: #888; }}
</style>
</head><body>
<h1>SuperTrend({period},{mult}) Credit Spread — Trade Verification Charts</h1>
<h2>NIFTY {DEFAULT_SPREAD}pt Spread | {DEFAULT_LOTS} lots × 65 = {DEFAULT_LOTS*65} qty</h2>

<div class="summary">
    <div class="stat">
        <div class="stat-value">{len(all_days_data)}</div>
        <div class="stat-label">Trading Days</div>
    </div>
    <div class="stat">
        <div class="stat-value">{total_trades}</div>
        <div class="stat-label">Trades Shown</div>
    </div>
    <div class="stat">
        <div class="stat-value">{total_csv_trades}</div>
        <div class="stat-label">Total in Journal</div>
    </div>
    <div class="stat">
        <div class="stat-value">{total_wins}</div>
        <div class="stat-label">Winners</div>
    </div>
    <div class="stat">
        <div class="stat-value {'profit' if total_pnl>0 else 'loss'}">
            ₹{total_pnl:,.0f}
        </div>
        <div class="stat-label">Net P&L (filtered)</div>
    </div>
</div>

<p>Click any date to view NIFTY + SuperTrend + Option candle charts with entry/exit markers.</p>
<p><em>Trades may span multiple days (overnight holds). Entry day shows the trade; exit may be on a later date.</em></p>

<table>
<tr>
    <th>Date</th><th>Day</th><th>Trades</th>
    <th>Wins</th><th>Losses</th><th>Day P&L</th><th>Cum P&L</th>
</tr>
{rows}
</table>
</body></html>"""

    with open(os.path.join(charts_dir, "index.html"), "w") as f:
        f.write(html)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="Generate per-day trade charts for SuperTrend Credit Spread"
    )
    parser.add_argument("--date",   help="Single date (YYYY-MM-DD)")
    parser.add_argument("--month",  help="Month (YYYY-MM)")
    parser.add_argument("--config", default="80_3.6",
                        help="ST config period_mult (default: 80_3.6)")
    parser.add_argument("--csv",    default=None,
                        help="Override CSV path (default: auto-detect from config)")
    args = parser.parse_args()

    # Parse config
    parts  = args.config.split("_")
    period = int(parts[0])
    mult   = float(parts[1])

    # Locate CSV — try several naming conventions
    if args.csv:
        csv_path = args.csv
    else:
        candidates = [
            os.path.join(BASE_DIR, f"trades_ST{period}_{mult}_{DEFAULT_SPREAD}pt_{DEFAULT_LOTS}lots.csv"),
            os.path.join(BASE_DIR, f"overnight_trades_ST{period}_{mult}_{DEFAULT_SPREAD}.csv"),
            os.path.join(BASE_DIR, f"trades_ST{period}_{str(mult).replace('.','_')}_{DEFAULT_SPREAD}pt_{DEFAULT_LOTS}lots.csv"),
        ]
        csv_path = next((p for p in candidates if os.path.exists(p)), None)

    if not csv_path or not os.path.exists(csv_path):
        print(f"❌  Trade CSV not found. Tried:")
        for c in candidates:
            print(f"    {c}")
        print("Run the backtest first, or pass --csv <path>")
        sys.exit(1)

    print(f"✅  Using trade journal: {csv_path}", flush=True)

    # Output folder
    charts_dir = os.path.join(BASE_DIR, f"charts_ST{period}_{mult}")
    os.makedirs(charts_dir, exist_ok=True)

    # Load journal
    print(f"Loading trade journal: ST({period},{mult})...", flush=True)
    journal = pd.read_csv(csv_path, parse_dates=["entry_time", "exit_time"])
    journal["expiry"]     = pd.to_datetime(journal["expiry"])
    journal["entry_date"] = journal["entry_time"].dt.date
    journal["exit_date"]  = journal["exit_time"].dt.date
    journal["trade_no"]   = range(1, len(journal) + 1)
    total_csv_trades = len(journal)

    # Filter by date / month if specified
    if args.date:
        target_date = pd.Timestamp(args.date).date()
        journal = journal[
            (journal["entry_date"] == target_date) |
            (journal["exit_date"]  == target_date)
        ]
        if journal.empty:
            print(f"No trades on {args.date}")
            return

    elif args.month:
        journal = journal[
            journal["entry_time"].dt.strftime("%Y-%m") == args.month
        ]
        if journal.empty:
            print(f"No trades in {args.month}")
            return

    # Get unique entry dates
    trade_dates = sorted(journal["entry_date"].unique())
    print(f"  {len(journal)} trades across {len(trade_dates)} entry days", flush=True)

    # Load NIFTY spot
    nifty_1m = load_spot_data()

    # Compute 5-min + SuperTrend on full history (for ST continuity)
    print("Computing SuperTrend on full 5-min series...", flush=True)
    nifty_5m = resample_5m(nifty_1m)

    highs  = nifty_5m["high"].values.astype(float)
    lows   = nifty_5m["low"].values.astype(float)
    closes = nifty_5m["close"].values.astype(float)
    st_dir_arr, st_val_arr = compute_supertrend(highs, lows, closes, period, mult)

    st_direction = pd.Series(st_dir_arr, index=nifty_5m.index)
    st_values    = pd.Series(st_val_arr,  index=nifty_5m.index)
    print(f"  SuperTrend computed on {len(nifty_5m):,} bars", flush=True)

    # Load option contracts
    opt_con   = duckdb.connect(EXPIRYTRACK_DB, read_only=True)
    contracts = load_contracts(opt_con)

    # Generate per-day charts
    print(f"\nGenerating charts for {len(trade_dates)} days...", flush=True)
    all_days_data = {}

    for idx, date in enumerate(trade_dates):
        day_trades = journal[journal["entry_date"] == date].copy()
        if day_trades.empty:
            continue

        day_pnl   = day_trades["total_pnl"].sum()
        wins      = (day_trades["total_pnl"] > 0).sum()
        losses    = (day_trades["total_pnl"] <= 0).sum()
        overnight = (day_trades["days_held"] > 0).sum()
        fname     = f"{date.isoformat()}.html"

        fig = generate_day_chart(
            date, day_trades, nifty_5m, st_direction, st_values,
            nifty_1m, contracts, opt_con, period, mult
        )
        if fig is None:
            print(f"  ⚠️  {date} — no 5-min data, skipping")
            continue

        table_rows = build_trade_table_html(day_trades)
        chart_html = fig.to_html(full_html=False, include_plotlyjs="cdn")
        pnl_color  = "#26a69a" if day_pnl > 0 else "#ef5350"

        # Navigation links
        prev_link = (f"<a href='{trade_dates[idx-1].isoformat()}.html'>← Prev Day</a>"
                     if idx > 0 else "")
        next_link = (f"<a href='{trade_dates[idx+1].isoformat()}.html'>Next Day →</a>"
                     if idx < len(trade_dates) - 1 else "")

        page_html = f"""<!DOCTYPE html>
<html><head>
<title>ST Credit Spread - {date.isoformat()}</title>
<style>
    body {{ font-family: 'Segoe UI', sans-serif; background: #1a1a2e; color: #eee; margin: 0; padding: 15px; }}
    .nav {{ margin-bottom: 10px; }}
    .nav a {{ color: #00d2ff; text-decoration: none; margin-right: 15px; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 10px; font-size: 13px; }}
    th {{ background: #16213e; color: #00d2ff; padding: 8px; text-align: left; position: sticky; top: 0; }}
    td {{ padding: 6px 8px; border-bottom: 1px solid #333; }}
    .profit {{ color: #26a69a; font-weight: bold; }}
    .loss   {{ color: #ef5350; font-weight: bold; }}
    h3 {{ color: #aaa; margin-top: 20px; }}
</style>
</head><body>
<div class="nav">
    <a href="index.html">← All Days</a>
    {prev_link}
    {next_link}
</div>
{chart_html}
<h3>Trade Details — SuperTrend({period},{mult}) Credit Spread</h3>
<table>
<tr>
    <th>#</th><th>Entry Time</th><th>Exit Time</th><th>Spread</th>
    <th>Sell Leg</th><th>Buy Leg</th><th>Spot</th>
    <th>Sell Prem</th><th>Buy Prem</th><th>Net Credit</th>
    <th>Sell Exit</th><th>Buy Exit</th><th>Exit Spread</th>
    <th>Hold</th><th>Gross P&L</th><th>Charges</th><th>Net P&L</th>
    <th>Exit</th><th>Qty</th><th>Margin Req</th>
</tr>
{table_rows}
</table>
</body></html>"""

        with open(os.path.join(charts_dir, fname), "w") as f:
            f.write(page_html)

        all_days_data[date] = {
            "filename": fname, "trades": len(day_trades),
            "wins": wins, "losses": losses,
            "pnl": day_pnl, "overnight": overnight,
        }

        if (idx + 1) % 20 == 0 or (idx + 1) == len(trade_dates):
            print(f"  {idx+1}/{len(trade_dates)} days done...", flush=True)

    generate_index_html(all_days_data, charts_dir, period, mult, total_csv_trades)

    opt_con.close()

    print(f"\n✅  Done! {len(all_days_data)} day charts saved to:")
    print(f"    {charts_dir}/")
    print(f"    Open {charts_dir}/index.html to browse.")


if __name__ == "__main__":
    main()
