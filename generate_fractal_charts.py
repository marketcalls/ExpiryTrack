#!/usr/bin/env python
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "plotly",
#   "pandas",
#   "numpy",
#   "duckdb",
# ]
# ///
"""
Generate Interactive Trade Verification Charts — William Fractals Credit Spread
================================================================================
For each trading day, creates an interactive HTML chart with:
  - Row 1 : NIFTY 5-min candlestick + fractal markers (▲ up / ▼ down)
            + entry / exit trade arrows
  - Row 2 : Fractal signal bar (green = bullish / red = bearish active signal)
  - Row 3+: Sell-leg option 1-min candles per trade (entry & exit price lines)

Fractal logic matches backtest exactly:
  - n=10 each side → 21-candle window  (matches TradingView "Fractals (21)")
  - Marker placed at CONFIRMATION bar (centre + n), not at centre bar
  - SESSION BOUNDARY safe: left window never crosses a calendar date boundary
  - DEDUP: one signal per unique fractal extreme (same as TV)

Usage:
  uv run python generate_fractal_charts.py
  uv run python generate_fractal_charts.py --date 2026-03-13
  uv run python generate_fractal_charts.py --month 2026-03
  uv run python generate_fractal_charts.py --date 2026-03-13 --fractal-n 10
"""

import os
import sys
import argparse
import warnings
from datetime import time, timedelta

import numpy as np
import pandas as pd
import duckdb
import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings("ignore")

# ── Paths & config ─────────────────────────────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
DB_PATH        = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"
TIMEFRAME      = 5          # 5-min bars for NIFTY chart
DEFAULT_N      = 10         # n=10 → 21-candle fractal (matches TV "Fractals (21)")
DEFAULT_SPREAD = 500
TRADES_CSV     = "trades_WF10_500pt_3lots.csv"   # default filename, override with --trades-csv


# ══════════════════════════════════════════════════════════════════════════════
# WILLIAM FRACTALS  (session-boundary safe, deduplicated)
# ══════════════════════════════════════════════════════════════════════════════

def compute_fractals(highs: np.ndarray, lows: np.ndarray,
                     dates: np.ndarray, n: int):
    """
    Compute William Fractals.

    Rules that match TradingView exactly:
      1. SESSION BOUNDARY — left-window bars must belong to the same calendar
         date as the centre bar.  Prevents phantom fractals at session open
         (09:15-09:55) where left bars bleed into the previous trading day.
      2. STRICT inequality on both sides (> / <).
      3. CONFIRMATION bar = centre + n  (marker placed here, not at centre).
      4. DEDUP — returns centre index alongside confirm index so caller can
         suppress duplicate signals from the same fractal extreme.

    Returns
    -------
    up_fractal, down_fractal : np.ndarray[bool]  — True at confirmation bar
    up_centre,  dn_centre    : np.ndarray[int]   — centre bar index (-1 = none)
    """
    size     = len(highs)
    up_frac  = np.zeros(size, dtype=bool)
    dn_frac  = np.zeros(size, dtype=bool)
    up_ctr   = np.full(size, -1, dtype=int)
    dn_ctr   = np.full(size, -1, dtype=int)

    for c in range(n, size - n):
        # Session boundary: leftmost bar must be same date as centre
        if dates[c - n] != dates[c]:
            continue

        confirm = c + n

        is_up = (highs[c] > np.max(highs[c - n: c]) and
                 highs[c] > np.max(highs[c + 1: c + n + 1]))
        is_dn = (lows[c]  < np.min(lows[c  - n: c]) and
                 lows[c]  < np.min(lows[c  + 1: c + n + 1]))

        if is_up:
            up_frac[confirm] = True
            up_ctr[confirm]  = c
        if is_dn:
            dn_frac[confirm] = True
            dn_ctr[confirm]  = c

    return up_frac, dn_frac, up_ctr, dn_ctr


# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

def load_spot_data(con) -> pd.DataFrame:
    print("Loading NIFTY 1-min data...", flush=True)
    df = con.execute("""
        SELECT timestamp, open, high, low, close, volume
        FROM candle_data
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND interval = '1minute'
          AND timestamp >= '2024-08-01'
        ORDER BY timestamp
    """).fetchdf()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    df = df[~df.index.duplicated(keep="first")]
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    print(f"  {len(df):,} 1-min bars loaded", flush=True)
    return df


def resample_5m(df_1m: pd.DataFrame) -> pd.DataFrame:
    """Resample 1-min to 5-min, grouped by date to avoid cross-day bars."""
    parts = []
    for _date, grp in df_1m.groupby(df_1m.index.date):
        r = grp.resample(f"{TIMEFRAME}min").agg(
            {"open": "first", "high": "max", "low": "min", "close": "last"}
        ).dropna()
        parts.append(r)
    return pd.concat(parts).sort_index()


def load_option_candles(contract_key: str, date, con) -> pd.DataFrame | None:
    date_str = date.isoformat()
    next_d   = (date + timedelta(days=1)).isoformat()
    df = con.execute(f"""
        SELECT timestamp, open, high, low, close
        FROM historical_data
        WHERE TRIM(expired_instrument_key) = '{contract_key}'
          AND timestamp >= '{date_str}' AND timestamp < '{next_d}'
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


def load_contracts(con) -> pd.DataFrame:
    print("Loading option contracts...", flush=True)
    df = con.execute("""
        SELECT expired_instrument_key, expiry_date, contract_type, strike_price
        FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND data_fetched = true AND no_data = false
    """).fetchdf()
    df["expiry_date"]  = pd.to_datetime(df["expiry_date"])
    df["strike_price"] = df["strike_price"].astype(float)
    print(f"  {len(df):,} contracts loaded", flush=True)
    return df


def find_option_key(contracts: pd.DataFrame, strike, opt_type: str, expiry) -> str | None:
    expiry = pd.Timestamp(expiry)
    m = contracts[
        (contracts["strike_price"] == float(strike)) &
        (contracts["contract_type"] == opt_type) &
        (contracts["expiry_date"]   == expiry)
    ]
    return None if m.empty else m.iloc[0]["expired_instrument_key"]


# ══════════════════════════════════════════════════════════════════════════════
# CHART GENERATION — single day
# ══════════════════════════════════════════════════════════════════════════════

def generate_day_chart(date, day_trades, nifty_5m,
                       up_frac_s, dn_frac_s,          # pd.Series bool aligned to nifty_5m
                       up_ctr_s,  dn_ctr_s,            # pd.Series int  aligned to nifty_5m
                       nifty_1m, contracts, opt_con, n):
    """Build a Plotly figure for one trading day."""

    day_5m   = nifty_5m[nifty_5m.index.date == date]
    day_upf  = up_frac_s[up_frac_s.index.date == date]
    day_dnf  = dn_frac_s[dn_frac_s.index.date == date]
    day_upc  = up_ctr_s[up_ctr_s.index.date == date]
    day_dnc  = dn_ctr_s[dn_ctr_s.index.date == date]

    if day_5m.empty:
        return None

    n_trades    = len(day_trades)
    MAX_TRADES  = 5
    show_trades = min(n_trades, MAX_TRADES)

    # Row layout
    n_rows      = 2 + show_trades
    row_heights = [0.45, 0.10] + ([0.45 / max(show_trades, 1)] * show_trades if show_trades else [0.45])
    if show_trades == 0:
        n_rows      = 2
        row_heights = [0.75, 0.25]

    subtitles = [
        f"NIFTY 50 ({TIMEFRAME}min) + William Fractals (n={n}, {2*n+1}-candle)",
        "Active fractal signal direction"
    ]
    for i, (_, t) in enumerate(day_trades.head(show_trades).iterrows()):
        ot  = "PE" if t["type"] == "BULL_PUT" else "CE"
        pnl = f"₹{t['total_pnl']:+,.0f}"
        subtitles.append(
            f"T{i+1}  SELL {ot} {int(t['sell_strike'])}  |  {t['type']}  |  "
            f"{t['exit_reason']}  |  {pnl}"
        )

    v_spacing = min(0.02, 0.9 / max(n_rows - 1, 1))
    fig = make_subplots(
        rows=n_rows, cols=1,
        shared_xaxes=True,
        vertical_spacing=v_spacing,
        row_heights=row_heights,
        subplot_titles=subtitles,
    )

    # ── Row 1: NIFTY candlestick ──────────────────────────────────────────────
    fig.add_trace(go.Candlestick(
        x=day_5m.index,
        open=day_5m["open"], high=day_5m["high"],
        low=day_5m["low"],   close=day_5m["close"],
        name="NIFTY 5m",
        increasing_line_color="#26a69a",
        decreasing_line_color="#ef5350",
    ), row=1, col=1)

    # ── Fractal markers on NIFTY chart ───────────────────────────────────────
    # Dedup: track which centre bars have already been marked this day
    seen_up_centres = set()
    seen_dn_centres = set()

    up_times, up_prices, up_hover = [], [], []
    dn_times, dn_prices, dn_hover = [], [], []

    for ts in day_5m.index:
        if ts not in up_frac_s.index:
            continue
        # UP fractal (bearish → BEAR_CALL)
        if up_frac_s[ts]:
            centre_idx = int(up_ctr_s[ts])
            if centre_idx not in seen_up_centres:
                seen_up_centres.add(centre_idx)
                price = day_5m.loc[ts, "high"] * 1.0008   # slightly above bar high
                up_times.append(ts)
                up_prices.append(price)
                # Recover centre bar time from global index
                ctr_time = nifty_5m.index[centre_idx] if 0 <= centre_idx < len(nifty_5m) else ts
                up_hover.append(
                    f"UP FRACTAL (bearish)<br>"
                    f"Centre bar: {ctr_time.strftime('%H:%M')}<br>"
                    f"Confirm bar: {ts.strftime('%H:%M')}<br>"
                    f"Signal: BEAR_CALL → sell CE"
                )

        # DOWN fractal (bullish → BULL_PUT)
        if dn_frac_s[ts]:
            centre_idx = int(dn_ctr_s[ts])
            if centre_idx not in seen_dn_centres:
                seen_dn_centres.add(centre_idx)
                price = day_5m.loc[ts, "low"] * 0.9992    # slightly below bar low
                dn_times.append(ts)
                dn_prices.append(price)
                ctr_time = nifty_5m.index[centre_idx] if 0 <= centre_idx < len(nifty_5m) else ts
                dn_hover.append(
                    f"DOWN FRACTAL (bullish)<br>"
                    f"Centre bar: {ctr_time.strftime('%H:%M')}<br>"
                    f"Confirm bar: {ts.strftime('%H:%M')}<br>"
                    f"Signal: BULL_PUT → sell PE"
                )

    # UP fractal markers — red triangle ABOVE bar (bearish, sell CE)
    if up_times:
        fig.add_trace(go.Scatter(
            x=up_times, y=up_prices,
            mode="markers",
            marker=dict(symbol="triangle-down", size=14,
                        color="#ef5350", line=dict(width=1, color="#ff8a8a")),
            name="Up Fractal (sell CE)",
            hovertext=up_hover,
            hoverinfo="text",
        ), row=1, col=1)

    # DOWN fractal markers — green triangle BELOW bar (bullish, sell PE)
    if dn_times:
        fig.add_trace(go.Scatter(
            x=dn_times, y=dn_prices,
            mode="markers",
            marker=dict(symbol="triangle-up", size=14,
                        color="#26a69a", line=dict(width=1, color="#5fffb0")),
            name="Down Fractal (sell PE)",
            hovertext=dn_hover,
            hoverinfo="text",
        ), row=1, col=1)

    # ── Trade entry / exit markers on NIFTY chart ─────────────────────────────
    for i, (_, t) in enumerate(day_trades.iterrows()):
        entry_t  = pd.Timestamp(t["entry_time"])
        exit_t   = pd.Timestamp(t["exit_time"])
        is_bull  = t["type"] == "BULL_PUT"
        col      = "#26a69a" if is_bull else "#ef5350"
        sym      = "circle"
        label    = "Bull Put" if is_bull else "Bear Call"

        fig.add_trace(go.Scatter(
            x=[entry_t], y=[t["entry_spot"]],
            mode="markers+text",
            marker=dict(symbol=sym, size=14, color=col,
                        line=dict(width=2, color="white")),
            text=[f"T{i+1}"],
            textposition="top center",
            textfont=dict(size=9, color="white"),
            name=f"T{i+1} {label}",
            hovertext=(f"ENTRY T{i+1} {label}<br>"
                       f"Spot: {t['entry_spot']:.0f}<br>"
                       f"Sell: ₹{t['sell_entry']:.2f} | Buy: ₹{t['buy_entry']:.2f}<br>"
                       f"Net Credit: ₹{t['net_credit']:.2f}"),
            hoverinfo="text",
            showlegend=False,
        ), row=1, col=1)

        if exit_t.date() == date:
            ex_col = "#26a69a" if t["total_pnl"] > 0 else "#ef5350"
            fig.add_trace(go.Scatter(
                x=[exit_t], y=[t["entry_spot"]],
                mode="markers",
                marker=dict(symbol="x", size=14, color=ex_col,
                            line=dict(width=2, color="white")),
                name=f"T{i+1} Exit",
                hovertext=(f"EXIT T{i+1} — {t['exit_reason']}<br>"
                           f"P&L: ₹{t['total_pnl']:+,.0f}  |  Days held: {t['days_held']}"),
                hoverinfo="text",
                showlegend=False,
            ), row=1, col=1)

    # ── Row 2: Active fractal direction bar ───────────────────────────────────
    # Track rolling last-signal direction across the day for the direction bar
    last_dir = 0  # 0=none, 1=bullish (dn fractal), -1=bearish (up fractal)
    dir_vals, dir_cols = [], []
    last_up_seen = -1
    last_dn_seen = -1

    for ts in day_5m.index:
        if ts in up_frac_s.index and up_frac_s[ts]:
            c = int(up_ctr_s[ts])
            if c != last_up_seen:
                last_up_seen = c
                last_dir = -1
        if ts in dn_frac_s.index and dn_frac_s[ts]:
            c = int(dn_ctr_s[ts])
            if c != last_dn_seen:
                last_dn_seen = c
                last_dir = 1
        dir_vals.append(last_dir if last_dir != 0 else np.nan)
        dir_cols.append("#26a69a" if last_dir == 1 else "#ef5350" if last_dir == -1 else "#555")

    fig.add_trace(go.Bar(
        x=day_5m.index, y=[1 if v == 1 else -1 if v == -1 else 0 for v in dir_vals],
        marker_color=dir_cols,
        name="Fractal Direction",
        showlegend=False,
        hoverinfo="skip",
    ), row=2, col=1)

    # ── Rows 3+: Sell-leg option 1-min candles ────────────────────────────────
    for i, (_, t) in enumerate(day_trades.head(show_trades).iterrows()):
        opt_row = 3 + i
        ot      = "PE" if t["type"] == "BULL_PUT" else "CE"
        expiry  = t["expiry"]
        entry_t = pd.Timestamp(t["entry_time"])
        exit_t  = pd.Timestamp(t["exit_time"])

        sell_key = find_option_key(contracts, t["sell_strike"], ot, expiry)
        if sell_key:
            sell_df = load_option_candles(sell_key, date, opt_con)
            if sell_df is not None and not sell_df.empty:
                fig.add_trace(go.Candlestick(
                    x=sell_df.index,
                    open=sell_df["open"], high=sell_df["high"],
                    low=sell_df["low"],   close=sell_df["close"],
                    name=f"SELL {ot} {int(t['sell_strike'])}",
                    showlegend=False,
                    increasing_line_color="#26a69a",
                    decreasing_line_color="#ef5350",
                ), row=opt_row, col=1)

                fig.add_hline(y=t["sell_entry"], line_dash="dash",
                              line_color="#2196F3", line_width=1,
                              annotation_text=f"Entry ₹{t['sell_entry']:.2f}",
                              annotation_font_size=10,
                              row=opt_row, col=1)
                fig.add_hline(y=t["sell_exit"], line_dash="dot",
                              line_color="#FF9800", line_width=1,
                              annotation_text=f"Exit ₹{t['sell_exit']:.2f}",
                              annotation_font_size=10,
                              row=opt_row, col=1)

                # Entry dot on option chart
                fig.add_trace(go.Scatter(
                    x=[entry_t], y=[t["sell_entry"]],
                    mode="markers",
                    marker=dict(symbol="triangle-down", size=13,
                                color="#ef5350", line=dict(width=1, color="white")),
                    showlegend=False,
                    hovertext=f"SELL {ot} {int(t['sell_strike'])} @ ₹{t['sell_entry']:.2f}<br>Credit: ₹{t['net_credit']:.2f}",
                    hoverinfo="text",
                ), row=opt_row, col=1)

                if exit_t.date() == date:
                    ex_col = "#26a69a" if t["total_pnl"] > 0 else "#ef5350"
                    fig.add_trace(go.Scatter(
                        x=[exit_t], y=[t["sell_exit"]],
                        mode="markers",
                        marker=dict(symbol="star", size=13,
                                    color=ex_col, line=dict(width=1, color="white")),
                        showlegend=False,
                        hovertext=f"Buy back @ ₹{t['sell_exit']:.2f}<br>{t['exit_reason']} | P&L: ₹{t['total_pnl']:+,.0f}",
                        hoverinfo="text",
                    ), row=opt_row, col=1)

    # ── Layout ────────────────────────────────────────────────────────────────
    day_pnl  = day_trades["total_pnl"].sum()
    wins     = (day_trades["total_pnl"] > 0).sum()
    losses   = (day_trades["total_pnl"] <= 0).sum()
    on_count = (day_trades["days_held"] > 0).sum()
    on_tag   = f" | {on_count} overnight" if on_count else ""
    p_col    = "green" if day_pnl >= 0 else "red"

    # Count fractal signals for the day
    n_up = len(up_times)
    n_dn = len(dn_times)

    fig.update_layout(
        title=dict(
            text=(f"<b>{date.strftime('%A %d %b %Y')}</b>  |  "
                  f"William Fractals n={n} ({2*n+1}-candle)  |  "
                  f"↓{n_up} Bear signals  ↑{n_dn} Bull signals  |  "
                  f"{n_trades} trades{on_tag}  W:{wins} L:{losses}  |  "
                  f"<span style='color:{p_col}'>Net P&L: ₹{day_pnl:,.0f}</span>"),
            font=dict(size=15),
        ),
        height=320 + 200 * n_rows,
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        margin=dict(l=60, r=40, t=90, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    # Remove rangesliders from all subplots
    for k in range(1, n_rows + 1):
        ax = f"xaxis{k}" if k > 1 else "xaxis"
        fig.update_layout(**{ax: dict(rangeslider=dict(visible=False))})

    return fig


# ══════════════════════════════════════════════════════════════════════════════
# TRADE TABLE HTML
# ══════════════════════════════════════════════════════════════════════════════

def build_trade_table_html(day_trades: pd.DataFrame) -> str:
    rows = ""
    for _, t in day_trades.iterrows():
        cls  = "profit" if t["total_pnl"] > 0 else "loss"
        ot   = "PE" if t["type"] == "BULL_PUT" else "CE"
        hold = f"{t['days_held']}D" if t["days_held"] > 0 else "Intraday"
        et   = pd.Timestamp(t["entry_time"]).strftime("%Y-%m-%d %H:%M")
        xt   = pd.Timestamp(t["exit_time"]).strftime("%Y-%m-%d %H:%M")
        rows += f"""<tr>
            <td>{t.get('trade_no','')}</td>
            <td>{et}</td><td>{xt}</td><td>{t['type']}</td>
            <td>{ot} {int(t['sell_strike'])}</td><td>{ot} {int(t['buy_strike'])}</td>
            <td>{t['entry_spot']:.0f}</td>
            <td>{t['sell_entry']:.2f}</td><td>{t['buy_entry']:.2f}</td>
            <td>{t['net_credit']:.2f}</td>
            <td>{t['sell_exit']:.2f}</td><td>{t['buy_exit']:.2f}</td>
            <td>{t['exit_spread']:.2f}</td><td>{hold}</td>
            <td>₹{t['gross_pnl']:,.0f}</td><td>₹{t['charges']:,.0f}</td>
            <td class="{cls}">₹{t['total_pnl']:,.0f}</td>
            <td>{t['exit_reason']}</td>
        </tr>"""
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# INDEX HTML
# ══════════════════════════════════════════════════════════════════════════════

def generate_index_html(all_days: dict, charts_dir: str, n: int):
    rows    = ""
    cum_pnl = 0
    for date, info in sorted(all_days.items()):
        cum_pnl += info["pnl"]
        pc  = "profit" if info["pnl"] >= 0 else "loss"
        cc  = "profit" if cum_pnl >= 0 else "loss"
        on  = f" ({info['overnight']}ON)" if info["overnight"] > 0 else ""
        rows += f"""<tr>
            <td><a href="{info['filename']}">{date.strftime('%Y-%m-%d')}</a></td>
            <td>{date.strftime('%A')}</td>
            <td>{info['trades']}{on}</td>
            <td>{info['wins']}</td><td>{info['losses']}</td>
            <td class="{pc}">₹{info['pnl']:,.0f}</td>
            <td class="{cc}">₹{cum_pnl:,.0f}</td>
        </tr>"""

    total_trades = sum(d["trades"] for d in all_days.values())
    total_pnl    = sum(d["pnl"]    for d in all_days.values())
    total_wins   = sum(d["wins"]   for d in all_days.values())

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<title>William Fractals (n={n}) — Trade Charts</title>
<style>
  body{{font-family:'Segoe UI',sans-serif;background:#1a1a2e;color:#eee;margin:20px}}
  h1{{color:#f0b90b}} h2{{color:#aaa}}
  table{{border-collapse:collapse;width:100%;margin-top:15px}}
  th{{background:#16213e;color:#00d2ff;padding:10px;text-align:left;position:sticky;top:0}}
  td{{padding:8px 10px;border-bottom:1px solid #333}}
  tr:hover{{background:#16213e}}
  a{{color:#00d2ff;text-decoration:none}} a:hover{{text-decoration:underline}}
  .profit{{color:#26a69a;font-weight:bold}} .loss{{color:#ef5350;font-weight:bold}}
  .summary{{background:#16213e;padding:15px;border-radius:8px;margin:15px 0;display:flex;gap:30px;flex-wrap:wrap}}
  .stat{{text-align:center}} .sv{{font-size:24px;font-weight:bold}} .sl{{font-size:12px;color:#888}}
  .legend{{background:#16213e;padding:12px 18px;border-radius:6px;margin:12px 0;font-size:13px;display:flex;gap:28px}}
  .lrow{{display:flex;align-items:center;gap:8px}}
  .dot{{width:12px;height:12px;border-radius:50%;display:inline-block}}
</style>
</head><body>
<h1>William Fractals (n={n}, {2*n+1}-candle) — NIFTY Credit Spread</h1>
<h2>500pt Spread | 3 lots × 65 | Session-boundary safe | Matches TradingView Fractals(21)</h2>

<div class="legend">
  <div class="lrow"><div class="dot" style="background:#ef5350"></div>
    <span>Up Fractal ▼ above bar → BEAR_CALL (sell CE hedge)</span></div>
  <div class="lrow"><div class="dot" style="background:#26a69a"></div>
    <span>Down Fractal ▲ below bar → BULL_PUT (sell PE hedge)</span></div>
  <div class="lrow"><span style="color:#aaa;font-size:12px">
    Marker placed at <b>confirmation bar</b> (centre + {n} bars). Centre bar time shown in tooltip.</span></div>
</div>

<div class="summary">
  <div class="stat"><div class="sv">{len(all_days)}</div><div class="sl">Trading Days</div></div>
  <div class="stat"><div class="sv">{total_trades}</div><div class="sl">Total Trades</div></div>
  <div class="stat"><div class="sv">{total_wins}</div><div class="sl">Winners</div></div>
  <div class="stat"><div class="sv {'profit' if total_pnl>=0 else 'loss'}">₹{total_pnl:,.0f}</div><div class="sl">Net P&L</div></div>
</div>

<p>Click any date to view NIFTY + Fractal signals + Option candle charts.</p>
<table>
<tr><th>Date</th><th>Day</th><th>Trades</th><th>Wins</th><th>Losses</th><th>Day P&L</th><th>Cum P&L</th></tr>
{rows}
</table>
</body></html>"""

    with open(os.path.join(charts_dir, "index.html"), "w") as f:
        f.write(html)
    print(f"  Index: {os.path.join(charts_dir, 'index.html')}", flush=True)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Generate fractal signal charts for WF Credit Spread backtest"
    )
    parser.add_argument("--date",      help="Single date YYYY-MM-DD")
    parser.add_argument("--month",     help="Full month  YYYY-MM")
    parser.add_argument("--fractal-n", type=int, default=DEFAULT_N,
                        help=f"Fractal n (bars each side, default {DEFAULT_N})")
    parser.add_argument("--trades-csv", default=TRADES_CSV,
                        help="Path to backtest trades CSV")
    args = parser.parse_args()

    n        = args.fractal_n
    csv_arg  = args.trades_csv

    # Search order: (1) as-is, (2) BASE_DIR, (3) common backtest output dirs
    candidate_dirs = [
        "",                          # absolute or relative to cwd
        BASE_DIR,                    # same folder as this script
        os.path.join(BASE_DIR, "backtesting"),
        os.path.join(BASE_DIR, "backtest"),
        os.path.expanduser("~/Desktop/Project/ExpiryTrack"),
    ]
    # Also search for any matching folder that starts with backtest_WF
    for d in os.listdir(BASE_DIR):
        full = os.path.join(BASE_DIR, d)
        if os.path.isdir(full) and d.startswith("backtest_WF"):
            candidate_dirs.append(full)

    csv_path = None
    for d in candidate_dirs:
        p = csv_arg if d == "" else os.path.join(d, csv_arg)
        if os.path.exists(p):
            csv_path = p
            print(f"Found trades CSV: {p}", flush=True)
            break

    if csv_path is None:
        print(f"\nCould not find trades CSV: {csv_arg}")
        print("Searched in:")
        for d in candidate_dirs:
            p = csv_arg if d == "" else os.path.join(d, csv_arg)
            print(f"  {p}")
        print("\nPass the full path explicitly:")
        print(f"  uv run python generate_fractal_charts.py --date 2026-03-13 --trades-csv /full/path/to/trades_WF10_500pt_3lots.csv")
        sys.exit(1)

    charts_dir = os.path.join(BASE_DIR, f"charts_WF{n}")
    os.makedirs(charts_dir, exist_ok=True)

    # ── Load trades ───────────────────────────────────────────────────────────
    print(f"Loading trades: {csv_path}", flush=True)
    journal = pd.read_csv(csv_path, parse_dates=["entry_time", "exit_time"])
    journal["expiry"]     = pd.to_datetime(journal["expiry"])
    journal["entry_date"] = journal["entry_time"].dt.date
    journal["exit_date"]  = journal["exit_time"].dt.date
    journal["trade_no"]   = range(1, len(journal) + 1)

    # Filter by date / month
    if args.date:
        td = pd.Timestamp(args.date).date()
        journal = journal[(journal["entry_date"] == td) | (journal["exit_date"] == td)]
        if journal.empty:
            print(f"No trades on {args.date}"); return
    elif args.month:
        journal = journal[journal["entry_time"].dt.strftime("%Y-%m") == args.month]
        if journal.empty:
            print(f"No trades in {args.month}"); return

    trade_dates = sorted(set(journal["entry_date"]))
    print(f"  {len(journal)} trades across {len(trade_dates)} entry days", flush=True)

    # ── Load NIFTY data ───────────────────────────────────────────────────────
    con = duckdb.connect(DB_PATH, read_only=True)
    nifty_1m = load_spot_data(con)

    print("Resampling to 5-min...", flush=True)
    nifty_5m = resample_5m(nifty_1m)
    print(f"  {len(nifty_5m):,} 5-min bars", flush=True)

    # ── Compute fractals on full series (continuity across sessions) ──────────
    print(f"Computing William Fractals (n={n}, {2*n+1}-candle)...", flush=True)
    highs     = nifty_5m["high"].values.astype(float)
    lows      = nifty_5m["low"].values.astype(float)
    bar_dates = np.array(nifty_5m.index.date)

    up_frac, dn_frac, up_ctr, dn_ctr = compute_fractals(highs, lows, bar_dates, n)

    up_frac_s = pd.Series(up_frac, index=nifty_5m.index)
    dn_frac_s = pd.Series(dn_frac, index=nifty_5m.index)
    up_ctr_s  = pd.Series(up_ctr,  index=nifty_5m.index)
    dn_ctr_s  = pd.Series(dn_ctr,  index=nifty_5m.index)

    total_up = int(up_frac_s.sum())
    total_dn = int(dn_frac_s.sum())
    print(f"  {total_up} up fractals, {total_dn} down fractals across full history", flush=True)

    # ── Load contracts for option charts ─────────────────────────────────────
    opt_con   = duckdb.connect(DB_PATH, read_only=True)
    contracts = load_contracts(opt_con)

    # ── Generate one HTML per entry day ──────────────────────────────────────
    print(f"\nGenerating charts for {len(trade_dates)} days...", flush=True)
    all_days = {}

    for idx, date in enumerate(trade_dates):
        day_trades = journal[journal["entry_date"] == date].copy()
        if day_trades.empty:
            continue

        n_tr    = len(day_trades)
        day_pnl = day_trades["total_pnl"].sum()
        wins    = int((day_trades["total_pnl"] > 0).sum())
        losses  = int((day_trades["total_pnl"] <= 0).sum())
        on_cnt  = int((day_trades["days_held"] > 0).sum())

        fname = f"{date.isoformat()}.html"

        fig = generate_day_chart(
            date, day_trades,
            nifty_5m, up_frac_s, dn_frac_s, up_ctr_s, dn_ctr_s,
            nifty_1m, contracts, opt_con, n
        )
        if fig is None:
            continue

        table_rows = build_trade_table_html(day_trades)
        chart_html = fig.to_html(full_html=False, include_plotlyjs="cdn")

        p_col  = "#26a69a" if day_pnl >= 0 else "#ef5350"
        prev_l = (f"<a href='{trade_dates[idx-1].isoformat()}.html'>← Prev</a>"
                  if idx > 0 else "")
        next_l = (f"<a href='{trade_dates[idx+1].isoformat()}.html'>Next →</a>"
                  if idx < len(trade_dates) - 1 else "")

        page = f"""<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<title>WF Fractals — {date.isoformat()}</title>
<style>
  body{{font-family:'Segoe UI',sans-serif;background:#1a1a2e;color:#eee;margin:0;padding:15px}}
  .nav{{margin-bottom:10px;font-size:13px}}
  .nav a{{color:#00d2ff;text-decoration:none;margin-right:15px}}
  h3{{color:#aaa;margin-top:20px;font-size:14px}}
  table{{border-collapse:collapse;width:100%;margin-top:10px;font-size:12px}}
  th{{background:#16213e;color:#00d2ff;padding:7px;text-align:left;position:sticky;top:0}}
  td{{padding:5px 7px;border-bottom:1px solid #2a2a3e}}
  .profit{{color:#26a69a;font-weight:bold}} .loss{{color:#ef5350;font-weight:bold}}
</style>
</head><body>
<div class="nav">
  <a href="index.html">← All Days</a>{prev_l} {next_l}
</div>
{chart_html}
<h3>Trade Details — William Fractals (n={n}) Credit Spread</h3>
<table>
<tr>
  <th>#</th><th>Entry</th><th>Exit</th><th>Spread</th>
  <th>Sell Leg</th><th>Buy Leg</th><th>Spot</th>
  <th>Sell₹</th><th>Buy₹</th><th>Credit</th>
  <th>Sell Exit</th><th>Buy Exit</th><th>Exit Sprd</th>
  <th>Hold</th><th>Gross</th><th>Charges</th><th>Net P&L</th><th>Reason</th>
</tr>
{table_rows}
</table>
</body></html>"""

        with open(os.path.join(charts_dir, fname), "w") as f:
            f.write(page)

        all_days[date] = {
            "filename": fname, "trades": n_tr,
            "wins": wins, "losses": losses,
            "pnl": day_pnl, "overnight": on_cnt,
        }

        if (idx + 1) % 10 == 0 or (idx + 1) == len(trade_dates):
            print(f"  {idx+1}/{len(trade_dates)} days done", flush=True)

    # ── Index page ────────────────────────────────────────────────────────────
    generate_index_html(all_days, charts_dir, n)

    opt_con.close()
    con.close()

    print(f"\nDone! {len(all_days)} charts saved to: {charts_dir}/")
    print(f"Open: {charts_dir}/index.html")


if __name__ == "__main__":
    main()
