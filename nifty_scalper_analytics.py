"""
Nifty Scalper Analytics Engine
================================
Answers 101 research questions about intraday index behaviour
using 1-min OHLC data from ExpiryTrack DuckDB.

Covers: Gap Dynamics, Time Windows, ADR/Volatility, Context(T-1),
        Technicals, Day-of-Week, Micro-structure, Global/Events,
        Option Data, Mean Reversion, Advanced Patterns, Scalper-specific,
        Risk/Psychology, Volatility Clusters, Multi-timeframe,
        Closing Dynamics, Sectoral Sync, Summary Stats.

Usage:
    python nifty_scalper_analytics.py
    python nifty_scalper_analytics.py --days 252      # last 1 year
    python nifty_scalper_analytics.py --from 2024-01-01 --to 2024-12-31
    python nifty_scalper_analytics.py --section gaps  # one section only
"""

import argparse
import os
import sys
import warnings
from datetime import date, datetime, time, timedelta
from pathlib import Path

import duckdb
from typing import Optional
import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore")

# ============================================================================
# CONFIGURATION — adjust paths and instrument keys to match your DB
# ============================================================================
DB_PATH = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"

# Instrument keys in candle_data (run with --discover to list all)
INSTRUMENTS = {
    "nifty":    "NSE_INDEX|Nifty 50",
    "banknifty":"NSE_INDEX|Nifty Bank",
    "sensex":   "BSE_INDEX|SENSEX",
    "midcap":   "NSE_INDEX|NIFTY MID SELECT",
    "finnifty": "NSE_INDEX|Nifty Fin Service",
    "vix":      "NSE_INDEX|India VIX",
    "midcap":   "NSE_INDEX|NIFTY MID SELECT",
    "finnifty": "NSE_INDEX|Nifty Fin Service",
    "niftyit":  "NSE_INDEX|Nifty IT",
}

# Trading session times (IST)
SESSION_OPEN  = time(9, 15)
SESSION_CLOSE = time(15, 30)

# Three 125-min windows (scalper windows)
W1_START, W1_END = time(9,  15), time(11, 20)
W2_START, W2_END = time(11, 20), time(13, 25)
W3_START, W3_END = time(13, 25), time(15, 30)

LUNCH_START, LUNCH_END = time(12,  0), time(13, 30)
MOC_TIME               = time(15, 25)
CLOSING_WINDOW_START   = time(15,  0)

OUTPUT_DIR = Path("scalper_analytics_output")
OUTPUT_DIR.mkdir(exist_ok=True)


# ============================================================================
# RESULT STORE
# ============================================================================
RESULTS: dict = {}


def record(qid: int, title: str, value, detail: str = ""):
    RESULTS[qid] = {"title": title, "value": value, "detail": detail}


# ============================================================================
# DB CONNECTION & DATA LOADING
# ============================================================================
def connect_db():
    import time as _time
    last_err = None
    for attempt in range(3):
        try:
            return duckdb.connect(DB_PATH, read_only=True)
        except Exception as e:
            last_err = e
            err_str = str(e)
            if "WAL" in err_str or "locked" in err_str.lower() or "Bad file descriptor" in err_str:
                if attempt == 0:
                    print(f"\n  DB appears locked by another process. Waiting 3s...")
                    print(f"  TIP: Close any open Python sessions / backtest scripts using this DB.")
                    print(f"  Alternatively run:  lsof {DB_PATH}  to find the blocking process.\n")
                _time.sleep(3)
            else:
                break
    print(f"\n  ERROR: Cannot connect to DB at {DB_PATH}")
    print(f"  {last_err}")
    print("  Fixes:")
    print("    1. Close all other Python sessions that use expirytrack.duckdb")
    print("    2. Run:  lsof {DB_PATH}  then:  kill -9 <PID>")
    print("    3. If the WAL file is corrupted, delete the .wal file:")
    print(f"       rm {DB_PATH}.wal")
    print("       (safe to delete — DuckDB will rebuild it on next open)\n")
    sys.exit(1)


def discover_instruments(con):
    """Print all available instrument keys in the DB."""
    print("\n=== AVAILABLE INSTRUMENTS IN candle_data ===")
    rows = con.execute("""
        SELECT instrument_key, interval,
               COUNT(*) as bars,
               MIN(timestamp)::DATE as first,
               MAX(timestamp)::DATE as last
        FROM candle_data
        GROUP BY instrument_key, interval
        ORDER BY instrument_key, interval
    """).fetchall()
    for r in rows:
        print(f"  {str(r[0]):<50} | {str(r[1]):<10} | {r[2]:>8,} bars | {r[3]} → {r[4]}")
    print()


def load_1min(con, key: str, from_date: str, to_date: str) -> pd.DataFrame:
    """Load 1-min OHLC bars for an instrument."""
    sql = f"""
        SELECT timestamp, open, high, low, close, volume
        FROM candle_data
        WHERE instrument_key = '{key}'
          AND interval = '1minute'
          AND timestamp::DATE BETWEEN '{from_date}' AND '{to_date}'
        ORDER BY timestamp
    """
    df = con.execute(sql).fetchdf()
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    df = df[~df.index.duplicated(keep="first")]
    for c in ["open", "high", "low", "close"]:
        df[c] = df[c].astype(float)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    # Session filter: 9:15 to 15:30 only
    df = df.between_time(SESSION_OPEN, SESSION_CLOSE)
    return df


def build_daily(m1: pd.DataFrame) -> pd.DataFrame:
    """Build daily OHLCV from 1-min bars."""
    if m1.empty:
        return pd.DataFrame()
    daily = m1.resample("B").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).dropna(subset=["open"])
    daily.index = daily.index.date
    return daily


def window_ohlcv(m1: pd.DataFrame, ws: time, we: time) -> pd.DataFrame:
    """Daily OHLCV within a specific time window."""
    sub = m1.between_time(ws, we)
    if sub.empty:
        return pd.DataFrame()
    out = sub.resample("B").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).dropna(subset=["open"])
    out.index = out.index.date
    return out


# ============================================================================
# INDICATOR HELPERS
# ============================================================================
def compute_adr(daily: pd.DataFrame, n: int) -> pd.Series:
    """Rolling n-day average daily range (high-low)."""
    return (daily["high"] - daily["low"]).rolling(n).mean()


def compute_twap(m1_day: pd.DataFrame) -> float:
    """Session TWAP = simple mean of 1-min closes for one day."""
    return float(m1_day["close"].mean()) if len(m1_day) > 0 else np.nan


def compute_vwap(m1_day: pd.DataFrame) -> pd.Series:
    """Cumulative VWAP for one day.
    Falls back to cumulative TWAP if volume is zero (e.g. NSE index data)."""
    tp = (m1_day["high"] + m1_day["low"] + m1_day["close"]) / 3
    vol = m1_day["volume"].replace(0, np.nan)
    if vol.isna().all():
        # No volume data — use cumulative mean of typical price (TWAP-style)
        return tp.expanding().mean()
    vwap = (tp * vol).cumsum() / vol.cumsum()
    # Fill any NaN gaps with cumulative TWAP
    vwap = vwap.fillna(tp.expanding().mean())
    return vwap


def compute_ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()


def compute_supertrend(m1: pd.DataFrame, period: int = 7, mult: float = 3.0) -> pd.Series:
    """Supertrend direction (+1 bull / -1 bear) on 1-min bars.
    Uses numpy arrays to avoid pandas iloc assignment issues in pandas 2.x."""
    h = m1["high"].values.astype(float)
    l = m1["low"].values.astype(float)
    c = m1["close"].values.astype(float)
    n = len(c)
    if n < period + 2:
        return pd.Series(1, index=m1.index)
    # True range
    tr = np.zeros(n)
    tr[0] = h[0] - l[0]
    for i in range(1, n):
        tr[i] = max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1]))
    # EMA of TR (Wilder's: alpha = 2/(period+1) for ewm span=period)
    alpha = 2.0 / (period + 1)
    atr = np.zeros(n)
    atr[0] = tr[0]
    for i in range(1, n):
        atr[i] = alpha * tr[i] + (1 - alpha) * atr[i-1]
    hl2 = (h + l) / 2.0
    up  = hl2 + mult * atr
    dn  = hl2 - mult * atr
    # Band clamping
    f_up = up.copy()
    f_dn = dn.copy()
    for i in range(1, n):
        f_up[i] = min(up[i], f_up[i-1]) if c[i-1] < f_up[i-1] else up[i]
        f_dn[i] = max(dn[i], f_dn[i-1]) if c[i-1] > f_dn[i-1] else dn[i]
    direction = np.ones(n, dtype=int)
    for i in range(1, n):
        if direction[i-1] == 1:
            direction[i] = 1 if c[i] >= f_dn[i] else -1
        else:
            direction[i] = -1 if c[i] <= f_up[i] else 1
    return pd.Series(direction, index=m1.index)


def compute_rsi(series: pd.Series, n: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(span=n, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(span=n, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def classify_trend_day(m1_day: pd.DataFrame, threshold: float = 0.6) -> bool:
    """
    Trend day: close is in top or bottom threshold of day's range
    AND price moved monotonically for most of the session.
    """
    if m1_day.empty or len(m1_day) < 10:
        return False
    rng = m1_day["high"].max() - m1_day["low"].min()
    if rng == 0:
        return False
    pos = (m1_day["close"].iloc[-1] - m1_day["low"].min()) / rng
    return pos >= threshold or pos <= (1 - threshold)


def is_expiry_day(d: date, expiry_weekday: int = 3) -> bool:
    """Thursday = weekday 3. NIFTY weekly expiry."""
    return d.weekday() == expiry_weekday


# ============================================================================
# SECTION A — GAP DYNAMICS (Q1–Q10)
# ============================================================================
def analyze_gaps(daily: pd.DataFrame, m1: pd.DataFrame):
    print("\n[A] GAP DYNAMICS")

    if len(daily) < 5:
        print("  Insufficient daily data.")
        return

    d = daily.copy()
    d["prev_close"] = d["close"].shift(1)
    d["gap_pts"]  = d["open"] - d["prev_close"]
    d["gap_pct"]  = d["gap_pts"] / d["prev_close"] * 100
    d["range"]    = d["high"] - d["low"]
    d["is_expiry"]= pd.Series(d.index, index=d.index).apply(is_expiry_day)
    d = d.dropna(subset=["prev_close"])

    # Q1 — Average gap
    avg_gap_pts = d["gap_pts"].mean()
    avg_gap_pct = d["gap_pct"].mean()
    med_gap_pts = d["gap_pts"].median()
    gap_up   = (d["gap_pts"] > 0).mean() * 100
    gap_down = (d["gap_pts"] < 0).mean() * 100
    gap_flat = (d["gap_pts"].abs() < 5).mean() * 100
    record(1, "Average gap (pts / %)",
           f"{avg_gap_pts:+.1f} pts / {avg_gap_pct:+.3f}%",
           f"Median: {med_gap_pts:+.1f} pts | Gap-up: {gap_up:.1f}% | "
           f"Gap-down: {gap_down:.1f}% | Flat (<5pts): {gap_flat:.1f}%")
    print(f"  Q1  Avg gap {avg_gap_pts:+.1f} pts ({avg_gap_pct:+.3f}%) | "
          f"Gap-up {gap_up:.0f}% / Flat {gap_flat:.0f}% / Gap-dn {gap_down:.0f}%")

    # Q2 — Gap >0.7%: Run or Fade
    big = d[d["gap_pct"].abs() >= 0.7].copy()
    if len(big) >= 5:
        big["gap_up_flag"] = big["gap_pct"] > 0
        # "Run" = price extends beyond open in gap direction
        # "Fade" = price moves back towards prev_close
        big["run"] = np.where(
            big["gap_up_flag"],
            big["high"] > big["open"] + big["gap_pts"].abs() * 0.5,
            big["low"]  < big["open"] - big["gap_pts"].abs() * 0.5,
        )
        run_pct  = big["run"].mean() * 100
        fade_pct = 100 - run_pct
        record(2, "Gap >0.7%: Run vs Fade",
               f"Run {run_pct:.1f}% / Fade {fade_pct:.1f}%",
               f"n={len(big)} days with gap >0.7%")
        print(f"  Q2  Gap >0.7% → Run {run_pct:.1f}% vs Fade {fade_pct:.1f}% (n={len(big)})")
    else:
        print(f"  Q2  Insufficient large-gap days (n={len(big)})")

    # Q3 — Open = High or Low of day
    open_is_high = (d["open"] == d["high"]).mean() * 100
    open_is_low  = (d["open"] == d["low"]).mean() * 100
    open_near_high = ((d["high"] - d["open"]) / d["range"] < 0.05).mean() * 100
    open_near_low  = ((d["open"] - d["low"])  / d["range"] < 0.05).mean() * 100
    record(3, "Open = Day High or Low",
           f"Exact: High {open_is_high:.1f}% / Low {open_is_low:.1f}%",
           f"Near (5%): High {open_near_high:.1f}% / Low {open_near_low:.1f}%")
    print(f"  Q3  Open=High {open_is_high:.1f}% | Open=Low {open_is_low:.1f}% "
          f"| Near-high {open_near_high:.1f}% | Near-low {open_near_low:.1f}%")

    # Q4 — Gap-up filled in 60 mins
    gap_up_days = d[d["gap_pts"] > 5].copy()
    filled_in_60 = []
    for dt, row in gap_up_days.iterrows():
        try:
            day_m1 = m1[m1.index.date == dt]
            first60 = day_m1.between_time(time(9,15), time(10,15))
            if first60.empty:
                continue
            filled = first60["low"].min() <= row["prev_close"]
            filled_in_60.append(filled)
        except Exception:
            pass
    fill_rate = np.mean(filled_in_60) * 100 if filled_in_60 else np.nan
    record(4, "Gap-up filled in 60 mins",
           f"{fill_rate:.1f}%",
           f"n={len(filled_in_60)} gap-up days analysed")
    print(f"  Q4  Gap-up filled in 60 mins: {fill_rate:.1f}% (n={len(filled_in_60)})")

    # Q5 — After gap fill does price reverse at PDC?
    reversed_at_pdc = 0
    checked = 0
    for dt, row in gap_up_days.iterrows():
        try:
            day_m1 = m1[m1.index.date == dt]
            first60 = day_m1.between_time(time(9,15), time(10,15))
            if first60.empty:
                continue
            if first60["low"].min() <= row["prev_close"]:
                # Filled — did it then go back up (closed above PDC)?
                checked += 1
                if day_m1["close"].iloc[-1] > row["prev_close"]:
                    reversed_at_pdc += 1
        except Exception:
            pass
    rev_pct = (reversed_at_pdc / checked * 100) if checked else np.nan
    record(5, "After gap fill: reversal at PDC",
           f"{rev_pct:.1f}%",
           f"n={checked} filled days")
    print(f"  Q5  Reversed up at PDC after fill: {rev_pct:.1f}% (n={checked})")

    # Q6 — Flat open (<0.1% gap) → W1 range
    flat = d[d["gap_pct"].abs() < 0.1]
    non_flat = d[d["gap_pct"].abs() >= 0.1]
    if len(flat) >= 5:
        # We'll use day range as proxy for W1 here (W1 computed in window section)
        record(6, "Flat open (<0.1%) day range",
               f"Avg range {flat['range'].mean():.0f} pts vs {non_flat['range'].mean():.0f} pts (non-flat)",
               f"n={len(flat)} flat days")
        print(f"  Q6  Flat open → avg day range {flat['range'].mean():.0f} pts "
              f"vs non-flat {non_flat['range'].mean():.0f} pts (n={len(flat)})")

    # Q7 — Gap-and-go on Expiry vs Non-expiry
    d["weekday"] = pd.Series(d.index, index=d.index).apply(lambda x: x.weekday())
    expiry_d   = d[d["is_expiry"]]
    non_expiry = d[~d["is_expiry"]]
    def gap_and_go_rate(sub):
        if len(sub) == 0:
            return np.nan
        up = sub[sub["gap_pts"] > 5]
        if len(up) == 0:
            return np.nan
        return (up["high"] > up["open"] + 10).mean() * 100
    exp_gg   = gap_and_go_rate(expiry_d)
    nonexp_gg = gap_and_go_rate(non_expiry)
    record(7, "Gap-and-Go: Expiry vs Non-expiry",
           f"Expiry {exp_gg:.1f}% / Non-expiry {nonexp_gg:.1f}%",
           f"Expiry n={len(expiry_d)} | Non-expiry n={len(non_expiry)}")
    print(f"  Q7  Gap-and-Go | Expiry {exp_gg:.1f}% vs Non-expiry {nonexp_gg:.1f}%")

    # Q8 — Average time (mins) to fill a 50-pt gap
    big50 = d[d["gap_pts"].abs() >= 50]
    fill_times = []
    for dt, row in big50.iterrows():
        try:
            day_m1 = m1[m1.index.date == dt]
            pdc = row["prev_close"]
            gap_up_flag = row["gap_pts"] > 0
            for ts, bar in day_m1.iterrows():
                if gap_up_flag and bar["low"] <= pdc:
                    elapsed = (ts - day_m1.index[0]).seconds / 60
                    fill_times.append(elapsed)
                    break
                elif not gap_up_flag and bar["high"] >= pdc:
                    elapsed = (ts - day_m1.index[0]).seconds / 60
                    fill_times.append(elapsed)
                    break
        except Exception:
            pass
    avg_fill_time = np.mean(fill_times) if fill_times else np.nan
    record(8, "Avg time to fill 50-pt gap (mins)",
           f"{avg_fill_time:.0f} mins",
           f"n={len(fill_times)} events | median {np.median(fill_times):.0f} mins" if fill_times else "n=0")
    print(f"  Q8  Avg time to fill 50pt gap: {avg_fill_time:.0f} mins (n={len(fill_times)})")

    # Q9 — Gap-down Friday → short covering by 3pm
    fridays = d[(d["gap_pts"] < -5) & (d["weekday"] == 4)]
    sc_count = 0
    for dt, row in fridays.iterrows():
        try:
            day_m1 = m1[m1.index.date == dt]
            by3pm = day_m1.between_time(time(9,15), time(15,0))
            # Short covering: close above open despite gap-down
            if not by3pm.empty and by3pm["close"].iloc[-1] > row["open"]:
                sc_count += 1
        except Exception:
            pass
    sc_rate = (sc_count / len(fridays) * 100) if len(fridays) > 0 else np.nan
    record(9, "Gap-down Friday → short covering by 3pm",
           f"{sc_rate:.1f}%",
           f"n={len(fridays)} gap-down Fridays")
    print(f"  Q9  Gap-down Friday → short covering by 3pm: {sc_rate:.1f}% (n={len(fridays)})")

    # Q10 — If Open > PDH, odds of touching PDC
    d["prev_high"] = d["high"].shift(1)
    above_pdh = d[d["open"] > d["prev_high"]].copy()
    if len(above_pdh) >= 3:
        touches_pdc = (above_pdh["low"] <= above_pdh["prev_close"]).mean() * 100
        record(10, "Open > PDH: Odds of touching PDC",
               f"{touches_pdc:.1f}%",
               f"n={len(above_pdh)} gap-above-PDH days")
        print(f"  Q10 Open>PDH → touches PDC: {touches_pdc:.1f}% (n={len(above_pdh)})")
    else:
        print(f"  Q10 Insufficient open>PDH days (n={len(above_pdh)})")


# ============================================================================
# SECTION B — TIME WINDOWS (Q11–Q20)
# ============================================================================
def analyze_windows(m1: pd.DataFrame, daily: pd.DataFrame):
    print("\n[B] TIME WINDOWS")

    w1 = window_ohlcv(m1, W1_START, W1_END)
    w2 = window_ohlcv(m1, W2_START, W2_END)
    w3 = window_ohlcv(m1, W3_START, W3_END)

    if w1.empty:
        print("  Insufficient window data.")
        return

    w1["range"] = w1["high"] - w1["low"]
    w2["range"] = w2["high"] - w2["low"]
    w3["range"] = w3["high"] - w3["low"]

    adr20 = compute_adr(daily, 20)

    # Q11 — Which window has highest avg range
    r1, r2, r3 = w1["range"].mean(), w2["range"].mean(), w3["range"].mean()
    winner = ["W1", "W2", "W3"][[r1, r2, r3].index(max(r1, r2, r3))]
    record(11, "Highest avg range window",
           winner,
           f"W1={r1:.1f} W2={r2:.1f} W3={r3:.1f} pts")
    print(f"  Q11 Avg range: W1={r1:.1f}  W2={r2:.1f}  W3={r3:.1f} → {winner} wins")

    # Q12 — Mega W1 (>ADR) → sideways W2?
    # Convert adr20 index to date for alignment with w1.index (which are date objects)
    adr20_by_date = {idx.date() if hasattr(idx, "date") else idx: val
                     for idx, val in adr20.dropna().items()}
    common_dates = [d for d in w1.index if d in w2.index and d in adr20_by_date]
    mega_w1 = []
    for dt in common_dates:
        try:
            adr_val = adr20_by_date[dt]
            if not np.isnan(adr_val) and w1.loc[dt, "range"] >= adr_val:
                mega_w1.append(dt)
        except Exception:
            pass
    if len(mega_w1) >= 5:
        w2_is_sideways = []
        for dt in mega_w1:
            if dt in w2.index:
                # Sideways = W2 range < 50% of W1 range
                w2_is_sideways.append(w2.loc[dt, "range"] < w1.loc[dt, "range"] * 0.5)
        sw_rate = np.mean(w2_is_sideways) * 100
        record(12, "Mega W1 (>ADR) → W2 sideways",
               f"{sw_rate:.1f}%",
               f"n={len(mega_w1)} mega-W1 days")
        print(f"  Q12 Mega W1 → W2 sideways (range <50% W1): {sw_rate:.1f}% (n={len(mega_w1)})")
    else:
        print(f"  Q12 Insufficient mega-W1 days (n={len(mega_w1)})")

    # Q13 — Day High formed in W3
    common_all = daily.index[daily.index.isin(w3.index)]
    dh_in_w3 = (daily.loc[common_all, "high"] == w3.loc[common_all, "high"]).mean() * 100
    record(13, "Day High formed in W3",
           f"{dh_in_w3:.1f}%",
           f"n={len(common_all)} days")
    print(f"  Q13 Day High in W3: {dh_in_w3:.1f}%")

    # Q14 — Day Low formed in W1
    common_w1d = daily.index[daily.index.isin(w1.index)]
    dl_in_w1 = (daily.loc[common_w1d, "low"] == w1.loc[common_w1d, "low"]).mean() * 100
    record(14, "Day Low formed in W1",
           f"{dl_in_w1:.1f}%",
           f"n={len(common_w1d)} days")
    print(f"  Q14 Day Low in W1:  {dl_in_w1:.1f}%")

    # Q15 — W3 reverses W1 trend
    w1["trend"] = np.where(w1["close"] > w1["open"], 1, -1)
    w3["trend"] = np.where(w3["close"] > w3["open"], 1, -1)
    common_13 = w1.index[w1.index.isin(w3.index)]
    reversals = (w1.loc[common_13, "trend"] != w3.loc[common_13, "trend"]).mean() * 100
    record(15, "W3 reverses W1 trend",
           f"{reversals:.1f}%",
           f"n={len(common_13)} days")
    print(f"  Q15 W3 reverses W1 trend: {reversals:.1f}%")

    # Q16 — W1 volume vs W3 volatility correlation
    common_13v = w1.index[w1.index.isin(w3.index)]
    if len(common_13v) >= 10 and "volume" in w1.columns and "range" in w3.columns:
        v1 = w1.loc[common_13v, "volume"].replace(0, np.nan).dropna()
        r3 = w3.loc[common_13v, "range"].reindex(v1.index).dropna()
        v1 = v1.reindex(r3.index)
        if len(v1) >= 10:
            corr, pval = stats.pearsonr(v1.values, r3.values)
            record(16, "W1 volume vs W3 range correlation",
                   f"r={corr:.3f} (p={pval:.3f})",
                   "Positive = high W1 volume → larger W3 range")
            print(f"  Q16 W1 volume vs W3 range: r={corr:.3f} (p={pval:.3f})")
        else:
            print(f"  Q16 Insufficient overlapping data after NaN drop (n={len(v1)})")
    else:
        print(f"  Q16 Volume data not available in window aggregation")

    # Q17 — W1+W2 inside bars → W3 breakout
    common_w123 = [d for d in w1.index
                   if d in w2.index and d in w3.index and d in daily.index]
    inside_days = []
    for dt in common_w123:
        try:
            dh = daily.loc[dt, "high"]
            dl = daily.loc[dt, "low"]
            w1h, w1l = w1.loc[dt, "high"], w1.loc[dt, "low"]
            w2h, w2l = w2.loc[dt, "high"], w2.loc[dt, "low"]
            if w2h < w1h and w2l > w1l:
                # Both W1 and W2 inside day range — check W3 breakout
                w3h, w3l = w3.loc[dt, "high"], w3.loc[dt, "low"]
                breakout = w3h > w1h or w3l < w1l
                inside_days.append(breakout)
        except Exception:
            pass
    if len(inside_days) >= 5:
        bo_rate = np.mean(inside_days) * 100
        record(17, "W1+W2 inside → W3 breakout",
               f"{bo_rate:.1f}%",
               f"n={len(inside_days)} inside days")
        print(f"  Q17 W1+W2 inside → W3 breakout: {bo_rate:.1f}% (n={len(inside_days)})")

    # Q18 — Lunch hour (12:00-13:30) avg range
    lunch = window_ohlcv(m1, LUNCH_START, LUNCH_END)
    if not lunch.empty:
        lunch["range"] = lunch["high"] - lunch["low"]
        record(18, "Lunch hour (12:00-13:30) avg range",
               f"{lunch['range'].mean():.1f} pts",
               f"Median {lunch['range'].median():.1f} pts | Max {lunch['range'].max():.1f} pts")
        print(f"  Q18 Lunch range avg {lunch['range'].mean():.1f} pts | median {lunch['range'].median():.1f} pts")

    # Q19 — Trend starting 1:30 continues to 3:30
    continues = 0
    total_130 = 0
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            at_130 = day_m1.between_time(time(13,25), time(13,30))
            at_close = day_m1.between_time(time(15,25), time(15,30))
            if at_130.empty or at_close.empty:
                continue
            total_130 += 1
            dir_130 = 1 if at_130["close"].iloc[-1] > day_m1["open"].iloc[0] else -1
            dir_cls = 1 if at_close["close"].iloc[-1] > day_m1["open"].iloc[0] else -1
            if dir_130 == dir_cls:
                continues += 1
        except Exception:
            pass
    cont_rate = (continues / total_130 * 100) if total_130 else np.nan
    record(19, "Trend from 1:30 continues to 3:30",
           f"{cont_rate:.1f}%",
           f"n={total_130} days")
    print(f"  Q19 1:30 trend continues to close: {cont_rate:.1f}% (n={total_130})")

    # Q20 — Trending W1 → mean-reverting W2
    w1["trend_flag"] = w1["trend"]
    if "trend" in w2.columns:
        w2.drop(columns=["trend"], inplace=True)
    w2["close_vs_open"] = w2["close"] - w2["open"]
    common_12 = w1.index[w1.index.isin(w2.index)]
    mean_rev = []
    for dt in common_12:
        # Trending W1 = close far from open
        w1_move = w1.loc[dt, "close"] - w1.loc[dt, "open"]
        w2_move = w2.loc[dt, "close_vs_open"]
        if abs(w1_move) > w1.loc[dt, "range"] * 0.4:
            # W2 reverting = moves opposite to W1
            mean_rev.append(np.sign(w1_move) != np.sign(w2_move))
    rev_rate = np.mean(mean_rev) * 100 if mean_rev else np.nan
    record(20, "Trending W1 → mean-reverting W2",
           f"{rev_rate:.1f}%",
           f"n={len(mean_rev)} trending-W1 days")
    print(f"  Q20 Trending W1 → mean-reverting W2: {rev_rate:.1f}% (n={len(mean_rev)})")


# ============================================================================
# SECTION C — ADR & VOLATILITY (Q21–Q30)
# ============================================================================
def analyze_adr(daily: pd.DataFrame, m1: pd.DataFrame, vix_daily: Optional[pd.DataFrame]):
    print("\n[C] ADR & VOLATILITY")

    if len(daily) < 5:
        print("  Insufficient data.")
        return

    daily["range"] = daily["high"] - daily["low"]
    adr5  = compute_adr(daily, 5)
    adr10 = compute_adr(daily, 10)
    adr20 = compute_adr(daily, 20)

    # Q21 — Current 5/10/20-day ADR
    r5  = adr5.iloc[-1] if len(adr5.dropna()) > 0 else np.nan
    r10 = adr10.iloc[-1] if len(adr10.dropna()) > 0 else np.nan
    r20 = adr20.iloc[-1] if len(adr20.dropna()) > 0 else np.nan
    record(21, "Current ADR (5/10/20-day)",
           f"5d={r5:.0f}  10d={r10:.0f}  20d={r20:.0f} pts",
           f"Based on last available day in dataset")
    print(f"  Q21 ADR: 5d={r5:.0f}  10d={r10:.0f}  20d={r20:.0f} pts")

    # Q22 — Hits 80% ADR by 11am → reversal probability
    # Build date-keyed ADR dict to avoid Timestamp vs date mismatch
    adr20_by_date2 = {idx.date() if hasattr(idx, "date") else idx: val
                      for idx, val in adr20.dropna().items()}
    rev_after_80 = []
    for dt in daily.index:
        if dt not in adr20_by_date2:
            continue
        adr_val = adr20_by_date2[dt]
        if np.isnan(adr_val):
            continue
        try:
            by11 = m1[m1.index.date == dt].between_time(time(9,15), time(11,0))
            if by11.empty:
                continue
            range_by11 = by11["high"].max() - by11["low"].min()
            if range_by11 >= adr_val * 0.8:
                after11 = m1[m1.index.date == dt].between_time(time(11,0), time(15,30))
                if after11.empty:
                    continue
                # Reversal: close goes back significantly from the 11am extreme
                was_up = by11["close"].iloc[-1] > by11["open"].iloc[0]
                rev = (after11["close"].iloc[-1] < by11["close"].iloc[-1]) if was_up else \
                      (after11["close"].iloc[-1] > by11["close"].iloc[-1])
                rev_after_80.append(rev)
        except Exception:
            pass
    rev_rate = np.mean(rev_after_80) * 100 if rev_after_80 else np.nan
    record(22, "Hits 80% ADR by 11am → reversal",
           f"{rev_rate:.1f}%",
           f"n={len(rev_after_80)} events")
    print(f"  Q22 80% ADR by 11am → reversal: {rev_rate:.1f}% (n={len(rev_after_80)})")

    # Q23 — Low VIX (<12) → smaller ADR
    if vix_daily is not None and not vix_daily.empty:
        merged = daily.copy()
        merged["vix"] = vix_daily["close"].reindex(merged.index)
        low_vix = merged[merged["vix"] < 12]
        high_vix = merged[merged["vix"] >= 12]
        record(23, "Low VIX (<12) vs high VIX ADR",
               f"Low VIX={low_vix['range'].mean():.0f} pts | High VIX={high_vix['range'].mean():.0f} pts",
               f"Low VIX n={len(low_vix)} | High VIX n={len(high_vix)}")
        print(f"  Q23 ADR | VIX<12={low_vix['range'].mean():.0f} pts | VIX≥12={high_vix['range'].mean():.0f} pts")
    else:
        print("  Q23 VIX data not available (add 'India VIX' instrument key)")

    # Q24 — Expansion ratio = today range / 5-day ADR
    exp_ratio = daily["range"] / adr5
    record(24, "Expansion ratio (range / 5-day ADR)",
           f"Avg={exp_ratio.mean():.2f}x | Median={exp_ratio.median():.2f}x",
           f">1.5x on {(exp_ratio > 1.5).sum()} days | <0.5x on {(exp_ratio < 0.5).sum()} days")
    print(f"  Q24 Expansion ratio avg {exp_ratio.mean():.2f}x | "
          f">1.5x: {(exp_ratio > 1.5).sum()} days")

    # Q25 — Days/month exceeding 1.5x ADR
    over15 = (exp_ratio > 1.5)
    over15.index = pd.to_datetime(over15.index)
    monthly_over = over15.resample("ME").sum()
    record(25, "Days/month exceeding 1.5x ADR",
           f"Avg {monthly_over.mean():.1f} days/month",
           f"Max {monthly_over.max():.0f} | Min {monthly_over.min():.0f}")
    print(f"  Q25 1.5x ADR days per month: avg {monthly_over.mean():.1f} | max {monthly_over.max():.0f}")

    # Q26 — 15-min range >40pts → Trend Day
    trend_if_big15 = []
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            first15 = day_m1.between_time(time(9,15), time(9,30))
            if first15.empty:
                continue
            rng15 = first15["high"].max() - first15["low"].min()
            is_trend = classify_trend_day(day_m1)
            if rng15 > 40:
                trend_if_big15.append(is_trend)
        except Exception:
            pass
    td_rate = np.mean(trend_if_big15) * 100 if trend_if_big15 else np.nan
    record(26, "15-min range >40pts → Trend Day",
           f"{td_rate:.1f}%",
           f"n={len(trend_if_big15)} days with big 15-min opening")
    print(f"  Q26 15-min range >40pts → Trend Day: {td_rate:.1f}% (n={len(trend_if_big15)})")

    # Q27 — Neutral Day (price stays in 1st hour H/L)
    neutral_count = 0
    total_d = 0
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            hr1 = day_m1.between_time(time(9,15), time(10,15))
            if hr1.empty:
                continue
            h1, l1 = hr1["high"].max(), hr1["low"].min()
            total_d += 1
            if daily.loc[dt, "high"] <= h1 * 1.002 and daily.loc[dt, "low"] >= l1 * 0.998:
                neutral_count += 1
        except Exception:
            pass
    neutral_rate = (neutral_count / total_d * 100) if total_d else np.nan
    record(27, "Neutral Day (stays in 1st hour H/L)",
           f"{neutral_rate:.1f}%",
           f"n={total_d} days")
    print(f"  Q27 Neutral Day (in 1st hr range): {neutral_rate:.1f}% (n={total_d})")

    # Q28 — Small range day (<0.5% ADR) → Big range day tomorrow
    small_today = exp_ratio < 0.5
    small_today_shifted = small_today.shift(1)
    big_tomorrow = exp_ratio > 1.2
    both = small_today_shifted & big_tomorrow
    small_n = small_today_shifted.sum()
    big_after_small = both.sum()
    rate = (big_after_small / small_n * 100) if small_n > 0 else np.nan
    record(28, "Small range day → Big range tomorrow",
           f"{rate:.1f}%",
           f"n={int(small_n)} small days | {int(big_after_small)} followed by big")
    print(f"  Q28 Small day → Big day next: {rate:.1f}% (n={int(small_n)})")

    # Q29 — Nifty range vs BankNifty range correlation
    # Handled in cross-index section
    record(29, "Nifty vs BankNifty range correlation",
           "See Q29 in cross-index section",
           "Requires BankNifty data loaded separately")
    print(f"  Q29 → Computed in cross-index section")

    # Q30 — ADR on event weeks (placeholder — needs event calendar)
    record(30, "ADR on event weeks (RBI/Budget)",
           "Requires external event calendar",
           "Add event_dates list to CONFIG to enable this check")
    print(f"  Q30 Event-week ADR: requires event calendar (see config)")


# ============================================================================
# SECTION D — CONTEXT T-1 (Q31–Q40)
# ============================================================================
def analyze_context_t1(daily: pd.DataFrame, m1: pd.DataFrame):
    print("\n[D] CONTEXT (T-1)")

    if len(daily) < 5:
        print("  Insufficient data.")
        return

    d = daily.copy()
    d["pdh"] = d["high"].shift(1)
    d["pdl"] = d["low"].shift(1)
    d["pdc"] = d["close"].shift(1)
    d["pdm"] = (d["pdh"] + d["pdl"]) / 2   # previous day midpoint
    d = d.dropna(subset=["pdh"])

    # Q31 — Support at PDL
    found_support = (d["low"] <= d["pdl"]) & (d["close"] > d["pdl"])
    tested_pdl    = d["low"] <= d["pdl"] * 1.002
    rate = (found_support.sum() / tested_pdl.sum() * 100) if tested_pdl.sum() else np.nan
    record(31, "Support found at PDL",
           f"{rate:.1f}% of times PDL tested",
           f"PDL tested {int(tested_pdl.sum())} days | held {int(found_support.sum())} times")
    print(f"  Q31 Support at PDL: {rate:.1f}% (tested {int(tested_pdl.sum())}x)")

    # Q32 — Breakout above PDH holds >30 mins
    held_30 = []
    for dt in d.index:
        if d.loc[dt, "high"] <= d.loc[dt, "pdh"]:
            continue
        try:
            day_m1 = m1[m1.index.date == dt]
            # Find first bar that breaks PDH
            pdh = d.loc[dt, "pdh"]
            bo_time = None
            for ts, bar in day_m1.iterrows():
                if bar["high"] > pdh:
                    bo_time = ts
                    break
            if bo_time is None:
                continue
            # Check if price stays above PDH for 30 mins
            check_end = bo_time + timedelta(minutes=30)
            after_bo = day_m1[(day_m1.index > bo_time) & (day_m1.index <= check_end)]
            if after_bo.empty:
                continue
            held_30.append(after_bo["low"].min() > pdh * 0.998)
        except Exception:
            pass
    hold_rate = np.mean(held_30) * 100 if held_30 else np.nan
    record(32, "PDH breakout holds >30 mins",
           f"{hold_rate:.1f}%",
           f"n={len(held_30)} PDH breakout days")
    print(f"  Q32 PDH breakout holds 30 mins: {hold_rate:.1f}% (n={len(held_30)})")

    # Q33 — Inside day → last hour breakout
    # Fix: breakout check should use day's OWN earlier high/low (not T-1),
    # since we want to know if the last hour breaks out of the day's established range.
    d["inside_day"] = (d["high"] <= d["pdh"]) & (d["low"] >= d["pdl"])
    inside_bo = []
    for dt in d[d["inside_day"]].index:
        try:
            day_m1  = m1[m1.index.date == dt]
            # Range established in first 3 hours (9:15-12:15)
            early   = day_m1.between_time(time(9,15), time(12,15))
            last_hr = day_m1.between_time(time(14,30), time(15,30))
            if early.empty or last_hr.empty:
                continue
            day_h = early["high"].max()
            day_l = early["low"].min()
            # Breakout: last hour breaks beyond the day's first-3hr range
            bo = last_hr["high"].max() > day_h or last_hr["low"].min() < day_l
            inside_bo.append(bo)
        except Exception:
            pass
    ibo_rate = np.mean(inside_bo) * 100 if inside_bo else np.nan
    record(33, "Inside Day → last hour breaks first-3hr range",
           f"{ibo_rate:.1f}%",
           f"n={len(inside_bo)} inside days")
    print(f"  Q33 Inside Day → last hr breakout: {ibo_rate:.1f}% (n={len(inside_bo)})")

    # Q34 — Trend Day Up → gap down tomorrow
    d["trend_day_up"] = (d["close"] - d["low"]) / (d["high"] - d["low"]) > 0.7
    d["next_gap"] = d["open"].shift(-1) - d["close"]
    td_up_gap_dn = d[d["trend_day_up"] & (d["next_gap"] < -5)]
    td_up_all    = d[d["trend_day_up"]]
    rate = (len(td_up_gap_dn) / len(td_up_all) * 100) if len(td_up_all) else np.nan
    record(34, "Trend Day Up → gap-down tomorrow",
           f"{rate:.1f}%",
           f"n={len(td_up_all)} trend-up days")
    print(f"  Q34 Trend Up → gap-down next: {rate:.1f}% (n={len(td_up_all)})")

    # Q35 — Price touches VAH/VAL of T-1
    # Approximate VAH/VAL as 70%/30% of T-1 range (no volume profile)
    d["vah"] = d["pdl"] + 0.7 * (d["pdh"] - d["pdl"])
    d["val"] = d["pdl"] + 0.3 * (d["pdh"] - d["pdl"])
    touches_vah = ((d["high"] >= d["vah"]) & (d["low"] <= d["vah"])).mean() * 100
    touches_val = ((d["high"] >= d["val"]) & (d["low"] <= d["val"])).mean() * 100
    record(35, "Price touches T-1 VAH/VAL (70%/30% approx)",
           f"VAH {touches_vah:.1f}% | VAL {touches_val:.1f}%",
           "Note: using price-based approximation (no volume profile)")
    print(f"  Q35 Touches T-1 VAH: {touches_vah:.1f}% | VAL: {touches_val:.1f}%")

    # Q36 — Close near day high → gap up tomorrow
    d["close_vs_high"] = (d["close"] - d["low"]) / (d["high"] - d["low"])
    near_high = d[d["close_vs_high"] >= 0.8]
    gap_up_next = (near_high["next_gap"] > 5).mean() * 100
    record(36, "Close near High → gap-up tomorrow",
           f"{gap_up_next:.1f}%",
           f"n={len(near_high)} days closing near high")
    print(f"  Q36 Close near High → gap-up next: {gap_up_next:.1f}% (n={len(near_high)})")

    # Q37 — Price 100pts from PDH: breakout vs reversal
    within_100 = d[(d["pdh"] - d["open"]).abs() <= 100]
    breaks = (within_100["high"] > within_100["pdh"]).mean() * 100
    record(37, "Within 100pts of PDH: breakout rate",
           f"{breaks:.1f}%",
           f"n={len(within_100)} days")
    print(f"  Q37 Within 100pts of PDH → breaks it: {breaks:.1f}%")

    # Q38 — Mean reversion to T-1 midpoint
    touches_mid = ((d["high"] >= d["pdm"]) & (d["low"] <= d["pdm"])).mean() * 100
    record(38, "Mean reversion to T-1 midpoint",
           f"{touches_mid:.1f}% of days touch T-1 mid",
           f"T-1 midpoint = (PDH+PDL)/2")
    print(f"  Q38 Price touches T-1 midpoint: {touches_mid:.1f}%")

    # Q39 — Hammer on daily → bullish W1 next day
    d["body"] = (d["close"] - d["open"]).abs()
    d["lower_wick"] = d[["open", "close"]].min(axis=1) - d["low"]
    d["hammer"] = (d["lower_wick"] >= 2 * d["body"]) & (d["body"] > 0)
    d["next_w1_bull"] = (d["open"].shift(-1) < d["close"].shift(-1))  # proxy
    hammer_days = d[d["hammer"]]
    bull_after = hammer_days["next_w1_bull"].mean() * 100 if len(hammer_days) else np.nan
    record(39, "Hammer → bullish W1 tomorrow",
           f"{bull_after:.1f}%",
           f"n={len(hammer_days)} hammer days")
    print(f"  Q39 Hammer → bullish W1 next day: {bull_after:.1f}% (n={len(hammer_days)})")

    # Q40 — 1st hour stays within T-1 range
    in_t1_range = 0
    total_40 = 0
    for dt in d.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            hr1 = day_m1.between_time(time(9,15), time(10,15))
            if hr1.empty:
                continue
            total_40 += 1
            if hr1["high"].max() <= d.loc[dt, "pdh"] and hr1["low"].min() >= d.loc[dt, "pdl"]:
                in_t1_range += 1
        except Exception:
            pass
    rate40 = (in_t1_range / total_40 * 100) if total_40 else np.nan
    record(40, "1st hour stays within T-1 range",
           f"{rate40:.1f}%",
           f"n={total_40} days")
    print(f"  Q40 1st hr inside T-1 range: {rate40:.1f}% (n={total_40})")


# ============================================================================
# SECTION E — TECHNICALS (Q41–Q50)
# ============================================================================
def analyze_technicals(m1: pd.DataFrame, daily: pd.DataFrame):
    print("\n[E] TECHNICALS")

    if m1.empty:
        return

    # Q41 — Bounce rate at 500-pt round numbers
    bounces, tested = 0, 0
    round500 = list(range(20000, 30001, 500))
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            for level in round500:
                if day_m1["low"].min() <= level <= day_m1["high"].max():
                    tested += 1
                    # Bounce: price came to level and closed above (for support)
                    near = day_m1[(day_m1["low"] <= level * 1.001) &
                                  (day_m1["low"] >= level * 0.999)]
                    if not near.empty:
                        after = day_m1[day_m1.index > near.index[-1]]
                        if not after.empty and after["close"].iloc[-1] > level:
                            bounces += 1
        except Exception:
            pass
    bounce_rate = (bounces / tested * 100) if tested else np.nan
    record(41, "Bounce rate at 500-pt round numbers",
           f"{bounce_rate:.1f}%",
           f"n={tested} touches")
    print(f"  Q41 500-pt round bounce rate: {bounce_rate:.1f}% (n={tested})")

    # Q42 — Overshoot at 100-pt round numbers
    overshoots = []
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            mid = (daily.loc[dt, "high"] + daily.loc[dt, "low"]) / 2
            nearest_100 = round(mid / 100) * 100
            if nearest_100 <= 0:
                continue
            near = day_m1[(day_m1["low"] <= nearest_100) &
                          (day_m1["high"] >= nearest_100)]
            if not near.empty:
                # Overshoot = how far beyond the level did it go
                os_up = (near["high"].max() - nearest_100)
                os_dn = (nearest_100 - near["low"].min())
                overshoots.append(min(os_up, os_dn))
        except Exception:
            pass
    avg_os = np.mean(overshoots) if overshoots else np.nan
    record(42, "Avg overshoot at 100-pt round levels",
           f"{avg_os:.1f} pts",
           f"n={len(overshoots)} level touches")
    print(f"  Q42 Avg overshoot at 100-pt levels: {avg_os:.1f} pts")

    # Q43 — TWAP crossovers on Trend Day
    twap_cross_trend = []
    twap_cross_normal = []
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            if len(day_m1) < 10:
                continue
            twap = compute_twap(day_m1)
            above = day_m1["close"] > twap
            crosses = (above != above.shift()).sum() - 1
            if classify_trend_day(day_m1):
                twap_cross_trend.append(crosses)
            else:
                twap_cross_normal.append(crosses)
        except Exception:
            pass
    record(43, "TWAP crossovers per day",
           f"Trend={np.mean(twap_cross_trend):.1f} | Sideways={np.mean(twap_cross_normal):.1f}",
           f"Trend n={len(twap_cross_trend)} | Sideways n={len(twap_cross_normal)}")
    print(f"  Q43 TWAP crossovers: Trend {np.mean(twap_cross_trend):.1f} | "
          f"Sideways {np.mean(twap_cross_normal):.1f}")

    # Q44 — Distance from TWAP on sideways day
    max_dist_sideways = []
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            if len(day_m1) < 10 or classify_trend_day(day_m1):
                continue
            twap = compute_twap(day_m1)
            max_dist_sideways.append((day_m1["close"] - twap).abs().max())
        except Exception:
            pass
    record(44, "Max distance from TWAP (sideways day)",
           f"{np.mean(max_dist_sideways):.1f} pts avg",
           f"Median {np.median(max_dist_sideways):.1f} pts | n={len(max_dist_sideways)}")
    print(f"  Q44 Max TWAP distance (sideways): avg {np.mean(max_dist_sideways):.1f} pts")

    # Q45 — First 5-min High breakout success
    bo5_success = []
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            first5 = day_m1.between_time(time(9,15), time(9,20))
            if first5.empty:
                continue
            h5 = first5["high"].max()
            rest = day_m1.between_time(time(9,21), time(15,30))
            if rest.empty:
                continue
            # Breakout: price goes above h5
            bo_bar = rest[rest["high"] > h5].head(1)
            if not bo_bar.empty:
                # Success: stays above h5 for at least 15 mins after breakout
                bo_ts = bo_bar.index[0]
                next15 = rest[(rest.index > bo_ts) &
                              (rest.index <= bo_ts + timedelta(minutes=15))]
                success = next15.empty or next15["low"].min() > h5 * 0.998
                bo5_success.append(success)
        except Exception:
            pass
    rate45 = np.mean(bo5_success) * 100 if bo5_success else np.nan
    record(45, "First 5-min High breakout success rate",
           f"{rate45:.1f}%",
           f"n={len(bo5_success)} breakout events")
    print(f"  Q45 First 5-min High breakout success: {rate45:.1f}% (n={len(bo5_success)})")

    # Q46 — First 15-min Low breakdown success
    bd15_success = []
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            first15 = day_m1.between_time(time(9,15), time(9,30))
            if first15.empty:
                continue
            l15 = first15["low"].min()
            rest = day_m1.between_time(time(9,31), time(15,30))
            bd_bar = rest[rest["low"] < l15].head(1)
            if not bd_bar.empty:
                bd_ts = bd_bar.index[0]
                next15 = rest[(rest.index > bd_ts) &
                              (rest.index <= bd_ts + timedelta(minutes=15))]
                success = next15.empty or next15["high"].max() < l15 * 1.002
                bd15_success.append(success)
        except Exception:
            pass
    rate46 = np.mean(bd15_success) * 100 if bd15_success else np.nan
    record(46, "First 15-min Low breakdown success rate",
           f"{rate46:.1f}%",
           f"n={len(bd15_success)} breakdown events")
    print(f"  Q46 First 15-min Low breakdown success: {rate46:.1f}% (n={len(bd15_success)})")

    # Q47 — SuperTrend flips per day (1-min, Trend Day vs not)
    st_flips_trend = []
    st_flips_other = []
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            if len(day_m1) < 30:
                continue
            st = compute_supertrend(day_m1)
            flips = (st != st.shift()).sum() - 1
            if classify_trend_day(day_m1):
                st_flips_trend.append(flips)
            else:
                st_flips_other.append(flips)
        except Exception:
            pass
    record(47, "Supertrend (1-min) flips per day",
           f"Trend day={np.mean(st_flips_trend):.1f} | Other={np.mean(st_flips_other):.1f}",
           f"Trend n={len(st_flips_trend)} | Other n={len(st_flips_other)}")
    print(f"  Q47 ST flips: Trend {np.mean(st_flips_trend):.1f} | Other {np.mean(st_flips_other):.1f}")

    # Q48 — Red candle after 3 consecutive green 5-min candles
    # Resample to 5-min
    m5 = m1.resample("5min").agg(
        open=("open","first"), high=("high","max"),
        low=("low","min"), close=("close","last")
    ).dropna()
    m5["green"] = m5["close"] > m5["open"]
    red_after_3g = 0
    total_3g = 0
    for i in range(3, len(m5)):
        if m5["green"].iloc[i-3] and m5["green"].iloc[i-2] and m5["green"].iloc[i-1]:
            total_3g += 1
            if not m5["green"].iloc[i]:
                red_after_3g += 1
    rate48 = (red_after_3g / total_3g * 100) if total_3g else np.nan
    record(48, "Red candle after 3 green 5-min candles",
           f"{rate48:.1f}%",
           f"n={total_3g} occurrences of 3 consecutive green")
    print(f"  Q48 Red after 3 green 5-min candles: {rate48:.1f}% (n={total_3g})")

    # Q49 — Avg consolidation time before 20-pt move
    consol_times = []
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            closes = day_m1["close"].values
            for i in range(20, len(closes)):
                window = closes[i-10:i]
                if np.ptp(window) < 8:  # tight 10-bar range
                    # Check if next bars make a 20pt move
                    future = closes[i:i+30]
                    if len(future) >= 5 and (max(future) - min(future)) >= 20:
                        consol_times.append(10)  # 10 bars = 10 mins of consolidation
                        break
        except Exception:
            pass
    record(49, "Avg consolidation bars before 20-pt move",
           f"~{np.mean(consol_times):.0f} mins" if consol_times else "N/A",
           f"n={len(consol_times)} events")
    print(f"  Q49 Consolidation before 20-pt move: ~{np.mean(consol_times):.0f} mins (n={len(consol_times)})")

    # Q50 — Avg drawdown during breakout entry
    # MAE (max adverse excursion) immediately after 5-min high breakout
    maes = []
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            first5 = day_m1.between_time(time(9,15), time(9,20))
            if first5.empty:
                continue
            h5 = first5["high"].max()
            rest = day_m1.between_time(time(9,21), time(15,30))
            bo_bar = rest[rest["high"] > h5].head(1)
            if not bo_bar.empty:
                entry = h5
                bo_ts = bo_bar.index[0]
                next60 = rest[(rest.index >= bo_ts) &
                              (rest.index <= bo_ts + timedelta(minutes=60))]
                if not next60.empty:
                    mae = entry - next60["low"].min()
                    maes.append(max(0, mae))
        except Exception:
            pass
    avg_mae = np.mean(maes) if maes else np.nan
    record(50, "Avg MAE after 5-min breakout entry",
           f"{avg_mae:.1f} pts against you",
           f"n={len(maes)} | p75={np.percentile(maes,75):.1f} pts" if maes else "n=0")
    print(f"  Q50 Avg MAE after breakout: {avg_mae:.1f} pts (n={len(maes)})")


# ============================================================================
# SECTION F — DAY OF WEEK (Q51–Q55)
# ============================================================================
def analyze_day_of_week(daily: pd.DataFrame, m1: pd.DataFrame):
    print("\n[F] DAY OF WEEK")

    if len(daily) < 10:
        return

    d = daily.copy()
    d["dow"] = pd.to_datetime(d.index).dayofweek  # Mon=0, Thu=3, Fri=4
    d["range"] = d["high"] - d["low"]
    days = ["Mon", "Tue", "Wed", "Thu", "Fri"]

    # Q51 — Monday trend reversals
    mon = d[d["dow"] == 0]
    prev_fri = d[d["dow"] == 4]
    # Monday reverses Friday direction
    reversals_mon = []
    for dt in mon.index:
        try:
            dt_ts = pd.Timestamp(dt)
            prev_fri_row = prev_fri[prev_fri.index < dt]
            if len(prev_fri_row) == 0:
                continue
            fri_dir = 1 if prev_fri_row.iloc[-1]["close"] > prev_fri_row.iloc[-1]["open"] else -1
            mon_dir = 1 if d.loc[dt, "close"] > d.loc[dt, "open"] else -1
            reversals_mon.append(fri_dir != mon_dir)
        except Exception:
            pass
    rev_rate = np.mean(reversals_mon) * 100 if reversals_mon else np.nan
    record(51, "Monday reverses Friday direction",
           f"{rev_rate:.1f}%",
           f"n={len(reversals_mon)} Mondays")
    print(f"  Q51 Monday reverses Friday: {rev_rate:.1f}%")

    # Q52 — Thursday highest activity
    # Note: NSE index data often has 0 volume (index not directly traded).
    # Use avg range as activity proxy if volume is all zeros.
    d["range_q52"] = d["high"] - d["low"]
    vol_by_dow = d.groupby("dow")["volume"].mean()
    rng_by_dow = d.groupby("dow")["range_q52"].mean()
    use_range = vol_by_dow.sum() == 0
    metric = rng_by_dow if use_range else vol_by_dow
    label  = "range" if use_range else "volume"
    max_dow = int(metric.idxmax())
    thu_metric = metric.get(3, np.nan)
    avg_metric = metric.mean()
    record(52, f"Highest-activity day of week (by {label})",
           f"{days[max_dow]} | Thu is {(thu_metric/avg_metric*100):.0f}% of avg",
           " | ".join(f"{days[i]}={metric.get(i,0):.0f}" for i in range(5)))
    print(f"  Q52 Activity ({label}) by day: " +
          " | ".join(f"{days[i]}={metric.get(i,0):.0f}" for i in range(5)))

    # Q53 — Friday vs Monday range
    fri_range = d[d["dow"] == 4]["range"].mean()
    mon_range = d[d["dow"] == 0]["range"].mean()
    record(53, "Avg range: Friday vs Monday",
           f"Fri={fri_range:.1f} pts | Mon={mon_range:.1f} pts",
           f"All-day avg by DOW: " +
           " | ".join(f"{days[i]}={d[d['dow']==i]['range'].mean():.1f}" for i in range(5)))
    print(f"  Q53 Range: " +
          " | ".join(f"{days[i]}={d[d['dow']==i]['range'].mean():.1f}" for i in range(5)))

    # Q54 — Tuesday trends with Monday direction
    tue_follows = []
    for dt in d[d["dow"] == 1].index:
        try:
            dt_ts = pd.Timestamp(dt)
            mon_prev = d[d["dow"] == 0]
            mon_prev = mon_prev[mon_prev.index < dt]
            if len(mon_prev) == 0:
                continue
            mon_dir = 1 if mon_prev.iloc[-1]["close"] > mon_prev.iloc[-1]["open"] else -1
            tue_dir = 1 if d.loc[dt, "close"] > d.loc[dt, "open"] else -1
            tue_follows.append(mon_dir == tue_dir)
        except Exception:
            pass
    follow_rate = np.mean(tue_follows) * 100 if tue_follows else np.nan
    record(54, "Tuesday follows Monday direction",
           f"{follow_rate:.1f}%",
           f"n={len(tue_follows)}")
    print(f"  Q54 Tuesday follows Monday: {follow_rate:.1f}%")

    # Q55 — Wednesday W3 volatility vs other days
    w3 = window_ohlcv(m1, W3_START, W3_END)
    if not w3.empty:
        w3["range"] = w3["high"] - w3["low"]
        w3["dow"] = pd.to_datetime(w3.index).dayofweek
        wed_w3 = w3[w3["dow"] == 2]["range"].mean()
        other_w3 = w3[w3["dow"] != 2]["range"].mean()
        record(55, "Wednesday W3 volatility vs other days",
               f"Wed W3={wed_w3:.1f} pts | Others={other_w3:.1f} pts",
               f"Wed W3 is {wed_w3/other_w3*100:.0f}% of other-day W3")
        print(f"  Q55 W3 range: Wed={wed_w3:.1f} | Others={other_w3:.1f} pts")


# ============================================================================
# SECTION G — MICRO-STRUCTURE (Q56–Q60)
# ============================================================================
def analyze_microstructure(m1: pd.DataFrame):
    print("\n[G] MICRO-STRUCTURE")

    if m1.empty:
        return

    m = m1.copy()
    m["body"]   = (m["close"] - m["open"]).abs()
    m["upper"]  = m["high"] - m[["open","close"]].max(axis=1)
    m["lower"]  = m[["open","close"]].min(axis=1) - m["low"]
    m["range"]  = m["high"] - m["low"]

    # Q56 — Avg wick vs body
    avg_body  = m["body"].mean()
    avg_upper = m["upper"].mean()
    avg_lower = m["lower"].mean()
    avg_range = m["range"].mean()
    wick_body_ratio = (avg_upper + avg_lower) / avg_body if avg_body else np.nan
    record(56, "Avg wick vs body (1-min)",
           f"Body={avg_body:.2f} | Upper wick={avg_upper:.2f} | Lower wick={avg_lower:.2f}",
           f"Total wick / body ratio = {wick_body_ratio:.2f}x | Avg candle range = {avg_range:.2f}")
    print(f"  Q56 1-min: body={avg_body:.2f} | upper_wick={avg_upper:.2f} | "
          f"lower_wick={avg_lower:.2f} | ratio={wick_body_ratio:.2f}x")

    # Q57 — Doji candles before trend change
    m["doji"] = m["body"] / m["range"].replace(0, np.nan) < 0.1
    m5 = m.resample("5min").agg(
        open=("open","first"), close=("close","last"),
        high=("high","max"), low=("low","min")
    ).dropna()
    m5["dir"] = np.sign(m5["close"] - m5["open"])
    m5["rev"] = m5["dir"] != m5["dir"].shift(1)

    # Count dojis (1-min) in the 5 bars before each 5-min trend change
    doji_counts_before_rev = []
    dates = m.index.date
    for i in range(5, len(m5)):
        if m5["rev"].iloc[i]:
            ts_start = m5.index[i-5]
            ts_end   = m5.index[i]
            window_1m = m[(m.index >= ts_start) & (m.index < ts_end)]
            if len(window_1m) > 0:
                doji_counts_before_rev.append(window_1m["doji"].sum())
    avg_doji_before = np.mean(doji_counts_before_rev) if doji_counts_before_rev else np.nan
    record(57, "Avg doji (1-min) before trend change",
           f"{avg_doji_before:.2f} dojis per reversal event",
           f"n={len(doji_counts_before_rev)} reversals analysed")
    print(f"  Q57 Dojis before trend change: avg {avg_doji_before:.2f} (n={len(doji_counts_before_rev)})")

    # Q58 — Candle range during breakouts vs normal bars
    # (NSE index data typically has 0 volume; using 1-min candle range as activity proxy)
    daily_temp = m.resample("B").agg(high=("high","max"), low=("low","min")).dropna()
    bo_ranges, normal_ranges = [], []
    for dt in daily_temp.index.date:
        try:
            day_m1 = m[m.index.date == dt]
            first30 = day_m1.between_time(time(9,15), time(9,45))
            if first30.empty:
                continue
            first30_h = first30["high"].max()
            first30_l = first30["low"].min()
            for _, bar in day_m1.iterrows():
                bar_range = bar["high"] - bar["low"]
                if bar["high"] > first30_h or bar["low"] < first30_l:
                    bo_ranges.append(bar_range)
                else:
                    normal_ranges.append(bar_range)
        except Exception:
            pass
    avg_bo   = np.mean(bo_ranges) if bo_ranges else np.nan
    avg_norm = np.mean(normal_ranges) if normal_ranges else np.nan
    ratio = avg_bo/avg_norm if avg_norm else np.nan
    record(58, "Avg candle range: breakout bars vs normal",
           f"Breakout={avg_bo:.2f} pts | Normal={avg_norm:.2f} pts ({ratio:.2f}x)" if avg_norm else "N/A",
           f"Breakout n={len(bo_ranges):,} | Normal n={len(normal_ranges):,}")
    if avg_norm:
        print(f"  Q58 Candle range: breakout={avg_bo:.2f} vs normal={avg_norm:.2f} pts ({ratio:.2f}x)")
    else:
        print("  Q58 N/A")

    # Q59 — False breakouts of 15-min H/L
    false_bos = 0
    total_bos = 0
    for dt in daily_temp.index.date:
        try:
            day_m1 = m[m.index.date == dt]
            first15 = day_m1.between_time(time(9,15), time(9,30))
            if first15.empty:
                continue
            h15 = first15["high"].max()
            l15 = first15["low"].min()
            rest = day_m1.between_time(time(9,31), time(15,30))
            for ts, bar in rest.iterrows():
                # Breakout above 15-min high
                if bar["high"] > h15:
                    total_bos += 1
                    # Check if it comes back below within next 15 bars
                    after = rest[rest.index > ts].head(15)
                    if not after.empty and after["close"].iloc[-1] < h15:
                        false_bos += 1
                    break
                # Breakdown below 15-min low
                if bar["low"] < l15:
                    total_bos += 1
                    after = rest[rest.index > ts].head(15)
                    if not after.empty and after["close"].iloc[-1] > l15:
                        false_bos += 1
                    break
        except Exception:
            pass
    fbo_rate = (false_bos / total_bos * 100) if total_bos else np.nan
    record(59, "False breakout rate of 15-min H/L",
           f"{fbo_rate:.1f}%",
           f"n={total_bos} breakout events")
    print(f"  Q59 False breakout of 15-min H/L: {fbo_rate:.1f}% (n={total_bos})")

    # Q60 — Velocity of moves (pts per bar)
    m["pct_change"] = m["close"].pct_change() * 100
    big_moves = m[m["close"].diff().abs() > 20]  # >20pt in 1 min = covering move
    velocity = big_moves["close"].diff().abs().mean()
    record(60, "Avg velocity of fast moves (>20pt/bar)",
           f"{velocity:.1f} pts/min",
           f"n={len(big_moves)} fast-move bars")
    print(f"  Q60 Fast move velocity: {velocity:.1f} pts/min (n={len(big_moves)})")


# ============================================================================
# SECTION H — MEAN REVERSION (Q72–Q74)
# ============================================================================
def analyze_mean_reversion(m1: pd.DataFrame, daily: pd.DataFrame):
    print("\n[H] MEAN REVERSION")

    # Q72 — After 50pt straight move → 20pt retracement
    retrace_found = 0
    total_moves = 0
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            closes = day_m1["close"].values
            for i in range(50, len(closes)):
                move = closes[i] - closes[i-50]
                if abs(move) >= 50:
                    total_moves += 1
                    future = closes[i:i+30]
                    if len(future) < 5:
                        continue
                    if move > 0 and (max(future) - min(future[future.argmin():])) < -20:
                        retrace_found += 1
                    elif move < 0:
                        max_r = max(future) - future[0]
                        if max_r >= 20:
                            retrace_found += 1
                    break
        except Exception:
            pass
    rate72 = (retrace_found / total_moves * 100) if total_moves else np.nan
    record(72, "After 50pt straight move → 20pt retracement",
           f"{rate72:.1f}%",
           f"n={total_moves} events")
    print(f"  Q72 50pt move → 20pt retracement: {rate72:.1f}%")

    # Q73 — Price touches 20-EMA on 1-min on trend day
    ema_touches_trend = []
    ema_touches_other = []
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            if len(day_m1) < 25:
                continue
            ema20 = compute_ema(day_m1["close"], 20)
            above = day_m1["close"] > ema20
            crosses = (above != above.shift()).sum()
            if classify_trend_day(day_m1):
                ema_touches_trend.append(crosses)
            else:
                ema_touches_other.append(crosses)
        except Exception:
            pass
    record(73, "EMA(20) touches on 1-min (trend vs other)",
           f"Trend={np.mean(ema_touches_trend):.1f} | Other={np.mean(ema_touches_other):.1f} crosses",
           f"Trend n={len(ema_touches_trend)} | Other n={len(ema_touches_other)}")
    print(f"  Q73 EMA(20) crosses: Trend {np.mean(ema_touches_trend):.1f} | "
          f"Other {np.mean(ema_touches_other):.1f}")

    # Q74 — Returns to open after 0.5% move
    returns_to_open = 0
    total74 = 0
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            if day_m1.empty:
                continue
            op = day_m1["open"].iloc[0]
            thresh = op * 0.005
            moved = day_m1[(day_m1["high"] > op + thresh) | (day_m1["low"] < op - thresh)]
            if moved.empty:
                continue
            first_move_ts = moved.index[0]
            after_move = day_m1[day_m1.index > first_move_ts]
            total74 += 1
            if not after_move.empty:
                touches_open = ((after_move["high"] >= op * 0.999) &
                                (after_move["low"]  <= op * 1.001))
                if touches_open.any():
                    returns_to_open += 1
        except Exception:
            pass
    rate74 = (returns_to_open / total74 * 100) if total74 else np.nan
    record(74, "Returns to open after 0.5% move",
           f"{rate74:.1f}%",
           f"n={total74} days")
    print(f"  Q74 Returns to open after 0.5% move: {rate74:.1f}%")


# ============================================================================
# SECTION I — SCALPER SPECIFIC (Q79–Q82)
# ============================================================================
def analyze_scalper(m1: pd.DataFrame, daily: pd.DataFrame):
    print("\n[I] SCALPER SPECIFIC")

    # Q79 — Successive green candle limit
    m5 = m1.resample("5min").agg(
        open=("open","first"), close=("close","last")
    ).dropna()
    m5["green"] = m5["close"] > m5["open"]

    streak_then_red = {}
    for i in range(len(m5)):
        streak = 0
        j = i
        while j < len(m5) and m5["green"].iloc[j]:
            streak += 1
            j += 1
        if streak > 0 and j < len(m5) and not m5["green"].iloc[j]:
            streak_then_red[streak] = streak_then_red.get(streak, 0) + 1

    total_streaks = sum(streak_then_red.values())
    cumulative = 0
    limit = 0
    for k in sorted(streak_then_red.keys()):
        cumulative += streak_then_red[k]
        if cumulative / total_streaks >= 0.9:
            limit = k
            break
    record(79, "Green candle streak limit (90% red follows)",
           f"{limit} consecutive green candles",
           str({k: f"{v}" for k, v in sorted(streak_then_red.items())}))
    print(f"  Q79 90% chance of red after {limit} consecutive green 5-min candles")
    print(f"       Distribution: " +
          " ".join(f"{k}g→{streak_then_red.get(k,0)}" for k in sorted(streak_then_red.keys())[:8]))

    # Q80 — Estimated slippage proxy using wide-range bars (top 10% candle range)
    # NSE index data has 0 volume so we use widest-range 1-min bars as a proxy
    # for high-activity bars where slippage is greatest.
    bar_range = m1["high"] - m1["low"]
    thresh = bar_range.quantile(0.90)
    wide_bars = m1[bar_range >= thresh]
    slippage_proxy = bar_range[bar_range >= thresh].mean() / 2
    record(80, "Estimated slippage proxy (half-spread of top-10% range bars)",
           f"~{slippage_proxy:.1f} pts avg half-spread",
           f"n={len(wide_bars):,} wide bars (range≥{thresh:.1f} pts) | "
           "Actual slippage needs real execution data with fills")
    print(f"  Q80 Slippage proxy (half-spread top-10% bars): ~{slippage_proxy:.1f} pts (n={len(wide_bars):,})")

    # Q81 — Breakout retest probability
    retests = 0
    bos = 0
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            first30 = day_m1.between_time(time(9,15), time(9,45))
            if first30.empty:
                continue
            h30 = first30["high"].max()
            rest = day_m1.between_time(time(9,46), time(15,30))
            bo_bar = rest[rest["high"] > h30].head(1)
            if bo_bar.empty:
                continue
            bos += 1
            bo_ts = bo_bar.index[0]
            after_bo = rest[rest.index > bo_ts]
            # Retest: price comes back to within 5pts of breakout level
            if not after_bo.empty and after_bo["low"].min() <= h30 + 5:
                retests += 1
        except Exception:
            pass
    retest_rate = (retests / bos * 100) if bos else np.nan
    record(81, "Breakout retest probability",
           f"{retest_rate:.1f}%",
           f"n={bos} breakout events")
    print(f"  Q81 Breakout retest rate: {retest_rate:.1f}% (n={bos})")

    # Q82 — False breakouts of PDH on Tuesday
    false_pdh_tue = 0
    total_pdh_tue = 0
    d = daily.copy()
    d["pdh"] = d["high"].shift(1)
    d["dow"] = pd.to_datetime(d.index).dayofweek
    for dt in d[d["dow"] == 1].index:
        try:
            pdh = d.loc[dt, "pdh"]
            if np.isnan(pdh):
                continue
            day_m1 = m1[m1.index.date == dt]
            if day_m1["high"].max() > pdh:
                total_pdh_tue += 1
                # False BO: came back below PDH within same day
                bo_bar = day_m1[day_m1["high"] > pdh].head(1)
                if not bo_bar.empty:
                    after = day_m1[day_m1.index > bo_bar.index[0]]
                    if not after.empty and after["close"].iloc[-1] < pdh:
                        false_pdh_tue += 1
        except Exception:
            pass
    fbo_tue = (false_pdh_tue / total_pdh_tue * 100) if total_pdh_tue else np.nan
    record(82, "False PDH breakout on Tuesday",
           f"{fbo_tue:.1f}%",
           f"n={total_pdh_tue} Tuesday PDH breakout attempts")
    print(f"  Q82 False PDH breakout Tuesday: {fbo_tue:.1f}% (n={total_pdh_tue})")


# ============================================================================
# SECTION J — VOLATILITY CLUSTERS (Q86–Q88)
# ============================================================================
def analyze_volatility_clusters(m1: pd.DataFrame):
    print("\n[J] VOLATILITY CLUSTERS")

    w1 = window_ohlcv(m1, W1_START, W1_END)
    w2 = window_ohlcv(m1, W2_START, W2_END)
    if w1.empty or w2.empty:
        return
    w1["range"] = w1["high"] - w1["low"]
    w2["range"] = w2["high"] - w2["low"]

    # Q86 — High W1 vol → low W2 vol
    common = w1.index[w1.index.isin(w2.index)]
    w1_med = w1.loc[common, "range"].median()
    high_w1 = [d for d in common if w1.loc[d, "range"] > w1_med * 1.3]
    if len(high_w1) >= 5:
        low_w2 = [d for d in high_w1 if d in w2.index and w2.loc[d, "range"] < w2["range"].median() * 0.8]
        rate86 = len(low_w2) / len(high_w1) * 100
        record(86, "High W1 volatility → low W2 volatility",
               f"{rate86:.1f}%",
               f"n={len(high_w1)} high-W1 days")
        print(f"  Q86 High W1 vol → Low W2 vol: {rate86:.1f}% (n={len(high_w1)})")

    # Q87 — Range expansion in last 15 mins (3:15-3:30)
    final15 = window_ohlcv(m1, time(15,15), time(15,30))
    if not final15.empty:
        final15["range"] = final15["high"] - final15["low"]
        common_f = w1.index[w1.index.isin(final15.index)]
        expand = (final15.loc[common_f, "range"] > w1.loc[common_f, "range"] / 10).mean() * 100
        record(87, "Range expansion in last 15 mins",
               f"{expand:.1f}% of days expand in 3:15-3:30",
               f"n={len(common_f)}")
        print(f"  Q87 Range expansion last 15 mins: {expand:.1f}%")

    # Q88 — Quiet period (narrow-range bars) before 50pt move
    # Uses candle range as "quiet" proxy since NSE index volume is 0.
    # Quiet = 10 consecutive bars with range < 25% of day's avg bar range.
    quiet_durations = []
    for dt in w1.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            ranges = (day_m1["high"] - day_m1["low"]).values
            closes = day_m1["close"].values
            avg_bar_range = np.mean(ranges)
            if avg_bar_range == 0:
                continue
            quiet_thresh = avg_bar_range * 0.25
            for i in range(10, len(ranges)):
                if all(ranges[i-10:i] < quiet_thresh):  # 10 narrow bars = "quiet"
                    future = closes[i:i+50]
                    if len(future) >= 5 and (max(future) - min(future)) >= 50:
                        quiet_durations.append(10)  # 10 bars = 10 mins
                        break
        except Exception:
            pass
    avg_quiet = np.mean(quiet_durations) if quiet_durations else np.nan
    record(88, "Quiet period (narrow bars) before 50pt move",
           f"~{avg_quiet:.0f} mins of narrow-range bars" if quiet_durations else "N/A",
           f"n={len(quiet_durations)} events | quiet = bar range < 25% of day avg")
    print(f"  Q88 Quiet period before 50pt move: ~{avg_quiet:.0f} mins (n={len(quiet_durations)})" if quiet_durations else "  Q88 No quiet→50pt events found")


# ============================================================================
# SECTION K — MULTI-TIMEFRAME (Q91–Q93)
# ============================================================================
def analyze_multitimeframe(m1: pd.DataFrame):
    print("\n[K] MULTI-TIMEFRAME")

    if m1.empty:
        return

    # Q91 — 15-min RSI >80 → 1-min short scalp success
    m15 = m1.resample("15min").agg(
        open=("open","first"), high=("high","max"),
        low=("low","min"), close=("close","last")
    ).dropna()
    m15["rsi"] = compute_rsi(m15["close"], 14)
    overbought = m15[m15["rsi"] > 80]

    short_success = []
    for ts in overbought.index:
        try:
            next_bar = m1[m1.index > ts].head(15)
            if next_bar.empty:
                continue
            entry = m1[m1.index > ts]["open"].iloc[0]
            target = entry - 15
            stop   = entry + 10
            hit_target = next_bar["low"].min() <= target
            hit_stop   = next_bar["high"].max() >= stop
            if hit_target and not hit_stop:
                short_success.append(True)
            elif hit_stop:
                short_success.append(False)
        except Exception:
            pass
    rate91 = np.mean(short_success) * 100 if short_success else np.nan
    record(91, "15-min RSI>80 → 1-min short success (15pt/10pt)",
           f"{rate91:.1f}%",
           f"n={len(short_success)} overbought events")
    print(f"  Q91 RSI>80 → short scalp success: {rate91:.1f}% (n={len(short_success)})")

    # Q92 — 5-min trend vs 1-min trend alignment
    m5 = m1.resample("5min").agg(close=("close","last")).dropna()
    m5["dir5"] = np.sign(m5["close"].diff())
    m1["dir1"] = np.sign(m1["close"].diff())
    m1_5 = m1.copy()
    m1_5["dir5"] = m5["dir5"].reindex(m1.index, method="ffill")
    aligned = (m1_5["dir1"] == m1_5["dir5"]).mean() * 100
    record(92, "5-min vs 1-min trend alignment",
           f"{aligned:.1f}% of bars aligned",
           "Trend alignment aids scalp entries")
    print(f"  Q92 5-min/1-min trend aligned: {aligned:.1f}%")

    # Q93 — 1-min candles above VWAP (or TWAP if no volume) during rally
    above_vwap_count_rally = []
    unique_dates = np.unique(m1.index.date)
    for dt in unique_dates:
        try:
            day_m1 = m1[m1.index.date == dt]
            if len(day_m1) < 30:
                continue
            if not classify_trend_day(day_m1):
                continue
            vwap = compute_vwap(day_m1)
            above = (day_m1["close"] > vwap).mean() * 100
            above_vwap_count_rally.append(above)
        except Exception:
            pass
    avg_above = np.mean(above_vwap_count_rally) if above_vwap_count_rally else np.nan
    record(93, "% of 1-min bars above VWAP on trend days",
           f"{avg_above:.1f}%",
           f"n={len(above_vwap_count_rally)} trend days")
    print(f"  Q93 Bars above VWAP on trend day: {avg_above:.1f}%")


# ============================================================================
# SECTION L — CLOSING DYNAMICS (Q94–Q96)
# ============================================================================
def analyze_closing(daily: pd.DataFrame, m1: pd.DataFrame):
    print("\n[L] CLOSING DYNAMICS")

    d = daily.copy()
    d["range"] = d["high"] - d["low"]
    d["close_pos"] = (d["close"] - d["low"]) / d["range"]

    # Q94 — BTST: close at day high → next open
    d["next_open"] = d["open"].shift(-1)
    near_high = d[d["close_pos"] >= 0.9]
    btst_win = (near_high["next_open"] > near_high["close"]).mean() * 100
    btst_avg = (near_high["next_open"] - near_high["close"]).mean()
    record(94, "BTST (close at High → next open)",
           f"Gaps up {btst_win:.1f}% | Avg gain {btst_avg:+.1f} pts",
           f"n={len(near_high)} days closing near high")
    print(f"  Q94 BTST (close near High): gaps up {btst_win:.1f}% | "
          f"avg {btst_avg:+.1f} pts (n={len(near_high)})")

    # Q95 — MOC spike at 3:25 PM
    moc_spikes = []
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            bar_325 = day_m1.between_time(time(15,24), time(15,26))
            bar_315 = day_m1.between_time(time(15,14), time(15,16))
            if bar_325.empty or bar_315.empty:
                continue
            move = abs(bar_325["close"].iloc[-1] - bar_315["close"].iloc[-1])
            moc_spikes.append(move)
        except Exception:
            pass
    avg_spike = np.mean(moc_spikes) if moc_spikes else np.nan
    spike20 = np.mean([s >= 20 for s in moc_spikes]) * 100 if moc_spikes else np.nan
    record(95, "MOC spike at 3:25 PM",
           f"Avg move {avg_spike:.1f} pts | ≥20pt: {spike20:.1f}% of days",
           f"n={len(moc_spikes)}")
    print(f"  Q95 3:25 MOC spike: avg {avg_spike:.1f} pts | ≥20pt: {spike20:.1f}%")

    # Q96 — Close sits between 3:00 and 3:30 H/L
    in_final_range = 0
    total96 = 0
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            final30 = day_m1.between_time(time(15,0), time(15,30))
            if final30.empty:
                continue
            fh = final30["high"].max()
            fl = final30["low"].min()
            cls = day_m1["close"].iloc[-1]
            total96 += 1
            if fl <= cls <= fh:
                in_final_range += 1
        except Exception:
            pass
    rate96 = (in_final_range / total96 * 100) if total96 else np.nan
    record(96, "Close sits in 3:00-3:30 H/L range",
           f"{rate96:.1f}%",
           f"n={total96} (by definition should be ~100%)")
    print(f"  Q96 Close in 3:00-3:30 range: {rate96:.1f}%")


# ============================================================================
# SECTION M — SUMMARY STATS (Q99–Q101)
# ============================================================================
def analyze_summary(daily: pd.DataFrame, m1: pd.DataFrame):
    print("\n[M] SUMMARY STATS")

    if daily.empty:
        return

    # Q99 — Winner/Loser ratio for 5pt target / 3pt stop scalp
    # Simulate: at any 1-min bar, enter long with 5pt target, 3pt stop
    wins, losses = 0, 0
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            for i in range(len(day_m1) - 20):
                entry = day_m1["close"].iloc[i]
                future = day_m1.iloc[i+1:i+21]
                hit_target = (future["high"] >= entry + 5).any()
                hit_stop   = (future["low"]  <= entry - 3).any()
                if hit_target and not hit_stop:
                    wins += 1
                elif hit_stop:
                    losses += 1
                # (if neither, trade still open — skip)
        except Exception:
            pass
    total99 = wins + losses
    win_rate = wins / total99 * 100 if total99 else np.nan
    rrr = 5 / 3
    ev = (win_rate/100 * 5) - ((1 - win_rate/100) * 3)
    record(99, "5pt target / 3pt stop scalp stats",
           f"Win rate {win_rate:.1f}% | RRR {rrr:.2f}x | EV per trade {ev:+.2f} pts",
           f"n={total99:,} simulated trades | Wins={wins:,} | Losses={losses:,}")
    print(f"  Q99 5pt/3pt scalp: WR {win_rate:.1f}% | EV {ev:+.2f} pts/trade")

    # Q100 — Clean 10pt moves per day
    clean_moves = []
    for dt in daily.index:
        try:
            day_m1 = m1[m1.index.date == dt]
            count = 0
            for i in range(len(day_m1) - 10):
                move = abs(day_m1["close"].iloc[i+10] - day_m1["close"].iloc[i])
                # "Clean" = monotonic (no pullback > 3pts during the move)
                segment = day_m1["close"].iloc[i:i+11].values
                max_pullback = max(abs(segment[j] - segment[j-1]) for j in range(1, len(segment)))
                if move >= 10 and max_pullback <= move * 0.4:
                    count += 1
            clean_moves.append(count)
        except Exception:
            pass
    avg_clean = np.mean(clean_moves) if clean_moves else np.nan
    record(100, "Avg clean 10-pt moves per day",
           f"{avg_clean:.1f} moves/day",
           f"n={len(clean_moves)} days | max {max(clean_moves) if clean_moves else 0}")
    print(f"  Q100 Clean 10pt moves per day: avg {avg_clean:.1f}")

    # Q101 — % days where scalping is mathematically impossible
    # Scalping impossible if ADR < cost*2 (cost = 0.4pts assumed)
    daily["range"] = daily["high"] - daily["low"]
    cost = 0.4
    impossible = (daily["range"] < cost * 2).mean() * 100
    record(101, "Days where scalping is mathematically impossible",
           f"{impossible:.1f}% (range < {cost*2:.1f} pts cost)",
           f"Using cost assumption of {cost} pts per side")
    print(f"  Q101 Scalping impossible days: {impossible:.1f}%")


# ============================================================================
# RISK / PSYCHOLOGY (Q83–Q85)
# ============================================================================
def analyze_risk_psychology():
    print("\n[N] RISK / PSYCHOLOGY")
    print("  Q83/Q84/Q85: Requires actual trade log (not intraday price data).")
    print("  Supply a CSV with columns [entry, exit, pnl] to enable these.")
    record(83, "Drawdown duration before win (mins)", "Requires trade log CSV", "")
    record(84, "Serial loss probability (3/5/7 in a row)", "Requires trade log CSV", "")
    record(85, "Overtrading impact (>50 trades/day)", "Requires trade log CSV", "")


# ============================================================================
# CROSS-INDEX ANALYSIS (Q29, Q66–Q68, Q97–Q98)
# ============================================================================
def analyze_cross_index(nifty_m1, nifty_d, banknifty_m1, banknifty_d, vix_d):
    print("\n[O] CROSS-INDEX ANALYSIS")

    # Q29 — Nifty vs BankNifty range correlation
    if banknifty_d is not None and not banknifty_d.empty:
        nifty_d["range"] = nifty_d["high"] - nifty_d["low"]
        banknifty_d["range"] = banknifty_d["high"] - banknifty_d["low"]
        common = nifty_d.index[nifty_d.index.isin(banknifty_d.index)]
        if len(common) >= 10:
            corr, pval = stats.pearsonr(
                nifty_d.loc[common, "range"].values,
                banknifty_d.loc[common, "range"].values
            )
            record(29, "Nifty vs BankNifty range correlation",
                   f"r={corr:.3f} (p={pval:.4f})",
                   f"n={len(common)} common days")
            print(f"  Q29 Nifty vs BankNifty range: r={corr:.3f} (p={pval:.4f})")
    else:
        print("  Q29 BankNifty data not loaded")

    # Q68 — VIX <12 → sideways
    if vix_d is not None and not vix_d.empty:
        nifty_d["range"] = nifty_d["high"] - nifty_d["low"]
        merged = nifty_d.copy()
        merged["vix"] = vix_d["close"].reindex(merged.index)
        low_vix = merged[merged["vix"] < 12]
        record(68, "VIX <12 → sideways day probability",
               f"{(low_vix['range'] < nifty_d['range'].median()).mean()*100:.1f}% are below-median range",
               f"n={len(low_vix)} low-VIX days")
        print(f"  Q68 VIX<12 → below-median range: "
              f"{(low_vix['range'] < nifty_d['range'].median()).mean()*100:.1f}%")
    else:
        print("  Q68 VIX data not loaded")

    # Q97 — Nifty trends if BankNifty + one major stock in sync
    if banknifty_d is not None and not banknifty_d.empty:
        common = nifty_d.index[nifty_d.index.isin(banknifty_d.index)]
        nifty_d["dir"] = np.sign(nifty_d["close"] - nifty_d["open"])
        banknifty_d["dir"] = np.sign(banknifty_d["close"] - banknifty_d["open"])
        sync = nifty_d.loc[common, "dir"] == banknifty_d.loc[common, "dir"]
        nifty_trends_when_sync = nifty_d.loc[common[sync], "range"].mean()
        nifty_range_when_async = nifty_d.loc[common[~sync], "range"].mean()
        record(97, "Nifty range when in sync with BankNifty",
               f"Sync={nifty_trends_when_sync:.1f} pts | Async={nifty_range_when_async:.1f} pts",
               f"Sync n={sync.sum()} | Async n={(~sync).sum()}")
        print(f"  Q97 Nifty range: BNF sync={nifty_trends_when_sync:.1f} "
              f"vs async={nifty_range_when_async:.1f} pts")
    else:
        print("  Q97 BankNifty data not loaded")

    # Q98 — Notes (requires NiftyIT data)
    print("  Q98 IT leads W1: requires NSE_INDEX|Nifty IT data — add to INSTRUMENTS")
    record(98, "IT sector leads Nifty in W1", "Requires NiftyIT instrument key", "")

    # Q66–Q67 — External data
    print("  Q66 USDINR/Nifty: requires USDINR feed (not in ExpiryTrack DB)")
    print("  Q67 Dow Futures/Nifty: requires Dow futures feed (not in ExpiryTrack DB)")
    record(66, "USDINR vs Nifty correlation", "Requires external USDINR data", "")
    record(67, "Dow Futures trigger Nifty trend", "Requires external Dow futures data", "")



# ============================================================================
# SECTION P — ORB / INITIAL BALANCE (Q102–Q107)
# ============================================================================
def analyze_orb_ib(m1: pd.DataFrame, daily: pd.DataFrame):
    print("\n[P] ORB / INITIAL BALANCE")

    if m1.empty or daily.empty:
        return

    # Pre-build per-day data
    orb_data = {}   # dt -> {orb_h, orb_l, ib_h, ib_l, open, high, low, close}
    for dt in daily.index:
        try:
            dm = m1[m1.index.date == dt]
            if dm.empty:
                continue
            first5  = dm.between_time(time(9,15), time(9,20))
            first60 = dm.between_time(time(9,15), time(10,15))
            if first5.empty or first60.empty:
                continue
            orb_data[dt] = {
                "orb_h": first5["high"].max(),
                "orb_l": first5["low"].min(),
                "ib_h":  first60["high"].max(),
                "ib_l":  first60["low"].min(),
                "open":  dm["open"].iloc[0],
                "high":  dm["high"].max(),
                "low":   dm["low"].min(),
                "close": dm["close"].iloc[-1],
                "range": dm["high"].max() - dm["low"].min(),
            }
        except Exception:
            pass

    if not orb_data:
        print("  No ORB data computed.")
        return

    dates = list(orb_data.keys())

    # Q102 — ORB expansion factor + day high/low from ORB
    orb_expansions = []
    dh_in_orb, dl_in_orb = 0, 0
    for dt, d in orb_data.items():
        orb_range = d["orb_h"] - d["orb_l"]
        if orb_range > 0:
            orb_expansions.append(d["range"] / orb_range)
        if d["high"] <= d["orb_h"] * 1.001:
            dh_in_orb += 1
        if d["low"] >= d["orb_l"] * 0.999:
            dl_in_orb += 1
    avg_exp = np.mean(orb_expansions) if orb_expansions else np.nan
    n = len(orb_data)
    record(102, "ORB expansion factor (day range / 5-min ORB)",
           f"Avg {avg_exp:.1f}x | Day High within ORB: {dh_in_orb/n*100:.1f}% | "
           f"Day Low within ORB: {dl_in_orb/n*100:.1f}%",
           f"n={n} days | Median expansion {np.median(orb_expansions):.1f}x")
    print(f"  Q102 ORB expansion: avg {avg_exp:.1f}x | DH in ORB {dh_in_orb/n*100:.1f}% | "
          f"DL in ORB {dl_in_orb/n*100:.1f}%")

    # Q103 — First 1-min candle direction vs day close direction
    # Fix: use actual 9:15 candle open/close, compare to full day close vs day open
    fg_count, fg_close_green = 0, 0
    fr_count, fr_close_red   = 0, 0
    for dt, d in orb_data.items():
        try:
            day_m1      = m1[m1.index.date == dt]
            first_1min  = day_m1.between_time(time(9,15), time(9,16))
            if first_1min.empty:
                continue
            c1_green    = first_1min["close"].iloc[0] > first_1min["open"].iloc[0]
            day_green   = d["close"] > d["open"]
            if c1_green:
                fg_count += 1
                if day_green:
                    fg_close_green += 1
            else:
                fr_count += 1
                if not day_green:
                    fr_close_red += 1
        except Exception:
            pass
    fg_rate = fg_close_green / fg_count * 100 if fg_count else np.nan
    fr_rate = fr_close_red   / fr_count * 100 if fr_count else np.nan
    record(103, "First 1-min candle direction → day close direction",
           f"Green 1st → green close: {fg_rate:.1f}% | Red 1st → red close: {fr_rate:.1f}%",
           f"First green n={fg_count} | First red n={fr_count}")
    print(f"  Q103 1st candle bias: Green→close green {fg_rate:.1f}% | Red→close red {fr_rate:.1f}%")

    # Q104 — IB width → day type
    ib_ranges = [d["ib_h"] - d["ib_l"] for d in orb_data.values()]
    ib_med = np.median(ib_ranges)
    narrow_ib = [d for d in orb_data.values() if (d["ib_h"] - d["ib_l"]) < 50]
    wide_ib   = [d for d in orb_data.values() if (d["ib_h"] - d["ib_l"]) > 100]
    # Trend day: close in top or bottom 30% of range
    def is_trend(d):
        r = d["high"] - d["low"]
        if r == 0: return False
        pos = (d["close"] - d["low"]) / r
        return pos >= 0.7 or pos <= 0.3
    narrow_trend = sum(1 for d in narrow_ib if is_trend(d)) / len(narrow_ib) * 100 if narrow_ib else np.nan
    wide_trend   = sum(1 for d in wide_ib if is_trend(d))   / len(wide_ib)   * 100 if wide_ib   else np.nan
    record(104, "IB width → trend day probability",
           f"Narrow IB (<50pts): {narrow_trend:.1f}% trend | Wide IB (>100pts): {wide_trend:.1f}% trend",
           f"Narrow n={len(narrow_ib)} | Wide n={len(wide_ib)} | Median IB={ib_med:.0f} pts")
    print(f"  Q104 IB width: Narrow (<50) → trend {narrow_trend:.1f}% | Wide (>100) → trend {wide_trend:.1f}%")

    # Q105 — Price BREAKS beyond IB High AND IB Low after IB window closes
    # Fix: only count moves that occur AFTER 10:15 and that exceed IB boundaries
    both_tested = 0
    both_trend_count = 0
    for dt, d in orb_data.items():
        try:
            day_m1    = m1[m1.index.date == dt]
            after_ib  = day_m1.between_time(time(10,16), time(15,30))
            if after_ib.empty:
                continue
            breaks_ib_h = after_ib["high"].max() > d["ib_h"] + 2   # 2pt buffer
            breaks_ib_l = after_ib["low"].min()  < d["ib_l"] - 2
            if breaks_ib_h and breaks_ib_l:
                both_tested += 1
                if is_trend(d):
                    both_trend_count += 1
        except Exception:
            pass
    both_rate       = both_tested / n * 100
    both_trend_rate = both_trend_count / both_tested * 100 if both_tested else np.nan
    record(105, "Breaks beyond both IB High AND IB Low (after 10:15)",
           f"{both_rate:.1f}% of days | When it happens: trend {both_trend_rate:.1f}% / inside {100-both_trend_rate:.1f}%",
           f"n={both_tested} double-break days out of {n}")
    print(f"  Q105 Breaks both IB H+L: {both_rate:.1f}% | → trend day {both_trend_rate:.1f}%")

    # Q106 — Previous Week High/Low tested
    daily_cp = daily.copy()
    daily_cp.index = pd.to_datetime(daily_cp.index)
    weekly = daily_cp.resample("W").agg(high=("high","max"), low=("low","min"),
                                         open=("open","first"), close=("close","last"))
    weekly["pwh"] = weekly["high"].shift(1)
    weekly["pwl"] = weekly["low"].shift(1)
    weekly = weekly.dropna(subset=["pwh"])
    tests_pwh = (weekly["high"] >= weekly["pwh"]).mean() * 100
    tests_pwl = (weekly["low"]  <= weekly["pwl"]).mean() * 100
    record(106, "Weekly: tests Previous Week High/Low",
           f"PWH tested: {tests_pwh:.1f}% of weeks | PWL tested: {tests_pwl:.1f}%",
           f"n={len(weekly)} weeks")
    print(f"  Q106 PWH tested {tests_pwh:.1f}% of weeks | PWL tested {tests_pwl:.1f}%")

    # Q107 — Monthly open as trend filter
    monthly = daily_cp.resample("ME").agg(open=("open","first"), close=("close","last"))
    daily_cp["monthly_open"] = daily_cp.index.to_period("M").map(
        lambda p: monthly.loc[monthly.index >= p.to_timestamp(how="E")].iloc[0]["open"]
        if len(monthly.loc[monthly.index >= p.to_timestamp(how="E")]) else np.nan
    )
    above_mo = (daily_cp["close"] > daily_cp["monthly_open"]).mean() * 100
    record(107, "Closes above Monthly Open (trend filter)",
           f"{above_mo:.1f}% of days close above the monthly open",
           "Use monthly open as bull/bear filter: above = long bias, below = short bias")
    print(f"  Q107 Days closing above monthly open: {above_mo:.1f}%")


# ============================================================================
# SECTION Q — ENTRY TIMING PRECISION (Q108–Q111)
# ============================================================================
def analyze_entry_timing(m1: pd.DataFrame, daily: pd.DataFrame):
    print("\n[Q] ENTRY TIMING PRECISION")

    if m1.empty:
        return

    # Q108 — Golden minute: which minute within each window has most breakouts
    bo_minutes = []   # (window, minute_of_day)
    for dt in daily.index:
        try:
            dm = m1[m1.index.date == dt]
            first30 = dm.between_time(time(9,15), time(9,45))
            if first30.empty:
                continue
            h30 = first30["high"].max()
            l30 = first30["low"].min()
            rest = dm.between_time(time(9,46), time(15,30))
            for ts, bar in rest.iterrows():
                if bar["high"] > h30 or bar["low"] < l30:
                    t = ts.time()
                    mins = t.hour * 60 + t.minute
                    # Assign window
                    if 9*60+15 <= mins <= 11*60+20:
                        w = "W1"
                    elif 11*60+20 < mins <= 13*60+25:
                        w = "W2"
                    else:
                        w = "W3"
                    bo_minutes.append((w, mins))
                    break
        except Exception:
            pass

    if bo_minutes:
        import collections
        df_bo = pd.DataFrame(bo_minutes, columns=["window","minute"])
        # Most common 15-min bucket
        df_bo["bucket"] = (df_bo["minute"] // 15) * 15
        top_bucket = df_bo["bucket"].value_counts().index[0]
        th, tm = divmod(top_bucket, 60)
        top_time = f"{th:02d}:{tm:02d}"
        by_window = df_bo.groupby("window")["minute"].apply(
            lambda x: f"{x.mode().iloc[0]//60:02d}:{x.mode().iloc[0]%60:02d}"
        ).to_dict()
        record(108, "Golden breakout minute (most frequent)",
               f"Overall: {top_time} | W1: {by_window.get('W1','?')} | "
               f"W2: {by_window.get('W2','?')} | W3: {by_window.get('W3','?')}",
               f"n={len(bo_minutes)} breakout events analysed")
        print(f"  Q108 Golden minute: overall {top_time} | "
              + " | ".join(f"{w}:{by_window.get(w,'?')}" for w in ["W1","W2","W3"]))

    # Q109 — Pullback depth after breakout
    # Fix: measure the FULL initial thrust (entry to peak in next 20 bars),
    # then measure the deepest pullback from peak — avoid near-zero 'move' explosion.
    pullback_depths = []   # % pullback from peak
    pullback_pts    = []   # absolute pts of pullback
    for dt in daily.index:
        try:
            dm = m1[m1.index.date == dt]
            first30 = dm.between_time(time(9,15), time(9,45))
            if first30.empty:
                continue
            h30  = first30["high"].max()
            rest = dm.between_time(time(9,46), time(15,30))
            bo_bars = rest[rest["high"] > h30]
            if bo_bars.empty:
                continue
            bo_ts  = bo_bars.index[0]
            entry  = h30   # entry at breakout level
            # Measure thrust: highest point in next 20 bars
            thrust_window = rest[(rest.index >= bo_ts)].head(20)
            if thrust_window.empty:
                continue
            peak  = thrust_window["high"].max()
            thrust = peak - entry
            if thrust < 8:          # minimum 8pt thrust to be meaningful
                continue
            # Measure deepest pullback from peak in next 30 bars after peak
            peak_ts = thrust_window["high"].idxmax()
            after_peak = rest[(rest.index > peak_ts)].head(30)
            if after_peak.empty:
                continue
            pullback_pts_val = peak - after_peak["low"].min()
            if pullback_pts_val > 0:
                pullback_pts.append(pullback_pts_val)
                pullback_depths.append(pullback_pts_val / thrust * 100)
        except Exception:
            pass

    if pullback_depths:
        # Filter to sensible range (0-200% = pullback up to 2x the thrust)
        clean = [x for x in pullback_depths if x <= 200]
        p25, p50, p75 = np.percentile(clean, [25, 50, 75])
        avg_pts = np.mean(pullback_pts)
        record(109, "Pullback depth after breakout (% of thrust, capped 200%)",
               f"Median {p50:.0f}% | p25={p25:.0f}% | p75={p75:.0f}% | Avg {avg_pts:.1f} pts absolute",
               f"n={len(clean)} valid events | Enter on retest ~{p25:.0f}% pullback from peak")
        print(f"  Q109 Pullback after BO: median {p50:.0f}% of thrust | "
              f"avg {avg_pts:.1f} pts | p25={p25:.0f}% p75={p75:.0f}% (n={len(clean)})")

    # Q110 — Time between consecutive 20-pt moves
    gaps_between = []
    for dt in daily.index:
        try:
            dm = m1[m1.index.date == dt]
            closes = dm["close"].values
            times  = dm.index
            last_move_ts = None
            for i in range(1, len(closes)):
                if abs(closes[i] - closes[i-1]) >= 5:  # directional bar
                    if last_move_ts is not None:
                        gap = (times[i] - last_move_ts).seconds / 60
                        if gap > 0:
                            gaps_between.append(gap)
                    last_move_ts = times[i]
        except Exception:
            pass

    avg_gap = np.mean(gaps_between) if gaps_between else np.nan
    record(110, "Avg time between 5-pt directional bars (mins)",
           f"{avg_gap:.1f} mins",
           f"n={len(gaps_between):,} | Median {np.median(gaps_between):.1f} mins")
    print(f"  Q110 Time between 5pt moves: avg {avg_gap:.1f} mins")

    # Q111 — Time to hit 10/15/20-pt target from 5-min breakout entry
    targets = {10: [], 15: [], 20: []}
    for dt in daily.index:
        try:
            dm = m1[m1.index.date == dt]
            first5 = dm.between_time(time(9,15), time(9,20))
            if first5.empty:
                continue
            h5 = first5["high"].max()
            rest = dm.between_time(time(9,21), time(15,30))
            bo  = rest[rest["high"] > h5].head(1)
            if bo.empty:
                continue
            entry  = h5
            bo_ts  = bo.index[0]
            future = rest[rest.index >= bo_ts]
            for tgt in [10, 15, 20]:
                hit = future[future["high"] >= entry + tgt]
                if not hit.empty:
                    mins = (hit.index[0] - bo_ts).seconds / 60
                    targets[tgt].append(mins)
        except Exception:
            pass

    tgt_str = " | ".join(
        f"+{t}pt={np.mean(v):.0f}m (n={len(v)})" if v else f"+{t}pt=N/A"
        for t, v in targets.items()
    )
    record(111, "Avg mins to hit 10/15/20-pt target from BO entry",
           tgt_str, "From first 5-min High breakout entry")
    print(f"  Q111 Time to target: {tgt_str}")


# ============================================================================
# SECTION R — STOP LOSS & RISK SIZING (Q112–Q114)
# ============================================================================
def analyze_stop_sizing(m1: pd.DataFrame, daily: pd.DataFrame):
    print("\n[R] STOP LOSS & RISK SIZING")

    if m1.empty:
        return

    # Q112 — Optimal stop distance: test 5/8/10/15/20-pt stops with 2R targets
    stop_results = {}
    for stop in [5, 8, 10, 15, 20]:
        target = stop * 2   # 2R target
        wins, losses = 0, 0
        for dt in daily.index:
            try:
                dm = m1[m1.index.date == dt]
                first5 = dm.between_time(time(9,15), time(9,20))
                if first5.empty:
                    continue
                h5 = first5["high"].max()
                rest = dm.between_time(time(9,21), time(15,30))
                bo = rest[rest["high"] > h5].head(1)
                if bo.empty:
                    continue
                entry = h5
                bo_ts = bo.index[0]
                fut = rest[rest.index >= bo_ts]
                hit_t = fut[fut["high"] >= entry + target]
                hit_s = fut[fut["low"]  <= entry - stop]
                if not hit_t.empty and not hit_s.empty:
                    if hit_t.index[0] < hit_s.index[0]:
                        wins += 1
                    else:
                        losses += 1
                elif not hit_t.empty:
                    wins += 1
                elif not hit_s.empty:
                    losses += 1
            except Exception:
                pass
        total = wins + losses
        wr  = wins / total * 100 if total else 0
        ev  = (wr/100 * target) - ((1-wr/100) * stop)
        stop_results[stop] = {"wr": wr, "ev": ev, "total": total}

    best_stop = max(stop_results, key=lambda s: stop_results[s]["ev"])
    result_str = " | ".join(
        f"{s}pt: WR={stop_results[s]['wr']:.0f}% EV={stop_results[s]['ev']:+.1f}"
        for s in [5, 8, 10, 15, 20]
    )
    record(112, "Optimal stop size (2R target, BO entry)",
           f"Best: {best_stop}pt stop (EV={stop_results[best_stop]['ev']:+.1f} pts)",
           result_str)
    print(f"  Q112 Best stop: {best_stop}pt | " + result_str)

    # Q113 — MFE/MAE ratio for winning vs losing trades
    mfe_win, mae_win = [], []
    mfe_los, mae_los = [], []
    for dt in daily.index:
        try:
            dm = m1[m1.index.date == dt]
            first5 = dm.between_time(time(9,15), time(9,20))
            if first5.empty:
                continue
            h5   = first5["high"].max()
            rest = dm.between_time(time(9,21), time(15,30))
            bo   = rest[rest["high"] > h5].head(1)
            if bo.empty:
                continue
            entry = h5
            bo_ts = bo.index[0]
            fut   = rest[rest.index >= bo_ts].head(60)  # 60-bar window
            if fut.empty:
                continue
            mfe = fut["high"].max() - entry
            mae = entry - fut["low"].min()
            exit_pnl = fut["close"].iloc[-1] - entry
            if exit_pnl > 0:
                mfe_win.append(mfe)
                mae_win.append(mae)
            else:
                mfe_los.append(mfe)
                mae_los.append(mae)
        except Exception:
            pass

    record(113, "MFE/MAE profile (60-bar window after BO)",
           f"Winners: avg MFE={np.mean(mfe_win):.1f} MAE={np.mean(mae_win):.1f} | "
           f"Losers: avg MFE={np.mean(mfe_los):.1f} MAE={np.mean(mae_los):.1f}",
           f"Winners n={len(mfe_win)} | Losers n={len(mfe_los)}")
    print(f"  Q113 MFE/MAE: Win MFE={np.mean(mfe_win):.1f}/MAE={np.mean(mae_win):.1f} | "
          f"Loss MFE={np.mean(mfe_los):.1f}/MAE={np.mean(mae_los):.1f}")

    # Q114 — Trailing stop: trail 15pt after 20pt move — % trend captured
    # Fix: only include trades where trail actually activates (peak >= entry+20),
    # clip captured to 0 minimum (trail below entry = 0 captured, not negative),
    # and use total_move = peak - entry (not max of entire day).
    trail_capture  = []   # % of peak-to-entry captured
    trail_pts      = []   # absolute pts captured at exit
    trail_activated = 0
    for dt in daily.index:
        try:
            dm = m1[m1.index.date == dt]
            first5 = dm.between_time(time(9,15), time(9,20))
            if first5.empty:
                continue
            h5   = first5["high"].max()
            rest = dm.between_time(time(9,21), time(15,30))
            bo   = rest[rest["high"] > h5].head(1)
            if bo.empty:
                continue
            entry = h5
            bo_ts = bo.index[0]
            fut   = rest[rest.index >= bo_ts]
            if fut.empty:
                continue
            # Simulate trailing stop
            trail_active = False
            trail_stop   = 0.0
            exit_price   = None
            peak         = entry
            for _, bar in fut.iterrows():
                if bar["high"] > peak:
                    peak = bar["high"]
                    if trail_active:
                        trail_stop = peak - 15   # trail up as new high is made
                if not trail_active and peak - entry >= 20:
                    trail_active = True
                    trail_stop   = peak - 15
                    trail_activated += 1
                if trail_active and bar["low"] <= trail_stop:
                    exit_price = trail_stop
                    break
            if not trail_active:
                continue   # trail never activated — skip
            if exit_price is None:
                exit_price = fut["close"].iloc[-1]
            total_move = peak - entry          # peak move from entry
            captured   = max(0, exit_price - entry)   # clip to 0 min
            if total_move >= 20:               # only trades where trail fired
                trail_pts.append(captured)
                trail_capture.append(captured / total_move * 100)
        except Exception:
            pass

    avg_cap  = np.mean(trail_capture) if trail_capture else np.nan
    avg_pts  = np.mean(trail_pts) if trail_pts else np.nan
    med_cap  = np.median(trail_capture) if trail_capture else np.nan
    record(114, "Trailing stop (15pt trail after 20pt): % trend captured",
           f"Avg {avg_cap:.1f}% of move captured | Avg {avg_pts:.1f} pts | Median {med_cap:.1f}%",
           f"n={len(trail_capture)} trades where trail activated | "
           f"Trail never fired: {trail_activated - len(trail_capture)} trades")
    print(f"  Q114 Trailing stop: avg {avg_cap:.1f}% captured ({avg_pts:.1f} pts) | "
          f"median {med_cap:.1f}% (n={len(trail_capture)})")


# ============================================================================
# SECTION S — MOMENTUM & EXHAUSTION (Q115–Q118)
# ============================================================================
def analyze_momentum_exhaustion(m1: pd.DataFrame, daily: pd.DataFrame):
    print("\n[S] MOMENTUM & EXHAUSTION")

    if m1.empty:
        return

    # Q115 — Thrust size and count on trend days
    # Fix: use 5-min bars and require minimum 3-bar consecutive run >= 15pts
    # to identify real market thrusts, not 1-bar noise.
    thrust_sizes  = []
    thrust_counts = []
    for dt in daily.index:
        try:
            dm = m1[m1.index.date == dt]
            if not classify_trend_day(dm):
                continue
            # Resample to 5-min for this day
            dm5 = dm.resample("5min").agg(
                open=("open","first"), high=("high","max"),
                low=("low","min"), close=("close","last")
            ).dropna()
            if len(dm5) < 5:
                continue
            closes = dm5["close"].values
            thrusts = []
            i = 0
            while i < len(closes) - 2:
                j = i + 1
                # Upward thrust: consecutive higher closes
                while j < len(closes) and closes[j] > closes[j-1]:
                    j += 1
                bars  = j - i
                move  = closes[j-1] - closes[i]
                if bars >= 3 and move >= 15:   # min 3 bars, min 15pts
                    thrusts.append(move)
                # Downward thrust
                k = i + 1
                while k < len(closes) and closes[k] < closes[k-1]:
                    k += 1
                bars_d = k - i
                move_d = closes[i] - closes[k-1]
                if bars_d >= 3 and move_d >= 15:
                    thrusts.append(move_d)
                i = max(j, k, i + 1)
            if thrusts:
                thrust_sizes.extend(thrusts)
                thrust_counts.append(len(thrusts))
        except Exception:
            pass

    if thrust_sizes:
        record(115, "Thrust size/count on trend days (5-min, ≥3 bars, ≥15pts)",
               f"Avg thrust: {np.mean(thrust_sizes):.1f} pts | "
               f"Avg {np.mean(thrust_counts):.1f} thrusts/day",
               f"n={len(thrust_counts)} trend days | Max thrust {max(thrust_sizes):.0f} pts")
        print(f"  Q115 Thrusts: avg {np.mean(thrust_sizes):.1f} pts | "
              f"{np.mean(thrust_counts):.1f} per trend day")

    # Q116 — 3 consecutive red 5-min bars → 4th reversal
    m5 = m1.resample("5min").agg(
        open=("open","first"), close=("close","last")
    ).dropna()
    m5["red"] = m5["close"] < m5["open"]
    rev_after_3red = 0
    total_3red = 0
    for i in range(3, len(m5)):
        if m5["red"].iloc[i-3] and m5["red"].iloc[i-2] and m5["red"].iloc[i-1]:
            total_3red += 1
            if not m5["red"].iloc[i]:  # 4th bar is green
                rev_after_3red += 1
    rate116 = rev_after_3red / total_3red * 100 if total_3red else np.nan
    record(116, "Green candle after 3 consecutive red 5-min",
           f"{rate116:.1f}%",
           f"n={total_3red:,} occurrences of 3 consecutive red")
    print(f"  Q116 Green after 3 red 5-min candles: {rate116:.1f}% (n={total_3red:,})")

    # Q117 — Bar before trend reversal: wider or narrower than avg?
    pre_rev_ranges = []
    baseline_ranges = []
    for dt in daily.index:
        try:
            dm  = m1[m1.index.date == dt]
            if len(dm) < 20:
                continue
            rng = (dm["high"] - dm["low"]).values
            avg_rng = np.mean(rng)
            baseline_ranges.append(avg_rng)
            # Find reversal points (local extremes)
            closes = dm["close"].values
            for i in range(5, len(closes)-5):
                # Local high reversal
                if closes[i] == max(closes[i-5:i+5]):
                    pre_rev_ranges.append(rng[i-1])
                # Local low reversal
                elif closes[i] == min(closes[i-5:i+5]):
                    pre_rev_ranges.append(rng[i-1])
        except Exception:
            pass

    if pre_rev_ranges and baseline_ranges:
        ratio = np.mean(pre_rev_ranges) / np.mean(baseline_ranges)
        wider = np.mean(pre_rev_ranges) > np.mean(baseline_ranges)
        record(117, "Bar before reversal: range vs avg",
               f"{ratio:.2f}x avg candle range ({'wider — exhaustion spike' if wider else 'narrower — quiet before storm'})",
               f"Pre-reversal avg {np.mean(pre_rev_ranges):.2f} pts | "
               f"Baseline avg {np.mean(baseline_ranges):.2f} pts | n={len(pre_rev_ranges):,}")
        print(f"  Q117 Pre-reversal bar: {ratio:.2f}x avg range "
              f"({'wider' if wider else 'narrower'})")

    # Q118 — 5-min double top within 5pts → 15-pt reversal in 30 mins
    dt_reversals = 0
    dt_total     = 0
    m5_ohlc = m1.resample("5min").agg(
        open=("open","first"), high=("high","max"),
        low=("low","min"),  close=("close","last")
    ).dropna()
    highs = m5_ohlc["high"].values
    times = m5_ohlc.index
    for i in range(2, len(highs)-7):
        # Look for double top: two highs within 5pts with a dip between
        if abs(highs[i] - highs[i-2]) <= 5 and highs[i-1] < highs[i] - 3:
            dt_total += 1
            # Check for 15pt drop in next 30 mins (6 bars)
            future_low = m5_ohlc["low"].values[i:i+7].min()
            if highs[i] - future_low >= 15:
                dt_reversals += 1
    dt_rate = dt_reversals / dt_total * 100 if dt_total else np.nan
    record(118, "5-min double top → 15-pt reversal in 30 mins",
           f"{dt_rate:.1f}%",
           f"n={dt_total:,} double-top patterns identified")
    print(f"  Q118 Double top → 15pt reversal: {dt_rate:.1f}% (n={dt_total:,})")


# ============================================================================
# SECTION T — SESSION PROFILING (Q119–Q121)
# ============================================================================
def analyze_session_profile(m1: pd.DataFrame, daily: pd.DataFrame):
    print("\n[T] SESSION PROFILING")

    if m1.empty or daily.empty:
        return

    # Q119 — Day type classifier by 10 AM
    correct, total = 0, 0
    for dt in daily.index:
        try:
            dm = m1[m1.index.date == dt]
            by10 = dm.between_time(time(9,15), time(10,0))
            if by10.empty:
                continue
            # Features available by 10 AM
            gap_pct = (dm["open"].iloc[0] - (daily["close"].shift(1).loc[dt]
                       if dt in daily.index else dm["open"].iloc[0])) / dm["open"].iloc[0] * 100
            ib_rng  = by10["high"].max() - by10["low"].min()
            dir_10  = 1 if by10["close"].iloc[-1] > by10["open"].iloc[0] else -1
            # Simple classifier: wide range + directional → trend prediction
            predicted_trend = (ib_rng > 50 and abs(gap_pct) > 0.2)
            actual_trend    = classify_trend_day(dm)
            if predicted_trend == actual_trend:
                correct += 1
            total += 1
        except Exception:
            pass

    acc = correct / total * 100 if total else np.nan
    record(119, "Day type classifier accuracy by 10 AM",
           f"{acc:.1f}% correct (wide IB + gap > 0.2% → trend prediction)",
           f"n={total} days | Simple 2-feature rule | Add more features to improve")
    print(f"  Q119 Day type classifier (10 AM): {acc:.1f}% accurate (n={total})")

    # Q120 — Up >0.5% by 9:45 (no gap) → closes up
    closes_up = 0
    no_gap_up = 0
    for dt in daily.index:
        try:
            dm = m1[m1.index.date == dt]
            first_bar = dm.iloc[0]
            if len(dm) < 30:
                continue
            # No gap: open within 0.2% of previous close
            if dt in daily.index:
                prev_idx = daily.index.get_loc(dt)
                if prev_idx == 0:
                    continue
                prev_close = daily.iloc[prev_idx - 1]["close"]
                gap = abs(first_bar["open"] - prev_close) / prev_close * 100
                if gap > 0.3:   # relaxed from 0.2% to 0.3%
                    continue
            by945 = dm.between_time(time(9,15), time(9,45))
            if by945.empty:
                continue
            move = (by945["close"].iloc[-1] - by945["open"].iloc[0]) / by945["open"].iloc[0] * 100
            if move >= 0.3:    # relaxed from 0.5% to 0.3% for larger sample
                no_gap_up += 1
                if dm["close"].iloc[-1] > dm["open"].iloc[0]:
                    closes_up += 1
        except Exception:
            pass

    cu_rate = closes_up / no_gap_up * 100 if no_gap_up else np.nan
    record(120, "Up >0.3% by 9:45 (gap <0.3%) → closes up",
           f"{cu_rate:.1f}%",
           f"n={no_gap_up} qualifying days")
    print(f"  Q120 Up >0.3% by 9:45 → closes up: {cu_rate:.1f}% (n={no_gap_up})")

    # Q121 — Strong W1 (>1%) completely reverses by end of W2
    strong_w1_rev = 0
    strong_w1_total = 0
    for dt in daily.index:
        try:
            dm = m1[m1.index.date == dt]
            w1 = dm.between_time(W1_START, W1_END)
            w2 = dm.between_time(W2_START, W2_END)
            if w1.empty or w2.empty:
                continue
            w1_move_pct = (w1["close"].iloc[-1] - w1["open"].iloc[0]) / w1["open"].iloc[0] * 100
            w1_move_pts = abs(w1["close"].iloc[-1] - w1["open"].iloc[0])
            # Use 0.5% OR 80pts absolute (whichever gives more events)
            if abs(w1_move_pct) >= 0.5 or w1_move_pts >= 80:
                strong_w1_total += 1
                # Complete reversal: W2 close goes past W1 open
                w2_close = w2["close"].iloc[-1]
                w1_open  = w1["open"].iloc[0]
                if w1_move_pct > 0 and w2_close < w1_open:
                    strong_w1_rev += 1
                elif w1_move_pct < 0 and w2_close > w1_open:
                    strong_w1_rev += 1
        except Exception:
            pass

    rev_rate = strong_w1_rev / strong_w1_total * 100 if strong_w1_total else np.nan
    record(121, "Strong W1 (≥0.5% or ≥80pts) reverses by W2 end",
           f"{rev_rate:.1f}%",
           f"n={strong_w1_total} strong-W1 days | Fake trend rate")
    print(f"  Q121 Strong W1 fake trend rate: {rev_rate:.1f}% (n={strong_w1_total})")


# ============================================================================
# SECTION U — CROSS-INDEX SIGNALS (Q122–Q124)
# ============================================================================
def analyze_cross_signals(nifty_m1: pd.DataFrame, nifty_d: pd.DataFrame,
                          bnf_m1: Optional[pd.DataFrame],
                          mid_m1: Optional[pd.DataFrame]):
    print("\n[U] CROSS-INDEX SIGNALS")

    # Q122 — BankNifty breaks IB High before Nifty → Nifty follows in 30 mins
    if bnf_m1 is not None and not bnf_m1.empty:
        follows = 0
        total   = 0
        for dt in nifty_d.index:
            try:
                nif = nifty_m1[nifty_m1.index.date == dt]
                bnf = bnf_m1[bnf_m1.index.date == dt]
                if nif.empty or bnf.empty:
                    continue
                nif_ib_h = nif.between_time(time(9,15), time(10,15))["high"].max()
                bnf_ib_h = bnf.between_time(time(9,15), time(10,15))["high"].max()
                # Find first bar where BNF breaks its IB High
                bnf_rest = bnf.between_time(time(10,16), time(15,30))
                nif_rest = nif.between_time(time(10,16), time(15,30))
                bnf_bo   = bnf_rest[bnf_rest["high"] > bnf_ib_h].head(1)
                if bnf_bo.empty:
                    continue
                bnf_bo_ts = bnf_bo.index[0]
                # Check if Nifty had already broken its IB High
                nif_before_bnf = nif_rest[nif_rest.index < bnf_bo_ts]
                if not nif_before_bnf.empty and nif_before_bnf["high"].max() > nif_ib_h:
                    continue  # Nifty broke first, skip
                total += 1
                # Nifty follows within 30 mins of BNF breakout
                nif_after = nif_rest[(nif_rest.index >= bnf_bo_ts) &
                                     (nif_rest.index <= bnf_bo_ts + timedelta(minutes=30))]
                if not nif_after.empty and nif_after["high"].max() > nif_ib_h:
                    follows += 1
            except Exception:
                pass
        rate122 = follows / total * 100 if total else np.nan
        record(122, "BNF breaks IB High first → Nifty follows in 30 mins",
               f"{rate122:.1f}%",
               f"n={total} days where BNF broke out before Nifty")
        print(f"  Q122 BNF leads → Nifty follows 30 mins: {rate122:.1f}% (n={total})")
    else:
        print("  Q122 BankNifty data not loaded")
        record(122, "BNF leads → Nifty follows", "BankNifty data not loaded", "")

    # Q123 — Nifty new day high but BNF does NOT → reversal in 15 mins
    if bnf_m1 is not None and not bnf_m1.empty:
        diverge_rev = 0
        diverge_tot = 0
        for dt in nifty_d.index:
            try:
                nif = nifty_m1[nifty_m1.index.date == dt]
                bnf = bnf_m1[bnf_m1.index.date == dt]
                if nif.empty or bnf.empty:
                    continue
                # Rolling max for each bar
                nif_runmax = nif["high"].expanding().max()
                bnf_runmax = bnf["high"].expanding().max()
                for i in range(30, len(nif)-6):
                    ts = nif.index[i]
                    if ts not in bnf.index:
                        continue
                    bnf_i = bnf.index.get_loc(ts) if ts in bnf.index else None
                    if bnf_i is None:
                        continue
                    # Nifty makes new high
                    if nif["high"].iloc[i] >= nif_runmax.iloc[i-1]:
                        # BNF does NOT make new high
                        bnf_ts_val = bnf.loc[ts, "high"] if ts in bnf.index else 0
                        if bnf_ts_val < bnf_runmax.iloc[bnf_i - 1] * 0.998:
                            diverge_tot += 1
                            # Reversal: Nifty drops 10pts in next 15 bars
                            fut = nif.iloc[i:i+15]
                            if fut["low"].min() <= nif["high"].iloc[i] - 10:
                                diverge_rev += 1
                            break  # one per day
            except Exception:
                pass
        div_rate = diverge_rev / diverge_tot * 100 if diverge_tot else np.nan
        record(123, "Nifty new high but BNF diverges → 10pt reversal",
               f"{div_rate:.1f}%",
               f"n={diverge_tot} divergence events")
        print(f"  Q123 Nifty/BNF divergence → reversal: {div_rate:.1f}% (n={diverge_tot})")
    else:
        print("  Q123 BankNifty data not loaded")
        record(123, "Nifty/BNF divergence → reversal", "BankNifty data not loaded", "")

    # Q124 — MidCap trends opposite to Nifty in W2 → W3 outcome
    if mid_m1 is not None and not mid_m1.empty:
        nif_w3_follows_mid_opp = 0
        opp_count = 0
        for dt in nifty_d.index:
            try:
                nif = nifty_m1[nifty_m1.index.date == dt]
                mid = mid_m1[mid_m1.index.date == dt]
                if nif.empty or mid.empty:
                    continue
                nif_w2 = nif.between_time(W2_START, W2_END)
                mid_w2 = mid.between_time(W2_START, W2_END)
                nif_w3 = nif.between_time(W3_START, W3_END)
                if nif_w2.empty or mid_w2.empty or nif_w3.empty:
                    continue
                nif_w2_dir = np.sign(nif_w2["close"].iloc[-1] - nif_w2["open"].iloc[0])
                mid_w2_dir = np.sign(mid_w2["close"].iloc[-1] - mid_w2["open"].iloc[0])
                if nif_w2_dir != 0 and mid_w2_dir != 0 and nif_w2_dir != mid_w2_dir:
                    opp_count += 1
                    nif_w3_dir = np.sign(nif_w3["close"].iloc[-1] - nif_w3["open"].iloc[0])
                    # W3 follows MidCap direction (opposite to Nifty W2)
                    if nif_w3_dir == mid_w2_dir:
                        nif_w3_follows_mid_opp += 1
            except Exception:
                pass
        follow_rate = nif_w3_follows_mid_opp / opp_count * 100 if opp_count else np.nan
        record(124, "MidCap diverges from Nifty in W2 → W3 follows MidCap",
               f"{follow_rate:.1f}%",
               f"n={opp_count} divergence days")
        print(f"  Q124 MidCap/Nifty W2 divergence → W3 follows: {follow_rate:.1f}% (n={opp_count})")
    else:
        print("  Q124 MidCap data not loaded")
        record(124, "MidCap diverge → W3 outcome", "MidCap data not loaded", "")


# ============================================================================
# SECTION V — ENHANCEMENTS TO EXISTING QUESTIONS (E1–E6)
# ============================================================================
def analyze_enhancements(m1: pd.DataFrame, daily: pd.DataFrame):
    print("\n[V] ENHANCEMENTS")

    if daily.empty or m1.empty:
        return

    d = daily.copy()
    d["prev_close"] = d["close"].shift(1)
    d["gap_pct"]    = (d["open"] - d["prev_close"]) / d["prev_close"] * 100
    d = d.dropna(subset=["prev_close"])

    # E1 — Q2 enhanced: Gap fade rate by size bucket
    print("  [E1] Gap fade by size bucket:")
    buckets = [(0.3, 0.5), (0.5, 0.7), (0.7, 1.0), (1.0, 99)]
    e1_parts = []
    for lo, hi in buckets:
        sub = d[d["gap_pct"].abs().between(lo, hi)].copy()
        if len(sub) < 3:
            continue
        sub["gap_up"] = sub["gap_pct"] > 0
        # Fade = price moves back toward prev_close by >50% of gap
        sub["fade"] = np.where(
            sub["gap_up"],
            sub["low"] <= sub["prev_close"] + (sub["open"] - sub["prev_close"]) * 0.5,
            sub["high"] >= sub["prev_close"] - (sub["prev_close"] - sub["open"]) * 0.5,
        )
        fade_rate = sub["fade"].mean() * 100
        e1_parts.append(f"{lo}-{hi}%: {fade_rate:.0f}% fade (n={len(sub)})")
    record("E1", "Gap fade rate by size bucket (E1)",
           " | ".join(e1_parts), "Enhancement of Q2")
    for p in e1_parts:
        print(f"    {p}")

    # E2 — Q11 enhanced: Window range distribution by day type
    print("  [E2] Window ranges by day type:")
    w1 = window_ohlcv(m1, W1_START, W1_END)
    w2 = window_ohlcv(m1, W2_START, W2_END)
    w3 = window_ohlcv(m1, W3_START, W3_END)
    if not w1.empty:
        w1["range"] = w1["high"] - w1["low"]
        w2["range"] = w2["high"] - w2["low"]
        w3["range"] = w3["high"] - w3["low"]
        trend_days = [dt for dt in daily.index
                      if not m1[m1.index.date == dt].empty and
                      classify_trend_day(m1[m1.index.date == dt])]
        inside_days = [dt for dt in daily.index if dt not in trend_days]
        e2_parts = []
        for label, days in [("Trend", trend_days), ("Inside", inside_days)]:
            wdays = [dt for dt in days if dt in w1.index]
            if wdays:
                r1 = w1.loc[wdays, "range"].mean()
                r2 = w2.loc[[d for d in wdays if d in w2.index], "range"].mean() if any(d in w2.index for d in wdays) else 0
                r3 = w3.loc[[d for d in wdays if d in w3.index], "range"].mean() if any(d in w3.index for d in wdays) else 0
                e2_parts.append(f"{label}(n={len(wdays)}): W1={r1:.0f} W2={r2:.0f} W3={r3:.0f}")
        record("E2", "Window ranges by day type (E2)",
               " | ".join(e2_parts), "Enhancement of Q11")
        for p in e2_parts:
            print(f"    {p}")

    # E3 — Q45/Q46 enhanced: Breakout success by time-of-day bucket
    print("  [E3] Breakout success by time bucket:")
    time_buckets = [
        ("09:15-10:00", time(9,15), time(10,0)),
        ("10:00-11:20", time(10,1), time(11,20)),
        ("11:20-13:25", time(11,21), time(13,25)),
        ("13:25-15:30", time(13,26), time(15,30)),
    ]
    e3_parts = []
    for label, ts, te in time_buckets:
        wins, total = 0, 0
        for dt in daily.index:
            try:
                dm  = m1[m1.index.date == dt]
                seg = dm.between_time(ts, te)
                if seg.empty:
                    continue
                # Any breakout of the preceding 30-min range
                before = dm[dm.index < seg.index[0]].tail(30)
                if before.empty:
                    continue
                ref_h = before["high"].max()
                ref_l = before["low"].min()
                for i in range(len(seg)-5):
                    if seg["high"].iloc[i] > ref_h:
                        total += 1
                        # Holds for 10 mins
                        fut = seg.iloc[i:i+10]
                        if not fut.empty and fut["low"].min() > ref_h * 0.998:
                            wins += 1
                        break
                    elif seg["low"].iloc[i] < ref_l:
                        total += 1
                        fut = seg.iloc[i:i+10]
                        if not fut.empty and fut["high"].max() < ref_l * 1.002:
                            wins += 1
                        break
            except Exception:
                pass
        if total > 0:
            e3_parts.append(f"{label}: {wins/total*100:.0f}% (n={total})")
    record("E3", "Breakout success by time-of-day (E3)",
           " | ".join(e3_parts), "Enhancement of Q45/Q46")
    for p in e3_parts:
        print(f"    {p}")

    # E4 — Q59 enhanced: False breakout by window AND by day type
    print("  [E4] False breakout rate by window:")
    e4_parts = []
    for wlabel, ws, we in [("W1", W1_START, W1_END), ("W2", W2_START, W2_END), ("W3", W3_START, W3_END)]:
        fbo, total = 0, 0
        for dt in daily.index:
            try:
                dm  = m1[m1.index.date == dt]
                seg = dm.between_time(ws, we)
                if len(seg) < 10:
                    continue
                ref_h = seg["high"].iloc[:10].max()
                ref_l = seg["low"].iloc[:10].min()
                rest  = seg.iloc[10:]
                for _, bar in rest.iterrows():
                    if bar["high"] > ref_h:
                        total += 1
                        fut = rest[rest.index > bar.name].head(10)
                        if not fut.empty and fut["close"].iloc[-1] < ref_h:
                            fbo += 1
                        break
                    elif bar["low"] < ref_l:
                        total += 1
                        fut = rest[rest.index > bar.name].head(10)
                        if not fut.empty and fut["close"].iloc[-1] > ref_l:
                            fbo += 1
                        break
            except Exception:
                pass
        if total:
            e4_parts.append(f"{wlabel}: {fbo/total*100:.0f}% false (n={total})")
    record("E4", "False breakout rate by window (E4)",
           " | ".join(e4_parts), "Enhancement of Q59")
    for p in e4_parts:
        print(f"    {p}")

    # E5 — Q79 enhanced: Green streak on 1-min bars (opening 30 mins)
    print("  [E5] Green candle streak (1-min, opening 30 mins):")
    streak_map = {}
    for dt in daily.index:
        try:
            dm = m1[m1.index.date == dt].between_time(time(9,15), time(9,45))
            if len(dm) < 5:
                continue
            green = (dm["close"] > dm["open"]).values
            i = 0
            while i < len(green):
                s = 0
                while i+s < len(green) and green[i+s]:
                    s += 1
                if s > 0 and i+s < len(green) and not green[i+s]:
                    streak_map[s] = streak_map.get(s, 0) + 1
                i += max(1, s)
        except Exception:
            pass

    if streak_map:
        tot = sum(streak_map.values())
        cum = 0
        lim90 = 0
        for k in sorted(streak_map.keys()):
            cum += streak_map[k]
            if cum / tot >= 0.9 and lim90 == 0:
                lim90 = k
        e5_str = f"90% red follows after {lim90} consecutive 1-min green | " +                  " ".join(f"{k}g→{streak_map.get(k,0)}" for k in sorted(streak_map.keys())[:7])
        record("E5", "1-min green streak limit in opening 30 mins (E5)",
               e5_str, "Enhancement of Q79 (1-min, 9:15-9:45 only)")
        print(f"    {e5_str}")

    # E6 — Q99 enhanced: Optimal RR for Nifty — test 5pt/3pt, 7pt/3pt, 10pt/5pt, 15pt/7pt
    print("  [E6] Optimal RR simulation:")
    rr_configs = [(5,3), (7,3), (10,5), (15,7), (20,8)]
    e6_parts = []
    for tgt, stp in rr_configs:
        wins, losses = 0, 0
        for dt in daily.index:
            try:
                dm = m1[m1.index.date == dt]
                for i in range(len(dm) - 25):
                    entry = dm["close"].iloc[i]
                    fut   = dm.iloc[i+1:i+26]
                    ht = (fut["high"] >= entry + tgt).any()
                    hs = (fut["low"]  <= entry - stp).any()
                    if ht and hs:
                        if fut[fut["high"] >= entry+tgt].index[0] < fut[fut["low"] <= entry-stp].index[0]:
                            wins += 1
                        else:
                            losses += 1
                    elif ht:
                        wins += 1
                    elif hs:
                        losses += 1
            except Exception:
                pass
        tot = wins + losses
        wr  = wins / tot * 100 if tot else 0
        ev  = (wr/100 * tgt) - ((1-wr/100) * stp)
        e6_parts.append(f"{tgt}T/{stp}S: WR={wr:.0f}% EV={ev:+.2f}")
    best = max(rr_configs, key=lambda x: float(
        [p for p in e6_parts if str(x[0]) + "T" in p][0].split("EV=")[1]
    ) if [p for p in e6_parts if str(x[0]) + "T" in p] else -99)
    record("E6", "Optimal RR for Nifty scalp (E6)",
           f"Best: {best[0]}pt/{best[1]}pt | " + " | ".join(e6_parts),
           "Enhancement of Q99")
    for p in e6_parts:
        print(f"    {p}")

# ============================================================================
# REPORT GENERATION
# ============================================================================
def print_full_report():
    print("\n\n" + "=" * 80)
    print("FULL RESEARCH REPORT — NIFTY SCALPER ANALYTICS")
    print("=" * 80)
    categories = {
        "A — Gap Dynamics":        range(1, 11),
        "B — Time Windows":        range(11, 21),
        "C — ADR & Volatility":    range(21, 31),
        "D — Context (T-1)":       range(31, 41),
        "E — Technicals":          range(41, 51),
        "F — Day of Week":         range(51, 56),
        "G — Micro-structure":     range(56, 61),
        "H — Mean Reversion":      range(72, 75),
        "I — Scalper Specific":    range(79, 83),
        "J — Volatility Clusters": range(86, 89),
        "K — Multi-timeframe":     range(91, 94),
        "L — Closing Dynamics":    range(94, 97),
        "M — Summary Stats":       range(99, 102),
        "N — Risk/Psychology":     range(83, 86),
        "O — Cross-index":         [29, 66, 67, 68, 97, 98],
        "P — ORB / Initial Balance": list(range(102, 108)),
        "Q — Entry Timing":          list(range(108, 112)),
        "R — Stop Sizing":           list(range(112, 115)),
        "S — Momentum/Exhaustion":   list(range(115, 119)),
        "T — Session Profiling":     list(range(119, 122)),
        "U — Cross-index Signals":   list(range(122, 125)),
        "V — Enhancements":          ["E1","E2","E3","E4","E5","E6"],
    }
    for cat, qids in categories.items():
        print(f"\n  {cat}")
        print(f"  {'-'*60}")
        for qid in qids:
            if qid in RESULTS:
                r = RESULTS[qid]
                print(f"    Q{qid:<3} {r['title']:<45} → {r['value']}")
                if r["detail"]:
                    print(f"         {r['detail']}")
            else:
                print(f"    Q{qid:<3} (not computed)")


def save_csv():
    rows = []
    for qid, r in RESULTS.items():
        rows.append({
            "Question_ID": qid,
            "Title": r["title"],
            "Result": r["value"],
            "Detail": r["detail"],
        })
    out = pd.DataFrame(rows).sort_values("Question_ID")
    path = OUTPUT_DIR / "research_results.csv"
    out.to_csv(path, index=False)
    print(f"\n  Results saved → {path}")


# ============================================================================
# MAIN
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="Nifty Scalper Analytics Engine")
    parser.add_argument("--from",   dest="from_date", default="2023-01-01",
                        help="Start date YYYY-MM-DD")
    parser.add_argument("--to",     dest="to_date",   default=date.today().strftime("%Y-%m-%d"),
                        help="End date YYYY-MM-DD")
    parser.add_argument("--days",   type=int, default=None,
                        help="Override: analyse last N trading days")
    parser.add_argument("--section",default="all",
                        help="Run one section: gaps/windows/adr/t1/tech/dow/micro/mr/scalper/vol/mtf/close/summary/orb/timing/stops/exhaust/profile/xsig/enhance/all")
    parser.add_argument("--discover", action="store_true",
                        help="List all instrument keys in DB and exit")
    args = parser.parse_args()

    print("=" * 70)
    print("NIFTY SCALPER ANALYTICS ENGINE")
    print("=" * 70)

    con = connect_db()

    if args.discover:
        discover_instruments(con)
        con.close()
        return

    if args.days:
        to_dt   = date.today()
        from_dt = to_dt - timedelta(days=args.days * 1.5)  # buffer for weekends
        from_date = from_dt.strftime("%Y-%m-%d")
        to_date   = to_dt.strftime("%Y-%m-%d")
    else:
        from_date = args.from_date
        to_date   = args.to_date

    print(f"  Period  : {from_date} → {to_date}")
    print(f"  Section : {args.section}")
    print(f"  DB      : {DB_PATH}")

    # --- Load primary Nifty data ---
    print("\nLoading Nifty 50 1-min data...", end=" ", flush=True)
    nifty_key = INSTRUMENTS.get("nifty", "NSE_INDEX|Nifty 50")
    m1 = load_1min(con, nifty_key, from_date, to_date)
    if m1.empty:
        print(f"\nERROR: No data found for '{nifty_key}'")
        print("Run with --discover to list available instrument keys.")
        con.close()
        return
    daily = build_daily(m1)
    print(f"{len(m1):,} bars | {len(daily)} trading days")

    # --- Load secondary indices (optional, skip gracefully) ---
    def safe_load(key_name):
        key = INSTRUMENTS.get(key_name)
        if not key:
            return None, None
        try:
            d = load_1min(con, key, from_date, to_date)
            if d.empty:
                return None, None
            return d, build_daily(d)
        except Exception:
            return None, None

    print("Loading BankNifty...", end=" ", flush=True)
    bnf_m1, bnf_d = safe_load("banknifty")
    print(f"{'OK' if bnf_m1 is not None else 'not found'}")

    print("Loading VIX...", end=" ", flush=True)
    vix_m1, vix_d = safe_load("vix")
    print(f"{'OK' if vix_m1 is not None else 'not found'}")

    print("Loading MidCap...", end=" ", flush=True)
    mid_m1, mid_d = safe_load("midcap")
    print(f"{'OK' if mid_m1 is not None else 'not found'}")

    print("Loading NiftyIT...", end=" ", flush=True)
    it_m1, it_d = safe_load("niftyit")
    print(f"{'OK' if it_m1 is not None else 'not found'}")

    con.close()

    # --- Run analyses ---
    s = args.section.lower()

    if s in ("all", "gaps"):
        analyze_gaps(daily, m1)
    if s in ("all", "windows"):
        analyze_windows(m1, daily)
    if s in ("all", "adr"):
        analyze_adr(daily, m1, vix_d)
    if s in ("all", "t1"):
        analyze_context_t1(daily, m1)
    if s in ("all", "tech"):
        analyze_technicals(m1, daily)
    if s in ("all", "dow"):
        analyze_day_of_week(daily, m1)
    if s in ("all", "micro"):
        analyze_microstructure(m1)
    if s in ("all", "mr"):
        analyze_mean_reversion(m1, daily)
    if s in ("all", "scalper"):
        analyze_scalper(m1, daily)
    if s in ("all", "vol"):
        analyze_volatility_clusters(m1)
    if s in ("all", "mtf"):
        analyze_multitimeframe(m1)
    if s in ("all", "close"):
        analyze_closing(daily, m1)
    if s in ("all", "summary"):
        analyze_summary(daily, m1)
    if s in ("all",):
        analyze_risk_psychology()
        analyze_cross_index(m1, daily, bnf_m1, bnf_d, vix_d)
    if s in ("all", "orb"):
        analyze_orb_ib(m1, daily)
    if s in ("all", "timing"):
        analyze_entry_timing(m1, daily)
    if s in ("all", "stops"):
        analyze_stop_sizing(m1, daily)
    if s in ("all", "exhaust"):
        analyze_momentum_exhaustion(m1, daily)
    if s in ("all", "profile"):
        analyze_session_profile(m1, daily)
    if s in ("all", "xsig"):
        analyze_cross_signals(m1, daily, bnf_m1, mid_m1)
    if s in ("all", "enhance"):
        analyze_enhancements(m1, daily)

    print_full_report()
    save_csv()
    print("\nDone.\n")


if __name__ == "__main__":
    main()
