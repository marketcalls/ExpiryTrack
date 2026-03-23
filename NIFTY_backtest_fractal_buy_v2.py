"""
William Fractals Option BUYING Backtest — NIFTY 50
====================================================
STRATEGY:
  - Down Fractal (bullish signal) → BUY ITM CE  (calls, direction = up)
  - Up   Fractal (bearish signal) → BUY ITM PE  (puts,  direction = down)

STRIKE SELECTION  (fully configurable):
  ITM_DEPTH = 1  → buy 1 strike IN the money  (e.g. spot=23150 → CE 23100)
  ITM_DEPTH = 2  → buy 2 strikes IN the money  (e.g. spot=23150 → CE 23050)
  ITM options have high delta (~0.6–0.8) so they move close to 1:1 with Nifty.

EXIT RULES  (all configurable):
  TARGET_PCT   = 0.25  → exit when option gains 25% of entry premium
  MAX_LOSS_PCT = 0.25  → exit when option loses 25% of entry premium
  Opposite fractal fires → exit immediately (signal reversal)
  Expiry day 15:15      → force exit at settlement price
  Intraday only option  → force exit at INTRADAY_EXIT time if no other exit

NO LOOK-AHEAD:
  Fractal confirmed only after n right-side candles close.
  Entry on close of confirmation bar (same as selling version).

SESSION BOUNDARY SAFE:
  Left-window never crosses calendar date. No phantom morning fractals.
"""

import os
import json
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from collections import defaultdict

import duckdb
import numpy as np
import pandas as pd

# ── PATHS ──────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"

# ══════════════════════════════════════════════════════════════════════════════
# ── STRATEGY CONFIG  (change these numbers to backtest different settings) ───
# ══════════════════════════════════════════════════════════════════════════════

FRACTAL_N       = 2         # KEY CHANGE: n=2 -> 5-candle (10-min lag not 50-min)
                            # n=10 has hard PF ceiling ~1.36 because momentum
                            # is exhausted by the 50-min confirmation delay.
                            # n=2: signal fires 10 min after fractal forms.
                            # Try: 2 (classic BW), 3, 5
TIMEFRAME       = "5min"
STRIKE_INTERVAL = 50

# -- STRIKE SELECTION ---------------------------------------------------------
ITM_DEPTH = 1               # 1 = 1 strike ITM, 2 = 2 strikes ITM

# -- POSITION SIZING ----------------------------------------------------------
LOT_SIZE  = 75
NUM_LOTS  = 1
QTY       = LOT_SIZE * NUM_LOTS

# -- EXIT RULES ---------------------------------------------------------------
TARGET_PCT   = 0.40         # KEY CHANGE: 40% target (was 25%)
                            # Analysis showed avg target gain was already 33%
                            # on n=10 trades. n=2 catches fresh momentum ->
                            # 40% achievable. Try: 0.30, 0.40, 0.50

MAX_LOSS_PCT = 0.20         # KEY CHANGE: tight SL 20% (was 50%)
                            # SL50 = avg -51K per hit = catastrophic.
                            # Reversal exit saves 14K vs SL25 -> keep it.
                            # Try: 0.15, 0.20, 0.25

INTRADAY_EXIT  = dtime(15, 15)

TRADE_START    = dtime(9, 15)
EXPIRY_EXIT    = dtime(15, 15)

# -- ENTRY WINDOW FILTER ------------------------------------------------------
# Only trade in proven high-PF time windows.
# From n=10 data: 10:30 (PF 1.66), 11:00 (PF 1.33), 11:30 (PF 1.49),
#                 13:00 (PF 1.21), 14:30 (PF 1.34)
# Worst: 09:00 (0.86), 12:00-12:30 (0.80-0.82), 14:00 (0.75), 15:00 (0.50)
# Format: list of (start_h, start_m, end_h, end_m) or None for all hours
ENTRY_WINDOWS = [
    (10, 30, 12,  0),   # mid-morning: 10:30-12:00
    (13,  0, 14, 30),   # post-lunch:  13:00-14:30
]

# -- PREMIUM FILTER -----------------------------------------------------------
# <50 = too cheap/OTM, noisy. >300 = expensive, wide spread, poor fill.
MIN_ENTRY_PREMIUM = 50
MAX_ENTRY_PREMIUM = 300     # set to None to disable

# -- ADX TREND FILTER ---------------------------------------------------------
# Only trade when market is trending (ADX >= threshold).
# Fractals in trending markets >> choppy months (Dec 25, Jul 25 both terrible).
ADX_PERIOD = 14             # standard Wilder ADX
ADX_MIN    = 20             # skip entry if ADX < this. Try: 15, 20, 25. 0=off.

# -- CAPITAL ------------------------------------------------------------------
CAPITAL = 200_000

# ── CHARGES (option buying) ───────────────────────────────────────────────────
# For buying: STT only on SELL side (when you exit/sell the option)
# Brokerage: flat ₹20 per order × 2 orders (buy + sell)
BROKERAGE_PER_ORDER = 20
STT_SELL_PCT        = 0.001      # STT on sell leg only (exit)
TXN_CHARGE_PCT      = 0.0003553
SEBI_PER_CRORE      = 10
GST_PCT             = 0.18
STAMP_BUY_PCT       = 0.00003    # stamp duty on buy leg (entry)

WARMUP_BARS_REQUIRED = max(FRACTAL_N * 10, ADX_PERIOD * 3)


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT FOLDER
# ══════════════════════════════════════════════════════════════════════════════

def make_output_dir(trade_start: str, trade_end: str) -> Path:
    name = (
        f"backtest_BUY_WF{FRACTAL_N}_"
        f"ITM{ITM_DEPTH}_"
        f"T{int(TARGET_PCT*100)}SL{int(MAX_LOSS_PCT*100)}_"
        f"ADX{ADX_MIN}_"
        f"{NUM_LOTS}lots_"
        f"{trade_start}_{trade_end}"
    )
    out = Path(BASE_DIR) / name
    out.mkdir(parents=True, exist_ok=True)
    print(f"  Output folder: {out}")
    return out


# ══════════════════════════════════════════════════════════════════════════════
# WILLIAM FRACTALS — session-boundary safe, deduplicated
# ══════════════════════════════════════════════════════════════════════════════

def compute_fractals(highs: np.ndarray, lows: np.ndarray,
                     dates: np.ndarray, n: int):
    """
    Returns:
      up_fractal, down_fractal : bool arrays, True at confirmation bar
      up_centre,  dn_centre    : int  arrays, centre bar index (-1 = none)
    """
    size   = len(highs)
    up_f   = np.zeros(size, dtype=bool)
    dn_f   = np.zeros(size, dtype=bool)
    up_c   = np.full(size, -1, dtype=int)
    dn_c   = np.full(size, -1, dtype=int)

    for c in range(n, size - n):
        # Session boundary: left bar must be same date as centre
        if dates[c - n] != dates[c]:
            continue
        confirm = c + n
        is_up = (highs[c] > np.max(highs[c - n: c]) and
                 highs[c] > np.max(highs[c + 1: c + n + 1]))
        is_dn = (lows[c]  < np.min(lows[c  - n: c]) and
                 lows[c]  < np.min(lows[c  + 1: c + n + 1]))
        if is_up:
            up_f[confirm] = True;  up_c[confirm] = c
        if is_dn:
            dn_f[confirm] = True;  dn_c[confirm] = c

    return up_f, dn_f, up_c, dn_c




# ══════════════════════════════════════════════════════════════════════════════
# ADX COMPUTATION  (Wilder smoothed, standard 14-period)
# ══════════════════════════════════════════════════════════════════════════════

def compute_adx(highs: np.ndarray, lows: np.ndarray,
                closes: np.ndarray, period: int) -> np.ndarray:
    """
    Wilder ADX. Values in the first 2*period bars are 0 (warmup).
    ADX >= ADX_MIN = trending market -> entry allowed.
    ADX <  ADX_MIN = choppy market  -> skip entry.
    """
    n   = len(closes)
    adx = np.zeros(n)
    if n < period * 2:
        return adx
    tr       = np.zeros(n)
    plus_dm  = np.zeros(n)
    minus_dm = np.zeros(n)
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        tr[i]       = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        up          = highs[i]  - highs[i-1]
        down        = lows[i-1] - lows[i]
        plus_dm[i]  = up   if up > down and up > 0   else 0.0
        minus_dm[i] = down if down > up and down > 0 else 0.0
    def ws(arr):
        out = np.zeros(n)
        out[period] = arr[1:period+1].sum()
        for i in range(period+1, n):
            out[i] = out[i-1] - out[i-1]/period + arr[i]
        return out
    atr_s = ws(tr); pdm_s = ws(plus_dm); mdm_s = ws(minus_dm)
    pdi   = np.where(atr_s>0, 100*pdm_s/atr_s, 0.0)
    mdi   = np.where(atr_s>0, 100*mdm_s/atr_s, 0.0)
    dx    = np.where((pdi+mdi)>0, 100*np.abs(pdi-mdi)/(pdi+mdi), 0.0)
    adx[period*2-1] = dx[period:period*2].mean()
    for i in range(period*2, n):
        adx[i] = (adx[i-1]*(period-1) + dx[i]) / period
    return adx

# ══════════════════════════════════════════════════════════════════════════════
# DATA VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_data(con) -> dict:
    print("\n" + "=" * 90)
    print("PHASE 0 — DATA VALIDATION")
    print("=" * 90)
    results = {}

    print("\n[1/3] Spot data ...")
    spot_meta = con.execute("""
        SELECT MIN(timestamp), MAX(timestamp), COUNT(*)
        FROM candle_data
        WHERE instrument_key = 'NSE_INDEX|Nifty 50' AND interval = '1minute'
    """).fetchone()
    spot_first = pd.Timestamp(spot_meta[0])
    spot_last  = pd.Timestamp(spot_meta[1])
    print(f"  OK {spot_first.date()} to {spot_last.date()} ({spot_meta[2]:,} bars)")

    print("\n[2/3] Option contracts ...")
    contracts = con.execute("""
        SELECT expired_instrument_key, strike_price, contract_type, expiry_date
        FROM contracts
        WHERE instrument_key='NSE_INDEX|Nifty 50' AND contract_type IN ('CE','PE')
        ORDER BY expiry_date, strike_price
    """).fetchdf()
    contracts["expiry_date"]  = pd.to_datetime(contracts["expiry_date"]).dt.date
    contracts["strike_price"] = contracts["strike_price"].astype(float)
    all_expiries = sorted(contracts["expiry_date"].unique())
    print(f"  OK {len(contracts):,} contracts, {len(all_expiries)} expiries")
    results["all_expiries"] = all_expiries

    print("\n[3/3] Effective backtest range ...")
    warmup_minutes   = WARMUP_BARS_REQUIRED * 5
    warmup_end_ts    = spot_first + timedelta(minutes=warmup_minutes)
    trade_start_date = warmup_end_ts.date()
    effective_end    = spot_last.date()
    print(f"  OK {trade_start_date}  to  {effective_end}")
    results.update({
        "spot_start":       spot_first,
        "spot_end":         spot_last,
        "trade_start_date": trade_start_date,
        "trade_end_date":   effective_end,
    })
    return results


# ══════════════════════════════════════════════════════════════════════════════
# SPOT DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

def load_spot_data(con, warmup_start: str) -> pd.DataFrame:
    print(f"\nLoading NIFTY spot data from {warmup_start} ...")
    df = con.execute(f"""
        SELECT timestamp, open, high, low, close, volume
        FROM candle_data
        WHERE instrument_key='NSE_INDEX|Nifty 50' AND interval='1minute'
          AND timestamp >= '{warmup_start}'
        ORDER BY timestamp
    """).fetchdf()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    df = df[~df.index.duplicated(keep="first")]
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    df["volume"] = df["volume"].fillna(0).astype(int)
    print(f"  1-min: {len(df):,} bars ({df.index[0]} to {df.index[-1]})")

    # Resample per-day to avoid cross-day bars
    parts = []
    for _date, grp in df.groupby(df.index.date):
        r = grp.resample(TIMEFRAME).agg({
            "open": "first", "high": "max", "low": "min",
            "close": "last", "volume": "sum"
        }).dropna()
        parts.append(r)
    df_5m = pd.concat(parts).sort_index()
    print(f"  5-min: {len(df_5m):,} bars")
    return df_5m


# ══════════════════════════════════════════════════════════════════════════════
# OPTION DATA PROVIDER
# ══════════════════════════════════════════════════════════════════════════════

class OptionDataProvider:
    def __init__(self, db_path):
        self.con = duckdb.connect(db_path, read_only=True)
        self._price_cache: dict = {}
        self._load_contracts()
        self._load_expiries()
        self._build_contract_index()

    def _load_contracts(self):
        print("  Loading NIFTY option contracts...")
        rows = self.con.execute("""
            SELECT expired_instrument_key, trading_symbol, strike_price,
                   contract_type, expiry_date, lot_size
            FROM contracts
            WHERE instrument_key='NSE_INDEX|Nifty 50' AND contract_type IN ('CE','PE')
            ORDER BY expiry_date, strike_price
        """).fetchall()
        self.contracts_raw = rows
        print(f"    {len(rows):,} contracts")

    def _load_expiries(self):
        expiry_set = set()
        for row in self.contracts_raw:
            exp = row[4]
            if isinstance(exp, datetime):
                exp = exp.date()
            expiry_set.add(exp)
        self.expiry_dates = sorted(expiry_set)

    def _build_contract_index(self):
        self.contract_index = {}
        for row in self.contracts_raw:
            key    = row[0]
            strike = float(row[2])
            ctype  = row[3]
            exp    = row[4]
            if isinstance(exp, datetime):
                exp = exp.date()
            self.contract_index[(strike, ctype, exp)] = key

    def get_nearest_expiry(self, trade_date):
        td = trade_date.date() if isinstance(trade_date, (datetime, pd.Timestamp)) else trade_date
        for exp in self.expiry_dates:
            if exp >= td:
                return exp
        return None

    def get_next_expiry(self, trade_date):
        td = trade_date.date() if isinstance(trade_date, (datetime, pd.Timestamp)) else trade_date
        found = False
        for exp in self.expiry_dates:
            if exp >= td:
                if not found:
                    found = True; continue
                return exp
        return None

    def get_contract_key(self, strike, option_type, expiry):
        if isinstance(expiry, (datetime, pd.Timestamp)):
            expiry = expiry.date()
        return self.contract_index.get((float(strike), option_type, expiry))

    def get_option_price(self, contract_key, timestamp, lookback_minutes=30,
                         use_cache_fallback=True):
        if contract_key is None:
            return None, None
        ts = timestamp
        if isinstance(ts, pd.Timestamp):
            ts = ts.to_pydatetime()
        ts = ts.replace(second=0, microsecond=0)
        for offset in range(lookback_minutes + 1):
            check_ts = ts - timedelta(minutes=offset)
            row = self.con.execute("""
                SELECT close FROM historical_data
                WHERE expired_instrument_key=? AND timestamp=?
            """, [contract_key, check_ts]).fetchone()
            if row and row[0] is not None and float(row[0]) > 0:
                price = float(row[0])
                self._price_cache[contract_key] = (price, check_ts)
                return price, check_ts
        if use_cache_fallback and contract_key in self._price_cache:
            return self._price_cache[contract_key]
        return None, None

    def get_option_price_at_expiry_settlement(self, contract_key, expiry_date):
        if contract_key is None:
            return 0.0
        for hh, mm in [(15, 29), (15, 25), (15, 20), (15, 15),
                       (15, 10), (15, 5), (15, 0), (14, 55)]:
            ts = datetime.combine(expiry_date, dtime(hh, mm))
            row = self.con.execute("""
                SELECT close FROM historical_data
                WHERE expired_instrument_key=? AND timestamp=?
            """, [contract_key, ts]).fetchone()
            if row and row[0] is not None:
                return float(row[0])
        if contract_key in self._price_cache:
            return self._price_cache[contract_key][0]
        return 0.0

    def close(self):
        self.con.close()


# ══════════════════════════════════════════════════════════════════════════════
# CHARGES — option buying (different from selling)
# ══════════════════════════════════════════════════════════════════════════════

def calc_charges_buy(entry_premium: float, exit_premium: float) -> float:
    """
    Option buying charges:
      Entry (BUY leg):  brokerage + stamp duty + txn + SEBI + GST
      Exit  (SELL leg): brokerage + STT + txn + SEBI + GST
    STT is only on the sell side (when you exit/sell the option).
    """
    brokerage = BROKERAGE_PER_ORDER * 2                       # buy + sell order
    stt       = STT_SELL_PCT * exit_premium * QTY             # only on exit
    turnover  = (entry_premium + exit_premium) * QTY
    txn       = TXN_CHARGE_PCT * turnover
    sebi      = SEBI_PER_CRORE * turnover / 1e7
    gst       = GST_PCT * (brokerage + txn + sebi)
    stamp     = STAMP_BUY_PCT * entry_premium * QTY           # only on buy entry
    return brokerage + stt + txn + sebi + gst + stamp


# ══════════════════════════════════════════════════════════════════════════════
# EXIT TRADE
# ══════════════════════════════════════════════════════════════════════════════

def _exit_trade(provider, trade_data, exit_time, reason, use_settlement=False):
    opt_key = trade_data["opt_key"]

    if use_settlement:
        exit_price = provider.get_option_price_at_expiry_settlement(
            opt_key, trade_data["expiry"])
        if exit_price == 0.0:
            exit_price, _ = provider.get_option_price(
                opt_key, exit_time, use_cache_fallback=True)
    else:
        exit_price, _ = provider.get_option_price(
            opt_key, exit_time, use_cache_fallback=True)

    if exit_price is None:
        return None

    entry_premium = trade_data["entry_premium"]
    gross_pnl     = (exit_price - entry_premium) * QTY
    charges       = calc_charges_buy(entry_premium, exit_price)
    total_pnl     = gross_pnl - charges

    entry_dt  = pd.Timestamp(trade_data["entry_time"])
    exit_dt   = pd.Timestamp(exit_time)
    days_held = (exit_dt.date() - entry_dt.date()).days

    return {
        "entry_time":     trade_data["entry_time"],
        "exit_time":      exit_time,
        "type":           trade_data["trade_type"],      # BUY_CE or BUY_PE
        "signal":         trade_data["signal"],          # BULLISH or BEARISH
        "entry_spot":     trade_data["entry_spot"],
        "strike":         trade_data["strike"],
        "opt_type":       trade_data["opt_type"],        # CE or PE
        "itm_depth":      ITM_DEPTH,
        "entry_premium":  entry_premium,
        "exit_premium":   exit_price,
        "gross_pnl":      gross_pnl,
        "charges":        charges,
        "total_pnl":      total_pnl,
        "exit_reason":    reason,
        "expiry":         trade_data["expiry"],
        "days_held":      days_held,
        "overnight":      days_held > 0,
        "qty":            QTY,
        "equity_at_entry": trade_data.get("equity_at_entry", 0),
    }


# ══════════════════════════════════════════════════════════════════════════════
# SIMULATION
# ══════════════════════════════════════════════════════════════════════════════

def run_simulation(df, provider, trade_start_date, trade_end_date):
    close     = df["close"].values.astype(float)
    high      = df["high"].values.astype(float)
    low       = df["low"].values.astype(float)
    n_bars    = len(close)
    bar_dates = np.array(df.index.date)

    # Compute fractals (session-boundary safe, deduplicated)
    up_fractal, down_fractal, up_frac_ctr, dn_frac_ctr = compute_fractals(
        high, low, bar_dates, FRACTAL_N
    )
    # Compute ADX for trend filter
    adx_values = compute_adx(high, low, close, ADX_PERIOD) if ADX_MIN > 0 else None

    # Find simulation range
    start_idx = 1
    for idx in range(1, n_bars):
        if df.index[idx].date() >= trade_start_date:
            start_idx = idx; break
    end_idx = n_bars
    for idx in range(n_bars - 1, 0, -1):
        if df.index[idx].date() <= trade_end_date:
            end_idx = idx + 1; break

    print(f"\n  Warmup bars   : {start_idx}")
    print(f"  Trading range : {df.index[start_idx].date()} to "
          f"{df.index[end_idx-1].date()} ({end_idx - start_idx:,} bars)")

    # State
    trades        = []
    total_pnl     = 0.0
    running_pnl   = 0.0
    in_trade      = False
    trade_data    = {}
    signal_log    = []
    pending_flip  = None          # queued direction waiting for entry next bar
    last_frac_dir = None          # last confirmed fractal direction
    last_up_ctr   = -1            # dedup
    last_dn_ctr   = -1

    # Counters
    total_signals = 0
    skipped_no_contract = 0
    skipped_no_price    = 0
    skipped_no_exit     = 0
    skipped_adx         = 0
    skipped_window      = 0
    skipped_premium     = 0
    pending_entries     = 0
    target_exits        = 0
    stoploss_exits      = 0
    reversal_exits      = 0
    expiry_exits        = 0
    intraday_exits      = 0

    # ── inner helper: try to enter a BUY trade ────────────────────────────────
    def _try_enter(bar_time, bar_date, spot, direction, trigger="Signal"):
        nonlocal skipped_no_contract, skipped_no_price

        atm_strike     = round(spot / STRIKE_INTERVAL) * STRIKE_INTERVAL
        nearest_expiry = provider.get_nearest_expiry(bar_time)
        is_expiry_day  = nearest_expiry is not None and bar_date == nearest_expiry

        # On expiry day use next expiry
        if is_expiry_day:
            expiry = provider.get_next_expiry(bar_time)
            if expiry is None:
                skipped_no_contract += 1
                signal_log.append({"bar": str(bar_time), "dir": direction,
                    "trigger": trigger, "outcome": "FAIL:no_next_expiry"})
                return None
        else:
            expiry = nearest_expiry
        if expiry is None:
            skipped_no_contract += 1
            signal_log.append({"bar": str(bar_time), "dir": direction,
                "trigger": trigger, "outcome": "FAIL:no_expiry"})
            return None

        # Strike selection: ITM based on direction
        if direction == "BULLISH":
            # Down fractal → buy CE → ITM CE = strike BELOW spot
            opt_type   = "CE"
            trade_type = "BUY_CE"
            strike     = atm_strike - ITM_DEPTH * STRIKE_INTERVAL
        else:
            # Up fractal → buy PE → ITM PE = strike ABOVE spot
            opt_type   = "PE"
            trade_type = "BUY_PE"
            strike     = atm_strike + ITM_DEPTH * STRIKE_INTERVAL

        opt_key = provider.get_contract_key(strike, opt_type, expiry)
        if opt_key is None:
            skipped_no_contract += 1
            signal_log.append({"bar": str(bar_time), "dir": direction,
                "trigger": trigger,
                "outcome": f"FAIL:no_contract {opt_type} {strike} exp={expiry}"})
            return None

        entry_price, _ = provider.get_option_price(
            opt_key, bar_time, use_cache_fallback=False)
        if entry_price is None or entry_price <= 0:
            skipped_no_price += 1
            signal_log.append({"bar": str(bar_time), "dir": direction,
                "trigger": trigger,
                "outcome": f"FAIL:no_price {opt_type} {strike} exp={expiry}"})
            return None

        # Entry window filter
        if ENTRY_WINDOWS:
            bar_mins = bar_time.hour * 60 + bar_time.minute
            in_win = any(h1*60+m1 <= bar_mins < h2*60+m2
                         for h1,m1,h2,m2 in ENTRY_WINDOWS)
            if not in_win:
                signal_log.append({"bar": str(bar_time), "dir": direction,
                    "trigger": trigger, "outcome": "SKIP:outside_window"})
                return None
        # Premium filter
        if MIN_ENTRY_PREMIUM and entry_price < MIN_ENTRY_PREMIUM:
            signal_log.append({"bar": str(bar_time), "dir": direction,
                "trigger": trigger, "outcome": f"SKIP:prem_low {entry_price:.2f}"})
            return None
        if MAX_ENTRY_PREMIUM and entry_price > MAX_ENTRY_PREMIUM:
            signal_log.append({"bar": str(bar_time), "dir": direction,
                "trigger": trigger, "outcome": f"SKIP:prem_high {entry_price:.2f}"})
            return None
        equity_now = CAPITAL + running_pnl
        cost       = entry_price * QTY   # cash outflow to buy
        if equity_now < cost:
            signal_log.append({"bar": str(bar_time), "dir": direction,
                "trigger": trigger, "outcome": "FAIL:insufficient_capital"})
            return None

        signal_log.append({"bar": str(bar_time), "dir": direction,
            "trigger": trigger,
            "outcome": (f"ENTERED {trade_type} {opt_type} {strike} "
                        f"exp={expiry} prem={entry_price:.2f}")})
        return {
            "trade_type":    trade_type,
            "signal":        direction,
            "entry_time":    bar_time,
            "entry_spot":    spot,
            "strike":        strike,
            "opt_type":      opt_type,
            "opt_key":       opt_key,
            "entry_premium": entry_price,
            "expiry":        expiry,
            "equity_at_entry": round(equity_now),
        }

    # ── main bar loop ─────────────────────────────────────────────────────────
    for i in range(start_idx, end_idx):
        bar_time = df.index[i]
        t        = bar_time.time()
        bar_date = bar_time.date()
        spot     = close[i]

        # ── Post-expiry force close ───────────────────────────────────────────
        if in_trade and bar_date > trade_data["expiry"]:
            expiry   = trade_data["expiry"]
            exit_pnl = None
            for hh, mm in [(15, 29), (15, 25), (15, 20), (15, 15),
                           (15, 10), (15, 5), (15, 0)]:
                candidate = pd.Timestamp(datetime.combine(expiry, dtime(hh, mm)))
                exit_pnl  = _exit_trade(provider, trade_data, candidate,
                                        "Expiry", use_settlement=True)
                if exit_pnl:
                    break
            if exit_pnl:
                trades.append(exit_pnl)
                total_pnl   += exit_pnl["total_pnl"]
                running_pnl += exit_pnl["total_pnl"]
                expiry_exits += 1
            in_trade = False

        # ── Expiry-day forced exit at 15:15 ──────────────────────────────────
        if in_trade and t >= EXPIRY_EXIT and bar_date == trade_data["expiry"]:
            exit_pnl = _exit_trade(provider, trade_data, bar_time,
                                   "Expiry", use_settlement=True)
            if exit_pnl:
                trades.append(exit_pnl)
                total_pnl   += exit_pnl["total_pnl"]
                running_pnl += exit_pnl["total_pnl"]
                expiry_exits += 1
            in_trade = False
            continue

        # ── Intraday forced exit ──────────────────────────────────────────────
        if in_trade and INTRADAY_EXIT and t >= INTRADAY_EXIT:
            exit_pnl = _exit_trade(provider, trade_data, bar_time, "IntradayExit")
            if exit_pnl:
                trades.append(exit_pnl)
                total_pnl   += exit_pnl["total_pnl"]
                running_pnl += exit_pnl["total_pnl"]
                intraday_exits += 1
            in_trade    = False
            pending_flip = None
            continue

        # ── Fractal signal detection (no look-ahead, deduplicated) ───────────
        any_flip = None
        if up_fractal[i]:
            ctr = int(up_frac_ctr[i])
            if ctr != last_up_ctr:
                any_flip    = "BEARISH"
                total_signals += 1
                last_up_ctr = ctr
        elif down_fractal[i]:
            ctr = int(dn_frac_ctr[i])
            if ctr != last_dn_ctr:
                any_flip    = "BULLISH"
                total_signals += 1
                last_dn_ctr = ctr

        # ── In-trade monitoring ───────────────────────────────────────────────
        if in_trade:
            entry_p = trade_data["entry_premium"]

            # Reversal: opposite fractal fires → exit immediately
            if any_flip:
                is_ce    = trade_data["opt_type"] == "CE"
                opposite = (is_ce and any_flip == "BEARISH") or \
                           (not is_ce and any_flip == "BULLISH")
                if opposite:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "Reversal")
                    if exit_pnl:
                        trades.append(exit_pnl)
                        total_pnl   += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]
                        reversal_exits += 1
                        in_trade     = False
                        pending_flip = any_flip   # enter opposite on next bar
                        last_frac_dir = any_flip
                        continue
                    else:
                        skipped_no_exit += 1
                        continue

            # Target and stop-loss checks every bar
            curr_price, _ = provider.get_option_price(
                trade_data["opt_key"], bar_time, use_cache_fallback=True)

            if curr_price is not None:
                profit_pct = (curr_price - entry_p) / entry_p

                # Target hit
                if profit_pct >= TARGET_PCT:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "Target")
                    if exit_pnl:
                        trades.append(exit_pnl)
                        total_pnl   += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]
                        target_exits += 1
                        in_trade = False
                        # Re-enter same direction immediately on target hit
                        new_td = _try_enter(bar_time, bar_date, spot,
                                            trade_data["signal"], "Target-ReEntry")
                        if new_td:
                            trade_data = new_td
                            in_trade   = True
                        continue

                # Stop-loss hit
                if profit_pct <= -MAX_LOSS_PCT:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "StopLoss")
                    if exit_pnl:
                        trades.append(exit_pnl)
                        total_pnl   += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]
                        stoploss_exits += 1
                        in_trade = False
                        continue

        # ── Pending flip: enter on next bar after signal ──────────────────────
        # ADX gate: only accept new signals when market is trending
        adx_now = adx_values[i] if (adx_values is not None and ADX_MIN > 0) else ADX_MIN + 1
        adx_ok  = adx_now >= ADX_MIN

        if any_flip and not in_trade:
            if adx_ok:
                last_frac_dir = any_flip
                pending_flip  = any_flip
            else:
                signal_log.append({"bar": str(bar_time), "dir": any_flip,
                    "trigger": "Signal",
                    "outcome": f"SKIP:ADX_low {adx_now:.1f}<{ADX_MIN}"})

        if pending_flip and not in_trade and t >= TRADE_START:
            # Also gate actual entry on current-bar ADX
            if not adx_ok:
                if t >= dtime(9, 45):
                    pending_flip = None   # give up if still choppy
            else:
                new_td = _try_enter(bar_time, bar_date, spot, pending_flip, "PendingFlip")
                if new_td:
                    trade_data      = new_td
                    in_trade        = True
                    pending_entries += 1
                    pending_flip    = None
                elif t >= dtime(9, 45):
                    pending_flip = None

    # ── DataEnd force close ───────────────────────────────────────────────────
    if in_trade:
        exit_pnl = None
        for lb in range(min(78, len(df))):
            candidate = df.index[-(1 + lb)]
            exit_pnl  = _exit_trade(provider, trade_data, candidate,
                                    "DataEnd", use_settlement=True)
            if exit_pnl:
                break
        if exit_pnl:
            trades.append(exit_pnl)
            total_pnl   += exit_pnl["total_pnl"]
            running_pnl += exit_pnl["total_pnl"]
            print(f"  DataEnd close P&L: ₹{exit_pnl['total_pnl']:+,.0f}")

    # Print counters
    print(f"\n  Counters:")
    print(f"  Fractal signals             : {total_signals}  (n={FRACTAL_N}, {2*FRACTAL_N+1}-candle)")
    print(f"  Pending flip entries        : {pending_entries}")
    print(f"  Target exits                : {target_exits}")
    print(f"  StopLoss exits              : {stoploss_exits}")
    print(f"  Reversal exits              : {reversal_exits}")
    print(f"  Expiry exits                : {expiry_exits}")
    print(f"  Intraday exits              : {intraday_exits}")
    print(f"  Skipped - no contract       : {skipped_no_contract}")
    print(f"  Skipped - no price (entry)  : {skipped_no_price}")
    print(f"  Dropped - no exit price     : {skipped_no_exit}")
    print(f"  Executed trades             : {len(trades)}")

    return trades, total_pnl, signal_log


# ══════════════════════════════════════════════════════════════════════════════
# CONSOLE REPORT
# ══════════════════════════════════════════════════════════════════════════════

def print_console_report(tdf, total_pnl):
    wins   = tdf[tdf["total_pnl"] > 0]
    losses = tdf[tdf["total_pnl"] <= 0]
    n_tr   = len(tdf)
    wr     = len(wins) / n_tr * 100 if n_tr > 0 else 0
    pf     = (wins["total_pnl"].sum() / abs(losses["total_pnl"].sum())
              if len(losses) > 0 and losses["total_pnl"].sum() != 0 else 99)
    roi    = total_pnl / CAPITAL * 100
    cum    = tdf["total_pnl"].cumsum()
    mdd    = (cum.cummax() - cum).max()
    avg_w  = wins["total_pnl"].mean()   if len(wins)   > 0 else 0
    avg_l  = losses["total_pnl"].mean() if len(losses) > 0 else 0

    print(f"\n{'='*90}")
    print(f"  RESULTS  |  "
          f"{pd.Timestamp(tdf['entry_time'].min()).date()} to "
          f"{pd.Timestamp(tdf['exit_time'].max()).date()}")
    print(f"  {'-'*65}")
    print(f"  Strategy         : BUY ITM{ITM_DEPTH} | Target {int(TARGET_PCT*100)}% | SL {int(MAX_LOSS_PCT*100)}%")
    print(f"  Fractal          : n={FRACTAL_N} ({2*FRACTAL_N+1}-candle) | {TIMEFRAME}")
    print(f"  Entry windows    : {ENTRY_WINDOWS if ENTRY_WINDOWS else 'All hours'}")
    print(f"  Premium filter   : ₹{MIN_ENTRY_PREMIUM}–₹{MAX_ENTRY_PREMIUM}")
    print(f"  ADX filter       : min {ADX_MIN} (period {ADX_PERIOD})")
    print(f"  Sizing           : {NUM_LOTS} lot × {LOT_SIZE} = {QTY} qty")
    print(f"  Total Trades     : {n_tr}")
    print(f"  Win Rate         : {wr:.1f}%  ({len(wins)}W / {len(losses)}L)")
    print(f"  Profit Factor    : {pf:.2f}")
    print(f"  Avg Winner       : ₹{avg_w:,.0f}")
    print(f"  Avg Loser        : ₹{avg_l:,.0f}")
    print(f"  Total P&L        : ₹{total_pnl:+,.0f}")
    print(f"  ROI on {CAPITAL/1e5:.0f}L       : {roi:+.1f}%")
    print(f"  Max Drawdown     : ₹{mdd:,.0f}  ({mdd/CAPITAL*100:.1f}% of capital)")

    # Type breakdown
    for trade_type in ["BUY_CE", "BUY_PE"]:
        sub = tdf[tdf["type"] == trade_type]
        if len(sub) > 0:
            sub_wr  = (sub["total_pnl"] > 0).mean() * 100
            sub_pnl = sub["total_pnl"].sum()
            print(f"  {trade_type:8s}          : {len(sub):3d} trades | WR:{sub_wr:5.1f}% | P&L:₹{sub_pnl:+,.0f}")

    # Exit breakdown
    print(f"\n  EXIT BREAKDOWN:")
    for reason in ["Target", "StopLoss", "Reversal", "IntradayExit", "Expiry", "DataEnd"]:
        sub = tdf[tdf["exit_reason"] == reason]
        if len(sub) > 0:
            sub_wr  = (sub["total_pnl"] > 0).mean() * 100
            sub_pnl = sub["total_pnl"].sum()
            print(f"    {reason:15s}: {len(sub):3d} trades | WR:{sub_wr:5.1f}% | P&L:₹{sub_pnl:+,.0f}")

    # Monthly
    tdf = tdf.copy()
    tdf["month"] = tdf["entry_time"].dt.to_period("M")
    print(f"\n  MONTHLY P&L:")
    cumulative = 0
    for month, grp in tdf.groupby("month"):
        m_pnl       = grp["total_pnl"].sum()
        cumulative += m_pnl
        m_wr        = (grp["total_pnl"] > 0).mean() * 100
        print(f"    {month} | {len(grp):3d} trades | WR:{m_wr:5.1f}% | "
              f"P&L: ₹{m_pnl:+10,.0f} | Cum: ₹{cumulative:+10,.0f}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 90)
    print("WILLIAM FRACTALS — OPTION BUYING BACKTEST")
    print("=" * 90)
    print(f"  Signal  : WF(n={FRACTAL_N}, {2*FRACTAL_N+1}-candle) | {TIMEFRAME}")
    print(f"  Windows : {ENTRY_WINDOWS if ENTRY_WINDOWS else 'All hours'}")
    print(f"  Premium : ₹{MIN_ENTRY_PREMIUM}–₹{MAX_ENTRY_PREMIUM}")
    print(f"  ADX     : min={ADX_MIN} (period={ADX_PERIOD})")
    print(f"  Strike  : ITM{ITM_DEPTH} ({ITM_DEPTH} strike{'s' if ITM_DEPTH>1 else ''} in the money)")
    print(f"  Target  : +{int(TARGET_PCT*100)}% of entry premium")
    print(f"  SL      : -{int(MAX_LOSS_PCT*100)}% of entry premium")
    print(f"  Sizing  : {NUM_LOTS}×{LOT_SIZE}={QTY} qty | Capital ₹{CAPITAL:,}")
    print(f"  Intraday exit: {INTRADAY_EXIT if INTRADAY_EXIT else 'Disabled (overnight allowed)'}")

    con = duckdb.connect(DB_PATH, read_only=True)
    val = validate_data(con)

    warmup_start_str = str(
        (pd.Timestamp(val["trade_start_date"]) - pd.DateOffset(months=3)).date()
    )
    out_dir = make_output_dir(str(val["trade_start_date"]), str(val["trade_end_date"]))

    df_spot = load_spot_data(con, warmup_start_str)
    con.close()

    print(f"\n{'='*90}")
    print(f"PHASE 1 — SIMULATION  |  "
          f"{val['trade_start_date']} to {val['trade_end_date']}")
    print("=" * 90)

    provider = OptionDataProvider(DB_PATH)
    trades, total_pnl, signal_log = run_simulation(
        df_spot, provider,
        trade_start_date=val["trade_start_date"],
        trade_end_date=val["trade_end_date"],
    )
    provider.close()

    if not trades:
        print("  No trades executed.")
        raise SystemExit(0)

    tdf = pd.DataFrame(trades)
    tdf["entry_time"] = pd.to_datetime(tdf["entry_time"])
    tdf["exit_time"]  = pd.to_datetime(tdf["exit_time"])

    print(f"\n{'='*90}")
    print("PHASE 2 — RESULTS")
    print("=" * 90)
    print_console_report(tdf, total_pnl)

    # Save CSV
    csv_name = (
        f"trades_BUY_WF{FRACTAL_N}_ITM{ITM_DEPTH}_"
        f"T{int(TARGET_PCT*100)}SL{int(MAX_LOSS_PCT*100)}_"
        f"ADX{ADX_MIN}_{NUM_LOTS}lots.csv"
    )
    csv_path = out_dir / csv_name
    tdf.drop(columns=["month"], errors="ignore").to_csv(csv_path, index=False)
    print(f"\n  Trades CSV  : {csv_path}")

    sig_csv = out_dir / "signal_audit.csv"
    pd.DataFrame(signal_log).to_csv(sig_csv, index=False)
    print(f"  Signal log  : {sig_csv}")
    print(f"\nAll outputs saved to: {out_dir}/")
    print("Done.")
