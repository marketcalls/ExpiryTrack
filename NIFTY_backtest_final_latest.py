"""
SuperTrend Credit Spread Backtest — Full Data Validation + Rich Dashboard
=========================================================================
FEATURES:
  [1] Dynamic output folder: backtest_ST{P}_{M}_{W}pt_{L}lots_{start}_{end}/
  [2] Generates a self-contained interactive HTML dashboard (no Plotly dependency)
  [3] Full data validation phase before any simulation
  [4] Price cache + 30-min lookback + expiry settlement fallback
  [5] Signal audit log — every flip recorded with outcome
  [6] All charts: cumulative P&L, drawdown, monthly, exit reasons, distribution,
      holding period, overnight vs intraday, heatmap, full filterable trade log

Usage:
    uv run python NIFTY_backtest_validated.py
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

# ── STRATEGY CONFIG ────────────────────────────────────────────────────────────
ST_PERIOD      = 80
ST_MULTIPLIER  = 3.6
TIMEFRAME      = "5min"

SPREAD_WIDTH    = 500
STRIKE_INTERVAL = 50

LOT_SIZE  = 65
NUM_LOTS  = 8
QTY       = LOT_SIZE * NUM_LOTS

TARGET_PCT   = 0.95
MAX_LOSS_PCT = 0.60

# OVERNIGHT GAP STOP — mirrors live strategy exactly.
# If NIFTY opens adversely by more than this many points vs previous day close,
# exit the overnight position at the 09:15 bar WITHOUT waiting for ST reversal.
# Adverse = DOWN for BULL_PUT,  UP for BEAR_CALL.
# Set to 99999 to disable and compare with/without.
OVERNIGHT_GAP_PTS = 200

# GAP STOP RE-ENTRY DELAY — how many minutes to wait after a gap stop exit
# before allowing a new entry. Gives volatile post-gap market time to stabilise.
#
# Options to backtest (change this one number, re-run, compare):
#   0  → enter at next 5-min bar (09:20) — current behavior, no wait
#   10 → wait for 10-min candle to complete (enter at 09:25 bar or later)
#   15 → wait for 15-min candle to complete (enter at 09:30 bar or later)
#
# After the wait expires, entry happens on the first available ST flip or
# DailyReEntry — whatever direction ST shows at that bar.
GAP_STOP_REENTRY_WAIT = 10   # minutes. Try 0, 10, 15

TRADE_START  = dtime(9, 15)
EXPIRY_EXIT  = dtime(15, 15)

CAPITAL = 400_000

# ── CHARGES ────────────────────────────────────────────────────────────────────
BROKERAGE_PER_ORDER = 20
STT_SELL_PCT        = 0.001
TXN_CHARGE_PCT      = 0.0003553
SEBI_PER_CRORE      = 10
GST_PCT             = 0.18
STAMP_BUY_PCT       = 0.00003

# ── MARGIN ─────────────────────────────────────────────────────────────────────
SIMULATE_MARGIN      = True
MARGIN_BASE_SPOT     = 24_000
MARGIN_NORMAL_BASE   = 68_000
MARGIN_ELM_EFFECTIVE = 30_000
ELM_RAW_PCT          = 0.02
MARGIN_PEAK_BUFFER   = 1.10
PEAK_SNAPSHOT_TIMES  = [dtime(10, 0), dtime(11, 30), dtime(13, 0), dtime(14, 30)]

WARMUP_BARS_REQUIRED = ST_PERIOD * 3


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT FOLDER — dynamic name from config
# ══════════════════════════════════════════════════════════════════════════════

def make_output_dir(trade_start: str, trade_end: str) -> Path:
    """
    Creates and returns a uniquely-named output directory:
      backtest_ST{P}_{M}_{W}pt_{L}lots_{start}_{end}/
    e.g.  backtest_ST80_3.6_500pt_3lots_2024-10-01_2026-03-17/
    """
    name = (
        f"backtest_ST{ST_PERIOD}_{ST_MULTIPLIER}_"
        f"{SPREAD_WIDTH}pt_{NUM_LOTS}lots_"
        f"gap{OVERNIGHT_GAP_PTS}pts_"
        f"wait{GAP_STOP_REENTRY_WAIT}min_"
        f"{trade_start}_{trade_end}"
    )
    out = Path(BASE_DIR) / name
    out.mkdir(parents=True, exist_ok=True)
    print(f"  Output folder: {out}")
    return out


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 0 — DATA VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_data(con) -> dict:
    print("\n" + "=" * 90)
    print("PHASE 0 — DATA VALIDATION")
    print("=" * 90)
    results = {}

    # Spot data
    print("\n[1/5] Spot data ...")
    spot_meta = con.execute("""
        SELECT MIN(timestamp), MAX(timestamp), COUNT(*)
        FROM candle_data
        WHERE instrument_key = 'NSE_INDEX|Nifty 50' AND interval = '1minute'
    """).fetchone()
    spot_first = pd.Timestamp(spot_meta[0])
    spot_last  = pd.Timestamp(spot_meta[1])
    spot_total = spot_meta[2]
    print(f"  OK {spot_first.date()} to {spot_last.date()} ({spot_total:,} bars)")

    daily_bars = con.execute("""
        SELECT DATE(timestamp) AS d, COUNT(*) AS bars
        FROM candle_data
        WHERE instrument_key='NSE_INDEX|Nifty 50' AND interval='1minute'
        GROUP BY d ORDER BY d
    """).fetchdf()
    thin_days = daily_bars[daily_bars["bars"] < 200]
    gap_dates = []
    if len(thin_days):
        print(f"  WARNING: {len(thin_days)} sessions < 200 bars:")
        for _, r in thin_days.iterrows():
            print(f"     {r['d']}  -> {r['bars']} bars")
            gap_dates.append(str(r["d"]))
    else:
        print("  OK All sessions complete")
    results.update({"spot_start": spot_first, "spot_end": spot_last,
                    "spot_total_bars": spot_total, "spot_gaps": gap_dates})

    # Contracts
    print("\n[2/5] Option contracts ...")
    contracts = con.execute("""
        SELECT expired_instrument_key, strike_price, contract_type, expiry_date
        FROM contracts
        WHERE instrument_key='NSE_INDEX|Nifty 50' AND contract_type IN ('CE','PE')
        ORDER BY expiry_date, strike_price
    """).fetchdf()
    contracts["expiry_date"]  = pd.to_datetime(contracts["expiry_date"]).dt.date
    contracts["strike_price"] = contracts["strike_price"].astype(float)
    all_expiries = sorted(contracts["expiry_date"].unique())
    print(f"  OK {len(contracts):,} contracts, {len(all_expiries)} expiries "
          f"({all_expiries[0]} to {all_expiries[-1]})")
    results.update({"all_expiries": all_expiries,
                    "options_contract_count": len(contracts)})

    # Option OHLC coverage
    print("\n[3/5] Option OHLC coverage per expiry ...")
    opt_meta = con.execute("""
        SELECT c.expiry_date, c.contract_type,
               COUNT(DISTINCT h.expired_instrument_key) AS contracts_with_data,
               COUNT(h.timestamp) AS total_rows,
               MIN(h.timestamp) AS first_ts, MAX(h.timestamp) AS last_ts
        FROM historical_data h
        JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
        WHERE c.instrument_key='NSE_INDEX|Nifty 50' AND c.contract_type IN ('CE','PE')
        GROUP BY c.expiry_date, c.contract_type
        ORDER BY c.expiry_date, c.contract_type
    """).fetchdf()
    opt_meta["expiry_date"] = pd.to_datetime(opt_meta["expiry_date"]).dt.date

    expiry_coverage = defaultdict(dict)
    for _, row in opt_meta.iterrows():
        expiry_coverage[row["expiry_date"]][row["contract_type"]] = {
            "contracts": int(row["contracts_with_data"]),
            "rows":  int(row["total_rows"]),
            "first": str(row["first_ts"]),
            "last":  str(row["last_ts"]),
        }

    missing_expiry_data = [e for e in all_expiries if e not in expiry_coverage]
    if missing_expiry_data:
        print(f"  FAIL: {len(missing_expiry_data)} expiries with NO OHLC:")
        for e in missing_expiry_data:
            print(f"    {e}")
    else:
        print(f"  OK All {len(all_expiries)} expiries have OHLC data")
    results.update({"expiry_coverage": dict(expiry_coverage),
                    "missing_expiry_dates": missing_expiry_data})

    # Re-downloaded expiry check
    REDOWNLOADED = [
        '2025-02-06', '2025-02-13', '2025-02-20', '2025-02-27',
        '2025-03-06', '2025-03-13', '2025-03-20', '2025-03-27',
        '2026-03-02', '2026-03-10', '2026-03-17',
    ]
    print(f"\n[4/5] Re-downloaded expiry dates check ...")
    for exp_str in REDOWNLOADED:
        exp_dt = pd.Timestamp(exp_str).date()
        if exp_dt in expiry_coverage:
            cov = expiry_coverage[exp_dt]
            ce  = cov.get("CE", {})
            pe  = cov.get("PE", {})
            print(f"  OK {exp_str}  CE:{ce.get('contracts',0)}c/{ce.get('rows',0):,}r "
                  f" PE:{pe.get('contracts',0)}c/{pe.get('rows',0):,}r "
                  f" last:{ce.get('last','?')}")
        else:
            print(f"  FAIL {exp_str} — STILL MISSING")

    # Effective range
    print(f"\n[5/5] Effective backtest range ...")
    warmup_minutes   = WARMUP_BARS_REQUIRED * 5
    warmup_end_ts    = spot_first + timedelta(minutes=warmup_minutes)
    trade_start_date = warmup_end_ts.date()
    options_first    = pd.Timestamp(opt_meta["first_ts"].min())
    effective_start  = max(trade_start_date, options_first.date())
    effective_end    = spot_last.date()
    print(f"  OK {effective_start}  to  {effective_end}")
    results.update({"warmup_end": warmup_end_ts,
                    "trade_start_date": effective_start,
                    "trade_end_date":   effective_end,
                    "options_start":    options_first})
    return results


# ══════════════════════════════════════════════════════════════════════════════
# SUPERTREND (exact copy from live)
# ══════════════════════════════════════════════════════════════════════════════

def compute_supertrend(highs, lows, closes, period, multiplier):
    n  = len(closes)
    tr = np.zeros(n)
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i],
                    abs(highs[i] - closes[i - 1]),
                    abs(lows[i]  - closes[i - 1]))
    alpha = 1.0 / period
    atr   = np.zeros(n)
    if n >= period:
        atr[period - 1] = np.mean(tr[:period])
        for i in range(period, n):
            atr[i] = alpha * tr[i] + (1 - alpha) * atr[i - 1]
    else:
        atr[:] = np.mean(tr[:n]) if n > 0 else 0.0
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
                    found = True
                    continue
                return exp
        return None

    def get_contract_key(self, strike, option_type, expiry):
        if isinstance(expiry, (datetime, pd.Timestamp)):
            expiry = expiry.date()
        return self.contract_index.get((float(strike), option_type, expiry))

    def get_option_price(self, contract_key, timestamp,
                         lookback_minutes=30, use_cache_fallback=True):
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
    df["volume"] = df["volume"].astype(int)
    print(f"  1-min: {len(df):,} bars ({df.index[0]} to {df.index[-1]})")
    df_5m = df.resample(TIMEFRAME).agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum"
    }).dropna()
    print(f"  5-min: {len(df_5m):,} bars")
    return df_5m


# ══════════════════════════════════════════════════════════════════════════════
# MARGIN
# ══════════════════════════════════════════════════════════════════════════════

def compute_margin(spot, is_expiry_day):
    scale         = spot / MARGIN_BASE_SPOT
    normal        = MARGIN_NORMAL_BASE * scale
    elm_component = (MARGIN_ELM_EFFECTIVE * scale) if is_expiry_day else 0.0
    margin        = normal + elm_component
    margin_req    = margin * MARGIN_PEAK_BUFFER
    elm_raw       = ELM_RAW_PCT * spot * LOT_SIZE * NUM_LOTS
    return margin, margin_req, elm_component, elm_raw


# ══════════════════════════════════════════════════════════════════════════════
# EXIT TRADE
# ══════════════════════════════════════════════════════════════════════════════

def _exit_trade(provider, trade_data, exit_time, reason, use_settlement=False):
    sell_exit, _ = provider.get_option_price(
        trade_data["sell_key"], exit_time, use_cache_fallback=True)
    buy_exit, _  = provider.get_option_price(
        trade_data["buy_key"],  exit_time, use_cache_fallback=True)
    if use_settlement:
        expiry = trade_data["expiry"]
        if sell_exit is None:
            sell_exit = provider.get_option_price_at_expiry_settlement(
                trade_data["sell_key"], expiry)
        if buy_exit is None:
            buy_exit = provider.get_option_price_at_expiry_settlement(
                trade_data["buy_key"], expiry)
    if sell_exit is None or buy_exit is None:
        return None

    exit_spread  = sell_exit - buy_exit
    pnl_per_unit = trade_data["net_credit"] - exit_spread
    gross_pnl    = pnl_per_unit * QTY

    se = trade_data["sell_premium"]
    be = trade_data["buy_premium"]
    brokerage = BROKERAGE_PER_ORDER * 4
    stt       = STT_SELL_PCT * (se * QTY + sell_exit * QTY + buy_exit * QTY)
    turnover  = (se + be + sell_exit + buy_exit) * QTY
    txn       = TXN_CHARGE_PCT * turnover
    sebi      = SEBI_PER_CRORE * turnover / 1e7
    gst       = GST_PCT * (brokerage + txn + sebi)
    stamp     = STAMP_BUY_PCT * (be * QTY + sell_exit * QTY)
    charges   = brokerage + stt + txn + sebi + gst + stamp
    total_pnl = gross_pnl - charges

    entry_dt  = pd.Timestamp(trade_data["entry_time"])
    exit_dt   = pd.Timestamp(exit_time)
    days_held = (exit_dt.date() - entry_dt.date()).days

    return {
        "entry_time":     trade_data["entry_time"],
        "exit_time":      exit_time,
        "type":           trade_data["spread_type"],
        "entry_spot":     trade_data["entry_spot"],
        "sell_strike":    trade_data["sell_strike"],
        "buy_strike":     trade_data["buy_strike"],
        "sell_entry":     se,
        "buy_entry":      be,
        "sell_exit":      sell_exit,
        "buy_exit":       buy_exit,
        "net_credit":     trade_data["net_credit"],
        "exit_spread":    exit_spread,
        "pnl_per_unit":   pnl_per_unit,
        "gross_pnl":      gross_pnl,
        "charges":        charges,
        "total_pnl":      total_pnl,
        "exit_reason":    reason,
        "expiry":         trade_data["expiry"],
        "days_held":      days_held,
        "overnight":      days_held > 0,
        "is_next_expiry": trade_data.get("is_next_expiry", False),
        "qty":            QTY,
        "equity_at_entry":  trade_data.get("equity_at_entry", 0),
        "margin_at_entry":  trade_data.get("margin_at_entry", 0),
        "margin_req":       trade_data.get("margin_req", 0),
        "elm_component":    trade_data.get("elm_component", 0),
        "margin_util_pct":  trade_data.get("margin_util_pct", 0),
    }


# ══════════════════════════════════════════════════════════════════════════════
# SIMULATION
# ══════════════════════════════════════════════════════════════════════════════

def run_simulation(df, provider, trade_start_date, trade_end_date):
    close = df["close"].values.astype(float)
    high  = df["high"].values.astype(float)
    low   = df["low"].values.astype(float)
    n     = len(close)
    st_dir, _ = compute_supertrend(high, low, close, ST_PERIOD, ST_MULTIPLIER)

    start_idx = 1
    for idx in range(1, n):
        if df.index[idx].date() >= trade_start_date:
            start_idx = idx
            break
    end_idx = n
    for idx in range(n - 1, 0, -1):
        if df.index[idx].date() <= trade_end_date:
            end_idx = idx + 1
            break

    print(f"\n  Warmup bars   : {start_idx}")
    print(f"  Trading range : {df.index[start_idx].date()} to "
          f"{df.index[end_idx-1].date()} ({end_idx - start_idx:,} bars)")

    # Pre-compute each trading day's LAST bar close.
    # Used by gap stop: compare today's 09:15 open vs yesterday's session close.
    # This mirrors live strategy where saved_spot = bar_closes[-1] of previous day.
    daily_last_close = {}
    _cur_date = None
    for idx in range(n):
        d = df.index[idx].date()
        if d != _cur_date:
            _cur_date = d
        daily_last_close[d] = close[idx]   # overwritten each bar → ends up as last

    trades = []; total_pnl = 0.0; running_pnl = 0.0
    in_trade = False; trade_data = {}
    signal_log = []
    skipped_no_contract = 0; skipped_no_price = 0
    skipped_negative_credit = 0; skipped_used_next_expiry = 0
    skipped_no_exit_price = 0; skipped_margin = 0
    target_reentries = 0; pending_flip_entries = 0
    reentry_when_flat = 0; total_signals = 0
    in_trade_price_failures = 0
    gap_stop_exits = 0          # count of overnight gap stop exits
    gap_reentry_wait_until = None  # datetime: block new entries until this bar
    pending_flip = None
    peak_violations = 0; peak_violation_dates = []
    _last_snapshot_date = None; _snapshots_checked = set()

    def _try_enter(bar_time, bar_date, spot, direction, trigger="Signal"):
        nonlocal skipped_no_contract, skipped_no_price, skipped_negative_credit
        nonlocal skipped_used_next_expiry, skipped_margin
        atm_strike     = round(spot / STRIKE_INTERVAL) * STRIKE_INTERVAL
        nearest_expiry = provider.get_nearest_expiry(bar_time)
        is_expiry_day  = nearest_expiry is not None and bar_date == nearest_expiry
        if is_expiry_day:
            expiry = provider.get_next_expiry(bar_time)
            if expiry:
                skipped_used_next_expiry += 1
            else:
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
        if direction == "BULLISH":
            sell_strike = atm_strike; buy_strike = atm_strike - SPREAD_WIDTH
            opt_type = "PE"; spread_type = "BULL_PUT"
        else:
            sell_strike = atm_strike; buy_strike = atm_strike + SPREAD_WIDTH
            opt_type = "CE"; spread_type = "BEAR_CALL"
        sell_key = provider.get_contract_key(sell_strike, opt_type, expiry)
        buy_key  = provider.get_contract_key(buy_strike,  opt_type, expiry)
        if sell_key is None or buy_key is None:
            skipped_no_contract += 1
            signal_log.append({"bar": str(bar_time), "dir": direction,
                "trigger": trigger,
                "outcome": (f"FAIL:no_contract sell={'OK' if sell_key else 'MISS'} "
                            f"buy={'OK' if buy_key else 'MISS'} exp={expiry}")})
            return None
        sell_premium, _ = provider.get_option_price(sell_key, bar_time,
                                                     use_cache_fallback=False)
        buy_premium, _  = provider.get_option_price(buy_key,  bar_time,
                                                     use_cache_fallback=False)
        if sell_premium is None or buy_premium is None:
            skipped_no_price += 1
            signal_log.append({"bar": str(bar_time), "dir": direction,
                "trigger": trigger,
                "outcome": (f"FAIL:no_price sell={'OK' if sell_premium else 'MISS'} "
                            f"buy={'OK' if buy_premium else 'MISS'} exp={expiry}")})
            return None
        net_credit = sell_premium - buy_premium
        if net_credit <= 0:
            skipped_negative_credit += 1
            signal_log.append({"bar": str(bar_time), "dir": direction,
                "trigger": trigger, "outcome": f"FAIL:neg_credit {net_credit:.2f}"})
            return None
        equity_now = CAPITAL + running_pnl
        margin_at, margin_req, elm_comp, elm_raw = compute_margin(spot, is_expiry_day)
        if SIMULATE_MARGIN and equity_now < margin_req:
            skipped_margin += 1
            signal_log.append({"bar": str(bar_time), "dir": direction,
                "trigger": trigger, "outcome": "FAIL:margin"})
            return None
        signal_log.append({"bar": str(bar_time), "dir": direction,
            "trigger": trigger,
            "outcome": (f"ENTERED {spread_type} exp={expiry} "
                        f"sell={sell_strike} buy={buy_strike} cr={net_credit:.2f}")})
        return {
            "spread_type": spread_type, "entry_time": bar_time,
            "entry_spot": spot, "atm_strike": atm_strike,
            "sell_strike": sell_strike, "buy_strike": buy_strike,
            "opt_type": opt_type, "sell_key": sell_key, "buy_key": buy_key,
            "sell_premium": sell_premium, "buy_premium": buy_premium,
            "net_credit": net_credit, "expiry": expiry,
            "is_next_expiry": is_expiry_day,
            "equity_at_entry": round(equity_now),
            "margin_at_entry": round(margin_at),
            "margin_req": round(margin_req),
            "elm_component": round(elm_comp),
            "elm_raw": round(elm_raw),
            "margin_util_pct": round(margin_at / equity_now * 100, 1)
                               if equity_now > 0 else 0,
        }

    for i in range(start_idx, end_idx):
        bar_time = df.index[i]
        t        = bar_time.time()
        bar_date = bar_time.date()
        spot     = close[i]

        # SEBI peak snapshots
        if SIMULATE_MARGIN and in_trade:
            if bar_date != _last_snapshot_date:
                _last_snapshot_date = bar_date
                _snapshots_checked  = set()
            if t in PEAK_SNAPSHOT_TIMES and t not in _snapshots_checked:
                _snapshots_checked.add(t)
                ne = provider.get_nearest_expiry(bar_time)
                is_exp = ne is not None and bar_date == ne
                _, mr, _, _ = compute_margin(spot, is_exp)
                eq = CAPITAL + running_pnl
                if eq < mr:
                    peak_violations += 1
                    peak_violation_dates.append({
                        "date": str(bar_date), "time": str(t),
                        "equity": round(eq), "margin_req": round(mr),
                        "shortfall": round(mr - eq), "is_expiry": is_exp,
                    })

        # ── OVERNIGHT GAP STOP ────────────────────────────────────────────────
        # Mirrors live strategy exactly:
        #   - Only fires at the 09:15 bar (first bar of the day)
        #   - Only fires if we hold an overnight position (entered on a prior day)
        #   - Compares today's 09:15 bar OPEN vs previous trading day's LAST close
        #   - BULL_PUT: exit if gap down >= OVERNIGHT_GAP_PTS (put goes ITM)
        #   - BEAR_CALL: exit if gap up   >= OVERNIGHT_GAP_PTS (call goes ITM)
        #   - After exit: do NOT re-enter immediately — wait for 09:20 bar ST flip
        #     (same as live: st_dir at 09:15 is stale; gap bar must close first)
        if (in_trade
                and t == TRADE_START
                and OVERNIGHT_GAP_PTS < 99999
                and pd.Timestamp(trade_data["entry_time"]).date() < bar_date):

            # Find previous trading day by walking back through daily_last_close
            prev_dates = sorted(d for d in daily_last_close if d < bar_date)
            if prev_dates:
                prev_close  = daily_last_close[prev_dates[-1]]
                today_open  = df["open"].iloc[i]   # 09:15 bar open = actual gap open
                gap_move    = today_open - prev_close
                is_bull_put  = trade_data["spread_type"] == "BULL_PUT"
                adverse_gap  = (is_bull_put  and gap_move <= -OVERNIGHT_GAP_PTS) or \
                               (not is_bull_put and gap_move >=  OVERNIGHT_GAP_PTS)

                if adverse_gap:
                    # Exit at the 09:15 bar using OPEN price (most realistic fill)
                    # We pass bar_time (09:15 timestamp) to _exit_trade which looks
                    # up option prices at that timestamp with 30-min lookback fallback
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "GapStop")
                    if exit_pnl:
                        trades.append(exit_pnl)
                        total_pnl    += exit_pnl["total_pnl"]
                        running_pnl  += exit_pnl["total_pnl"]
                        in_trade      = False
                        gap_stop_exits += 1
                        pending_flip  = None   # clear any stale pending flip
                        # Set re-entry wait window. Bar timestamps mark bar START:
                        #   09:15 bar → wait 10min → first allowed entry at 09:25 bar
                        #   09:15 bar → wait 15min → first allowed entry at 09:30 bar
                        #   09:15 bar → wait  0min → first allowed entry at 09:20 bar (no block)
                        gap_reentry_wait_until = (
                            bar_time + timedelta(minutes=GAP_STOP_REENTRY_WAIT)
                            if GAP_STOP_REENTRY_WAIT > 0 else None
                        )
                        if gap_reentry_wait_until:
                            print(f"  GapStop: re-entry blocked until {gap_reentry_wait_until.time()} "
                                  f"({GAP_STOP_REENTRY_WAIT}-min wait)")
                        continue
                    else:
                        # No option price at 09:15 — fall through to normal ST logic
                        skipped_no_exit_price += 1
                        in_trade_price_failures += 1
        # ── END GAP STOP ─────────────────────────────────────────────────────

        # Post-expiry force close
        if in_trade and bar_date > trade_data["expiry"]:
            expiry   = trade_data["expiry"]
            exit_pnl = None
            for hh, mm in [(15, 29), (15, 25), (15, 20), (15, 15),
                           (15, 10), (15, 5), (15, 0), (14, 55), (14, 45)]:
                candidate = pd.Timestamp(datetime.combine(expiry, dtime(hh, mm)))
                exit_pnl  = _exit_trade(provider, trade_data, candidate,
                                        "Expiry", use_settlement=True)
                if exit_pnl:
                    break
            if exit_pnl:
                trades.append(exit_pnl)
                total_pnl   += exit_pnl["total_pnl"]
                running_pnl += exit_pnl["total_pnl"]
            else:
                skipped_no_exit_price += 1
            in_trade = False

        # Expiry-day forced exit at 15:15
        if in_trade and t >= EXPIRY_EXIT and bar_date == trade_data["expiry"]:
            exit_pnl = _exit_trade(provider, trade_data, bar_time,
                                   "Expiry", use_settlement=True)
            if exit_pnl:
                trades.append(exit_pnl)
                total_pnl   += exit_pnl["total_pnl"]
                running_pnl += exit_pnl["total_pnl"]
                in_trade = False
                continue
            else:
                skipped_no_exit_price += 1
                continue

        # ST flip detection
        any_flip = None
        if i > 0:
            if   st_dir[i] == 1 and st_dir[i-1] == -1:
                any_flip = "BULLISH"; total_signals += 1
            elif st_dir[i] == -1 and st_dir[i-1] == 1:
                any_flip = "BEARISH"; total_signals += 1

        # In-trade monitoring
        if in_trade:
            if any_flip:
                should_exit = (
                    (trade_data["spread_type"] == "BULL_PUT"  and any_flip == "BEARISH") or
                    (trade_data["spread_type"] == "BEAR_CALL" and any_flip == "BULLISH")
                )
                if should_exit:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "Reversal")
                    if exit_pnl:
                        trades.append(exit_pnl)
                        total_pnl   += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]
                        in_trade = False
                        new_td = _try_enter(bar_time, bar_date, spot,
                                            any_flip, "Reversal-ReEntry")
                        if new_td:
                            trade_data   = new_td
                            in_trade     = True
                            pending_flip = None
                        else:
                            pending_flip = any_flip
                        continue
                    else:
                        skipped_no_exit_price   += 1
                        in_trade_price_failures += 1
                        continue

            sp, _ = provider.get_option_price(trade_data["sell_key"], bar_time,
                                              use_cache_fallback=True)
            bp, _ = provider.get_option_price(trade_data["buy_key"],  bar_time,
                                              use_cache_fallback=True)
            if sp is not None and bp is not None:
                curr_spread = sp - bp
                if curr_spread >= SPREAD_WIDTH * MAX_LOSS_PCT:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "MaxLoss")
                    if exit_pnl:
                        trades.append(exit_pnl)
                        total_pnl   += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]
                        in_trade = False
                        continue
                    else:
                        skipped_no_exit_price   += 1
                        in_trade_price_failures += 1
                        continue
                ec = trade_data["net_credit"]
                if ec > 0 and (ec - curr_spread) >= ec * TARGET_PCT:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "Target")
                    if exit_pnl:
                        trades.append(exit_pnl)
                        total_pnl   += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]
                        in_trade = False
                        curr_dir = "BULLISH" if st_dir[i] == 1 else "BEARISH"
                        new_td = _try_enter(bar_time, bar_date, spot,
                                            curr_dir, "Target-ReEntry")
                        if new_td:
                            trade_data = new_td
                            in_trade   = True
                            target_reentries += 1
                        continue
                    else:
                        skipped_no_exit_price   += 1
                        in_trade_price_failures += 1
            else:
                in_trade_price_failures += 1

        # Flip when flat
        if any_flip and not in_trade:
            # Respect gap stop re-entry wait window
            if gap_reentry_wait_until and bar_time < gap_reentry_wait_until:
                pending_flip = any_flip   # remember direction, enter when wait expires
            else:
                gap_reentry_wait_until = None   # clear once window has passed
                new_td = _try_enter(bar_time, bar_date, spot, any_flip, "Flip")
                if new_td:
                    trade_data   = new_td
                    in_trade     = True
                    pending_flip = None
                else:
                    pending_flip = any_flip

        # Pending flip retry till 09:45
        if pending_flip and not in_trade and t >= TRADE_START:
            # Respect gap stop re-entry wait window
            if gap_reentry_wait_until and bar_time < gap_reentry_wait_until:
                pass   # keep pending_flip alive, try again next bar
            else:
                gap_reentry_wait_until = None   # clear once window has passed
                new_td = _try_enter(bar_time, bar_date, spot, pending_flip, "PendingFlip")
                if new_td:
                    trade_data           = new_td
                    in_trade             = True
                    pending_flip_entries += 1
                    pending_flip         = None
                elif t >= dtime(9, 45):
                    pending_flip = None

        # Daily re-entry at 09:15
        if not in_trade and t == TRADE_START:
            # Respect gap stop re-entry wait window (will be None on normal days)
            if gap_reentry_wait_until and bar_time < gap_reentry_wait_until:
                pass   # gap stop just fired this bar — wait window starts next bar
            else:
                gap_reentry_wait_until = None
                pending_flip = None
                curr_dir = "BULLISH" if st_dir[i] == 1 else "BEARISH"
                new_td = _try_enter(bar_time, bar_date, spot, curr_dir, "DailyReEntry")
                if new_td:
                    trade_data        = new_td
                    in_trade          = True
                    reentry_when_flat += 1

    # DataEnd force close
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
            print(f"  DataEnd close P&L: Rs {exit_pnl['total_pnl']:+,.0f}")
        else:
            print("  WARNING: DataEnd close failed — dropped!")
            skipped_no_exit_price += 1

    print(f"\n  Counters:")
    print(f"  ST flip signals             : {total_signals}")
    print(f"  Pending flip entries        : {pending_flip_entries}")
    print(f"  Daily re-entries (09:15)    : {reentry_when_flat}")
    print(f"  Target re-entries           : {target_reentries}")
    print(f"  Overnight gap stop exits    : {gap_stop_exits}  (threshold: {OVERNIGHT_GAP_PTS} pts)")
    print(f"  Gap stop re-entry wait      : {GAP_STOP_REENTRY_WAIT} min  "
          f"({'enter at 09:20' if GAP_STOP_REENTRY_WAIT==0 else f'enter at 09:{15+GAP_STOP_REENTRY_WAIT:02d}+'})")
    print(f"  Used next-week expiry       : {skipped_used_next_expiry}")
    print(f"  Skipped - no contract       : {skipped_no_contract}")
    print(f"  Skipped - no price (entry)  : {skipped_no_price}")
    print(f"  Skipped - negative credit   : {skipped_negative_credit}")
    print(f"  Skipped - margin            : {skipped_margin}")
    print(f"  Dropped - no exit price     : {skipped_no_exit_price}")
    print(f"  In-trade price failures     : {in_trade_price_failures}")
    if SIMULATE_MARGIN:
        print(f"  SEBI peak violations        : {peak_violations}")
    print(f"  Executed trades             : {len(trades)}")

    return trades, total_pnl, signal_log, peak_violations, peak_violation_dates, gap_stop_exits


# ══════════════════════════════════════════════════════════════════════════════
# HTML DASHBOARD GENERATOR
# ══════════════════════════════════════════════════════════════════════════════

def build_dashboard(tdf: pd.DataFrame, total_pnl: float,
                    signal_log: list, peak_viol: int,
                    peak_viol_dates: list, gap_stop_exits: int,
                    out_dir: Path) -> Path:
    """
    Generates a fully self-contained interactive HTML dashboard.
    Uses Chart.js CDN for charts (no Plotly required).
    All data is embedded as JSON — single HTML file, works offline
    once Chart.js is cached by the browser.
    """
    wins   = tdf[tdf["total_pnl"] > 0]
    losses = tdf[tdf["total_pnl"] <= 0]
    n_tr   = len(tdf)
    wr     = len(wins) / n_tr * 100 if n_tr > 0 else 0
    pf     = (wins["total_pnl"].sum() / abs(losses["total_pnl"].sum())
              if len(losses) > 0 and losses["total_pnl"].sum() != 0 else 99)
    roi    = total_pnl / CAPITAL * 100
    cum    = tdf["total_pnl"].cumsum()
    mdd    = (cum.cummax() - cum).max()
    avg_win  = wins["total_pnl"].mean()   if len(wins)   > 0 else 0
    avg_loss = losses["total_pnl"].mean() if len(losses) > 0 else 0
    rr_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0

    # Monthly aggregates
    tdf = tdf.copy()
    tdf["month"] = tdf["entry_time"].dt.to_period("M")
    monthly_list = []
    for month, grp in tdf.groupby("month"):
        monthly_list.append({
            "month":    str(month),
            "trades":   int(len(grp)),
            "wins":     int((grp["total_pnl"] > 0).sum()),
            "losses":   int((grp["total_pnl"] <= 0).sum()),
            "pnl":      round(float(grp["total_pnl"].sum()), 0),
            "wr":       round(float((grp["total_pnl"] > 0).mean() * 100), 1),
            "overnight": int(grp["overnight"].sum()),
            "nxexp":    int(grp["is_next_expiry"].sum()),
            "avg_credit": round(float(grp["net_credit"].mean()), 2),
            "avg_days": round(float(grp["days_held"].mean()), 1),
            "target":   int((grp["exit_reason"] == "Target").sum()),
            "reversal": int((grp["exit_reason"] == "Reversal").sum()),
        })

    # Days held
    days_data = []
    for d in sorted(tdf["days_held"].unique()):
        sub = tdf[tdf["days_held"] == d]
        days_data.append({
            "days":  int(d),
            "trades": int(len(sub)),
            "wr":    round(float((sub["total_pnl"] > 0).mean() * 100), 1),
            "avg":   round(float(sub["total_pnl"].mean()), 0),
            "total": round(float(sub["total_pnl"].sum()), 0),
        })

    # Exit reasons
    exit_data = {}
    for reason, grp in tdf.groupby("exit_reason"):
        exit_data[reason] = {
            "count":    int(len(grp)),
            "wr":       round(float((grp["total_pnl"] > 0).mean() * 100), 1),
            "avg":      round(float(grp["total_pnl"].mean()), 0),
            "total":    round(float(grp["total_pnl"].sum()), 0),
            "avg_days": round(float(grp["days_held"].mean()), 1),
        }

    # Overnight / intraday
    ot = tdf[tdf["overnight"]]
    it = tdf[~tdf["overnight"]]
    overnight_stats = {
        "trades": int(len(ot)),
        "wr":     round(float((ot["total_pnl"] > 0).mean() * 100), 1) if len(ot) else 0,
        "avg":    round(float(ot["total_pnl"].mean()), 0) if len(ot) else 0,
        "total":  round(float(ot["total_pnl"].sum()), 0) if len(ot) else 0,
    }
    intraday_stats = {
        "trades": int(len(it)),
        "wr":     round(float((it["total_pnl"] > 0).mean() * 100), 1) if len(it) else 0,
        "avg":    round(float(it["total_pnl"].mean()), 0) if len(it) else 0,
        "total":  round(float(it["total_pnl"].sum()), 0) if len(it) else 0,
    }

    # Type breakdown
    type_data = {}
    for t in ["BULL_PUT", "BEAR_CALL"]:
        sub = tdf[tdf["type"] == t]
        if len(sub) > 0:
            type_data[t] = {
                "trades": int(len(sub)),
                "wr":     round(float((sub["total_pnl"] > 0).mean() * 100), 1),
                "total":  round(float(sub["total_pnl"].sum()), 0),
            }

    # P&L distribution
    bins = [
        ("<-30K",     -1e9, -30000),
        ("-30K to -15K", -30000, -15000),
        ("-15K to 0",  -15000, 0),
        ("0 to 5K",    0, 5000),
        ("5K to 15K",  5000, 15000),
        ("15K to 30K", 15000, 30000),
        (">30K",       30000, 1e9),
    ]
    dist_data = []
    for label, lo, hi in bins:
        count = int(((tdf["total_pnl"] > lo) & (tdf["total_pnl"] <= hi)).sum())
        dist_data.append({"label": label, "count": count})

    # Trade P&L series (for drawdown chart)
    tdf["cum_pnl"]  = tdf["total_pnl"].cumsum()
    tdf["drawdown"] = tdf["cum_pnl"].cummax() - tdf["cum_pnl"]

    # Signal audit
    failed_sigs = [s for s in signal_log if s["outcome"].startswith("FAIL")]
    fail_by_month: dict = defaultdict(int)
    for s in failed_sigs:
        fail_by_month[s["bar"][:7]] += 1
    fail_reasons: dict = defaultdict(int)
    for s in failed_sigs:
        fail_reasons[s["outcome"].split(":")[1].split(" ")[0]] += 1

    # Trading gaps
    tdf_s = tdf.sort_values("entry_time")
    tdf_s["prev_exit"] = tdf_s["exit_time"].shift(1)
    tdf_s["gap_days"] = (
        tdf_s["entry_time"] - tdf_s["prev_exit"]
    ).dt.total_seconds() / 86400
    gaps = []
    for _, row in tdf_s[tdf_s["gap_days"] > 3].dropna().iterrows():
        gaps.append({
            "start": pd.Timestamp(row["prev_exit"]).strftime("%Y-%m-%d"),
            "end":   pd.Timestamp(row["entry_time"]).strftime("%Y-%m-%d"),
            "days":  round(float(row["gap_days"]), 1),
        })

    # Compact trade records for JS (shorten key names to keep HTML size down)
    trades_js = []
    for _, r in tdf.iterrows():
        trades_js.append({
            "en": pd.Timestamp(r["entry_time"]).strftime("%Y-%m-%dT%H:%M"),
            "ex": pd.Timestamp(r["exit_time"]).strftime("%Y-%m-%dT%H:%M"),
            "ty": r["type"],
            "re": r["exit_reason"],
            "dy": int(r["days_held"]),
            "pl": round(float(r["total_pnl"]), 0),
            "cu": round(float(r["cum_pnl"]), 0),
            "dd": round(float(r["drawdown"]), 0),
            "cr": round(float(r["net_credit"]), 2),
            "sl": int(r["sell_strike"]),
            "bu": int(r["buy_strike"]),
            "nx": bool(r["is_next_expiry"]),
            "ov": bool(r["overnight"]),
            "sp": round(float(r["entry_spot"]), 0),
        })

    config_str = (f"ST({ST_PERIOD}, {ST_MULTIPLIER}) · {TIMEFRAME} · "
                  f"NIFTY {SPREAD_WIDTH}pt Bull-Put / Bear-Call · "
                  f"{NUM_LOTS} lots ({QTY} qty) · Capital ₹{CAPITAL:,}")
    date_range = (
        f"{pd.Timestamp(tdf['entry_time'].min()).strftime('%b %Y')} → "
        f"{pd.Timestamp(tdf['exit_time'].max()).strftime('%b %Y')}"
    )

    # ── Gap stop statistics ────────────────────────────────────────────────────
    gap_stop_trades = tdf[tdf["exit_reason"] == "GapStop"]
    n_gap = len(gap_stop_trades)
    gap_pnl_total = round(float(gap_stop_trades["total_pnl"].sum()), 0) if n_gap > 0 else 0
    gap_pnl_avg   = round(float(gap_stop_trades["total_pnl"].mean()), 0) if n_gap > 0 else 0
    gap_wr        = round(float((gap_stop_trades["total_pnl"] > 0).mean() * 100), 1) if n_gap > 0 else 0

    # Gap stop trades by month
    gap_by_month = {}
    if n_gap > 0:
        for month, grp in gap_stop_trades.groupby("month"):
            gap_by_month[str(month)] = {
                "count": int(len(grp)),
                "pnl":   round(float(grp["total_pnl"].sum()), 0),
                "wr":    round(float((grp["total_pnl"] > 0).mean() * 100), 1),
            }

    # Max loss threshold in points
    max_loss_pts = round(SPREAD_WIDTH * MAX_LOSS_PCT, 0)

    # Embed all data as JSON constants
    data_js = f"""
const CONFIG={{
  st_period:{ST_PERIOD},
  st_mult:{ST_MULTIPLIER},
  spread:{SPREAD_WIDTH},
  lots:{NUM_LOTS},
  qty:{QTY},
  capital:{CAPITAL},
  target_pct:{TARGET_PCT},
  max_loss_pct:{MAX_LOSS_PCT},
  max_loss_pts:{max_loss_pts},
  strike_interval:{STRIKE_INTERVAL},
  timeframe:{json.dumps(TIMEFRAME)},
  warmup_bars:{WARMUP_BARS_REQUIRED},
  overnight_gap_pts:{OVERNIGHT_GAP_PTS},
  gap_reentry_wait:{GAP_STOP_REENTRY_WAIT},
  gap_stop_enabled:{str(OVERNIGHT_GAP_PTS < 99999).lower()},
  config_str:{json.dumps(config_str)},
  date_range:{json.dumps(date_range)}
}};
const SUMMARY={{
  total_pnl:{round(total_pnl,0)},
  roi:{round(roi,1)},
  wr:{round(wr,1)},
  pf:{round(pf,2)},
  mdd:{round(mdd,0)},
  avg_win:{round(avg_win,0)},
  avg_loss:{round(avg_loss,0)},
  rr_ratio:{round(rr_ratio,2)},
  n_trades:{n_tr},
  n_wins:{len(wins)},
  n_losses:{len(losses)},
  peak_violations:{peak_viol},
  gap_stop_exits:{gap_stop_exits},
  gap_stop_n:{n_gap},
  gap_stop_pnl:{gap_pnl_total},
  gap_stop_avg:{gap_pnl_avg},
  gap_stop_wr:{gap_wr}
}};
const MONTHLY={json.dumps(monthly_list)};
const DAYS_DATA={json.dumps(days_data)};
const EXIT_DATA={json.dumps(exit_data)};
const TYPE_DATA={json.dumps(type_data)};
const DIST_DATA={json.dumps(dist_data)};
const OT_STATS={json.dumps(overnight_stats)};
const IT_STATS={json.dumps(intraday_stats)};
const TRADES={json.dumps(trades_js)};
const GAPS={json.dumps(gaps)};
const FAIL_REASONS={json.dumps(dict(fail_reasons))};
const FAIL_BY_MONTH={json.dumps(dict(fail_by_month))};
const GAP_BY_MONTH={json.dumps(gap_by_month)};
const SIGNAL_TOTAL={len(signal_log)};
const SIGNAL_ENTERED={len([s for s in signal_log if s['outcome'].startswith('ENTERED')])};
const PEAK_VIOLATIONS={json.dumps(peak_viol_dates[:20])};
const GENERATED={json.dumps(datetime.now().strftime('%Y-%m-%d %H:%M'))};
"""

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title id="pageTitle">Backtest Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;700;800&display=swap');
:root{--bg:#070a0f;--surface:#0d1117;--border:#1e2c3a;--accent:#00d4ff;
--accent2:#ff6b35;--green:#00e676;--red:#ff3d57;--yellow:#ffd740;
--muted:#4a6278;--text:#c8d8e8;--text2:#7a9bb5;}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Syne',sans-serif;min-height:100vh;overflow-x:hidden}
.header{padding:26px 36px 18px;border-bottom:1px solid var(--border);
  background:linear-gradient(135deg,rgba(0,212,255,0.04),transparent)}
.header h1{font-size:1.5rem;font-weight:800;color:#fff;letter-spacing:-.5px}
.header h1 span{color:var(--accent)}
.header p{font-family:'Space Mono',monospace;font-size:.7rem;color:var(--text2);margin-top:4px}
.badges{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
.badge{font-family:'Space Mono',monospace;font-size:.63rem;padding:3px 9px;
  border-radius:3px;border:1px solid;font-weight:700}
.b-blue{border-color:var(--accent);color:var(--accent);background:rgba(0,212,255,.07)}
.b-green{border-color:var(--green);color:var(--green);background:rgba(0,230,118,.07)}
.b-orange{border-color:var(--accent2);color:var(--accent2);background:rgba(255,107,53,.07)}
.b-yellow{border-color:var(--yellow);color:var(--yellow);background:rgba(255,215,64,.07)}
.b-red{border-color:var(--red);color:var(--red);background:rgba(255,61,87,.07)}
.alerts{padding:0 36px}
.alert{display:flex;align-items:flex-start;gap:12px;margin-top:12px;padding:10px 14px;
  border-radius:4px;font-family:'Space Mono',monospace;font-size:.68rem;border-left:3px solid}
.al-red{background:rgba(255,61,87,.07);border-left-color:var(--red);color:#ff9aaa}
.al-yellow{background:rgba(255,215,64,.07);border-left-color:var(--yellow);color:var(--yellow)}
.main{padding:22px 36px}
.sec{font-family:'Space Mono',monospace;font-size:.65rem;letter-spacing:2px;color:var(--muted);
  text-transform:uppercase;margin-bottom:10px;display:flex;align-items:center;gap:10px}
.sec::after{content:'';flex:1;height:1px;background:var(--border)}
.metrics{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:16px}
.mc{background:var(--surface);border:1px solid var(--border);border-radius:6px;
  padding:14px 16px;position:relative;overflow:hidden;transition:border-color .2s}
.mc:hover{border-color:var(--accent)}
.mc::after{content:'';position:absolute;top:0;left:0;right:0;height:2px}
.mc-gr::after{background:var(--green)}.mc-bl::after{background:var(--accent)}
.mc-or::after{background:var(--accent2)}.mc-yw::after{background:var(--yellow)}
.mc-re::after{background:var(--red)}.mc-pu::after{background:#c084fc}
.ml{font-family:'Space Mono',monospace;font-size:.6rem;color:var(--text2);
  letter-spacing:.8px;text-transform:uppercase;margin-bottom:7px}
.mv{font-size:1.4rem;font-weight:800;letter-spacing:-1px;line-height:1}
.ms{font-family:'Space Mono',monospace;font-size:.62rem;color:var(--text2);margin-top:5px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:6px;
  padding:16px 18px;margin-bottom:12px}
.card h3{font-family:'Space Mono',monospace;font-size:.65rem;color:var(--text2);
  text-transform:uppercase;letter-spacing:.8px;margin-bottom:12px}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.g3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}
.g4{display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:12px}
table{width:100%;border-collapse:collapse}
th{font-family:'Space Mono',monospace;font-size:.58rem;color:var(--muted);text-align:left;
  padding:6px 8px;border-bottom:1px solid var(--border);letter-spacing:.5px;
  text-transform:uppercase;white-space:nowrap}
td{font-family:'Space Mono',monospace;font-size:.66rem;padding:6px 8px;
  border-bottom:1px solid rgba(30,44,58,.4);color:var(--text)}
tr:hover td{background:rgba(255,255,255,.02)}
td.pos{color:var(--green)} td.neg{color:var(--red)} td.hl{color:#fff;font-weight:700}
.wr{display:flex;align-items:center;gap:7px}
.wrt{flex:1;height:4px;background:var(--border);border-radius:2px;overflow:hidden}
.wrf{height:100%;border-radius:2px}
.wrn{font-family:'Space Mono',monospace;font-size:.63rem;min-width:34px;text-align:right}
.srow{display:flex;justify-content:space-between;align-items:center;padding:7px 0;
  border-bottom:1px solid rgba(30,44,58,.4);font-family:'Space Mono',monospace;font-size:.66rem}
.srow:last-child{border-bottom:none}
.gi{display:flex;justify-content:space-between;align-items:flex-start;
  padding:10px 12px;border-radius:4px;margin-bottom:8px;
  font-family:'Space Mono',monospace;font-size:.66rem}
.gi-r{background:rgba(255,61,87,.06);border:1px solid rgba(255,61,87,.2)}
.gi-y{background:rgba(255,215,64,.06);border:1px solid rgba(255,215,64,.2);color:var(--yellow)}
.filters{display:flex;gap:7px;flex-wrap:wrap;margin-bottom:10px}
.fb{font-family:'Space Mono',monospace;font-size:.6rem;padding:4px 10px;
  border:1px solid var(--border);background:transparent;color:var(--text2);
  border-radius:3px;cursor:pointer;transition:all .15s}
.fb:hover,.fb.active{border-color:var(--accent);color:var(--accent);
  background:rgba(0,212,255,.07)}
.tscroll{max-height:360px;overflow-y:auto}
.tscroll::-webkit-scrollbar{width:3px}
.tscroll::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
.pgi{font-family:'Space Mono',monospace;font-size:.6rem;color:var(--muted);
  text-align:center;margin-top:8px}
.footer{border-top:1px solid var(--border);padding:12px 36px;display:flex;
  justify-content:space-between;font-family:'Space Mono',monospace;font-size:.6rem;color:var(--muted)}
@media(max-width:1100px){.metrics,.g2,.g3,.g4{grid-template-columns:1fr 1fr}}
</style>
</head>
<body>
<script>""" + data_js + """</script>
<div class="header">
  <h1>SuperTrend Credit Spread <span>Backtest</span></h1>
  <p id="cfgLine"></p>
  <div class="badges" id="hdBadges"></div>
</div>
<div class="alerts" id="alertsDiv"></div>
<div class="main">
  <div class="metrics" id="metricsGrid"></div>

  <div class="sec">Configuration <span style="font-size:.55rem;letter-spacing:1px;color:#3d5068;margin-left:6px">ALL PARAMETERS USED IN THIS BACKTEST RUN</span></div>
  <div class="card" style="margin-bottom:12px" id="configPanel"></div>

  <div class="sec" id="gapStopSec">Gap Stop Analysis <span style="font-size:.55rem;letter-spacing:1px;color:#3d5068;margin-left:6px">OVERNIGHT GAP PROTECTION</span></div>
  <div id="gapStopPanel" style="margin-bottom:12px"></div>

  <div class="sec">Performance Curve</div>
  <div class="card"><h3>Cumulative P&L + Monthly Bars + Drawdown</h3>
    <div style="height:270px"><canvas id="cumC"></canvas></div></div>
  <div class="sec">Monthly Breakdown</div>
  <div class="g2">
    <div class="card"><h3>Monthly P&L (Rs)</h3>
      <div style="height:220px"><canvas id="mthC"></canvas></div></div>
    <div class="card"><h3>Win Rate &amp; Trades per Month</h3>
      <div style="overflow-y:auto;max-height:240px">
        <table><thead><tr><th>Month</th><th>Trades</th><th>Win Rate</th>
        <th>P&amp;L</th><th>Target</th></tr></thead>
        <tbody id="mthTb"></tbody></table></div></div></div>
  <div class="sec">Strategy Insights</div>
  <div class="g4">
    <div class="card"><h3>Exit Reason P&amp;L</h3>
      <div style="height:190px"><canvas id="exC"></canvas></div></div>
    <div class="card"><h3>Bull Put vs Bear Call</h3>
      <div style="height:190px"><canvas id="tyC"></canvas></div></div>
    <div class="card"><h3>Win Rate by Holding Period</h3>
      <div style="height:190px"><canvas id="dyC"></canvas></div></div>
    <div class="card"><h3>P&amp;L Distribution</h3>
      <div style="height:190px"><canvas id="diC"></canvas></div></div></div>
  <div class="sec">Overnight vs Intraday &amp; Signal Audit</div>
  <div class="g2">
    <div class="card"><h3>Overnight vs Intraday Deep Dive</h3>
      <table><thead><tr><th>Category</th><th>Trades</th><th>Win Rate</th>
      <th>Avg P&amp;L</th><th>Total P&amp;L</th></tr></thead>
      <tbody id="otTb"></tbody></table>
      <div style="margin-top:14px">
        <div style="font-family:'Space Mono',monospace;font-size:.6rem;color:var(--muted);
          text-transform:uppercase;letter-spacing:.8px;margin-bottom:8px">Win Rate by Days Held</div>
        <table><thead><tr><th>Days</th><th>Trades</th><th>Win Rate</th>
        <th>Avg P&amp;L</th><th>Total</th></tr></thead>
        <tbody id="dyTb"></tbody></table></div></div>
    <div class="card"><h3>Signal Audit &amp; Data Quality</h3>
      <div id="sigDiv"></div>
      <div style="margin-top:14px">
        <div style="font-family:'Space Mono',monospace;font-size:.6rem;color:var(--muted);
          text-transform:uppercase;letter-spacing:.8px;margin-bottom:8px">Signal Failures by Month</div>
        <div style="height:120px"><canvas id="faC"></canvas></div></div></div></div>
  <div class="sec">Data Gaps Detected</div>
  <div id="gapsDiv"></div>
  <div class="sec">Trade Log</div>
  <div class="card">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
      <h3 style="margin-bottom:0" id="tlTitle">All Trades</h3>
      <div class="filters">
        <button class="fb active" onclick="ft('ALL',this)">ALL</button>
        <button class="fb" onclick="ft('BULL_PUT',this)">BULL PUT</button>
        <button class="fb" onclick="ft('BEAR_CALL',this)">BEAR CALL</button>
        <button class="fb" onclick="ft('WIN',this)">WINNERS</button>
        <button class="fb" onclick="ft('LOSS',this)">LOSERS</button>
        <button class="fb" onclick="ft('Target',this)">TARGET</button>
        <button class="fb" onclick="ft('overnight',this)">OVERNIGHT</button>
        <button class="fb" onclick="ft('nxexp',this)">NEXT EXP</button>
        <button class="fb" onclick="ft('GapStop',this)" style="border-color:rgba(255,107,53,0.5);color:var(--accent2)">GAP STOP</button>
      </div></div>
    <div class="tscroll">
      <table><thead><tr><th>#</th><th>Entry</th><th>Exit</th><th>Type</th>
        <th>Sell</th><th>Buy</th><th>Credit</th><th>Days</th>
        <th>Reason</th><th>P&amp;L</th><th>Cumulative</th><th>Drawdown</th>
      </tr></thead><tbody id="tlTb"></tbody></table></div>
    <div class="pgi" id="pgi"></div></div>
</div>
<div class="footer">
  <span id="ftLeft"></span>
  <span id="ftRight"></span>
</div>
<script>
// Helpers
const fmt=v=>Math.round(v).toLocaleString('en-IN');
const CF={family:"'Space Mono',monospace",size:10,color:'#4a6278'};
const TT={backgroundColor:'#0d1117',borderColor:'#1e2c3a',borderWidth:1,titleFont:CF,bodyFont:CF};

// Page title & header
document.getElementById('pageTitle').textContent=
  `Backtest ST(${CONFIG.st_period},${CONFIG.st_mult}) ${CONFIG.spread}pt`;
document.getElementById('cfgLine').textContent=CONFIG.config_str;
document.getElementById('ftLeft').textContent=
  `ST(${CONFIG.st_period},${CONFIG.st_mult}) | NIFTY Weekly | ${CONFIG.spread}pt | ${CONFIG.lots}lots`;
document.getElementById('ftRight').textContent=`Generated: ${GENERATED}`;

document.getElementById('hdBadges').innerHTML=`
  <span class="badge b-blue">${CONFIG.date_range}</span>
  <span class="badge b-green">Rs ${Math.round(SUMMARY.total_pnl/100000*10)/10}L P&L</span>
  <span class="badge b-orange">${SUMMARY.n_trades} TRADES</span>
  <span class="badge b-yellow">${SUMMARY.wr}% WIN RATE</span>
  <span class="badge b-red">PF ${SUMMARY.pf}x</span>
  <span class="badge" style="border-color:#64748b;color:#94a3b8;background:rgba(100,116,139,.07)">${CONFIG.timeframe} bars · ${CONFIG.strike_interval}pt strikes</span>
  ${CONFIG.gap_stop_enabled
    ? `<span class="badge b-orange">GAP STOP ${CONFIG.overnight_gap_pts}pts · WAIT ${CONFIG.gap_reentry_wait}min</span>`
    : `<span class="badge" style="border-color:#3d5068;color:#3d5068">GAP STOP DISABLED</span>`
  }`;

// Alerts for gaps
const ad=document.getElementById('alertsDiv');
GAPS.forEach(g=>{
  const cls=g.days>5?'al-red':'al-yellow';
  ad.innerHTML+=`<div class="alert ${cls}"><span>!</span>
    <div><strong>DATA GAP: ${g.start} to ${g.end} (${g.days} days)</strong>
    — Zero trades. Option data missing. P&L impact unknown.</div></div>`;
});

// Metric cards
const mcDefs=[
  {cls:'mc-gr',c:'var(--green)',   lab:'Total P&L',   val:`Rs ${fmt(SUMMARY.total_pnl)}`, sub:`ROI: +${SUMMARY.roi}%`},
  {cls:'mc-bl',c:'var(--accent)',  lab:'Win Rate',    val:`${SUMMARY.wr}%`,  sub:`${SUMMARY.n_wins}W / ${SUMMARY.n_losses}L`},
  {cls:'mc-or',c:'var(--accent2)', lab:'Profit Factor',val:`${SUMMARY.pf}x`, sub:'Gross win/loss ratio'},
  {cls:'mc-yw',c:'var(--yellow)',  lab:'Max Drawdown', val:`Rs ${fmt(SUMMARY.mdd)}`,
   sub:`${Math.round(SUMMARY.mdd/CONFIG.capital*100)}% of capital`},
  {cls:'mc-pu',c:'#c084fc',        lab:'Avg Winner',   val:`Rs ${fmt(SUMMARY.avg_win)}`,
   sub:`Avg loser: -Rs ${fmt(Math.abs(SUMMARY.avg_loss))}`},
  {cls:'mc-re',c:'#ff8a9a',        lab:'Reward/Risk',  val:`${SUMMARY.rr_ratio}x`,sub:'Avg win / avg loss'},
];
document.getElementById('metricsGrid').innerHTML=mcDefs.map(m=>`
  <div class="mc ${m.cls}"><div class="ml">${m.lab}</div>
  <div class="mv" style="color:${m.c}">${m.val}</div>
  <div class="ms">${m.sub}</div></div>`).join('');

// ── CONFIG PANEL ──────────────────────────────────────────────────────────────
document.getElementById('configPanel').innerHTML=`
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px">
${[
  ['SuperTrend Period',    CONFIG.st_period],
  ['ST Multiplier',        CONFIG.st_mult],
  ['Bar Timeframe',        CONFIG.timeframe],
  ['Spread Width',         CONFIG.spread+'pt'],
  ['Strike Interval',      CONFIG.strike_interval+'pt'],
  ['Lots / Qty',           CONFIG.lots+' lots · '+CONFIG.qty+' qty'],
  ['Capital',              'Rs '+CONFIG.capital.toLocaleString('en-IN')],
  ['Target Exit',          (CONFIG.target_pct*100).toFixed(0)+'% credit decay'],
  ['Max Loss Exit',        (CONFIG.max_loss_pct*100).toFixed(0)+'% of spread = Rs '+CONFIG.max_loss_pts+'/unit'],
  ['Warmup Bars',          CONFIG.warmup_bars+' (ST period x 3)'],
  ['Gap Stop Threshold',   CONFIG.gap_stop_enabled?CONFIG.overnight_gap_pts+' pts adverse gap':'DISABLED'],
  ['Gap Reentry Wait',     CONFIG.gap_stop_enabled?(CONFIG.gap_reentry_wait===0?'No wait (09:20)':CONFIG.gap_reentry_wait+' min (09:'+(15+CONFIG.gap_reentry_wait).toString().padStart(2,'0')+')'):'N/A'],
].map(([k,v])=>`
  <div style="background:var(--surface2);border:1px solid var(--border);border-radius:4px;padding:10px 12px">
    <div style="font-family:'Space Mono',monospace;font-size:.57rem;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:5px">${k}</div>
    <div style="font-family:'Space Mono',monospace;font-size:.72rem;color:#fff;font-weight:700">${v}</div>
  </div>`).join('')}
</div>`;

// ── GAP STOP PANEL ────────────────────────────────────────────────────────────
(function(){
  const gsp=document.getElementById('gapStopPanel');
  if(!CONFIG.gap_stop_enabled){
    document.getElementById('gapStopSec').style.display='none';
    gsp.style.display='none';
    return;
  }
  const gapMonths=Object.keys(GAP_BY_MONTH).sort();
  gsp.innerHTML=`
  <div class="g2">
    <div class="card">
      <h3>Gap Stop Configuration &amp; Results</h3>
      <div style="display:flex;flex-direction:column;gap:5px">
      ${[
        ['Threshold',         CONFIG.overnight_gap_pts+' pts — exit if spot moves this much adversely overnight'],
        ['Re-entry wait',     CONFIG.gap_reentry_wait===0?'None — enter at 09:20 (first 5-min bar)':CONFIG.gap_reentry_wait+' min — enter at 09:'+(15+CONFIG.gap_reentry_wait).toString().padStart(2,'0')+' or later'],
        ['Times triggered',   SUMMARY.gap_stop_exits+' gap stop occasions across full backtest'],
        ['Trades closed',     SUMMARY.gap_stop_n+' trades exited via gap stop'],
        ['Total P&L',         (SUMMARY.gap_stop_pnl>=0?'+':'')+'Rs '+SUMMARY.gap_stop_pnl.toLocaleString('en-IN')],
        ['Avg P&L per trade', (SUMMARY.gap_stop_avg>=0?'+':'')+'Rs '+SUMMARY.gap_stop_avg.toLocaleString('en-IN')],
        ['Win rate',          SUMMARY.gap_stop_wr+'%  (gap stop exits that still ended positive)'],
      ].map(([k,v])=>`
        <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;padding:7px 10px;background:var(--surface2);border-radius:3px">
          <span style="font-family:'Space Mono',monospace;font-size:.62rem;color:var(--text2);white-space:nowrap">${k}</span>
          <span style="font-family:'Space Mono',monospace;font-size:.62rem;color:#fff;font-weight:700;text-align:right">${v}</span>
        </div>`).join('')}
      </div>
    </div>
    <div class="card">
      <h3>Gap Stop Exits by Month</h3>
      ${gapMonths.length>0?`
      <table style="width:100%;border-collapse:collapse">
        <thead><tr>
          <th style="font-family:'Space Mono',monospace;font-size:.58rem;color:var(--muted);text-align:left;padding:5px 8px;border-bottom:1px solid var(--border);text-transform:uppercase">Month</th>
          <th style="font-family:'Space Mono',monospace;font-size:.58rem;color:var(--muted);text-align:left;padding:5px 8px;border-bottom:1px solid var(--border);text-transform:uppercase">Exits</th>
          <th style="font-family:'Space Mono',monospace;font-size:.58rem;color:var(--muted);text-align:left;padding:5px 8px;border-bottom:1px solid var(--border);text-transform:uppercase">P&amp;L</th>
          <th style="font-family:'Space Mono',monospace;font-size:.58rem;color:var(--muted);text-align:left;padding:5px 8px;border-bottom:1px solid var(--border);text-transform:uppercase">WR</th>
        </tr></thead>
        <tbody>${gapMonths.map(m=>{
          const g=GAP_BY_MONTH[m];
          const pnlStr=(g.pnl>=0?'+':'')+'Rs '+g.pnl.toLocaleString('en-IN');
          return `<tr>
            <td style="font-family:'Space Mono',monospace;font-size:.66rem;padding:6px 8px;border-bottom:1px solid rgba(30,44,58,.4);color:#fff;font-weight:700">${m}</td>
            <td style="font-family:'Space Mono',monospace;font-size:.66rem;padding:6px 8px;border-bottom:1px solid rgba(30,44,58,.4);color:var(--accent2)">${g.count}</td>
            <td style="font-family:'Space Mono',monospace;font-size:.66rem;padding:6px 8px;border-bottom:1px solid rgba(30,44,58,.4);color:${g.pnl>=0?'var(--green)':'var(--red)'}">${pnlStr}</td>
            <td style="font-family:'Space Mono',monospace;font-size:.66rem;padding:6px 8px;border-bottom:1px solid rgba(30,44,58,.4);color:${g.wr>=60?'var(--green)':'var(--red)'}">${g.wr}%</td>
          </tr>`;
        }).join('')}</tbody>
      </table>`:`<div style="font-family:'Space Mono',monospace;font-size:.68rem;color:var(--muted);padding:16px 0">No gap stop exits triggered — no gap exceeded ${CONFIG.overnight_gap_pts}pts in the backtest period.</div>`}
    </div>
  </div>`;
})();
let cumArr=[];let run=0;
MONTHLY.forEach(m=>{run+=m.pnl;cumArr.push(run);});
new Chart(document.getElementById('cumC').getContext('2d'),{
  data:{labels:MONTHLY.map(m=>m.month),datasets:[
    {type:'line',label:'Cumulative P&L',data:cumArr,borderColor:'#00d4ff',
     backgroundColor:'rgba(0,212,255,.07)',fill:true,tension:.3,
     pointRadius:3,borderWidth:2,yAxisID:'y'},
    {type:'bar',label:'Monthly P&L',data:MONTHLY.map(m=>m.pnl),
     backgroundColor:MONTHLY.map(m=>m.pnl>=0?'rgba(0,230,118,.5)':'rgba(255,61,87,.5)'),
     borderColor:MONTHLY.map(m=>m.pnl>=0?'#00e676':'#ff3d57'),
     borderWidth:1,yAxisID:'y2'},
  ]},
  options:{responsive:true,maintainAspectRatio:false,
    interaction:{intersect:false,mode:'index'},
    plugins:{legend:{labels:{color:'#7a9bb5',font:CF}},
      tooltip:{...TT,callbacks:{label:ctx=>' Rs '+ctx.raw.toLocaleString('en-IN')}}},
    scales:{
      x:{grid:{color:'rgba(30,44,58,.4)'},ticks:{color:'#4a6278',font:{...CF,size:9}}},
      y:{grid:{color:'rgba(30,44,58,.4)'},ticks:{color:'#7a9bb5',font:CF,
          callback:v=>'Rs '+Math.round(v/1000)+'K'}},
      y2:{position:'right',grid:{display:false},ticks:{color:'#4a6278',
          font:{...CF,size:9},callback:v=>'Rs '+Math.round(v/1000)+'K'}},
    }}
});

// Monthly bar
new Chart(document.getElementById('mthC').getContext('2d'),{
  type:'bar',
  data:{labels:MONTHLY.map(m=>m.month.slice(2)),
    datasets:[{data:MONTHLY.map(m=>m.pnl),borderRadius:2,borderWidth:1,
      backgroundColor:MONTHLY.map(m=>m.pnl>=0?'rgba(0,230,118,.65)':'rgba(255,61,87,.65)'),
      borderColor:MONTHLY.map(m=>m.pnl>=0?'#00e676':'#ff3d57')}]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{...TT,
      callbacks:{label:ctx=>'Rs '+ctx.raw.toLocaleString('en-IN')}}},
    scales:{
      x:{grid:{color:'rgba(30,44,58,.3)'},ticks:{color:'#4a6278',font:{...CF,size:8}}},
      y:{grid:{color:'rgba(30,44,58,.4)'},ticks:{color:'#7a9bb5',font:CF,
          callback:v=>'Rs '+Math.round(v/1000)+'K'}}}}
});

// Monthly table
document.getElementById('mthTb').innerHTML=MONTHLY.map(m=>{
  const c=m.wr>=70?'var(--green)':m.wr>=55?'var(--yellow)':'var(--red)';
  return `<tr><td class="hl">${m.month}</td><td>${m.trades}</td>
    <td><div class="wr"><div class="wrt"><div class="wrf"
      style="width:${m.wr}%;background:${c}"></div></div>
    <span class="wrn" style="color:${c}">${m.wr}%</span></div></td>
    <td class="${m.pnl>=0?'pos':'neg'}">Rs ${fmt(m.pnl)}</td>
    <td style="color:${m.target>0?'var(--yellow)':'var(--muted)'}">${m.target}</td></tr>`;
}).join('');

// Exit reasons doughnut
const ek=Object.keys(EXIT_DATA);
new Chart(document.getElementById('exC').getContext('2d'),{
  type:'doughnut',
  data:{labels:ek.map(k=>`${k} (${EXIT_DATA[k].count})`),
    datasets:[{data:ek.map(k=>EXIT_DATA[k].total),
      backgroundColor:['rgba(0,212,255,.7)','rgba(0,230,118,.7)','rgba(255,215,64,.7)','rgba(255,61,87,.7)'],
      borderColor:['#00d4ff','#00e676','#ffd740','#ff3d57'],borderWidth:1}]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{position:'bottom',labels:{color:'#7a9bb5',font:{...CF,size:9},padding:6,boxWidth:10}},
      tooltip:{...TT,callbacks:{label:ctx=>' Rs '+ctx.raw.toLocaleString('en-IN')}}}}
});

// Type doughnut
const tk=Object.keys(TYPE_DATA);
new Chart(document.getElementById('tyC').getContext('2d'),{
  type:'doughnut',
  data:{labels:tk.map(k=>`${k} (${TYPE_DATA[k].wr}%WR)`),
    datasets:[{data:tk.map(k=>TYPE_DATA[k].total),
      backgroundColor:['rgba(255,107,53,.7)','rgba(192,132,252,.7)'],
      borderColor:['#ff6b35','#c084fc'],borderWidth:1}]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{position:'bottom',labels:{color:'#7a9bb5',font:{...CF,size:9},padding:6,boxWidth:10}},
      tooltip:{...TT,callbacks:{label:ctx=>' Rs '+ctx.raw.toLocaleString('en-IN')}}}}
});

// Days held combo
new Chart(document.getElementById('dyC').getContext('2d'),{
  data:{labels:DAYS_DATA.map(d=>d.days===0?'Same':d.days+'d'),
    datasets:[
      {type:'bar',label:'Win Rate %',data:DAYS_DATA.map(d=>d.wr),yAxisID:'y',borderRadius:2,borderWidth:0,
        backgroundColor:DAYS_DATA.map(d=>d.wr>=80?'rgba(0,230,118,.7)':d.wr>=60?'rgba(255,215,64,.7)':'rgba(255,61,87,.7)')},
      {type:'line',label:'Avg P&L',data:DAYS_DATA.map(d=>d.avg),yAxisID:'y2',
        borderColor:'#00d4ff',backgroundColor:'transparent',pointRadius:4,borderWidth:2}]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{labels:{color:'#7a9bb5',font:{...CF,size:9},padding:6,boxWidth:10}},tooltip:{...TT}},
    scales:{
      x:{grid:{display:false},ticks:{color:'#4a6278',font:{...CF,size:9}}},
      y:{grid:{color:'rgba(30,44,58,.4)'},ticks:{color:'#7a9bb5',font:CF,callback:v=>v+'%'},max:105},
      y2:{position:'right',grid:{display:false},ticks:{color:'#4a6278',font:CF,
          callback:v=>'Rs '+Math.round(v/1000)+'K'}}}}
});

// Distribution
new Chart(document.getElementById('diC').getContext('2d'),{
  type:'bar',
  data:{labels:DIST_DATA.map(d=>d.label),
    datasets:[{data:DIST_DATA.map(d=>d.count),borderWidth:0,borderRadius:2,
      backgroundColor:['rgba(255,61,87,.9)','rgba(255,61,87,.65)','rgba(255,61,87,.4)',
        'rgba(0,230,118,.3)','rgba(0,230,118,.6)','rgba(0,230,118,.8)','rgba(0,230,118,1)']}]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{...TT,callbacks:{label:ctx=>ctx.raw+' trades'}}},
    scales:{x:{grid:{display:false},ticks:{color:'#4a6278',font:{...CF,size:8}}},
      y:{grid:{color:'rgba(30,44,58,.4)'},ticks:{color:'#7a9bb5',font:CF}}}}
});

// OT / IT table
document.getElementById('otTb').innerHTML=[
  [OT_STATS,'Overnight'],[IT_STATS,'Intraday']
].map(([s,label])=>{
  const c=s.wr>=60?'var(--green)':'var(--red)';
  return `<tr><td class="hl">${label}</td><td>${s.trades}</td>
    <td><div class="wr"><div class="wrt"><div class="wrf" style="width:${s.wr}%;background:${c}"></div></div>
    <span class="wrn" style="color:${c}">${s.wr}%</span></div></td>
    <td class="pos">Rs ${fmt(s.avg)}</td><td class="pos">Rs ${fmt(s.total)}</td></tr>`;
}).join('');

// Days table
document.getElementById('dyTb').innerHTML=DAYS_DATA.map(d=>{
  const c=d.wr>=80?'var(--green)':d.wr>=60?'var(--yellow)':'var(--red)';
  const lbl=d.days===0?'Same Day':d.days+' day'+(d.days>1?'s':'');
  return `<tr><td class="hl">${lbl}</td><td>${d.trades}</td>
    <td><div class="wr"><div class="wrt"><div class="wrf" style="width:${d.wr}%;background:${c}"></div></div>
    <span class="wrn" style="color:${c}">${d.wr}%</span></div></td>
    <td class="${d.avg>=0?'pos':'neg'}">Rs ${fmt(d.avg)}</td>
    <td class="${d.total>=0?'pos':'neg'}">Rs ${fmt(d.total)}</td></tr>`;
}).join('');

// Signal audit
const failTotal=Object.values(FAIL_REASONS).reduce((a,b)=>a+b,0);
document.getElementById('sigDiv').innerHTML=`
  <div class="srow"><span style="color:var(--text2)">Total signal attempts</span>
    <strong>${SIGNAL_TOTAL}</strong></div>
  <div class="srow"><span style="color:var(--green)">Entered</span>
    <span style="color:var(--green);font-weight:700">${SIGNAL_ENTERED} (${Math.round(SIGNAL_ENTERED/SIGNAL_TOTAL*100)}%)</span></div>
  ${Object.entries(FAIL_REASONS).map(([k,v])=>`
  <div class="srow"><span style="color:var(--red)">Failed: ${k}</span>
    <span style="color:var(--red);font-weight:700">${v}</span></div>`).join('')}`;

// Fail by month chart
const fbm=Object.keys(FAIL_BY_MONTH).sort();
new Chart(document.getElementById('faC').getContext('2d'),{
  type:'bar',
  data:{labels:fbm,datasets:[{data:fbm.map(m=>FAIL_BY_MONTH[m]),
    backgroundColor:'rgba(255,61,87,.6)',borderColor:'#ff3d57',borderWidth:1,borderRadius:2}]},
  options:{responsive:true,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{...TT,callbacks:{label:ctx=>ctx.raw+' failures'}}},
    scales:{x:{grid:{display:false},ticks:{color:'#4a6278',font:{...CF,size:8}}},
      y:{grid:{color:'rgba(30,44,58,.3)'},ticks:{color:'#7a9bb5',font:CF}}}}
});

// Gaps
const gd=document.getElementById('gapsDiv');
if(GAPS.length===0){
  gd.innerHTML='<div style="font-family:Space Mono,monospace;font-size:.68rem;color:var(--muted);padding:8px 0 14px">No significant trading gaps detected.</div>';
}else{
  gd.innerHTML=GAPS.map(g=>{
    const cls=g.days>5?'gi-r':'gi-y';
    const sev=g.days>5?'CRITICAL GAP':'MINOR GAP';
    const col=g.days>5?'var(--red)':'var(--yellow)';
    return `<div class="gi ${cls}"><div>
      <div style="font-weight:700;margin-bottom:3px;color:${col}">${sev}: ${g.start} to ${g.end}</div>
      <div style="font-size:.63rem;opacity:.8">${g.days} trading days with no trades. Check option data.</div>
    </div><span style="font-size:1rem;font-weight:800;white-space:nowrap;margin-left:16px">${g.days}d</span></div>`;
  }).join('');
}

// Trade table
function ft(filter,btn){
  document.querySelectorAll('.fb').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  const src=TRADES.filter(t=>{
    if(filter==='ALL')       return true;
    if(filter==='BULL_PUT')  return t.ty==='BULL_PUT';
    if(filter==='BEAR_CALL') return t.ty==='BEAR_CALL';
    if(filter==='WIN')       return t.pl>0;
    if(filter==='LOSS')      return t.pl<=0;
    if(filter==='Target')    return t.re==='Target';
    if(filter==='overnight') return t.ov;
    if(filter==='nxexp')     return t.nx;
    if(filter==='GapStop')   return t.re==='GapStop';
    return true;
  });
  document.getElementById('tlTitle').textContent=`${src.length} Trades (${filter})`;
  document.getElementById('tlTb').innerHTML=src.map((t,i)=>`<tr>
    <td style="color:var(--muted)">${i+1}</td>
    <td>${t.en.replace('T',' ')}</td>
    <td>${t.ex.replace('T',' ')}</td>
    <td style="color:${t.ty==='BULL_PUT'?'#c084fc':'var(--accent2)'}"><b>${t.ty}</b></td>
    <td>${t.sl}</td><td>${t.bu}</td><td>Rs ${t.cr}</td><td>${t.dy}</td>
    <td style="color:${t.re==='Target'?'var(--green)':t.re==='GapStop'?'var(--accent2)':t.re==='MaxLoss'?'var(--red)':'var(--text2)'}">${t.re}</td>
    <td class="${t.pl>0?'pos':'neg'}" style="font-weight:700">Rs ${t.pl.toLocaleString('en-IN')}</td>
    <td style="color:var(--text2)">Rs ${t.cu.toLocaleString('en-IN')}</td>
    <td style="color:${t.dd>0?'var(--red)':'var(--muted)'}">${t.dd>0?'-Rs '+t.dd.toLocaleString('en-IN'):'--'}</td>
  </tr>`).join('');
  document.getElementById('pgi').textContent=`Showing ${src.length} of ${TRADES.length} trades`;
}
ft('ALL',document.querySelector('.fb.active'));
</script>
</body>
</html>"""

    out_path = out_dir / "dashboard.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"  Dashboard   : {out_path}")
    return out_path


# ══════════════════════════════════════════════════════════════════════════════
# CONSOLE REPORT
# ══════════════════════════════════════════════════════════════════════════════

def print_console_report(tdf, total_pnl, peak_viol, peak_viol_dates):
    wins   = tdf[tdf["total_pnl"] > 0]
    losses = tdf[tdf["total_pnl"] <= 0]
    n_tr   = len(tdf)
    wr     = len(wins) / n_tr * 100 if n_tr > 0 else 0
    pf     = (wins["total_pnl"].sum() / abs(losses["total_pnl"].sum())
              if len(losses) > 0 and losses["total_pnl"].sum() != 0 else 99)
    roi    = total_pnl / CAPITAL * 100
    cum    = tdf["total_pnl"].cumsum()
    mdd    = (cum.cummax() - cum).max()

    print(f"\n{'='*90}")
    print(f"  RESULTS  |  "
          f"{pd.Timestamp(tdf['entry_time'].min()).date()} to "
          f"{pd.Timestamp(tdf['exit_time'].max()).date()}")
    print(f"  {'-'*65}")
    print(f"  Total Trades     : {n_tr}")
    print(f"  Win Rate         : {wr:.1f}%  ({len(wins)}W / {len(losses)}L)")
    print(f"  Profit Factor    : {pf:.2f}")
    print(f"  Total P&L        : Rs {total_pnl:+,.0f}")
    print(f"  ROI on {CAPITAL/1e5:.0f}L        : {roi:+.1f}%")
    print(f"  Max Drawdown     : Rs {mdd:,.0f}  ({mdd/CAPITAL*100:.1f}% of capital)")

    tdf["month"] = tdf["entry_time"].dt.to_period("M")
    print(f"\n  MONTHLY P&L:")
    cumulative = 0
    for month, grp in tdf.groupby("month"):
        m_pnl       = grp["total_pnl"].sum()
        cumulative += m_pnl
        m_wr        = (grp["total_pnl"] > 0).mean() * 100
        print(f"    {month} | {len(grp):3d} trades | WR:{m_wr:5.1f}% | "
              f"P&L: Rs {m_pnl:+10,.0f} | Cum: Rs {cumulative:+10,.0f}")

    if SIMULATE_MARGIN:
        print(f"\n  SEBI MARGIN:")
        print(f"    Peak violations: {peak_viol}")
        if peak_viol == 0:
            print("    OK No peak margin breaches")
        for v in peak_viol_dates[:5]:
            print(f"    FAIL {v['date']} {v['time']} | "
                  f"equity={v['equity']:,} req={v['margin_req']:,} "
                  f"short={v['shortfall']:,}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 90)
    print("SUPERTREND CREDIT SPREAD BACKTEST — VALIDATED + RICH DASHBOARD")
    print("=" * 90)
    print(f"  ST({ST_PERIOD}, {ST_MULTIPLIER}) | {TIMEFRAME} | {SPREAD_WIDTH}pt | "
          f"{NUM_LOTS}x{LOT_SIZE}={QTY} qty | Capital Rs {CAPITAL:,}")

    # Phase 0: validate data
    con = duckdb.connect(DB_PATH, read_only=True)
    val = validate_data(con)

    # Warmup start (3 months before effective trade start)
    warmup_start_str = str(
        (pd.Timestamp(val["trade_start_date"]) - pd.DateOffset(months=3)).date()
    )

    # Create dynamic output folder
    out_dir = make_output_dir(
        str(val["trade_start_date"]),
        str(val["trade_end_date"])
    )

    # Load spot data
    df_spot = load_spot_data(con, warmup_start_str)
    con.close()

    # Phase 1: simulate
    print(f"\n{'='*90}")
    print(f"PHASE 1 — SIMULATION  |  "
          f"{val['trade_start_date']} to {val['trade_end_date']}")
    print("=" * 90)

    provider = OptionDataProvider(DB_PATH)
    trades, total_pnl, signal_log, peak_viol, peak_viol_dates, gap_stop_exits = run_simulation(
        df_spot, provider,
        trade_start_date=val["trade_start_date"],
        trade_end_date=val["trade_end_date"],
    )
    provider.close()

    if not trades:
        print("  No trades executed.")
        raise SystemExit(0)

    # Build DataFrame
    tdf = pd.DataFrame(trades)
    tdf["entry_time"] = pd.to_datetime(tdf["entry_time"])
    tdf["exit_time"]  = pd.to_datetime(tdf["exit_time"])

    # Phase 2: console report
    print(f"\n{'='*90}")
    print("PHASE 2 — RESULTS")
    print("=" * 90)
    print_console_report(tdf, total_pnl, peak_viol, peak_viol_dates)

    # Save CSV with dynamic filename
    csv_name = (
        f"trades_ST{ST_PERIOD}_{ST_MULTIPLIER}_"
        f"{SPREAD_WIDTH}pt_{NUM_LOTS}lots.csv"
    )
    csv_path = out_dir / csv_name
    tdf.drop(columns=["month"], errors="ignore").to_csv(csv_path, index=False)
    print(f"\n  Trades CSV  : {csv_path}")

    # Save signal audit CSV
    sig_csv = out_dir / "signal_audit.csv"
    pd.DataFrame(signal_log).to_csv(sig_csv, index=False)
    print(f"  Signal log  : {sig_csv}")

    # Phase 3: generate HTML dashboard
    print(f"\n{'='*90}")
    print("PHASE 3 — GENERATING INTERACTIVE DASHBOARD")
    print("=" * 90)
    dash_path = build_dashboard(
        tdf.drop(columns=["month"], errors="ignore").copy(),
        total_pnl, signal_log,
        peak_viol, peak_viol_dates,
        gap_stop_exits,
        out_dir
    )

    # Open in browser
    try:
        import webbrowser
        webbrowser.open(f"file://{dash_path.resolve()}")
    except Exception:
        pass

    print(f"\nAll outputs saved to:")
    print(f"  {out_dir}/")
    print(f"  |- {csv_name}")
    print(f"  |- signal_audit.csv")
    print(f"  |- dashboard.html")
    print("\nDone.")
