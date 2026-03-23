"""
SuperTrend Credit Spread Backtest — With Full Data Validation
=============================================================
IMPROVEMENTS OVER PREVIOUS VERSION:
  [1] Phase-0 data validation: scans DB for exact spot + option data ranges
  [2] Per-expiry contract coverage check — flags missing strikes/types
  [3] Price cache (last-known-price fallback) — no more silently dropped trades
  [4] Lookback expanded to 30 min; expiry-day settlement fallback to 0
  [5] DataEnd force-close scans full last session (78 bars)
  [6] Signal audit log — every ST flip is recorded with entry outcome
  [7] In-trade monitoring failures are logged (not silently skipped)
  [8] Backtest date range auto-detected from DB (no hard-coded end date)
  [9] Stale pending_flip cleared each morning (existing bug fix retained)
  [10] Comprehensive per-month and per-expiry-week P&L with miss-detection

ST params, charges, margin simulation: identical to live script.
"""

import os
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from collections import defaultdict

import duckdb
import numpy as np
import pandas as pd

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DB_PATH   = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"

# ── STRATEGY PARAMS (mirrors live CONFIG) ─────────────────────────────────────
ST_PERIOD      = 80
ST_MULTIPLIER  = 3.6
TIMEFRAME      = "5min"

SPREAD_WIDTH    = 500
STRIKE_INTERVAL = 50

LOT_SIZE  = 65
NUM_LOTS  = 3
QTY       = LOT_SIZE * NUM_LOTS

TARGET_PCT   = 0.95
MAX_LOSS_PCT = 0.85

TRADE_START  = dtime(9, 15)
EXPIRY_EXIT  = dtime(15, 15)

CAPITAL = 300_000

# ── CHARGES ───────────────────────────────────────────────────────────────────
BROKERAGE_PER_ORDER = 20
STT_SELL_PCT        = 0.001
TXN_CHARGE_PCT      = 0.0003553
SEBI_PER_CRORE      = 10
GST_PCT             = 0.18
STAMP_BUY_PCT       = 0.00003

# ── MARGIN SIMULATION ─────────────────────────────────────────────────────────
SIMULATE_MARGIN      = True
MARGIN_BASE_SPOT     = 24_000
MARGIN_NORMAL_BASE   = 68_000
MARGIN_ELM_EFFECTIVE = 30_000
ELM_RAW_PCT          = 0.02
MARGIN_PEAK_BUFFER   = 1.10
PEAK_SNAPSHOT_TIMES  = [dtime(10, 0), dtime(11, 30), dtime(13, 0), dtime(14, 30)]

WARMUP_BARS_REQUIRED = ST_PERIOD * 3   # ~3× period for stable SuperTrend


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 0 — DATA VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_data(con) -> dict:
    """
    Full pre-flight data check. Returns a dict with:
      spot_start, spot_end, spot_gaps, spot_total_bars
      options_start, options_end, options_contract_count
      expiry_coverage: {expiry_date -> {'CE': set(strikes), 'PE': set(strikes)}}
      missing_expiry_dates: list of expiry dates with no data at all
      trade_start_date: first date with enough warmup bars
    """
    print("\n" + "═" * 90)
    print("PHASE 0 — DATA VALIDATION")
    print("═" * 90)

    results = {}

    # ── Spot data ────────────────────────────────────────────────────────────
    print("\n[1/5] Spot data (NSE_INDEX|Nifty 50) ...")
    spot_meta = con.execute("""
        SELECT
            MIN(timestamp) AS first_bar,
            MAX(timestamp) AS last_bar,
            COUNT(*)       AS total_bars
        FROM candle_data
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND interval = '1minute'
    """).fetchone()

    spot_first = pd.Timestamp(spot_meta[0])
    spot_last  = pd.Timestamp(spot_meta[1])
    spot_total = spot_meta[2]
    results["spot_start"]      = spot_first
    results["spot_end"]        = spot_last
    results["spot_total_bars"] = spot_total
    print(f"  ✓ Spot data : {spot_first.date()} → {spot_last.date()} ({spot_total:,} 1-min bars)")

    # Check for session gaps (days with < 300 bars in a trading day = suspicious)
    print("  Checking for spot data gaps by date ...")
    daily_bars = con.execute("""
        SELECT DATE(timestamp) AS d, COUNT(*) AS bars
        FROM candle_data
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND interval = '1minute'
        GROUP BY d
        ORDER BY d
    """).fetchdf()
    # A normal 6h15m session = 375 bars; flag days < 200 as suspicious
    thin_days = daily_bars[daily_bars["bars"] < 200]
    gap_dates = []
    if len(thin_days) > 0:
        print(f"  ⚠  Spot sessions with < 200 bars ({len(thin_days)} days):")
        for _, row in thin_days.iterrows():
            print(f"       {row['d']}  →  {row['bars']} bars")
            gap_dates.append(str(row["d"]))
    else:
        print("  ✓ All spot sessions look complete (≥ 200 bars each)")
    results["spot_gaps"] = gap_dates

    # ── Options contracts ────────────────────────────────────────────────────
    print("\n[2/5] Option contracts (NIFTY weekly expiries) ...")
    contracts = con.execute("""
        SELECT expired_instrument_key, strike_price, contract_type, expiry_date
        FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND contract_type IN ('CE', 'PE')
        ORDER BY expiry_date, strike_price
    """).fetchdf()
    contracts["expiry_date"] = pd.to_datetime(contracts["expiry_date"]).dt.date
    contracts["strike_price"] = contracts["strike_price"].astype(float)

    all_expiries = sorted(contracts["expiry_date"].unique())
    results["options_contract_count"] = len(contracts)
    results["all_expiries"] = all_expiries
    print(f"  ✓ {len(contracts):,} contracts across {len(all_expiries)} expiry dates")
    print(f"    Range: {all_expiries[0]} → {all_expiries[-1]}")

    # ── Option historical data coverage ──────────────────────────────────────
    print("\n[3/5] Option OHLC data coverage per expiry week ...")
    opt_data_meta = con.execute("""
        SELECT
            c.expiry_date,
            c.contract_type,
            COUNT(DISTINCT h.expired_instrument_key) AS contracts_with_data,
            COUNT(h.timestamp)                       AS total_rows,
            MIN(h.timestamp)                         AS first_ts,
            MAX(h.timestamp)                         AS last_ts
        FROM historical_data h
        JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
        WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
          AND c.contract_type IN ('CE', 'PE')
        GROUP BY c.expiry_date, c.contract_type
        ORDER BY c.expiry_date, c.contract_type
    """).fetchdf()
    opt_data_meta["expiry_date"] = pd.to_datetime(opt_data_meta["expiry_date"]).dt.date

    expiry_coverage = defaultdict(dict)
    for _, row in opt_data_meta.iterrows():
        expiry_coverage[row["expiry_date"]][row["contract_type"]] = {
            "contracts": int(row["contracts_with_data"]),
            "rows":      int(row["total_rows"]),
            "first":     row["first_ts"],
            "last":      row["last_ts"],
        }

    # Flag expiries present in contracts but with no OHLC data
    missing_expiry_data = []
    for exp in all_expiries:
        if exp not in expiry_coverage:
            missing_expiry_data.append(exp)

    if missing_expiry_data:
        print(f"  ✗ {len(missing_expiry_data)} expiry dates have NO option OHLC data:")
        for exp in missing_expiry_data:
            print(f"      {exp}")
    else:
        print(f"  ✓ All {len(all_expiries)} expiry weeks have option OHLC data")

    results["expiry_coverage"]      = dict(expiry_coverage)
    results["missing_expiry_dates"] = missing_expiry_data

    # ── Specifically-redownloaded expiry dates check ──────────────────────────
    REDOWNLOADED = [
        '2025-02-06', '2025-02-13', '2025-02-20', '2025-02-27',
        '2025-03-06', '2025-03-13', '2025-03-20', '2025-03-27',
        '2026-03-02', '2026-03-10', '2026-03-17',
    ]
    print(f"\n[4/5] Checking {len(REDOWNLOADED)} re-downloaded expiry dates ...")
    for exp_str in REDOWNLOADED:
        exp_dt = pd.Timestamp(exp_str).date()
        if exp_dt in expiry_coverage:
            cov = expiry_coverage[exp_dt]
            ce_info = cov.get("CE", {})
            pe_info = cov.get("PE", {})
            ce_str  = f"CE: {ce_info.get('contracts',0)} contracts, {ce_info.get('rows',0):,} rows → {ce_info.get('last','?')}"
            pe_str  = f"PE: {pe_info.get('contracts',0)} contracts, {pe_info.get('rows',0):,} rows → {pe_info.get('last','?')}"
            print(f"  ✓ {exp_str}  |  {ce_str}  |  {pe_str}")
        else:
            print(f"  ✗ {exp_str}  →  NO coverage (still missing!)")

    # ── Compute effective trade date range ────────────────────────────────────
    print(f"\n[5/5] Determining effective backtest range ...")
    warmup_minutes = WARMUP_BARS_REQUIRED * 5    # 5-min bars → minutes
    warmup_end_ts  = spot_first + timedelta(minutes=warmup_minutes)
    trade_start_date = warmup_end_ts.date()

    # Options data start
    if opt_data_meta.empty:
        options_first = spot_first
    else:
        options_first = pd.Timestamp(opt_data_meta["first_ts"].min())

    # Find latest first date where BOTH spot + options have data
    effective_start = max(trade_start_date, options_first.date())
    effective_end   = spot_last.date()

    results["warmup_end"]       = warmup_end_ts
    results["trade_start_date"] = effective_start
    results["trade_end_date"]   = effective_end
    results["options_start"]    = options_first

    print(f"  Spot data starts    : {spot_first.date()}")
    print(f"  Warmup needed       : {WARMUP_BARS_REQUIRED} 5-min bars "
          f"(≈ {warmup_minutes//60}h {warmup_minutes%60}m)")
    print(f"  Options data starts : {options_first.date()}")
    print(f"  ✓ Effective backtest : {effective_start}  →  {effective_end}")

    return results


# ══════════════════════════════════════════════════════════════════════════════
# SUPERTREND (exact copy from live script)
# ══════════════════════════════════════════════════════════════════════════════

def compute_supertrend(highs, lows, closes, period, multiplier):
    n  = len(closes)
    tr = np.zeros(n)
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        tr[i] = max(highs[i]   - lows[i],
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
# OPTION DATA PROVIDER  (with price cache + wider lookback)
# ══════════════════════════════════════════════════════════════════════════════

class OptionDataProvider:
    def __init__(self, db_path):
        self.con = duckdb.connect(db_path, read_only=True)
        self._price_cache: dict = {}   # contract_key → last valid (price, ts)
        self._load_contracts()
        self._load_expiries()
        self._build_contract_index()

    def _load_contracts(self):
        print("  Loading NIFTY option contracts...")
        rows = self.con.execute("""
            SELECT expired_instrument_key, trading_symbol, strike_price,
                   contract_type, expiry_date, lot_size
            FROM contracts
            WHERE instrument_key = 'NSE_INDEX|Nifty 50'
              AND contract_type IN ('CE', 'PE')
            ORDER BY expiry_date, strike_price
        """).fetchall()
        self.contracts_raw = rows
        print(f"    Loaded {len(rows):,} contracts")

    def _load_expiries(self):
        expiry_set = set()
        for row in self.contracts_raw:
            exp = row[4]
            if isinstance(exp, datetime):
                exp = exp.date()
            expiry_set.add(exp)
        self.expiry_dates = sorted(expiry_set)
        print(f"    Expiries: {len(self.expiry_dates)} "
              f"({self.expiry_dates[0]} → {self.expiry_dates[-1]})")

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
        print(f"    Contract index: {len(self.contract_index):,} entries")

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

    def get_option_price(self, contract_key, timestamp, lookback_minutes=30,
                         use_cache_fallback=True):
        """
        Returns (price, timestamp_found).
        If no live price found within lookback_minutes, returns last cached
        price if use_cache_fallback=True, else (None, None).
        """
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
                WHERE expired_instrument_key = ?
                  AND timestamp = ?
            """, [contract_key, check_ts]).fetchone()
            if row and row[0] is not None and float(row[0]) > 0:
                price = float(row[0])
                self._price_cache[contract_key] = (price, check_ts)
                return price, check_ts

        # Fallback: last cached price
        if use_cache_fallback and contract_key in self._price_cache:
            cached_price, cached_ts = self._price_cache[contract_key]
            return cached_price, cached_ts

        return None, None

    def get_option_price_at_expiry_settlement(self, contract_key, expiry_date,
                                              spot_close):
        """
        Settlement price for expired options.
        Tries actual last bar first; falls back to intrinsic value / 0.
        """
        if contract_key is None:
            return 0.0

        # Try last bar of expiry day
        for hh, mm in [(15, 29), (15, 25), (15, 20), (15, 15), (15, 10),
                       (15, 5), (15, 0), (14, 55)]:
            ts = datetime.combine(expiry_date, dtime(hh, mm))
            row = self.con.execute("""
                SELECT close FROM historical_data
                WHERE expired_instrument_key = ?
                  AND timestamp = ?
            """, [contract_key, ts]).fetchone()
            if row and row[0] is not None:
                return float(row[0])

        # Cache fallback
        if contract_key in self._price_cache:
            return self._price_cache[contract_key][0]

        return 0.0   # option expired worthless

    def close(self):
        self.con.close()


# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

def load_spot_data(con, warmup_start: str) -> pd.DataFrame:
    print(f"\nLoading NIFTY spot data from {warmup_start} ...")
    df = con.execute(f"""
        SELECT timestamp, open, high, low, close, volume
        FROM candle_data
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND interval = '1minute'
          AND timestamp >= '{warmup_start}'
        ORDER BY timestamp
    """).fetchdf()

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    df = df[~df.index.duplicated(keep="first")]
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    df["volume"] = df["volume"].astype(int)
    print(f"  1-min bars: {len(df):,}  ({df.index[0]} → {df.index[-1]})")

    df_5m = df.resample(TIMEFRAME).agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum"
    }).dropna()
    print(f"  5-min bars: {len(df_5m):,}")
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

def _exit_trade(provider, trade_data, exit_time, reason,
                use_settlement=False, settlement_spot=None):
    """
    Returns trade result dict, or None if prices completely unavailable.
    use_settlement=True: for expiry-day exits, fall back to settlement (0).
    """
    sell_exit, sell_ts = provider.get_option_price(
        trade_data["sell_key"], exit_time, use_cache_fallback=True)
    buy_exit,  buy_ts  = provider.get_option_price(
        trade_data["buy_key"],  exit_time, use_cache_fallback=True)

    # Expiry settlement fallback
    if use_settlement:
        expiry = trade_data["expiry"]
        if sell_exit is None:
            sell_exit = provider.get_option_price_at_expiry_settlement(
                trade_data["sell_key"], expiry, settlement_spot)
        if buy_exit is None:
            buy_exit = provider.get_option_price_at_expiry_settlement(
                trade_data["buy_key"],  expiry, settlement_spot)

    if sell_exit is None or buy_exit is None:
        return None

    exit_spread_value = sell_exit - buy_exit
    pnl_per_unit      = trade_data["net_credit"] - exit_spread_value
    gross_pnl         = pnl_per_unit * QTY

    sell_entry = trade_data["sell_premium"]
    buy_entry  = trade_data["buy_premium"]

    brokerage = BROKERAGE_PER_ORDER * 4
    stt       = STT_SELL_PCT * (
                    sell_entry * QTY
                  + sell_exit  * QTY
                  + buy_exit   * QTY)
    total_turnover = (sell_entry + buy_entry + sell_exit + buy_exit) * QTY
    txn_charges    = TXN_CHARGE_PCT * total_turnover
    sebi           = SEBI_PER_CRORE * total_turnover / 1e7
    gst            = GST_PCT * (brokerage + txn_charges + sebi)
    stamp          = STAMP_BUY_PCT * (buy_entry * QTY + sell_exit * QTY)
    total_charges  = brokerage + stt + txn_charges + sebi + gst + stamp
    total_pnl      = gross_pnl - total_charges

    entry_dt = pd.Timestamp(trade_data["entry_time"])
    exit_dt  = pd.Timestamp(exit_time)
    days_held = (exit_dt.date() - entry_dt.date()).days

    return {
        "entry_time":    trade_data["entry_time"],
        "exit_time":     exit_time,
        "type":          trade_data["spread_type"],
        "entry_spot":    trade_data["entry_spot"],
        "sell_strike":   trade_data["sell_strike"],
        "buy_strike":    trade_data["buy_strike"],
        "sell_entry":    sell_entry,
        "buy_entry":     buy_entry,
        "sell_exit":     sell_exit,
        "buy_exit":      buy_exit,
        "net_credit":    trade_data["net_credit"],
        "exit_spread":   exit_spread_value,
        "pnl_per_unit":  pnl_per_unit,
        "gross_pnl":     gross_pnl,
        "charges":       total_charges,
        "total_pnl":     total_pnl,
        "exit_reason":   reason,
        "expiry":        trade_data["expiry"],
        "days_held":     days_held,
        "overnight":     days_held > 0,
        "is_next_expiry": trade_data.get("is_next_expiry", False),
        "qty":           QTY,
        "sell_ts_found": str(sell_ts) if sell_ts else "cache",
        "buy_ts_found":  str(buy_ts)  if buy_ts  else "cache",
        # Margin fields
        "equity_at_entry": trade_data.get("equity_at_entry", 0),
        "margin_at_entry": trade_data.get("margin_at_entry", 0),
        "margin_req":      trade_data.get("margin_req", 0),
        "elm_component":   trade_data.get("elm_component", 0),
        "elm_raw":         trade_data.get("elm_raw", 0),
        "margin_util_pct": trade_data.get("margin_util_pct", 0),
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

    # Find start & end indices
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

    print(f"\n  Warmup bars    : {start_idx}")
    print(f"  Trading range  : bar {start_idx} ({df.index[start_idx]}) "
          f"→ bar {end_idx-1} ({df.index[end_idx-1]})")
    print(f"  Total bars     : {end_idx - start_idx:,}")

    trades      = []
    total_pnl   = 0.0
    running_pnl = 0.0
    in_trade    = False
    trade_data  = {}

    # ── Counters ──────────────────────────────────────────────────────────────
    skipped_no_contract     = 0
    skipped_no_price        = 0
    skipped_negative_credit = 0
    skipped_used_next_expiry = 0
    skipped_no_exit_price   = 0
    skipped_margin          = 0
    target_reentries        = 0
    pending_flip_entries    = 0
    reentry_when_flat       = 0
    total_signals           = 0
    in_trade_price_failures = 0

    pending_flip = None

    # ── Signal audit log ──────────────────────────────────────────────────────
    signal_log = []   # list of dicts

    # ── SEBI margin tracking ──────────────────────────────────────────────────
    peak_violations      = 0
    peak_violation_dates = []
    _last_snapshot_date  = None
    _snapshots_checked   = set()

    # ─────────────────────────────────────────────────────────────────────────
    def _try_enter(bar_time, bar_date, spot, direction, trigger_reason="Signal"):
        nonlocal skipped_no_contract, skipped_no_price, skipped_negative_credit
        nonlocal skipped_used_next_expiry, skipped_margin

        atm_strike     = round(spot / STRIKE_INTERVAL) * STRIKE_INTERVAL
        nearest_expiry = provider.get_nearest_expiry(bar_time)
        is_expiry_day  = nearest_expiry is not None and bar_date == nearest_expiry

        if is_expiry_day:
            expiry = provider.get_next_expiry(bar_time)
            if expiry is not None:
                skipped_used_next_expiry += 1
            else:
                skipped_no_contract += 1
                signal_log.append({"bar": bar_time, "direction": direction,
                    "trigger": trigger_reason, "outcome": "FAIL:no_next_expiry"})
                return None
        else:
            expiry = nearest_expiry

        if expiry is None:
            skipped_no_contract += 1
            signal_log.append({"bar": bar_time, "direction": direction,
                "trigger": trigger_reason, "outcome": "FAIL:no_expiry"})
            return None

        if direction == "BULLISH":
            sell_strike = atm_strike
            buy_strike  = atm_strike - SPREAD_WIDTH
            opt_type    = "PE"
            spread_type = "BULL_PUT"
        else:
            sell_strike = atm_strike
            buy_strike  = atm_strike + SPREAD_WIDTH
            opt_type    = "CE"
            spread_type = "BEAR_CALL"

        sell_key = provider.get_contract_key(sell_strike, opt_type, expiry)
        buy_key  = provider.get_contract_key(buy_strike,  opt_type, expiry)

        if sell_key is None or buy_key is None:
            skipped_no_contract += 1
            signal_log.append({"bar": bar_time, "direction": direction,
                "trigger": trigger_reason,
                "outcome": f"FAIL:no_contract sell={'OK' if sell_key else 'MISS'} "
                           f"buy={'OK' if buy_key else 'MISS'} "
                           f"expiry={expiry} strike={atm_strike}"})
            return None

        sell_premium, _ = provider.get_option_price(sell_key, bar_time,
                                                     use_cache_fallback=False)
        buy_premium,  _ = provider.get_option_price(buy_key,  bar_time,
                                                     use_cache_fallback=False)

        if sell_premium is None or buy_premium is None:
            skipped_no_price += 1
            signal_log.append({"bar": bar_time, "direction": direction,
                "trigger": trigger_reason,
                "outcome": f"FAIL:no_price sell={'OK' if sell_premium else 'MISS'} "
                           f"buy={'OK' if buy_premium else 'MISS'} expiry={expiry}"})
            return None

        net_credit = sell_premium - buy_premium
        if net_credit <= 0:
            skipped_negative_credit += 1
            signal_log.append({"bar": bar_time, "direction": direction,
                "trigger": trigger_reason,
                "outcome": f"FAIL:neg_credit {net_credit:.2f}"})
            return None

        equity_now = CAPITAL + running_pnl
        margin_at_entry, margin_req_at_entry, elm_comp, elm_raw = \
            compute_margin(spot, is_expiry_day)

        if SIMULATE_MARGIN and equity_now < margin_req_at_entry:
            skipped_margin += 1
            signal_log.append({"bar": bar_time, "direction": direction,
                "trigger": trigger_reason, "outcome": "FAIL:margin"})
            return None

        signal_log.append({"bar": bar_time, "direction": direction,
            "trigger": trigger_reason,
            "outcome": f"ENTERED {spread_type} expiry={expiry} "
                       f"sell={sell_strike} buy={buy_strike} credit={net_credit:.2f}"})

        return {
            "spread_type":    spread_type,
            "entry_time":     bar_time,
            "entry_spot":     spot,
            "atm_strike":     atm_strike,
            "sell_strike":    sell_strike,
            "buy_strike":     buy_strike,
            "opt_type":       opt_type,
            "sell_key":       sell_key,
            "buy_key":        buy_key,
            "sell_premium":   sell_premium,
            "buy_premium":    buy_premium,
            "net_credit":     net_credit,
            "expiry":         expiry,
            "is_next_expiry": is_expiry_day,
            "equity_at_entry":  round(equity_now),
            "margin_at_entry":  round(margin_at_entry),
            "margin_req":       round(margin_req_at_entry),
            "elm_component":    round(elm_comp),
            "elm_raw":          round(elm_raw),
            "margin_util_pct":  round(margin_at_entry / equity_now * 100, 1)
                                if equity_now > 0 else 0,
        }

    # ─────────────────────────────────────────────────────────────────────────
    for i in range(start_idx, end_idx):
        bar_time = df.index[i]
        t        = bar_time.time()
        bar_date = bar_time.date()
        spot     = close[i]

        # ── SEBI PEAK MARGIN SNAPSHOTS ────────────────────────────────────────
        if SIMULATE_MARGIN and in_trade:
            if bar_date != _last_snapshot_date:
                _last_snapshot_date = bar_date
                _snapshots_checked  = set()
            if t in PEAK_SNAPSHOT_TIMES and t not in _snapshots_checked:
                _snapshots_checked.add(t)
                nearest_exp = provider.get_nearest_expiry(bar_time)
                is_exp      = nearest_exp is not None and bar_date == nearest_exp
                _, margin_req, _, _ = compute_margin(spot, is_exp)
                equity_now  = CAPITAL + running_pnl
                if equity_now < margin_req:
                    peak_violations += 1
                    peak_violation_dates.append({
                        "date": bar_date, "time": t,
                        "equity": round(equity_now),
                        "margin_req": round(margin_req),
                        "shortfall": round(margin_req - equity_now),
                        "is_expiry": is_exp,
                    })

        # ── POST-EXPIRY FORCE CLOSE (option expired, retroactive close) ───────
        if in_trade and bar_date > trade_data["expiry"]:
            expiry = trade_data["expiry"]
            exit_pnl = None
            for hh, mm in [(15, 29), (15, 25), (15, 20), (15, 15),
                           (15, 10), (15,  5), (15,  0), (14, 55), (14, 45)]:
                candidate = pd.Timestamp(datetime.combine(expiry, dtime(hh, mm)))
                exit_pnl  = _exit_trade(provider, trade_data, candidate,
                                        "Expiry", use_settlement=True,
                                        settlement_spot=spot)
                if exit_pnl:
                    break
            if exit_pnl:
                trades.append(exit_pnl)
                total_pnl   += exit_pnl["total_pnl"]
                running_pnl += exit_pnl["total_pnl"]
            else:
                print(f"  WARNING: Post-expiry close failed (expiry={expiry}) — dropped!")
                skipped_no_exit_price += 1
            in_trade = False

        # ── EXPIRY DAY FORCED EXIT AT 15:15 ──────────────────────────────────
        if in_trade and t >= EXPIRY_EXIT and bar_date == trade_data["expiry"]:
            exit_pnl = _exit_trade(provider, trade_data, bar_time,
                                   "Expiry", use_settlement=True,
                                   settlement_spot=spot)
            if exit_pnl:
                trades.append(exit_pnl)
                total_pnl   += exit_pnl["total_pnl"]
                running_pnl += exit_pnl["total_pnl"]
                in_trade = False
                continue
            else:
                skipped_no_exit_price += 1
                continue

        # ── DETECT ST FLIP ────────────────────────────────────────────────────
        any_flip = None
        if i > 0:
            if   st_dir[i] == 1 and st_dir[i-1] == -1:
                any_flip = "BULLISH"
                total_signals += 1
            elif st_dir[i] == -1 and st_dir[i-1] == 1:
                any_flip = "BEARISH"
                total_signals += 1

        # ── IN-TRADE MONITORING ───────────────────────────────────────────────
        if in_trade:

            # -- Reversal exit
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
                        skipped_no_exit_price += 1
                        in_trade_price_failures += 1
                        continue

            # -- MaxLoss / Target check
            sell_price, _ = provider.get_option_price(
                trade_data["sell_key"], bar_time, use_cache_fallback=True)
            buy_price, _  = provider.get_option_price(
                trade_data["buy_key"],  bar_time, use_cache_fallback=True)

            if sell_price is not None and buy_price is not None:
                current_spread     = sell_price - buy_price
                max_loss_threshold = SPREAD_WIDTH * MAX_LOSS_PCT

                if current_spread >= max_loss_threshold:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "MaxLoss")
                    if exit_pnl:
                        trades.append(exit_pnl)
                        total_pnl   += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]
                        in_trade = False
                        continue
                    else:
                        skipped_no_exit_price += 1
                        in_trade_price_failures += 1
                        continue

                entry_credit  = trade_data["net_credit"]
                profit        = entry_credit - current_spread
                target_profit = entry_credit * TARGET_PCT

                if entry_credit > 0 and profit >= target_profit:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "Target")
                    if exit_pnl:
                        trades.append(exit_pnl)
                        total_pnl   += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]
                        in_trade = False

                        curr_direction = "BULLISH" if st_dir[i] == 1 else "BEARISH"
                        new_td = _try_enter(bar_time, bar_date, spot,
                                            curr_direction, "Target-ReEntry")
                        if new_td:
                            trade_data = new_td
                            in_trade   = True
                            target_reentries += 1
                        continue
                    else:
                        skipped_no_exit_price += 1
                        in_trade_price_failures += 1
            else:
                in_trade_price_failures += 1

        # ── FLIP SIGNAL WHEN FLAT ─────────────────────────────────────────────
        if any_flip and not in_trade:
            new_td = _try_enter(bar_time, bar_date, spot, any_flip, "Flip")
            if new_td:
                trade_data   = new_td
                in_trade     = True
                pending_flip = None
            else:
                pending_flip = any_flip

        # ── PENDING FLIP: retry till 09:45 ───────────────────────────────────
        if pending_flip and not in_trade and t >= TRADE_START:
            new_td = _try_enter(bar_time, bar_date, spot,
                                pending_flip, "PendingFlip")
            if new_td:
                trade_data         = new_td
                in_trade           = True
                pending_flip_entries += 1
                pending_flip       = None
            elif t >= dtime(9, 45):
                pending_flip = None

        # ── DAILY RE-ENTRY WHEN FLAT AT 09:15 ────────────────────────────────
        if not in_trade and t == TRADE_START:
            pending_flip = None   # clear stale flip from prev day
            curr_direction = "BULLISH" if st_dir[i] == 1 else "BEARISH"
            new_td = _try_enter(bar_time, bar_date, spot,
                                curr_direction, "DailyReEntry")
            if new_td:
                trade_data        = new_td
                in_trade          = True
                reentry_when_flat += 1

    # ── FORCE CLOSE AT DATA END ───────────────────────────────────────────────
    if in_trade:
        exit_pnl = None
        # Scan last full session (up to 78 bars = 6.5 h of 5-min)
        for lookback in range(min(78, len(df))):
            candidate = df.index[-(1 + lookback)]
            exit_pnl  = _exit_trade(provider, trade_data, candidate,
                                    "DataEnd", use_settlement=True,
                                    settlement_spot=close[-1])
            if exit_pnl:
                break
        if exit_pnl:
            trades.append(exit_pnl)
            total_pnl   += exit_pnl["total_pnl"]
            running_pnl += exit_pnl["total_pnl"]
            print(f"  DataEnd close: {trade_data['spread_type']} "
                  f"P&L Rs {exit_pnl['total_pnl']:+,.0f}")
        else:
            print("  WARNING: DataEnd close failed — dropped!")
            skipped_no_exit_price += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n  ── Simulation Counters ──────────────────────────────────────────")
    print(f"  ST flip signals             : {total_signals}")
    print(f"  Pending flip retries        : {pending_flip_entries}")
    print(f"  Daily re-entries (09:15)    : {reentry_when_flat}")
    print(f"  Target re-entries           : {target_reentries}")
    print(f"  Used next-week expiry       : {skipped_used_next_expiry}")
    print(f"  Skipped – no contract       : {skipped_no_contract}")
    print(f"  Skipped – no price (entry)  : {skipped_no_price}")
    print(f"  Skipped – negative credit   : {skipped_negative_credit}")
    print(f"  Skipped – insufficient margin: {skipped_margin}")
    print(f"  Dropped – no exit price     : {skipped_no_exit_price}")
    print(f"  In-trade price failures     : {in_trade_price_failures}")
    if SIMULATE_MARGIN:
        print(f"  SEBI peak margin violations : {peak_violations}")
    print(f"  Executed trades             : {len(trades)}")

    return trades, total_pnl, signal_log, peak_violations, peak_violation_dates


# ══════════════════════════════════════════════════════════════════════════════
# REPORTING
# ══════════════════════════════════════════════════════════════════════════════

def format_expiry_tag(expiry_date):
    if isinstance(expiry_date, str):
        expiry_date = pd.to_datetime(expiry_date)
    return expiry_date.strftime("%d %b %y").upper() if hasattr(expiry_date, "strftime") else str(expiry_date)

def build_contract_name(strike, opt_type, expiry):
    return f"NIFTY {int(strike)} {opt_type} {format_expiry_tag(expiry)}"

def print_results(tdf, total_pnl, signal_log, peak_viol, peak_viol_dates,
                  validation_results, out_dir):

    wins   = tdf[tdf["total_pnl"] > 0]
    losses = tdf[tdf["total_pnl"] <= 0]
    n_tr   = len(tdf)
    wr     = len(wins) / n_tr * 100
    avg_win  = wins["total_pnl"].mean()   if len(wins)   > 0 else 0
    avg_loss = losses["total_pnl"].mean() if len(losses) > 0 else 0
    gross_win  = wins["total_pnl"].sum()           if len(wins)   > 0 else 0
    gross_loss = abs(losses["total_pnl"].sum())    if len(losses) > 0 else 1
    pf   = gross_win / gross_loss if gross_loss > 0 else 99
    roi  = total_pnl / CAPITAL * 100
    cum  = tdf["total_pnl"].cumsum()
    mdd  = (cum.cummax() - cum).max()

    print(f"\n{'═'*90}")
    print("  RESULTS")
    print(f"  {'─'*65}")
    print(f"  Backtest range      : "
          f"{pd.Timestamp(tdf['entry_time'].min()).date()} → "
          f"{pd.Timestamp(tdf['exit_time'].max()).date()}")
    print(f"  Total Trades        : {n_tr}")
    print(f"  Winners             : {len(wins)} ({wr:.1f}%)")
    print(f"  Losers              : {len(losses)} ({100-wr:.1f}%)")
    print(f"  Avg Winner          : Rs {avg_win:+,.0f}")
    print(f"  Avg Loser           : Rs {avg_loss:+,.0f}")
    print(f"  Profit Factor       : {pf:.2f}")
    print(f"  Total P&L           : Rs {total_pnl:+,.0f}")
    print(f"  ROI on {CAPITAL/1e5:.0f}L           : {roi:+.1f}%")
    print(f"  Max Drawdown        : Rs {mdd:,.0f}")

    overnight_trades = tdf[tdf["overnight"]]
    intraday_trades  = tdf[~tdf["overnight"]]
    next_exp_trades  = tdf[tdf["is_next_expiry"]]

    print(f"\n  EXPIRY DAY TRADES (used next expiry):")
    if len(next_exp_trades) > 0:
        ne_wr = (next_exp_trades["total_pnl"] > 0).mean() * 100
        print(f"    {len(next_exp_trades)} trades | WR: {ne_wr:.1f}% | "
              f"Avg: Rs {next_exp_trades['total_pnl'].mean():+,.0f} | "
              f"Total: Rs {next_exp_trades['total_pnl'].sum():+,.0f}")

    print(f"\n  OVERNIGHT vs INTRADAY:")
    print(f"  {'':>22}  {'Trades':>7} {'WR%':>6} {'Avg P&L':>10} {'Total P&L':>12} {'Avg Days':>9}")
    print(f"  {'─'*70}")
    for label, sub in [("Overnight", overnight_trades), ("Intraday", intraday_trades)]:
        if len(sub) > 0:
            sub_wr = (sub["total_pnl"] > 0).mean() * 100
            print(f"  {label:>22}  {len(sub):>7} {sub_wr:>6.1f} "
                  f"{sub['total_pnl'].mean():>+10,.0f} "
                  f"{sub['total_pnl'].sum():>+12,.0f} "
                  f"{sub['days_held'].mean():>9.1f}")

    print(f"\n  DAYS HELD DISTRIBUTION:")
    for d in sorted(tdf["days_held"].unique()):
        sub  = tdf[tdf["days_held"] == d]
        dwr  = (sub["total_pnl"] > 0).mean() * 100
        lbl  = "same day" if d == 0 else f"{d} day{'s' if d>1 else ''}"
        print(f"    {lbl:>10}: {len(sub):4d} trades | WR: {dwr:5.1f}% | "
              f"Avg: Rs {sub['total_pnl'].mean():+,.0f} | "
              f"Total: Rs {sub['total_pnl'].sum():+,.0f}")

    print(f"\n  EXIT REASONS:")
    for reason, grp in tdf.groupby("exit_reason"):
        g_wr = (grp["total_pnl"] > 0).mean() * 100
        print(f"    {reason:10}: {len(grp):4d} trades | WR: {g_wr:5.1f}% | "
              f"Avg Days: {grp['days_held'].mean():.1f} | "
              f"P&L: Rs {grp['total_pnl'].sum():+,.0f}")

    for stype in ["BULL_PUT", "BEAR_CALL"]:
        sub = tdf[tdf["type"] == stype]
        if len(sub) > 0:
            swr = (sub["total_pnl"] > 0).mean() * 100
            print(f"\n  {stype}: {len(sub)} trades | WR: {swr:.1f}% | "
                  f"P&L: Rs {sub['total_pnl'].sum():+,.0f}")

    print(f"\n  PREMIUM ANALYSIS:")
    print(f"    Avg Sell Premium : Rs {tdf['sell_entry'].mean():.2f}")
    print(f"    Avg Buy Premium  : Rs {tdf['buy_entry'].mean():.2f}")
    print(f"    Avg Net Credit   : Rs {tdf['net_credit'].mean():.2f}")
    print(f"    Avg Exit Spread  : Rs {tdf['exit_spread'].mean():.2f}")
    print(f"    Avg P&L/unit     : Rs {tdf['pnl_per_unit'].mean():.2f}")

    # ── SIGNAL AUDIT ──────────────────────────────────────────────────────────
    print(f"\n  SIGNAL AUDIT (failures only):")
    fail_signals = [s for s in signal_log if s["outcome"].startswith("FAIL")]
    fail_counts  = defaultdict(int)
    for s in fail_signals:
        reason = s["outcome"].split(":")[1].split(" ")[0]
        fail_counts[reason] += 1
    if fail_counts:
        for k, v in sorted(fail_counts.items(), key=lambda x: -x[1]):
            print(f"    {k:25}: {v}")
    else:
        print("    None — all signals entered successfully")

    # Group failures by date to spot problem days
    if fail_signals:
        print(f"\n  PROBLEM DATES (> 3 failures on same day):")
        by_date = defaultdict(list)
        for s in fail_signals:
            by_date[pd.Timestamp(s["bar"]).date()].append(s)
        for d, sigs in sorted(by_date.items()):
            if len(sigs) >= 3:
                reasons = [s["outcome"].split(":")[1].split(" ")[0] for s in sigs]
                print(f"    {d}: {len(sigs)} failures → {set(reasons)}")

    # ── TRADE LOG ─────────────────────────────────────────────────────────────
    print(f"\n  TRADE LOG:")
    hdr = (f"  {'#':>4} {'Entry':>19} {'Exit':>19} {'Days':>4} {'NxExp':>5} "
           f"{'Type':>10} {'Sell Contract':>28} {'Buy Contract':>28} "
           f"{'Credit':>7} {'Reason':>8} {'P&L':>10}")
    print(hdr)
    print(f"  {'─'*len(hdr)}")
    for idx_t, row in tdf.iterrows():
        e_ts   = pd.Timestamp(row["entry_time"])
        x_ts   = pd.Timestamp(row["exit_time"])
        otype  = "PE" if row["type"] == "BULL_PUT" else "CE"
        sell_c = build_contract_name(row["sell_strike"], otype, row["expiry"])
        buy_c  = build_contract_name(row["buy_strike"],  otype, row["expiry"])
        ne_flg = "YES" if row.get("is_next_expiry", False) else ""
        print(f"  {idx_t+1:>4d} "
              f"{e_ts.strftime('%Y-%m-%d %H:%M'):>19} "
              f"{x_ts.strftime('%Y-%m-%d %H:%M'):>19} "
              f"{row['days_held']:>4.0f} "
              f"{ne_flg:>5} "
              f"{row['type']:>10} "
              f"{sell_c:>28} {buy_c:>28} "
              f"{row['net_credit']:>7.2f} "
              f"{row['exit_reason']:>8} "
              f"{row['total_pnl']:>+10,.0f}")

    # ── MONTHLY P&L ───────────────────────────────────────────────────────────
    print(f"\n  MONTHLY P&L:")
    tdf["month"] = tdf["entry_time"].dt.to_period("M")
    cumulative   = 0
    for month, grp in tdf.groupby("month"):
        m_pnl      = grp["total_pnl"].sum()
        cumulative += m_pnl
        m_wr        = (grp["total_pnl"] > 0).mean() * 100
        m_overnight = (grp["days_held"] > 0).sum()
        m_next      = grp["is_next_expiry"].sum()
        print(f"    {month} | {len(grp):3d} trades "
              f"({m_overnight} overnight, {m_next} next-exp) | "
              f"WR: {m_wr:5.1f}% | "
              f"P&L: Rs {m_pnl:+10,.0f} | Cumulative: Rs {cumulative:+10,.0f}")

    # ── SEBI MARGIN ───────────────────────────────────────────────────────────
    if SIMULATE_MARGIN and "equity_at_entry" in tdf.columns:
        for col in ["equity_at_entry", "margin_req", "margin_util_pct",
                    "elm_component", "elm_raw"]:
            tdf[col] = pd.to_numeric(tdf[col], errors="coerce").fillna(0)

        exp_day = tdf[tdf["elm_component"] > 0]
        norm_day = tdf[tdf["elm_component"] == 0]

        print(f"\n  SEBI MARGIN REPORT")
        print(f"  {'='*65}")
        print(f"  Capital simulated       : Rs {CAPITAL:>10,}")
        print(f"  SEBI Peak buffer        : {MARGIN_PEAK_BUFFER:.0%}")
        print(f"  Peak margin violations  : {peak_viol}")

        for label, sub in [("NORMAL DAY", norm_day), ("EXPIRY DAY", exp_day)]:
            print(f"\n  {label} ({len(sub)} trades):")
            if len(sub) > 0:
                print(f"    Avg margin required : Rs {sub['margin_req'].mean():>10,.0f}")
                print(f"    Max margin required : Rs {sub['margin_req'].max():>10,.0f}")
                print(f"    Avg utilisation     : {sub['margin_util_pct'].mean():.1f}%")
                print(f"    Min equity at entry : Rs {sub['equity_at_entry'].min():>10,.0f}")

        worst_idx = tdf["equity_at_entry"].idxmin()
        worst     = tdf.loc[worst_idx]
        headroom  = worst["equity_at_entry"] - worst["margin_req"]
        print(f"\n  WORST CAPITAL MOMENT:")
        print(f"    Date       : {pd.Timestamp(worst['entry_time']).date()}")
        print(f"    Equity     : Rs {worst['equity_at_entry']:>10,.0f}")
        print(f"    Margin req : Rs {worst['margin_req']:>10,.0f}")
        print(f"    Headroom   : Rs {headroom:>10,.0f}  "
              f"{'SAFE' if headroom >= 0 else '*** BREACHED ***'}")

        if peak_viol > 0:
            print(f"\n  PEAK MARGIN BREACH DETAIL:")
            print(f"  {'Date':<12} {'Time':<8} {'Equity':>12} "
                  f"{'Required':>12} {'Shortfall':>12} {'Expiry?':>8}")
            for v in peak_viol_dates[:20]:
                print(f"  {str(v['date']):<12} {str(v['time']):<8} "
                      f"Rs {v['equity']:>9,} "
                      f"Rs {v['margin_req']:>9,} "
                      f"Rs {v['shortfall']:>9,} "
                      f"{'YES' if v['is_expiry'] else 'no':>8}")

    # ── SAVE FILES ────────────────────────────────────────────────────────────
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    csv_path = out_path / f"backtest_ST{ST_PERIOD}_{ST_MULTIPLIER}_{SPREAD_WIDTH}.csv"
    tdf.drop(columns=["month"], errors="ignore").to_csv(csv_path, index=False)
    print(f"\n  Trades CSV  : {csv_path}")

    sig_path = out_path / "signal_audit.csv"
    pd.DataFrame(signal_log).to_csv(sig_path, index=False)
    print(f"  Signal log  : {sig_path}")

    # Plotly chart
    try:
        import plotly.graph_objects as go
        cum_pnl = tdf["total_pnl"].cumsum()
        fig     = go.Figure()
        fig.add_trace(go.Scatter(
            x=tdf["entry_time"], y=cum_pnl,
            mode="lines+markers", name=f"ST({ST_PERIOD},{ST_MULTIPLIER})",
            fill="tozeroy",
            marker=dict(size=4),
        ))
        fig.update_layout(
            title=(f"SuperTrend Credit Spread — ST({ST_PERIOD},{ST_MULTIPLIER}) "
                   f"| {SPREAD_WIDTH}pt Spread<br>"
                   f"<sub>Total P&L: Rs {total_pnl:+,.0f} | "
                   f"WR: {wr:.1f}% | PF: {pf:.2f} | ROI: {roi:+.1f}% | "
                   f"MDD: Rs {mdd:,.0f}</sub>"),
            xaxis_title="Entry Date",
            yaxis_title="Cumulative P&L (Rs)",
            template="plotly_dark",
            height=600, width=1500,
        )
        html_path = out_path / "backtest_pnl.html"
        fig.write_html(str(html_path))
        print(f"  Chart       : {html_path}")
        fig.show()
    except Exception as e:
        print(f"  Plotly error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("═" * 90)
    print("SUPERTREND CREDIT SPREAD BACKTEST — FULL DATA VALIDATION + ROBUST PRICING")
    print("═" * 90)
    print(f"  ST({ST_PERIOD}, {ST_MULTIPLIER}) | {TIMEFRAME} | "
          f"{SPREAD_WIDTH}pt spread | {NUM_LOTS}×{LOT_SIZE}={QTY} qty | "
          f"Capital: Rs {CAPITAL:,}")

    # ── Phase 0: validate data ────────────────────────────────────────────────
    con = duckdb.connect(DB_PATH, read_only=True)
    val = validate_data(con)

    # ── Determine warmup start (3 months before effective trade start) ────────
    warmup_start = (pd.Timestamp(val["trade_start_date"]) - pd.DateOffset(months=3)).date()
    warmup_start_str = str(warmup_start)

    # ── Load spot data ────────────────────────────────────────────────────────
    df_spot = load_spot_data(con, warmup_start_str)
    con.close()

    # ── Phase 1: run simulation ───────────────────────────────────────────────
    print(f"\n{'═'*90}")
    print(f"PHASE 1 — SIMULATION")
    print(f"  Trading: {val['trade_start_date']} → {val['trade_end_date']}")
    print("═" * 90)

    provider = OptionDataProvider(DB_PATH)

    trades, total_pnl, signal_log, peak_viol, peak_viol_dates = run_simulation(
        df_spot,
        provider,
        trade_start_date=val["trade_start_date"],
        trade_end_date=val["trade_end_date"],
    )

    provider.close()

    if not trades:
        print("\n  No trades executed.")
        exit(0)

    # ── Phase 2: reporting ────────────────────────────────────────────────────
    print(f"\n{'═'*90}")
    print("PHASE 2 — RESULTS")
    print("═" * 90)

    tdf = pd.DataFrame(trades)
    tdf["entry_time"] = pd.to_datetime(tdf["entry_time"])
    tdf["exit_time"]  = pd.to_datetime(tdf["exit_time"])

    print_results(
        tdf, total_pnl, signal_log,
        peak_viol, peak_viol_dates,
        val,
        out_dir=BASE_DIR,
    )

    print("\n✓ Done.")
