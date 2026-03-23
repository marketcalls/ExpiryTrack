"""
FULL STRADDLE BACKTEST + ANALYSIS (SINGLE FILE)
==============================================
✔ Backtest on DuckDB option data
✔ Detailed trades (entry/exit/strike/premiums)
✔ Day-wise + Month-wise analytics
✔ Profit Factor (overall, daily, monthly)
✔ Equity + Drawdown + Daily + Monthly charts (Plotly, category axis)
"""

import duckdb
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, time as dtime, timedelta

# ======================
# CONFIG
# ======================
DB_PATH = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"
START_DATE = "2024-10-03"
END_DATE = "2026-03-09"

LOT_SIZE = 65
LOTS = 5
QTY = LOT_SIZE * LOTS

ADJ_POINTS = 48
ENTRY_TIME = dtime(9, 15)
EXIT_TIME = dtime(15, 29)

# ======================
# OPTION PROVIDER
# ======================
class OptionDataProvider:

    def __init__(self, db_path):
        print("🔄 Loading contracts...")
        self.con = duckdb.connect(db_path)

        rows = self.con.execute("""
            SELECT expired_instrument_key, strike_price,
                   contract_type, expiry_date
            FROM contracts
            WHERE instrument_key = 'NSE_INDEX|Nifty 50'
              AND contract_type IN ('CE','PE')
        """).fetchall()

        self.contract_index = {}
        self.expiries = set()

        for key, strike, ctype, expiry in rows:
            expiry = pd.to_datetime(expiry).date()
            self.contract_index[(float(strike), ctype, expiry)] = key
            self.expiries.add(expiry)

        self.expiries = sorted(self.expiries)
        print(f"✅ Contracts Loaded: {len(self.contract_index)}")

    def get_expiry(self, date):
        for exp in self.expiries:
            if exp >= date:
                return exp
        return None

    def get_contract(self, strike, opt_type, expiry):
        return self.contract_index.get((float(strike), opt_type, expiry))

    def get_price(self, key, ts):
        ts = ts.replace(second=0, microsecond=0)

        for i in range(5):
            t = ts - timedelta(minutes=i)
            row = self.con.execute("""
                SELECT close FROM historical_data
                WHERE expired_instrument_key = ?
                AND timestamp = ?
            """, [key, t]).fetchone()

            if row and row[0] and row[0] > 0:
                return float(row[0])

        return None

    def close(self):
        self.con.close()

# ======================
# LOAD SPOT
# ======================
def load_spot():
    print("🔄 Loading spot data...")
    con = duckdb.connect(DB_PATH)

    df = con.execute(f"""
        SELECT timestamp, open, high, low, close
        FROM candle_data
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND interval = '1minute'
          AND timestamp >= '{START_DATE}'
          AND timestamp <= '{END_DATE}'
        ORDER BY timestamp
    """).fetchdf()

    con.close()

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")

    return df

# ======================
# STRATEGY
# ======================
def get_atm(price):
    return round(price / 100) * 100


def enter_straddle(provider, ts, spot, expiry):
    strike = get_atm(spot)

    ce = provider.get_contract(strike, "CE", expiry)
    pe = provider.get_contract(strike, "PE", expiry)

    if not ce or not pe:
        return None

    ce_price = provider.get_price(ce, ts)
    pe_price = provider.get_price(pe, ts)

    if ce_price is None or pe_price is None:
        return None

    return {
        "entry_time": ts,
        "strike": strike,
        "ce_key": ce,
        "pe_key": pe,
        "ce_entry": ce_price,
        "pe_entry": pe_price,
        "entry_val": ce_price + pe_price
    }


def exit_straddle(provider, trade, ts):
    ce = provider.get_price(trade["ce_key"], ts)
    pe = provider.get_price(trade["pe_key"], ts)

    if ce is None or pe is None:
        return None

    exit_val = ce + pe
    pnl = (trade["entry_val"] - exit_val) * QTY

    return {
        "entry_time": trade["entry_time"],
        "exit_time": ts,
        "strike": trade["strike"],
        "ce_entry": trade["ce_entry"],
        "pe_entry": trade["pe_entry"],
        "ce_exit": ce,
        "pe_exit": pe,
        "entry_val": trade["entry_val"],
        "exit_val": exit_val,
        "pnl": pnl
    }

# ======================
# BACKTEST
# ======================
def run_backtest():

    print("🔁 OpenAlgo Python Bot is running.")

    df = load_spot()
    provider = OptionDataProvider(DB_PATH)

    trades = []

    df["date"] = df.index.date
    grouped = df.groupby("date")

    for day, day_df in grouped:

        expiry = provider.get_expiry(day)
        if expiry is None:
            continue

        in_trade = False
        trade = None
        current_strike = None

        for ts, row in day_df.iterrows():

            t = ts.time()
            spot = row["close"]

            if not in_trade and t == ENTRY_TIME:
                trade = enter_straddle(provider, ts, spot, expiry)
                if trade:
                    in_trade = True
                    current_strike = trade["strike"]

            elif in_trade:
                if abs(spot - current_strike) >= ADJ_POINTS:
                    result = exit_straddle(provider, trade, ts)
                    if result:
                        trades.append(result)

                    trade = enter_straddle(provider, ts, spot, expiry)
                    if trade:
                        current_strike = trade["strike"]
                    else:
                        in_trade = False

            if in_trade and t == EXIT_TIME:
                result = exit_straddle(provider, trade, ts)
                if result:
                    trades.append(result)
                in_trade = False

    provider.close()

    df_trades = pd.DataFrame(trades)
    df_trades.to_csv("straddle_full_backtest.csv", index=False)

    return df_trades

# ======================
# ANALYSIS (FULL)
# ======================
def profit_factor(series):
    wins = series[series > 0].sum()
    losses = abs(series[series <= 0].sum())
    return (wins / losses) if losses != 0 else np.nan


def run_analysis(df):

    # ---------- BASIC FIELDS ----------
    df['date'] = pd.to_datetime(df['entry_time']).dt.date
    df['month'] = pd.to_datetime(df['entry_time']).dt.to_period('M').astype(str)

    # ---------- SAVE FULL DETAILS ----------
    df.to_csv("detailed_trades_daywise.csv", index=False)

    print("\n📊 SAMPLE TRADE DETAILS")
    print(df[['entry_time','exit_time','strike','ce_entry','pe_entry','ce_exit','pe_exit','pnl']].head(10))

    # ---------- AGGREGATIONS ----------
    daily = df.groupby('date').agg(pnl=('pnl','sum'), trades=('pnl','count'))
    monthly = df.groupby('month').agg(pnl=('pnl','sum'), trades=('pnl','count'))

    # ---------- PROFIT FACTOR ----------
    overall_pf = profit_factor(df['pnl'])
    daily['pf'] = df.groupby('date')['pnl'].apply(profit_factor)
    monthly['pf'] = df.groupby('month')['pnl'].apply(profit_factor)

    # ---------- EQUITY + DRAWDOWN ----------
    df = df.sort_values('exit_time').reset_index(drop=True)
    df['cum_pnl'] = df['pnl'].cumsum()
    df['peak'] = df['cum_pnl'].cummax()
    df['drawdown'] = df['peak'] - df['cum_pnl']

    # ---------- PRINT ----------
    print("\n📊 OVERALL")
    print(f"Total Trades: {len(df)}")
    print(f"Total PnL: {df['pnl'].sum():,.2f}")
    print(f"Profit Factor: {overall_pf:.2f}")
    print(f"Max Drawdown: {df['drawdown'].max():,.2f}")

    print("\n📅 DAILY (TOP 10)")
    print(daily.head(10))

    print("\n📅 MONTHLY")
    print(monthly)

    # ---------- SAVE SUMMARIES ----------
    daily.to_csv("daily_summary.csv")
    monthly.to_csv("monthly_summary.csv")

    # ======================
    # PLOTLY DASHBOARD
    # ======================
    fig = make_subplots(
        rows=4, cols=1,
        subplot_titles=(
            "Equity Curve",
            "Drawdown",
            "Daily PnL",
            "Monthly PnL"
        )
    )

    # Equity
    fig.add_trace(go.Scatter(
        x=df.index.astype(str),
        y=df['cum_pnl'],
        mode='lines',
        line=dict(color='cyan')
    ), row=1, col=1)

    # Drawdown
    fig.add_trace(go.Scatter(
        x=df.index.astype(str),
        y=df['drawdown'],
        mode='lines',
        line=dict(color='orange')
    ), row=2, col=1)

    # Daily colored
    dcolors = ['green' if x > 0 else 'red' for x in daily['pnl']]
    fig.add_trace(go.Bar(
        x=daily.index.astype(str),
        y=daily['pnl'],
        marker_color=dcolors
    ), row=3, col=1)

    # Monthly colored
    mcolors = ['green' if x > 0 else 'red' for x in monthly['pnl']]
    fig.add_trace(go.Bar(
        x=monthly.index.astype(str),
        y=monthly['pnl'],
        marker_color=mcolors
    ), row=4, col=1)

    fig.update_layout(
        template="plotly_dark",
        height=1000,
        xaxis=dict(type="category"),
        xaxis2=dict(type="category"),
        xaxis3=dict(type="category"),
        xaxis4=dict(type="category")
    )

    fig.show()

# ======================
# MAIN
# ======================
if __name__ == "__main__":

    df_trades = run_backtest()

    if not df_trades.empty:
        run_analysis(df_trades)
    else:
        print("No trades generated")
