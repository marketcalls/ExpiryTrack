import pandas as pd
import numpy as np
import plotly.graph_objects as go

# ==========================
# LOAD DATA
# ==========================
file_path = '/Users/bond7/Desktop/Project/ExpiryTrack/straddle_full_backtest.csv'
df = pd.read_csv(file_path, parse_dates=['entry_time','exit_time'])

if df.empty:
    print("No data found in CSV")
    exit()

# ==========================
# CORE METRICS
# ==========================
total_trades = len(df)
total_pnl = df['pnl'].sum()

wins = df[df['pnl'] > 0]
losses = df[df['pnl'] <= 0]

win_rate = len(wins) / total_trades * 100
avg_win = wins['pnl'].mean() if len(wins) else 0
avg_loss = losses['pnl'].mean() if len(losses) else 0

gross_profit = wins['pnl'].sum()
gross_loss = abs(losses['pnl'].sum()) if len(losses) else 1
profit_factor = gross_profit / gross_loss if gross_loss != 0 else 0

# ==========================
# EQUITY + DRAWDOWN
# ==========================
df['cum_pnl'] = df['pnl'].cumsum()
df['peak'] = df['cum_pnl'].cummax()
df['drawdown'] = df['peak'] - df['cum_pnl']
max_dd = df['drawdown'].max()

# ==========================
# TIME ANALYSIS
# ==========================
df['date'] = df['entry_time'].dt.date
df['month'] = df['entry_time'].dt.to_period('M')

daily = df.groupby('date')['pnl'].sum()
monthly = df.groupby('month')['pnl'].sum()

# ==========================
# STREAK ANALYSIS
# ==========================
df['win'] = df['pnl'] > 0
streak = 0
max_win_streak = 0
max_loss_streak = 0

for val in df['win']:
    if val:
        streak = streak + 1 if streak > 0 else 1
        max_win_streak = max(max_win_streak, streak)
    else:
        streak = streak - 1 if streak < 0 else -1
        max_loss_streak = min(max_loss_streak, streak)

# ==========================
# BEST / WORST DAYS
# ==========================
best_day = daily.idxmax()
worst_day = daily.idxmin()

# ==========================
# REPORT PRINT
# ==========================
print("\n================ PROFESSSIONAL REPORT ================")

print("\n📊 OVERALL PERFORMANCE")
print(f"Total Trades      : {total_trades}")
print(f"Total PnL         : {total_pnl:,.2f}")
print(f"Win Rate          : {win_rate:.2f}%")
print(f"Profit Factor     : {profit_factor:.2f}")
print(f"Max Drawdown      : {max_dd:,.2f}")

print("\n📈 TRADE STATS")
print(f"Avg Win           : {avg_win:,.2f}")
print(f"Avg Loss          : {avg_loss:,.2f}")
print(f"Win/Loss Ratio    : {abs(avg_win/avg_loss) if avg_loss != 0 else 0:.2f}")

print("\n🔥 STREAK ANALYSIS")
print(f"Max Win Streak    : {max_win_streak}")
print(f"Max Loss Streak   : {abs(max_loss_streak)}")

print("\n📅 BEST / WORST DAYS")
print(f"Best Day          : {best_day} → {daily.max():,.2f}")
print(f"Worst Day         : {worst_day} → {daily.min():,.2f}")

print("\n📅 MONTHLY PERFORMANCE")
print(monthly)

# ==========================
# STRATEGY DIAGNOSIS
# ==========================
print("\n================ STRATEGY DIAGNOSIS ================")

if profit_factor < 1.2:
    print("❌ Weak edge: Profit factor too low")
elif profit_factor < 1.5:
    print("⚠️ Moderate edge: Needs improvement")
else:
    print("✅ Strong edge")

if max_dd > total_pnl * 0.5:
    print("❌ High drawdown risk")
else:
    print("✅ Acceptable drawdown")

if abs(avg_loss) > avg_win * 2:
    print("❌ Losses too large → add stop loss")
else:
    print("✅ Risk balanced")

# ==========================
# EQUITY CURVE (PLOTLY)
# ==========================
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df.index.astype(str),
    y=df['cum_pnl'],
    mode='lines',
    name='Equity Curve'
))

fig.update_layout(
    title="Straddle Strategy Equity Curve",
    xaxis=dict(type="category"),
    template="plotly_dark"
)

fig.show()

# ==========================
# SAVE OUTPUT
# ==========================
df.to_csv('/mnt/data/straddle_analysis_output.csv', index=False)
print("\n✅ Saved: straddle_analysis_output.csv")