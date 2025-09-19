"""Check exported CSV file content"""
import pandas as pd

# Read the exported file
df = pd.read_csv('exports/OpenAlgo_ExpiryTrack_Nifty_50_20250919_090900.csv')

print("File Statistics:")
print(f"Total rows: {len(df)}")
print(f"Columns: {list(df.columns)}")
print(f"\nFirst 5 rows:")
print(df.head())

# Check for different types of options
print(f"\n\nSample CALL option:")
call_sample = df[df['openalgo_symbol'].str.contains('C', na=False)].head(1)
print(call_sample.to_string())

print(f"\n\nSample PUT option:")
put_sample = df[df['openalgo_symbol'].str.contains('P', na=False) & ~df['openalgo_symbol'].str.contains('C', na=False)].head(1)
print(put_sample.to_string())

# Check open interest values
print(f"\n\nOpen Interest Statistics:")
print(f"Min OI: {df['oi'].min()}")
print(f"Max OI: {df['oi'].max()}")
print(f"Mean OI: {df['oi'].mean():.2f}")
print(f"Non-zero OI rows: {(df['oi'] != 0).sum()}")

# Check date and time columns
print(f"\n\nDate Range:")
print(f"Start: {df['date'].min()} {df['time'].min()}")
print(f"End: {df['date'].max()} {df['time'].max()}")