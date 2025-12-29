import pandas as pd
from datetime import timedelta
import os

# File to verify
filepath = 'data/binance_complete_minute_data.csv'

print("="*70)
print("VERIFYING: binance_complete_minute_data.csv")
print("="*70)

# Check if file exists
if not os.path.exists(filepath):
    print("\n✗ FILE NOT FOUND")
    exit(1)

# Get file size
file_size_mb = os.path.getsize(filepath) / (1024**2)
print(f"\n✓ File exists")
print(f"  Size: {file_size_mb:.1f} MB")

# Load data
print("\nLoading data...")
df = pd.read_csv(filepath)
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Basic stats
total_rows = len(df)
symbols = df['symbol'].unique() if 'symbol' in df.columns else []

print(f"\n✓ Data loaded")
print(f"  Total rows: {total_rows:,}")
print(f"  Symbols: {', '.join(sorted(symbols))}")

# Date range
start_date = df['timestamp'].min()
end_date = df['timestamp'].max()
days = (end_date - start_date).days

print(f"\n✓ Date Range")
print(f"  Start: {start_date}")
print(f"  End: {end_date}")
print(f"  Duration: {days} days")

# Per symbol analysis
print("\n" + "="*70)
print("PER SYMBOL ANALYSIS")
print("="*70)

all_complete = True

for symbol in sorted(symbols):
    symbol_df = df[df['symbol'] == symbol].sort_values('timestamp')
    
    start = symbol_df['timestamp'].min()
    end = symbol_df['timestamp'].max()
    expected_minutes = int((end - start).total_seconds() / 60) + 1
    actual_minutes = len(symbol_df)
    completeness = (actual_minutes / expected_minutes) * 100
    
    # Check for gaps
    symbol_df = symbol_df.copy()
    symbol_df['time_diff'] = symbol_df['timestamp'].diff()
    gaps = symbol_df[symbol_df['time_diff'] > timedelta(minutes=1)]
    
    print(f"\n{symbol}:")
    print(f"  Rows: {actual_minutes:,}")
    print(f"  Expected: {expected_minutes:,}")
    print(f"  Completeness: {completeness:.4f}%")
    print(f"  Gaps: {len(gaps)}")
    
    if completeness < 100:
        all_complete = False
        print(f"  ⚠ Missing {expected_minutes - actual_minutes:,} minutes")
        
        if len(gaps) > 0:
            print(f"\n  Top 5 largest gaps:")
            top_gaps = gaps.nlargest(5, 'time_diff')[['timestamp', 'time_diff']]
            for idx, row in top_gaps.iterrows():
                gap_start = row['timestamp'] - row['time_diff']
                print(f"    {gap_start} to {row['timestamp']}: {row['time_diff']}")

# VERIFICATION AGAINST CLAIMS
print("\n" + "="*70)
print("VERIFICATION AGAINST CLAIMS")
print("="*70)

claims = {
    "Date Range": "September 1 - December 26, 2025 (115 days)",
    "Total Candles": "423,876 (141,292 per symbol)",
    "File Size": "29.8 MB",
    "Symbols": "BTCUSDT, ETHUSDT, SOLUSDT"
}

# Check date range
expected_start = pd.Timestamp('2025-09-01')
expected_end = pd.Timestamp('2025-12-26')
expected_days = 115

date_range_match = (
    start_date.date() == expected_start.date() and
    end_date.date() <= expected_end.date() and
    days >= expected_days - 1  # Allow 1 day tolerance
)

print(f"\n✓ Date Range Claim: {claims['Date Range']}")
print(f"  Actual: {start_date.date()} to {end_date.date()} ({days} days)")
print(f"  Status: {'✓ MATCH' if date_range_match else '✗ MISMATCH'}")

# Check total candles
expected_total = 423876
expected_per_symbol = 141292

candles_match = total_rows == expected_total

print(f"\n✓ Total Candles Claim: {claims['Total Candles']}")
print(f"  Actual: {total_rows:,} total")
for symbol in sorted(symbols):
    count = len(df[df['symbol'] == symbol])
    print(f"    {symbol}: {count:,}")
print(f"  Status: {'✓ MATCH' if candles_match else '✗ MISMATCH'}")

# Check file size
expected_size = 29.8
size_match = abs(file_size_mb - expected_size) < 2.0  # Allow 2MB tolerance

print(f"\n✓ File Size Claim: {claims['File Size']}")
print(f"  Actual: {file_size_mb:.1f} MB")
print(f"  Status: {'✓ MATCH' if size_match else '✗ MISMATCH'}")

# Check symbols
expected_symbols = set(['BTCUSDT', 'ETHUSDT', 'SOLUSDT'])
actual_symbols = set(symbols)
symbols_match = actual_symbols == expected_symbols

print(f"\n✓ Symbols Claim: {claims['Symbols']}")
print(f"  Actual: {', '.join(sorted(actual_symbols))}")
print(f"  Status: {'✓ MATCH' if symbols_match else '✗ MISMATCH'}")

# Final verdict
print("\n" + "="*70)
print("FINAL VERDICT")
print("="*70)

all_match = date_range_match and candles_match and size_match and symbols_match

if all_match and all_complete:
    print("\n✓✓✓ ALL CLAIMS VERIFIED - DATA IS COMPLETE ✓✓✓")
elif all_match and not all_complete:
    print("\n⚠ CLAIMS ARE ACCURATE BUT DATA HAS GAPS")
else:
    print("\n✗ SOME CLAIMS DO NOT MATCH ACTUAL DATA")

print("\n" + "="*70)

