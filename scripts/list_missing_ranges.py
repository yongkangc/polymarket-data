import pandas as pd
from datetime import timedelta

# Load the "complete" data
filepath = 'data/binance_complete_minute_data.csv'

print("="*70)
print("MISSING DATA RANGES IN: binance_complete_minute_data.csv")
print("="*70)

df = pd.read_csv(filepath)
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Analyze first symbol (all symbols have same gaps)
symbol = 'BTCUSDT'
symbol_df = df[df['symbol'] == symbol].sort_values('timestamp')

# Calculate time differences
symbol_df = symbol_df.copy()
symbol_df['time_diff'] = symbol_df['timestamp'].diff()

# Find gaps > 1 minute
gaps = symbol_df[symbol_df['time_diff'] > timedelta(minutes=1)]

print(f"\nAnalyzing {symbol} (all symbols have identical gaps)")
print(f"Total gaps found: {len(gaps)}")
print(f"\n{'='*70}")
print("DETAILED MISSING RANGES")
print('='*70)

total_missing_minutes = 0

for i, (idx, row) in enumerate(gaps.iterrows(), 1):
    gap_end = row['timestamp']
    gap_duration = row['time_diff']
    gap_start = gap_end - gap_duration
    
    # Missing range is from (last_timestamp + 1min) to (current_timestamp - 1min)
    missing_start = gap_start + timedelta(minutes=1)
    missing_end = gap_end - timedelta(minutes=1)
    missing_duration = gap_duration - timedelta(minutes=1)
    missing_minutes = int(missing_duration.total_seconds() / 60)
    
    total_missing_minutes += missing_minutes
    
    print(f"\nGap #{i}:")
    print(f"  Missing: {missing_start} to {missing_end}")
    print(f"  Duration: {missing_duration} ({missing_minutes:,} minutes)")
    
    # Show which day ranges
    if missing_duration.days > 0:
        print(f"  Impact: {missing_duration.days} days + {missing_duration.seconds//3600} hours")

print(f"\n{'='*70}")
print("SUMMARY")
print('='*70)
print(f"Total gaps: {len(gaps)}")
print(f"Total missing minutes per symbol: {total_missing_minutes:,}")
print(f"Total missing hours per symbol: {total_missing_minutes/60:.1f}")
print(f"Total missing days per symbol: {total_missing_minutes/1440:.1f}")
print(f"\nFor all 3 symbols combined:")
print(f"  Missing: {total_missing_minutes * 3:,} minutes")
print(f"  Missing: {(total_missing_minutes * 3)/60:.1f} hours")
print(f"  Missing: {(total_missing_minutes * 3)/1440:.1f} days")

# Group by month to show pattern
print(f"\n{'='*70}")
print("MISSING DATA BY MONTH")
print('='*70)

for i, (idx, row) in enumerate(gaps.iterrows(), 1):
    gap_end = row['timestamp']
    gap_duration = row['time_diff']
    gap_start = gap_end - gap_duration
    missing_start = gap_start + timedelta(minutes=1)
    missing_minutes = int((gap_duration - timedelta(minutes=1)).total_seconds() / 60)
    
    month = missing_start.strftime('%B %Y')
    date_range = f"{missing_start.strftime('%b %d %H:%M')} - {gap_end.strftime('%b %d %H:%M')}"
    print(f"{month:15} | {date_range:30} | {missing_minutes:6,} min")

print('='*70)

