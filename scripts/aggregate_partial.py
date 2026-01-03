"""
Aggregate the partial raw data we have and combine into final output.
This will work with incomplete chunks - missing dates will just be gaps.
"""
import pandas as pd
from pathlib import Path
import glob

CHUNK_DIR = 'data/chunks'
OUTPUT_FILE = 'data/binance_sep_dec28_2025_partial_minute_data.csv'

print("="*70)
print("AGGREGATING PARTIAL DATA")
print("="*70)

# Find all raw chunks
raw_files = sorted(glob.glob(f"{CHUNK_DIR}/chunk_*_raw.csv"))
print(f"\nFound {len(raw_files)} raw chunk files")

all_agg_dfs = []

for i, raw_file in enumerate(raw_files, 1):
    print(f"\n[{i}/{len(raw_files)}] Processing {Path(raw_file).name}...")
    
    # Read raw data
    df = pd.read_csv(raw_file)
    print(f"  Loaded {len(df):,} raw trades")
    
    # Parse timestamps (use mixed format to handle varying precision)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
    df['price'] = df['price'].astype(float)
    df['amount'] = df['amount'].astype(float)
    
    # Round to minute
    df['minute'] = df['timestamp'].dt.floor('1min')
    
    # Aggregate to OHLCV
    agg_df = df.groupby(['symbol', 'minute']).agg({
        'price': ['first', 'max', 'min', 'last'],
        'amount': 'sum',
        'timestamp': 'count'
    }).reset_index()
    
    agg_df.columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'trades']
    
    print(f"  Aggregated to {len(agg_df):,} minute candles")
    all_agg_dfs.append(agg_df)
    del df  # Free memory

# Combine all
print("\nCombining all aggregated chunks...")
final_df = pd.concat(all_agg_dfs, ignore_index=True)
final_df = final_df.sort_values(['symbol', 'timestamp']).reset_index(drop=True)

print(f"\nTotal minute candles: {len(final_df):,}")
print(f"Date range: {final_df['timestamp'].min()} to {final_df['timestamp'].max()}")
print(f"\nPer symbol:")
print(final_df.groupby('symbol')['timestamp'].agg(['min', 'max', 'count']))

# Save
Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
final_df.to_csv(OUTPUT_FILE, index=False)

file_size = Path(OUTPUT_FILE).stat().st_size / (1024 * 1024)
print(f"\n✓ Saved to: {OUTPUT_FILE}")
print(f"✓ File size: {file_size:.2f} MB")
print("="*70)
