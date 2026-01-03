"""Aggregate the 12 GB partial raw data into minute candles."""
import pandas as pd
from pathlib import Path
import glob
import time

CHUNK_DIR = 'data/chunks'
OUTPUT_FILE = 'data/binance_sep_dec28_2025_partial_minute_data.csv'

print("="*70)
print("AGGREGATING 12 GB PARTIAL DATA")
print("="*70)

# Find all raw chunks
raw_files = sorted(glob.glob(f"{CHUNK_DIR}/chunk_*_raw.csv"))
print(f"\nFound {len(raw_files)} raw chunk files")

all_agg_dfs = []
start_time = time.time()

for i, raw_file in enumerate(raw_files, 1):
    chunk_start = time.time()
    print(f"\n[{i}/{len(raw_files)}] Processing {Path(raw_file).name}...")
    
    # Read raw data
    df = pd.read_csv(raw_file)
    print(f"  Loaded {len(df):,} raw trades")
    
    # Parse and convert
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['price'] = df['price'].astype(float)
    df['amount'] = df['amount'].astype(float)
    df['minute'] = df['timestamp'].dt.floor('1min')
    
    # Aggregate to OHLCV
    agg_df = df.groupby(['symbol', 'minute']).agg({
        'price': ['first', 'max', 'min', 'last'],
        'amount': 'sum',
        'timestamp': 'count'
    }).reset_index()
    
    agg_df.columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'trades']
    
    chunk_time = time.time() - chunk_start
    print(f"  → {len(agg_df):,} minute candles ({chunk_time:.1f}s)")
    
    all_agg_dfs.append(agg_df)
    del df  # Free memory

# Combine all
print("\n" + "="*70)
print("PHASE 3: MERGING")
print("="*70)
final_df = pd.concat(all_agg_dfs, ignore_index=True)
final_df = final_df.sort_values(['symbol', 'timestamp']).reset_index(drop=True)

# Save
Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
final_df.to_csv(OUTPUT_FILE, index=False)

# Summary
file_size = Path(OUTPUT_FILE).stat().st_size / (1024 * 1024)
total_time = time.time() - start_time

print(f"\n{'='*70}")
print("RESULTS")
print("="*70)
print(f"✓ Total minute candles: {len(final_df):,}")
print(f"✓ Date range: {final_df['timestamp'].min()} to {final_df['timestamp'].max()}")
print(f"✓ File: {OUTPUT_FILE}")
print(f"✓ Size: {file_size:.2f} MB")
print(f"✓ Time: {total_time/60:.1f} minutes")

print(f"\nPer symbol:")
summary = final_df.groupby('symbol').agg({
    'timestamp': ['min', 'max', 'count'],
    'volume': 'sum',
    'trades': 'sum'
})
print(summary)
print("="*70)
