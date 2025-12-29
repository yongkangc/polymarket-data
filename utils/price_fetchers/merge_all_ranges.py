"""
Merge all aggregated ranges (1-20) into final complete dataset.
"""
import pandas as pd
from pathlib import Path

CHUNK_DIR = 'data/fill_chunks'
OUTPUT_FILE = 'data/binance_complete_minute_data.csv'

def merge_all():
    """Merge all aggregated files (ranges 1-20)."""
    print("=" * 70)
    print("MERGING ALL RANGES (1-20)")
    print("=" * 70)

    print("\nFinding aggregated files...")
    agg_files = sorted(Path(CHUNK_DIR).glob("fill_*_agg.csv"))

    print(f"Found {len(agg_files)} aggregated files")

    # Load all
    print("\nLoading files...")
    dfs = []
    for agg_file in agg_files:
        df = pd.read_csv(agg_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        dfs.append(df)
        print(f"  Loaded {agg_file.name}: {len(df):,} candles")

    # Merge all
    print("\nMerging...")
    final_df = pd.concat(dfs, ignore_index=True)
    print(f"  Total rows: {len(final_df):,}")

    # Remove any duplicates
    print("\nDeduplicating...")
    initial_rows = len(final_df)
    final_df = final_df.drop_duplicates(subset=['symbol', 'timestamp'])
    removed = initial_rows - len(final_df)
    if removed > 0:
        print(f"  Removed {removed:,} duplicate candles")
    else:
        print(f"  No duplicates found")

    # Sort by symbol and time
    print("\nSorting...")
    final_df = final_df.sort_values(['symbol', 'timestamp']).reset_index(drop=True)

    # Save
    print(f"\nWriting to {OUTPUT_FILE}...")
    final_df.to_csv(OUTPUT_FILE, index=False)

    file_size = Path(OUTPUT_FILE).stat().st_size / (1024 * 1024)

    print("\n" + "=" * 70)
    print("✓ MERGE COMPLETE!")
    print("=" * 70)
    print(f"Total rows: {len(final_df):,}")
    print(f"File size: {file_size:.1f} MB")
    print(f"Date range: {final_df['timestamp'].min()} to {final_df['timestamp'].max()}")
    print(f"Symbols: {', '.join(sorted(final_df['symbol'].unique()))}")

    # Per-symbol stats
    print("\nPer-symbol candles:")
    for symbol in sorted(final_df['symbol'].unique()):
        count = len(final_df[final_df['symbol'] == symbol])
        print(f"  {symbol}: {count:,} candles")

    print("=" * 70)

if __name__ == "__main__":
    merge_all()
