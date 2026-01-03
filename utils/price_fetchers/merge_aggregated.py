"""
Merge all aggregated files (ranges 1-12) into a single CSV.
Memory-safe: aggregated files are tiny (~1.3 MB each).
"""
import pandas as pd
from pathlib import Path

CHUNK_DIR = 'data/fill_chunks'
OUTPUT_FILE = 'data/binance_ranges_1_12_minute_data.csv'

def merge_all_aggregated():
    """Merge all aggregated files (ranges 1-12 only)."""
    print("=" * 70)
    print("MERGING AGGREGATED DATA (RANGES 1-12)")
    print("=" * 70)

    print("\nFinding aggregated files...")
    agg_files = sorted(Path(CHUNK_DIR).glob("fill_*_agg.csv"))

    # Filter to ranges 1-12 only
    agg_files = [f for f in agg_files if int(f.stem.split('_')[1]) <= 12]

    print(f"Found {len(agg_files)} aggregated files")

    if len(agg_files) == 0:
        print("ERROR: No aggregated files found!")
        return

    # Load all (tiny files, ~15 MB total)
    print("\nLoading files...")
    dfs = []
    for agg_file in agg_files:
        df = pd.read_csv(agg_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        dfs.append(df)
        print(f"  Loaded {agg_file.name}: {len(df):,} candles")

    # Merge all (low memory - only ~15 MB)
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
    print(f"Rows: {len(final_df):,}")
    print(f"File size: {file_size:.1f} MB")
    print(f"Date range: {final_df['timestamp'].min()} to {final_df['timestamp'].max()}")
    print(f"Symbols: {', '.join(sorted(final_df['symbol'].unique()))}")
    print("=" * 70)

if __name__ == "__main__":
    merge_all_aggregated()
