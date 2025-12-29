"""
Aggregate ranges 11-12 (raw data only) to minute candles.
Memory-safe: processes one file at a time.
"""
import pandas as pd
from pathlib import Path

CHUNK_DIR = 'data/fill_chunks'

def aggregate_range(range_id: int):
    """Aggregate one raw file at a time."""
    raw_path = f"{CHUNK_DIR}/fill_{range_id:02d}_raw.csv"
    agg_path = f"{CHUNK_DIR}/fill_{range_id:02d}_agg.csv"

    # Skip if already aggregated
    if Path(agg_path).exists():
        file_size = Path(agg_path).stat().st_size / (1024 * 1024)
        print(f"Range {range_id} already aggregated ({file_size:.1f} MB), skipping")
        return

    if not Path(raw_path).exists():
        print(f"Range {range_id} raw file not found, skipping")
        return

    raw_size = Path(raw_path).stat().st_size / (1024 * 1024)
    print(f"Aggregating range {range_id} ({raw_size:.0f} MB raw)...")

    # Load one file at a time (1-2 GB peak memory)
    df = pd.read_csv(raw_path)
    print(f"  Loaded {len(df):,} trades")

    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
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

    # Free memory before writing
    del df

    # Write aggregated (tiny file ~1.3 MB)
    agg_df.to_csv(agg_path, index=False)

    agg_size = Path(agg_path).stat().st_size / (1024 * 1024)
    print(f"✓ Range {range_id} complete: {len(agg_df):,} candles ({agg_size:.1f} MB)")

if __name__ == "__main__":
    print("=" * 70)
    print("AGGREGATING RANGES 11-12")
    print("=" * 70)

    # Process ranges 11-12 sequentially
    for range_id in [11, 12]:
        aggregate_range(range_id)
        print()

    print("=" * 70)
    print("✓ All ranges aggregated!")
    print("=" * 70)
