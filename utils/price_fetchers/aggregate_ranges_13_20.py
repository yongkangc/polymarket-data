"""
Aggregate ranges 13-20 (including 20a, 20b, 20c sub-chunks) to minute candles.
Memory-safe: processes one file at a time.
"""
import pandas as pd
from pathlib import Path

CHUNK_DIR = 'data/fill_chunks'

# All 10 chunks
CHUNK_IDS = [13, 14, 15, 16, 17, 18, 19, "20a", "20b", "20c"]

def aggregate_chunk(chunk_id):
    """Aggregate one raw file at a time."""
    raw_path = f"{CHUNK_DIR}/fill_{chunk_id}_raw.csv"
    agg_path = f"{CHUNK_DIR}/fill_{chunk_id}_agg.csv"

    # Skip if already aggregated
    if Path(agg_path).exists():
        file_size = Path(agg_path).stat().st_size / (1024 * 1024)
        print(f"Chunk {chunk_id} already aggregated ({file_size:.1f} MB), skipping")
        return

    if not Path(raw_path).exists():
        print(f"Chunk {chunk_id} raw file not found, skipping")
        return

    raw_size = Path(raw_path).stat().st_size / (1024 * 1024)
    print(f"Aggregating chunk {chunk_id} ({raw_size:.0f} MB raw)...")

    # Load one file at a time
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

    # Write aggregated
    agg_df.to_csv(agg_path, index=False)

    agg_size = Path(agg_path).stat().st_size / (1024 * 1024)
    print(f"✓ Chunk {chunk_id} complete: {len(agg_df):,} candles ({agg_size:.1f} MB)")

if __name__ == "__main__":
    print("=" * 70)
    print("AGGREGATING RANGES 13-20 (10 CHUNKS)")
    print("=" * 70)

    # Process all chunks sequentially
    for chunk_id in CHUNK_IDS:
        aggregate_chunk(chunk_id)
        print()

    print("=" * 70)
    print("✓ All chunks aggregated!")
    print("=" * 70)
