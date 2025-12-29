"""
Memory-safe parallel Binance data fetcher using Tardis API

This script uses a THREE-PHASE approach to avoid OOM while maintaining speed:

PHASE 1: Fetch raw trades (PARALLEL) ✓ Memory-safe
  - Streams trades and writes directly to CSV (no aggregation)
  - Can run 10 chunks in parallel safely (only write buffer in memory)

PHASE 2: Aggregate to minute candles (SEQUENTIAL) ✓ Memory-safe
  - Processes one raw CSV at a time
  - Aggregates to OHLCV minute candles

PHASE 3: Merge aggregated chunks ✓ Memory-safe
  - Combines all minute-level CSVs into final output

Usage:
    python fetch_binance_parallel.py

Configuration:
    - START_DATE: First date to fetch (YYYY-MM-DD)
    - END_DATE: Last date to fetch (YYYY-MM-DD)
    - NUM_CHUNKS: Number of chunks to split into
    - MAX_PARALLEL: Chunks to fetch simultaneously (safe to use 10+)
    - SYMBOLS: List of trading pairs to fetch (lowercase for Binance)
"""
import asyncio
import os
import time
import csv
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
from tardis_client import TardisClient, Channel
from typing import List, Tuple, Dict
from pathlib import Path


# =============================================================================
# CONFIGURATION - Edit these parameters
# =============================================================================

START_DATE = "2025-09-01"  # First date to fetch
END_DATE = "2025-12-28"    # Last date to fetch (inclusive)
NUM_CHUNKS = 20            # Total number of chunks (more chunks = more granular progress)
MAX_PARALLEL = 20          # Max chunks to fetch in parallel (aggressive - all at once)

SYMBOLS = [
    'btcusdt',   # Bitcoin/USDT
    'ethusdt',   # Ethereum/USDT
    'solusdt'    # Solana/USDT
]

OUTPUT_FILE = 'data/binance_sep_dec28_2025_minute_data.csv'
CHUNK_DIR = 'data/chunks'    # Directory to store intermediate chunk files
KEEP_RAW = False             # Keep raw trade CSVs after aggregation
KEEP_CHUNKS = False          # Keep aggregated chunk CSVs after combining


# =============================================================================
# Helper Functions
# =============================================================================

def get_chunk_filename(chunk_id: int, from_date: str, to_date: str, suffix: str = "agg") -> str:
    """Generate filename for a chunk file.

    Args:
        chunk_id: Chunk number
        from_date: Start date
        to_date: End date
        suffix: File type - "raw" for raw trades, "agg" for aggregated candles
    """
    return f"chunk_{chunk_id:02d}_{from_date}_to_{to_date}_{suffix}.csv"


def chunk_exists(chunk_id: int, from_date: str, to_date: str, suffix: str = "agg") -> bool:
    """Check if a chunk file already exists."""
    chunk_path = Path(CHUNK_DIR) / get_chunk_filename(chunk_id, from_date, to_date, suffix)
    return chunk_path.exists()


def split_date_range(start_date: str, end_date: str, num_chunks: int) -> List[Tuple[str, str]]:
    """Split a date range into roughly equal chunks."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    total_days = (end - start).days + 1
    days_per_chunk = max(1, total_days // num_chunks)

    chunks = []
    current_start = start

    for i in range(num_chunks):
        if i == num_chunks - 1:
            current_end = end
        else:
            current_end = current_start + timedelta(days=days_per_chunk - 1)

        if current_end > end:
            current_end = end

        chunks.append((
            current_start.strftime("%Y-%m-%d"),
            current_end.strftime("%Y-%m-%d")
        ))

        current_start = current_end + timedelta(days=1)
        if current_start > end:
            break

    return chunks


def finalize_candles(candles: Dict) -> pd.DataFrame:
    """
    Convert aggregated candle data to DataFrame.

    Args:
        candles: Dict of {symbol: {minute: [trades]}}

    Returns:
        DataFrame with OHLCV data
    """
    rows = []
    for symbol, minute_data in candles.items():
        for minute, trades in sorted(minute_data.items()):
            prices = [t['price'] for t in trades]
            amounts = [t['amount'] for t in trades]

            rows.append({
                'symbol': symbol,
                'timestamp': minute,
                'open': prices[0],
                'high': max(prices),
                'low': min(prices),
                'close': prices[-1],
                'volume': sum(amounts),
                'trades': len(trades)
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(['symbol', 'timestamp']).reset_index(drop=True)

    return df


async def fetch_chunk_raw(
    api_key: str,
    symbols: List[str],
    from_date: str,
    to_date: str,
    chunk_id: int
) -> str:
    """
    Fetch raw trades and write directly to CSV (NO aggregation, minimal memory).

    This function streams trades and writes them immediately to disk.
    Memory usage: O(write buffer) = ~5-10 MB per chunk

    Args:
        api_key: Tardis API key
        symbols: List of trading pair symbols
        from_date: Start date
        to_date: End date
        chunk_id: Chunk identifier

    Returns:
        Path to saved raw chunk file
    """
    raw_filename = get_chunk_filename(chunk_id, from_date, to_date, suffix="raw")
    raw_path = Path(CHUNK_DIR) / raw_filename

    # Skip if already exists
    if raw_path.exists():
        file_size = raw_path.stat().st_size / (1024 * 1024)
        print(f"[Chunk {chunk_id}] ✓ Raw file exists ({file_size:.2f} MB), skipping")
        return str(raw_path)

    chunk_start_time = time.time()
    print(f"\n{'='*70}")
    print(f"[Chunk {chunk_id}] PHASE 1: Fetching raw trades: {from_date} to {to_date}")
    print(f"{'='*70}")

    # Initialize Tardis client
    tardis_client = TardisClient(api_key=api_key)

    # Calculate API end date
    to_date_obj = datetime.strptime(to_date, "%Y-%m-%d") + timedelta(days=1)
    to_date_api = to_date_obj.strftime("%Y-%m-%d")

    # Fetch trades
    messages = tardis_client.replay(
        exchange="binance",
        from_date=from_date,
        to_date=to_date_api,
        filters=[Channel(name="trade", symbols=symbols)]
    )

    # Write raw trades directly to CSV
    print(f"[Chunk {chunk_id}] Writing raw trades to disk...")

    Path(CHUNK_DIR).mkdir(parents=True, exist_ok=True)
    trade_count = 0

    # Open CSV file and write trades as they stream
    with open(raw_path, 'w', newline='') as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow(['timestamp', 'symbol', 'price', 'amount'])

        async for timestamp, trade_msg in messages:
            # Extract trade data
            trade_data = trade_msg.get('data', {})
            symbol = trade_data.get('s')
            price_str = trade_data.get('p', '0')
            amount_str = trade_data.get('q', '0')

            if not symbol:
                continue

            # Write row immediately (no buffering in memory)
            writer.writerow([
                timestamp.isoformat(),
                symbol,
                price_str,
                amount_str
            ])

            trade_count += 1

            # Progress update every 1M trades
            if trade_count % 1000000 == 0:
                print(f"[Chunk {chunk_id}] Wrote {trade_count:,} raw trades...")

    file_size = raw_path.stat().st_size / (1024 * 1024)
    chunk_elapsed = time.time() - chunk_start_time

    print(f"[Chunk {chunk_id}] ✓ Wrote {trade_count:,} raw trades ({file_size:.2f} MB) in {chunk_elapsed:.1f}s")
    print(f"{'='*70}\n")

    return str(raw_path)


def aggregate_chunk(
    chunk_id: int,
    from_date: str,
    to_date: str,
    raw_path: str
) -> str:
    """
    Aggregate raw trades into minute candles (OHLCV).

    Reads raw CSV one chunk at a time and aggregates to minute-level data.
    Memory usage: O(single raw file) = ~500 MB - 1 GB

    Args:
        chunk_id: Chunk identifier
        from_date: Start date
        to_date: End date
        raw_path: Path to raw trades CSV

    Returns:
        Path to saved aggregated chunk file
    """
    agg_filename = get_chunk_filename(chunk_id, from_date, to_date, suffix="agg")
    agg_path = Path(CHUNK_DIR) / agg_filename

    # Skip if already exists
    if agg_path.exists():
        file_size = agg_path.stat().st_size / (1024 * 1024)
        print(f"[Chunk {chunk_id}] ✓ Aggregated file exists ({file_size:.2f} MB), skipping")
        return str(agg_path)

    print(f"\n{'='*70}")
    print(f"[Chunk {chunk_id}] PHASE 2: Aggregating to minute candles...")
    print(f"{'='*70}")

    agg_start_time = time.time()

    # Read raw CSV
    print(f"[Chunk {chunk_id}] Reading raw trades...")
    df = pd.read_csv(raw_path)
    print(f"[Chunk {chunk_id}] Loaded {len(df):,} raw trades")

    # Parse timestamps and convert to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['price'] = df['price'].astype(float)
    df['amount'] = df['amount'].astype(float)

    # Round timestamp to minute
    df['minute'] = df['timestamp'].dt.floor('1min')

    # Group by symbol and minute, aggregate to OHLCV
    print(f"[Chunk {chunk_id}] Aggregating to minute candles...")
    agg_df = df.groupby(['symbol', 'minute']).agg({
        'price': ['first', 'max', 'min', 'last'],  # OHLC
        'amount': 'sum',  # Volume
        'timestamp': 'count'  # Number of trades
    }).reset_index()

    # Flatten column names
    agg_df.columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'trades']

    # Sort by symbol and timestamp
    agg_df = agg_df.sort_values(['symbol', 'timestamp']).reset_index(drop=True)

    print(f"[Chunk {chunk_id}] Created {len(agg_df):,} minute candles")

    # Save aggregated data
    agg_df.to_csv(agg_path, index=False)

    file_size = agg_path.stat().st_size / (1024 * 1024)
    agg_elapsed = time.time() - agg_start_time

    print(f"[Chunk {chunk_id}] ✓ Aggregated to {file_size:.2f} MB in {agg_elapsed:.1f}s")
    print(f"{'='*70}\n")

    return str(agg_path)


async def fetch_raw_parallel(
    api_key: str,
    symbols: List[str],
    date_chunks: List[Tuple[str, str]],
    max_parallel: int
) -> List[str]:
    """
    PHASE 1: Fetch raw trades in parallel (memory-safe).

    Uses a semaphore to limit how many chunks run concurrently.
    Each chunk writes raw trades directly to disk with minimal memory usage.

    Args:
        api_key: Tardis API key
        symbols: List of trading pairs
        date_chunks: List of (from_date, to_date) tuples
        max_parallel: Maximum number of chunks to fetch simultaneously

    Returns:
        List of raw chunk file paths
    """
    print(f"\n{'='*70}")
    print(f"PHASE 1: PARALLEL RAW FETCH")
    print(f"{'='*70}")
    print(f"Fetching {len(date_chunks)} chunks with max {max_parallel} parallel")
    print(f"Memory usage: ~{max_parallel * 10} MB (write buffers only)")
    print(f"{'='*70}\n")

    # Check existing raw chunks
    existing_chunks = []
    for i, (from_date, to_date) in enumerate(date_chunks, 1):
        if chunk_exists(i, from_date, to_date, suffix="raw"):
            existing_chunks.append(i)

    if existing_chunks:
        print(f"Found {len(existing_chunks)} existing raw chunks: {existing_chunks}")
        print(f"Will skip these and fetch remaining {len(date_chunks) - len(existing_chunks)} chunks\n")

    # Semaphore to limit concurrent chunks
    semaphore = asyncio.Semaphore(max_parallel)

    async def fetch_with_semaphore(chunk_id: int, from_date: str, to_date: str) -> str:
        """Fetch a chunk with semaphore control."""
        async with semaphore:
            return await fetch_chunk_raw(api_key, symbols, from_date, to_date, chunk_id)

    # Create all tasks
    tasks = [
        fetch_with_semaphore(i+1, from_date, to_date)
        for i, (from_date, to_date) in enumerate(date_chunks)
    ]

    # Run with limited parallelism
    print(f"Fetching with max {max_parallel} concurrent chunks...\n")
    total_start = time.time()
    chunk_paths = await asyncio.gather(*tasks)
    total_elapsed = time.time() - total_start

    print(f"\n{'='*70}")
    print(f"✓ All {len(date_chunks)} chunks completed in {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
    print(f"{'='*70}\n")

    return chunk_paths


def combine_chunks(chunk_paths: List[str], output_file: str) -> pd.DataFrame:
    """Combine all chunk files into final output."""
    print(f"\n{'='*70}")
    print(f"Combining {len(chunk_paths)} chunks into final output")
    print(f"{'='*70}\n")

    combine_start = time.time()

    # Read chunks
    print("Reading chunk files...")
    dfs = []
    for i, path in enumerate(chunk_paths, 1):
        if Path(path).exists():
            df = pd.read_csv(path)
            dfs.append(df)
            print(f"  [{i}/{len(chunk_paths)}] Read {len(df):,} rows from {Path(path).name}")

    # Combine
    print("\nCombining and sorting...")
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = combined_df.sort_values(['symbol', 'timestamp']).reset_index(drop=True)

    # Save
    print(f"Saving to {output_file}...")
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_csv(output_file, index=False)

    combine_elapsed = time.time() - combine_start
    file_size = Path(output_file).stat().st_size / (1024 * 1024)

    print(f"\n✓ Combined file: {file_size:.2f} MB")
    print(f"✓ Total rows: {len(combined_df):,}")
    print(f"✓ Combine time: {combine_elapsed:.1f}s")

    return combined_df


async def main():
    """Main execution with 3-phase approach."""
    script_start = time.time()

    # Load API key
    api_key = os.getenv('TARDIS_API_KEY')
    if not api_key:
        raise ValueError("TARDIS_API_KEY not found in environment variables")

    print(f"\n{'='*70}")
    print("MEMORY-SAFE PARALLEL BINANCE DATA FETCHER")
    print(f"{'='*70}")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print(f"Symbols: {', '.join(SYMBOLS)}")
    print(f"Chunks: {NUM_CHUNKS}")
    print(f"Max parallel: {MAX_PARALLEL}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Chunk dir: {CHUNK_DIR}")
    print(f"\n3-PHASE APPROACH:")
    print(f"  Phase 1: Fetch raw trades (parallel, ~{MAX_PARALLEL * 10}MB RAM)")
    print(f"  Phase 2: Aggregate to minutes (sequential, ~1-2GB RAM)")
    print(f"  Phase 3: Merge aggregated data (~100MB RAM)")
    print(f"{'='*70}\n")

    # Calculate days
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")
    total_days = (end_dt - start_dt).days + 1
    print(f"Total days: {total_days}\n")

    # Split into chunks
    date_chunks = split_date_range(START_DATE, END_DATE, NUM_CHUNKS)

    print("Chunks:")
    for i, (from_date, to_date) in enumerate(date_chunks, 1):
        chunk_days = (datetime.strptime(to_date, "%Y-%m-%d") -
                     datetime.strptime(from_date, "%Y-%m-%d")).days + 1
        raw_exists = "✓ RAW" if chunk_exists(i, from_date, to_date, "raw") else ""
        agg_exists = "✓ AGG" if chunk_exists(i, from_date, to_date, "agg") else ""
        status = f"{raw_exists} {agg_exists}".strip() or ""
        print(f"  Chunk {i}: {from_date} to {to_date} ({chunk_days} days) {status}")
    print()

    # PHASE 1: Fetch raw trades in parallel
    raw_paths = await fetch_raw_parallel(api_key, SYMBOLS, date_chunks, MAX_PARALLEL)

    # PHASE 2: Aggregate raw chunks to minute candles (sequential)
    print(f"\n{'='*70}")
    print(f"PHASE 2: SEQUENTIAL AGGREGATION")
    print(f"{'='*70}")
    print(f"Aggregating {len(date_chunks)} raw chunks to minute candles")
    print(f"Processing one chunk at a time (memory-safe)")
    print(f"{'='*70}\n")

    agg_paths = []
    for i, (from_date, to_date) in enumerate(date_chunks, 1):
        raw_path = raw_paths[i-1]
        agg_path = aggregate_chunk(i, from_date, to_date, raw_path)
        agg_paths.append(agg_path)

    # PHASE 3: Combine aggregated chunks
    print(f"\n{'='*70}")
    print(f"PHASE 3: MERGE AGGREGATED CHUNKS")
    print(f"{'='*70}\n")
    df = combine_chunks(agg_paths, OUTPUT_FILE)

    # Cleanup
    if not KEEP_RAW:
        print(f"\nCleaning up raw files...")
        for path in raw_paths:
            if Path(path).exists():
                Path(path).unlink()
                print(f"  Deleted: {Path(path).name}")
        print(f"✓ Raw files deleted")

    if not KEEP_CHUNKS:
        print(f"\nCleaning up aggregated chunk files...")
        for path in agg_paths:
            if Path(path).exists():
                Path(path).unlink()
                print(f"  Deleted: {Path(path).name}")
        print(f"✓ Aggregated chunk files deleted")
    else:
        print(f"\n✓ Chunk files kept in: {CHUNK_DIR}")

    # Summary
    print(f"\n{'='*70}")
    print("RESULTS")
    print(f"{'='*70}")
    print(f"✓ Data saved to: {OUTPUT_FILE}")
    print(f"\nData summary:")
    print(f"  Total minute candles: {len(df):,}")
    print(f"  Symbols: {df['symbol'].nunique()}")
    print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

    print(f"\nData per symbol:")
    summary = df.groupby('symbol').agg({
        'timestamp': ['min', 'max', 'count'],
        'volume': 'sum',
        'trades': 'sum'
    })
    print(summary)

    total_elapsed = time.time() - script_start
    print(f"\n{'='*70}")
    print(f"✓ COMPLETED IN {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    asyncio.run(main())
