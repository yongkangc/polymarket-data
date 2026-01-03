"""
Fill in missing data ranges identified from existing dataset.
This script fetches only the missing minute ranges to complete the dataset.
"""
import asyncio
import csv
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from tardis_client import TardisClient, Channel

# ============================================================================
# CONFIGURATION
# ============================================================================

# API Configuration
API_KEY = ""  # Will be set from environment

# Data Configuration
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]  # lowercase for Binance API

# Fetch Configuration
MAX_PARALLEL = 2  # Conservative to avoid timeouts
CHUNK_DIR = 'data/fill_chunks'

# Missing ranges (from analysis)
MISSING_RANGES = [
    ("2025-09-01T21:54:00", "2025-09-05T23:59:00"),
    ("2025-09-06T21:54:00", "2025-09-10T23:59:00"),
    ("2025-09-11T21:25:00", "2025-09-15T23:59:00"),
    ("2025-09-16T21:54:00", "2025-09-20T23:59:00"),
    ("2025-09-21T21:12:00", "2025-09-25T23:59:00"),
    ("2025-09-26T21:08:00", "2025-09-30T23:59:00"),
    ("2025-10-01T21:54:00", "2025-10-05T23:59:00"),
    ("2025-10-06T21:50:00", "2025-10-10T23:59:00"),
    ("2025-10-11T21:20:00", "2025-10-15T23:59:00"),
    ("2025-10-16T21:23:00", "2025-10-20T23:59:00"),
    ("2025-10-21T21:54:00", "2025-10-25T23:59:00"),
    ("2025-10-26T21:23:00", "2025-10-30T23:59:00"),
    ("2025-10-31T21:23:00", "2025-11-04T23:59:00"),
    ("2025-11-05T21:54:00", "2025-11-09T23:59:00"),
    ("2025-11-10T21:54:00", "2025-11-14T23:59:00"),
    ("2025-11-15T21:02:00", "2025-11-19T23:59:00"),
    ("2025-11-20T21:54:00", "2025-11-24T23:59:00"),
    ("2025-11-25T21:54:00", "2025-11-29T23:59:00"),
    ("2025-11-30T21:06:00", "2025-12-04T23:59:00"),
    ("2025-12-05T20:57:00", "2025-12-28T23:59:00"),
]

# ============================================================================
# FETCH RAW TRADES
# ============================================================================

async def fetch_range_raw(
    api_key: str,
    symbols: list,
    from_datetime: str,
    to_datetime: str,
    range_id: int
) -> str:
    """
    Fetch raw trades for a specific missing date range.

    Returns path to raw CSV file.
    """
    Path(CHUNK_DIR).mkdir(parents=True, exist_ok=True)

    # Format datetime for display
    from_dt = datetime.fromisoformat(from_datetime)
    to_dt = datetime.fromisoformat(to_datetime)
    duration = to_dt - from_dt
    duration_hours = duration.total_seconds() / 3600

    # Output file
    raw_path = f"{CHUNK_DIR}/fill_{range_id:02d}_raw.csv"

    print(f"\n[{range_id}/20] Fetching {from_dt.strftime('%Y-%m-%d %H:%M')} to {to_dt.strftime('%Y-%m-%d %H:%M')} ({duration_hours:.1f} hrs)")

    try:
        trade_count = 0

        # Initialize Tardis client
        tardis_client = TardisClient(api_key=api_key)

        # Open CSV and write trades as they stream
        with open(raw_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'symbol', 'price', 'amount'])

            # Fetch trades using replay
            messages = tardis_client.replay(
                exchange="binance",
                from_date=from_datetime,
                to_date=to_datetime,
                filters=[Channel(name="trade", symbols=symbols)]
            )

            async for timestamp, trade_msg in messages:
                trade_data = trade_msg.get('data', {})
                symbol = trade_data.get('s')

                # Extract price and amount
                price_str = trade_data.get('p', '0')
                amount_str = trade_data.get('q', '0')

                # Write row immediately
                writer.writerow([
                    timestamp.isoformat(),
                    symbol,
                    price_str,
                    amount_str
                ])

                trade_count += 1

                # Progress indicator
                if trade_count % 100000 == 0:
                    print(f"  {trade_count:,} trades...", end='\r')

        # Get file size
        file_size = Path(raw_path).stat().st_size / (1024 * 1024)
        print(f"  ✓ Fetched {trade_count:,} trades ({file_size:.1f} MB)")

        return raw_path

    except Exception as e:
        print(f"  ✗ Error: {e}")
        # Create empty file to mark as attempted
        Path(raw_path).touch()
        raise

# ============================================================================
# AGGREGATE TO MINUTE CANDLES
# ============================================================================

def aggregate_range(
    range_id: int,
    from_datetime: str,
    to_datetime: str,
    raw_path: str
) -> str:
    """
    Aggregate raw trades to minute OHLCV candles.

    Returns path to aggregated CSV file.
    """
    agg_path = f"{CHUNK_DIR}/fill_{range_id:02d}_agg.csv"

    # Check if raw file exists and has data
    if not Path(raw_path).exists() or Path(raw_path).stat().st_size < 100:
        print(f"  ⚠ Skipping aggregation (no data)")
        return None

    print(f"  Aggregating to minute candles...")

    # Read raw data
    df = pd.read_csv(raw_path)

    if len(df) == 0:
        print(f"  ⚠ No trades to aggregate")
        return None

    # Parse timestamps
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

    # Save
    agg_df.to_csv(agg_path, index=False)

    print(f"  ✓ Aggregated to {len(agg_df):,} minute candles")

    return agg_path

# ============================================================================
# MAIN PIPELINE
# ============================================================================

async def fetch_all_missing(api_key: str):
    """
    Fetch all missing ranges with controlled parallelism.
    """
    print("=" * 70)
    print("FILLING MISSING DATA RANGES")
    print("=" * 70)
    print(f"\nTotal ranges to fetch: {len(MISSING_RANGES)}")
    print(f"Max parallel: {MAX_PARALLEL}")
    print(f"Symbols: {', '.join(SYMBOLS)}")

    # Create semaphore for rate limiting
    semaphore = asyncio.Semaphore(MAX_PARALLEL)

    async def fetch_with_semaphore(range_id, from_dt, to_dt):
        async with semaphore:
            try:
                raw_path = await fetch_range_raw(
                    api_key=api_key,
                    symbols=SYMBOLS,
                    from_datetime=from_dt,
                    to_datetime=to_dt,
                    range_id=range_id
                )
                # Aggregate immediately after fetching
                agg_path = aggregate_range(range_id, from_dt, to_dt, raw_path)
                return (range_id, raw_path, agg_path)
            except Exception as e:
                print(f"[{range_id}/20] Failed: {e}")
                return (range_id, None, None)

    # Launch all fetch tasks
    tasks = [
        fetch_with_semaphore(i+1, from_dt, to_dt)
        for i, (from_dt, to_dt) in enumerate(MISSING_RANGES)
    ]

    # Wait for all to complete
    results = await asyncio.gather(*tasks)

    # Summary
    successful = sum(1 for _, _, agg in results if agg is not None)
    print(f"\n{'=' * 70}")
    print(f"Fetch complete: {successful}/{len(MISSING_RANGES)} ranges successful")
    print(f"{'=' * 70}")

    return results

def merge_all_data():
    """
    Merge existing data with newly fetched data.
    """
    print("\n" + "=" * 70)
    print("MERGING ALL DATA")
    print("=" * 70)

    # Load existing data
    print("\nLoading existing data...")
    existing_df = pd.read_csv('data/binance_sep_dec28_2025_partial_minute_data.csv')
    existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
    print(f"  Existing: {len(existing_df):,} rows")

    # Load all fill chunks
    print("\nLoading fill chunks...")
    fill_files = sorted(Path(CHUNK_DIR).glob("fill_*_agg.csv"))

    if not fill_files:
        print("  ⚠ No fill chunks found!")
        return

    fill_dfs = []
    for fill_file in fill_files:
        try:
            df = pd.read_csv(fill_file)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            fill_dfs.append(df)
            print(f"  {fill_file.name}: {len(df):,} rows")
        except Exception as e:
            print(f"  ✗ Error reading {fill_file.name}: {e}")

    if not fill_dfs:
        print("  ⚠ No valid fill chunks to merge!")
        return

    # Combine all
    print("\nCombining all data...")
    all_data = [existing_df] + fill_dfs
    final_df = pd.concat(all_data, ignore_index=True)

    # Remove duplicates (in case of overlap)
    print("Removing duplicates...")
    before = len(final_df)
    final_df = final_df.drop_duplicates(subset=['symbol', 'timestamp'])
    after = len(final_df)
    if before > after:
        print(f"  Removed {before - after:,} duplicate rows")

    # Sort
    print("Sorting...")
    final_df = final_df.sort_values(['symbol', 'timestamp']).reset_index(drop=True)

    # Stats
    print(f"\nFinal dataset:")
    print(f"  Total rows: {len(final_df):,}")
    print(f"  Date range: {final_df['timestamp'].min()} to {final_df['timestamp'].max()}")
    print(f"  Per symbol:")
    print(final_df.groupby('symbol')['timestamp'].agg(['min', 'max', 'count']))

    # Calculate completeness
    target_start = datetime(2025, 9, 1, 0, 0, 0)
    target_end = datetime(2025, 12, 28, 23, 59, 0)
    expected_minutes = int((target_end - target_start).total_seconds() / 60) + 1
    actual_minutes = len(final_df[final_df['symbol'] == 'BTCUSDT'])
    completeness = actual_minutes / expected_minutes * 100

    print(f"\nCompleteness: {completeness:.2f}% ({actual_minutes:,}/{expected_minutes:,} minutes)")

    # Save
    output_path = 'data/binance_sep_dec28_2025_complete_minute_data.csv'
    final_df.to_csv(output_path, index=False)

    file_size = Path(output_path).stat().st_size / (1024 * 1024)
    print(f"\n✓ Saved to: {output_path}")
    print(f"✓ File size: {file_size:.2f} MB")
    print("=" * 70)

async def main():
    """Main entry point."""
    import os

    # Get API key from environment
    api_key = os.environ.get('TARDIS_API_KEY')
    if not api_key:
        print("ERROR: TARDIS_API_KEY environment variable not set")
        print("Please run: export TARDIS_API_KEY=your_key_here")
        sys.exit(1)

    # Phase 1: Fetch all missing ranges
    print("\n" + "=" * 70)
    print("PHASE 1: FETCH MISSING RANGES")
    print("=" * 70)
    results = await fetch_all_missing(api_key)

    # Phase 2: Merge everything
    print("\n" + "=" * 70)
    print("PHASE 2: MERGE ALL DATA")
    print("=" * 70)
    merge_all_data()

    print("\n✓ Complete!")

if __name__ == "__main__":
    asyncio.run(main())
