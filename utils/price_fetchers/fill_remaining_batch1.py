"""
Fill ranges 13-19 (the 7 remaining small ranges) with MAX_PARALLEL=4
"""
import asyncio
import csv
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from tardis_client import TardisClient, Channel

# Configuration
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
MAX_PARALLEL = 4
CHUNK_DIR = 'data/fill_chunks'

# Ranges 13-19 only
RANGES_TO_FETCH = [
    (13, "2025-10-31T21:23:00", "2025-11-04T23:59:00"),
    (14, "2025-11-05T21:54:00", "2025-11-09T23:59:00"),
    (15, "2025-11-10T21:54:00", "2025-11-14T23:59:00"),
    (16, "2025-11-15T21:02:00", "2025-11-19T23:59:00"),
    (17, "2025-11-20T21:54:00", "2025-11-24T23:59:00"),
    (18, "2025-11-25T21:54:00", "2025-11-29T23:59:00"),
    (19, "2025-11-30T21:06:00", "2025-12-04T23:59:00"),
]

async def fetch_range_raw(api_key: str, symbols: list, from_datetime: str, to_datetime: str, range_id: int) -> str:
    """Fetch raw trades for a specific date range."""
    Path(CHUNK_DIR).mkdir(parents=True, exist_ok=True)

    from_dt = datetime.fromisoformat(from_datetime)
    to_dt = datetime.fromisoformat(to_datetime)
    duration_hours = (to_dt - from_dt).total_seconds() / 3600

    raw_path = f"{CHUNK_DIR}/fill_{range_id:02d}_raw.csv"

    # Skip if exists
    if Path(raw_path).exists() and Path(raw_path).stat().st_size > 1000:
        file_size = Path(raw_path).stat().st_size / (1024 * 1024)
        print(f"[{range_id}] ✓ Already exists ({file_size:.0f} MB), skipping")
        return raw_path

    print(f"[{range_id}] Fetching {from_dt.strftime('%b %d')} to {to_dt.strftime('%b %d')} ({duration_hours:.0f} hrs)")

    try:
        tardis_client = TardisClient(api_key=api_key)
        trade_count = 0

        with open(raw_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'symbol', 'price', 'amount'])

            messages = tardis_client.replay(
                exchange="binance",
                from_date=from_datetime,
                to_date=to_datetime,
                filters=[Channel(name="trade", symbols=symbols)]
            )

            async for timestamp, trade_msg in messages:
                trade_data = trade_msg.get('data', {})
                symbol = trade_data.get('s')
                price_str = trade_data.get('p', '0')
                amount_str = trade_data.get('q', '0')

                writer.writerow([timestamp.isoformat(), symbol, price_str, amount_str])
                trade_count += 1

                if trade_count % 500000 == 0:
                    print(f"[{range_id}] {trade_count:,} trades...", end='\r')

        file_size = Path(raw_path).stat().st_size / (1024 * 1024)
        print(f"[{range_id}] ✓ {trade_count:,} trades ({file_size:.0f} MB)                    ")
        return raw_path

    except Exception as e:
        print(f"[{range_id}] ✗ Error: {e}")
        Path(raw_path).touch()
        raise

def aggregate_range(range_id: int, raw_path: str) -> str:
    """Aggregate raw trades to minute candles."""
    agg_path = f"{CHUNK_DIR}/fill_{range_id:02d}_agg.csv"

    if not Path(raw_path).exists() or Path(raw_path).stat().st_size < 1000:
        return None

    print(f"[{range_id}] Aggregating...")

    df = pd.read_csv(raw_path)
    if len(df) == 0:
        return None

    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
    df['price'] = df['price'].astype(float)
    df['amount'] = df['amount'].astype(float)
    df['minute'] = df['timestamp'].dt.floor('1min')

    agg_df = df.groupby(['symbol', 'minute']).agg({
        'price': ['first', 'max', 'min', 'last'],
        'amount': 'sum',
        'timestamp': 'count'
    }).reset_index()

    agg_df.columns = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'trades']
    agg_df.to_csv(agg_path, index=False)

    print(f"[{range_id}] ✓ {len(agg_df):,} candles")
    return agg_path

async def main():
    import os

    api_key = os.environ.get('TARDIS_API_KEY')
    if not api_key:
        print("ERROR: TARDIS_API_KEY not set")
        sys.exit(1)

    print("=" * 70)
    print("FILLING RANGES 13-19 (7 ranges)")
    print("=" * 70)
    print(f"Max parallel: {MAX_PARALLEL}")

    semaphore = asyncio.Semaphore(MAX_PARALLEL)

    async def fetch_with_semaphore(range_id, from_dt, to_dt):
        async with semaphore:
            try:
                raw_path = await fetch_range_raw(api_key, SYMBOLS, from_dt, to_dt, range_id)
                agg_path = aggregate_range(range_id, raw_path)
                return (range_id, raw_path, agg_path)
            except Exception as e:
                return (range_id, None, None)

    tasks = [fetch_with_semaphore(rid, from_dt, to_dt) for rid, from_dt, to_dt in RANGES_TO_FETCH]
    results = await asyncio.gather(*tasks)

    successful = sum(1 for _, _, agg in results if agg is not None)
    print(f"\n{'=' * 70}")
    print(f"Batch 1 complete: {successful}/{len(RANGES_TO_FETCH)} ranges successful")
    print(f"{'=' * 70}")

if __name__ == "__main__":
    asyncio.run(main())
