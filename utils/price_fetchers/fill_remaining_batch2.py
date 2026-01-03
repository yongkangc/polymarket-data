"""
Fill range 20 (the large 23-day range) with MAX_PARALLEL=1 (dedicated)
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
CHUNK_DIR = 'data/fill_chunks'

# Range 20 only (the big one - 23 days)
RANGE_ID = 20
FROM_DATETIME = "2025-12-05T20:57:00"
TO_DATETIME = "2025-12-28T23:59:00"

async def fetch_range_raw(api_key: str, symbols: list, from_datetime: str, to_datetime: str, range_id: int) -> str:
    """Fetch raw trades for range 20."""
    Path(CHUNK_DIR).mkdir(parents=True, exist_ok=True)

    from_dt = datetime.fromisoformat(from_datetime)
    to_dt = datetime.fromisoformat(to_datetime)
    duration_hours = (to_dt - from_dt).total_seconds() / 3600

    raw_path = f"{CHUNK_DIR}/fill_{range_id:02d}_raw.csv"

    # Skip if exists
    if Path(raw_path).exists() and Path(raw_path).stat().st_size > 1000:
        file_size = Path(raw_path).stat().st_size / (1024 * 1024)
        print(f"✓ Range {range_id} already exists ({file_size:.0f} MB), skipping")
        return raw_path

    print("=" * 70)
    print(f"FETCHING RANGE 20 (THE BIG ONE)")
    print("=" * 70)
    print(f"Date range: {from_dt.strftime('%b %d')} to {to_dt.strftime('%b %d')}")
    print(f"Duration: {duration_hours:.0f} hours ({duration_hours/24:.1f} days)")
    print(f"Expected size: ~10-15 GB")
    print(f"Estimated time: 35-45 minutes")
    print("=" * 70)

    try:
        tardis_client = TardisClient(api_key=api_key)
        trade_count = 0
        last_report = 0

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

                # Progress every 1M trades
                if trade_count - last_report >= 1000000:
                    file_size = Path(raw_path).stat().st_size / (1024 * 1024)
                    print(f"Progress: {trade_count:,} trades ({file_size:.0f} MB)")
                    last_report = trade_count

        file_size = Path(raw_path).stat().st_size / (1024 * 1024)
        print(f"\n✓ Complete: {trade_count:,} trades ({file_size:.0f} MB)")
        return raw_path

    except Exception as e:
        print(f"✗ Error: {e}")
        raise

def aggregate_range(range_id: int, raw_path: str) -> str:
    """Aggregate raw trades to minute candles."""
    agg_path = f"{CHUNK_DIR}/fill_{range_id:02d}_agg.csv"

    if not Path(raw_path).exists() or Path(raw_path).stat().st_size < 1000:
        return None

    print(f"\nAggregating range {range_id}...")

    df = pd.read_csv(raw_path)
    if len(df) == 0:
        return None

    print(f"  Loaded {len(df):,} trades")

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

    print(f"  ✓ Aggregated to {len(agg_df):,} minute candles")
    return agg_path

async def main():
    import os

    api_key = os.environ.get('TARDIS_API_KEY')
    if not api_key:
        print("ERROR: TARDIS_API_KEY not set")
        sys.exit(1)

    try:
        raw_path = await fetch_range_raw(api_key, SYMBOLS, FROM_DATETIME, TO_DATETIME, RANGE_ID)
        agg_path = aggregate_range(RANGE_ID, raw_path)

        if agg_path:
            print("\n" + "=" * 70)
            print("✓ RANGE 20 COMPLETE")
            print("=" * 70)
        else:
            print("\n✗ Aggregation failed")
            sys.exit(1)

    except Exception as e:
        print(f"\n✗ Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
