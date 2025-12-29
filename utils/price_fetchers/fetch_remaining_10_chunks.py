"""
Fetch ranges 13-20 with 10 parallel chunks for maximum CPU utilization.

Strategy:
- Ranges 13-19: 7 chunks (Oct 31 - Dec 4, ~4-5 days each)
- Range 20: Split into 3 sub-chunks (Dec 5-28, ~7-8 days each)
- Total: 10 chunks running in parallel with MAX_PARALLEL=10

Memory-safe: Streams trades directly to CSV without buffering.
"""
import asyncio
import csv
import sys
from pathlib import Path
from datetime import datetime
from tardis_client import TardisClient, Channel

# Configuration
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
MAX_PARALLEL = 5  # Reduced from 10 to avoid API timeouts
CHUNK_DIR = 'data/fill_chunks'

# 10 chunks: Ranges 13-19 (7 chunks) + Range 20 split into 3 sub-chunks
CHUNKS_TO_FETCH = [
    # Ranges 13-19 (as-is)
    (13, "2025-10-31T21:23:00", "2025-11-04T23:59:00"),
    (14, "2025-11-05T21:54:00", "2025-11-09T23:59:00"),
    (15, "2025-11-10T21:54:00", "2025-11-14T23:59:00"),
    (16, "2025-11-15T21:02:00", "2025-11-19T23:59:00"),
    (17, "2025-11-20T21:54:00", "2025-11-24T23:59:00"),
    (18, "2025-11-25T21:54:00", "2025-11-29T23:59:00"),
    (19, "2025-11-30T21:06:00", "2025-12-04T23:59:00"),
    # Range 20 split into 3 sub-chunks
    ("20a", "2025-12-05T20:57:00", "2025-12-12T23:59:00"),
    ("20b", "2025-12-13T00:00:00", "2025-12-20T23:59:00"),
    ("20c", "2025-12-21T00:00:00", "2025-12-28T23:59:00"),
]

async def fetch_chunk_raw(api_key: str, symbols: list, from_datetime: str,
                          to_datetime: str, chunk_id: str) -> str:
    """Fetch raw trades for a specific chunk, streaming directly to CSV."""
    Path(CHUNK_DIR).mkdir(parents=True, exist_ok=True)

    from_dt = datetime.fromisoformat(from_datetime)
    to_dt = datetime.fromisoformat(to_datetime)
    duration_hours = (to_dt - from_dt).total_seconds() / 3600

    raw_path = f"{CHUNK_DIR}/fill_{chunk_id}_raw.csv"

    # Skip if exists
    if Path(raw_path).exists() and Path(raw_path).stat().st_size > 1000:
        file_size = Path(raw_path).stat().st_size / (1024 * 1024)
        print(f"[{chunk_id}] ✓ Already exists ({file_size:.0f} MB), skipping")
        return raw_path

    print(f"[{chunk_id}] Fetching {from_dt.strftime('%b %d')} to {to_dt.strftime('%b %d')} ({duration_hours:.0f} hrs)")

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
                    print(f"[{chunk_id}] {trade_count:,} trades...", end='\r')

        file_size = Path(raw_path).stat().st_size / (1024 * 1024)
        print(f"[{chunk_id}] ✓ {trade_count:,} trades ({file_size:.0f} MB)                    ")
        return raw_path

    except Exception as e:
        print(f"[{chunk_id}] ✗ Error: {e}")
        # Create empty file to mark as attempted
        Path(raw_path).touch()
        raise

async def main():
    import os

    api_key = os.environ.get('TARDIS_API_KEY')
    if not api_key:
        print("ERROR: TARDIS_API_KEY not set")
        sys.exit(1)

    print("=" * 70)
    print("FETCHING RANGES 13-20 IN 10 PARALLEL CHUNKS")
    print("=" * 70)
    print(f"Max parallel: {MAX_PARALLEL}")
    print(f"Total chunks: {len(CHUNKS_TO_FETCH)}")
    print()

    semaphore = asyncio.Semaphore(MAX_PARALLEL)

    async def fetch_with_semaphore(chunk_id, from_dt, to_dt):
        async with semaphore:
            try:
                raw_path = await fetch_chunk_raw(api_key, SYMBOLS, from_dt, to_dt, str(chunk_id))
                return (chunk_id, raw_path, True)
            except Exception as e:
                return (chunk_id, None, False)

    tasks = [fetch_with_semaphore(cid, from_dt, to_dt)
             for cid, from_dt, to_dt in CHUNKS_TO_FETCH]
    results = await asyncio.gather(*tasks)

    successful = sum(1 for _, _, success in results if success)
    print(f"\n{'=' * 70}")
    print(f"FETCH COMPLETE: {successful}/{len(CHUNKS_TO_FETCH)} chunks successful")
    print(f"{'=' * 70}")

    if successful < len(CHUNKS_TO_FETCH):
        print("\nFailed chunks:")
        for chunk_id, _, success in results:
            if not success:
                print(f"  - Chunk {chunk_id}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
