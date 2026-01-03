"""
Fetch ranges 13-20 with 10 chunks, retry logic, and exponential backoff.

Features:
- MAX_PARALLEL=5 for reliability
- 5 retries with exponential backoff (2^n seconds)
- Streams trades directly to CSV without buffering
"""
import asyncio
import csv
import sys
import time
from pathlib import Path
from datetime import datetime
from tardis_client import TardisClient, Channel

# Configuration
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
MAX_PARALLEL = 5
MAX_RETRIES = 5
CHUNK_DIR = 'data/fill_chunks'

# 10 chunks: Ranges 13-19 (7 chunks) + Range 20 split into 3 sub-chunks
CHUNKS_TO_FETCH = [
    (13, "2025-10-31T21:23:00", "2025-11-04T23:59:00"),
    (14, "2025-11-05T21:54:00", "2025-11-09T23:59:00"),
    (15, "2025-11-10T21:54:00", "2025-11-14T23:59:00"),
    (16, "2025-11-15T21:02:00", "2025-11-19T23:59:00"),
    (17, "2025-11-20T21:54:00", "2025-11-24T23:59:00"),
    (18, "2025-11-25T21:54:00", "2025-11-29T23:59:00"),
    (19, "2025-11-30T21:06:00", "2025-12-04T23:59:00"),
    ("20a", "2025-12-05T20:57:00", "2025-12-12T23:59:00"),
    ("20b", "2025-12-13T00:00:00", "2025-12-20T23:59:00"),
    ("20c", "2025-12-21T00:00:00", "2025-12-28T23:59:00"),
]

async def fetch_chunk_with_retry(api_key: str, symbols: list, from_datetime: str,
                                 to_datetime: str, chunk_id: str) -> tuple:
    """Fetch with exponential backoff retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            raw_path = await fetch_chunk_raw(api_key, symbols, from_datetime,
                                             to_datetime, chunk_id, attempt)
            return (chunk_id, raw_path, True, attempt)
        except Exception as e:
            if attempt < MAX_RETRIES:
                backoff = 2 ** attempt  # 2, 4, 8, 16, 32 seconds
                print(f"[{chunk_id}] Attempt {attempt} failed: {e}")
                print(f"[{chunk_id}] Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
            else:
                print(f"[{chunk_id}] ✗ All {MAX_RETRIES} attempts failed: {e}")
                return (chunk_id, None, False, attempt)

async def fetch_chunk_raw(api_key: str, symbols: list, from_datetime: str,
                          to_datetime: str, chunk_id: str, attempt: int = 1) -> str:
    """Fetch raw trades for a specific chunk, streaming directly to CSV."""
    Path(CHUNK_DIR).mkdir(parents=True, exist_ok=True)

    from_dt = datetime.fromisoformat(from_datetime)
    to_dt = datetime.fromisoformat(to_datetime)
    duration_hours = (to_dt - from_dt).total_seconds() / 3600

    raw_path = f"{CHUNK_DIR}/fill_{chunk_id}_raw.csv"

    # Skip if exists and looks complete (> 100 MB as sanity check)
    if Path(raw_path).exists() and Path(raw_path).stat().st_size > 100_000_000:
        file_size = Path(raw_path).stat().st_size / (1024 * 1024)
        print(f"[{chunk_id}] ✓ Already exists ({file_size:.0f} MB), skipping")
        return raw_path

    attempt_suffix = f" (attempt {attempt}/{MAX_RETRIES})" if attempt > 1 else ""
    print(f"[{chunk_id}] Fetching {from_dt.strftime('%b %d')} to {to_dt.strftime('%b %d')} ({duration_hours:.0f} hrs){attempt_suffix}")

    tardis_client = TardisClient(api_key=api_key)
    trade_count = 0
    temp_path = f"{raw_path}.tmp"

    try:
        with open(temp_path, 'w', newline='') as f:
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

        # Only rename if we got a reasonable amount of data (> 100k trades minimum)
        if trade_count < 100000:
            raise Exception(f"Only {trade_count:,} trades downloaded (expected millions). Likely incomplete.")

        # Rename temp to final
        Path(temp_path).rename(raw_path)

        file_size = Path(raw_path).stat().st_size / (1024 * 1024)
        print(f"[{chunk_id}] ✓ {trade_count:,} trades ({file_size:.0f} MB)                    ")
        return raw_path

    except Exception as e:
        # Clean up temp file
        if Path(temp_path).exists():
            Path(temp_path).unlink()
        raise e

async def main():
    import os

    api_key = os.environ.get('TARDIS_API_KEY')
    if not api_key:
        print("ERROR: TARDIS_API_KEY not set")
        sys.exit(1)

    print("=" * 70)
    print("FETCHING RANGES 13-20 WITH RETRY LOGIC")
    print("=" * 70)
    print(f"Max parallel: {MAX_PARALLEL}")
    print(f"Max retries: {MAX_RETRIES} (with exponential backoff)")
    print(f"Total chunks: {len(CHUNKS_TO_FETCH)}")
    print()

    semaphore = asyncio.Semaphore(MAX_PARALLEL)

    async def fetch_with_semaphore(chunk_id, from_dt, to_dt):
        async with semaphore:
            return await fetch_chunk_with_retry(api_key, SYMBOLS, from_dt, to_dt, str(chunk_id))

    tasks = [fetch_with_semaphore(cid, from_dt, to_dt)
             for cid, from_dt, to_dt in CHUNKS_TO_FETCH]
    results = await asyncio.gather(*tasks)

    successful = sum(1 for _, _, success, _ in results if success)
    print(f"\n{'=' * 70}")
    print(f"FETCH COMPLETE: {successful}/{len(CHUNKS_TO_FETCH)} chunks successful")
    print(f"{'=' * 70}")

    # Show retry statistics
    for chunk_id, _, success, attempts in results:
        if attempts > 1:
            status = "✓" if success else "✗"
            print(f"{status} Chunk {chunk_id}: {attempts} attempts")

    if successful < len(CHUNKS_TO_FETCH):
        print("\nFailed chunks:")
        for chunk_id, _, success, _ in results:
            if not success:
                print(f"  - Chunk {chunk_id}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
