"""
Re-fetch chunk 20c only (Dec 21-28)
"""
import asyncio
import csv
import sys
from pathlib import Path
from datetime import datetime
from tardis_client import TardisClient, Channel

SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
CHUNK_DIR = 'data/fill_chunks'
CHUNK_ID = "20c"
FROM_DATETIME = "2025-12-21T00:00:00"
TO_DATETIME = "2025-12-26T10:00:00"  # Latest available data (today)

async def fetch_chunk():
    import os
    api_key = os.environ.get('TARDIS_API_KEY')
    if not api_key:
        print("ERROR: TARDIS_API_KEY not set")
        sys.exit(1)

    Path(CHUNK_DIR).mkdir(parents=True, exist_ok=True)
    raw_path = f"{CHUNK_DIR}/fill_{CHUNK_ID}_raw.csv"

    from_dt = datetime.fromisoformat(FROM_DATETIME)
    to_dt = datetime.fromisoformat(TO_DATETIME)
    duration_hours = (to_dt - from_dt).total_seconds() / 3600

    print(f"Fetching chunk 20c: {from_dt.strftime('%b %d')} to {to_dt.strftime('%b %d')} ({duration_hours:.0f} hrs)")

    tardis_client = TardisClient(api_key=api_key)
    trade_count = 0

    with open(raw_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'symbol', 'price', 'amount'])

        messages = tardis_client.replay(
            exchange="binance",
            from_date=FROM_DATETIME,
            to_date=TO_DATETIME,
            filters=[Channel(name="trade", symbols=SYMBOLS)]
        )

        async for timestamp, trade_msg in messages:
            trade_data = trade_msg.get('data', {})
            symbol = trade_data.get('s')
            price_str = trade_data.get('p', '0')
            amount_str = trade_data.get('q', '0')

            writer.writerow([timestamp.isoformat(), symbol, price_str, amount_str])
            trade_count += 1

            if trade_count % 500000 == 0:
                print(f"[20c] {trade_count:,} trades...", end='\r')

    file_size = Path(raw_path).stat().st_size / (1024 * 1024)
    print(f"\n✓ Chunk 20c complete: {trade_count:,} trades ({file_size:.0f} MB)")

if __name__ == "__main__":
    asyncio.run(fetch_chunk())
