"""
Fetch XRP minute-by-minute price data from Tardis for September-December 2024
Appends to existing complete_minute_data.csv
"""
import asyncio
import os
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
import pandas as pd

try:
    from tardis_client import TardisClient, Channel
    TARDIS_AVAILABLE = True
except ImportError:
    TARDIS_AVAILABLE = False
    print("Warning: tardis_client not installed. Install with: pip install tardis-client")


def aggregate_trades_to_minutes(trades_data):
    """Aggregate trade data into minute-by-minute OHLCV candles"""
    if not trades_data:
        return pd.DataFrame()

    candles = defaultdict(lambda: defaultdict(list))

    for timestamp, trade_msg in trades_data:
        trade_data = trade_msg.get('data', {})
        symbol = trade_data.get('s')
        price_str = trade_data.get('p', '0')
        amount_str = trade_data.get('q', '0')

        if not symbol:
            continue

        price = float(price_str)
        amount = float(amount_str)
        minute = timestamp.replace(second=0, microsecond=0)

        candles[symbol][minute].append({
            'price': price,
            'amount': amount
        })

    rows = []
    for symbol, minute_data in candles.items():
        for minute, trades in sorted(minute_data.items()):
            prices = [t['price'] for t in trades]
            amounts = [t['amount'] for t in trades]

            rows.append({
                'symbol': symbol.upper(),  # Ensure uppercase for consistency
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


async def fetch_xrp_day(api_key: str, date_str: str) -> pd.DataFrame:
    """Fetch XRP data for a single day"""
    tardis_client = TardisClient(api_key=api_key)
    
    from_date = date_str
    to_date_obj = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
    to_date = to_date_obj.strftime("%Y-%m-%d")

    trades_data = []
    messages = tardis_client.replay(
        exchange="binance",
        from_date=from_date,
        to_date=to_date,
        filters=[Channel(name="trade", symbols=['xrpusdt'])]
    )

    count = 0
    async for local_timestamp, message in messages:
        trades_data.append((local_timestamp, message))
        count += 1
        if count % 50000 == 0:
            print(f"    {count:,} trades...")

    return aggregate_trades_to_minutes(trades_data)


async def fetch_xrp_range(api_key: str, start_date: str, end_date: str, output_file: str):
    """Fetch XRP data for a date range"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    all_data = []
    current = start
    
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        print(f"Fetching XRP data for {date_str}...")
        
        try:
            df = await fetch_xrp_day(api_key, date_str)
            if not df.empty:
                all_data.append(df)
                print(f"  ✓ Got {len(df)} minutes of data")
            else:
                print(f"  ⚠ No data for {date_str}")
        except Exception as e:
            print(f"  ✗ Error: {e}")
        
        current += timedelta(days=1)
    
    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        result = result.sort_values(['symbol', 'timestamp']).reset_index(drop=True)
        
        # Save to file
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        result.to_csv(output_file, index=False)
        print(f"\n✅ Saved {len(result)} rows to {output_file}")
        return result
    
    return pd.DataFrame()


def append_to_complete_data(xrp_file: str, complete_file: str):
    """Append XRP data to the complete minute data file"""
    xrp_df = pd.read_csv(xrp_file)
    complete_df = pd.read_csv(complete_file)
    
    # Remove any existing XRP data
    complete_df = complete_df[complete_df['symbol'] != 'XRPUSDT']
    
    # Append XRP data
    combined = pd.concat([complete_df, xrp_df], ignore_index=True)
    combined = combined.sort_values(['symbol', 'timestamp']).reset_index(drop=True)
    
    # Save back
    combined.to_csv(complete_file, index=False)
    print(f"✅ Appended XRP data to {complete_file}")
    print(f"   Total rows: {len(combined):,}")


async def main():
    api_key = os.getenv('TARDIS_API_KEY')
    if not api_key:
        print("=" * 60)
        print("TARDIS_API_KEY not found in environment variables!")
        print("\nTo fetch XRP data, either:")
        print("  1. Set TARDIS_API_KEY environment variable")
        print("  2. Get a free API key at https://tardis.dev/")
        print("=" * 60)
        return
    
    if not TARDIS_AVAILABLE:
        print("tardis_client not installed. Run: pip install tardis-client")
        return
    
    # Fetch XRP data for Sep 1 - Dec 26, 2025
    output_file = 'data/raw/binance/xrp_minute_data.csv'
    
    await fetch_xrp_range(
        api_key=api_key,
        start_date="2025-09-01",
        end_date="2025-12-26",
        output_file=output_file
    )
    
    # Append to complete data
    complete_file = 'data/raw/binance/complete_minute_data.csv'
    if os.path.exists(complete_file) and os.path.exists(output_file):
        append_to_complete_data(output_file, complete_file)


if __name__ == "__main__":
    asyncio.run(main())

