"""
Fetch Binance minute-by-minute price data for December 25, 2024 using Tardis
"""
import asyncio
import os
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
from tardis_client import TardisClient, Channel


def aggregate_trades_to_minutes(trades_data):
    """
    Aggregate trade data into minute-by-minute OHLCV candles

    Args:
        trades_data: List of (timestamp, trade) tuples

    Returns:
        DataFrame with minute-by-minute OHLCV data
    """
    if not trades_data:
        return pd.DataFrame()

    # Group trades by symbol and minute
    candles = defaultdict(lambda: defaultdict(list))

    for timestamp, trade_msg in trades_data:
        # Extract trade data from nested structure
        trade_data = trade_msg.get('data', {})
        symbol = trade_data.get('s')  # Symbol field
        price_str = trade_data.get('p', '0')  # Price field
        amount_str = trade_data.get('q', '0')  # Quantity field

        if not symbol:
            continue

        price = float(price_str)
        amount = float(amount_str)

        # Round timestamp to minute (timestamp is already a datetime object)
        minute = timestamp.replace(second=0, microsecond=0)

        candles[symbol][minute].append({
            'price': price,
            'amount': amount
        })

    # Build OHLCV candles
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


async def fetch_binance_data(api_key, symbols, date_str="2024-12-25"):
    """
    Fetch Binance trade data for specified symbols and date

    Args:
        api_key: Tardis API key
        symbols: List of trading pair symbols (e.g., ['BTCUSDT', 'ETHUSDT'])
        date_str: Date in YYYY-MM-DD format

    Returns:
        DataFrame with minute-by-minute OHLCV data
    """
    print(f"Fetching Binance data for {date_str}...")
    print(f"Symbols: {', '.join(symbols)}")

    # Initialize Tardis client
    tardis_client = TardisClient(api_key=api_key)

    # Set date range (full day UTC)
    from_date = date_str
    to_date_obj = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
    to_date = to_date_obj.strftime("%Y-%m-%d")

    print(f"Date range: {from_date} to {to_date}")

    # Fetch trade data
    trades_data = []

    messages = tardis_client.replay(
        exchange="binance",
        from_date=from_date,
        to_date=to_date,
        filters=[
            Channel(name="trade", symbols=symbols)
        ]
    )

    print("Streaming trade data...")
    count = 0
    async for local_timestamp, message in messages:
        trades_data.append((local_timestamp, message))
        count += 1
        if count % 10000 == 0:
            print(f"  Processed {count:,} trades...")

    print(f"Total trades received: {count:,}")

    # Aggregate to minute candles
    print("Aggregating trades into minute candles...")
    df = aggregate_trades_to_minutes(trades_data)

    return df


async def main():
    # Load API key from environment
    api_key = os.getenv('TARDIS_API_KEY')
    if not api_key:
        raise ValueError("TARDIS_API_KEY not found in environment variables")

    # Define symbols to fetch (Binance spot pairs - lowercase!)
    symbols = [
        'btcusdt',
        'ethusdt',
        'solusdt'
    ]

    # Fetch data for December 25, 2025
    df = await fetch_binance_data(
        api_key=api_key,
        symbols=symbols,
        date_str="2025-12-25"
    )

    # Save to CSV
    output_file = 'data/binance_dec25_minute_data.csv'
    os.makedirs('data', exist_ok=True)
    df.to_csv(output_file, index=False)

    print(f"\n✅ Data saved to: {output_file}")
    print(f"\nData summary:")
    print(f"  Total minutes: {len(df)}")

    if len(df) > 0:
        print(f"  Symbols: {df['symbol'].nunique()}")
        print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"\nFirst few rows:")
        print(df.head(10))
        print(f"\nData per symbol:")
        print(df.groupby('symbol').agg({
            'timestamp': ['min', 'max', 'count'],
            'volume': 'sum',
            'trades': 'sum'
        }))
    else:
        print("  ⚠️ No data received. The date might be outside the available range.")


if __name__ == "__main__":
    asyncio.run(main())
