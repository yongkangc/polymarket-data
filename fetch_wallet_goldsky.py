#!/usr/bin/env python3
"""
Fetch trades for a single wallet directly from Goldsky GraphQL API.
Much faster than scanning the 68GB CSV file!
"""

import warnings
warnings.filterwarnings('ignore')

import sys
import os
from pathlib import Path
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from datetime import datetime, timezone
import polars as pl

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from poly_utils.utils import get_markets


GOLDSKY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"


def fetch_wallet_events_for_role(wallet_address: str, role: str, batch_size: int = 1000) -> list:
    """Fetch events where wallet is in specified role (maker or taker)"""
    wallet_lower = wallet_address.lower()

    transport = RequestsHTTPTransport(url=GOLDSKY_URL, verify=True, retries=3)
    client = Client(transport=transport)

    all_events = []
    last_timestamp = 0
    batch_num = 0

    while True:
        batch_num += 1

        query_string = f'''
        query {{
            orderFilledEvents(
                orderBy: timestamp
                first: {batch_size}
                where: {{
                    timestamp_gt: "{last_timestamp}"
                    {role}: "{wallet_lower}"
                }}
            ) {{
                timestamp
                maker
                makerAssetId
                makerAmountFilled
                taker
                takerAssetId
                takerAmountFilled
                transactionHash
            }}
        }}
        '''

        query = gql(query_string)

        try:
            result = client.execute(query)
            events = result['orderFilledEvents']

            if not events or len(events) == 0:
                break

            all_events.extend(events)
            last_timestamp = events[-1]['timestamp']

            readable_time = datetime.fromtimestamp(int(last_timestamp), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            print(f"    Batch {batch_num} ({role}): {len(events)} events (up to {readable_time})")

            if len(events) < batch_size:
                break

        except Exception as e:
            print(f"❌ Error fetching {role} batch {batch_num}: {e}")
            break

    return all_events


def fetch_wallet_events(wallet_address: str, batch_size: int = 1000) -> pl.DataFrame:
    """
    Fetch all orderFilledEvents for a specific wallet from Goldsky API.
    Queries maker and taker separately, then combines.
    """
    print(f"🔍 Fetching events from Goldsky API for wallet: {wallet_address}")
    print(f"   API: {GOLDSKY_URL}")

    # Fetch maker events
    print(f"\n  📥 Fetching maker events...")
    maker_events = fetch_wallet_events_for_role(wallet_address, "maker", batch_size)
    print(f"  ✓ Found {len(maker_events):,} maker events")

    # Fetch taker events
    print(f"\n  📥 Fetching taker events...")
    taker_events = fetch_wallet_events_for_role(wallet_address, "taker", batch_size)
    print(f"  ✓ Found {len(taker_events):,} taker events")

    # Combine and deduplicate
    all_events = maker_events + taker_events

    if not all_events:
        print("⚠️  No events found for this wallet")
        return None

    # Convert to Polars DataFrame
    df = pl.DataFrame(all_events)

    # Deduplicate by transactionHash (wallet could be both maker and taker in same tx)
    df = df.unique(subset=["transactionHash"])

    print(f"\n✓ Total unique events: {len(df):,}")

    # Convert timestamp to datetime
    df = df.with_columns(
        pl.from_epoch(pl.col('timestamp').cast(pl.Int64), time_unit='s').alias('timestamp')
    )

    return df


def transform_to_trades(events_df: pl.DataFrame, markets_df: pl.DataFrame) -> pl.DataFrame:
    """Transform raw goldsky events to structured trade format."""
    markets_df = markets_df.rename({'id': 'market_id'})

    # Make markets long
    markets_long = (
        markets_df
        .select(["market_id", "token1", "token2"])
        .melt(id_vars="market_id", value_vars=["token1", "token2"],
            variable_name="side", value_name="asset_id")
    )

    # Identify non-USDC asset
    df = events_df.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    # Join with markets
    df = df.join(markets_long, left_on="nonusdc_asset_id", right_on="asset_id", how="left")

    # Label columns
    df = df.with_columns([
        pl.when(pl.col("makerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("makerAsset"),
        pl.when(pl.col("takerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("takerAsset"),
        pl.col("market_id"),
    ])

    df = df[['timestamp', 'market_id', 'maker', 'makerAsset', 'makerAmountFilled', 'taker', 'takerAsset', 'takerAmountFilled', 'transactionHash']]

    # Convert amounts
    df = df.with_columns([
        (pl.col("makerAmountFilled").cast(pl.Int64) / 10**6).alias("makerAmountFilled"),
        (pl.col("takerAmountFilled").cast(pl.Int64) / 10**6).alias("takerAmountFilled"),
    ])

    # Determine directions
    df = df.with_columns([
        pl.when(pl.col("takerAsset") == "USDC").then(pl.lit("BUY")).otherwise(pl.lit("SELL")).alias("taker_direction"),
        pl.when(pl.col("takerAsset") == "USDC").then(pl.lit("SELL")).otherwise(pl.lit("BUY")).alias("maker_direction"),
    ])

    # Calculate derived fields
    df = df.with_columns([
        pl.when(pl.col("makerAsset") != "USDC").then(pl.col("makerAsset")).otherwise(pl.col("takerAsset")).alias("nonusdc_side"),
        pl.when(pl.col("takerAsset") == "USDC").then(pl.col("takerAmountFilled")).otherwise(pl.col("makerAmountFilled")).alias("usd_amount"),
        pl.when(pl.col("takerAsset") != "USDC").then(pl.col("takerAmountFilled")).otherwise(pl.col("makerAmountFilled")).alias("token_amount"),
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled") / pl.col("makerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled") / pl.col("takerAmountFilled"))
        .cast(pl.Float64).alias("price")
    ])

    df = df[['timestamp', 'market_id', 'maker', 'taker', 'nonusdc_side', 'maker_direction', 'taker_direction', 'price', 'usd_amount', 'token_amount', 'transactionHash']]
    return df


def print_summary(trades_df: pl.DataFrame, markets_df: pl.DataFrame, wallet_address: str):
    """Print trading statistics"""
    print("\n" + "="*60)
    print(f"WALLET TRADING SUMMARY: {wallet_address}")
    print("="*60)

    print(f"\n📊 Overall Stats:")
    print(f"  Total trades: {len(trades_df):,}")
    print(f"  Date range: {trades_df['timestamp'].min()} to {trades_df['timestamp'].max()}")
    print(f"  Unique markets: {trades_df['market_id'].n_unique()}")
    print(f"  Total volume: ${trades_df['usd_amount'].sum():,.2f}")

    print(f"\n📈 Top 10 Markets:")
    trades_with_ticker = trades_df.join(
        markets_df.select(["id", "ticker"]),
        left_on="market_id", right_on="id", how="left"
    )

    top_markets = (
        trades_with_ticker
        .groupby("ticker")
        .agg([pl.count("timestamp").alias("trades"), pl.sum("usd_amount").alias("volume")])
        .sort("trades", descending=True)
        .head(10)
    )

    for row in top_markets.iter_rows(named=True):
        ticker = row['ticker'] if row['ticker'] else "Unknown"
        print(f"  {ticker:40s} {row['trades']:6,} trades  ${row['volume']:12,.2f}")


def process_wallet_from_api(wallet_address: str, output_format: str = "parquet") -> Path:
    """Fetch and process wallet trades from Goldsky API"""

    print("="*60)
    print(f"Processing wallet: {wallet_address}")
    print("="*60)

    # 1. Fetch from API
    events_df = fetch_wallet_events(wallet_address)

    if events_df is None:
        return None

    # 2. Load markets
    print(f"\n📂 Loading markets metadata...")
    markets = get_markets()
    print(f"✓ Loaded {len(markets):,} markets")

    # 3. Transform
    print(f"\n⚙️  Transforming to structured trades...")
    trades = transform_to_trades(events_df, markets)

    trades_with_market = trades.filter(pl.col("market_id").is_not_null())
    unmatched = len(trades) - len(trades_with_market)
    if unmatched > 0:
        print(f"⚠️  {unmatched:,} events couldn't be matched ({unmatched/len(trades)*100:.1f}%)")

    trades = trades_with_market
    print(f"✓ Processed {len(trades):,} trades")

    # 4. Save
    if not os.path.isdir('processed'):
        os.makedirs('processed')

    output_file = f"processed/wallet_{wallet_address}.{output_format}"
    print(f"\n💾 Saving to {output_file}...")

    if output_format == "parquet":
        trades.write_parquet(output_file)
    else:
        trades.write_csv(output_file)

    print(f"✓ Saved successfully")

    # 5. Summary
    print_summary(trades, markets, wallet_address)

    return Path(output_file)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch wallet trades from Goldsky GraphQL API")
    parser.add_argument("wallet", nargs="?",
                       default="0x5248313731287b61d714ab9df655442d6ed28aa2",
                       help="Wallet address")
    parser.add_argument("--format", choices=["csv", "parquet"], default="parquet",
                       help="Output format")

    args = parser.parse_args()

    output_path = process_wallet_from_api(args.wallet, output_format=args.format)

    if output_path:
        print(f"\n✅ Done! Output: {output_path}")
