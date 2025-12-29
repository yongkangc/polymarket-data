#!/usr/bin/env python3
"""
Process trades for a single wallet from goldsky blockchain data.

Usage:
    python process_wallet.py [wallet_address] [--format csv|parquet]

Example:
    python process_wallet.py 0x5248313731287b61d714ab9df655442d6ed28aa2 --format parquet
"""

import warnings
warnings.filterwarnings('ignore')

import sys
import os
from pathlib import Path
import polars as pl

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from poly_utils.utils import get_markets


def transform_to_trades(events_df: pl.DataFrame, markets_df: pl.DataFrame) -> pl.DataFrame:
    """
    Transform raw goldsky events to structured trade format.

    Adapted from update_utils/process_live.py get_processed_df()
    """
    markets_df = markets_df.rename({'id': 'market_id'})

    # 1) Make markets long: (market_id, side, asset_id) where side ∈ {"token1", "token2"}
    markets_long = (
        markets_df
        .select(["market_id", "token1", "token2"])
        .melt(id_vars="market_id", value_vars=["token1", "token2"],
            variable_name="side", value_name="asset_id")
    )

    # 2) Identify the non-USDC asset for each trade (the one that isn't 0)
    df = events_df.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    # 3) Join once on that non-USDC asset to recover the market + side ("token1" or "token2")
    df = df.join(
        markets_long,
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="left",
    )

    # 4) Label columns and keep market_id
    df = df.with_columns([
        pl.when(pl.col("makerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("makerAsset"),
        pl.when(pl.col("takerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("takerAsset"),
        pl.col("market_id"),
    ])

    df = df[['timestamp', 'market_id', 'maker', 'makerAsset', 'makerAmountFilled', 'taker', 'takerAsset', 'takerAmountFilled', 'transactionHash']]

    # 5) Convert amounts from wei to tokens (divide by 10^6)
    df = df.with_columns([
        (pl.col("makerAmountFilled") / 10**6).alias("makerAmountFilled"),
        (pl.col("takerAmountFilled") / 10**6).alias("takerAmountFilled"),
    ])

    # 6) Determine trade directions
    df = df.with_columns([
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("BUY"))
        .otherwise(pl.lit("SELL"))
        .alias("taker_direction"),

        # reverse of taker_direction
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("SELL"))
        .otherwise(pl.lit("BUY"))
        .alias("maker_direction"),
    ])

    # 7) Calculate derived fields
    df = df.with_columns([
        pl.when(pl.col("makerAsset") != "USDC")
        .then(pl.col("makerAsset"))
        .otherwise(pl.col("takerAsset"))
        .alias("nonusdc_side"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled"))
        .alias("usd_amount"),

        pl.when(pl.col("takerAsset") != "USDC")
        .then(pl.col("takerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled"))
        .alias("token_amount"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled") / pl.col("makerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled") / pl.col("takerAmountFilled"))
        .cast(pl.Float64)
        .alias("price")
    ])

    # 8) Select final columns
    df = df[['timestamp', 'market_id', 'maker', 'taker', 'nonusdc_side', 'maker_direction', 'taker_direction', 'price', 'usd_amount', 'token_amount', 'transactionHash']]

    return df


def print_wallet_summary(trades_df: pl.DataFrame, markets_df: pl.DataFrame, wallet_address: str):
    """Print trading statistics for the wallet"""
    print("\n" + "="*60)
    print(f"WALLET TRADING SUMMARY: {wallet_address}")
    print("="*60)

    print(f"\n📊 Overall Stats:")
    print(f"  Total trades: {len(trades_df):,}")
    print(f"  Date range: {trades_df['timestamp'].min()} to {trades_df['timestamp'].max()}")
    print(f"  Unique markets: {trades_df['market_id'].n_unique()}")
    total_volume = trades_df['usd_amount'].sum()
    print(f"  Total volume: ${total_volume:,.2f}")
    print(f"  Average trade size: ${trades_df['usd_amount'].mean():,.2f}")

    # Market categories
    print(f"\n📈 Top 10 Markets by Trade Count:")
    trades_with_ticker = (
        trades_df
        .join(markets_df.select(["id", "ticker"]), left_on="market_id", right_on="id", how="left")
    )

    top_markets = (
        trades_with_ticker
        .groupby("ticker")
        .agg([
            pl.count("timestamp").alias("trades"),
            pl.sum("usd_amount").alias("volume")
        ])
        .sort("trades", descending=True)
        .head(10)
    )

    for row in top_markets.iter_rows(named=True):
        ticker = row['ticker'] if row['ticker'] else "Unknown"
        print(f"  {ticker:40s} {row['trades']:6,} trades  ${row['volume']:12,.2f}")

    # Trading direction analysis
    print(f"\n📊 Trading Patterns:")
    wallet_lower = wallet_address.lower()

    # Trades as maker vs taker
    maker_trades = trades_df.filter(pl.col("maker").str.to_lowercase() == wallet_lower)
    taker_trades = trades_df.filter(pl.col("taker").str.to_lowercase() == wallet_lower)

    print(f"  As maker: {len(maker_trades):,} trades (${maker_trades['usd_amount'].sum():,.2f})")
    print(f"  As taker: {len(taker_trades):,} trades (${taker_trades['usd_amount'].sum():,.2f})")

    # Buy vs sell
    buys = trades_df.filter(
        ((pl.col("maker").str.to_lowercase() == wallet_lower) & (pl.col("maker_direction") == "BUY")) |
        ((pl.col("taker").str.to_lowercase() == wallet_lower) & (pl.col("taker_direction") == "BUY"))
    )
    sells = trades_df.filter(
        ((pl.col("maker").str.to_lowercase() == wallet_lower) & (pl.col("maker_direction") == "SELL")) |
        ((pl.col("taker").str.to_lowercase() == wallet_lower) & (pl.col("taker_direction") == "SELL"))
    )

    print(f"  Buy orders: {len(buys):,} trades (${buys['usd_amount'].sum():,.2f})")
    print(f"  Sell orders: {len(sells):,} trades (${sells['usd_amount'].sum():,.2f})")

    print("\n" + "="*60)


def process_wallet_trades(wallet_address: str, output_format: str = "parquet") -> Path:
    """
    Process all trades for a single wallet from goldsky data.

    Args:
        wallet_address: Ethereum address (0x...)
        output_format: "parquet" or "csv"

    Returns:
        Path to output file
    """
    wallet_lower = wallet_address.lower()

    print("="*60)
    print(f"Processing trades for wallet: {wallet_address}")
    print("="*60)

    # 1. Load and filter goldsky data
    print(f"\n🔍 Loading goldsky data for wallet {wallet_address}...")
    goldsky = pl.scan_csv(
        "goldsky/orderFilled.csv",
        schema_overrides={"makerAssetId": pl.String, "takerAssetId": pl.String},
        truncate_ragged_lines=True
    )

    wallet_events = goldsky.filter(
        (pl.col("maker").str.to_lowercase() == wallet_lower) |
        (pl.col("taker").str.to_lowercase() == wallet_lower)
    ).collect()

    print(f"✓ Found {len(wallet_events):,} blockchain events")

    if len(wallet_events) == 0:
        print("⚠️  No events found for this wallet. Exiting.")
        return None

    # 2. Load markets
    print(f"\n📂 Loading markets metadata...")
    markets = get_markets()
    print(f"✓ Loaded {len(markets):,} markets")

    # 3. Transform to trade format
    print(f"\n⚙️  Transforming events to structured trades...")
    trades = transform_to_trades(wallet_events, markets)

    # Remove trades without market_id (couldn't be matched)
    trades_with_market = trades.filter(pl.col("market_id").is_not_null())
    unmatched = len(trades) - len(trades_with_market)
    if unmatched > 0:
        print(f"⚠️  {unmatched:,} events couldn't be matched to markets (dropped)")

    trades = trades_with_market
    print(f"✓ Processed {len(trades):,} trades")

    # 4. Save output
    if not os.path.isdir('processed'):
        os.makedirs('processed')

    output_file = f"processed/wallet_{wallet_address}.{output_format}"
    print(f"\n💾 Saving to {output_file}...")

    if output_format == "parquet":
        trades.write_parquet(output_file)
    else:
        trades.write_csv(output_file)

    print(f"✓ Saved successfully")

    # 5. Print summary statistics
    print_wallet_summary(trades, markets, wallet_address)

    return Path(output_file)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process trades for a single wallet from goldsky data")
    parser.add_argument("wallet", nargs="?",
                       default="0x5248313731287b61d714ab9df655442d6ed28aa2",
                       help="Wallet address (default: 0x5248313731287b61d714ab9df655442d6ed28aa2)")
    parser.add_argument("--format", choices=["csv", "parquet"], default="parquet",
                       help="Output format (default: parquet)")

    args = parser.parse_args()

    output_path = process_wallet_trades(args.wallet, output_format=args.format)

    if output_path:
        print(f"\n✅ Done! Output saved to: {output_path}")
        print(f"\nTo view the data:")
        if args.format == "parquet":
            print(f"  python -c \"import polars as pl; print(pl.read_parquet('{output_path}').head())\"")
        else:
            print(f"  head {output_path}")
