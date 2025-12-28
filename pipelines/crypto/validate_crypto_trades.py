#!/usr/bin/env python3
"""
Phase 3: Validate Crypto Trades

Generates statistics and validation metrics for the processed crypto trades.

Input:
- data/processed/crypto_trades.parquet (from Phase 2)
- data/processed/crypto_markets.csv (from Phase 1)

Output:
- Console report with statistics
"""

import warnings
warnings.filterwarnings('ignore')

import sys
import os
from pathlib import Path
import polars as pl
from datetime import datetime


def print_overall_stats(trades: pl.LazyFrame):
    """Print overall trade statistics"""
    print("\n📊 Overall Statistics:")
    print("-" * 60)

    stats = trades.select([
        pl.count().alias("total_trades"),
        pl.col("market_id").n_unique().alias("unique_markets"),
        pl.col("timestamp").min().alias("earliest_trade"),
        pl.col("timestamp").max().alias("latest_trade"),
        pl.col("usd_amount").sum().alias("total_volume"),
        pl.col("maker").n_unique().alias("unique_makers"),
        pl.col("taker").n_unique().alias("unique_takers"),
    ]).collect()

    for row in stats.iter_rows(named=True):
        print(f"  Total trades:      {row['total_trades']:>15,}")
        print(f"  Unique markets:    {row['unique_markets']:>15,}")
        print(f"  Earliest trade:    {row['earliest_trade']}")
        print(f"  Latest trade:      {row['latest_trade']}")
        print(f"  Total volume:      ${row['total_volume']:>14,.2f}")
        print(f"  Unique makers:     {row['unique_makers']:>15,}")
        print(f"  Unique takers:     {row['unique_takers']:>15,}")


def print_asset_breakdown(trades: pl.LazyFrame, markets: pl.DataFrame):
    """Print volume breakdown by crypto asset"""
    print("\n📈 Volume by Crypto Asset:")
    print("-" * 60)

    # Join with markets to get question text
    markets_with_id = markets.select(["id", "question"]).rename({"id": "market_id"})

    # Aggregate by asset pattern
    asset_patterns = {
        "Bitcoin (BTC)": r'\b(bitcoin|btc)\b',
        "Ethereum (ETH)": r'\b(ethereum|eth)\b',
        "Solana (SOL)": r'\b(solana|sol)\b',
        "XRP": r'\bxrp\b',
        "Cardano (ADA)": r'\b(cardano|ada)\b',
        "Polkadot (DOT)": r'\b(polkadot|dot)\b',
        "Filecoin (FIL)": r'\b(filecoin|fil)\b',
        "Binance (BNB)": r'\b(binance|bnb)\b',
        "Chainlink (LINK)": r'\b(chainlink|link)\b',
    }

    for asset_name, pattern in asset_patterns.items():
        # Filter markets by pattern
        asset_markets = markets_with_id.filter(
            pl.col("question").str.to_lowercase().str.contains(pattern)
        )
        asset_market_ids = set(asset_markets["market_id"].to_list())

        # Calculate volume for this asset
        asset_trades = trades.filter(
            pl.col("market_id").is_in(asset_market_ids)
        ).select([
            pl.count().alias("trades"),
            pl.col("usd_amount").sum().alias("volume"),
            pl.col("market_id").n_unique().alias("markets")
        ]).collect()

        if len(asset_trades) > 0:
            row = asset_trades.row(0, named=True)
            print(f"  {asset_name:20s} {row['trades']:>10,} trades  ${row['volume']:>15,.2f}  ({row['markets']:,} markets)")


def print_top_markets(trades: pl.LazyFrame, markets: pl.DataFrame):
    """Print top 20 markets by trade count"""
    print("\n🔝 Top 20 Markets by Trade Count:")
    print("-" * 60)

    # Get top markets
    top_markets = (
        trades
        .group_by("market_id")
        .agg([
            pl.count().alias("trades"),
            pl.sum("usd_amount").alias("volume")
        ])
        .sort("trades", descending=True)
        .head(20)
        .collect()
    )

    # Join with market metadata
    markets_meta = markets.select(["id", "ticker", "question"]).rename({"id": "market_id"})
    top_markets = top_markets.join(markets_meta, on="market_id", how="left")

    print(f"  {'Ticker':<30s} {'Trades':>10s}  {'Volume':>15s}")
    print("  " + "-" * 58)

    for row in top_markets.iter_rows(named=True):
        ticker = row['ticker'][:30] if row['ticker'] else "Unknown"
        print(f"  {ticker:<30s} {row['trades']:>10,}  ${row['volume']:>14,.2f}")


def print_sample_trades(trades: pl.LazyFrame):
    """Print sample of first 10 trades"""
    print("\n📋 Sample Trades (first 10):")
    print("-" * 60)

    sample = trades.head(10).collect()

    print(sample.select([
        "timestamp",
        "market_id",
        "taker_direction",
        pl.col("price").round(6),
        pl.col("usd_amount").round(2),
        pl.col("token_amount").round(2)
    ]))


def main():
    print("="*60)
    print("PHASE 3: Validate Crypto Trades")
    print("="*60)

    start_time = datetime.now()
    project_root = Path(__file__).parent.parent.parent

    # 1. Load crypto trades
    print("\n📂 Loading crypto trades...")
    trades_path = project_root / "data" / "processed" / "crypto_trades.parquet"

    if not trades_path.exists():
        print(f"❌ Error: {trades_path} not found")
        print("   Please run Phase 2 first: python pipelines/crypto/process_crypto_trades.py")
        return 1

    trades = pl.scan_parquet(trades_path)
    print(f"✓ Loaded crypto_trades.parquet")

    file_size_gb = trades_path.stat().st_size / 1024 / 1024 / 1024
    print(f"  File size: {file_size_gb:.2f} GB")

    # 2. Load markets metadata
    print("\n📂 Loading crypto markets metadata...")
    markets_path = project_root / "data" / "processed" / "crypto_markets.csv"

    if not markets_path.exists():
        print(f"❌ Error: {markets_path} not found")
        return 1

    markets = pl.read_csv(
        markets_path,
        schema_overrides={"token1": pl.String, "token2": pl.String}
    )
    print(f"✓ Loaded {len(markets):,} crypto markets")

    # 3. Generate statistics
    print_overall_stats(trades)
    print_asset_breakdown(trades, markets)
    print_top_markets(trades, markets)
    print_sample_trades(trades)

    # 4. Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    print("\n" + "="*60)
    print(f"✅ Phase 3 Complete in {elapsed:.1f} seconds")
    print("="*60)

    print("\n✨ Pipeline Complete! Crypto trades are ready to use.")
    print(f"   Output: {trades_path}")


if __name__ == "__main__":
    main()
