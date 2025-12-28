#!/usr/bin/env python3
"""
Phase 2: Process Crypto Trades

Streams the 68GB goldsky orderFilled.csv, filters to crypto markets only,
transforms to structured trade format, and writes to Parquet incrementally.

Input:
- data/raw/goldsky/orderFilled.csv (279M events, 68GB)
- data/processed/crypto_markets.csv (from Phase 1)

Output:
- data/processed/crypto_trades.parquet (expected 25-35M trades, 3-5GB)
"""

import warnings
warnings.filterwarnings('ignore')

import sys
import os
from pathlib import Path
import polars as pl
from datetime import datetime


def transform_to_trades(df: pl.LazyFrame, markets_df: pl.DataFrame) -> pl.LazyFrame:
    """
    Transform raw goldsky events to structured trade format.

    Based on transformation logic from pipelines/full/process_live.py
    """
    # Rename markets id column to avoid conflicts
    markets_df = markets_df.rename({'id': 'market_id'})

    # Create markets long format (melt token1/token2 into rows)
    markets_long = (
        markets_df
        .select(["market_id", "token1", "token2"])
        .melt(
            id_vars="market_id",
            value_vars=["token1", "token2"],
            variable_name="side",
            value_name="asset_id"
        )
    )

    # Identify non-USDC asset (USDC = assetId "0")
    df = df.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    # Join with markets to get market_id (inner join = automatic filter!)
    df = df.join(
        markets_long.lazy(),
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="inner"
    )

    # Label assets (USDC vs token side)
    df = df.with_columns([
        pl.when(pl.col("makerAssetId") == "0")
        .then(pl.lit("USDC"))
        .otherwise(pl.col("side"))
        .alias("makerAsset"),

        pl.when(pl.col("takerAssetId") == "0")
        .then(pl.lit("USDC"))
        .otherwise(pl.col("side"))
        .alias("takerAsset"),
    ])

    # Convert amounts from wei to tokens (divide by 10^6)
    df = df.with_columns([
        (pl.col("makerAmountFilled").cast(pl.Int64) / 10**6).alias("makerAmountFilled"),
        (pl.col("takerAmountFilled").cast(pl.Int64) / 10**6).alias("takerAmountFilled"),
    ])

    # Determine trade directions (BUY if paying USDC, SELL if receiving USDC)
    df = df.with_columns([
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("BUY"))
        .otherwise(pl.lit("SELL"))
        .alias("taker_direction"),

        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.lit("SELL"))
        .otherwise(pl.lit("BUY"))
        .alias("maker_direction"),
    ])

    # Calculate price, USD amount, token amount
    df = df.with_columns([
        # USD amount (the USDC side)
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled"))
        .alias("usd_amount"),

        # Token amount (the non-USDC side)
        pl.when(pl.col("takerAsset") != "USDC")
        .then(pl.col("takerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled"))
        .alias("token_amount"),

        # Price = USD / Token
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled") / pl.col("makerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled") / pl.col("takerAmountFilled"))
        .cast(pl.Float64)
        .alias("price")
    ])

    # Select final columns
    df = df.select([
        'timestamp',
        'market_id',
        'maker',
        'taker',
        'maker_direction',
        'taker_direction',
        'price',
        'usd_amount',
        'token_amount',
        'transactionHash'
    ])

    return df


def main():
    print("="*60)
    print("PHASE 2: Process Crypto Trades")
    print("="*60)

    start_time = datetime.now()
    project_root = Path(__file__).parent.parent.parent

    # 1. Load crypto markets from Phase 1
    print("\n📂 Loading crypto markets metadata (from Phase 1)...")
    crypto_markets_path = project_root / "data" / "processed" / "crypto_markets.csv"

    if not crypto_markets_path.exists():
        print(f"❌ Error: {crypto_markets_path} not found")
        print("   Please run Phase 1 first: python pipelines/crypto/prepare_crypto_markets.py")
        return 1

    crypto_markets = pl.read_csv(
        crypto_markets_path,
        schema_overrides={"token1": pl.String, "token2": pl.String}
    )
    print(f"✓ Loaded {len(crypto_markets):,} crypto markets")

    # 2. Stream goldsky orderFilled.csv
    print("\n📥 Streaming goldsky orderFilled.csv (68GB, 279M events)...")
    goldsky_path = project_root / "data" / "raw" / "goldsky" / "orderFilled.csv"

    if not goldsky_path.exists():
        print(f"❌ Error: {goldsky_path} not found")
        return 1

    events = pl.scan_csv(
        goldsky_path,
        schema_overrides={
            "makerAssetId": pl.String,
            "takerAssetId": pl.String
        },
        truncate_ragged_lines=True
    )

    print("✓ CSV scan initialized (lazy evaluation)")

    # 3. Transform to trades
    print("\n⚙️  Transforming to structured trades...")
    print("   - Joining with crypto markets (inner join = automatic filtering)")
    print("   - Converting amounts from wei to tokens")
    print("   - Calculating price, direction, USD amount")
    print("   - This will take 25-35 minutes...")

    trades = transform_to_trades(events, crypto_markets)

    # 4. Write incrementally to Parquet
    output_path = project_root / "data" / "processed" / "crypto_trades.parquet"
    print(f"\n💾 Writing to: {output_path}")
    print("   Using incremental sink (streaming write, no memory spike)")

    trades.sink_parquet(
        output_path,
        compression="zstd",
        compression_level=3
    )

    # 5. Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    elapsed_min = elapsed / 60

    print("\n" + "="*60)
    print(f"✅ Phase 2 Complete in {elapsed_min:.1f} minutes ({elapsed:.0f} seconds)")
    print("="*60)

    # Quick stats
    output_size_gb = output_path.stat().st_size / 1024 / 1024 / 1024
    print(f"\nOutput: {output_path}")
    print(f"Size: {output_size_gb:.2f} GB")
    print(f"\nNext step: Run Phase 3 (validate_crypto_trades.py)")


if __name__ == "__main__":
    main()
