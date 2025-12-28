"""
Phase 2.1: Join Polymarket Trades with Price Data

This script enriches Polymarket trades with:
- Crypto prices at the time of each trade
- Distance from target price
- Time remaining features
- Market probability features
"""

import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import logging
from analysis.config import (
    DATA_DIR,
    ASSETS,
    EXCHANGE_SYMBOLS
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_price_candles() -> dict:
    """
    Load 1-minute candle data for all assets

    Returns:
        Dict mapping asset -> candle DataFrame
    """
    logger.info("Loading price candle data...")

    candles_dict = {}

    for asset in ASSETS:
        # Try parquet first (faster), fall back to CSV
        parquet_file = DATA_DIR / f"{asset.lower()}_1min_candles.parquet"
        csv_file = DATA_DIR / f"{asset.lower()}_1min_candles.csv"

        if parquet_file.exists():
            logger.info(f"  Loading {asset} from parquet...")
            candles = pl.read_parquet(parquet_file)
        elif csv_file.exists():
            logger.info(f"  Loading {asset} from CSV...")
            candles = pl.read_csv(csv_file)

            # Parse timestamp
            candles = candles.with_columns([
                pl.col('timestamp').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S%.f%z')
            ])
        else:
            logger.warning(f"  No price data found for {asset}")
            continue

        # Add asset column
        candles = candles.with_columns([
            pl.lit(asset).alias('asset')
        ])

        candles_dict[asset] = candles
        logger.info(f"    ✓ Loaded {len(candles):,} candles for {asset}")

    return candles_dict


def join_trades_with_prices(trades: pl.DataFrame, candles_dict: dict) -> pl.DataFrame:
    """
    Join trades with price data

    Args:
        trades: Polymarket trades DataFrame
        candles_dict: Dict of asset -> candles DataFrame

    Returns:
        Enriched trades DataFrame
    """
    logger.info("Joining trades with price data...")

    # Parse trade datetime
    if trades.schema['trade_datetime'] == pl.Utf8:
        logger.info("  Parsing trade_datetime...")
        trades = trades.with_columns([
            pl.col('trade_datetime').str.strptime(
                pl.Datetime(time_unit='us', time_zone='UTC'),
                '%Y-%m-%dT%H:%M:%S%.f%z'
            )
        ])

    # Round trade timestamp to nearest minute for joining
    trades = trades.with_columns([
        pl.col('trade_datetime').dt.truncate('1m').alias('trade_minute')
    ])

    # Concatenate all candle data
    all_candles = pl.concat([
        candles.select(['timestamp', 'asset', 'open', 'high', 'low', 'close', 'volume', 'num_trades'])
        for candles in candles_dict.values()
    ])

    logger.info(f"  Total candles: {len(all_candles):,}")
    logger.info(f"  Total trades: {len(trades):,}")

    # Join trades with candles
    enriched = trades.join(
        all_candles,
        left_on=['asset', 'trade_minute'],
        right_on=['asset', 'timestamp'],
        how='left'
    )

    # Check for missing prices
    missing_prices = enriched.filter(pl.col('close').is_null())
    if len(missing_prices) > 0:
        logger.warning(f"  ⚠ {len(missing_prices):,} trades ({len(missing_prices)/len(trades)*100:.1f}%) missing exact price match")
        logger.info("  Attempting to fill with nearest prices...")

        # Try ±1 minute for missing prices
        enriched = fill_missing_prices(enriched, all_candles)

    # Rename columns for clarity
    enriched = enriched.rename({
        'open': 'price_open',
        'high': 'price_high',
        'low': 'price_low',
        'close': 'crypto_price',
        'volume': 'volume_1min',
        'num_trades': 'num_crypto_trades_1min'
    })

    logger.info(f"✓ Joined {len(enriched):,} trades with prices")

    return enriched


def fill_missing_prices(trades: pl.DataFrame, candles: pl.DataFrame) -> pl.DataFrame:
    """
    Fill missing prices using nearest available candle (±1 minute)

    Args:
        trades: Trades DataFrame with some missing prices
        candles: All candle data

    Returns:
        Trades with filled prices
    """
    # Get rows with missing prices
    missing = trades.filter(pl.col('close').is_null())

    if len(missing) == 0:
        return trades

    logger.info(f"  Filling {len(missing):,} missing prices...")

    # Try -1 minute
    missing_minus1 = missing.with_columns([
        (pl.col('trade_minute') - timedelta(minutes=1)).alias('trade_minute_adj')
    ])

    filled_minus1 = missing_minus1.join(
        candles.select(['timestamp', 'asset', 'close']),
        left_on=['asset', 'trade_minute_adj'],
        right_on=['asset', 'timestamp'],
        how='left',
        suffix='_minus1'
    )

    # Try +1 minute for still-missing
    still_missing = filled_minus1.filter(pl.col('close_minus1').is_null())

    if len(still_missing) > 0:
        missing_plus1 = still_missing.with_columns([
            (pl.col('trade_minute') + timedelta(minutes=1)).alias('trade_minute_adj')
        ])

        filled_plus1 = missing_plus1.join(
            candles.select(['timestamp', 'asset', 'close']),
            left_on=['asset', 'trade_minute_adj'],
            right_on=['asset', 'timestamp'],
            how='left',
            suffix='_plus1'
        )

        # Use whichever is available
        filled = filled_plus1.with_columns([
            pl.coalesce(['close_minus1', 'close_plus1', 'close']).alias('close')
        ])
    else:
        filled = filled_minus1.with_columns([
            pl.coalesce(['close_minus1', 'close']).alias('close')
        ])

    # Combine filled with non-missing
    non_missing = trades.filter(pl.col('close').is_not_null())
    result = pl.concat([non_missing, filled.drop(['trade_minute_adj', 'close_minus1'])])

    # Check final missing count
    final_missing = result.filter(pl.col('close').is_null())
    logger.info(f"    After filling: {len(final_missing):,} trades still missing prices ({len(final_missing)/len(trades)*100:.2f}%)")

    return result


def calculate_distance_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate distance from target price

    Args:
        df: DataFrame with crypto_price and target_price columns

    Returns:
        DataFrame with distance features
    """
    logger.info("Calculating distance features...")

    df = df.with_columns([
        # Absolute distance
        ((pl.col('crypto_price') - pl.col('target_price')) / pl.col('target_price')).alias('distance_from_target'),

        # Direction (above/below)
        pl.when(pl.col('crypto_price') >= pl.col('target_price'))
          .then(pl.lit('above'))
          .otherwise(pl.lit('below'))
          .alias('direction'),

        # Absolute distance percentage
        (((pl.col('crypto_price') - pl.col('target_price')) / pl.col('target_price')).abs()).alias('distance_pct')
    ])

    logger.info("✓ Distance features calculated")

    return df


def calculate_time_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate time remaining features
    Note: time_remaining_sec already exists in the trades data

    Args:
        df: DataFrame with time_remaining_sec column

    Returns:
        DataFrame with additional time features
    """
    logger.info("Calculating time features...")

    df = df.with_columns([
        # Time remaining in different units
        (pl.col('time_remaining_sec') / 60).alias('time_remaining_minutes'),
        (pl.col('time_remaining_sec') / 3600).alias('time_remaining_hours'),
        (pl.col('time_remaining_sec') / 86400).alias('time_remaining_days'),
    ])

    logger.info("✓ Time features calculated")

    return df


def calculate_market_probability(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate market probability from trade price

    In Polymarket:
    - token1 = YES side
    - token2 = NO side
    - price = probability for that side

    Args:
        df: DataFrame with nonusdc_side and price columns

    Returns:
        DataFrame with market_probability column
    """
    logger.info("Calculating market probability...")

    # Market probability for YES outcome
    df = df.with_columns([
        pl.when(pl.col('nonusdc_side') == 'token1')
          .then(pl.col('price'))  # YES side: price IS the probability
          .otherwise(1 - pl.col('price'))  # NO side: probability is (1 - price)
          .alias('market_probability_yes')
    ])

    logger.info("✓ Market probability calculated")

    return df


def validate_enriched_data(df: pl.DataFrame) -> bool:
    """
    Validate the enriched dataset

    Args:
        df: Enriched DataFrame

    Returns:
        True if validation passes
    """
    logger.info("Validating enriched data...")

    # Check for critical nulls
    critical_cols = ['crypto_price', 'distance_from_target', 'time_remaining_minutes', 'market_probability_yes']

    for col in critical_cols:
        null_count = df[col].null_count()
        if null_count > 0:
            pct = null_count / len(df) * 100
            logger.warning(f"  {col}: {null_count:,} nulls ({pct:.2f}%)")

            if pct > 5:
                logger.error(f"  ✗ Too many nulls in {col}!")
                return False

    # Check value ranges
    if (df['market_probability_yes'] < 0).any() or (df['market_probability_yes'] > 1).any():
        logger.error("  ✗ market_probability_yes out of range [0, 1]!")
        return False

    if (df['time_remaining_minutes'] < 0).any():
        logger.error("  ✗ Negative time remaining found!")
        return False

    logger.info("✓ Validation passed")

    return True


def main():
    """
    Main execution: enrich trades with price and feature data
    """
    logger.info("=== Phase 2.1: Enrich Trades with Price Data ===")

    # Load trades
    trades_file = DATA_DIR / "polymarket_crypto_trades.csv"

    if not trades_file.exists():
        logger.error(f"Trades file not found: {trades_file}")
        logger.error("Please run extract_polymarket_data.py first")
        return

    logger.info(f"Loading trades from {trades_file}...")
    trades = pl.read_csv(trades_file)
    logger.info(f"✓ Loaded {len(trades):,} trades")

    # Load price candles
    candles_dict = load_price_candles()

    if not candles_dict:
        logger.error("No price candle data found!")
        logger.error("Please run price_aggregator.py first")
        return

    # Join trades with prices
    enriched = join_trades_with_prices(trades, candles_dict)

    # Calculate features
    enriched = calculate_distance_features(enriched)
    enriched = calculate_time_features(enriched)
    enriched = calculate_market_probability(enriched)

    # Validate
    if not validate_enriched_data(enriched):
        logger.error("Validation failed!")
        return

    # Save enriched trades
    output_file = DATA_DIR / "enriched_trades.csv"
    logger.info(f"Saving enriched trades to {output_file}...")

    enriched.write_csv(output_file)

    # Also save as parquet
    output_parquet = DATA_DIR / "enriched_trades.parquet"
    enriched.write_parquet(output_parquet)

    logger.info(f"✓ Saved {len(enriched):,} enriched trades")
    logger.info(f"  CSV: {output_file}")
    logger.info(f"  Parquet: {output_parquet}")

    # Print summary statistics
    logger.info("\n=== Summary Statistics ===")
    logger.info(f"Total trades: {len(enriched):,}")
    logger.info(f"Date range: {enriched['trade_datetime'].min()} to {enriched['trade_datetime'].max()}")
    logger.info(f"\nBy asset:")
    for asset in ASSETS:
        asset_trades = enriched.filter(pl.col('asset') == asset)
        logger.info(f"  {asset}: {len(asset_trades):,} trades")

    logger.info(f"\nAverage distance from target: {enriched['distance_pct'].mean():.2%}")
    logger.info(f"Average time remaining: {enriched['time_remaining_hours'].mean():.1f} hours")
    logger.info(f"Average market probability (YES): {enriched['market_probability_yes'].mean():.2f}")

    logger.info("\n=== Phase 2.1 Complete ===")
    logger.info("Next step: Run bucketing.py for Phase 2.2")


if __name__ == "__main__":
    main()
