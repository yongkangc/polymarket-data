"""
Phase 2.2: Apply Bucketing Strategy

This script assigns each trade to buckets based on:
- Distance from target (6 buckets)
- Time remaining (5 buckets)
- Market probability (3 buckets)

Total: 6 × 5 × 3 = 90 possible combinations
"""

import polars as pl
from pathlib import Path
import logging
import hashlib
from analysis.config import (
    DATA_DIR,
    DISTANCE_BUCKETS,
    TIME_BUCKETS,
    PROBABILITY_BUCKETS
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def assign_to_bucket(value: float, bucket_dict: dict) -> str:
    """
    Assign a value to its bucket

    Args:
        value: Numeric value to bucket
        bucket_dict: Dict of bucket_name -> (min, max) tuples

    Returns:
        Bucket name or "unknown"
    """
    for name, (min_val, max_val) in bucket_dict.items():
        if min_val <= value < max_val:
            return name

    return "unknown"


def assign_distance_bucket(df: pl.DataFrame) -> pl.DataFrame:
    """
    Assign distance bucket to each trade

    Args:
        df: DataFrame with distance_from_target column

    Returns:
        DataFrame with distance_bucket column
    """
    logger.info("Assigning distance buckets...")

    # Create a mapping expression
    bucket_expr = pl.lit("unknown")

    for bucket_name, (min_val, max_val) in DISTANCE_BUCKETS.items():
        bucket_expr = pl.when(
            (pl.col('distance_from_target') >= min_val) &
            (pl.col('distance_from_target') < max_val)
        ).then(pl.lit(bucket_name)).otherwise(bucket_expr)

    df = df.with_columns([
        bucket_expr.alias('distance_bucket')
    ])

    # Check for unknowns
    unknown_count = df.filter(pl.col('distance_bucket') == 'unknown').shape[0]
    if unknown_count > 0:
        logger.warning(f"  ⚠ {unknown_count} trades assigned to 'unknown' distance bucket")

    logger.info("✓ Distance buckets assigned")

    return df


def assign_time_bucket(df: pl.DataFrame) -> pl.DataFrame:
    """
    Assign time bucket to each trade

    Args:
        df: DataFrame with time_remaining_sec column

    Returns:
        DataFrame with time_bucket column
    """
    logger.info("Assigning time buckets...")

    # Create a mapping expression
    bucket_expr = pl.lit("unknown")

    for bucket_name, (min_val, max_val) in TIME_BUCKETS.items():
        bucket_expr = pl.when(
            (pl.col('time_remaining_sec') >= min_val) &
            (pl.col('time_remaining_sec') < max_val)
        ).then(pl.lit(bucket_name)).otherwise(bucket_expr)

    df = df.with_columns([
        bucket_expr.alias('time_bucket')
    ])

    # Check for unknowns
    unknown_count = df.filter(pl.col('time_bucket') == 'unknown').shape[0]
    if unknown_count > 0:
        logger.warning(f"  ⚠ {unknown_count} trades assigned to 'unknown' time bucket")

    logger.info("✓ Time buckets assigned")

    return df


def assign_probability_bucket(df: pl.DataFrame) -> pl.DataFrame:
    """
    Assign probability bucket to each trade

    Args:
        df: DataFrame with market_probability_yes column

    Returns:
        DataFrame with probability_bucket column
    """
    logger.info("Assigning probability buckets...")

    # Create a mapping expression
    bucket_expr = pl.lit("unknown")

    for bucket_name, (min_val, max_val) in PROBABILITY_BUCKETS.items():
        bucket_expr = pl.when(
            (pl.col('market_probability_yes') >= min_val) &
            (pl.col('market_probability_yes') < max_val)
        ).then(pl.lit(bucket_name)).otherwise(bucket_expr)

    df = df.with_columns([
        bucket_expr.alias('probability_bucket')
    ])

    # Check for unknowns
    unknown_count = df.filter(pl.col('probability_bucket') == 'unknown').shape[0]
    if unknown_count > 0:
        logger.warning(f"  ⚠ {unknown_count} trades assigned to 'unknown' probability bucket")

    logger.info("✓ Probability buckets assigned")

    return df


def create_combined_bucket_id(df: pl.DataFrame) -> pl.DataFrame:
    """
    Create combined bucket ID from distance, time, and probability buckets

    Args:
        df: DataFrame with bucket columns

    Returns:
        DataFrame with bucket_id column
    """
    logger.info("Creating combined bucket IDs...")

    df = df.with_columns([
        # Combined bucket ID: "distance|time|probability"
        (
            pl.col('distance_bucket') + pl.lit('|') +
            pl.col('time_bucket') + pl.lit('|') +
            pl.col('probability_bucket')
        ).alias('bucket_id')
    ])

    logger.info("✓ Combined bucket IDs created")

    return df


def analyze_bucket_distribution(df: pl.DataFrame) -> None:
    """
    Analyze and report bucket distribution

    Args:
        df: DataFrame with bucket columns
    """
    logger.info("\n=== Bucket Distribution Analysis ===")

    # Distance buckets
    logger.info("\nDistance Buckets:")
    dist_dist = df.group_by('distance_bucket').agg([
        pl.count().alias('count'),
        pl.col('market_id').n_unique().alias('unique_markets')
    ]).sort('count', descending=True)

    for row in dist_dist.iter_rows(named=True):
        pct = row['count'] / len(df) * 100
        logger.info(f"  {row['distance_bucket']:15s}: {row['count']:8,} trades ({pct:5.1f}%), {row['unique_markets']:4} markets")

    # Time buckets
    logger.info("\nTime Buckets:")
    time_dist = df.group_by('time_bucket').agg([
        pl.count().alias('count'),
        pl.col('market_id').n_unique().alias('unique_markets')
    ]).sort('count', descending=True)

    for row in time_dist.iter_rows(named=True):
        pct = row['count'] / len(df) * 100
        logger.info(f"  {row['time_bucket']:15s}: {row['count']:8,} trades ({pct:5.1f}%), {row['unique_markets']:4} markets")

    # Probability buckets
    logger.info("\nProbability Buckets:")
    prob_dist = df.group_by('probability_bucket').agg([
        pl.count().alias('count'),
        pl.col('market_id').n_unique().alias('unique_markets')
    ]).sort('count', descending=True)

    for row in prob_dist.iter_rows(named=True):
        pct = row['count'] / len(df) * 100
        logger.info(f"  {row['probability_bucket']:15s}: {row['count']:8,} trades ({pct:5.1f}%), {row['unique_markets']:4} markets")

    # Combined bucket IDs
    logger.info("\nCombined Bucket Distribution:")
    combined_dist = df.group_by('bucket_id').agg([
        pl.count().alias('count'),
        pl.col('market_id').n_unique().alias('unique_markets')
    ]).sort('count', descending=True)

    logger.info(f"  Total unique bucket combinations: {len(combined_dist)}")
    logger.info(f"  Maximum possible combinations: {len(DISTANCE_BUCKETS) * len(TIME_BUCKETS) * len(PROBABILITY_BUCKETS)}")

    # Show top 20 most common buckets
    logger.info("\n  Top 20 most common bucket combinations:")
    for i, row in enumerate(combined_dist.head(20).iter_rows(named=True), 1):
        pct = row['count'] / len(df) * 100
        logger.info(f"    {i:2}. {row['bucket_id']:50s}: {row['count']:7,} trades ({pct:4.1f}%), {row['unique_markets']:4} markets")

    # Show buckets with very few samples
    sparse_buckets = combined_dist.filter(pl.col('count') < 10)
    logger.info(f"\n  Buckets with <10 samples: {len(sparse_buckets)} ({len(sparse_buckets)/len(combined_dist)*100:.1f}% of populated buckets)")

    # Empty buckets
    empty_buckets = (len(DISTANCE_BUCKETS) * len(TIME_BUCKETS) * len(PROBABILITY_BUCKETS)) - len(combined_dist)
    logger.info(f"  Empty buckets (0 trades): {empty_buckets}")


def validate_bucketing(df: pl.DataFrame) -> bool:
    """
    Validate bucketing assignments

    Args:
        df: DataFrame with bucket columns

    Returns:
        True if validation passes
    """
    logger.info("Validating bucketing...")

    # Check for unknown buckets
    unknown_distance = df.filter(pl.col('distance_bucket') == 'unknown').shape[0]
    unknown_time = df.filter(pl.col('time_bucket') == 'unknown').shape[0]
    unknown_prob = df.filter(pl.col('probability_bucket') == 'unknown').shape[0]

    total_unknown = unknown_distance + unknown_time + unknown_prob

    if total_unknown > 0:
        logger.warning(f"  ⚠ Found {total_unknown} 'unknown' bucket assignments")
        logger.warning(f"    Distance: {unknown_distance}, Time: {unknown_time}, Probability: {unknown_prob}")

        if total_unknown > len(df) * 0.01:  # More than 1%
            logger.error("  ✗ Too many unknown buckets!")
            return False

    # Check that all trades have bucket_id
    null_bucket_ids = df.filter(pl.col('bucket_id').is_null()).shape[0]
    if null_bucket_ids > 0:
        logger.error(f"  ✗ {null_bucket_ids} trades missing bucket_id!")
        return False

    logger.info("✓ Validation passed")

    return True


def main():
    """
    Main execution: apply bucketing strategy to enriched trades
    """
    logger.info("=== Phase 2.2: Apply Bucketing Strategy ===")

    # Load enriched trades
    enriched_file = DATA_DIR / "enriched_trades.parquet"

    if not enriched_file.exists():
        # Try CSV
        enriched_file = DATA_DIR / "enriched_trades.csv"

    if not enriched_file.exists():
        logger.error(f"Enriched trades file not found")
        logger.error("Please run trade_enricher.py first")
        return

    logger.info(f"Loading enriched trades from {enriched_file}...")

    if enriched_file.suffix == '.parquet':
        trades = pl.read_parquet(enriched_file)
    else:
        trades = pl.read_csv(enriched_file)

    logger.info(f"✓ Loaded {len(trades):,} trades")

    # Assign buckets
    bucketed = assign_distance_bucket(trades)
    bucketed = assign_time_bucket(bucketed)
    bucketed = assign_probability_bucket(bucketed)

    # Create combined bucket ID
    bucketed = create_combined_bucket_id(bucketed)

    # Validate
    if not validate_bucketing(bucketed):
        logger.error("Validation failed!")
        return

    # Analyze distribution
    analyze_bucket_distribution(bucketed)

    # Save bucketed trades
    output_file = DATA_DIR / "bucketed_trades.csv"
    logger.info(f"\nSaving bucketed trades to {output_file}...")

    bucketed.write_csv(output_file)

    # Also save as parquet
    output_parquet = DATA_DIR / "bucketed_trades.parquet"
    bucketed.write_parquet(output_parquet)

    logger.info(f"✓ Saved {len(bucketed):,} bucketed trades")
    logger.info(f"  CSV: {output_file}")
    logger.info(f"  Parquet: {output_parquet}")

    logger.info("\n=== Phase 2.2 Complete ===")
    logger.info("Next step: Run pattern_analyzer.py for Phase 3.1")


if __name__ == "__main__":
    main()
