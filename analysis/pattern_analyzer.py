"""
Phase 3: Pattern Analysis & Win Rate Calculation

This script:
1. Calculates historical win rates by pattern bucket
2. Identifies high-edge patterns
3. Calculates expected value and Kelly criterion
"""

import polars as pl
import numpy as np
from pathlib import Path
import logging
from typing import Dict, List
from analysis.config import (
    DATA_DIR,
    RESULTS_DIR,
    MIN_SAMPLE_SIZE,
    MIN_EDGE
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def calculate_win_rates(trades: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate win rates by bucket

    Args:
        trades: Bucketed trades DataFrame

    Returns:
        DataFrame with win rates by bucket
    """
    logger.info("Calculating win rates by bucket...")

    # Group by asset and bucket_id
    win_rates = trades.group_by(['asset', 'bucket_id', 'distance_bucket', 'time_bucket', 'probability_bucket']).agg([
        # Sample statistics
        pl.count().alias('sample_size'),
        pl.col('market_id').n_unique().alias('unique_markets'),
        pl.col('usd_amount').sum().alias('total_volume_usd'),

        # Win rate calculation - YES side
        # Trades where nonusdc_side='token1' and outcome='YES'
        (
            pl.col('outcome').filter(pl.col('nonusdc_side') == 'token1').eq('YES').sum() /
            pl.col('nonusdc_side').eq('token1').sum()
        ).alias('yes_win_rate'),

        # Win rate calculation - NO side
        # Trades where nonusdc_side='token2' and outcome='NO'
        (
            pl.col('outcome').filter(pl.col('nonusdc_side') == 'token2').eq('NO').sum() /
            pl.col('nonusdc_side').eq('token2').sum()
        ).alias('no_win_rate'),

        # Overall win rate (betting with market direction)
        # If market_probability_yes > 0.5, count as "bet YES", compare to outcome
        (
            (
                (pl.col('market_probability_yes') > 0.5).and_(pl.col('outcome') == 'YES').sum() +
                (pl.col('market_probability_yes') <= 0.5).and_(pl.col('outcome') == 'NO').sum()
            ) / pl.count()
        ).alias('overall_win_rate'),

        # Market statistics
        pl.col('market_probability_yes').mean().alias('avg_market_prob'),
        pl.col('market_probability_yes').median().alias('median_market_prob'),
        pl.col('market_probability_yes').std().alias('std_market_prob'),

        # Trade characteristics
        pl.col('usd_amount').mean().alias('avg_trade_size_usd'),
        pl.col('distance_pct').mean().alias('avg_distance_pct'),
        pl.col('time_remaining_minutes').mean().alias('avg_time_remaining_min'),

        # Direction statistics
        pl.col('direction').mode().first().alias('most_common_direction'),
    ])

    # Calculate edge (historical win rate - market probability)
    win_rates = win_rates.with_columns([
        (pl.col('yes_win_rate') - pl.col('avg_market_prob')).alias('edge'),
        (pl.col('yes_win_rate') - pl.col('avg_market_prob')).abs().alias('abs_edge'),
        ((pl.col('yes_win_rate') - pl.col('avg_market_prob')) / pl.col('avg_market_prob')).alias('edge_pct'),
    ])

    # Filter for statistical significance
    win_rates = win_rates.filter(pl.col('sample_size') >= MIN_SAMPLE_SIZE)

    logger.info(f"✓ Calculated win rates for {len(win_rates)} buckets (sample_size >= {MIN_SAMPLE_SIZE})")

    return win_rates


def bootstrap_confidence_intervals(trades: pl.DataFrame, win_rates: pl.DataFrame, n_iterations: int = 1000) -> pl.DataFrame:
    """
    Calculate bootstrap confidence intervals for win rates

    Args:
        trades: Full bucketed trades DataFrame
        win_rates: Aggregated win rates DataFrame
        n_iterations: Number of bootstrap iterations

    Returns:
        win_rates DataFrame with CI columns added
    """
    logger.info(f"Calculating bootstrap confidence intervals ({n_iterations} iterations)...")

    # For each bucket, bootstrap sample and recalculate win rate
    ci_results = []

    for row in win_rates.iter_rows(named=True):
        bucket_id = row['bucket_id']
        asset = row['asset']

        # Get trades for this bucket
        bucket_trades = trades.filter(
            (pl.col('asset') == asset) &
            (pl.col('bucket_id') == bucket_id)
        )

        if len(bucket_trades) < 10:
            # Not enough samples for bootstrap
            ci_results.append({
                'asset': asset,
                'bucket_id': bucket_id,
                'ci_lower': None,
                'ci_upper': None,
                'confidence_level': 'low'
            })
            continue

        # Bootstrap
        win_rates_bootstrap = []

        for _ in range(n_iterations):
            # Resample with replacement
            sample = bucket_trades.sample(n=len(bucket_trades), with_replacement=True, seed=None)

            # Calculate win rate for YES side
            yes_trades = sample.filter(pl.col('nonusdc_side') == 'token1')
            if len(yes_trades) > 0:
                yes_wins = yes_trades.filter(pl.col('outcome') == 'YES').shape[0]
                wr = yes_wins / len(yes_trades)
                win_rates_bootstrap.append(wr)

        if len(win_rates_bootstrap) > 0:
            ci_lower = np.percentile(win_rates_bootstrap, 2.5)
            ci_upper = np.percentile(win_rates_bootstrap, 97.5)

            # Determine confidence level
            ci_width = ci_upper - ci_lower
            if ci_width < 0.10:
                confidence = 'high'
            elif ci_width < 0.20:
                confidence = 'medium'
            else:
                confidence = 'low'

            ci_results.append({
                'asset': asset,
                'bucket_id': bucket_id,
                'ci_lower': ci_lower,
                'ci_upper': ci_upper,
                'confidence_level': confidence
            })
        else:
            ci_results.append({
                'asset': asset,
                'bucket_id': bucket_id,
                'ci_lower': None,
                'ci_upper': None,
                'confidence_level': 'low'
            })

    # Convert to DataFrame
    ci_df = pl.DataFrame(ci_results)

    # Join with win_rates
    win_rates_with_ci = win_rates.join(ci_df, on=['asset', 'bucket_id'], how='left')

    logger.info("✓ Bootstrap confidence intervals calculated")

    return win_rates_with_ci


def identify_high_edge_patterns(win_rates: pl.DataFrame, min_edge: float = MIN_EDGE) -> pl.DataFrame:
    """
    Identify patterns with significant edge

    Args:
        win_rates: Win rates DataFrame
        min_edge: Minimum edge threshold

    Returns:
        Filtered DataFrame with high-edge patterns
    """
    logger.info(f"Identifying high-edge patterns (min_edge={min_edge})...")

    # Filter for significant edge
    high_edge = win_rates.filter(
        (pl.col('abs_edge') > min_edge) &
        (pl.col('confidence_level') != 'low')
    ).sort('abs_edge', descending=True)

    logger.info(f"✓ Found {len(high_edge)} high-edge patterns")

    return high_edge


def calculate_expected_value(win_rates: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate expected value per $1 bet

    Args:
        win_rates: Win rates DataFrame

    Returns:
        DataFrame with EV columns added
    """
    logger.info("Calculating expected value...")

    # Expected value for YES bet
    # If win: gain (1 - market_prob) per $1
    # If lose: lose $1
    # EV = win_rate * (1 - market_prob) - (1 - win_rate) * 1
    win_rates = win_rates.with_columns([
        (
            pl.col('yes_win_rate') * (1 - pl.col('avg_market_prob')) -
            (1 - pl.col('yes_win_rate')) * pl.col('avg_market_prob')
        ).alias('expected_value'),

        # Expected ROI
        (
            (pl.col('yes_win_rate') * (1 - pl.col('avg_market_prob')) -
             (1 - pl.col('yes_win_rate')) * pl.col('avg_market_prob')) /
            pl.col('avg_market_prob')
        ).alias('expected_roi')
    ])

    logger.info("✓ Expected value calculated")

    return win_rates


def calculate_kelly_fraction(win_rates: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate Kelly criterion optimal bet size

    Kelly fraction: f* = (bp - q) / b
    where:
        b = odds (decimal odds - 1)
        p = win probability
        q = loss probability (1 - p)

    Args:
        win_rates: Win rates DataFrame

    Returns:
        DataFrame with Kelly fraction column
    """
    logger.info("Calculating Kelly fractions...")

    # For Polymarket:
    # If you buy YES at price P:
    #   Win: receive $1, profit = $1 - $P
    #   Lose: lose $P
    #   Decimal odds = 1/P
    #   b = (1/P) - 1 = (1 - P)/P

    win_rates = win_rates.with_columns([
        (
            (
                ((1 - pl.col('avg_market_prob')) / pl.col('avg_market_prob')) * pl.col('yes_win_rate') -
                (1 - pl.col('yes_win_rate'))
            ) / ((1 - pl.col('avg_market_prob')) / pl.col('avg_market_prob'))
        ).alias('kelly_fraction')
    ])

    # Cap Kelly fraction at reasonable values
    win_rates = win_rates.with_columns([
        pl.when(pl.col('kelly_fraction') < 0).then(0.0)
         .when(pl.col('kelly_fraction') > 0.25).then(0.25)  # Cap at 25%
         .otherwise(pl.col('kelly_fraction'))
         .alias('kelly_fraction_capped')
    ])

    logger.info("✓ Kelly fractions calculated")

    return win_rates


def categorize_edges(win_rates: pl.DataFrame) -> pl.DataFrame:
    """
    Categorize patterns by edge strength

    Args:
        win_rates: Win rates DataFrame

    Returns:
        DataFrame with edge_category column
    """
    logger.info("Categorizing edge strengths...")

    win_rates = win_rates.with_columns([
        pl.when(pl.col('edge') > 0.10).then(pl.lit('strong_yes'))
         .when(pl.col('edge') > 0.05).then(pl.lit('moderate_yes'))
         .when(pl.col('edge') < -0.10).then(pl.lit('strong_no'))
         .when(pl.col('edge') < -0.05).then(pl.lit('moderate_no'))
         .otherwise(pl.lit('weak'))
         .alias('edge_category')
    ])

    logger.info("✓ Edge categories assigned")

    return win_rates


def generate_summary_report(win_rates: pl.DataFrame, high_edge: pl.DataFrame) -> None:
    """
    Generate summary statistics report

    Args:
        win_rates: Full win rates DataFrame
        high_edge: High-edge patterns DataFrame
    """
    logger.info("\n" + "="*80)
    logger.info("PATTERN ANALYSIS SUMMARY")
    logger.info("="*80)

    logger.info(f"\n📊 Overall Statistics:")
    logger.info(f"  Total patterns analyzed: {len(win_rates)}")
    logger.info(f"  High-edge patterns (|edge| > {MIN_EDGE}): {len(high_edge)}")
    logger.info(f"  Average sample size: {win_rates['sample_size'].mean():.0f}")
    logger.info(f"  Median sample size: {win_rates['sample_size'].median():.0f}")

    logger.info(f"\n💰 Edge Distribution:")
    edge_dist = win_rates.group_by('edge_category').agg([
        pl.count().alias('count')
    ]).sort('count', descending=True)

    for row in edge_dist.iter_rows(named=True):
        logger.info(f"  {row['edge_category']:15s}: {row['count']:4} patterns")

    logger.info(f"\n🎯 Top 10 Highest-Edge Patterns:")
    for i, row in enumerate(high_edge.head(10).iter_rows(named=True), 1):
        logger.info(f"\n  {i}. {row['asset']} | {row['bucket_id']}")
        logger.info(f"     Edge: {row['edge']:+.2%} ({row['edge_category']})")
        logger.info(f"     Win Rate: {row['yes_win_rate']:.1%} vs Market: {row['avg_market_prob']:.1%}")
        logger.info(f"     Sample: {row['sample_size']} trades, {row['unique_markets']} markets")
        logger.info(f"     Expected Value: ${row['expected_value']:.3f} per $1")
        logger.info(f"     Kelly Fraction: {row['kelly_fraction_capped']:.1%}")

    logger.info(f"\n📈 By Asset:")
    for asset in ['BTC', 'ETH', 'SOL']:
        asset_patterns = win_rates.filter(pl.col('asset') == asset)
        asset_high_edge = high_edge.filter(pl.col('asset') == asset)

        if len(asset_patterns) > 0:
            logger.info(f"  {asset}:")
            logger.info(f"    Total patterns: {len(asset_patterns)}")
            logger.info(f"    High-edge patterns: {len(asset_high_edge)}")
            logger.info(f"    Avg edge: {asset_patterns['edge'].mean():+.2%}")
            logger.info(f"    Best edge: {asset_patterns['edge'].max():+.2%}")


def main():
    """
    Main execution: analyze patterns and calculate win rates
    """
    logger.info("=== Phase 3: Pattern Analysis & Win Rate Calculation ===")

    # Load bucketed trades
    bucketed_file = DATA_DIR / "bucketed_trades.parquet"

    if not bucketed_file.exists():
        # Try CSV
        bucketed_file = DATA_DIR / "bucketed_trades.csv"

    if not bucketed_file.exists():
        logger.error(f"Bucketed trades file not found")
        logger.error("Please run bucketing.py first")
        return

    logger.info(f"Loading bucketed trades from {bucketed_file}...")

    if bucketed_file.suffix == '.parquet':
        trades = pl.read_parquet(bucketed_file)
    else:
        trades = pl.read_csv(bucketed_file)

    logger.info(f"✓ Loaded {len(trades):,} trades")

    # Calculate win rates
    win_rates = calculate_win_rates(trades)

    # Bootstrap confidence intervals (skip for now if too slow)
    # win_rates = bootstrap_confidence_intervals(trades, win_rates, n_iterations=100)

    # For now, add simple confidence levels based on sample size
    win_rates = win_rates.with_columns([
        pl.when(pl.col('sample_size') >= 50).then(pl.lit('high'))
         .when(pl.col('sample_size') >= 20).then(pl.lit('medium'))
         .otherwise(pl.lit('low'))
         .alias('confidence_level')
    ])

    # Calculate expected value and Kelly
    win_rates = calculate_expected_value(win_rates)
    win_rates = calculate_kelly_fraction(win_rates)
    win_rates = categorize_edges(win_rates)

    # Save pattern win rates
    output_file = RESULTS_DIR / "pattern_win_rates.csv"
    logger.info(f"\nSaving pattern win rates to {output_file}...")

    win_rates.write_csv(output_file)

    # Also save as parquet
    output_parquet = RESULTS_DIR / "pattern_win_rates.parquet"
    win_rates.write_parquet(output_parquet)

    logger.info(f"✓ Saved win rates for {len(win_rates)} patterns")

    # Identify high-edge patterns
    high_edge = identify_high_edge_patterns(win_rates, min_edge=MIN_EDGE)

    # Save high-edge patterns
    high_edge_file = RESULTS_DIR / "high_edge_patterns.csv"
    high_edge.write_csv(high_edge_file)

    high_edge_parquet = RESULTS_DIR / "high_edge_patterns.parquet"
    high_edge.write_parquet(high_edge_parquet)

    logger.info(f"✓ Saved {len(high_edge)} high-edge patterns to {high_edge_file}")

    # Generate summary report
    generate_summary_report(win_rates, high_edge)

    logger.info("\n=== Phase 3 Complete ===")
    logger.info("Next step: Build backtesting strategies (Phase 4)")


if __name__ == "__main__":
    main()
