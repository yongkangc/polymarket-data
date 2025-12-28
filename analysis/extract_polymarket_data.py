"""
Phase 1.1: Extract Polymarket Trade Data

This script:
1. Loads the 3,020 usable crypto markets
2. Filters processed/trades.csv in streaming batches
3. Determines market outcomes using 0.98/0.02 thresholds
4. Joins with market metadata
5. Outputs enriched trade data for analysis

Memory-efficient implementation using Polars streaming and batching.
"""

import polars as pl
from pathlib import Path
from datetime import datetime
import sys
from analysis.config import (
    DATA_DIR,
    RESULTS_DIR,
    BATCH_SIZE,
    STREAMING_MODE,
    OUTCOME_YES_THRESHOLD,
    OUTCOME_NO_THRESHOLD,
    PROJECT_ROOT
)

def load_usable_markets():
    """Load the 3,020 usable crypto markets"""
    print("Loading usable crypto markets...")
    markets_file = RESULTS_DIR / "usable_crypto_markets.csv"

    if not markets_file.exists():
        print(f"ERROR: {markets_file} not found!")
        print("Please run classify_crypto_markets.py first.")
        sys.exit(1)

    markets = pl.read_csv(markets_file)
    print(f"✓ Loaded {len(markets)} usable markets")

    # Get set of market IDs for fast lookup
    market_ids = set(markets['market_id'].to_list())

    return markets, market_ids


def compute_last_prices_from_trades(market_ids):
    """
    Compute the last price for each market from trades.csv

    Logic: Get the price of the last trade (by timestamp) for each market
    """
    print("Computing last prices from trades data...")

    trades_file = PROJECT_ROOT / "processed" / "trades.csv"

    if not trades_file.exists():
        print(f"ERROR: {trades_file} not found!")
        sys.exit(1)

    # Use lazy API to efficiently get last trades
    print("Scanning trades file (streaming mode)...")

    trades_lazy = pl.scan_csv(
        str(trades_file),
        schema_overrides={
            'market_id': pl.Utf8,
            'timestamp': pl.Utf8,  # Datetime strings
            'price': pl.Float64,
            'usd_amount': pl.Float64,
            'token_amount': pl.Float64,
        }
    )

    # Convert market_ids to strings for comparison
    market_id_strs = [str(mid) for mid in market_ids]

    # Filter to our markets and get last trade per market
    last_prices = (
        trades_lazy
        .filter(pl.col('market_id').is_in(market_id_strs))
        .with_columns([
            pl.col('timestamp').str.to_datetime(time_zone='UTC').alias('ts')
        ])
        .group_by('market_id')
        .agg([
            pl.col('price').sort_by('ts').last().alias('last_price'),
            pl.col('ts').max().alias('last_trade_time')
        ])
        .collect(engine='streaming' if STREAMING_MODE else 'cpu')
    )

    print(f"✓ Computed last prices for {len(last_prices)} markets")

    return last_prices


def determine_market_outcomes(markets_df):
    """
    Determine YES/NO outcomes for closed markets using last price threshold

    Logic from Example 1 notebook:
    - last_price > 0.98 → YES
    - last_price < 0.02 → NO
    - 0.02 <= last_price <= 0.98 → UNRESOLVED (exclude from analysis)
    """
    print("Determining market outcomes from last prices...")

    # Get set of market IDs
    market_ids = set(markets_df['market_id'].to_list())

    # Compute last prices from trades
    last_prices = compute_last_prices_from_trades(market_ids)

    # Convert market_id in last_prices to int64 to match markets_df
    last_prices = last_prices.with_columns([
        pl.col('market_id').cast(pl.Int64)
    ])

    # Join with market data
    markets_with_price = markets_df.join(
        last_prices,
        on='market_id',
        how='left'
    )

    # Determine outcomes
    markets_with_outcome = markets_with_price.with_columns([
        pl.when(pl.col('last_price') > OUTCOME_YES_THRESHOLD)
        .then(pl.lit('YES'))
        .when(pl.col('last_price') < OUTCOME_NO_THRESHOLD)
        .then(pl.lit('NO'))
        .otherwise(pl.lit('UNRESOLVED'))
        .alias('outcome')
    ])

    # Report statistics
    outcome_counts = markets_with_outcome.group_by('outcome').agg(pl.len().alias('count'))
    print("\nMarket outcomes:")
    print(outcome_counts)

    # Filter to only resolved markets (YES or NO)
    resolved_markets = markets_with_outcome.filter(
        pl.col('outcome').is_in(['YES', 'NO'])
    )

    print(f"\n✓ {len(resolved_markets)} markets with clear outcomes (YES or NO)")
    print(f"  {len(markets_with_outcome) - len(resolved_markets)} markets unresolved (excluded)")

    return resolved_markets


def extract_trades_streaming(market_ids, resolved_markets):
    """
    Extract trades for usable markets using streaming for memory efficiency

    Input: processed/trades.csv (32GB, ~10M trades)
    Output: Filtered trades for our 3,020 markets
    """
    trades_file = PROJECT_ROOT / "processed" / "trades.csv"

    if not trades_file.exists():
        print(f"ERROR: {trades_file} not found!")
        print("Please ensure processed/trades.csv is available.")
        sys.exit(1)

    print(f"\nStreaming trades from {trades_file}...")
    print(f"File size: {trades_file.stat().st_size / 1e9:.2f} GB")

    # Strategy: Scan in lazy mode, filter, then collect
    # This avoids loading all 32GB into memory

    print("Scanning and filtering trades (this may take several minutes)...")
    start_time = datetime.now()

    try:
        # Use lazy API for memory efficiency
        trades_lazy = pl.scan_csv(
            str(trades_file),
            schema_overrides={
                'market_id': pl.Utf8,
                'timestamp': pl.Utf8,  # Datetime strings
                'price': pl.Float64,
                'usd_amount': pl.Float64,
                'token_amount': pl.Float64,
            }
        )

        # Convert market_ids to strings
        market_id_strs = [str(mid) for mid in market_ids]

        # Filter to only our markets
        filtered_trades = trades_lazy.filter(
            pl.col('market_id').is_in(market_id_strs)
        )

        # Collect in streaming mode
        print("Collecting filtered trades...")
        trades_df = filtered_trades.collect(engine='streaming' if STREAMING_MODE else 'cpu')

        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"✓ Extracted {len(trades_df):,} trades in {elapsed:.1f} seconds")
        print(f"  Memory usage: {trades_df.estimated_size() / 1e6:.1f} MB")

    except Exception as e:
        print(f"ERROR during streaming: {e}")
        print("\nFalling back to batch processing...")
        trades_df = extract_trades_batched(trades_file, market_ids)

    return trades_df


def extract_trades_batched(trades_file, market_ids):
    """
    Fallback: Extract trades in batches if streaming fails
    """
    batch_num = 0
    all_trades = []

    # Convert market_ids to strings
    market_id_strs = [str(mid) for mid in market_ids]

    # Read file in chunks
    reader = pl.read_csv_batched(
        str(trades_file),
        batch_size=500_000,  # 500K rows per batch
        schema_overrides={
            'market_id': pl.Utf8,
            'timestamp': pl.Utf8,  # Datetime strings
            'price': pl.Float64,
            'usd_amount': pl.Float64,
            'token_amount': pl.Float64,
        }
    )

    while True:
        batch = reader.next_batches(1)
        if not batch:
            break

        batch_df = batch[0]
        batch_num += 1

        # Filter to our markets
        filtered = batch_df.filter(
            pl.col('market_id').is_in(market_id_strs)
        )

        if len(filtered) > 0:
            all_trades.append(filtered)
            print(f"  Batch {batch_num}: Found {len(filtered):,} matching trades")

    # Concatenate all batches
    if all_trades:
        trades_df = pl.concat(all_trades)
        print(f"\n✓ Total extracted: {len(trades_df):,} trades")
        return trades_df
    else:
        print("WARNING: No trades found for these markets!")
        return pl.DataFrame()


def enrich_trades_with_market_data(trades_df, resolved_markets):
    """
    Join trades with market metadata and outcomes
    """
    print("\nEnriching trades with market metadata...")

    # Select relevant market columns
    market_info = resolved_markets.select([
        'market_id',
        'question',
        'asset',
        'target_price',
        'closedTime',
        'volume',
        'outcome',
        'last_price'
    ])

    # Join trades with market info
    enriched = trades_df.join(
        market_info,
        on='market_id',
        how='left'
    )

    # Convert timestamp string to datetime and extract Unix epoch
    enriched = enriched.with_columns([
        pl.col('timestamp').str.to_datetime(time_zone='UTC')
        .alias('trade_datetime')
    ])

    enriched = enriched.with_columns([
        pl.col('trade_datetime').dt.epoch(time_unit='s')
        .alias('trade_ts_sec')
    ])

    # Calculate time remaining until market close (in seconds)
    # closedTime is a datetime string - convert to datetime then to Unix epoch
    enriched = enriched.with_columns([
        pl.col('closedTime').str.to_datetime(time_zone='UTC')
        .dt.epoch(time_unit='s')
        .alias('closedTime_sec')
    ])

    # closedTime_sec and trade_ts_sec are both Unix timestamps in seconds
    enriched = enriched.with_columns([
        (pl.col('closedTime_sec') - pl.col('trade_ts_sec'))
        .alias('time_remaining_sec')
    ])

    # Sort by market and timestamp
    enriched = enriched.sort(['market_id', 'timestamp'])

    print(f"✓ Enriched {len(enriched):,} trades")

    # Show sample
    print("\nSample of enriched data:")
    print(enriched.head(3).select([
        'market_id', 'trade_datetime', 'price', 'asset',
        'target_price', 'outcome', 'time_remaining_sec'
    ]))

    return enriched


def save_extracted_data(enriched_trades):
    """
    Save extracted and enriched trade data
    """
    output_file = DATA_DIR / "polymarket_crypto_trades.csv"

    print(f"\nSaving to {output_file}...")
    enriched_trades.write_csv(output_file)

    file_size_mb = output_file.stat().st_size / 1e6
    print(f"✓ Saved {len(enriched_trades):,} trades ({file_size_mb:.1f} MB)")

    return output_file


def generate_summary_stats(enriched_trades):
    """
    Generate summary statistics for validation
    """
    print("\n" + "="*70)
    print("EXTRACTION SUMMARY")
    print("="*70)

    # Overall stats
    print(f"\nTotal trades extracted: {len(enriched_trades):,}")
    print(f"Unique markets: {enriched_trades['market_id'].n_unique():,}")

    # By asset
    by_asset = enriched_trades.group_by('asset').agg([
        pl.len().alias('trades'),
        pl.col('market_id').n_unique().alias('markets')
    ]).sort('trades', descending=True)

    print("\nBreakdown by asset:")
    print(by_asset)

    # By outcome
    by_outcome = enriched_trades.group_by('outcome').agg([
        pl.len().alias('trades'),
        pl.col('market_id').n_unique().alias('markets')
    ]).sort('trades', descending=True)

    print("\nBreakdown by outcome:")
    print(by_outcome)

    # Time range
    min_date = enriched_trades['trade_datetime'].min()
    max_date = enriched_trades['trade_datetime'].max()
    print(f"\nTrade date range:")
    print(f"  Earliest: {min_date}")
    print(f"  Latest: {max_date}")

    # Volume
    total_volume = enriched_trades['usd_amount'].sum()
    print(f"\nTotal trading volume: ${total_volume:,.2f}")

    print("\n" + "="*70)


def main():
    """
    Main execution pipeline for Phase 1.1
    """
    print("="*70)
    print("PHASE 1.1: EXTRACT POLYMARKET TRADE DATA")
    print("="*70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    start_time = datetime.now()

    # Step 1: Load usable markets
    markets, market_ids = load_usable_markets()

    # Step 2: Determine market outcomes
    resolved_markets = determine_market_outcomes(markets)

    # Update market_ids to only include resolved markets
    market_ids = set(resolved_markets['market_id'].to_list())
    print(f"\nFiltering trades for {len(market_ids)} resolved markets...")

    # Step 3: Extract trades (streaming for memory efficiency)
    trades_df = extract_trades_streaming(market_ids, resolved_markets)

    if len(trades_df) == 0:
        print("\nERROR: No trades extracted. Exiting.")
        sys.exit(1)

    # Convert market_id in trades_df to int64 to match resolved_markets
    trades_df = trades_df.with_columns([
        pl.col('market_id').cast(pl.Int64)
    ])

    # Step 4: Enrich with market metadata
    enriched_trades = enrich_trades_with_market_data(trades_df, resolved_markets)

    # Step 5: Save output
    output_file = save_extracted_data(enriched_trades)

    # Step 6: Generate summary statistics
    generate_summary_stats(enriched_trades)

    # Final timing
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nTotal execution time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    print(f"\n✓ Phase 1.1 COMPLETE")
    print(f"✓ Output: {output_file}")


if __name__ == "__main__":
    main()
