"""
Classify and count crypto price prediction markets
"""
import re
from datetime import datetime
import polars as pl
import sys
sys.path.append('..')
from poly_utils import get_markets

def classify_crypto_markets(markets_df):
    """
    Identify crypto price prediction markets from the full market dataset

    Returns DataFrame with:
    - market_id
    - question
    - asset (BTC, ETH, SOL)
    - target_price
    - deadline
    - market_type (binary, range, comparison, announcement)
    - volume
    - closedTime
    """

    print("=" * 80)
    print("Classifying Crypto Price Prediction Markets")
    print("=" * 80)

    # Step 1: Filter for crypto-related markets
    print("\nStep 1: Filtering for crypto-related questions...")

    crypto_patterns = {
        'BTC': r'(?i)\b(btc|bitcoin|\$btc)\b',
        'ETH': r'(?i)\b(eth|ethereum|\$eth)\b',
        'SOL': r'(?i)\b(sol|solana|\$sol)\b',
    }

    crypto_markets = {}

    for asset, pattern in crypto_patterns.items():
        matches = markets_df.filter(
            pl.col('question').str.contains(pattern)
        )
        crypto_markets[asset] = matches
        print(f"  {asset}: {len(matches)} markets found")

    # Step 2: Identify market types
    print("\nStep 2: Classifying market types...")

    def classify_market_type(question, answer1, answer2):
        """Classify the type of price market"""
        question_lower = question.lower()

        # Price prediction patterns
        price_keywords = ['break', 'above', 'reach', 'hit', 'exceed', 'below', 'price']

        # Check if it's a price prediction market
        has_price_keyword = any(kw in question_lower for kw in price_keywords)

        # Check answer types
        is_binary = (answer1 in ['Yes', 'No']) or (answer2 in ['Yes', 'No'])
        is_range = (answer1 in ['Long', 'Short']) or (answer2 in ['Long', 'Short'])

        # Exclusion patterns
        comparison_keywords = ['tesla', 'apple', 'amazon', 'twitter', 'stock', 'vs', 'or', 'higher market cap']
        announcement_keywords = ['purchase', 'announce', 'buy', 'accept', 'adoption']

        is_comparison = any(kw in question_lower for kw in comparison_keywords)
        is_announcement = any(kw in question_lower for kw in announcement_keywords)

        # Classify
        if is_comparison:
            return 'comparison'
        elif is_announcement:
            return 'announcement'
        elif not has_price_keyword:
            return 'other'
        elif is_binary:
            return 'binary'
        elif is_range:
            return 'range'
        else:
            return 'unknown'

    def extract_target_price(question):
        """Extract target price from question text"""
        # Patterns: $15k, $20,000, 15k, 20000
        patterns = [
            r'\$?([\d,]+)k\b',  # 15k, $20k
            r'\$(\d{3,}(?:,\d{3})*)\b',  # $20,000
        ]

        for pattern in patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                value_str = match.group(1).replace(',', '')
                try:
                    # Handle 'k' suffix
                    if 'k' in question[match.start():match.end()].lower():
                        return float(value_str) * 1000
                    else:
                        return float(value_str)
                except:
                    pass
        return None

    def extract_deadline(question, closed_time):
        """Extract deadline from question or use closedTime"""
        # Common deadline patterns
        deadline_patterns = [
            r'before\s+(\w+\s+\d{1,2},?\s+\d{4})',  # "before January 1, 2021"
            r'before\s+(\w+)\b',  # "before Thanksgiving"
            r'on\s+(\w+\s+\d{1,2},?\s+\d{4})',  # "on February 24, 2021"
            r'by\s+(\w+\s+\d{1,2},?\s+\d{4})',  # "by March 1st"
        ]

        for pattern in deadline_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                return match.group(1)

        # Fall back to closedTime if available
        if closed_time and closed_time != 'null':
            return closed_time

        return None

    # Process each asset
    classified_results = []

    for asset, df in crypto_markets.items():
        print(f"\n  Processing {asset} markets...")

        for row in df.iter_rows(named=True):
            market_type = classify_market_type(
                row['question'],
                row.get('answer1', ''),
                row.get('answer2', '')
            )

            target_price = extract_target_price(row['question'])
            deadline = extract_deadline(row['question'], row.get('closedTime'))

            classified_results.append({
                'market_id': row['id'],
                'question': row['question'],
                'asset': asset,
                'market_type': market_type,
                'target_price': target_price,
                'deadline': deadline,
                'answer1': row.get('answer1', ''),
                'answer2': row.get('answer2', ''),
                'volume': row.get('volume', 0),
                'closedTime': row.get('closedTime'),
            })

    # Create DataFrame
    classified_df = pl.DataFrame(classified_results)

    # Step 3: Summary statistics
    print("\n" + "=" * 80)
    print("Classification Summary")
    print("=" * 80)

    for asset in crypto_patterns.keys():
        asset_df = classified_df.filter(pl.col('asset') == asset)

        print(f"\n{asset}:")
        print(f"  Total markets: {len(asset_df)}")

        # By market type
        type_counts = asset_df.group_by('market_type').agg(
            pl.len().alias('count')
        ).sort('count', descending=True)

        print("  By type:")
        for row in type_counts.iter_rows(named=True):
            print(f"    {row['market_type']:15s}: {row['count']:4d}")

        # Filter for usable markets
        usable = asset_df.filter(
            (pl.col('market_type') == 'binary') &
            (pl.col('target_price').is_not_null()) &
            (pl.col('closedTime').is_not_null()) &
            (pl.col('volume') > 10000)
        )

        print(f"\n  Usable markets (binary + price + closed + volume>$10K):")
        print(f"    Count: {len(usable)}")

        if len(usable) > 0:
            total_volume = usable['volume'].sum()
            print(f"    Total volume: ${total_volume:,.0f}")

            # Price target distribution
            price_targets = usable['target_price'].drop_nulls().sort()
            if len(price_targets) > 0:
                print(f"    Price targets: ${price_targets.min():,.0f} - ${price_targets.max():,.0f}")

    return classified_df


def analyze_usable_markets(classified_df):
    """
    Detailed analysis of usable markets
    """
    print("\n" + "=" * 80)
    print("Detailed Analysis of Usable Markets")
    print("=" * 80)

    # Filter to usable markets
    usable = classified_df.filter(
        (pl.col('market_type') == 'binary') &
        (pl.col('target_price').is_not_null()) &
        (pl.col('closedTime').is_not_null()) &
        (pl.col('volume') > 10000)
    )

    print(f"\nTotal usable markets across all assets: {len(usable)}")

    # Convert closedTime to datetime for analysis
    usable = usable.with_columns(
        pl.col('closedTime').str.to_datetime(time_zone="UTC").alias('closed_datetime')
    )

    # Year distribution
    usable = usable.with_columns(
        pl.col('closed_datetime').dt.year().alias('year')
    )

    print("\nDistribution by year:")
    year_counts = usable.group_by('year').agg(
        pl.len().alias('count'),
        pl.col('volume').sum().alias('total_volume')
    ).sort('year')

    for row in year_counts.iter_rows(named=True):
        print(f"  {row['year']}: {row['count']:3d} markets, ${row['total_volume']:,.0f} volume")

    # Top 10 markets by volume
    print("\nTop 10 markets by volume:")
    top_markets = usable.select([
        'asset', 'question', 'target_price', 'volume'
    ]).sort('volume', descending=True).head(10)

    for i, row in enumerate(top_markets.iter_rows(named=True), 1):
        print(f"\n  {i}. [{row['asset']}] ${row['target_price']:,.0f} target - ${row['volume']:,.0f}")
        question_short = row['question'][:70] + "..." if len(row['question']) > 70 else row['question']
        print(f"     {question_short}")

    # Sample size estimation
    print("\n" + "=" * 80)
    print("Sample Size Estimation for Bucketing")
    print("=" * 80)

    for asset in ['BTC', 'ETH', 'SOL']:
        asset_usable = usable.filter(pl.col('asset') == asset)
        count = len(asset_usable)

        if count > 0:
            print(f"\n{asset}:")
            print(f"  Usable markets: {count}")

            # Estimate samples per bucket with different granularities
            configs = [
                ("Original (ultra-granular)", 12 * 2, 20, 12),  # 5,760 buckets
                ("Coarse (recommended)", 5 * 2, 8, 5),  # 400 buckets
                ("Very coarse", 3 * 2, 5, 3),  # 90 buckets
            ]

            for name, distance_buckets, time_buckets, price_buckets in configs:
                total_buckets = distance_buckets * time_buckets * price_buckets
                samples_per_bucket = count / total_buckets

                print(f"  {name}:")
                print(f"    Total buckets: {total_buckets}")
                print(f"    Avg samples/bucket: {samples_per_bucket:.2f}")

                if samples_per_bucket < 1:
                    print(f"    ⚠ Most buckets will be EMPTY!")
                elif samples_per_bucket < 5:
                    print(f"    ⚠ Most buckets will have LOW sample size")
                elif samples_per_bucket < 10:
                    print(f"    ✓ Acceptable but sparse")
                else:
                    print(f"    ✓ Good sample size")

    return usable


if __name__ == "__main__":
    # Load markets
    print("Loading markets...")
    markets_df = get_markets()

    # Classify
    classified_df = classify_crypto_markets(markets_df)

    # Analyze usable markets
    usable_df = analyze_usable_markets(classified_df)

    # Save results
    print("\n" + "=" * 80)
    print("Saving Results")
    print("=" * 80)

    output_file = "results/classified_crypto_markets.csv"
    classified_df.write_csv(output_file)
    print(f"✓ Saved classified markets to: {output_file}")

    usable_output = "results/usable_crypto_markets.csv"
    usable_df.write_csv(usable_output)
    print(f"✓ Saved usable markets to: {usable_output}")

    print("\n" + "=" * 80)
    print("Phase 0 Validation Complete!")
    print("=" * 80)
