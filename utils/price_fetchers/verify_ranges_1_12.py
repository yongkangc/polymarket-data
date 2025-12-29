"""
Verify completeness of merged data (ranges 1-12).
Checks for gaps and calculates coverage statistics.
"""
import pandas as pd
from pathlib import Path

INPUT_FILE = 'data/binance_ranges_1_12_minute_data.csv'

def verify_data():
    """Verify merged data completeness."""
    print("=" * 70)
    print("VERIFYING MERGED DATA (RANGES 1-12)")
    print("=" * 70)

    if not Path(INPUT_FILE).exists():
        print(f"\nERROR: {INPUT_FILE} not found!")
        print("Run merge_aggregated.py first.")
        return

    print(f"\nLoading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    file_size = Path(INPUT_FILE).stat().st_size / (1024 * 1024)
    print(f"  File size: {file_size:.1f} MB")
    print(f"  Total rows: {len(df):,}")

    print("\n" + "=" * 70)
    print("PER-SYMBOL ANALYSIS")
    print("=" * 70)

    # Check per symbol
    for symbol in sorted(df['symbol'].unique()):
        symbol_df = df[df['symbol'] == symbol].sort_values('timestamp')

        start = symbol_df['timestamp'].min()
        end = symbol_df['timestamp'].max()
        duration_days = (end - start).total_seconds() / 86400
        expected_minutes = int((end - start).total_seconds() / 60) + 1
        actual_minutes = len(symbol_df)
        completeness = actual_minutes / expected_minutes * 100

        print(f"\n{symbol.upper()}:")
        print(f"  Date range: {start} to {end}")
        print(f"  Duration: {duration_days:.1f} days")
        print(f"  Expected minutes: {expected_minutes:,}")
        print(f"  Actual minutes: {actual_minutes:,}")
        print(f"  Completeness: {completeness:.1f}%")

        # Find gaps
        symbol_df = symbol_df.copy()
        symbol_df['gap'] = symbol_df['timestamp'].diff() > pd.Timedelta('1min')
        gaps = symbol_df[symbol_df['gap']]

        if len(gaps) > 0:
            print(f"  ⚠ Gaps found: {len(gaps)}")
            print(f"\n  First 5 gaps:")
            for idx, row in gaps.head(5).iterrows():
                prev_time = symbol_df[symbol_df['timestamp'] < row['timestamp']]['timestamp'].max()
                gap_minutes = int((row['timestamp'] - prev_time).total_seconds() / 60)
                print(f"    {prev_time} → {row['timestamp']} ({gap_minutes} min gap)")
        else:
            print(f"  ✓ No gaps - 100% continuous coverage!")

    print("\n" + "=" * 70)
    print("OVERALL SUMMARY")
    print("=" * 70)

    total_start = df['timestamp'].min()
    total_end = df['timestamp'].max()
    total_duration = (total_end - total_start).total_seconds() / 86400

    print(f"\nOverall date range: {total_start} to {total_end}")
    print(f"Overall duration: {total_duration:.1f} days")
    print(f"Total candles: {len(df):,}")
    print(f"Symbols: {', '.join(sorted(df['symbol'].unique()))}")

    # Quality checks
    print("\nQuality checks:")
    missing = df.isnull().sum().sum()
    duplicates = df.duplicated(subset=['symbol', 'timestamp']).sum()

    print(f"  Missing values: {missing}")
    print(f"  Duplicates: {duplicates}")

    # OHLC validity
    invalid_ohlc = ((df['high'] < df['open']) |
                    (df['high'] < df['close']) |
                    (df['low'] > df['open']) |
                    (df['low'] > df['close']) |
                    (df['high'] < df['low'])).sum()

    print(f"  Invalid OHLC: {invalid_ohlc}")

    if missing == 0 and duplicates == 0 and invalid_ohlc == 0:
        print("\n✓ All quality checks passed!")
    else:
        print("\n⚠ Some quality issues detected")

    print("=" * 70)

if __name__ == "__main__":
    verify_data()
