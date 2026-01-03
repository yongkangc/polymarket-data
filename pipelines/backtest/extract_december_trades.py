"""
Extract December trades from trader-dashboard enriched data.
The trader_crypto_enriched.parquet already has December 2025 data with:
- Trade details (price, amount, side)
- Market metadata (crypto_type, winning_token)
- Timing (time_to_close_sec)

Outputs: data/december_trades.parquet
"""
import polars as pl
from pathlib import Path


BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
TRADER_DATA = Path("/home/chiayongtcac/pm/trader-dashboard/dashboard/trader_crypto_enriched.parquet")
OUTPUT_FILE = DATA_DIR / "december_trades.parquet"


def extract_december_trades():
    """Extract December trades from trader-dashboard enriched data"""
    print("=" * 70)
    print("EXTRACT DECEMBER TRADES")
    print("=" * 70)
    
    # Load enriched data
    print(f"\n→ Loading from {TRADER_DATA}...")
    df = pl.read_parquet(TRADER_DATA)
    print(f"  Total trades: {len(df):,}")
    
    # Filter for December
    print(f"\n→ Filtering for December...")
    december = df.filter(pl.col('timestamp').dt.month() == 12)
    print(f"  December trades: {len(december):,}")
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    print(f"\n→ Date range:")
    print(f"   Min: {december['timestamp'].min()}")
    print(f"   Max: {december['timestamp'].max()}")
    
    print(f"\n→ By crypto_type:")
    for row in december.group_by('crypto_type').agg(pl.len().alias('count')).sort('crypto_type').iter_rows(named=True):
        print(f"   {row['crypto_type']}: {row['count']:,}")
    
    print(f"\n→ By trader_side:")
    for row in december.group_by('trader_side').agg(pl.len().alias('count')).iter_rows(named=True):
        print(f"   {row['trader_side']}: {row['count']:,}")
    
    print(f"\n→ By trader_role:")
    for row in december.group_by('trader_role').agg(pl.len().alias('count')).iter_rows(named=True):
        print(f"   {row['trader_role']}: {row['count']:,}")
    
    print(f"\n→ Volume: ${december['usd_amount'].sum():,.2f}")
    print(f"→ Unique markets: {december['market_id'].n_unique():,}")
    
    # Check winning_token distribution
    print(f"\n→ Resolution status:")
    resolved = december.filter(pl.col('winning_token').is_not_null())
    print(f"   Trades with resolution: {len(resolved):,} ({len(resolved)/len(december)*100:.1f}%)")
    
    # Save
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n→ Saving to {OUTPUT_FILE}...")
    december.write_parquet(OUTPUT_FILE)
    
    file_size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    print(f"  File size: {file_size_mb:.1f} MB")
    
    print(f"\n✅ Done!")
    return len(december)


if __name__ == "__main__":
    count = extract_december_trades()
    print(f"\nExtracted {count:,} December trades")

