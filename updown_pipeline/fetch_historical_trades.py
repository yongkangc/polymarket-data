"""
Stage 2A: Fetch Historical Trades
Filter existing trades.csv by up/down market IDs.
"""
import polars as pl
from typing import List

from . import config


def fetch_historical_trades() -> int:
    """
    Filter existing trades.csv for up/down markets

    Returns:
        Number of trades found
    """
    print("\n" + "="*70)
    print("STAGE 2A: FETCH HISTORICAL TRADES")
    print("="*70)

    # Check if files exist
    if not config.UPDOWN_MARKETS.exists():
        print(f"❌ Markets file not found: {config.UPDOWN_MARKETS}")
        print("   Run Stage 1 (market discovery) first")
        return 0

    if not config.EXISTING_TRADES.exists():
        print(f"❌ Trades file not found: {config.EXISTING_TRADES}")
        print(f"   Expected: {config.EXISTING_TRADES}")
        return 0

    # Load market IDs
    print(f"\n→ Loading market IDs from {config.UPDOWN_MARKETS.name}...")
    markets = pl.read_csv(config.UPDOWN_MARKETS, schema_overrides={
        'yes_token_id': pl.Utf8,
        'no_token_id': pl.Utf8
    })
    market_ids = markets['market_id'].unique().to_list()

    # Remove None values
    market_ids = [mid for mid in market_ids if mid is not None]

    print(f"   Loaded {len(market_ids)} unique market IDs")

    if not market_ids:
        print("❌ No market IDs found")
        return 0

    # Filter trades.csv
    print(f"\n→ Filtering {config.EXISTING_TRADES.name}...")
    print(f"   (This may take a few minutes for large files)")

    try:
        # Use streaming for memory efficiency
        trades = (
            pl.scan_csv(config.EXISTING_TRADES)
            .filter(pl.col('market_id').is_in(market_ids))
            .collect(streaming=True)
        )

        trade_count = len(trades)
        print(f"   Found {trade_count:,} historical trades")

        if trade_count == 0:
            print("⚠️ No trades found for these markets in historical data")
            print("   This might mean:")
            print("   - Markets are too recent (not in trades.csv yet)")
            print("   - Markets had no trading volume")
            print("   Will need to fetch from CLOB API in Stage 2B")

            # Create empty file with schema
            trades = pl.DataFrame(schema={
                'timestamp': pl.Utf8,
                'market_id': pl.Utf8,
                'maker': pl.Utf8,
                'taker': pl.Utf8,
                'nonusdc_side': pl.Utf8,
                'maker_direction': pl.Utf8,
                'taker_direction': pl.Utf8,
                'price': pl.Float64,
                'usd_amount': pl.Float64,
                'token_amount': pl.Float64,
                'transactionHash': pl.Utf8
            })

        # Save
        print(f"\n→ Saving to {config.UPDOWN_TRADES_HISTORICAL.name}...")
        trades.write_csv(config.UPDOWN_TRADES_HISTORICAL)

        # Summary stats
        if trade_count > 0:
            print(f"\n→ Summary:")
            print(f"   Total trades: {trade_count:,}")
            print(f"   Markets with trades: {trades['market_id'].n_unique()}")
            print(f"   Total volume: ${trades['usd_amount'].sum():,.2f}")
            print(f"   Date range: {trades['timestamp'].min()} to {trades['timestamp'].max()}")

        print(f"\n✅ Stage 2A complete: {trade_count:,} historical trades")
        print("="*70 + "\n")

        return trade_count

    except Exception as e:
        print(f"❌ Error filtering trades: {e}")
        import traceback
        traceback.print_exc()
        return 0


if __name__ == "__main__":
    # Test standalone
    count = fetch_historical_trades()
    print(f"\nFetched {count:,} historical trades")
