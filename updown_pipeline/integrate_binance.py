"""
Stage 3: Integrate Binance Prices
Join Polymarket trades with Binance price data and calculate features.
"""
import polars as pl
from datetime import datetime

from . import config


def parse_timestamp_column(df: pl.DataFrame, col_name: str) -> pl.DataFrame:
    """
    Parse timestamp column to Unix seconds

    Handles multiple formats:
    - Already Unix timestamp (int)
    - ISO datetime string
    - Other datetime formats
    """
    if col_name not in df.columns:
        return df

    # Check if already numeric
    if df[col_name].dtype in [pl.Int64, pl.Int32, pl.Float64]:
        return df

    # Try parsing as datetime
    try:
        df = df.with_columns([
            pl.col(col_name)
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f", strict=False)
            .dt.timestamp()
            .alias(col_name + '_sec')
        ])
        return df
    except:
        pass

    # Try parsing with timezone
    try:
        df = df.with_columns([
            pl.col(col_name)
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%z", strict=False)
            .dt.timestamp()
            .alias(col_name + '_sec')
        ])
        return df
    except:
        pass

    print(f"   ⚠️ Could not parse timestamp column: {col_name}")
    return df


def integrate_binance_prices() -> int:
    """
    Join trades with Binance prices and calculate features

    Returns:
        Number of enriched trades
    """
    print("\n" + "="*70)
    print("STAGE 3: INTEGRATE BINANCE PRICES")
    print("="*70)

    # Check if files exist
    if not config.UPDOWN_TRADES_HISTORICAL.exists():
        print(f"❌ Historical trades file not found: {config.UPDOWN_TRADES_HISTORICAL}")
        return 0

    if not config.UPDOWN_MARKETS.exists():
        print(f"❌ Markets file not found: {config.UPDOWN_MARKETS}")
        return 0

    if not config.BINANCE_DATA.exists():
        print(f"❌ Binance data file not found: {config.BINANCE_DATA}")
        return 0

    # Load data
    print(f"\n→ Loading data...")
    print(f"   - Markets: {config.UPDOWN_MARKETS.name}")
    trades = pl.read_csv(config.UPDOWN_TRADES_HISTORICAL)
    print(f"   - Trades: {len(trades):,} rows")

    if len(trades) == 0:
        print("⚠️ No trades to enrich")
        # Create empty enriched file
        pl.DataFrame().write_csv(config.UPDOWN_TRADES_ENRICHED)
        return 0

    markets = pl.read_csv(config.UPDOWN_MARKETS, schema_overrides={
        'yes_token_id': pl.Utf8,
        'no_token_id': pl.Utf8
    })
    print(f"   - Markets: {len(markets)} rows")

    print(f"   - Binance: {config.BINANCE_DATA.name}")
    binance = pl.read_csv(config.BINANCE_DATA)
    print(f"     {len(binance):,} rows")

    # Parse timestamps
    print(f"\n→ Parsing timestamps...")

    # Parse trade timestamps
    if 'timestamp' in trades.columns:
        trades = parse_timestamp_column(trades, 'timestamp')
        if 'timestamp_sec' in trades.columns:
            trades = trades.rename({'timestamp_sec': 'trade_ts_sec'})
        else:
            # Assume already in seconds
            if trades['timestamp'].dtype in [pl.Int64, pl.Int32]:
                trades = trades.rename({'timestamp': 'trade_ts_sec'})

    # Ensure we have trade_ts_sec
    if 'trade_ts_sec' not in trades.columns:
        print("❌ Could not extract trade timestamp in seconds")
        return 0

    # Parse Binance timestamps
    if 'timestamp' in binance.columns:
        # Check if it's a datetime string or already numeric
        if binance['timestamp'].dtype == pl.Utf8:
            binance = parse_timestamp_column(binance, 'timestamp')
            if 'timestamp_sec' in binance.columns:
                binance = binance.with_columns([
                    pl.col('timestamp_sec').alias('ts')
                ])
            else:
                binance = binance.with_columns([
                    pl.col('timestamp').alias('ts')
                ])
        else:
            binance = binance.with_columns([
                pl.col('timestamp').alias('ts')
            ])
    else:
        print("❌ Binance data missing 'timestamp' column")
        return 0

    print(f"   ✓ Timestamps parsed")

    # Join trades with market metadata
    print(f"\n→ Joining trades with market metadata...")
    trades_with_markets = trades.join(
        markets.select(['market_id', 'asset', 'duration', 'start_time', 'end_time', 'question']),
        on='market_id',
        how='left'
    )

    print(f"   ✓ {len(trades_with_markets):,} trades with market info")

    # Enrich by asset
    print(f"\n→ Enriching trades with Binance prices...")
    enriched_parts = []

    for asset in config.ASSETS:
        print(f"\n   Processing {asset}...")

        # Filter trades for this asset
        asset_trades = trades_with_markets.filter(pl.col('asset') == asset)

        if len(asset_trades) == 0:
            print(f"     No trades for {asset}")
            continue

        print(f"     {len(asset_trades):,} trades")

        # Get Binance symbol
        symbol = config.BINANCE_SYMBOL_MAP.get(asset)
        if not symbol:
            print(f"     ⚠️ No Binance symbol mapping for {asset}")
            continue

        # Filter Binance data for this symbol
        asset_prices = binance.filter(pl.col('symbol') == symbol)

        if len(asset_prices) == 0:
            print(f"     ⚠️ No Binance data for {symbol}")
            continue

        print(f"     {len(asset_prices):,} price points")

        # Join: trade time → asset price
        print(f"     Joining trade times...")
        enriched = asset_trades.join_asof(
            asset_prices.select(['ts', 'close']),
            left_on='trade_ts_sec',
            right_on='ts',
            strategy='nearest'
        ).rename({'close': 'asset_price_at_trade'})

        # Join: market open time → market open price
        print(f"     Joining market open times...")

        # Get unique markets for this asset with their open prices
        asset_markets = markets.filter(pl.col('asset') == asset)

        market_opens_with_price = asset_markets.join_asof(
            asset_prices.select(['ts', 'close']),
            left_on='start_time',
            right_on='ts',
            strategy='nearest'
        ).rename({'close': 'market_open_price'})

        enriched = enriched.join(
            market_opens_with_price.select(['market_id', 'market_open_price']),
            on='market_id',
            how='left'
        )

        print(f"     ✓ Enriched {len(enriched):,} trades")
        enriched_parts.append(enriched)

    if not enriched_parts:
        print("\n❌ No trades could be enriched")
        return 0

    # Combine all assets
    print(f"\n→ Combining all assets...")
    final = pl.concat(enriched_parts)

    # Calculate features
    print(f"\n→ Calculating features...")

    final = final.with_columns([
        # Price move %
        ((pl.col('asset_price_at_trade') - pl.col('market_open_price')) /
         pl.col('market_open_price') * 100).alias('move_pct'),

        # Time remaining (seconds)
        (pl.col('end_time') - pl.col('trade_ts_sec')).alias('time_remaining_sec')
    ])

    print(f"   ✓ Calculated move_pct and time_remaining_sec")

    # Summary stats
    print(f"\n→ Summary:")
    print(f"   Total enriched trades: {len(final):,}")
    print(f"   Markets: {final['market_id'].n_unique()}")
    print(f"   Assets: {', '.join(final['asset'].unique().to_list())}")

    if len(final) > 0:
        print(f"\n   Move % distribution:")
        print(f"     Min:    {final['move_pct'].min():.4f}%")
        print(f"     25th:   {final['move_pct'].quantile(0.25):.4f}%")
        print(f"     Median: {final['move_pct'].median():.4f}%")
        print(f"     75th:   {final['move_pct'].quantile(0.75):.4f}%")
        print(f"     Max:    {final['move_pct'].max():.4f}%")

    # Save
    print(f"\n→ Saving to {config.UPDOWN_TRADES_ENRICHED.name}...")
    final.write_csv(config.UPDOWN_TRADES_ENRICHED)

    print(f"\n✅ Stage 3 complete: {len(final):,} trades enriched")
    print("="*70 + "\n")

    return len(final)


if __name__ == "__main__":
    # Test standalone
    count = integrate_binance_prices()
    print(f"\nEnriched {count:,} trades")
