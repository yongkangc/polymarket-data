"""
Phase 2: Live Streaming
Continuously update data with new markets and trades.
"""
import time
from datetime import datetime, timezone
import polars as pl

from . import market_discovery
from . import fetch_clob_trades
from . import integrate_binance
from . import config


def get_last_trade_timestamp() -> int:
    """Get timestamp of most recent trade in enriched data"""
    if not config.UPDOWN_TRADES_ENRICHED.exists():
        return 0

    try:
        df = pl.read_csv(config.UPDOWN_TRADES_ENRICHED)
        if len(df) == 0 or 'trade_ts_sec' not in df.columns:
            return 0
        return int(df['trade_ts_sec'].max())
    except:
        return 0


def discover_and_fetch_new_markets() -> int:
    """
    Check for new markets and fetch their trades

    Returns:
        Number of new markets discovered
    """
    print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Checking for new markets...")

    # Discover markets (will update updown_markets.csv)
    market_count = market_discovery.discover_updown_markets(include_closed=False)

    # Fetch trades for new markets via CLOB API
    trade_count = fetch_clob_trades.fetch_clob_trades_for_new_markets()

    if trade_count > 0:
        # Re-run integration to enrich new trades
        print(f"\n→ Re-enriching with new trades...")
        integrate_binance.integrate_binance_prices()

    return market_count


def poll_new_trades() -> int:
    """
    Poll CLOB API for new trades on active markets

    Returns:
        Number of new trades found
    """
    # For simplicity, we'll re-run CLOB fetch
    # In production, you'd track last_trade_id and fetch incrementally
    trade_count = fetch_clob_trades.fetch_clob_trades_for_new_markets()

    if trade_count > 0:
        # Re-run integration
        integrate_binance.integrate_binance_prices()

    return trade_count


def stream_live():
    """
    Main streaming loop
    """
    print("\n" + "="*70)
    print("PHASE 2: LIVE STREAMING MODE")
    print("="*70)
    print("\n🔴 STREAMING STARTED")
    print(f"   Market check interval: {config.MARKET_CHECK_INTERVAL}s ({config.MARKET_CHECK_INTERVAL/60:.0f} min)")
    print(f"   Trade poll interval: {config.TRADE_POLL_INTERVAL}s")
    print("\n   Press Ctrl+C to stop\n")

    last_market_check = 0
    iterations = 0

    try:
        while True:
            iterations += 1
            current_time = time.time()

            # Every N minutes: check for new markets
            if current_time - last_market_check > config.MARKET_CHECK_INTERVAL:
                try:
                    new_count = discover_and_fetch_new_markets()
                    last_market_check = current_time
                except Exception as e:
                    print(f"❌ Error in market discovery: {e}")

            # Every N seconds: poll for new trades
            try:
                print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Polling for new trades (iteration {iterations})...")
                trade_count = poll_new_trades()

                if trade_count > 0:
                    print(f"   ✓ Added {trade_count} new trades")
                else:
                    print(f"   ○ No new trades")

            except KeyboardInterrupt:
                raise  # Re-raise to exit
            except Exception as e:
                print(f"❌ Error polling trades: {e}")

            # Wait before next poll
            time.sleep(config.TRADE_POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n\n⏹️  STREAMING STOPPED")
        print("="*70 + "\n")


if __name__ == "__main__":
    # Test standalone
    stream_live()
