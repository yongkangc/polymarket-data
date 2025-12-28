"""
Main Pipeline Orchestrator
Runs all stages with checkpoint management.
"""
import argparse
import sys
from pathlib import Path

from . import config
from .checkpoint import CheckpointManager
from . import market_discovery
from . import fetch_historical_trades
from . import fetch_clob_trades
from . import integrate_binance
from . import stream_live


def run_phase1(checkpoints: CheckpointManager, force_refresh: bool = False):
    """
    Run Phase 1: Historical data pipeline

    Args:
        checkpoints: Checkpoint manager
        force_refresh: Force re-run even if checkpoints exist
    """
    print("\n" + "="*70)
    print("PHASE 1: HISTORICAL DATA PIPELINE")
    print("="*70)

    # Stage 1: Market Discovery
    if force_refresh or not checkpoints.exists('markets'):
        print("\n→ Running Stage 1: Market Discovery")
        market_count = market_discovery.discover_updown_markets()

        if market_count > 0:
            checkpoints.mark_done('markets', {
                'markets_found': market_count,
                'output_file': str(config.UPDOWN_MARKETS)
            })
        else:
            print("❌ Failed: No markets discovered")
            return False
    else:
        print("\n✓ Stage 1: Already complete (use --force-refresh to re-run)")
        meta = checkpoints.get_metadata('markets')
        if meta and meta.get('metadata'):
            print(f"  Markets: {meta['metadata'].get('markets_found')}")

    # Stage 2A: Fetch Historical Trades
    if force_refresh or not checkpoints.exists('historical'):
        print("\n→ Running Stage 2A: Fetch Historical Trades")
        trade_count = fetch_historical_trades.fetch_historical_trades()

        # Stage 2B: Fetch CLOB Trades (for new markets)
        print("\n→ Running Stage 2B: Fetch CLOB Trades")
        clob_count = fetch_clob_trades.fetch_clob_trades_for_new_markets()

        total_trades = trade_count + clob_count

        checkpoints.mark_done('historical', {
            'trades_found': total_trades,
            'historical_trades': trade_count,
            'clob_trades': clob_count,
            'output_file': str(config.UPDOWN_TRADES_HISTORICAL)
        })

    else:
        print("\n✓ Stage 2: Already complete (use --force-refresh to re-run)")
        meta = checkpoints.get_metadata('historical')
        if meta and meta.get('metadata'):
            print(f"  Trades: {meta['metadata'].get('trades_found'):,}")

    # Stage 3: Integrate Binance Prices
    if force_refresh or not checkpoints.exists('enriched'):
        print("\n→ Running Stage 3: Integrate Binance Prices")
        enriched_count = integrate_binance.integrate_binance_prices()

        if enriched_count >= 0:  # 0 is OK (might have no trades)
            checkpoints.mark_done('enriched', {
                'trades_enriched': enriched_count,
                'output_file': str(config.UPDOWN_TRADES_ENRICHED)
            })
        else:
            print("❌ Failed: Could not enrich trades")
            return False
    else:
        print("\n✓ Stage 3: Already complete (use --force-refresh to re-run)")
        meta = checkpoints.get_metadata('enriched')
        if meta and meta.get('metadata'):
            print(f"  Enriched trades: {meta['metadata'].get('trades_enriched'):,}")

    print("\n" + "="*70)
    print("✅ PHASE 1 COMPLETE!")
    print("="*70)

    return True


def run_phase2():
    """
    Run Phase 2: Real-time streaming
    """
    stream_live.stream_live()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Up/Down Market Pipeline - Polymarket + Binance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # First run (full pipeline)
  python -m updown_pipeline.run_pipeline

  # Force refresh all data
  python -m updown_pipeline.run_pipeline --force-refresh

  # Skip to streaming only (requires Phase 1 complete)
  python -m updown_pipeline.run_pipeline --stream-only

  # Clear checkpoints and start fresh
  python -m updown_pipeline.run_pipeline --clear-checkpoints
        """
    )

    parser.add_argument(
        '--force-refresh',
        action='store_true',
        help='Force re-run all stages even if checkpoints exist'
    )

    parser.add_argument(
        '--stream-only',
        action='store_true',
        help='Skip Phase 1 and go directly to streaming (requires Phase 1 complete)'
    )

    parser.add_argument(
        '--no-stream',
        action='store_true',
        help='Run Phase 1 only, do not start streaming'
    )

    parser.add_argument(
        '--clear-checkpoints',
        action='store_true',
        help='Clear all checkpoints before starting'
    )

    args = parser.parse_args()

    # Initialize checkpoint manager
    checkpoints = CheckpointManager(config.CHECKPOINT_DIR)

    # Print banner
    print("\n" + "="*70)
    print("UP/DOWN MARKET PIPELINE")
    print("Polymarket (BTC/SOL/ETH) + Binance Price Data")
    print("="*70)

    # Clear checkpoints if requested
    if args.clear_checkpoints:
        print("\n→ Clearing all checkpoints...")
        checkpoints.clear()

    # Show checkpoint status
    checkpoints.print_status()

    # Handle stream-only mode
    if args.stream_only:
        if not checkpoints.all_phase1_complete():
            print("❌ Cannot start streaming: Phase 1 not complete")
            print("   Run without --stream-only first to complete Phase 1")
            sys.exit(1)

        print("→ Skipping to Phase 2 (streaming)")
        run_phase2()
        return

    # Check if Phase 1 is already complete and recent
    if checkpoints.all_phase1_complete() and not args.force_refresh:
        if checkpoints.is_recent('enriched', hours=config.CHECKPOINT_FRESHNESS_HOURS):
            print(f"✅ Phase 1 already complete and recent (< {config.CHECKPOINT_FRESHNESS_HOURS}h)")

            if args.no_stream:
                print("\n✅ Done (--no-stream specified)")
                return

            print("→ Starting Phase 2 (real-time streaming)")
            run_phase2()
            return
        else:
            print(f"⚠️ Phase 1 complete but outdated (> {config.CHECKPOINT_FRESHNESS_HOURS}h)")
            print("→ Will re-run Phase 1 for fresh data")

    # Run Phase 1
    success = run_phase1(checkpoints, force_refresh=args.force_refresh)

    if not success:
        print("\n❌ Pipeline failed")
        sys.exit(1)

    # Optionally start Phase 2
    if args.no_stream:
        print("\n✅ Pipeline complete (--no-stream specified)")
        print(f"\n📊 Output: {config.UPDOWN_TRADES_ENRICHED}")
        return

    print("\n→ Starting Phase 2 (real-time streaming)")
    run_phase2()


if __name__ == "__main__":
    main()
