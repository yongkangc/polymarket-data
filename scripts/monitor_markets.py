#!/usr/bin/env python3
"""
Simplified Market Monitor - Continuously discover new Up/Down markets

This script runs in a loop and checks for new markets every X minutes.
Focus: Market ID discovery only (lightweight, no trade fetching).

Usage:
    python scripts/monitor_markets.py [--interval MINUTES]

Example:
    python scripts/monitor_markets.py --interval 5
"""
import sys
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from updown_pipeline import market_discovery, config


def monitor_loop(interval_minutes: int = 5):
    """
    Main monitoring loop

    Args:
        interval_minutes: How often to check for new markets (default: 5 minutes)
    """
    print("\n" + "="*70)
    print("UP/DOWN MARKET MONITOR - CONTINUOUS MODE")
    print("="*70)
    print(f"\n✓ Monitoring for new markets")
    print(f"✓ Check interval: {interval_minutes} minutes")
    print(f"✓ Assets: {', '.join(config.ASSETS)}")
    print(f"✓ Durations: {', '.join(config.DURATIONS)}")
    print(f"✓ Output: {config.UPDOWN_MARKETS}")
    print(f"\nPress Ctrl+C to stop\n")
    print("="*70)

    iteration = 0
    interval_seconds = interval_minutes * 60

    try:
        while True:
            iteration += 1
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

            print(f"\n[{timestamp}] Iteration {iteration}")
            print("-" * 70)

            try:
                # Run market discovery
                market_count = market_discovery.discover_updown_markets(
                    include_closed=False
                )

                if market_count > 0:
                    print(f"✓ Discovered {market_count} active markets")
                else:
                    print("○ No markets found (API may be down or no active markets)")

            except KeyboardInterrupt:
                raise  # Re-raise to exit cleanly
            except Exception as e:
                print(f"❌ Error during discovery: {e}")
                print("   Will retry on next iteration...")

            # Wait for next check
            print(f"\n⏱️  Waiting {interval_minutes} minutes until next check...")
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\n\n" + "="*70)
        print("⏹️  MONITOR STOPPED")
        print("="*70)
        print(f"\nCompleted {iteration} iteration(s)")
        print(f"Data saved to: {config.UPDOWN_MARKETS}\n")


def main():
    """Parse arguments and start monitoring"""
    parser = argparse.ArgumentParser(
        description="Monitor Polymarket for new Up/Down markets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check every 5 minutes (default)
  python scripts/monitor_markets.py

  # Check every 15 minutes
  python scripts/monitor_markets.py --interval 15

  # Check every minute (for testing)
  python scripts/monitor_markets.py --interval 1
        """
    )

    parser.add_argument(
        '--interval',
        type=int,
        default=5,
        help='Check interval in minutes (default: 5)'
    )

    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once and exit (no continuous monitoring)'
    )

    args = parser.parse_args()

    if args.interval < 1:
        print("❌ Error: Interval must be at least 1 minute")
        sys.exit(1)

    if args.once:
        # Run once and exit
        print("\nRunning one-time market discovery...\n")
        market_count = market_discovery.discover_updown_markets(include_closed=False)
        print(f"\n✓ Discovered {market_count} markets")
        print(f"✓ Saved to: {config.UPDOWN_MARKETS}\n")
    else:
        # Continuous monitoring
        monitor_loop(interval_minutes=args.interval)


if __name__ == "__main__":
    main()
