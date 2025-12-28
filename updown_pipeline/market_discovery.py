"""
Stage 1: Market Discovery
Query Polymarket API and filter for up/down markets.
"""
import requests
import polars as pl
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
import ast

from . import config


def fetch_polymarket_events(closed: bool = False, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Fetch events from Polymarket API

    Args:
        closed: Include closed markets
        limit: Max number of events to fetch

    Returns:
        List of event dictionaries
    """
    url = f"{config.POLYMARKET_API_BASE}/events"
    params = {
        "limit": limit,
        "closed": "true" if closed else "false",
        "order": "new"
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        events = response.json()
        return events if isinstance(events, list) else []
    except Exception as e:
        print(f"❌ Error fetching events: {e}")
        return []


def extract_duration(slug: str) -> Optional[str]:
    """
    Extract duration from slug

    Examples:
        btc-updown-5m-1766972400 → 5m
        sol-updown-15m-1766972700 → 15m
        eth-updown-1h-1766973000 → 1h
    """
    slug_lower = slug.lower()

    for duration, patterns in config.DURATION_PATTERNS.items():
        if any(pattern in slug_lower for pattern in patterns):
            return duration

    return None


def is_updown_market(event: Dict[str, Any]) -> bool:
    """Check if event is an up/down market"""
    title = (event.get('title') or '').lower()
    question = (event.get('question') or '').lower()

    # Must have both "up" and "down"
    has_up = 'up' in title or 'up' in question
    has_down = 'down' in title or 'down' in question

    return has_up and has_down


def extract_asset(event: Dict[str, Any]) -> Optional[str]:
    """Extract asset (BTC/SOL/ETH) from event"""
    title = (event.get('title') or '').lower()
    question = (event.get('question') or '').lower()
    slug = (event.get('slug') or '').lower()

    combined = f"{title} {question} {slug}"

    for asset, patterns in config.ASSET_PATTERNS.items():
        if any(pattern in combined for pattern in patterns):
            return asset

    return None


def extract_market_data(event: Dict[str, Any], asset: str, duration: str) -> Dict[str, Any]:
    """
    Extract structured market data from event

    Returns:
        Dictionary with market metadata
    """
    # Get first market (up/down events usually have one market)
    markets = event.get('markets', [])
    market = markets[0] if markets else {}

    # Extract token IDs
    token_ids_str = market.get('clobTokenIds', '[]')
    try:
        token_ids = ast.literal_eval(token_ids_str)
        yes_token_id = token_ids[0] if len(token_ids) > 0 else None
        no_token_id = token_ids[1] if len(token_ids) > 1 else None
    except:
        yes_token_id = None
        no_token_id = None

    # Parse timestamps
    end_date = event.get('endDate') or market.get('endDate')
    event_start_time = market.get('eventStartTime')

    # Convert to Unix timestamps if needed
    def parse_timestamp(ts_str):
        if not ts_str:
            return None
        try:
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            return int(dt.timestamp())
        except:
            return None

    end_time = parse_timestamp(end_date)
    start_time = parse_timestamp(event_start_time)

    return {
        'event_id': event.get('id'),
        'market_id': market.get('id'),
        'slug': event.get('slug'),
        'asset': asset,
        'duration': duration,
        'question': market.get('question') or event.get('title'),
        'start_time': start_time,
        'end_time': end_time,
        'yes_token_id': yes_token_id,
        'no_token_id': no_token_id,
        'resolution_source': market.get('resolutionSource'),
        'volume': float(market.get('volume', 0)),
        'active': market.get('active', False),
        'closed': market.get('closed', False),
    }


def discover_updown_markets(include_closed: bool = False) -> int:
    """
    Main function: Discover and save up/down markets

    Returns:
        Number of markets discovered
    """
    print("\n" + "="*70)
    print("STAGE 1: MARKET DISCOVERY")
    print("="*70)

    print(f"\n→ Fetching events from Polymarket API...")
    print(f"   Assets: {', '.join(config.ASSETS)}")
    print(f"   Durations: {', '.join(config.DURATIONS)}")

    # Fetch events
    events = fetch_polymarket_events(closed=include_closed, limit=1000)
    print(f"   Retrieved {len(events)} events")

    if not events:
        print("❌ No events retrieved from API")
        return 0

    # Filter for up/down markets
    print(f"\n→ Filtering for up/down markets...")
    updown_markets = []

    for event in events:
        # Check if up/down market
        if not is_updown_market(event):
            continue

        # Extract asset
        asset = extract_asset(event)
        if not asset or asset not in config.ASSETS:
            continue

        # Extract duration
        slug = event.get('slug', '')
        duration = extract_duration(slug)
        if not duration or duration not in config.DURATIONS:
            continue

        # Extract structured data
        try:
            market_data = extract_market_data(event, asset, duration)
            if market_data.get('market_id'):  # Ensure we have a market ID
                updown_markets.append(market_data)
        except Exception as e:
            print(f"   ⚠️ Error processing event {event.get('id')}: {e}")
            continue

    print(f"   Found {len(updown_markets)} up/down markets")

    if not updown_markets:
        print("❌ No up/down markets found")
        return 0

    # Convert to DataFrame (with schema overrides for large token IDs)
    df = pl.DataFrame(updown_markets, schema_overrides={
        'yes_token_id': pl.Utf8,
        'no_token_id': pl.Utf8
    })

    # Summary by asset and duration
    print(f"\n→ Markets by asset:")
    asset_counts = df.group_by('asset').agg(pl.len().alias('count')).sort('asset')
    for row in asset_counts.iter_rows(named=True):
        print(f"   {row['asset']}: {row['count']}")

    print(f"\n→ Markets by duration:")
    duration_counts = df.group_by('duration').agg(pl.len().alias('count')).sort('duration')
    for row in duration_counts.iter_rows(named=True):
        print(f"   {row['duration']}: {row['count']}")

    # Save to CSV
    print(f"\n→ Saving to {config.UPDOWN_MARKETS}...")
    df.write_csv(config.UPDOWN_MARKETS)

    print(f"\n✅ Stage 1 complete: {len(updown_markets)} markets discovered")
    print("="*70 + "\n")

    return len(updown_markets)


if __name__ == "__main__":
    # Test standalone
    count = discover_updown_markets()
    print(f"\nDiscovered {count} markets")
