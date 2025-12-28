"""
Stage 2B: Fetch CLOB Trades
Query CLOB API for markets not in historical data.
"""
import polars as pl
import requests
import time
from typing import List, Dict, Any, Optional

from . import config


def fetch_market_trades_from_clob(market_id: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Fetch trades for a specific market from CLOB API

    Args:
        market_id: Market ID to fetch trades for
        limit: Maximum number of trades to fetch

    Returns:
        List of trade dictionaries
    """
    url = f"{config.CLOB_API_BASE}/trades"
    params = {
        "market": market_id,
        "limit": limit
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # CLOB API might return trades in different format
        # Adjust based on actual API response
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'data' in data:
            return data['data']
        else:
            return []

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            # Market not found or no trades
            return []
        else:
            print(f"   ⚠️ HTTP error for market {market_id}: {e}")
            return []
    except Exception as e:
        print(f"   ⚠️ Error fetching trades for market {market_id}: {e}")
        return []


def normalize_clob_trade(trade: Dict[str, Any], market_id: str) -> Dict[str, Any]:
    """
    Normalize CLOB API trade format to match trades.csv schema

    Args:
        trade: Raw trade from CLOB API
        market_id: Market ID

    Returns:
        Normalized trade dictionary
    """
    # CLOB API trade format (adjust based on actual response):
    # {
    #   "id": "...",
    #   "market": "...",
    #   "asset_id": "...",
    #   "maker_address": "...",
    #   "taker_address": "...",
    #   "price": "0.5",
    #   "size": "10",
    #   "side": "BUY",
    #   "timestamp": "2025-12-28T01:45:00Z",
    #   "hash": "0x..."
    # }

    return {
        'timestamp': trade.get('timestamp'),
        'market_id': market_id,
        'maker': trade.get('maker_address', trade.get('maker', '')),
        'taker': trade.get('taker_address', trade.get('taker', '')),
        'nonusdc_side': trade.get('asset_id', ''),  # token1 or token2
        'maker_direction': trade.get('maker_orders', [{}])[0].get('side', '') if 'maker_orders' in trade else '',
        'taker_direction': trade.get('side', ''),
        'price': float(trade.get('price', 0)),
        'usd_amount': float(trade.get('size', 0)) * float(trade.get('price', 0)),
        'token_amount': float(trade.get('size', 0)),
        'transactionHash': trade.get('transaction_hash', trade.get('hash', ''))
    }


def fetch_clob_trades_for_new_markets() -> int:
    """
    Query CLOB API for markets not in historical trades

    Returns:
        Number of new trades fetched
    """
    print("\n" + "="*70)
    print("STAGE 2B: FETCH CLOB TRADES (NEW MARKETS)")
    print("="*70)

    # Check if files exist
    if not config.UPDOWN_MARKETS.exists():
        print(f"❌ Markets file not found: {config.UPDOWN_MARKETS}")
        return 0

    if not config.UPDOWN_TRADES_HISTORICAL.exists():
        print(f"❌ Historical trades file not found: {config.UPDOWN_TRADES_HISTORICAL}")
        print("   Run Stage 2A first")
        return 0

    # Load data
    print(f"\n→ Loading market and trade data...")
    all_markets = pl.read_csv(config.UPDOWN_MARKETS, schema_overrides={
        'yes_token_id': pl.Utf8,
        'no_token_id': pl.Utf8
    })
    historical_trades = pl.read_csv(config.UPDOWN_TRADES_HISTORICAL)

    # Identify markets with no trades
    if len(historical_trades) == 0:
        new_market_ids = all_markets['market_id'].unique().to_list()
    else:
        historical_market_ids = historical_trades['market_id'].unique().to_list()
        new_markets = all_markets.filter(
            ~pl.col('market_id').is_in(historical_market_ids)
        )
        new_market_ids = new_markets['market_id'].to_list()

    # Remove None values
    new_market_ids = [mid for mid in new_market_ids if mid is not None]

    if not new_market_ids:
        print("\n✅ No new markets to fetch from CLOB API")
        print("   All markets already have historical trades")
        print("="*70 + "\n")
        return 0

    print(f"   Found {len(new_market_ids)} markets without historical trades")
    print(f"\n→ Fetching trades from CLOB API...")
    print(f"   (Rate limited: {config.API_DELAY}s between requests)")

    # Fetch trades for each new market
    all_new_trades = []
    markets_with_trades = 0

    for i, market_id in enumerate(new_market_ids, 1):
        print(f"   [{i}/{len(new_market_ids)}] Fetching market {market_id}...", end=" ")

        trades = fetch_market_trades_from_clob(market_id)

        if trades:
            normalized = [normalize_clob_trade(t, market_id) for t in trades]
            all_new_trades.extend(normalized)
            markets_with_trades += 1
            print(f"✓ {len(trades)} trades")
        else:
            print("○ no trades")

        # Rate limiting
        if i < len(new_market_ids):
            time.sleep(config.API_DELAY)

    print(f"\n→ Summary:")
    print(f"   Markets queried: {len(new_market_ids)}")
    print(f"   Markets with trades: {markets_with_trades}")
    print(f"   Total new trades: {len(all_new_trades):,}")

    if not all_new_trades:
        print("\n✅ Stage 2B complete: No new trades found")
        print("="*70 + "\n")
        return 0

    # Convert to DataFrame and append
    print(f"\n→ Appending to {config.UPDOWN_TRADES_HISTORICAL.name}...")
    new_df = pl.DataFrame(all_new_trades)

    # Append to existing file
    try:
        # Read existing, concatenate, write
        existing = pl.read_csv(config.UPDOWN_TRADES_HISTORICAL)
        combined = pl.concat([existing, new_df])
        combined.write_csv(config.UPDOWN_TRADES_HISTORICAL)

        print(f"   Total trades now: {len(combined):,}")

    except Exception as e:
        print(f"❌ Error appending trades: {e}")
        return 0

    print(f"\n✅ Stage 2B complete: {len(all_new_trades):,} new trades added")
    print("="*70 + "\n")

    return len(all_new_trades)


if __name__ == "__main__":
    # Test standalone
    count = fetch_clob_trades_for_new_markets()
    print(f"\nFetched {count:,} new trades from CLOB API")
