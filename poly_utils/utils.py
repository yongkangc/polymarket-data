import ast
import requests
from datetime import datetime
from operator import itemgetter
from dateutil.parser import isoparse
from zoneinfo import ZoneInfo
import polars as pl
import os

# Cached result so we don't hit the API 10 times on import
_cached_market = None
_cache_time = None
CACHE_SECONDS = 300  # 5 minutes

def get_markets():
    """Load markets.csv and return as polars DataFrame"""
    markets_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw", "markets.csv")
    return pl.read_csv(
        markets_path,
        schema_overrides={"token1": pl.String, "token2": pl.String}
    )

def update_missing_tokens():
    """Placeholder function for updating missing tokens"""
    pass

def get_ids() -> dict:
    """
    Returns the soonest-ending active BTC 15-minute market on Polymarket.

    Returns:
        dict with keys:
            - market_id: Event ID
            - yes_token_id: Token ID for YES outcome
            - no_token_id: Token ID for NO outcome
    """
    global _cached_market, _cache_time

    # Return cache if still fresh
    nyc_tz = ZoneInfo("America/New_York")
    now = datetime.now(nyc_tz)

    if _cached_market and _cache_time and (now - _cache_time).seconds < CACHE_SECONDS:
        return _cached_market

    # Fetch BTC 15-minute markets (tag_id=102467)
    url = "https://gamma-api.polymarket.com/events?tag_id=102467&limit=20&closed=false"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    events = resp.json()

    # Filter BTC markets and sort by ticker
    btc_events = sorted(
        (d for d in events if d.get("ticker", "").lower().startswith("btc")),
        key=itemgetter("ticker")
    )

    # Find soonest ending market that hasn't ended yet
    soonest_market = min(
        (d for d in btc_events if isoparse(d["endDate"]) > now),
        key=lambda d: isoparse(d["endDate"]),
        default=None
    )

    if not soonest_market:
        raise ValueError("No active BTC 15-minute markets found")

    # Extract token IDs
    raw_token_ids = soonest_market["markets"][0]["clobTokenIds"]
    token_ids = ast.literal_eval(raw_token_ids)
    yes_token_id, no_token_id = token_ids[0], token_ids[1]
    market_id = soonest_market["id"]

    result = {
        "market_id": market_id,
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id
    }

    # Cache the result
    _cached_market = result
    _cache_time = now

    return result
