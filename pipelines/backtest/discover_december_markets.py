"""
Discover December 2024 up/down markets for BTC, ETH, SOL, XRP
Outputs: data/december_markets.csv

This script searches both:
1. The existing markets.csv file for historical markets
2. The Polymarket API for any additional markets
"""
import re
import polars as pl
import requests
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List


# Configuration
ASSETS = ["BTC", "ETH", "SOL", "XRP"]
DURATIONS = ["15m", "1h"]
ASSET_PATTERNS = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "eth"],
    "SOL": ["solana", "sol"],
    "XRP": ["xrp", "ripple"],
}

BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_MARKETS = BASE_DIR / "data" / "raw" / "markets.csv"
OUTPUT_FILE = DATA_DIR / "december_markets.csv"


def extract_duration(ticker: str, question: str = "") -> Optional[str]:
    """Extract duration from ticker or question"""
    combined = f"{ticker} {question}".lower()
    
    # Check for 15m first (more specific)
    if re.search(r'-15m-|15\s*min|15-minute', combined):
        return "15m"
    # Check for 1h/hourly
    elif re.search(r'-1h-|1\s*hour|hourly', combined):
        return "1h"
    # Check for 5m (exclude from our analysis)
    elif re.search(r'-5m-|5\s*min', combined):
        return "5m"
    
    return None


def extract_asset(ticker: str, question: str = "") -> Optional[str]:
    """Extract asset from ticker or question"""
    combined = f"{ticker} {question}".lower()
    
    for asset, patterns in ASSET_PATTERNS.items():
        if any(pattern in combined for pattern in patterns):
            return asset
    
    return None


def is_december_market(question: str, ticker: str) -> bool:
    """Check if market is from December"""
    combined = f"{question} {ticker}".lower()
    return "december" in combined


def extract_unix_timestamp(ticker: str) -> Optional[int]:
    """Extract Unix timestamp from ticker if present"""
    # Pattern: btc-updown-15m-1764565200
    match = re.search(r'-(\d{10})$', ticker)
    if match:
        return int(match.group(1))
    return None


def parse_closed_time(closed_time_str: str) -> Optional[datetime]:
    """Parse closedTime string to datetime"""
    if not closed_time_str or closed_time_str == '':
        return None
    try:
        # Try ISO format
        return datetime.fromisoformat(closed_time_str.replace('Z', '+00:00'))
    except:
        return None


def discover_from_markets_csv() -> pl.DataFrame:
    """Discover December markets from existing markets.csv"""
    print(f"→ Loading markets from {RAW_MARKETS}...")
    
    # Read with all columns as strings to avoid schema issues
    df = pl.read_csv(RAW_MARKETS, infer_schema_length=0)
    print(f"  Total markets in file: {len(df):,}")
    
    # Filter for up/down markets
    df = df.filter(
        pl.col('ticker').str.to_lowercase().str.contains('updown') |
        pl.col('question').str.to_lowercase().str.contains('up or down')
    )
    print(f"  Up/down markets: {len(df):,}")
    
    # Filter for December
    df = df.filter(
        pl.col('question').str.to_lowercase().str.contains('december') |
        pl.col('ticker').str.to_lowercase().str.contains('december')
    )
    print(f"  December markets: {len(df):,}")
    
    # Process each market
    markets = []
    for row in df.iter_rows(named=True):
        ticker = row.get('ticker', '') or ''
        question = row.get('question', '') or ''
        
        # Extract asset
        asset = extract_asset(ticker, question)
        if not asset or asset not in ASSETS:
            continue
        
        # Extract duration
        duration = extract_duration(ticker, question)
        if not duration or duration not in DURATIONS:
            continue
        
        # Extract timestamps
        unix_ts = extract_unix_timestamp(ticker)
        closed_time = parse_closed_time(row.get('closedTime', ''))
        
        # Calculate start/end times
        if unix_ts:
            start_time = unix_ts
            if duration == "15m":
                end_time = unix_ts + 900  # 15 minutes
            elif duration == "1h":
                end_time = unix_ts + 3600  # 1 hour
            else:
                end_time = unix_ts + 300  # 5 minutes (fallback)
        elif closed_time:
            end_time = int(closed_time.timestamp())
            if duration == "15m":
                start_time = end_time - 900
            elif duration == "1h":
                start_time = end_time - 3600
            else:
                start_time = end_time - 300
        else:
            start_time = None
            end_time = None
        
        markets.append({
            'market_id': row.get('id'),
            'ticker': ticker,
            'asset': asset,
            'duration': duration,
            'question': question,
            'start_time': start_time,
            'end_time': end_time,
            'yes_token_id': row.get('token1'),
            'no_token_id': row.get('token2'),
            'volume': float(row.get('volume', 0) or 0),
            'closed': row.get('closedTime') is not None and row.get('closedTime') != '',
        })
    
    return pl.DataFrame(markets)


def discover_from_api() -> pl.DataFrame:
    """Discover additional markets from Polymarket API"""
    print(f"\n→ Querying Polymarket API for hourly markets...")
    
    # API only returns recent/active markets, so this may not find many
    url = "https://gamma-api.polymarket.com/events"
    
    try:
        response = requests.get(url, params={
            "limit": 500,
            "closed": "true",
            "order": "new"
        }, timeout=30)
        response.raise_for_status()
        events = response.json()
        print(f"  Retrieved {len(events)} events from API")
    except Exception as e:
        print(f"  ⚠ API error: {e}")
        return pl.DataFrame()
    
    markets = []
    for event in events:
        title = (event.get('title') or '').lower()
        slug = (event.get('slug') or '').lower()
        
        # Check for up/down market
        if not ('up' in title and 'down' in title):
            continue
        
        # Check for December
        if 'december' not in title and 'december' not in slug:
            continue
        
        # Extract asset and duration
        combined = f"{title} {slug}"
        asset = extract_asset(combined, '')
        duration = extract_duration(slug, title)
        
        if not asset or asset not in ASSETS:
            continue
        if not duration or duration not in DURATIONS:
            continue
        
        # Get market details
        event_markets = event.get('markets', [])
        if not event_markets:
            continue
        
        market = event_markets[0]
        
        # Extract token IDs
        token_ids = market.get('clobTokenIds', '[]')
        try:
            import ast
            tokens = ast.literal_eval(token_ids) if isinstance(token_ids, str) else token_ids
            yes_token = str(tokens[0]) if len(tokens) > 0 else None
            no_token = str(tokens[1]) if len(tokens) > 1 else None
        except:
            yes_token = None
            no_token = None
        
        markets.append({
            'market_id': market.get('id'),
            'ticker': event.get('slug'),
            'asset': asset,
            'duration': duration,
            'question': market.get('question') or event.get('title'),
            'start_time': None,  # API doesn't always provide this
            'end_time': None,
            'yes_token_id': yes_token,
            'no_token_id': no_token,
            'volume': float(market.get('volume', 0) or 0),
            'closed': market.get('closed', False),
        })
    
    return pl.DataFrame(markets) if markets else pl.DataFrame()


def main():
    """Main discovery function"""
    print("=" * 70)
    print("DECEMBER MARKET DISCOVERY")
    print(f"Assets: {', '.join(ASSETS)}")
    print(f"Durations: {', '.join(DURATIONS)}")
    print("=" * 70)
    
    # Discover from markets.csv
    df_csv = discover_from_markets_csv()
    print(f"\n  Markets from CSV: {len(df_csv):,}")
    
    # Discover from API
    df_api = discover_from_api()
    print(f"  Markets from API: {len(df_api):,}")
    
    # Combine and deduplicate
    if len(df_csv) > 0 and len(df_api) > 0:
        df = pl.concat([df_csv, df_api])
        df = df.unique(subset=['market_id'])
    elif len(df_csv) > 0:
        df = df_csv
    elif len(df_api) > 0:
        df = df_api
    else:
        print("\n❌ No markets found!")
        return 0
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    print(f"\n→ Total unique markets: {len(df):,}")
    
    print(f"\n→ By asset:")
    asset_counts = df.group_by('asset').agg(pl.len().alias('count')).sort('asset')
    for row in asset_counts.iter_rows(named=True):
        print(f"   {row['asset']}: {row['count']:,}")
    
    print(f"\n→ By duration:")
    duration_counts = df.group_by('duration').agg(pl.len().alias('count')).sort('duration')
    for row in duration_counts.iter_rows(named=True):
        print(f"   {row['duration']}: {row['count']:,}")
    
    print(f"\n→ By asset x duration:")
    cross = df.group_by(['asset', 'duration']).agg(pl.len().alias('count')).sort(['asset', 'duration'])
    for row in cross.iter_rows(named=True):
        print(f"   {row['asset']} {row['duration']}: {row['count']:,}")
    
    # Save
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.write_csv(OUTPUT_FILE)
    print(f"\n✅ Saved to {OUTPUT_FILE}")
    
    return len(df)


if __name__ == "__main__":
    count = main()
    print(f"\nDiscovered {count} December markets")

