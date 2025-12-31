"""
Fetch December trades from CLOB API for backtest markets.
Since trades.csv only goes to Oct 2025, we need to fetch December data.

This script fetches trades for a sample of December markets to enable backtesting.
"""
import polars as pl
import requests
import time
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional


BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
MARKETS_FILE = DATA_DIR / "december_markets.csv"
OUTPUT_FILE = DATA_DIR / "december_trades.parquet"

CLOB_API_BASE = "https://clob.polymarket.com"
API_DELAY = 0.3  # seconds between requests


def fetch_market_trades(token_id: str, limit: int = 500) -> List[Dict[str, Any]]:
    """Fetch trades for a specific token from CLOB API"""
    url = f"{CLOB_API_BASE}/trades"
    params = {
        "asset_id": token_id,
        "limit": limit
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else []
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return []
        return []
    except Exception as e:
        return []


def normalize_trade(trade: Dict[str, Any], market_info: Dict[str, Any], token_side: str) -> Dict[str, Any]:
    """Normalize CLOB trade to standard format"""
    try:
        price = float(trade.get('price', 0))
        size = float(trade.get('size', 0))
        
        # Parse timestamp
        ts_str = trade.get('match_time') or trade.get('timestamp') or trade.get('created_at')
        if ts_str:
            try:
                ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            except:
                ts = datetime.now()
        else:
            ts = datetime.now()
        
        return {
            'timestamp': ts,
            'market_id': market_info['market_id'],
            'asset': market_info['asset'],
            'duration': market_info['duration'],
            'start_time': market_info.get('start_time'),
            'end_time': market_info.get('end_time'),
            'nonusdc_side': token_side,
            'side': trade.get('side', 'UNKNOWN'),  # BUY or SELL from taker perspective
            'price': price,
            'usd_amount': price * size,
            'token_amount': size,
            'maker': trade.get('maker_address', ''),
            'taker': trade.get('taker_address', ''),
            'transaction_hash': trade.get('transaction_hash', ''),
        }
    except Exception as e:
        return None


def fetch_all_december_trades(max_markets: int = None, assets: List[str] = None):
    """
    Fetch trades for December markets from CLOB API
    
    Args:
        max_markets: Limit number of markets to process (for testing)
        assets: Filter to specific assets (e.g., ['BTC', 'ETH'])
    """
    print("=" * 70)
    print("FETCH DECEMBER TRADES FROM CLOB API")
    print("=" * 70)
    
    # Load markets
    print(f"\n→ Loading markets from {MARKETS_FILE}...")
    markets = pl.read_csv(MARKETS_FILE, infer_schema_length=0)
    print(f"  Total markets: {len(markets):,}")
    
    # Filter by assets if specified
    if assets:
        markets = markets.filter(pl.col('asset').is_in(assets))
        print(f"  Filtered to {assets}: {len(markets):,}")
    
    # Limit markets if specified
    if max_markets:
        markets = markets.head(max_markets)
        print(f"  Limited to first {max_markets}: {len(markets):,}")
    
    # Fetch trades for each market
    all_trades = []
    markets_with_trades = 0
    total_fetched = 0
    
    print(f"\n→ Fetching trades from CLOB API...")
    print(f"  Processing {len(markets)} markets...")
    
    for i, row in enumerate(markets.iter_rows(named=True), 1):
        market_id = row['market_id']
        yes_token = row['yes_token_id']
        no_token = row['no_token_id']
        
        market_info = {
            'market_id': market_id,
            'asset': row['asset'],
            'duration': row['duration'],
            'start_time': row.get('start_time'),
            'end_time': row.get('end_time'),
        }
        
        # Fetch YES token trades
        yes_trades = []
        if yes_token:
            raw_trades = fetch_market_trades(yes_token)
            yes_trades = [normalize_trade(t, market_info, 'token1') for t in raw_trades]
            yes_trades = [t for t in yes_trades if t is not None]
        
        time.sleep(API_DELAY)
        
        # Fetch NO token trades  
        no_trades = []
        if no_token:
            raw_trades = fetch_market_trades(no_token)
            no_trades = [normalize_trade(t, market_info, 'token2') for t in raw_trades]
            no_trades = [t for t in no_trades if t is not None]
        
        time.sleep(API_DELAY)
        
        # Combine
        market_trades = yes_trades + no_trades
        if market_trades:
            all_trades.extend(market_trades)
            markets_with_trades += 1
            total_fetched += len(market_trades)
        
        # Progress
        if i % 50 == 0 or i == len(markets):
            print(f"  [{i}/{len(markets)}] Markets processed, {total_fetched:,} trades fetched")
    
    print(f"\n→ Summary:")
    print(f"  Markets queried: {len(markets)}")
    print(f"  Markets with trades: {markets_with_trades}")
    print(f"  Total trades: {len(all_trades):,}")
    
    if not all_trades:
        print("\n❌ No trades found!")
        return 0
    
    # Convert to DataFrame
    print(f"\n→ Converting to DataFrame...")
    df = pl.DataFrame(all_trades)
    
    # Sort by timestamp
    df = df.sort('timestamp')
    
    # Summary stats
    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    
    print(f"\n→ Total trades: {len(df):,}")
    
    print(f"\n→ By asset:")
    asset_counts = df.group_by('asset').agg(pl.len().alias('count')).sort('asset')
    for row in asset_counts.iter_rows(named=True):
        print(f"   {row['asset']}: {row['count']:,}")
    
    print(f"\n→ By side (taker perspective):")
    side_counts = df.group_by('side').agg(pl.len().alias('count'))
    for row in side_counts.iter_rows(named=True):
        print(f"   {row['side']}: {row['count']:,}")
    
    print(f"\n→ Date range:")
    print(f"   Min: {df['timestamp'].min()}")
    print(f"   Max: {df['timestamp'].max()}")
    
    if 'usd_amount' in df.columns:
        total_vol = df['usd_amount'].sum()
        print(f"\n→ Total volume: ${total_vol:,.2f}")
    
    # Save
    print(f"\n→ Saving to {OUTPUT_FILE}...")
    df.write_parquet(OUTPUT_FILE)
    
    file_size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    print(f"  File size: {file_size_mb:.1f} MB")
    
    print(f"\n✅ Done!")
    return len(df)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fetch December trades from CLOB API')
    parser.add_argument('--max-markets', type=int, default=None, help='Limit markets to process')
    parser.add_argument('--assets', nargs='+', default=None, help='Filter to specific assets')
    
    args = parser.parse_args()
    
    count = fetch_all_december_trades(
        max_markets=args.max_markets,
        assets=args.assets
    )
    print(f"\nFetched {count:,} trades")

