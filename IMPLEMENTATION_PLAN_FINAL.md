# Up/Down Market Pipeline - Final Implementation Plan

## Confirmed Decisions

1. ✅ **Trade data:** Both historical trades.csv + CLOB API
2. ✅ **Durations:** All (5min, 15min, 1hr)
3. ✅ **Binance data:** `binance_complete_minute_data.csv` (single file, all assets)
4. ✅ **Execution:** Checkpoint system - Phase 1 → Phase 2 with resume capability
5. ✅ **Output:** `updown_trades_enriched.csv` (more descriptive name)

---

## Pipeline Architecture with Checkpoints

```
┌─────────────────────────────────────────────────────────────┐
│                         PHASE 1: Historical                  │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────────────────────────────────┐            │
│  │ Stage 1: Discover Markets                    │            │
│  │ - Query Polymarket API                       │            │
│  │ - Filter up/down (BTC/SOL/ETH, all durations)│            │
│  │ - Save: updown_markets.csv                   │            │
│  │ - Checkpoint: .checkpoints/markets.done      │            │
│  └──────────────────────────────────────────────┘            │
│                      ↓                                        │
│  ┌──────────────────────────────────────────────┐            │
│  │ Stage 2: Fetch Historical Trades             │            │
│  │ - Filter processed/trades.csv by market_ids  │            │
│  │ - Save: updown_trades_historical.csv         │            │
│  │ - Checkpoint: .checkpoints/historical.done   │            │
│  └──────────────────────────────────────────────┘            │
│                      ↓                                        │
│  ┌──────────────────────────────────────────────┐            │
│  │ Stage 3: Integrate Binance Prices            │            │
│  │ - Load binance_complete_minute_data.csv      │            │
│  │ - Join with trades (time-series)             │            │
│  │ - Calculate: move%, time_remaining           │            │
│  │ - Save: updown_trades_enriched.csv           │            │
│  │ - Checkpoint: .checkpoints/enriched.done     │            │
│  └──────────────────────────────────────────────┘            │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                      ↓ (if all checkpoints exist)
┌─────────────────────────────────────────────────────────────┐
│                         PHASE 2: Real-Time                   │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────────────────────────────────┐            │
│  │ Every 15 minutes:                            │            │
│  │ - Check for new up/down markets              │            │
│  │ - Fetch trades via CLOB API                  │            │
│  │ - Append to dataset                          │            │
│  └──────────────────────────────────────────────┘            │
│                      ↓                                        │
│  ┌──────────────────────────────────────────────┐            │
│  │ Every 60 seconds:                            │            │
│  │ - Poll CLOB API for new trades (active mkts) │            │
│  │ - Fetch latest Binance prices                │            │
│  │ - Enrich and append to CSV                   │            │
│  └──────────────────────────────────────────────┘            │
│                                                               │
└─────────────────────────────────────────────────────────────┘

CHECK BEFORE START:
├─ If .checkpoints/enriched.done exists AND recent:
│     → Skip Phase 1, go directly to Phase 2
├─ If partial checkpoints:
│     → Resume from last completed stage
└─ If no checkpoints:
      → Run full Phase 1 from scratch
```

---

## File Structure

```
poly_data/
├── updown_pipeline/
│   ├── __init__.py
│   ├── config.py                    # Configuration
│   ├── checkpoint.py                # Checkpoint manager
│   ├── market_discovery.py          # Stage 1
│   ├── fetch_historical_trades.py   # Stage 2A: Filter trades.csv
│   ├── fetch_clob_trades.py         # Stage 2B: Query CLOB API
│   ├── integrate_binance.py         # Stage 3
│   ├── stream_live.py               # Phase 2: Real-time
│   └── run_pipeline.py              # Main orchestrator
│
├── data/
│   ├── .checkpoints/
│   │   ├── markets.done             # JSON with timestamp + metadata
│   │   ├── historical.done          # JSON with timestamp + metadata
│   │   ├── enriched.done            # JSON with timestamp + metadata
│   │   └── last_update.json         # For incremental updates
│   │
│   ├── updown_markets.csv           # Discovered markets
│   ├── updown_trades_historical.csv # Historical trades (Stage 2)
│   ├── updown_trades_enriched.csv   # Final output ⭐
│   └── binance_complete_minute_data.csv
│
└── (existing files...)
```

---

## Checkpoint System

### Checkpoint Format
```json
{
  "stage": "market_discovery",
  "completed_at": "2025-12-28T02:30:00Z",
  "status": "success",
  "metadata": {
    "markets_found": 1247,
    "assets": ["BTC", "SOL", "ETH"],
    "durations": ["5m", "15m", "1h"],
    "output_file": "data/updown_markets.csv"
  }
}
```

### Pipeline Logic
```python
def run_pipeline():
    checkpoints = CheckpointManager()

    # Check Phase 1 completion
    if checkpoints.all_phase1_complete():
        if checkpoints.is_recent(hours=1):
            print("✅ Phase 1 already complete and recent")
            print("→ Starting Phase 2 (real-time streaming)")
            run_phase2()
            return
        else:
            print("⚠️ Phase 1 complete but outdated (>1hr)")
            print("→ Re-running incremental update")
            # Fall through to Phase 1

    # Run Phase 1 stages
    if not checkpoints.exists('markets'):
        print("→ Stage 1: Discovering markets...")
        discover_markets()
        checkpoints.mark_done('markets')

    if not checkpoints.exists('historical'):
        print("→ Stage 2: Fetching historical trades...")
        fetch_historical_trades()
        checkpoints.mark_done('historical')

    if not checkpoints.exists('enriched'):
        print("→ Stage 3: Integrating Binance prices...")
        integrate_binance()
        checkpoints.mark_done('enriched')

    print("✅ Phase 1 complete!")
    print("→ Starting Phase 2 (real-time streaming)")
    run_phase2()
```

---

## Configuration

### `config.py`
```python
from pathlib import Path

# Assets and durations to track
ASSETS = ["BTC", "SOL", "ETH"]
DURATIONS = ["5m", "15m", "1h"]  # All durations

# Asset name mappings
ASSET_PATTERNS = {
    "BTC": ["bitcoin", "btc"],
    "SOL": ["solana", "sol"],
    "ETH": ["ethereum", "eth"]
}

DURATION_PATTERNS = {
    "5m": ["5m", "5 min", "five minute"],
    "15m": ["15m", "15 min", "fifteen minute"],
    "1h": ["1h", "1 hour", "one hour"]
}

# File paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
CHECKPOINT_DIR = DATA_DIR / ".checkpoints"

# Input files
EXISTING_TRADES = DATA_DIR.parent / "processed" / "trades.csv"
EXISTING_MARKETS = DATA_DIR.parent / "markets.csv"
BINANCE_DATA = DATA_DIR / "binance_complete_minute_data.csv"

# Output files
UPDOWN_MARKETS = DATA_DIR / "updown_markets.csv"
UPDOWN_TRADES_HISTORICAL = DATA_DIR / "updown_trades_historical.csv"
UPDOWN_TRADES_ENRICHED = DATA_DIR / "updown_trades_enriched.csv"

# API settings
POLYMARKET_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

# Streaming settings
MARKET_CHECK_INTERVAL = 15 * 60  # 15 minutes
TRADE_POLL_INTERVAL = 60         # 60 seconds
CHECKPOINT_FRESHNESS = 1 * 60 * 60  # 1 hour
```

---

## Stage Details

### Stage 1: Market Discovery
```python
# market_discovery.py

def discover_markets():
    """
    Query Polymarket API and filter for up/down markets.
    Output: updown_markets.csv
    """

    # 1. Query API for active events
    events = fetch_all_events(closed=False, limit=1000)

    # 2. Filter for up/down markets
    updown_markets = []
    for event in events:
        title = (event.get('title') or '').lower()
        question = (event.get('question') or '').lower()
        slug = (event.get('slug') or '').lower()

        # Check if up/down market
        if not (('up' in title or 'up' in question) and
                ('down' in title or 'down' in question)):
            continue

        # Check asset
        asset = None
        for a, patterns in ASSET_PATTERNS.items():
            if any(p in title or p in question or p in slug
                   for p in patterns):
                asset = a
                break

        if not asset:
            continue

        # Extract duration from slug
        duration = extract_duration(slug)
        if duration not in DURATIONS:
            continue

        # Extract market data
        market_data = extract_market_data(event, asset, duration)
        updown_markets.append(market_data)

    # 3. Save to CSV
    df = pl.DataFrame(updown_markets)
    df.write_csv(UPDOWN_MARKETS)

    return len(updown_markets)
```

### Stage 2A: Fetch Historical Trades
```python
# fetch_historical_trades.py

def fetch_historical_trades():
    """
    Filter existing trades.csv by up/down market IDs.
    Output: updown_trades_historical.csv
    """

    # 1. Load market IDs
    markets = pl.read_csv(UPDOWN_MARKETS)
    market_ids = markets['market_id'].to_list()

    print(f"Filtering {len(market_ids)} markets from trades.csv...")

    # 2. Filter trades.csv (streaming for memory efficiency)
    trades = (
        pl.scan_csv(EXISTING_TRADES)
        .filter(pl.col('market_id').is_in(market_ids))
        .collect(streaming=True)
    )

    print(f"Found {len(trades):,} historical trades")

    # 3. Save
    trades.write_csv(UPDOWN_TRADES_HISTORICAL)

    return len(trades)
```

### Stage 2B: Fetch CLOB Trades (for new markets)
```python
# fetch_clob_trades.py

def fetch_clob_trades_for_new_markets():
    """
    Query CLOB API for markets not in historical trades.
    Append to: updown_trades_historical.csv
    """

    # 1. Identify new markets
    all_markets = pl.read_csv(UPDOWN_MARKETS)
    historical_trades = pl.read_csv(UPDOWN_TRADES_HISTORICAL)
    historical_market_ids = historical_trades['market_id'].unique().to_list()

    new_markets = all_markets.filter(
        ~pl.col('market_id').is_in(historical_market_ids)
    )

    if len(new_markets) == 0:
        print("No new markets to fetch from CLOB API")
        return 0

    print(f"Fetching trades for {len(new_markets)} new markets from CLOB API...")

    # 2. Fetch trades for each new market
    new_trades = []
    for market_id in new_markets['market_id']:
        trades = fetch_clob_trades(market_id)
        new_trades.extend(trades)
        time.sleep(0.5)  # Rate limiting

    # 3. Append to historical
    if new_trades:
        new_df = pl.DataFrame(new_trades)

        # Append to existing file
        with open(UPDOWN_TRADES_HISTORICAL, 'ab') as f:
            new_df.write_csv(f, include_header=False)

    return len(new_trades)
```

### Stage 3: Integrate Binance Prices
```python
# integrate_binance.py

def integrate_binance_prices():
    """
    Join trades with Binance prices, calculate features.
    Output: updown_trades_enriched.csv
    """

    print("Loading data...")

    # 1. Load trades
    trades = pl.read_csv(UPDOWN_TRADES_HISTORICAL)

    # 2. Load markets (for metadata)
    markets = pl.read_csv(UPDOWN_MARKETS)

    # 3. Load Binance data
    binance = pl.read_csv(BINANCE_DATA)

    # Ensure timestamp is in seconds
    if 'timestamp' not in binance.columns:
        # Convert datetime string to timestamp if needed
        binance = binance.with_columns([
            pl.col('timestamp').str.strptime(pl.Datetime)
            .dt.timestamp().alias('timestamp')
        ])

    print("Joining trades with market metadata...")

    # 4. Join trades with market info
    trades_with_markets = trades.join(
        markets.select(['market_id', 'asset', 'duration',
                       'event_start_time', 'end_time', 'question']),
        on='market_id',
        how='left'
    )

    print("Joining with Binance prices...")

    # 5. For each asset, join with corresponding Binance prices
    enriched_parts = []

    for asset in ASSETS:
        asset_trades = trades_with_markets.filter(pl.col('asset') == asset)

        if len(asset_trades) == 0:
            continue

        # Map asset to Binance symbol
        symbol_map = {
            'BTC': 'BTCUSDT',
            'SOL': 'SOLUSDT',
            'ETH': 'ETHUSDT'
        }
        symbol = symbol_map[asset]

        # Filter Binance data for this symbol
        asset_prices = binance.filter(pl.col('symbol') == symbol)

        # Join: trade time → asset price
        enriched = asset_trades.join_asof(
            asset_prices.select(['timestamp', 'close']),
            left_on='trade_ts_sec',
            right_on='timestamp',
            strategy='nearest'
        ).rename({'close': 'asset_price_at_trade'})

        # Join: market open time → market open price
        market_opens = markets.filter(pl.col('asset') == asset)
        market_opens_with_price = market_opens.join_asof(
            asset_prices.select(['timestamp', 'close']),
            left_on='event_start_time',
            right_on='timestamp',
            strategy='nearest'
        ).rename({'close': 'market_open_price'})

        enriched = enriched.join(
            market_opens_with_price.select(['market_id', 'market_open_price']),
            on='market_id',
            how='left'
        )

        enriched_parts.append(enriched)

    # 6. Combine all assets
    final = pl.concat(enriched_parts)

    # 7. Calculate features
    final = final.with_columns([
        # Price move %
        ((pl.col('asset_price_at_trade') - pl.col('market_open_price')) /
         pl.col('market_open_price') * 100).alias('move_pct'),

        # Time remaining (seconds)
        (pl.col('end_time') - pl.col('trade_ts_sec')).alias('time_remaining_sec')
    ])

    print(f"Enriched {len(final):,} trades")

    # 8. Save
    final.write_csv(UPDOWN_TRADES_ENRICHED)

    return len(final)
```

---

## Phase 2: Real-Time Streaming

```python
# stream_live.py

def stream_live():
    """
    Continuously update data with new markets and trades.
    """

    print("🔴 LIVE STREAMING MODE")
    print("Press Ctrl+C to stop")

    last_market_check = 0

    while True:
        try:
            current_time = time.time()

            # Every 15 minutes: check for new markets
            if current_time - last_market_check > MARKET_CHECK_INTERVAL:
                print("\n→ Checking for new markets...")
                new_count = discover_and_fetch_new_markets()
                if new_count > 0:
                    print(f"  Found {new_count} new markets!")
                last_market_check = current_time

            # Every 60 seconds: poll for new trades
            print("→ Polling for new trades...")
            trade_count = poll_new_trades()
            if trade_count > 0:
                print(f"  Added {trade_count} new trades")

            time.sleep(TRADE_POLL_INTERVAL)

        except KeyboardInterrupt:
            print("\n\n✅ Streaming stopped")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(10)  # Wait before retry
```

---

## Usage

```bash
# First run (full Phase 1)
cd poly_data/updown_pipeline
python run_pipeline.py

# Subsequent runs (resume or go to Phase 2)
python run_pipeline.py

# Force re-run from scratch
python run_pipeline.py --force-refresh

# Skip to Phase 2 only
python run_pipeline.py --stream-only
```

---

## Output Schema

### `updown_trades_enriched.csv`
```
timestamp              # Trade timestamp (Unix seconds)
market_id              # Polymarket market ID
event_id               # Polymarket event ID
asset                  # BTC / SOL / ETH
duration               # 5m / 15m / 1h
question               # Market question text
maker                  # Maker wallet address
taker                  # Taker wallet address
side                   # token1 or token2 (Up or Down)
direction              # BUY or SELL
price                  # Trade price (0-1)
amount                 # Token amount
usd_amount             # USD value
asset_price_at_trade   # Asset price when trade happened
market_open_price      # Asset price at market open
move_pct               # % move since market opened
time_remaining_sec     # Seconds until market closes
end_time               # Market close timestamp
tx_hash                # Transaction hash
```

---

## Performance Estimates

### Phase 1 (Historical)
- **Stage 1 (Discovery):** ~30 seconds
- **Stage 2A (Filter trades.csv):** ~10-30 minutes (depends on trades.csv size)
- **Stage 2B (CLOB API):** ~1-5 minutes per 100 new markets
- **Stage 3 (Integration):** ~5-15 minutes
- **Total:** ~20-60 minutes

### Phase 2 (Real-Time)
- **CPU:** Low (polling + append)
- **Memory:** ~100-500 MB
- **API calls:** ~100-200/hour
- **Latency:** < 60 seconds for new trades

---

## Next Steps

1. ✅ Create file structure
2. ✅ Implement checkpoint system
3. ✅ Implement Stage 1 (market discovery)
4. ✅ Implement Stage 2A (filter trades.csv)
5. ✅ Implement Stage 2B (CLOB API)
6. ✅ Implement Stage 3 (Binance integration)
7. ✅ Implement Phase 2 (streaming)
8. ✅ Create main orchestrator
9. ⏳ Test and validate
10. ⏳ Document usage

Ready to build! 🚀
