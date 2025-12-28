# Up/Down Market Data Pipeline - Design Plan

## Problem Statement

We need a **simplified, targeted pipeline** for Polymarket up/down markets combined with Binance price data.

### Current Challenges
- Existing `poly_data` pipeline fetches ALL markets (complex, slow, 68GB+ data)
- Need to filter post-facto to find up/down markets
- Want real-time streaming of new trades
- Need integration with Binance minute data

### Target Markets
- **Assets:** BTC, SOL, ETH
- **Durations:** 15-minute and 1-hour markets
- **Format:** Binary up/down prediction markets
- **Examples:**
  - https://polymarket.com/event/sol-updown-15m-1765062900
  - https://polymarket.com/event/ethereum-up-or-down-december-6-3pm-et

---

## Architecture Comparison

### Current poly_data Pipeline (Complex)
```
Polymarket API → markets.csv (ALL markets)
                      ↓
Goldsky Scraper → orderFilled.csv (68GB+ blockchain events)
                      ↓
Process Live → trades.csv (ALL trades)
                      ↓
Post-filter → btc_15min_trades.csv
```
**Issues:** Fetches everything, processes everything, filters later

### Proposed Simplified Pipeline
```
Polymarket API → updown_markets.csv (filtered by asset + duration)
                      ↓
CLOB API / Goldsky → updown_trades.csv (ONLY up/down market trades)
                      ↓
Binance Data → binance_minute_data.csv (already have this)
                      ↓
Integrate → enriched_trades.csv (trades + price moves)
                      ↓
Stream → real-time updates (new trades as they happen)
```
**Benefits:** Only fetch what we need, much faster, simpler code

---

## Key Design Decisions

### 1. Market Discovery Strategy

**Question:** How to identify up/down markets programmatically?

**Options:**
- **A) API Query with Tags/Filters**
  - Polymarket may have tags for up/down markets
  - Query: `gamma-api.polymarket.com/events?tag_id=XXXXX`
  - Need to discover the right tag IDs

- **B) Pattern Matching on Questions**
  - Filter questions containing: `"up" AND "down"`
  - Filter by asset: `"bitcoin|btc"`, `"ethereum|eth"`, `"solana|sol"`
  - Filter by duration: `"15m|15 min"`, `"1h|1 hour"`

- **C) URL Pattern Analysis**
  - Markets follow pattern: `event/{asset}-updown-{duration}-{timestamp}`
  - Could construct URLs directly if we know the schedule

**DECISION:** Use **Pattern Matching (B)** as primary method
- Most reliable
- Doesn't depend on discovering specific tags
- Can validate against URL patterns

### 2. Trade Data Source

**Question:** Where to fetch trades from?

**Options:**
- **A) Goldsky (Blockchain Events)**
  - Pros: Authoritative, complete history
  - Cons: 68GB+ data, complex scraping, slow
  - Used by current poly_data pipeline

- **B) CLOB API (Direct Trade API)**
  - Endpoint: `https://clob.polymarket.com/trades?market={market_id}`
  - Pros: Direct access, can query by market, simpler
  - Cons: Rate limits, may have data gaps

- **C) Hybrid Approach**
  - Historical: Filter existing Goldsky data
  - Real-time: CLOB API or WebSocket

**DECISION:** Use **Hybrid Approach (C)**
- For historical: Filter existing `processed/trades.csv` by market IDs
- For incremental: CLOB API for new markets
- Much faster than re-processing 68GB Goldsky data

### 3. Binance Data Integration

**Question:** How to combine Polymarket trades with Binance prices?

**Approach:**
```python
# We already have: binance_complete_minute_data.csv
# Schema: timestamp, symbol, open, high, low, close, volume

# For each Polymarket trade:
# 1. Get asset price at trade time (join on timestamp)
# 2. Get asset price at market open (market_open_ts = close_ts - duration)
# 3. Calculate move% = (current_price - open_price) / open_price * 100
# 4. Calculate time_remaining = close_ts - trade_ts
```

**DECISION:** Use `polars.join_asof()` for time-series joins (fast, memory efficient)

### 4. Real-Time Streaming

**Question:** How to get new trades as they happen?

**Options:**
- **A) WebSocket Subscription**
  - Real-time feed of trades
  - Pros: Instant updates, efficient
  - Cons: Complex connection handling, need to detect new markets

- **B) Polling CLOB API**
  - Query API every N seconds/minutes
  - Pros: Simple, reliable
  - Cons: Delay, more API calls

- **C) Scheduled Batch Updates**
  - Run pipeline every 15min/1hr
  - Pros: Simplest
  - Cons: Not truly real-time

**DECISION:** Start with **Polling (B)**, optionally add WebSocket later
- Poll every 60 seconds for new trades
- Detect new markets every 15 minutes
- Simpler to implement and debug

---

## Pipeline Stages

### Stage 1: Discover Up/Down Markets
**Script:** `updown_markets.py`

**Process:**
1. Query Polymarket API for all active events
2. Filter by question patterns:
   - Contains: "up" AND "down"
   - Contains: asset name (bitcoin/btc, ethereum/eth, solana/sol)
3. Extract metadata:
   - market_id
   - asset (BTC/SOL/ETH)
   - duration (15min/1hr)
   - open_time, close_time
   - token IDs (yes/no)
4. Save to `data/updown_markets.csv`

**Output Schema:**
```
market_id, asset, duration, question, open_time, close_time, yes_token_id, no_token_id
```

### Stage 2: Fetch Historical Trades
**Script:** `fetch_updown_trades.py`

**Process:**
1. Load market IDs from Stage 1
2. Load existing `processed/trades.csv`
3. Filter trades: `WHERE market_id IN (updown_market_ids)`
4. Save to `data/updown_trades_raw.csv`

**Alternative (for new markets not in trades.csv):**
1. Query CLOB API for each market
2. Fetch trades: `GET /trades?market={market_id}`
3. Append to dataset

**Output Schema:**
```
timestamp, market_id, maker, taker, side, direction, price, amount, usd_amount
```

### Stage 3: Load Binance Price Data
**Script:** `load_binance_data.py`

**Process:**
1. Load `binance_complete_minute_data.csv`
2. Ensure schema: `timestamp, symbol, open, high, low, close, volume`
3. Validate: Check for gaps, sort by timestamp
4. Convert timestamps to consistent format (Unix seconds)

**Output:** In-memory DataFrame (or cache to Parquet for speed)

### Stage 4: Integrate & Enrich
**Script:** `integrate_data.py`

**Process:**
```python
# For each trade:
# 1. Get asset price at trade time
trades_with_price = trades.join_asof(
    binance_data,
    left_on="timestamp",
    right_on="timestamp",
    strategy="nearest"
)

# 2. Get market open price
markets_with_open_price = markets.join_asof(
    binance_data,
    left_on="open_time",
    right_on="timestamp",
    strategy="nearest"
)

# 3. Combine and calculate features
enriched = (
    trades_with_price
    .join(markets_with_open_price, on="market_id")
    .with_columns([
        ((price - open_price) / open_price * 100).alias("move_pct"),
        (close_time - timestamp).alias("time_remaining_sec")
    ])
)
```

**Output Schema:**
```
timestamp, market_id, asset, duration, side, price, amount,
asset_price, market_open_price, move_pct, time_remaining_sec,
question, close_time
```

### Stage 5: Stream Real-Time Updates
**Script:** `stream_updates.py`

**Process:**
1. Every 15 minutes:
   - Check for new up/down markets
   - Add to market list
2. Every 60 seconds:
   - Query CLOB API for new trades (since last update)
   - Enrich with Binance prices
   - Append to dataset
3. Save updates incrementally

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    INITIALIZATION (Once)                     │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Polymarket API                 Existing Data                │
│       │                              │                        │
│       ├─> Discover Markets           ├─> Load trades.csv     │
│       │   (filter up/down)           │   (filter by markets) │
│       │                              │                        │
│       ▼                              ▼                        │
│  updown_markets.csv ──────> updown_trades_raw.csv            │
│                                      │                        │
│                                      │                        │
│  binance_complete_minute_data.csv    │                        │
│       │                              │                        │
│       └──────────> INTEGRATE ◄───────┘                        │
│                        │                                      │
│                        ▼                                      │
│               enriched_trades.csv                             │
│                                                               │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                  STREAMING (Continuous)                      │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Every 15 min:              Every 60 sec:                    │
│       │                          │                            │
│       ├─> Check new markets      ├─> Fetch new trades        │
│       │   (add to list)          │   (CLOB API)              │
│       │                          │                            │
│       ▼                          ▼                            │
│  Update markets.csv ──────> New trades                       │
│                                  │                            │
│                                  │                            │
│  Binance Live Data               │                            │
│  (fetch minute candles)          │                            │
│       │                          │                            │
│       └──────────> ENRICH ◄──────┘                            │
│                        │                                      │
│                        ▼                                      │
│            APPEND → enriched_trades.csv                       │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## Simplified File Structure

```
poly_data/
├── updown_pipeline/           # NEW: Simplified pipeline
│   ├── config.py             # Configuration (assets, durations, API keys)
│   ├── market_discovery.py   # Stage 1: Find up/down markets
│   ├── fetch_trades.py       # Stage 2: Get trades for markets
│   ├── integrate_prices.py   # Stage 4: Combine with Binance data
│   ├── stream_live.py        # Stage 5: Real-time updates
│   └── pipeline.py           # Main orchestrator
│
├── data/
│   ├── updown_markets.csv              # Discovered markets
│   ├── updown_trades_raw.csv           # Raw trades
│   ├── enriched_trades.csv             # Final enriched dataset
│   └── binance_complete_minute_data.csv # Price data (already exists)
│
└── notebooks/
    └── analyze_updown.ipynb  # Analysis notebook
```

---

## API Endpoints Reference

### Polymarket Gamma API
- **List Events:** `GET https://gamma-api.polymarket.com/events`
  - Params: `limit`, `offset`, `closed` (true/false), `tag_id`
- **Get Event:** `GET https://gamma-api.polymarket.com/events/{event_id}`
- **Search:** `GET https://gamma-api.polymarket.com/events?q={query}`

### CLOB API (for trades)
- **Market Trades:** `GET https://clob.polymarket.com/trades`
  - Params: `market={market_id}`, `before={timestamp}`, `after={timestamp}`
- **Market Book:** `GET https://clob.polymarket.com/book?token_id={token_id}`

### WebSocket (for streaming)
- **URL:** `wss://ws-subscriptions-clob.polymarket.com/ws/market/{market_id}`
- **Subscribe:** Send JSON with market ID
- **Receive:** Real-time trade events

---

## Implementation Priority

### Phase 1: Core Pipeline (Essential)
1. ✅ Market discovery (`market_discovery.py`)
2. ✅ Filter existing trades (`fetch_trades.py`)
3. ✅ Load Binance data (`integrate_prices.py` - Stage 3)
4. ✅ Integrate & enrich (`integrate_prices.py` - Stage 4)
5. ✅ Main orchestrator (`pipeline.py`)

### Phase 2: Streaming (Optional)
6. ⏳ Polling for new trades (`stream_live.py`)
7. ⏳ Detect new markets automatically
8. ⏳ Incremental updates

### Phase 3: Enhancements (Future)
9. ⏳ WebSocket streaming
10. ⏳ Dashboard/visualization
11. ⏳ Alert system for new markets

---

## Key Simplifications vs poly_data

| poly_data (Complex) | updown_pipeline (Simple) |
|---------------------|--------------------------|
| Fetches ALL markets | Only up/down markets |
| Goldsky scraping (68GB) | Filter existing trades.csv |
| Complex incremental updates | Simple: filter → enrich → save |
| 5+ stages, multiple files | 3 core scripts + orchestrator |
| Processes everything | Only process target markets |
| ~2+ days initial setup | ~1 hour with existing data |

---

## Configuration Example

```python
# config.py

ASSETS = ["BTC", "SOL", "ETH"]
DURATIONS = ["15min", "1hr"]

# Market identification patterns
QUESTION_PATTERNS = {
    "BTC": ["bitcoin", "btc"],
    "SOL": ["solana", "sol"],
    "ETH": ["ethereum", "eth"]
}

DURATION_PATTERNS = {
    "15min": ["15m", "15 min", "fifteen minute"],
    "1hr": ["1h", "1 hour", "one hour"]
}

# Data paths
BINANCE_DATA = "data/binance_complete_minute_data.csv"
EXISTING_TRADES = "processed/trades.csv"
EXISTING_MARKETS = "markets.csv"

OUTPUT_DIR = "data/updown/"
```

---

## Success Metrics

1. **Speed:** Complete historical processing in < 1 hour (vs 2+ days)
2. **Size:** Dataset < 1GB (vs 68GB+)
3. **Coverage:** Capture >95% of up/down market trades
4. **Freshness:** Real-time updates within 60 seconds
5. **Simplicity:** <500 lines of core code (vs 2000+ in poly_data)

---

## Next Steps

1. **Validate approach** - Confirm market discovery method works
2. **Build Phase 1** - Core pipeline (discovery → integrate)
3. **Test with sample data** - Verify enrichment logic
4. **Add Phase 2** - Streaming component
5. **Document usage** - Examples and tutorials

---

## Questions to Resolve

1. ✅ How to identify up/down markets? → Pattern matching
2. ✅ Historical trades source? → Filter existing trades.csv
3. ✅ Binance data integration? → join_asof() time-series join
4. ✅ Real-time approach? → Polling CLOB API
5. ⏳ What's the up/down market tag ID? → Need to query API to discover
6. ⏳ Rate limits on CLOB API? → Need to test
7. ⏳ How to detect new markets programmatically? → Poll every 15min

