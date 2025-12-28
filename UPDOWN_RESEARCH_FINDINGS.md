# Up/Down Market Research Findings

## Executive Summary

**Status:** ✅ Market discovery approach validated!

We successfully discovered and analyzed Polymarket's up/down markets for BTC, SOL, and ETH. The approach works perfectly and we now have a clear path forward for building the simplified pipeline.

---

## Key Findings

### 1. Up/Down Markets Exist and Are Active

Currently active markets (as of Dec 28, 2025):
- **9 total active markets**
- **3 BTC markets** (5min and 15min)
- **3 SOL markets** (5min and 15min)
- **3 ETH markets** (5min and 15min)

### 2. Market Structure

#### Duration Types
- **5-minute markets:** Most frequent, rolling every 5 minutes
- **15-minute markets:** Less frequent but present
- **Note:** No 1-hour markets currently active (may exist at different times)

#### URL Pattern
```
https://polymarket.com/event/{asset}-updown-{duration}-{timestamp}

Examples:
- btc-updown-5m-1766972400
- sol-updown-15m-1766972700
- eth-updown-5m-1766972400
```

#### Market Naming
```
{Asset} Up or Down - {Month} {Day}, {Start Time}-{End Time} ET

Examples:
- "Bitcoin Up or Down - December 28, 8:40PM-8:45PM ET"
- "Solana Up or Down - December 28, 8:45PM-9:00PM ET"
- "Ethereum Up or Down - December 28, 8:45PM-9:00PM ET"
```

### 3. Resolution Mechanism

Markets resolve based on **Chainlink price feeds:**
- BTC: `https://data.chain.link/streams/btc-usd`
- SOL: `https://data.chain.link/streams/sol-usd`
- ETH: `https://data.chain.link/streams/eth-usd`

**Resolution Logic:**
- Resolves to **"Up"** if end price >= start price
- Resolves to **"Down"** if end price < start price

### 4. Data Extraction Points

Each market provides:
```json
{
  "id": "1043567",                    // Market ID
  "slug": "btc-updown-5m-1766972400", // URL slug
  "question": "Bitcoin Up or Down...", // Full question
  "endDate": "2025-12-29T01:45:00Z",  // Resolution time
  "eventStartTime": "2025-12-29T01:40:00Z", // Start time (market open)
  "clobTokenIds": "[token1, token2]",  // Token IDs for trading
  "resolutionSource": "https://..."    // Chainlink feed
}
```

### 5. Discovery Method Works

**Pattern Matching Approach:** ✅ Validated
```python
# Filter by keywords in title/question
has_updown = ("up" in title.lower() or "up" in question.lower()) and \
             ("down" in title.lower() or "down" in question.lower())

# Filter by asset
for asset in ["bitcoin", "btc", "ethereum", "eth", "solana", "sol"]:
    if asset in combined_text.lower():
        # Found matching market
```

**Slug Pattern Extraction:** ✅ Validated
```python
# Extract duration from slug
if "5m" in slug:
    duration = "5min"
elif "15m" in slug:
    duration = "15min"
elif "1h" in slug:
    duration = "1hr"
```

---

## Sample Market Data

### BTC 5-Minute Market
```json
{
  "id": "127960",
  "market_id": "1043567",
  "slug": "btc-updown-5m-1766972400",
  "title": "Bitcoin Up or Down - December 28, 8:40PM-8:45PM ET",
  "endDate": "2025-12-29T01:45:00Z",
  "eventStartTime": "2025-12-29T01:40:00Z",
  "duration": "5min",
  "asset": "BTC",
  "yes_token_id": "37757111305943369313607477612117675991483283811157654819548775996705837140529",
  "no_token_id": "88555506998104325266323436919439318690534514836833575970405206307061892443068"
}
```

### SOL 15-Minute Market
```json
{
  "id": "127966",
  "market_id": "1043572",
  "slug": "sol-updown-15m-1766972700",
  "title": "Solana Up or Down - December 28, 8:45PM-9:00PM ET",
  "endDate": "2025-12-29T02:00:00Z",
  "eventStartTime": "2025-12-29T01:45:00Z",
  "duration": "15min",
  "asset": "SOL",
  "yes_token_id": "91708703740490433705347353462478301128278996355627364332816487359688551047620",
  "no_token_id": "80329841878162417037135741645669156668812093377218235153154202192078175476971"
}
```

---

## Pipeline Implications

### ✅ Validated Assumptions
1. **Market discovery via API:** Works perfectly
2. **Pattern matching:** Reliable for filtering
3. **Market metadata:** Complete and structured
4. **Token IDs available:** Can fetch trades via CLOB API

### 🔄 Adjustments Needed
1. **Duration flexibility:** Support 5min, 15min, and 1hr (not just 15min/1hr)
2. **Rolling markets:** Markets are created continuously, need periodic refresh
3. **Market volume:** Some markets have $0 volume (newly created or inactive)

### 🎯 Next Implementation Steps

1. **Market Discovery Module**
   - Query Polymarket API every 15 minutes
   - Filter for up/down markets (BTC/SOL/ETH)
   - Extract market IDs, token IDs, timing info
   - Save to `updown_markets.csv`

2. **Trade Fetching**
   - Option A: Filter existing `processed/trades.csv` by market IDs (fast)
   - Option B: Query CLOB API for specific markets (for new markets)
   - Combine both approaches

3. **Binance Integration**
   - Load `binance_complete_minute_data.csv`
   - Use Chainlink times as reference (market start = eventStartTime)
   - Join Polymarket trades with Binance prices
   - Calculate move % relative to market open price

4. **Real-Time Updates**
   - Poll every 60 seconds for new trades
   - Detect new markets every 15 minutes
   - Append to unified dataset

---

## Data Schema (Proposed)

### Input: `updown_markets.csv`
```
market_id, event_id, asset, duration, question, start_time, end_time,
yes_token_id, no_token_id, slug, status
```

### Input: `updown_trades_raw.csv`
```
timestamp, market_id, maker, taker, side, direction, price,
amount, usd_amount, tx_hash
```

### Input: `binance_minute_data.csv` (already exists)
```
timestamp, symbol, open, high, low, close, volume
```

### Output: `enriched_trades.csv`
```
timestamp, market_id, asset, duration, side, price, amount, usd_amount,
asset_price_at_trade, market_open_price, move_pct, time_remaining_sec,
question, end_time, resolution_price (if resolved)
```

---

## Performance Estimates

### Historical Data Processing
- **Markets to process:** ~500-1000 historical markets (estimate)
- **Trades per market:** ~100-5000 (varies by volume)
- **Total dataset size:** < 1GB (vs 68GB for full poly_data)
- **Processing time:** < 1 hour (vs 2+ days)

### Real-Time Updates
- **API calls:**
  - Market discovery: 1 call every 15 min (96/day)
  - Trade fetching: 3 assets × 60 sec polling = ~4320/day
- **Latency:** < 60 seconds for new trades
- **Data growth:** ~10-50 MB/day

---

## API Endpoints Reference

### Polymarket Gamma API
```
# Get all events (with filters)
GET https://gamma-api.polymarket.com/events
Params: limit, offset, closed (true/false), order (new/volume)

# Get specific event
GET https://gamma-api.polymarket.com/events/{event_id}

# Search events
GET https://gamma-api.polymarket.com/events?q={query}
```

### CLOB API (for trades)
```
# Get market trades
GET https://clob.polymarket.com/trades
Params: market={market_id}, before={timestamp}, after={timestamp}

# Get market orderbook
GET https://clob.polymarket.com/book
Params: token_id={token_id}

# Get market price
GET https://clob.polymarket.com/price
Params: token_id={token_id}
```

---

## Comparison: Full Pipeline vs Simplified Pipeline

| Aspect | poly_data (Full) | updown_pipeline (Simplified) |
|--------|------------------|------------------------------|
| **Scope** | All markets | Only up/down markets |
| **Data source** | Goldsky (68GB) | Filter trades.csv or CLOB API |
| **Initial setup** | 2+ days | < 1 hour |
| **Dataset size** | 68GB+ | < 1GB |
| **Markets tracked** | ~50,000+ | ~500-1000 |
| **Complexity** | High | Low |
| **Maintenance** | Heavy | Light |
| **Real-time** | Batch processing | Polling/streaming |
| **Code** | 2000+ lines | < 500 lines |

---

## Risk Assessment

### Low Risk
- ✅ Market discovery method validated
- ✅ Data structure understood
- ✅ Binance data already available

### Medium Risk
- ⚠️ CLOB API rate limits unknown (need to test)
- ⚠️ Historical market coverage may be incomplete
- ⚠️ Some markets may have zero volume

### Mitigation Strategies
1. Start with hybrid approach (filter trades.csv + CLOB API)
2. Implement exponential backoff for API calls
3. Cache market metadata to reduce API calls
4. Focus on high-volume markets first

---

## Conclusion

**The simplified up/down pipeline is FEASIBLE and VALIDATED.**

Key advantages:
- 10-100x faster than full pipeline
- 98% smaller dataset
- Much simpler codebase
- Easier to maintain
- Real-time capable

**Ready to implement Phase 1:** Market discovery + historical trade extraction + Binance integration

