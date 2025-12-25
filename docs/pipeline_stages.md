# Pipeline Stages

This document describes the three stages of the Polymarket data pipeline in detail.

## Overview

The pipeline runs sequentially:
1. **Update Markets** - Fetch market metadata from Polymarket API
2. **Update Goldsky** - Scrape order-filled events from blockchain
3. **Process Live** - Transform raw events into structured trades

## Pipeline Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           POLYMARKET DATA PIPELINE                          │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────┐         ┌──────────────────┐
    │   Polymarket     │         │    Goldsky       │
    │   Gamma API      │         │   Subgraph       │
    └────────┬─────────┘         └────────┬─────────┘
             │                            │
             │ REST API                   │ GraphQL
             │ (markets)                  │ (order events)
             ▼                            ▼
    ┌──────────────────┐         ┌──────────────────┐
    │  1. UPDATE       │         │  2. UPDATE       │
    │     MARKETS      │         │     GOLDSKY      │
    │                  │         │                  │
    │ update_markets.py│         │update_goldsky.py │
    └────────┬─────────┘         └────────┬─────────┘
             │                            │
             │                            │
             ▼                            ▼
    ┌──────────────────┐         ┌──────────────────┐
    │   markets.csv    │         │ goldsky/         │
    │                  │         │ orderFilled.csv  │
    │ • Market IDs     │         │                  │
    │ • Questions      │         │ • Raw events     │
    │ • Token IDs      │         │ • Asset IDs      │
    │ • Outcomes       │         │ • Amounts        │
    └────────┬─────────┘         └────────┬─────────┘
             │                            │
             └────────────┬───────────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │  3. PROCESS      │
                 │     LIVE         │
                 │                  │
                 │ process_live.py  │
                 │                  │
                 │ • Map tokens to  │
                 │   markets        │
                 │ • Calculate      │
                 │   prices         │
                 │ • Determine      │
                 │   directions     │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │ processed/       │
                 │ trades.csv       │
                 │                  │
                 │ • market_id      │
                 │ • price          │
                 │ • direction      │
                 │ • usd_amount     │
                 │ • token_amount   │
                 └──────────────────┘
```

## Data Flow Summary

```
Polymarket API ──► markets.csv ──┐
                                 ├──► process_live.py ──► trades.csv
Goldsky API ────► orderFilled.csv┘
```

---

## 1. Update Markets (`update_markets.py`)

Fetches all markets from the Polymarket API in chronological order.

### How It Works

1. Counts existing rows in `markets.csv` to determine offset
2. Fetches markets in batches of 500 using the Gamma API
3. Parses market metadata including token IDs, questions, and outcomes
4. Appends new markets to `markets.csv`

### Features

- **Idempotent**: Automatically resumes from last offset
- **Rate Limiting**: Built-in delays between requests
- **Error Handling**: Retries on network failures

### API Endpoint

```
GET https://gamma-api.polymarket.com/markets
Parameters:
  - limit: 500
  - offset: (current row count)
  - order: createdAt
  - ascending: true
```

### Output Schema

| Field | Description |
|-------|-------------|
| `createdAt` | Market creation timestamp |
| `id` | Unique market identifier |
| `question` | Market question text |
| `answer1`, `answer2` | Outcome options |
| `neg_risk` | Negative risk flag |
| `market_slug` | URL-friendly market name |
| `token1`, `token2` | CLOB token IDs for outcomes |
| `condition_id` | Condition identifier |
| `volume` | Total trading volume |
| `ticker` | Market ticker symbol |
| `closedTime` | Market close timestamp |

### Usage

```bash
uv run python -c "from update_utils.update_markets import update_markets; update_markets()"
```

---

## 2. Update Goldsky (`update_goldsky.py`)

Scrapes order-filled events from the Goldsky subgraph API.

### How It Works

1. Reads last timestamp from `goldsky/orderFilled.csv`
2. Queries Goldsky GraphQL API for new events
3. Paginates through results using timestamp cursor
4. Deduplicates and appends to CSV

### Features

- **Resume Support**: Starts from last recorded timestamp
- **Pagination**: Handles large result sets
- **Deduplication**: Prevents duplicate events

### GraphQL Query

```graphql
query OrderFilledEvents($timestamp: BigInt!) {
  orderFilleds(
    first: 1000
    where: { timestamp_gte: $timestamp }
    orderBy: timestamp
    orderDirection: asc
  ) {
    timestamp
    maker
    makerAssetId
    makerAmountFilled
    taker
    takerAssetId
    takerAmountFilled
    transactionHash
  }
}
```

### Output Schema

| Field | Description |
|-------|-------------|
| `timestamp` | Unix timestamp of fill event |
| `maker` | Maker wallet address |
| `makerAssetId` | Asset ID maker is selling (0 = USDC) |
| `makerAmountFilled` | Amount filled by maker (raw units) |
| `taker` | Taker wallet address |
| `takerAssetId` | Asset ID taker is selling (0 = USDC) |
| `takerAmountFilled` | Amount filled by taker (raw units) |
| `transactionHash` | Blockchain transaction hash |

### Usage

```bash
uv run python -c "from update_utils.update_goldsky import update_goldsky; update_goldsky()"
```

---

## 3. Process Live (`process_live.py`)

Transforms raw order-filled events into structured trade data.

### How It Works

1. Loads markets and creates token-to-market mapping
2. Reads unprocessed events from `goldsky/orderFilled.csv`
3. For each event:
   - Identifies the non-USDC asset (outcome token)
   - Maps token ID to market
   - Determines trade direction (BUY/SELL)
   - Calculates price and amounts
4. Appends processed trades to `processed/trades.csv`

### Processing Logic

#### Asset Identification
- `makerAssetId` or `takerAssetId` of `"0"` represents USDC
- Non-zero IDs are outcome token IDs (`token1` or `token2` from markets)

#### Direction Determination
- **Taker buys outcome**: Taker pays USDC (`takerAssetId = 0`)
- **Taker sells outcome**: Taker receives USDC (`makerAssetId = 0`)
- Maker direction is opposite of taker

#### Price Calculation
```
price = USDC amount / token amount
```
All amounts are divided by 10^6 to convert from raw units.

### Features

- **Incremental Processing**: Tracks last processed transaction
- **Missing Market Discovery**: Auto-fetches unknown markets
- **Token Mapping**: Identifies which outcome (token1/token2) was traded

### Output Schema

| Field | Description |
|-------|-------------|
| `timestamp` | Trade timestamp |
| `market_id` | Market identifier |
| `maker` | Maker wallet address |
| `taker` | Taker wallet address |
| `nonusdc_side` | Which token was traded (`token1` or `token2`) |
| `maker_direction` | Maker's side (`BUY` or `SELL`) |
| `taker_direction` | Taker's side (`BUY` or `SELL`) |
| `price` | Price in USDC per outcome token |
| `usd_amount` | Total USDC value of trade |
| `token_amount` | Number of outcome tokens traded |
| `transactionHash` | Blockchain transaction hash |

### Usage

```bash
uv run python -c "from update_utils.process_live import process_live; process_live()"
```

---

## Running the Full Pipeline

The `update_all.py` script runs all three stages sequentially:

```bash
uv run python update_all.py
```

This is equivalent to:
```python
from update_utils.update_markets import update_markets
from update_utils.update_goldsky import update_goldsky
from update_utils.process_live import process_live

update_markets()
update_goldsky()
process_live()
```
