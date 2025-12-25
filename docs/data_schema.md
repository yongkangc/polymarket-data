# Data Schema

This document describes the data files and their schemas in detail.

## File Overview

| File | Description | Size |
|------|-------------|------|
| `markets.csv` | Market metadata from Polymarket API | ~10K+ rows |
| `goldsky/orderFilled.csv` | Raw order-filled blockchain events | ~10M+ rows |
| `processed/trades.csv` | Structured trade data | ~10M+ rows |
| `missing_markets.csv` | Auto-discovered markets (generated) | Variable |

---

## markets.csv

Contains metadata for all Polymarket markets.

### Schema

| Field | Type | Description |
|-------|------|-------------|
| `createdAt` | datetime | When the market was created |
| `id` | string | Unique market identifier (UUID) |
| `question` | string | The market question (e.g., "Will X happen?") |
| `answer1` | string | First outcome option (typically "Yes") |
| `answer2` | string | Second outcome option (typically "No") |
| `neg_risk` | boolean | Whether this is a negative risk market |
| `market_slug` | string | URL-friendly market name |
| `token1` | string | CLOB token ID for answer1 (76-digit number) |
| `token2` | string | CLOB token ID for answer2 (76-digit number) |
| `condition_id` | string | Condition identifier for the market |
| `volume` | float | Total trading volume in USDC |
| `ticker` | string | Short ticker symbol |
| `closedTime` | datetime | When the market closed (null if still open) |

### Notes

- Token IDs are 76-digit numbers stored as strings to prevent precision loss
- `neg_risk` markets have special pricing mechanics
- `volume` is cumulative and updates with each API fetch

---

## goldsky/orderFilled.csv

Raw order-filled events from the Polymarket smart contracts via Goldsky subgraph.

### Schema

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | integer | Unix timestamp (seconds since epoch) |
| `maker` | string | Maker's Ethereum address (0x...) |
| `makerAssetId` | string | Asset ID the maker is selling |
| `makerAmountFilled` | integer | Amount filled by maker (raw units, divide by 10^6) |
| `taker` | string | Taker's Ethereum address (0x...) |
| `takerAssetId` | string | Asset ID the taker is selling |
| `takerAmountFilled` | integer | Amount filled by taker (raw units, divide by 10^6) |
| `transactionHash` | string | Blockchain transaction hash |

### Asset ID Interpretation

- `"0"` = USDC (the quote currency)
- Any other value = Outcome token ID (maps to `token1` or `token2` in markets.csv)

### Amount Conversion

Raw amounts are in 6-decimal fixed-point format:
```python
actual_amount = raw_amount / 1_000_000
```

---

## processed/trades.csv

Structured trade data derived from order-filled events.

### Schema

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | datetime | Trade timestamp |
| `market_id` | string | Market identifier (matches `id` in markets.csv) |
| `maker` | string | Maker's Ethereum address |
| `taker` | string | Taker's Ethereum address |
| `nonusdc_side` | string | Which token was traded: `"token1"` or `"token2"` |
| `maker_direction` | string | Maker's side: `"BUY"` or `"SELL"` |
| `taker_direction` | string | Taker's side: `"BUY"` or `"SELL"` |
| `price` | float | Price in USDC per outcome token (0.0 to 1.0) |
| `usd_amount` | float | Total USDC value of the trade |
| `token_amount` | float | Number of outcome tokens traded |
| `transactionHash` | string | Blockchain transaction hash |

### Direction Logic

The direction is determined by who is paying USDC:

| Scenario | Taker Direction | Maker Direction |
|----------|-----------------|-----------------|
| Taker pays USDC for tokens | BUY | SELL |
| Taker receives USDC for tokens | SELL | BUY |

### Price Interpretation

- Price is always expressed as USDC per outcome token
- Range is typically 0.0 to 1.0 (representing probability)
- `price = usd_amount / token_amount`

### Token Side Interpretation

- `nonusdc_side = "token1"` means the trade was for outcome 1 (usually "Yes")
- `nonusdc_side = "token2"` means the trade was for outcome 2 (usually "No")

To get the complementary price:
```python
token2_price = 1 - token1_price  # For standard markets
```

---

## missing_markets.csv

Auto-generated file containing markets discovered during trade processing that weren't in the original `markets.csv`.

### Schema

Same as `markets.csv`.

### When It's Generated

When `process_live.py` encounters a token ID that doesn't match any market in `markets.csv`, it:
1. Queries the Polymarket API for that token
2. Saves the market data to `missing_markets.csv`
3. Uses the data to continue processing

---

## Data Relationships

```
markets.csv
    ├── id ─────────────────────┐
    ├── token1 ──────┐          │
    └── token2 ──────┤          │
                     │          │
goldsky/orderFilled.csv         │
    ├── makerAssetId ┼──────────┤ (token lookup)
    ├── takerAssetId ┘          │
    └── transactionHash ────────┤
                                │
processed/trades.csv            │
    ├── market_id ──────────────┘
    ├── nonusdc_side (token1 or token2)
    └── transactionHash (from goldsky)
```

---

## Loading Data

### With Polars (Recommended)

```python
import polars as pl

# Load markets
markets = pl.read_csv("markets.csv")

# Load trades (lazy for large files)
trades = pl.scan_csv("processed/trades.csv").collect(streaming=True)

# Convert timestamp
trades = trades.with_columns(
    pl.col("timestamp").str.to_datetime()
)
```

### With Pandas

```python
import pandas as pd

markets = pd.read_csv("markets.csv")
trades = pd.read_csv("processed/trades.csv", parse_dates=["timestamp"])
```
