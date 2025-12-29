# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data pipeline for fetching, processing, and analyzing Polymarket trading data. The project consists of two main components:

1. **Data Pipeline** (`update_utils/`): Fetches markets from Polymarket API and order events from Goldsky blockchain data
2. **Analysis Framework** (`analysis/`): Analyzes crypto price prediction markets to identify trading patterns and calculate historical win rates

## Common Commands

### Environment Setup
```bash
# Install dependencies
uv sync

# Install with Jupyter notebooks support
uv sync --extra dev

# Activate virtual environment (optional)
source .venv/bin/activate
```

### Data Pipeline
```bash
# Run full pipeline (markets → goldsky → trades)
uv run python update_all.py

# Run individual stages
uv run python -c "from update_utils.update_markets import update_markets; update_markets()"
uv run python -c "from update_utils.update_goldsky import update_goldsky; update_goldsky()"
uv run python -c "from update_utils.process_live import process_live; process_live()"
```

### Analysis Scripts
```bash
# Step 1: Classify crypto markets
uv run python -m analysis.classify_crypto_markets

# Step 2: Extract trade data
uv run python -m analysis.extract_polymarket_data

# Step 3: Enrich with price data
uv run python -m analysis.trade_enricher

# Step 4: Apply bucketing
uv run python -m analysis.bucketing

# Step 5: Calculate win rates
uv run python -m analysis.pattern_analyzer
```

### Price Data Collection
```bash
# Fetch historical price data (parallel)
uv run python price/fetch_binance_parallel.py

# Single-threaded fallback
uv run python price/fetch_binance_tardis.py
```

### Jupyter Notebooks
```bash
uv run jupyter notebook
```

## Architecture

### Data Flow

The pipeline follows a three-stage sequential process:

```
Polymarket API → markets.csv ──┐
                               ├─→ process_live.py → trades.csv
Goldsky API → orderFilled.csv ─┘
```

1. **Update Markets**: Fetch market metadata from Polymarket Gamma API
2. **Update Goldsky**: Scrape order-filled events from Goldsky GraphQL subgraph
3. **Process Live**: Join markets + events → structured trades with price/direction

### Module Structure

**`update_utils/`** - Data collection pipeline
- `update_markets.py`: Fetches market metadata (questions, token IDs, outcomes)
- `update_goldsky.py`: Scrapes blockchain order events via GraphQL
- `process_live.py`: Transforms raw events into structured trades
  - Maps token IDs to markets using `poly_utils.utils.get_markets()`
  - Determines BUY/SELL direction based on USDC flow
  - Calculates price = USDC_amount / token_amount

**`analysis/`** - Pattern analysis framework
- `config.py`: Central configuration (bucketing strategy, paths, thresholds)
- `classify_crypto_markets.py`: Identifies crypto price prediction markets
- `extract_polymarket_data.py`: Filters trades and determines outcomes
- `trade_enricher.py`: Joins trades with historical price data
- `bucketing.py`: Assigns trades to distance/time/probability buckets
- `pattern_analyzer.py`: Calculates win rates by pattern

**`poly_utils/`** - Shared utilities
- `utils.py`: Market data helpers (get active markets, token ID mapping)

**`price/`** - Historical price data
- `fetch_binance_parallel.py`: Multi-threaded TARDIS API fetcher (10 concurrent sessions)
- `fetch_binance_tardis.py`: Single-threaded fallback

### Key Design Patterns

**Token-to-Market Mapping**
Markets have two outcome tokens (`token1`, `token2`). Order events reference token IDs. The pipeline:
1. Creates a lookup: `token_id → (market_id, side)`
2. Identifies non-USDC asset in each trade (asset_id ≠ "0")
3. Maps that asset to market and determines which side was traded

**Market Outcome Detection**
Markets resolve to YES/NO based on final trading price:
- `last_price > 0.98` → YES outcome
- `last_price < 0.02` → NO outcome
- Otherwise → UNRESOLVED (excluded from analysis)

**Memory-Efficient Processing**
Large files (trades.csv ~32GB) use Polars streaming:
```python
pl.scan_csv("processed/trades.csv")
  .filter(pl.col("market_id").is_in(market_ids))
  .collect(streaming=True)
```

Process markets in batches (default: 500) to manage memory.

**Bucketing Strategy**
Trades are categorized by 3 dimensions (90 total combinations):
- **Distance from target**: 6 buckets (3 above, 3 below threshold)
- **Time remaining**: 5 buckets (<10min, 10-60min, 1-6hr, 6-24hr, >24hr)
- **Market probability**: 3 buckets (low 0-33%, mid 33-67%, high 67-100%)

All bucket definitions are in `analysis/config.py`.

## Data Schema

### `markets.csv`
Core market metadata fetched from Polymarket API.

Key fields:
- `id`: Market identifier (used as `market_id` in joins)
- `question`: Market question text
- `token1`, `token2`: CLOB token IDs for YES/NO outcomes
- `condition_id`: Condition identifier
- `volume`: Total trading volume
- `closedTime`: Market resolution deadline

### `goldsky/orderFilled.csv`
Raw blockchain order events from Goldsky subgraph.

Key fields:
- `timestamp`: Unix timestamp
- `maker`, `taker`: Wallet addresses
- `makerAssetId`, `takerAssetId`: Asset IDs (0 = USDC)
- `makerAmountFilled`, `takerAmountFilled`: Raw amounts (divide by 10^6)
- `transactionHash`: Blockchain transaction hash

### `processed/trades.csv`
Structured trade data (output of pipeline).

Key fields:
- `market_id`: Market identifier
- `timestamp`: Trade timestamp
- `maker`, `taker`: Wallet addresses
- `nonusdc_side`: Which token traded ("token1" or "token2")
- `maker_direction`, `taker_direction`: BUY or SELL
- `price`: Price in USDC per token
- `usd_amount`: Total USDC value
- `token_amount`: Number of outcome tokens

## Environment Variables

Create a `.env` file in the project root:

```bash
TARDIS_API_KEY=your_tardis_api_key_here
```

Required for price data collection via `price/fetch_binance_*.py`.

## Dependencies

Core libraries:
- **polars**: Fast DataFrame operations, streaming support for large files
- **pandas**: Legacy support for some utilities
- **requests**: HTTP API calls
- **gql**: GraphQL client for Goldsky subgraph

Dev dependencies (optional):
- **jupyter**: Interactive notebooks
- **matplotlib**, **seaborn**: Visualization (used in analysis scripts)

## Important Notes

- **Pipeline is idempotent**: Each stage tracks progress and resumes from last offset/timestamp
- **Token ID 0 = USDC**: All trades involve USDC on one side (prediction market design)
- **Direction logic**: If taker pays USDC (`takerAssetId = 0`), taker is buying the outcome token
- **Price calculation**: Always USDC_amount / token_amount (not the reverse)
- **Market filtering**: Analysis focuses on crypto markets (BTC/ETH/SOL price predictions)
- **Streaming mode**: Enable for files >1GB to avoid OOM errors
- **Batch processing**: Default batch size is 500 markets (configurable in `config.py`)

## Analysis Pipeline

The analysis follows a sequential data enrichment pattern:

1. **Classify markets** → Identify 3,020+ usable crypto markets
2. **Extract trades** → Filter trades.csv for these markets + determine outcomes
3. **Enrich trades** → Join with historical crypto prices (TARDIS data)
4. **Apply bucketing** → Categorize by distance/time/probability
5. **Calculate patterns** → Aggregate win rates by bucket

Each stage outputs a CSV that becomes input for the next stage. All outputs go to `data/` directory. Analysis results go to `results/`.

## Testing and Development

When developing new analysis scripts:
1. Use small subsets first (e.g., single asset, date range)
2. Verify outputs at each stage before proceeding
3. Check memory usage with `htop` for large file operations
4. Use Polars streaming for files >5GB
5. Cache intermediate results to avoid re-processing

When modifying the pipeline:
1. Test each stage independently before running full pipeline
2. Verify `markets.csv` and `goldsky/orderFilled.csv` exist before processing
3. Check token mapping logic if markets are missing from output
4. Validate price calculations (should be between 0 and 1 for prediction markets)
