# Crypto Markets Pipeline

High-performance pipeline to process ALL crypto market trades from Goldsky blockchain data (Nov-Dec 2025), filtering 279M events to ~93k crypto markets and outputting structured trade data.

## Overview

**Input**: 68GB goldsky/orderFilled.csv with 279M blockchain events
**Output**: ~3-5GB Parquet file with crypto trades only (expected 25-35M trades)
**Runtime**: ~30-40 minutes total

## Architecture

### Phase 1: Pre-filter Markets (~1 second)
Loads markets.csv and filters to crypto markets using pattern matching:
- **Patterns**: bitcoin|btc|ethereum|eth|solana|sol|xrp|cardano|ada|polkadot|dot|filecoin|fil|binance|bnb|chainlink|link
- **Output**: 93,448 crypto markets (36.5% of 256k total)
- **Creates**: Fast O(1) lookup set for Phase 2

### Phase 2: Stream, Filter, Transform (25-35 minutes)
Streams 68GB orderFilled.csv with Polars LazyFrame:
- **Memory efficient**: Never loads 68GB into memory
- **Automatic filtering**: Inner join with crypto markets
- **Transformation**: Reuses proven logic from process_live.py
- **Incremental write**: sink_parquet() avoids memory spikes

### Phase 3: Validation (1 minute)
Generates comprehensive statistics:
- Overall stats (trades, volume, date range)
- Volume breakdown by asset (BTC, ETH, SOL, etc.)
- Top 20 markets by trade count
- Sample trades

## Quick Start

### Run All Phases (Recommended)

```bash
cd /mnt/data/poly_data
source .venv/bin/activate
python pipelines/crypto/run_pipeline.py
```

### Run Individual Phases

```bash
# Phase 1: Filter markets (~1 second)
python pipelines/crypto/prepare_crypto_markets.py

# Phase 2: Process trades (~25-35 minutes)
python pipelines/crypto/process_crypto_trades.py

# Phase 3: Validate (~1 minute)
python pipelines/crypto/validate_crypto_trades.py
```

## Phase 1 Results

**Test Run Output:**
```
✓ Loaded 256,157 total markets
✓ Found 93,448 crypto markets (36.5% of total)

Crypto Markets Breakdown:
  Bitcoin (BTC)         27,172 markets
  Ethereum (ETH)        26,189 markets
  Solana (SOL)          20,380 markets
  XRP                   19,646 markets
  Cardano (ADA)             15 markets
  Polkadot (DOT)             4 markets
  Filecoin (FIL)             1 markets
  Binance (BNB)             73 markets
  Chainlink (LINK)          35 markets

✅ Phase 1 Complete in 0.9 seconds
```

## Output Files

```
/mnt/data/poly_data/data/processed/
├── crypto_markets.csv          # 37.6 MB - Full metadata for 93k crypto markets
├── crypto_market_ids.pkl       # 455 KB - Fast O(1) lookup set
└── crypto_trades.parquet       # 3-5 GB - Final structured trades (after Phase 2)
```

## Output Schema

**crypto_trades.parquet columns:**
```
timestamp          - Trade timestamp (datetime)
market_id          - Polymarket market ID
maker              - Maker wallet address
taker              - Taker wallet address
maker_direction    - BUY or SELL (maker perspective)
taker_direction    - BUY or SELL (taker perspective)
price              - Price in USD (usd_amount / token_amount)
usd_amount         - USDC amount (in tokens, not wei)
token_amount       - Crypto token amount (in tokens, not wei)
transactionHash    - Blockchain transaction hash
```

## Performance

**Memory Usage:**
- Phase 1: ~200 MB (loads markets.csv)
- Phase 2: ~3-4 GB peak (Polars streaming buffers)
- Phase 3: ~1 GB (lazy evaluation)
- **Total**: 4 GB out of 16 GB available ✅

**Expected Results:**
- Input: 279M events (68GB CSV)
- Filter rate: ~10-12% (crypto markets only)
- Output: 25-35M trades
- Compression: 70% (ZSTD level 3)

## Why This Architecture?

**Load markets.csv into memory (vs streaming):**
- 105MB is small enough to fit in memory comfortably
- Pre-computing crypto market IDs enables O(1) hash lookups
- Alternative (streaming) would require 279M × 256k join operations
- Current approach: One-time 256k filter + 279M hash lookups (much faster!)

**Key advantages:**
1. **Memory Efficient**: Never loads 68GB into memory
2. **Fast Filtering**: Hash-set lookups vs. 279M regex operations
3. **Reusable Logic**: Proven transformation from process_live.py
4. **Better Format**: Parquet = 70% compression + faster reads
5. **Resumable**: Can checkpoint and restart if interrupted

## Troubleshooting

**Error: markets.csv not found**
- Fixed in utils.py (now looks in data/raw/markets.csv)

**Error: orderFilled.csv not found**
- Ensure goldsky data is in: `/mnt/data/poly_data/data/raw/goldsky/orderFilled.csv`

**Phase 2 taking too long?**
- Expected runtime: 25-35 minutes
- Check system load: `htop`
- Check I/O: `iotop -o`

## Next Steps After Completion

1. Load crypto_trades.parquet into your dashboard
2. Add crypto-only filter toggle to UI
3. Compare with full trades.csv to verify accuracy
4. Set up incremental updates for new data

## Technical Details

**Dependencies:**
- polars (streaming, lazy evaluation)
- pickle (fast serialization)

**Key Polars Features Used:**
- `scan_csv()` - Lazy loading without memory spike
- `sink_parquet()` - Incremental write
- `LazyFrame` - Query optimization
- Inner join - Automatic filtering

**Transformation Logic:**
Based on `/mnt/data/poly_data/pipelines/full/process_live.py` (lines 15-100)
