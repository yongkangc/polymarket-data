# Up/Down Market Pipeline

Simplified pipeline for tracking BTC/SOL/ETH up/down markets on Polymarket, enriched with Binance price data.

## Quick Start

```bash
# Run the full pipeline
cd /home/chiayongtcac/pm/poly_data
.venv/bin/python -m updown_pipeline.run_pipeline --no-stream

# Or with streaming
.venv/bin/python -m updown_pipeline.run_pipeline
```

## Features

✅ **Market Discovery**: Automatically finds BTC/SOL/ETH up/down markets (5m, 15m, 1h)
✅ **Dual Data Sources**: Uses both historical trades.csv + CLOB API
✅ **Binance Integration**: Enriches trades with price data and calculates features
✅ **Checkpoint System**: Smart resume - picks up where it left off
✅ **Real-Time Streaming**: Continuously polls for new markets and trades

## Architecture

### Phase 1: Historical Data (3 stages)

```
Stage 1: Market Discovery
  ↓ Discovers BTC/SOL/ETH up/down markets
  ↓ Output: data/updown_markets.csv

Stage 2A: Fetch Historical Trades
  ↓ Filters existing trades.csv by market IDs
  ↓ Output: data/updown_trades_historical.csv

Stage 2B: Fetch CLOB Trades
  ↓ Queries CLOB API for new markets
  ↓ Appends to: data/updown_trades_historical.csv

Stage 3: Integrate Binance Prices
  ↓ Joins with binance_complete_minute_data.csv
  ↓ Calculates: move_pct, time_remaining_sec
  ↓ Output: data/updown_trades_enriched.csv ⭐
```

### Phase 2: Real-Time Streaming

```
Every 15 min: Check for new markets
Every 60 sec: Poll for new trades
  ↓ Enriches with Binance data
  ↓ Appends to enriched CSV
```

## Output Schema

**`data/updown_trades_enriched.csv`**

| Column | Description |
|--------|-------------|
| `timestamp` | Trade timestamp |
| `market_id` | Polymarket market ID |
| `asset` | BTC / SOL / ETH |
| `duration` | 5m / 15m / 1h |
| `question` | Market question |
| `maker`, `taker` | Wallet addresses |
| `price` | Trade price (0-1) |
| `usd_amount` | USD value |
| `asset_price_at_trade` | Asset price when trade happened |
| `market_open_price` | Asset price at market open |
| `move_pct` | % move since market opened ⭐ |
| `time_remaining_sec` | Seconds until market closes ⭐ |
| `end_time` | Market close time |

## CLI Usage

```bash
# First run (full pipeline)
python -m updown_pipeline.run_pipeline

# Force refresh all data
python -m updown_pipeline.run_pipeline --force-refresh

# Historical data only (no streaming)
python -m updown_pipeline.run_pipeline --no-stream

# Skip to streaming (requires Phase 1 complete)
python -m updown_pipeline.run_pipeline --stream-only

# Clear checkpoints and start fresh
python -m updown_pipeline.run_pipeline --clear-checkpoints

# See help
python -m updown_pipeline.run_pipeline --help
```

## Testing Individual Stages

```bash
# Test Stage 1: Market Discovery
python -m updown_pipeline.market_discovery

# Test Stage 2A: Historical Trades
python -m updown_pipeline.fetch_historical_trades

# Test Stage 2B: CLOB Trades
python -m updown_pipeline.fetch_clob_trades

# Test Stage 3: Integration
python -m updown_pipeline.integrate_binance

# Test Streaming
python -m updown_pipeline.stream_live
```

## Configuration

Edit `updown_pipeline/config.py` to customize:

- **Assets**: `ASSETS = ["BTC", "SOL", "ETH"]`
- **Durations**: `DURATIONS = ["5m", "15m", "1h"]`
- **Polling intervals**: `TRADE_POLL_INTERVAL`, `MARKET_CHECK_INTERVAL`
- **File paths**: `BINANCE_DATA`, `EXISTING_TRADES`, etc.

## Checkpoint System

Checkpoints are stored in `data/.checkpoints/`:
- `markets.done` - Stage 1 complete
- `historical.done` - Stage 2 complete
- `enriched.done` - Stage 3 complete

The pipeline automatically:
- ✅ Skips completed stages (unless `--force-refresh`)
- ✅ Resumes from last checkpoint
- ✅ Goes straight to streaming if Phase 1 is recent (< 1hr)

## Performance

### Phase 1 (Historical)
- **Stage 1 (Discovery):** ~30 seconds
- **Stage 2A (Filter trades.csv):** ~10-30 minutes
- **Stage 2B (CLOB API):** ~1-5 minutes per 100 markets
- **Stage 3 (Integration):** ~5-15 minutes
- **Total:** ~20-60 minutes

### Phase 2 (Real-Time)
- **CPU:** Low (polling + append)
- **Memory:** ~100-500 MB
- **API calls:** ~100-200/hour
- **Latency:** < 60 seconds for new trades

## Validation Results

✅ **Stage 1 Tested**: Discovered 133 markets (44 BTC, 43 ETH, 46 SOL)
✅ **Token ID Fix**: Large numbers stored as strings (not i64)
✅ **Checkpoint System**: Working correctly
✅ **File Structure**: All modules created

## Troubleshooting

### "ModuleNotFoundError: No module named 'polars'"
Use the virtual environment:
```bash
.venv/bin/python -m updown_pipeline.run_pipeline
```

### "File not found: binance_complete_minute_data.csv"
Ensure the file exists at:
```
/home/chiayongtcac/pm/poly_data/data/binance_complete_minute_data.csv
```

### Stage 2A taking too long
The `trades.csv` file is large (68GB+). This is expected.
First run will take 10-30 minutes. Subsequent runs use checkpoints.

### No historical trades found
This means the markets are too recent (not yet in trades.csv).
Stage 2B will fetch from CLOB API instead.

## Files Created

```
updown_pipeline/
├── __init__.py
├── config.py                    # Configuration
├── checkpoint.py                # Checkpoint manager
├── market_discovery.py          # Stage 1
├── fetch_historical_trades.py   # Stage 2A
├── fetch_clob_trades.py         # Stage 2B
├── integrate_binance.py         # Stage 3
├── stream_live.py               # Phase 2
├── run_pipeline.py              # Main orchestrator
└── README.md                    # This file

data/
├── .checkpoints/                # Progress tracking
├── updown_markets.csv           # Discovered markets
├── updown_trades_historical.csv # Raw trades
└── updown_trades_enriched.csv   # Final output ⭐
```

## Next Steps

1. **Run the pipeline**: `.venv/bin/python -m updown_pipeline.run_pipeline --no-stream`
2. **Analyze the data**: Load `data/updown_trades_enriched.csv` in notebooks
3. **Enable streaming**: Run without `--no-stream` for real-time updates

## Support

See implementation plan: `../IMPLEMENTATION_PLAN_FINAL.md`
See research findings: `../UPDOWN_RESEARCH_FINDINGS.md`
