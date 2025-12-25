# Polymarket Data

A data pipeline for fetching, processing, and analyzing Polymarket trading data.

## Quick Start

```bash
# Install UV (if not installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install
git clone https://github.com/yongkangc/polymarket-data.git
cd polymarket-data
uv sync

# Run the pipeline
uv run python update_all.py
```

See [Installation Guide](docs/install.md) for detailed setup instructions.

## Data Snapshot

Download the [latest data snapshot](https://polydata-archive.s3.us-east-1.amazonaws.com/archive.tar.xz) to skip initial data collection (saves 2+ days).

## Documentation

| Document | Description |
|----------|-------------|
| [Installation](docs/install.md) | Setup, dependencies, and quick start |
| [Pipeline Stages](docs/pipeline_stages.md) | How the data pipeline works |
| [Data Schema](docs/data_schema.md) | File formats and field descriptions |
| [Analysis Guide](docs/analysis.md) | Loading data and example queries |

## Pipeline Overview

```
Polymarket API --> markets.csv --\
                                  +--> trades.csv
Goldsky API ----> orderFilled.csv /
```

1. **Update Markets** - Fetch market metadata from Polymarket API
2. **Update Goldsky** - Scrape order-filled events from blockchain
3. **Process Live** - Transform raw events into structured trades

See [Pipeline Stages](docs/pipeline_stages.md) for the full diagram.

## Data Files

| File | Description |
|------|-------------|
| `markets.csv` | Market metadata (questions, outcomes, token IDs) |
| `processed/trades.csv` | Structured trade data (prices, directions, amounts) |

See [Data Schema](docs/data_schema.md) for field details.

## Example Usage

```python
import polars as pl
from poly_utils import get_markets

# Load data
markets = get_markets()
trades = pl.scan_csv("processed/trades.csv").collect(streaming=True)

# Filter by user
user = "0x9d84ce0306f8551e02efef1680475fc0f1dc1344"
user_trades = trades.filter(pl.col("maker") == user)
```

See [Analysis Guide](docs/analysis.md) for more examples.

## Notebooks

- `Example 1 Trader Analysis.ipynb` - Analyze trader performance
- `Example 2 Backtest.ipynb` - Backtest trading strategies

## License

MIT
