# Installation

## Prerequisites

- Python 3.10+
- [UV](https://docs.astral.sh/uv/) package manager (recommended)

---

## Install UV

UV is a fast, reliable Python package manager.

### macOS / Linux

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Windows

```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### With pip

```bash
pip install uv
```

---

## Install Dependencies

```bash
# Clone the repository
git clone https://github.com/yongkangc/polymarket-data.git
cd polymarket-data

# Install all dependencies
uv sync

# Install with development dependencies (Jupyter, etc.)
uv sync --extra dev
```

---

## Quick Start

### Run the Full Pipeline

```bash
uv run python update_all.py
```

This will:
1. Fetch new markets from Polymarket API
2. Scrape order events from Goldsky
3. Process events into structured trades

### Or Activate Virtual Environment First

```bash
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
python update_all.py
```

---

## First-Time Setup

For faster initial setup, download the pre-built data snapshot:

1. Download [latest data snapshot](https://polydata-archive.s3.us-east-1.amazonaws.com/archive.tar.xz)
2. Extract in the repository root:
   ```bash
   tar -xf archive.tar.xz
   ```
3. Run the pipeline to fetch any new data:
   ```bash
   uv run python update_all.py
   ```

This saves over 2 days of initial data collection time.

---

## Running Individual Stages

### Update Markets Only

```bash
uv run python -c "from update_utils.update_markets import update_markets; update_markets()"
```

### Update Goldsky Only

```bash
uv run python -c "from update_utils.update_goldsky import update_goldsky; update_goldsky()"
```

### Process Trades Only

```bash
uv run python -c "from update_utils.process_live import process_live; process_live()"
```

---

## Jupyter Notebooks

To use the example notebooks:

```bash
# Install with dev dependencies
uv sync --extra dev

# Start Jupyter
uv run jupyter notebook
```

Then open one of:
- `Example 1 Trader Analysis.ipynb`
- `Example 2 Backtest.ipynb`

---

## Dependencies

All dependencies are managed via `pyproject.toml` and installed automatically with `uv sync`.

### Core Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `polars` | >=0.19.0 | Fast DataFrame operations, memory-efficient for large datasets |
| `pandas` | >=2.0.0 | Data manipulation and analysis |
| `requests` | >=2.31.0 | HTTP requests to Polymarket API |
| `gql[requests]` | >=3.4.0 | GraphQL client for Goldsky subgraph queries |
| `flatten-json` | >=0.1.13 | Flatten nested JSON responses from APIs |

### Development Dependencies (Optional)

Install with `uv sync --extra dev`:

| Package | Version | Purpose |
|---------|---------|---------|
| `jupyter` | >=1.0.0 | Interactive computing environment |
| `notebook` | >=7.0.0 | Jupyter notebook interface |
| `ipykernel` | >=6.25.0 | Python kernel for Jupyter |

### Python Version

Requires Python 3.8 or higher.

### Manual Installation (without UV)

If you prefer pip:

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install pandas>=2.0.0 polars>=0.19.0 requests>=2.31.0 "gql[requests]>=3.4.0" flatten-json>=0.1.13

# For development
pip install jupyter>=1.0.0 notebook>=7.0.0 ipykernel>=6.25.0
```
