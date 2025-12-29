# Polymarket Data Analysis Notebooks

This directory contains Jupyter notebooks for analyzing Polymarket trading data.

## Setup with `uv` and Zed REPL

### 1. Install Python Kernel for Zed

First, make sure you have `uv` installed. Then set up the Python environment with ipykernel:

```bash
# Navigate to the poly_data directory (if you're in notebooks/)
cd ..

# Install dependencies including dev dependencies (which includes ipykernel)
uv pip install -e ".[dev]"

# Install the kernel for Jupyter/Zed REPL
uv run python -m ipykernel install --user --name poly-data --display-name "Python (poly-data)"
```

### 2. Install Additional Dependencies

The notebooks require some additional packages:

```bash
# Install backtrader and plotting dependencies
uv pip install backtrader backtrader-plotting bokeh matplotlib numpy
```

### 3. Refresh Kernels in Zed

After installing the kernel:

1. Open Zed
2. Run the command palette (Cmd+Shift+P on macOS)
3. Search for and run: `repl: refresh kernelspecs`

### 4. Using the REPL in Zed

#### Convert Notebooks to Python Files

Zed's REPL works best with `.py` files using cell markers. Convert the notebooks:

```bash
# You can manually convert or just copy cells to a .py file
# Use # %% to mark cells
```

#### Example: Create a Python file with cells

```python
# %% Cell 1
import polars as pl
import pandas as pd

# %% Cell 2
df = pl.read_csv("processed/trades.csv")
print(df.head())

# %% Cell 3
# Your analysis here
```

#### Run Code

1. Open the `.py` file in Zed
2. Select code or place cursor in a cell
3. Press `Ctrl+Shift+Enter` (macOS) to run
4. Results appear inline below your code

#### Clear Outputs

Run command: `repl: clear outputs`

### 5. Configure Kernel in Zed Settings

Add to your Zed `settings.json`:

```json
{
  "jupyter": {
    "kernel_selections": {
      "python": "poly-data"
    }
  }
}
```

## Notebooks

### Example 1: Trader Analysis
- **File**: `Example 1 Trader Analysis.ipynb`
- **Description**: Analyzes individual trader performance on Polymarket
- **Features**:
  - Calculate trader P&L (Profit & Loss)
  - Track position sizes and inventory
  - Analyze trade patterns
  - Uses famous traders like 'domah' as examples

### Example 2: Backtest
- **File**: `Example 2 Backtest.ipynb`
- **Description**: Backtests trading strategies on Polymarket data
- **Features**:
  - Creates OHLCV (Open, High, Low, Close, Volume) data from trades
  - Implements Moving Average crossover strategy
  - Uses Backtrader framework
  - Generates interactive Bokeh plots
  - Example: Trump 2024 election market analysis

## Data Requirements

Both notebooks expect data in the `processed/` directory:
- `processed/trades.csv` - Trade data
- `processed/markets.csv` - Market metadata

Make sure you've run the data pipeline first:

```bash
cd ..  # if you're in notebooks/
uv run python goldsky/fetch_markets.py
uv run python goldsky/fetch_and_process_trades.py
```

## Tips for Zed REPL

1. **Cell Mode**: Use `# %%` markers to create cells in `.py` files
2. **Inline Results**: Results appear directly below your code
3. **No Restart Needed**: The kernel persists between runs
4. **Version Control Friendly**: `.py` files are much easier to version control than `.ipynb`

## Troubleshooting

### Kernel not found
```bash
# Check available kernels
jupyter kernelspec list

# Should see 'poly-data' in the list
```

### Import errors
```bash
# Make sure you're in the right environment
cd ../poly_data
uv pip list

# Reinstall if needed
uv pip install -e ".[dev]" backtrader backtrader-plotting bokeh matplotlib numpy
```

### Data not found
```bash
# Check if data directory exists
ls ../processed/  # or ls processed/ if you're in poly_data/

# If empty, run data pipeline
cd ..  # if you're in notebooks/
uv run python goldsky/fetch_markets.py
uv run python goldsky/fetch_and_process_trades.py
```

## Alternative: Traditional Jupyter

If you prefer traditional Jupyter notebooks:

```bash
cd ..  # if you're in notebooks/
uv pip install jupyter notebook
uv run jupyter notebook notebooks/
```

Then open the `.ipynb` files in your browser.