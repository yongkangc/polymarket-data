# Getting Started with Zed REPL for Polymarket Analysis

This guide will help you set up and use Zed's built-in REPL for interactive data analysis with Polymarket notebooks.

## 🚀 Quick Setup

### Option 1: Automated Setup (Recommended)

```bash
cd notebooks
./setup_zed_repl.sh
```

Then in Zed:
1. Open Command Palette: `Cmd+Shift+P` (macOS) or `Ctrl+Shift+P` (Linux/Windows)
2. Type and run: `repl: refresh kernelspecs`
3. Open `trader_analysis.py` or `backtest_strategy.py`
4. Select code and press `Ctrl+Shift+Enter` to run

### Option 2: Manual Setup

```bash
# From the poly_data directory
cd poly_data  # or wherever your poly_data folder is

# Install dependencies with dev packages (includes ipykernel)
uv pip install -e ".[dev]"

# Install additional backtest packages
uv pip install backtrader backtrader-plotting bokeh matplotlib numpy

# Install the Jupyter kernel for Zed REPL
uv run python -m ipykernel install --user --name poly-data --display-name "Python (poly-data)"
```

Then refresh kernels in Zed (see Option 1 step 2).

## 📁 Files in This Directory

### 📊 Python Scripts (Use these with Zed REPL!)
- `trader_analysis.py` - Calculate trader P&L and analyze trading patterns
- `backtest_strategy.py` - Backtest trading strategies on historical data

### 📓 Original Jupyter Notebooks
- `Example 1 Trader Analysis.ipynb` - Jupyter version of trader analysis
- `Example 2 Backtest.ipynb` - Jupyter version of backtest

### 📚 Documentation
- `README.md` - Comprehensive documentation
- `GETTING_STARTED.md` - This file
- `setup_zed_repl.sh` - Automated setup script

## 🎯 How to Use Zed REPL

### The Basics

1. **Open a Python file** (e.g., `trader_analysis.py`)
2. **Cells are marked with `# %%`** - This is how Zed knows where cells begin
3. **Run code**:
   - Select lines of code or place cursor in a cell
   - Press `Ctrl+Shift+Enter` (macOS) or `Ctrl+Shift+Enter` (Linux/Windows)
4. **View output** - Results appear inline below your code
5. **Clear outputs** - Command Palette → `repl: clear outputs`

### Example Workflow

```python
# %% Load Data
import polars as pl
df = pl.read_csv("processed/trades.csv")

# %% Analyze
# Select this cell and press Ctrl+Shift+Enter
print(df.head())
print(f"Total trades: {len(df)}")
```

## 🔧 Configure Zed (Optional)

Add this to your Zed `settings.json` to set the default kernel:

```json
{
  "jupyter": {
    "kernel_selections": {
      "python": "poly-data"
    }
  }
}
```

To open settings in Zed:
- macOS: `Cmd+,`
- Linux/Windows: `Ctrl+,`

## 📊 Running the Examples

### Example 1: Trader Analysis

```bash
# In Zed, open:
notebooks/trader_analysis.py

# Run each cell with Ctrl+Shift+Enter
# This will:
# - Load trade data
# - Calculate P&L for a trader (domah by default)
# - Show best and worst performing markets
```

### Example 2: Backtest Strategy

```bash
# In Zed, open:
notebooks/backtest_strategy.py

# Run each cell with Ctrl+Shift+Enter
# This will:
# - Load Trump 2024 election market data
# - Create OHLCV bars
# - Run a moving average crossover strategy
# - Show P&L and generate plot
```

## 🗂️ Data Requirements

Both scripts require data in the `processed/` directory. If you don't have data yet:

```bash
# From poly_data directory
cd poly_data

# Fetch markets data
uv run python goldsky/fetch_markets.py

# Fetch and process trades (this takes a while!)
uv run python goldsky/fetch_and_process_trades.py
```

## 🐛 Troubleshooting

### Kernel Not Found

```bash
# Check available kernels
jupyter kernelspec list

# You should see 'poly-data' in the list
# If not, reinstall:
uv run python -m ipykernel install --user --name poly-data --display-name "Python (poly-data)"
```

Then refresh kernels in Zed: `Cmd+Shift+P` → `repl: refresh kernelspecs`

### Import Errors

```bash
# Verify installed packages
uv pip list

# Reinstall if needed
uv pip install -e ".[dev]" backtrader backtrader-plotting bokeh matplotlib numpy
```

### Data Not Found Errors

```bash
# Check if processed data exists
ls processed/

# Should contain trades.csv and markets data
# If empty, run the data pipeline (see Data Requirements above)
```

### Module Not Found: poly_utils

```bash
# Make sure you're in the poly_data directory
cd poly_data

# Install in editable mode
uv pip install -e .
```

## 🆚 Zed REPL vs Traditional Jupyter

| Feature | Zed REPL | Jupyter Notebook |
|---------|----------|------------------|
| File format | `.py` files | `.ipynb` files |
| Version control | ✅ Easy (text files) | ⚠️ Harder (JSON files) |
| Speed | ⚡ Fast | Normal |
| Setup | Lightweight | Heavier |
| Inline output | ✅ Yes | ✅ Yes |
| Plots | ✅ Yes | ✅ Yes |

## 🎓 Next Steps

1. ✅ Run `setup_zed_repl.sh`
2. ✅ Refresh kernels in Zed
3. ✅ Open `trader_analysis.py`
4. ✅ Run your first cell!
5. 📚 Read `README.md` for more details
6. 🧪 Modify the scripts for your own analysis

## 🔗 Useful Resources

- [Zed REPL Documentation](https://zed.dev/docs/repl)
- [uv Documentation](https://github.com/astral-sh/uv)
- [Polars Documentation](https://pola-rs.github.io/polars/)
- [Backtrader Documentation](https://www.backtrader.com/)

## 💡 Tips

- **Cell markers**: Use `# %%` to create new cells
- **Quick run**: `Ctrl+Shift+Enter` runs the current cell
- **Stay focused**: No need to switch between editor and browser
- **Live coding**: Kernel persists between runs
- **Clear when needed**: Use `repl: clear outputs` to clean up

## ❓ Need Help?

- Check `README.md` for detailed documentation
- Look at the example scripts to see patterns
- Run cells one at a time to debug issues
- Make sure data is in `processed/` directory

---

Happy analyzing! 🚀📊