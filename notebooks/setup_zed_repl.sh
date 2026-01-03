#!/bin/bash

# Setup script for using Zed REPL with uv and Polymarket notebooks
# Run this from the pm/poly_data/notebooks directory

set -e

echo "🚀 Setting up Zed REPL for Polymarket Analysis"
echo "================================================"
echo ""

# Navigate to poly_data directory (parent directory)
cd ..

echo "📦 Step 1: Installing dependencies with uv..."
uv pip install -e ".[dev]"

echo ""
echo "📦 Step 2: Installing additional packages for backtesting..."
uv pip install backtrader backtrader-plotting bokeh matplotlib numpy

echo ""
echo "🔧 Step 3: Installing Python kernel for Jupyter/Zed REPL..."
uv run python -m ipykernel install --user --name poly-data --display-name "Python (poly-data)"

echo ""
echo "✅ Setup complete!"
echo ""
echo "📝 Next steps:"
echo "1. Open Zed editor"
echo "2. Run command (Cmd+Shift+P): 'repl: refresh kernelspecs'"
echo "3. Open notebooks/trader_analysis.py or notebooks/backtest_strategy.py"
echo "4. Select code and press Ctrl+Shift+Enter to run"
echo ""
echo "📚 Files available in notebooks/:"
echo "   - trader_analysis.py     - Analyze trader performance"
echo "   - backtest_strategy.py   - Backtest trading strategies"
echo ""
echo "⚙️  Optional: Add to Zed settings.json:"
echo '   {'
echo '     "jupyter": {'
echo '       "kernel_selections": {'
echo '         "python": "poly-data"'
echo '       }'
echo '     }'
echo '   }'
echo ""
echo "🗂️  Make sure you have data in processed/ directory:"
echo "   - Run: python goldsky/fetch_markets.py"
echo "   - Run: python goldsky/fetch_and_process_trades.py"
echo ""
echo "Happy analyzing! 📊"
