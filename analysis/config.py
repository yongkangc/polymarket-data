"""
Configuration file for Polymarket Crypto Pattern Analysis
"""
import os
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RESULTS_DIR = PROJECT_ROOT / "results"
FIGURES_DIR = RESULTS_DIR / "figures"
CACHE_DIR = DATA_DIR / "cache"

# Ensure directories exist
for directory in [DATA_DIR, RESULTS_DIR, FIGURES_DIR, CACHE_DIR]:
    directory.mkdir(exist_ok=True, parents=True)

# API Configuration
TARDIS_API_KEY = os.getenv('TARDIS_API_KEY')
TARDIS_BASE_URL = "https://api.tardis.dev/v1"

# Exchange configuration
PRIMARY_EXCHANGE = "coinbase"
EXCHANGE_SYMBOLS = {
    "BTC": "BTC-USD",
    "ETH": "ETH-USD",
    "SOL": "SOL-USD"
}

EXCHANGE_CHANNELS = {
    "coinbase": "match",  # Coinbase uses "match" channel for trades
    "binance": "trade",   # Binance uses "trade" channel
}

# Bucketing Strategy - "Very Coarse" (90 combinations)
# Distance from target (6 buckets)
DISTANCE_BUCKETS = {
    "below_0-1%": (-0.01, 0.00),
    "below_1-3%": (-0.03, -0.01),
    "below_>3%": (-float('inf'), -0.03),
    "above_0-1%": (0.00, 0.01),
    "above_1-3%": (0.01, 0.03),
    "above_>3%": (0.03, float('inf')),
}

# Time remaining until market close (5 buckets)
TIME_BUCKETS = {
    "<10min": (0, 600),                    # 0-10 minutes
    "10-60min": (600, 3600),               # 10 minutes to 1 hour
    "1-6hr": (3600, 21600),                # 1-6 hours
    "6-24hr": (21600, 86400),              # 6-24 hours
    ">24hr": (86400, float('inf')),        # More than 1 day
}

# Market probability (3 buckets)
PROBABILITY_BUCKETS = {
    "low_0-33%": (0.00, 0.33),
    "mid_33-67%": (0.33, 0.67),
    "high_67-100%": (0.67, 1.00),
}

# Market outcome thresholds (from Example 1 notebook)
OUTCOME_YES_THRESHOLD = 0.98  # last_price > 0.98 → YES
OUTCOME_NO_THRESHOLD = 0.02   # last_price < 0.02 → NO

# Filtering criteria
MIN_MARKET_VOLUME = 10000  # Minimum $10K volume for inclusion
MIN_SAMPLE_SIZE = 10       # Minimum trades per bucket for analysis
MIN_EDGE = 0.05            # Minimum 5% edge to consider

# Backtesting configuration
INITIAL_BANKROLL = 10000   # $10,000 starting capital
MAX_POSITION_FRACTION = 0.10  # Max 10% of bankroll per trade

# Memory management
BATCH_SIZE = 500           # Process markets in batches of 500
STREAMING_MODE = True      # Use streaming for large files

# Performance metrics
RISK_FREE_RATE = 0.05      # 5% annual risk-free rate for Sharpe
TRADING_DAYS_PER_YEAR = 252

# Visualization settings
FIGURE_DPI = 300
FIGURE_SIZE = (14, 8)

# Assets to analyze
ASSETS = ["BTC", "ETH", "SOL"]
PRIMARY_ASSET = "BTC"  # Start with BTC for MVP
