"""
Configuration for Up/Down Market Pipeline
"""
from pathlib import Path

# ============================================================================
# Assets and Market Filters
# ============================================================================

# Assets to track
ASSETS = ["BTC", "SOL", "ETH"]

# Durations to track (all)
DURATIONS = ["5m", "15m", "1h"]

# Asset name patterns for filtering
ASSET_PATTERNS = {
    "BTC": ["bitcoin", "btc"],
    "SOL": ["solana", "sol"],
    "ETH": ["ethereum", "eth"]
}

# Duration patterns for extraction
DURATION_PATTERNS = {
    "5m": ["5m", "-5m-", "5 min", "five minute"],
    "15m": ["15m", "-15m-", "15 min", "fifteen minute"],
    "1h": ["1h", "-1h-", "1 hour", "one hour"]
}

# ============================================================================
# File Paths
# ============================================================================

# Base directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
CHECKPOINT_DIR = DATA_DIR / ".checkpoints"

# Input files (existing poly_data files)
EXISTING_TRADES = BASE_DIR / "processed" / "trades.csv"
EXISTING_MARKETS = BASE_DIR / "markets.csv"
BINANCE_DATA = DATA_DIR / "binance_complete_minute_data.csv"

# Output files
UPDOWN_MARKETS = DATA_DIR / "updown_markets.csv"
UPDOWN_TRADES_HISTORICAL = DATA_DIR / "updown_trades_historical.csv"
UPDOWN_TRADES_ENRICHED = DATA_DIR / "updown_trades_enriched.csv"

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# API Configuration
# ============================================================================

# Polymarket API endpoints
POLYMARKET_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"

# API rate limiting (seconds between requests)
API_DELAY = 0.5

# ============================================================================
# Streaming Configuration
# ============================================================================

# How often to check for new markets (seconds)
MARKET_CHECK_INTERVAL = 15 * 60  # 15 minutes

# How often to poll for new trades (seconds)
TRADE_POLL_INTERVAL = 60  # 60 seconds

# Checkpoint freshness threshold (seconds)
CHECKPOINT_FRESHNESS_HOURS = 1  # 1 hour

# ============================================================================
# Data Processing
# ============================================================================

# Binance symbol mapping
BINANCE_SYMBOL_MAP = {
    'BTC': 'BTCUSDT',
    'SOL': 'SOLUSDT',
    'ETH': 'ETHUSDT'
}

# Timestamp column names (for compatibility)
TIMESTAMP_COLUMNS = ['timestamp', 'trade_ts_sec', 'ts']

# ============================================================================
# Logging
# ============================================================================

LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
