# Analysis Guide

This guide shows how to load and analyze the Polymarket trade data.

## Loading Data

### With Polars (Recommended for Large Files)

```python
import polars as pl
from poly_utils import get_markets, PLATFORM_WALLETS

# Load markets
markets_df = get_markets()

# Load trades with streaming (memory efficient)
trades = pl.scan_csv("processed/trades.csv").collect(streaming=True)

# Convert timestamp to datetime
trades = trades.with_columns(
    pl.col("timestamp").str.to_datetime()
)
```

### With Pandas

```python
import pandas as pd

markets = pd.read_csv("markets.csv")
trades = pd.read_csv("processed/trades.csv", parse_dates=["timestamp"])
```

---

## Filtering Trades by User

When filtering for a specific user's trades, filter by the `maker` column. This is how Polymarket generates events at the contract level.

```python
USERS = {
    'domah': '0x9d84ce0306f8551e02efef1680475fc0f1dc1344',
    '50pence': '0x3cf3e8d5427aed066a7a5926980600f6c3cf87b3',
    'fhantom': '0x6356fb47642a028bc09df92023c35a21a0b41885',
    'car': '0x7c3db723f1d4d8cb9c550095203b686cb11e5c6b',
    'theo4': '0x56687bf447db6ffa42ffe2204a05edaa20f55839'
}

# Get all trades for a specific user
trader_df = trades.filter(pl.col("maker") == USERS['domah'])
```

---

## Joining Markets with Trades

```python
# Join to get market questions with trades
trades_with_markets = trades.join(
    markets_df.select(["id", "question", "answer1", "answer2"]),
    left_on="market_id",
    right_on="id",
    how="left"
)
```

---

## Common Analysis Patterns

### Volume by Market

```python
volume_by_market = (
    trades
    .group_by("market_id")
    .agg(
        pl.col("usd_amount").sum().alias("total_volume"),
        pl.len().alias("trade_count")
    )
    .sort("total_volume", descending=True)
)
```

### Daily Trading Volume

```python
daily_volume = (
    trades
    .with_columns(pl.col("timestamp").dt.date().alias("date"))
    .group_by("date")
    .agg(pl.col("usd_amount").sum().alias("daily_volume"))
    .sort("date")
)
```

### User PnL Calculation

```python
def calculate_pnl(trader_df: pl.DataFrame) -> pl.DataFrame:
    """Calculate PnL per market for a trader."""
    return (
        trader_df
        .group_by("market_id")
        .agg(
            # Buy side
            (pl.when(pl.col("maker_direction") == "BUY")
                .then(pl.col("token_amount"))
                .otherwise(0.0)).sum().alias("buy_tokens"),
            (pl.when(pl.col("maker_direction") == "BUY")
                .then(pl.col("usd_amount"))
                .otherwise(0.0)).sum().alias("buy_usd"),
            # Sell side
            (pl.when(pl.col("maker_direction") == "SELL")
                .then(pl.col("token_amount"))
                .otherwise(0.0)).sum().alias("sell_tokens"),
            (pl.when(pl.col("maker_direction") == "SELL")
                .then(pl.col("usd_amount"))
                .otherwise(0.0)).sum().alias("sell_usd"),
            # Last price for unrealized
            pl.col("price").last().alias("last_price")
        )
        .with_columns(
            # Net position
            (pl.col("buy_tokens") - pl.col("sell_tokens")).alias("position"),
            # Realized PnL
            (pl.col("sell_usd") - pl.col("buy_usd")).alias("realized_pnl")
        )
        .with_columns(
            # Unrealized PnL (mark to market)
            (pl.col("position") * pl.col("last_price")).alias("unrealized_pnl")
        )
        .with_columns(
            (pl.col("realized_pnl") + pl.col("unrealized_pnl")).alias("total_pnl")
        )
    )
```

### Price Standardization

For binary markets, you may want to standardize prices to always represent the "Yes" outcome:

```python
# If trade was on token2 (No), convert price to Yes-equivalent
trades = trades.with_columns(
    pl.when(pl.col("nonusdc_side") == "token2")
        .then(1 - pl.col("price"))
        .otherwise(pl.col("price"))
        .alias("standardized_price")
)
```

---

## Platform Wallets

The `poly_utils` module includes known platform wallet addresses:

```python
from poly_utils import PLATFORM_WALLETS

# Filter out platform trades
user_trades = trades.filter(
    ~pl.col("maker").is_in(PLATFORM_WALLETS) &
    ~pl.col("taker").is_in(PLATFORM_WALLETS)
)
```

---

## Example Notebooks

See the included Jupyter notebooks for complete analysis examples:

- `Example 1 Trader Analysis.ipynb` - Analyzing individual trader performance
- `Example 2 Backtest.ipynb` - Backtesting trading strategies
- `Isolated.ipynb` - Isolated market analysis
