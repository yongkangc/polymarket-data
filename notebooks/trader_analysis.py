# %% Imports and Configuration
import warnings

warnings.filterwarnings("ignore")

import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
from poly_utils import get_markets, PLATFORM_WALLETS

pl.Config.set_tbl_rows(25)
pl.Config.set_tbl_cols(-1)  # Show all columns
cfg = pl.Config.set_tbl_width_chars(1000)  # Wider display

# %% Load Markets Data
markets_df = get_markets()

# %% Load and Process Trades Data
df = pl.scan_csv("processed/trades.csv").collect(streaming=True)

df = df.with_columns(pl.col("timestamp").str.to_datetime().alias("timestamp"))

# %% Define Traders to Analyze
USERS = {
    "domah": "0x9d84ce0306f8551e02efef1680475fc0f1dc1344",
    "50pence": "0x3cf3e8d5427aed066a7a5926980600f6c3cf87b3",
    "fhantom": "0x6356fb47642a028bc09df92023c35a21a0b41885",
    "car": "0x7c3db723f1d4d8cb9c550095203b686cb11e5c6b",
    "theo4": "0x56687bf447db6ffa42ffe2204a05edaa20f55839",
}

# %% Calculate Last Prices (Redemption Hack)
# A simple hack that does a good job of determining redemptions

df = df.with_columns(
    pl.col("price")
    .sort_by("timestamp")  # ensure we use the latest trade within each group
    .last()
    .over(["market_id", "nonusdc_side"])
    .alias("last_price")
)

df = df.with_columns(
    last_price=(
        pl.when(pl.col("last_price") > 0.98)
        .then(pl.lit(1.0))
        .when(pl.col("last_price") < 0.02)
        .then(pl.lit(0.0))
        .otherwise(pl.col("last_price"))
    )
)

# %% Filter Trades for Specific Trader
# This is how Polymarket generates its events and how you get all trades for a given user.
# Even if it looks like we are only getting data where the user is a maker,
# that is not how it works on the contract level.
# "maker" shows trades from that user's POV including price.

trader_df = df.filter((pl.col("maker") == USERS["domah"]))

# %% Select and Rename Columns
trader_df = trader_df[
    [
        "timestamp",
        "market_id",
        "maker",
        "taker",
        "maker_direction",
        "nonusdc_side",
        "price",
        "token_amount",
        "usd_amount",
        "transactionHash",
        "last_price",
    ]
]
trader_df = trader_df.rename({"maker_direction": "direction", "nonusdc_side": "side"})

# %% Calculate P&L by Market and Side
trader_df = (
    trader_df.group_by(["market_id", "side"])
    .agg(
        # USD volumes
        (
            pl.when(pl.col("direction") == "BUY")
            .then(pl.col("usd_amount"))
            .otherwise(0.0)
        )
        .sum()
        .alias("buy_usd"),
        (
            pl.when(pl.col("direction") == "SELL")
            .then(pl.col("usd_amount"))
            .otherwise(0.0)
        )
        .sum()
        .alias("sell_usd"),
        # Token volumes
        (
            pl.when(pl.col("direction") == "BUY")
            .then(pl.col("token_amount"))
            .otherwise(0.0)
        )
        .sum()
        .alias("buy_tokens"),
        (
            pl.when(pl.col("direction") == "SELL")
            .then(pl.col("token_amount"))
            .otherwise(0.0)
        )
        .sum()
        .alias("sell_tokens"),
        # Notionals for VWAPs
        (
            pl.when(pl.col("direction") == "BUY")
            .then(pl.col("price") * pl.col("token_amount"))
            .otherwise(0.0)
        )
        .sum()
        .alias("buy_notional"),
        (
            pl.when(pl.col("direction") == "SELL")
            .then(pl.col("price") * pl.col("token_amount"))
            .otherwise(0.0)
        )
        .sum()
        .alias("sell_notional"),

        pl.len().alias("trades"),
        pl.col("last_price").last().alias("last_price"),
    )
    .with_columns(
        (pl.col("sell_usd") - pl.col("buy_usd")).alias("cash_pnl_usd"),
        (pl.col("buy_tokens") - pl.col("sell_tokens")).alias("inventory_tokens"),
    )
    .with_columns(
        (pl.col("inventory_tokens") * pl.col("last_price")).alias("unrealized_usd"),
    )
    .with_columns(
        (pl.col("cash_pnl_usd") + pl.col("unrealized_usd")).alias("total_pnl_usd"),
    )
)

# %% Show Total P&L
# Domer's total P&L. Checks out within 1% in the UI
print(f"Total P&L: ${trader_df['total_pnl_usd'].sum():,.2f}")

# %% Show Top 10 Markets by P&L
top_markets = trader_df.sort("total_pnl_usd", descending=True).head(10)
print(top_markets)

# %% Show Bottom 10 Markets by P&L (Losses)
bottom_markets = trader_df.sort("total_pnl_usd", descending=False).head(10)
print(bottom_markets)
