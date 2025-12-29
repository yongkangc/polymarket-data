# %% Imports and Configuration
import warnings

warnings.filterwarnings("ignore")

import backtrader as bt
import numpy as np
import pandas as pd
import polars as pl
from backtrader.feeds import PandasData
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Blackly
from bokeh.plotting import output_file, save
from poly_utils import get_markets

# %% Load Trades Data
df = pl.scan_csv("processed/trades.csv").collect(streaming=True)

df = df.with_columns(pl.col("timestamp").str.to_datetime().alias("timestamp"))

# %% Load Markets Data
markets = get_markets()

# %% Process Market Timestamps
markets = markets.with_columns(pl.col("createdAt").str.to_datetime().alias("createdAt"))

markets = markets.with_columns(pl.col("createdAt").dt.replace_time_zone(None))

# %% Select Target Market
# Find the Trump 2024 election market (sorting by volume to get the main one)
target_id = (
    markets.filter(
        markets["question"].str.contains(
            "Will Donald Trump win the 2024 US Presidential Election"
        )
    )
    .sort("volume")
    .row(0, named=True)["id"]
)

print(f"Target market ID: {target_id}")

# %% Filter Trades for Target Market
sel_df = df.filter(pl.col("market_id") == target_id)

print(f"Total trades: {len(sel_df)}")

# %% Select Relevant Columns
sel_df = sel_df[["timestamp", "price", "usd_amount", "nonusdc_side"]]

# %% Convert to Pandas
sel_df = sel_df.to_pandas()

# %% Standardize Price
# Standardize price so token1 is always "Yes"
sel_df["price"] = np.where(
    sel_df["nonusdc_side"] == "token2", 1 - sel_df["price"], sel_df["price"]
)

# %% Select Final Columns
sel_df = sel_df[["timestamp", "price", "usd_amount"]]

# %% Create OHLCV Data
sel_df = sel_df.set_index("timestamp")

# Resample to 10-minute bars
ohlcv = sel_df.resample("10min").agg(
    {"price": ["first", "max", "min", "last"], "usd_amount": "sum"}
)

# Flatten column names
ohlcv.columns = ["open", "high", "low", "close", "volume"]

# Reset index
ohlcv = ohlcv.reset_index()

# %% Clean Data
ohlcv = ohlcv.dropna()

# %% Filter Date Range
# Focus on October-November 2024 (election period)
ohlcv = ohlcv[ohlcv["timestamp"] >= "2024-10-01"]

# %% Remove Broken Data Point (if any)
# Remove specific broken data for aesthetic purposes
ohlcv = ohlcv[ohlcv["timestamp"] != "2024-10-24 21:00:00"]

print(f"OHLCV data shape: {ohlcv.shape}")
print(f"\nDate range: {ohlcv['timestamp'].min()} to {ohlcv['timestamp'].max()}")

# %% Preview Data
print("\nFirst few rows:")
print(ohlcv.head())

print("\nLast few rows:")
print(ohlcv.tail())

# %% Define Strategy
class SimpleMAStrategy(bt.Strategy):
    params = (

        ("fast_period", 50),
        ("slow_period", 200),
        ("stop_loss", 0.02),  # 2% stop loss
        ("trailing_stop", False),  # Enable trailing stop
        ("risk_percent", 0.95),  # Use 95% of portfolio
    )

    def __init__(self):
        self.fast_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.fast_period
        )
        self.slow_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.slow_period
        )
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)
        self.crossover.plotinfo.plot = False
        self.crossover.plotinfo.plotmaster = self.data

        self.order = None
        self.buy_price = None

    def next(self):
        # Check stop loss first if we have a position
        if self.position:
            # Calculate stop loss price
            stop_price = self.buy_price * (1 - self.params.stop_loss)

            # Exit on stop loss
            if self.data.close[0] <= stop_price:
                self.close()
                return

            # Exit on MA crossover
            if self.crossover < 0:
                self.close()
                return

        # Entry logic
        if not self.position:
            if self.crossover > 0:
                # Calculate position size
                cash = self.broker.getcash() * self.params.risk_percent
                size = cash / self.data.close[0]
                self.buy(size=size)
                self.buy_price = self.data.close[0]

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.buy_price = order.executed.price


# %% Run Backtest
# Create cerebro engine
cerebro = bt.Cerebro()

# Prepare data - ensure timestamp is datetime and set as index
ohlcv_bt = ohlcv.copy()
ohlcv_bt["timestamp"] = pd.to_datetime(ohlcv_bt["timestamp"])  # Convert to datetime
ohlcv_bt = ohlcv_bt.set_index("timestamp")  # Set as index

# Add data feed using built-in PandasData
data = bt.feeds.PandasData(dataname=ohlcv_bt, openinterest=-1)
cerebro.adddata(data, name="Will Donald Trump win the 2024 US Presidential Election")

# Add strategy
cerebro.addstrategy(SimpleMAStrategy)

# Set initial cash
cerebro.broker.setcash(10000.0)

# Set commission (0% for this example)
cerebro.broker.setcommission(commission=0)

# Print starting conditions
print(f"\nStarting Portfolio Value: ${cerebro.broker.getvalue():.2f}")

# Run backtest
results = cerebro.run()

# Print final results
final_value = cerebro.broker.getvalue()
initial_value = 10000.0
pnl = final_value - initial_value
pnl_pct = (pnl / initial_value) * 100

print(f"Final Portfolio Value: ${final_value:.2f}")
print(f"P&L: ${pnl:.2f} ({pnl_pct:.2f}%)")

# %% Generate Plot
# Create Bokeh plot with dark theme
scheme = Blackly()

b = Bokeh(
    style="bar",
    plot_mode="single",
    scheme=scheme,
    output_mode="save",
    filename="processed/plot.html",
)

cerebro.plot(b)

print("\nPlot saved to: processed/plot.html")

# %% Summary Statistics
print("\n=== BACKTEST SUMMARY ===")
print(f"Strategy: Simple MA Crossover (Fast: 50, Slow: 200)")
print(f"Market: Trump 2024 Election")
print(f"Period: {ohlcv['timestamp'].min()} to {ohlcv['timestamp'].max()}")
print(f"Total Bars: {len(ohlcv)}")
print(f"Initial Capital: ${initial_value:,.2f}")
print(f"Final Capital: ${final_value:,.2f}")
print(f"Net P&L: ${pnl:,.2f}")
print(f"Return: {pnl_pct:.2f}%")
