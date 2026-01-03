# Market Monitoring Scripts

Scripts for discovering and monitoring Polymarket "Up or Down" markets.

## Quick Start

### One-Time Market Discovery

Fetch current active markets once:

```bash
cd /mnt/data/poly_data
python scripts/monitor_markets.py --once
```

### Continuous Monitoring

Monitor for new markets every 5 minutes (default):

```bash
python scripts/monitor_markets.py
```

Monitor with custom interval:

```bash
# Check every 15 minutes
python scripts/monitor_markets.py --interval 15

# Check every 1 minute (for testing)
python scripts/monitor_markets.py --interval 1
```

Stop with `Ctrl+C`.

## Output

Markets are saved to: `/mnt/data/poly_data/data/updown_markets.csv`

### Output Columns:

| Column | Description |
|--------|-------------|
| `event_id` | Polymarket event ID |
| `market_id` | Market ID (use for trading/data fetching) |
| `slug` | URL-friendly market identifier |
| `asset` | Crypto asset (BTC, ETH, SOL) |
| `duration` | Market duration (5m, 15m, 1h) |
| `question` | Market question text |
| `start_time` | Market start timestamp (Unix) |
| `end_time` | Market close timestamp (Unix) |
| `yes_token_id` | Token ID for "Yes" side |
| `no_token_id` | Token ID for "No" side |
| `resolution_source` | Data source for settlement |
| `volume` | Trading volume (USD) |
| `active` | Is market currently active? |
| `closed` | Is market closed? |

## Configuration

Edit `/mnt/data/poly_data/updown_pipeline/config.py` to customize:

```python
# Which assets to track
ASSETS = ["BTC", "SOL", "ETH"]

# Which durations to track
DURATIONS = ["5m", "15m", "1h"]

# API endpoints
POLYMARKET_API_BASE = "https://gamma-api.polymarket.com"
```

## Example: View Discovered Markets

```python
import polars as pl

# Load markets
df = pl.read_csv(
    'data/updown_markets.csv',
    schema_overrides={'yes_token_id': pl.String, 'no_token_id': pl.String}
)

# Show summary
print(f"Total markets: {len(df)}")
print(df.group_by('asset').agg(pl.len().alias('count')))

# Filter for active BTC markets
btc_active = df.filter((pl.col('asset') == 'BTC') & pl.col('active'))
print(f"Active BTC markets: {len(btc_active)}")
```

## Use Cases

### 1. Market ID Discovery
Get list of current "Up or Down" market IDs for trading:

```bash
python scripts/monitor_markets.py --once
# → Output: data/updown_markets.csv with market_id column
```

### 2. Continuous Market Tracking
Run as background service to always have latest markets:

```bash
nohup python scripts/monitor_markets.py --interval 10 > monitor.log 2>&1 &
```

### 3. Integration with Trading Bot
Use discovered market IDs to subscribe to orderbook updates:

```python
markets = pl.read_csv('data/updown_markets.csv', schema_overrides={...})
market_ids = markets['market_id'].to_list()

for market_id in market_ids:
    subscribe_to_market(market_id)
```

## What Markets Are Discovered?

Currently discovering **5-minute "Up or Down" markets** only:
- **42 BTC markets** (e.g., "Bitcoin Up or Down - December 28, 3:25PM-3:30PM ET")
- **47 ETH markets** (e.g., "Ethereum Up or Down - December 28, 2:15PM-2:20PM ET")
- **44 SOL markets** (e.g., "Solana Up or Down - December 28, 4:50PM-4:55PM ET")

Total: **133 active markets** (as of last run)

To include 15m and 1h markets, they would need to be added to the API response (Polymarket creates these dynamically).

## Systemd Service (Optional)

To run as a system service:

1. Create service file `/etc/systemd/system/monitor-markets.service`:
```ini
[Unit]
Description=Polymarket Market Monitor
After=network.target

[Service]
Type=simple
User=chiayongtcac
WorkingDirectory=/mnt/data/poly_data
Environment="PATH=/mnt/data/poly_data/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ExecStart=/mnt/data/poly_data/.venv/bin/python scripts/monitor_markets.py --interval 15
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target
```

2. Install and start:
```bash
sudo systemctl enable monitor-markets
sudo systemctl start monitor-markets
sudo systemctl status monitor-markets
```

## Related Scripts

- `updown_pipeline/market_discovery.py` - Core market discovery logic
- `updown_pipeline/stream_live.py` - Full pipeline with trades + prices
- `updown_pipeline/config.py` - Configuration
