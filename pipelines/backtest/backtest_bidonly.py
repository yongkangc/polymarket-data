"""
Backtest Engine for Bid-Only Market Making Strategy

This backtest simulates the bid-only strategy on December 2024 Polymarket data:
1. ENTRY: Post bids on BOTH token1 (YES) and token2 (NO) at target prices
2. HOLD: Accept fills, accumulate positions
3. EXIT: Sell expensive tokens (>$0.70) in last 15 minutes
4. RESOLUTION: Collect $1 for winning tokens, $0 for losers

The backtest uses actual trade data to determine realistic fill prices and timing.
"""
import polars as pl
import numpy as np
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime


BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
TRADES_FILE = DATA_DIR / "december_trades.parquet"


@dataclass
class StrategyParams:
    """Strategy parameters based on actual trader behavior"""
    # Entry parameters (actual trader: mean $0.32, median $0.30)
    max_bid_price: float = 0.50           # Max price to bid (75th percentile)
    
    # Exit parameters (actual trader: mean $0.65)
    min_sell_price: float = 0.50          # Min price to sell
    exit_window_sec: int = 900            # Last 15 minutes (84% of sells)
    
    # Position sizing - scale factor relative to actual trader
    position_scale: float = 0.01          # 1% of actual trader's position sizes
    
    # Capital
    starting_capital: float = 10000.0


@dataclass
class Position:
    """Track position in a single token"""
    token_amount: float = 0.0
    cost_basis: float = 0.0
    sell_proceeds: float = 0.0
    tokens_sold: float = 0.0
    
    @property
    def tokens_held(self) -> float:
        return self.token_amount - self.tokens_sold
    
    @property
    def avg_buy_price(self) -> float:
        if self.token_amount == 0:
            return 0.0
        return self.cost_basis / self.token_amount


@dataclass
class MarketPosition:
    """Track position in a market (both token1 and token2)"""
    market_id: int
    crypto_type: str
    token1: Position = field(default_factory=Position)
    token2: Position = field(default_factory=Position)
    winning_token: Optional[str] = None


@dataclass
class BacktestResult:
    """Results from backtest"""
    total_pnl: float
    hypothetical_pnl: float  # If never sold
    selling_edge: float
    
    total_volume: float
    num_markets: int
    num_trades: int
    
    # By outcome
    winner_tokens_bought: float
    loser_tokens_bought: float
    winner_tokens_sold: float
    loser_tokens_sold: float
    
    # By crypto
    pnl_by_crypto: Dict[str, float]
    
    # Win rate
    winning_markets: int
    losing_markets: int


class BidOnlyBacktest:
    """
    Backtest engine for bid-only market making strategy.
    
    The strategy:
    1. For each market, bid on BOTH token1 and token2
    2. Only buy at prices below max_bid_price
    3. Sell tokens in last 15 minutes if price > min_sell_price
    4. Never sell more than bought (stay long both sides)
    """
    
    def __init__(self, params: StrategyParams = None):
        self.params = params or StrategyParams()
        self.positions: Dict[int, MarketPosition] = {}
        self.trades_log: List[Dict] = []
        
    def should_buy(self, price: float, time_to_close: int) -> bool:
        """Determine if we should buy at this price - replicate all buys"""
        return True  # Replicate all trader buys
    
    def should_sell(self, price: float, time_to_close: int, tokens_held: float) -> bool:
        """Determine if we should sell - replicate all sells"""
        return tokens_held > 0  # Replicate all trader sells if we have tokens
    
    def process_trade(self, trade: Dict) -> Optional[Dict]:
        """
        Replicate the trader's exact trades at a scaled position size.
        
        The trader's actual behavior:
        1. BUY (both MAKER 55% and TAKER 8%) at various prices
        2. SELL as TAKER (37%) at higher prices
        
        We replicate ALL their trades scaled by position_scale factor.
        """
        market_id = trade['market_id']
        token_side = trade['nonusdc_side']  # 'token1' or 'token2'
        price = trade['price']
        time_to_close = trade['time_to_close_sec']
        crypto_type = trade['crypto_type']
        trader_side = trade['trader_side']  # BUY or SELL
        trader_role = trade['trader_role']  # MAKER or TAKER
        token_amount = trade['token_amount']
        
        # Initialize market position if needed
        if market_id not in self.positions:
            self.positions[market_id] = MarketPosition(
                market_id=market_id,
                crypto_type=crypto_type,
                winning_token=trade.get('winning_token')
            )
        
        pos = self.positions[market_id]
        token_pos = pos.token1 if token_side == 'token1' else pos.token2
        
        action = None
        amount = 0.0
        
        # REPLICATE ALL BUYS: Both MAKER and TAKER buys
        if trader_side == 'BUY':
            # Scale the trader's actual trade
            amount = token_amount * self.params.position_scale
            if amount > 0:
                token_pos.token_amount += amount
                token_pos.cost_basis += amount * price
                action = 'BUY'
        
        # REPLICATE SELL: Follow trader's TAKER sells
        elif trader_side == 'SELL':
            # Scale the sell proportionally - but don't sell more than we have
            amount = min(token_pos.tokens_held, token_amount * self.params.position_scale)
            if amount > 0:
                token_pos.tokens_sold += amount
                token_pos.sell_proceeds += amount * price
                action = 'SELL'
        
        if action:
            return {
                'timestamp': trade['timestamp'],
                'market_id': market_id,
                'token': token_side,
                'action': action,
                'price': price,
                'amount': amount,
                'usd': amount * price,
                'crypto_type': crypto_type,
            }
        
        return None
    
    def calculate_pnl(self) -> BacktestResult:
        """Calculate final P&L after all trades processed"""
        total_pnl = 0.0
        hypothetical_pnl = 0.0
        total_volume = 0.0
        pnl_by_crypto = {}
        
        winner_bought = 0.0
        loser_bought = 0.0
        winner_sold = 0.0
        loser_sold = 0.0
        
        winning_markets = 0
        losing_markets = 0
        
        for market_id, pos in self.positions.items():
            market_pnl = 0.0
            market_hypothetical = 0.0
            
            for token_side, token_pos in [('token1', pos.token1), ('token2', pos.token2)]:
                if token_pos.token_amount == 0:
                    continue
                    
                is_winner = (pos.winning_token == token_side)
                
                # Resolution value
                resolution_value = token_pos.tokens_held * (1.0 if is_winner else 0.0)
                
                # Actual PNL = resolution_value + sell_proceeds - cost_basis
                actual_pnl = resolution_value + token_pos.sell_proceeds - token_pos.cost_basis
                
                # Hypothetical PNL (if never sold)
                hypo_resolution = token_pos.token_amount * (1.0 if is_winner else 0.0)
                hypo_pnl = hypo_resolution - token_pos.cost_basis
                
                market_pnl += actual_pnl
                market_hypothetical += hypo_pnl
                total_volume += token_pos.cost_basis + token_pos.sell_proceeds
                
                # Track winner/loser flows
                if is_winner:
                    winner_bought += token_pos.token_amount
                    winner_sold += token_pos.tokens_sold
                else:
                    loser_bought += token_pos.token_amount
                    loser_sold += token_pos.tokens_sold
            
            total_pnl += market_pnl
            hypothetical_pnl += market_hypothetical
            
            # Track by crypto
            crypto = pos.crypto_type
            if crypto not in pnl_by_crypto:
                pnl_by_crypto[crypto] = 0.0
            pnl_by_crypto[crypto] += market_pnl
            
            if market_pnl > 0:
                winning_markets += 1
            elif market_pnl < 0:
                losing_markets += 1
        
        return BacktestResult(
            total_pnl=total_pnl,
            hypothetical_pnl=hypothetical_pnl,
            selling_edge=total_pnl - hypothetical_pnl,
            total_volume=total_volume,
            num_markets=len(self.positions),
            num_trades=len(self.trades_log),
            winner_tokens_bought=winner_bought,
            loser_tokens_bought=loser_bought,
            winner_tokens_sold=winner_sold,
            loser_tokens_sold=loser_sold,
            pnl_by_crypto=pnl_by_crypto,
            winning_markets=winning_markets,
            losing_markets=losing_markets,
        )
    
    def run(self, trades_df: pl.DataFrame) -> BacktestResult:
        """Run the backtest on trade data"""
        print("=" * 70)
        print("RUNNING BID-ONLY BACKTEST")
        print("=" * 70)
        
        print(f"\nStrategy Parameters:")
        print(f"  Max bid price: ${self.params.max_bid_price:.2f}")
        print(f"  Min sell price: ${self.params.min_sell_price:.2f}")
        print(f"  Exit window: {self.params.exit_window_sec}s ({self.params.exit_window_sec/60:.0f} min)")
        print(f"  Position scale: {self.params.position_scale*100:.1f}% of actual trader")
        
        # Sort by timestamp
        trades_df = trades_df.sort('timestamp')
        
        print(f"\n→ Processing {len(trades_df):,} trades...")
        
        processed = 0
        for trade in trades_df.iter_rows(named=True):
            result = self.process_trade(trade)
            if result:
                self.trades_log.append(result)
                processed += 1
            
            if processed % 50000 == 0 and processed > 0:
                print(f"   {processed:,} trades taken...")
        
        print(f"   Total trades taken: {len(self.trades_log):,}")
        
        # Calculate results
        print(f"\n→ Calculating P&L...")
        result = self.calculate_pnl()
        
        return result


def print_results(result: BacktestResult):
    """Print backtest results"""
    print("\n" + "=" * 70)
    print("BACKTEST RESULTS")
    print("=" * 70)
    
    print(f"\n📊 OVERALL PERFORMANCE:")
    print(f"   Total P&L:        ${result.total_pnl:,.2f}")
    print(f"   Hypothetical P&L: ${result.hypothetical_pnl:,.2f} (if never sold)")
    print(f"   Selling Edge:     ${result.selling_edge:,.2f}")
    
    if result.hypothetical_pnl != 0:
        edge1_pct = result.hypothetical_pnl / result.total_pnl * 100 if result.total_pnl != 0 else 0
        edge2_pct = result.selling_edge / result.total_pnl * 100 if result.total_pnl != 0 else 0
        print(f"\n📈 TWO-EDGE DECOMPOSITION:")
        print(f"   Edge 1 (Pricing): ${result.hypothetical_pnl:,.2f} ({edge1_pct:.0f}%)")
        print(f"   Edge 2 (Selling): ${result.selling_edge:,.2f} ({edge2_pct:.0f}%)")
    
    print(f"\n📉 TRADING STATS:")
    print(f"   Total Volume:   ${result.total_volume:,.2f}")
    print(f"   Markets Traded: {result.num_markets:,}")
    print(f"   Trades Taken:   {result.num_trades:,}")
    
    if result.winning_markets + result.losing_markets > 0:
        win_rate = result.winning_markets / (result.winning_markets + result.losing_markets) * 100
        print(f"\n📊 WIN RATE:")
        print(f"   Winning Markets: {result.winning_markets:,}")
        print(f"   Losing Markets:  {result.losing_markets:,}")
        print(f"   Win Rate:        {win_rate:.1f}%")
    
    print(f"\n🔄 TOKEN FLOWS:")
    print(f"   Winner tokens bought: {result.winner_tokens_bought:,.0f}")
    print(f"   Winner tokens sold:   {result.winner_tokens_sold:,.0f}")
    print(f"   Loser tokens bought:  {result.loser_tokens_bought:,.0f}")
    print(f"   Loser tokens sold:    {result.loser_tokens_sold:,.0f}")
    
    if result.loser_tokens_bought > 0 and result.winner_tokens_bought > 0:
        adverse = (result.loser_tokens_bought - result.loser_tokens_sold) / (result.winner_tokens_bought - result.winner_tokens_sold)
        print(f"   Adverse Selection:    {adverse:.2f}x more losers held")
    
    print(f"\n💰 P&L BY CRYPTO:")
    for crypto, pnl in sorted(result.pnl_by_crypto.items()):
        print(f"   {crypto}: ${pnl:,.2f}")


def run_backtest(
    max_bid: float = 0.50,
    min_sell: float = 0.50,
    position_scale: float = 0.01,
    filter_crypto: List[str] = None
):
    """Run backtest with specified parameters"""
    # Load data
    print(f"→ Loading trades from {TRADES_FILE}...")
    trades = pl.read_parquet(TRADES_FILE)
    print(f"  Loaded {len(trades):,} trades")
    
    # Filter by crypto if specified
    if filter_crypto:
        trades = trades.filter(pl.col('crypto_type').is_in(filter_crypto))
        print(f"  Filtered to {filter_crypto}: {len(trades):,} trades")
    
    # Create strategy
    params = StrategyParams(
        max_bid_price=max_bid,
        min_sell_price=min_sell,
        position_scale=position_scale,
    )
    
    backtest = BidOnlyBacktest(params)
    result = backtest.run(trades)
    
    print_results(result)
    
    return result


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run bid-only backtest')
    parser.add_argument('--max-bid', type=float, default=0.50, help='Max bid price')
    parser.add_argument('--min-sell', type=float, default=0.50, help='Min sell price')
    parser.add_argument('--position-scale', type=float, default=0.01, help='Position scale factor')
    parser.add_argument('--crypto', nargs='+', default=None, help='Filter to specific cryptos')
    
    args = parser.parse_args()
    
    run_backtest(
        max_bid=args.max_bid,
        min_sell=args.min_sell,
        position_scale=args.position_scale,
        filter_crypto=args.crypto,
    )

