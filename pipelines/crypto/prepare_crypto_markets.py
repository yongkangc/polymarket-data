#!/usr/bin/env python3
"""
Phase 1: Pre-filter Crypto Markets

Loads markets.csv (256k markets, 105MB) and filters to crypto markets only.
Creates a fast lookup set for Phase 2 streaming.

Output:
- processed/crypto_markets.csv: Full metadata for ~101k crypto markets
- processed/crypto_market_ids.pkl: Fast O(1) lookup set of market IDs
"""

import warnings
warnings.filterwarnings('ignore')

import sys
import os
from pathlib import Path
import polars as pl
import pickle
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from poly_utils.utils import get_markets


def filter_crypto_markets(markets_df: pl.DataFrame) -> pl.DataFrame:
    """
    Filter markets to crypto-related markets only.

    Uses regex pattern matching on question text to identify markets about:
    BTC, ETH, SOL, XRP, ADA, DOT, FIL, BNB, LINK
    """
    crypto_pattern = r'\b(bitcoin|btc|ethereum|eth|solana|sol|xrp|cardano|ada|polkadot|dot|filecoin|fil|binance|bnb|chainlink|link)\b'

    crypto_markets = markets_df.filter(
        pl.col("question").str.to_lowercase().str.contains(crypto_pattern)
    )

    return crypto_markets


def save_crypto_markets(crypto_markets: pl.DataFrame, output_dir: Path):
    """Save crypto markets to CSV and market IDs to pickle file"""

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save full crypto markets CSV
    crypto_csv_path = output_dir / "crypto_markets.csv"
    crypto_markets.write_csv(crypto_csv_path)
    print(f"✓ Saved crypto markets: {crypto_csv_path}")
    print(f"  Size: {crypto_csv_path.stat().st_size / 1024 / 1024:.1f} MB")

    # Create and save market IDs set for fast O(1) lookups
    market_ids = set(crypto_markets['id'].to_list())
    pkl_path = output_dir / "crypto_market_ids.pkl"
    with open(pkl_path, "wb") as f:
        pickle.dump(market_ids, f)
    print(f"✓ Saved market IDs lookup: {pkl_path}")
    print(f"  Size: {pkl_path.stat().st_size / 1024:.1f} KB")
    print(f"  IDs in set: {len(market_ids):,}")

    return crypto_csv_path, pkl_path


def print_crypto_breakdown(crypto_markets: pl.DataFrame):
    """Print breakdown of crypto markets by asset"""
    print("\n📊 Crypto Markets Breakdown:")

    assets = {
        "Bitcoin (BTC)": r'\b(bitcoin|btc)\b',
        "Ethereum (ETH)": r'\b(ethereum|eth)\b',
        "Solana (SOL)": r'\b(solana|sol)\b',
        "XRP": r'\bxrp\b',
        "Cardano (ADA)": r'\b(cardano|ada)\b',
        "Polkadot (DOT)": r'\b(polkadot|dot)\b',
        "Filecoin (FIL)": r'\b(filecoin|fil)\b',
        "Binance (BNB)": r'\b(binance|bnb)\b',
        "Chainlink (LINK)": r'\b(chainlink|link)\b',
    }

    for asset_name, pattern in assets.items():
        count = crypto_markets.filter(
            pl.col("question").str.to_lowercase().str.contains(pattern)
        ).height
        print(f"  {asset_name:20s} {count:7,} markets")


def main():
    print("="*60)
    print("PHASE 1: Prepare Crypto Markets")
    print("="*60)

    start_time = datetime.now()

    # 1. Load all markets
    print("\n📂 Loading markets.csv...")
    markets = get_markets()
    print(f"✓ Loaded {len(markets):,} total markets")

    # 2. Filter to crypto markets
    print("\n🔍 Filtering crypto markets...")
    print("   Pattern: bitcoin|btc|ethereum|eth|solana|sol|xrp|cardano|ada|polkadot|dot|filecoin|fil|binance|bnb|chainlink|link")
    crypto_markets = filter_crypto_markets(markets)
    print(f"✓ Found {len(crypto_markets):,} crypto markets ({len(crypto_markets)/len(markets)*100:.1f}% of total)")

    # 3. Print breakdown
    print_crypto_breakdown(crypto_markets)

    # 4. Save outputs
    print("\n💾 Saving outputs...")
    project_root = Path(__file__).parent.parent.parent
    output_dir = project_root / "data" / "processed"

    crypto_csv, pkl_path = save_crypto_markets(crypto_markets, output_dir)

    # 5. Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    print("\n" + "="*60)
    print(f"✅ Phase 1 Complete in {elapsed:.1f} seconds")
    print("="*60)
    print(f"\nOutputs:")
    print(f"  • {crypto_csv}")
    print(f"  • {pkl_path}")
    print(f"\nNext step: Run Phase 2 (process_crypto_trades.py)")


if __name__ == "__main__":
    main()
