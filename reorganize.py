#!/usr/bin/env python3
"""
Reorganize poly_data directory structure (Option B - Minimal Change)

This script reorganizes the directory to:
1. Group pipelines together
2. Organize data files by type
3. Consolidate utilities
4. Move loose scripts to scripts/

Run with: python reorganize.py --dry-run (to preview)
          python reorganize.py (to execute)
"""
import shutil
from pathlib import Path
import argparse


class Reorganizer:
    def __init__(self, base_dir: Path, dry_run: bool = True):
        self.base_dir = base_dir
        self.dry_run = dry_run
        self.moves = []

    def plan_move(self, source: str, dest: str, reason: str = ""):
        """Plan a file/directory move"""
        src = self.base_dir / source
        dst = self.base_dir / dest

        if src.exists():
            self.moves.append({
                'source': src,
                'dest': dst,
                'reason': reason,
                'is_dir': src.is_dir()
            })
        else:
            print(f"⚠️  Skip (not found): {source}")

    def execute(self):
        """Execute planned moves"""
        print("\n" + "="*70)
        print(f"{'DRY RUN - ' if self.dry_run else ''}REORGANIZATION PLAN")
        print("="*70)

        if not self.moves:
            print("❌ No moves planned!")
            return

        # Group by category
        categories = {
            'pipelines': [],
            'data': [],
            'utils': [],
            'scripts': [],
            'other': []
        }

        for move in self.moves:
            dest_str = str(move['dest'])
            if 'pipelines/' in dest_str:
                categories['pipelines'].append(move)
            elif 'data/' in dest_str:
                categories['data'].append(move)
            elif 'utils/' in dest_str:
                categories['utils'].append(move)
            elif 'scripts/' in dest_str:
                categories['scripts'].append(move)
            else:
                categories['other'].append(move)

        # Print plan
        for category, moves in categories.items():
            if not moves:
                continue

            print(f"\n📁 {category.upper()}")
            for move in moves:
                src_rel = move['source'].relative_to(self.base_dir)
                dst_rel = move['dest'].relative_to(self.base_dir)
                icon = "📂" if move['is_dir'] else "📄"
                print(f"  {icon} {src_rel} → {dst_rel}")
                if move['reason']:
                    print(f"     {move['reason']}")

        print(f"\n{'='*70}")
        print(f"Total moves: {len(self.moves)}")
        print(f"{'='*70}\n")

        if self.dry_run:
            print("🔍 DRY RUN - No changes made")
            print("   Run without --dry-run to execute")
            return

        # Execute moves
        print("Executing moves...")

        for i, move in enumerate(self.moves, 1):
            src = move['source']
            dst = move['dest']

            try:
                # Create parent directory
                dst.parent.mkdir(parents=True, exist_ok=True)

                # Move
                shutil.move(str(src), str(dst))
                print(f"  [{i}/{len(self.moves)}] ✓ Moved: {src.name}")

            except Exception as e:
                print(f"  [{i}/{len(self.moves)}] ❌ Error moving {src.name}: {e}")

        print(f"\n✅ Reorganization complete!")


def main():
    parser = argparse.ArgumentParser(description="Reorganize poly_data directory")
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without executing')
    parser.add_argument('--execute', action='store_true', help='Execute the reorganization')
    args = parser.parse_args()

    # Default to dry-run unless --execute is specified
    dry_run = not args.execute

    base_dir = Path(__file__).parent
    reorganizer = Reorganizer(base_dir, dry_run=dry_run)

    print("Planning reorganization...")

    # ========================================================================
    # PIPELINES: Group all pipeline code
    # ========================================================================

    # Move full pipeline
    reorganizer.plan_move(
        'update_utils',
        'pipelines/full',
        'Full pipeline (market updates, goldsky, processing)'
    )

    # Move updown pipeline (already exists but organize better)
    reorganizer.plan_move(
        'updown_pipeline',
        'pipelines/updown',
        'Up/down market pipeline'
    )

    # ========================================================================
    # DATA: Organize data files
    # ========================================================================

    # Raw data sources
    reorganizer.plan_move(
        'goldsky',
        'data/raw/goldsky',
        'Raw blockchain data (68GB orderFilled.csv)'
    )

    reorganizer.plan_move(
        'markets.csv',
        'data/raw/markets.csv',
        'Market metadata from Polymarket API'
    )

    # Move data/binance files to data/raw/binance
    reorganizer.plan_move(
        'data/binance_complete_minute_data.csv',
        'data/raw/binance/complete_minute_data.csv',
        'Binance complete minute data'
    )

    # Updown pipeline outputs - already in data/, just note location
    # data/updown_markets.csv, data/.checkpoints/ stay where they are

    # ========================================================================
    # UTILS: Consolidate utilities
    # ========================================================================

    # Keep poly_utils as is (utils/poly_utils)
    # Move price scripts to utils/price_fetchers
    reorganizer.plan_move(
        'price',
        'utils/price_fetchers',
        'Price fetching and processing utilities'
    )

    # Move backtrader_plotting to utils
    reorganizer.plan_move(
        'backtrader_plotting',
        'utils/backtrader_plotting',
        'Backtrader plotting utilities'
    )

    # ========================================================================
    # SCRIPTS: Move loose root scripts
    # ========================================================================

    loose_scripts = [
        'aggregate_12gb.py',
        'aggregate_partial.py',
        'compare_data_completeness.py',
        'fetch_wallet_goldsky.py',
        'find_missing_dates.py',
        'list_missing_ranges.py',
        'process_wallet.py',
        'research_updown_markets.py',
        'verify_complete_data.py',
    ]

    for script in loose_scripts:
        reorganizer.plan_move(
            script,
            f'scripts/{script}',
            'Utility script'
        )

    # Shell scripts
    shell_scripts = [
        'run_fetch.sh',
        'run_fetch_10_chunks.sh',
        'run_fill_batch1.sh',
        'run_fill_batch2.sh',
        'run_fill_gaps.sh',
    ]

    for script in shell_scripts:
        reorganizer.plan_move(
            script,
            f'scripts/{script}',
            'Shell script'
        )

    # ========================================================================
    # Execute
    # ========================================================================

    reorganizer.execute()

    if not dry_run:
        # Create README files for new directories
        print("\nCreating README files...")

        readme_content = {
            'pipelines/README.md': """# Pipelines

This directory contains all data pipeline implementations.

## Available Pipelines

- **full/** - Original full pipeline (all markets)
- **updown/** - Up/down market pipeline (BTC/SOL/ETH)

Each pipeline has its own README with usage instructions.
""",
            'data/raw/README.md': """# Raw Data

This directory contains raw input data from various sources.

- **goldsky/** - Blockchain event data (orderFilled.csv)
- **binance/** - Binance price data
- **markets.csv** - Market metadata from Polymarket API

Do not modify files in this directory.
""",
            'utils/README.md': """# Utilities

Shared utility modules and helper functions.

- **poly_utils/** - Polymarket-specific utilities
- **price_fetchers/** - Price data fetching tools
- **backtrader_plotting/** - Plotting and visualization

Import these in your analysis scripts and notebooks.
""",
            'scripts/README.md': """# Scripts

One-off utility scripts for various tasks.

These are typically run manually for specific operations like:
- Data verification
- Gap filling
- Wallet extraction
- Completeness checking

See individual scripts for usage.
"""
        }

        for path, content in readme_content.items():
            readme_path = base_dir / path
            readme_path.parent.mkdir(parents=True, exist_ok=True)
            readme_path.write_text(content)
            print(f"  ✓ Created: {path}")

        print("\n✅ All done!")


if __name__ == '__main__':
    main()
