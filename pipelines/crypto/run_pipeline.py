#!/usr/bin/env python3
"""
Crypto Markets Pipeline Orchestrator

Runs all three phases of the crypto markets pipeline:
1. Prepare crypto markets (filter markets.csv)
2. Process crypto trades (stream and transform orderFilled.csv)
3. Validate crypto trades (generate statistics)

Expected runtime: ~30-40 minutes total
- Phase 1: ~30 seconds
- Phase 2: ~25-35 minutes
- Phase 3: ~1 minute
"""

import warnings
warnings.filterwarnings('ignore')

import sys
import subprocess
from pathlib import Path
from datetime import datetime


def run_phase(phase_num: int, script_name: str, description: str) -> bool:
    """Run a phase script and return success status"""
    print("\n" + "="*70)
    print(f"PHASE {phase_num}: {description}")
    print("="*70)

    script_path = Path(__file__).parent / script_name
    start_time = datetime.now()

    try:
        # Run the phase script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            check=True,
            capture_output=False,
            text=True
        )

        elapsed = (datetime.now() - start_time).total_seconds()
        elapsed_min = elapsed / 60

        print(f"\n✅ Phase {phase_num} completed successfully in {elapsed_min:.1f} minutes")
        return True

    except subprocess.CalledProcessError as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        elapsed_min = elapsed / 60

        print(f"\n❌ Phase {phase_num} failed after {elapsed_min:.1f} minutes")
        print(f"   Error code: {e.returncode}")
        return False

    except KeyboardInterrupt:
        print(f"\n⚠️  Phase {phase_num} interrupted by user")
        return False


def main():
    print("="*70)
    print("CRYPTO MARKETS PIPELINE")
    print("="*70)
    print("\nThis pipeline will:")
    print("  1. Filter markets.csv to crypto markets (~30 seconds)")
    print("  2. Process 279M goldsky events to crypto trades (~25-35 minutes)")
    print("  3. Generate validation statistics (~1 minute)")
    print("\nTotal expected runtime: ~30-40 minutes")
    print("="*70)

    pipeline_start = datetime.now()

    # Phase 1: Prepare crypto markets
    if not run_phase(1, "prepare_crypto_markets.py", "Prepare Crypto Markets"):
        print("\n❌ Pipeline failed at Phase 1")
        return 1

    # Phase 2: Process crypto trades
    if not run_phase(2, "process_crypto_trades.py", "Process Crypto Trades"):
        print("\n❌ Pipeline failed at Phase 2")
        return 1

    # Phase 3: Validate crypto trades
    if not run_phase(3, "validate_crypto_trades.py", "Validate Crypto Trades"):
        print("\n❌ Pipeline failed at Phase 3")
        return 1

    # Pipeline complete
    total_elapsed = (datetime.now() - pipeline_start).total_seconds()
    total_min = total_elapsed / 60

    print("\n" + "="*70)
    print("✅ PIPELINE COMPLETE")
    print("="*70)
    print(f"\nTotal runtime: {total_min:.1f} minutes ({total_elapsed:.0f} seconds)")

    # Output location
    project_root = Path(__file__).parent.parent.parent
    output_path = project_root / "data" / "processed" / "crypto_trades.parquet"
    output_size_gb = output_path.stat().st_size / 1024 / 1024 / 1024

    print(f"\n📦 Output:")
    print(f"   {output_path}")
    print(f"   Size: {output_size_gb:.2f} GB")

    print("\n✨ Crypto trades are ready to use!")
    print("   Next steps:")
    print("   - Load crypto_trades.parquet into your dashboard")
    print("   - Add crypto-only filter toggle to UI")
    print("   - Compare with full trades to verify accuracy")

    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
