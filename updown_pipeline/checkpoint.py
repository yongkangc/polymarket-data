"""
Checkpoint manager for pipeline stages.
Tracks completion status and enables resume capability.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any


class CheckpointManager:
    """Manages pipeline stage checkpoints"""

    def __init__(self, checkpoint_dir: Path):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        self.checkpoints = {
            'markets': self.checkpoint_dir / 'markets.done',
            'historical': self.checkpoint_dir / 'historical.done',
            'enriched': self.checkpoint_dir / 'enriched.done',
        }

    def exists(self, stage: str) -> bool:
        """Check if a checkpoint exists"""
        checkpoint_file = self.checkpoints.get(stage)
        if not checkpoint_file:
            raise ValueError(f"Unknown stage: {stage}")
        return checkpoint_file.exists()

    def mark_done(self, stage: str, metadata: Optional[Dict[str, Any]] = None):
        """Mark a stage as complete with metadata"""
        checkpoint_file = self.checkpoints.get(stage)
        if not checkpoint_file:
            raise ValueError(f"Unknown stage: {stage}")

        data = {
            'stage': stage,
            'completed_at': datetime.now(timezone.utc).isoformat(),
            'status': 'success',
            'metadata': metadata or {}
        }

        with open(checkpoint_file, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"✓ Checkpoint saved: {stage}")

    def get_metadata(self, stage: str) -> Optional[Dict[str, Any]]:
        """Get checkpoint metadata"""
        checkpoint_file = self.checkpoints.get(stage)
        if not checkpoint_file or not checkpoint_file.exists():
            return None

        with open(checkpoint_file, 'r') as f:
            data = json.load(f)
            return data

    def get_timestamp(self, stage: str) -> Optional[datetime]:
        """Get checkpoint timestamp"""
        metadata = self.get_metadata(stage)
        if not metadata:
            return None

        timestamp_str = metadata.get('completed_at')
        if not timestamp_str:
            return None

        return datetime.fromisoformat(timestamp_str)

    def is_recent(self, stage: str, hours: int = 1) -> bool:
        """Check if checkpoint is recent (within N hours)"""
        timestamp = self.get_timestamp(stage)
        if not timestamp:
            return False

        now = datetime.now(timezone.utc)
        age_hours = (now - timestamp).total_seconds() / 3600
        return age_hours < hours

    def all_phase1_complete(self) -> bool:
        """Check if all Phase 1 stages are complete"""
        return all(self.exists(stage) for stage in ['markets', 'historical', 'enriched'])

    def clear(self, stage: Optional[str] = None):
        """Clear checkpoint(s)"""
        if stage:
            checkpoint_file = self.checkpoints.get(stage)
            if checkpoint_file and checkpoint_file.exists():
                checkpoint_file.unlink()
                print(f"✓ Cleared checkpoint: {stage}")
        else:
            # Clear all
            for checkpoint_file in self.checkpoints.values():
                if checkpoint_file.exists():
                    checkpoint_file.unlink()
            print("✓ Cleared all checkpoints")

    def print_status(self):
        """Print checkpoint status"""
        print("\n" + "="*60)
        print("CHECKPOINT STATUS")
        print("="*60)

        for stage, checkpoint_file in self.checkpoints.items():
            if checkpoint_file.exists():
                metadata = self.get_metadata(stage)
                timestamp = self.get_timestamp(stage)
                status = "✅ DONE"
                age = ""
                if timestamp:
                    now = datetime.now(timezone.utc)
                    age_hours = (now - timestamp).total_seconds() / 3600
                    if age_hours < 1:
                        age = f"({age_hours*60:.0f}m ago)"
                    else:
                        age = f"({age_hours:.1f}h ago)"

                print(f"{status} {stage:15} {age}")

                # Print key metadata
                if metadata and metadata.get('metadata'):
                    meta = metadata['metadata']
                    if 'markets_found' in meta:
                        print(f"     → Markets: {meta['markets_found']}")
                    if 'trades_found' in meta:
                        print(f"     → Trades: {meta['trades_found']:,}")
                    if 'output_file' in meta:
                        print(f"     → Output: {meta['output_file']}")
            else:
                print(f"⏸️  PENDING {stage}")

        print("="*60 + "\n")
