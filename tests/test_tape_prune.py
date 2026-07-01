#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator import prune_old_tapes  # noqa: E402


NOW = datetime(2026, 7, 1, 23, 0, 0, tzinfo=timezone.utc)


def make_tape(state_dir: Path, name: str, age_days: float, size: int = 100) -> Path:
    path = state_dir / name
    path.write_bytes(b"x" * size)
    mtime = (NOW - timedelta(days=age_days)).timestamp()
    os.utime(path, (mtime, mtime))
    return path


class TapePruneTests(unittest.TestCase):
    def test_prunes_only_stale_tapes_for_the_market(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            old = make_tape(state_dir, "orchestrator_xemm_HYPE_20260620T000000Z.jsonl.zst", 10, size=300)
            fresh = make_tape(state_dir, "orchestrator_xemm_HYPE_20260630T000000Z.jsonl.zst", 1)
            other_market = make_tape(state_dir, "orchestrator_xemm_ETH_20260620T000000Z.jsonl.zst", 10)
            not_a_tape = make_tape(state_dir, "orchestrator-xemm-HYPE-journal.jsonl", 10)

            pruned = prune_old_tapes(state_dir, "HYPE", 7.0, NOW)

            self.assertEqual([(old, 300)], pruned)
            self.assertFalse(old.exists())
            self.assertTrue(fresh.exists())
            self.assertTrue(other_market.exists())
            self.assertTrue(not_a_tape.exists())

    def test_zero_retention_disables_pruning(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            old = make_tape(state_dir, "orchestrator_xemm_HYPE_20260101T000000Z.jsonl.zst", 180)

            self.assertEqual([], prune_old_tapes(state_dir, "HYPE", 0, NOW))
            self.assertTrue(old.exists())


if __name__ == "__main__":
    unittest.main()
