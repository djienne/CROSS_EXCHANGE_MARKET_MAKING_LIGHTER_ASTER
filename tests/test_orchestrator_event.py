#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator import Orchestrator  # noqa: E402


def make_orch(state_dir: Path) -> Orchestrator:
    args = argparse.Namespace(
        market="HYPE",
        state_dir=state_dir,
        pnl_since="startup",
        max_loss_usdc=Decimal("15"),
        baseline_max_gap_hours=48.0,
        taker_observer_restart_sec=60,
    )
    return Orchestrator(args)


class EventNeverThrowsTests(unittest.TestCase):
    def test_event_survives_unwritable_events_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            blocked = Path(tmp) / "events_dir"
            blocked.mkdir()
            orch.events_path = blocked  # open("a") on a directory raises
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                orch.event("test_kind", detail=1)
            self.assertEqual(1, orch.event_write_failures)
            self.assertFalse(orch.shutdown_requested)
            self.assertIn("event write failed", stderr.getvalue())

    def test_persistent_event_write_failures_request_shutdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            blocked = Path(tmp) / "events_dir"
            blocked.mkdir()
            orch.events_path = blocked
            with contextlib.redirect_stderr(io.StringIO()):
                for _ in range(9):
                    orch.event("test_kind")
                self.assertFalse(orch.shutdown_requested)
                orch.event("test_kind")
            self.assertTrue(orch.shutdown_requested)

    def test_successful_write_resets_failure_counter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            good_path = orch.events_path
            blocked = Path(tmp) / "events_dir"
            blocked.mkdir()
            orch.events_path = blocked
            with contextlib.redirect_stderr(io.StringIO()):
                for _ in range(5):
                    orch.event("test_kind")
            self.assertEqual(5, orch.event_write_failures)
            orch.events_path = good_path
            orch.event("test_kind")
            self.assertEqual(0, orch.event_write_failures)
            self.assertFalse(orch.shutdown_requested)

    def test_event_survives_unserializable_detail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            with contextlib.redirect_stderr(io.StringIO()):
                orch.event("test_kind", weird=object())  # json_default raises TypeError
            self.assertEqual(1, orch.event_write_failures)

    def test_event_survives_closed_stdout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            closed = io.StringIO()
            closed.close()
            with contextlib.redirect_stdout(closed):
                orch.event("test_kind", detail=1)  # print raises ValueError, not OSError
            rows = orch.events_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual("test_kind", json.loads(rows[-1])["kind"])
            self.assertEqual(0, orch.event_write_failures)


if __name__ == "__main__":
    unittest.main()
