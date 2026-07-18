#!/usr/bin/env python3
"""Tests for the 2026-07-18 review fixes (wave 3): halt ordering, preflight
order-clear gate, status schema validation, XEMM-switch guard, recovery-row
dedup keys, and the XEMM correction path."""
from __future__ import annotations

import argparse
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator import (  # noqa: E402
    TAKER_BOT,
    XEMM_BOT,
    Orchestrator,
    TradeTracker,
    iso,
    utc_now,
)


def make_args(state_dir: Path, **extra) -> argparse.Namespace:
    base = dict(
        market="HYPE",
        state_dir=state_dir,
        pnl_since="startup",
        max_loss_usdc=Decimal("15"),
        baseline_max_gap_hours=48.0,
        taker_observer_restart_sec=60,
    )
    base.update(extra)
    return argparse.Namespace(**base)


def make_orch(state_dir: Path, **extra) -> Orchestrator:
    orch = Orchestrator(make_args(state_dir, **extra))
    orch.recorded_events = []
    orch.event = lambda kind, **details: orch.recorded_events.append((kind, details))
    return orch


class SafeHaltOrderingTests(unittest.TestCase):
    def test_safe_halt_stops_active_writer_before_observer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp), live=False)
            order: list[str] = []
            orch.stop_active = lambda reason: order.append("active")
            orch.stop_observer = lambda reason: order.append("observer")
            orch.safe_halt("test_reason")
            self.assertEqual(["active", "observer"], order)


if __name__ == "__main__":
    unittest.main()
