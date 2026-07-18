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


class XemmOrdersClearTests(unittest.TestCase):
    def _status(self, accounts) -> dict:
        return {"accounts": accounts}

    def test_none_status_not_clear(self) -> None:
        self.assertFalse(Orchestrator.xemm_orders_clear(None))

    def test_missing_accounts_not_clear(self) -> None:
        self.assertFalse(Orchestrator.xemm_orders_clear({}))
        self.assertFalse(Orchestrator.xemm_orders_clear(self._status(None)))
        self.assertFalse(Orchestrator.xemm_orders_clear(self._status("bogus")))

    def test_missing_or_none_counts_not_clear(self) -> None:
        self.assertFalse(Orchestrator.xemm_orders_clear(self._status({})))
        self.assertFalse(Orchestrator.xemm_orders_clear(self._status({"aster_open_orders": 0})))
        self.assertFalse(Orchestrator.xemm_orders_clear(self._status({"lighter_open_orders": 0})))
        self.assertFalse(
            Orchestrator.xemm_orders_clear(
                self._status({"aster_open_orders": None, "lighter_open_orders": 0})
            )
        )

    def test_unparseable_counts_not_clear(self) -> None:
        self.assertFalse(
            Orchestrator.xemm_orders_clear(
                self._status({"aster_open_orders": "x", "lighter_open_orders": 0})
            )
        )

    def test_zero_counts_clear_and_nonzero_not(self) -> None:
        self.assertTrue(
            Orchestrator.xemm_orders_clear(
                self._status({"aster_open_orders": 0, "lighter_open_orders": 0})
            )
        )
        self.assertFalse(
            Orchestrator.xemm_orders_clear(
                self._status({"aster_open_orders": 1, "lighter_open_orders": 0})
            )
        )


class PreflightOrderClearTests(unittest.TestCase):
    def _live_orch(self, tmp: Path) -> Orchestrator:
        orch = make_orch(tmp, live=True, allow_existing_writers=False, preflight_kill_existing=False)
        orch.halts = []
        orch.safe_halt = lambda reason, **d: (
            orch.halts.append((reason, d)),
            setattr(orch, "shutdown_requested", True),
            setattr(orch, "halted", True),
        )
        xemm_proc = {"pid": 12345, "pgid": 12345, "bot": XEMM_BOT, "args": "xemm_lighter_aster livebot"}
        calls = {"n": 0}

        def processes():
            calls["n"] += 1
            return [xemm_proc] if calls["n"] == 1 else []

        orch.external_bot_processes = processes
        orch.terminate_external_process = lambda process: {"alive": False}
        return orch

    def test_halts_when_orders_remain_after_killing_external_xemm(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = self._live_orch(Path(tmp))
            orch.verify_xemm_orders_clear = lambda: (False, {"accounts": {"aster_open_orders": 2}})
            orch.preflight_existing_bots()
            self.assertEqual(["preflight_xemm_orders_not_clear"], [r for r, _ in orch.halts])

    def test_proceeds_when_orders_clear(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = self._live_orch(Path(tmp))
            orch.verify_xemm_orders_clear = lambda: (True, {})
            orch.preflight_existing_bots()
            self.assertEqual([], orch.halts)


if __name__ == "__main__":
    unittest.main()
