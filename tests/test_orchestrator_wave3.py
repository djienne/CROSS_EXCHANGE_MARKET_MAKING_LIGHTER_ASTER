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


VALID_TAKER_STATUS = {"market": "HYPE", "positions": {}, "accounts": {}, "opportunities": []}
VALID_XEMM_STATUS = {"market": "HYPE", "reduce_position_only": True, "positions": {}, "accounts": {}}


class StatusSchemaTests(unittest.TestCase):
    def _orch(self, tmp: Path) -> Orchestrator:
        return make_orch(
            tmp,
            live=False,
            taker_bin="/bin/true",
            taker_config="cfg.toml",
            taker_repo=tmp,
            status_timeout_sec=5,
        )

    def test_valid_shapes_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = self._orch(Path(tmp))
            self.assertTrue(orch.status_schema_valid(TAKER_BOT, VALID_TAKER_STATUS))
            self.assertTrue(orch.status_schema_valid(XEMM_BOT, VALID_XEMM_STATUS))

    def test_fragments_and_missing_fields_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = self._orch(Path(tmp))
            self.assertFalse(orch.status_schema_valid(TAKER_BOT, None))
            self.assertFalse(orch.status_schema_valid(TAKER_BOT, {"tiny": 1}))
            self.assertFalse(orch.status_schema_valid(TAKER_BOT, {**VALID_TAKER_STATUS, "market": "OTHER"}))
            missing_opps = {k: v for k, v in VALID_TAKER_STATUS.items() if k != "opportunities"}
            self.assertFalse(orch.status_schema_valid(TAKER_BOT, missing_opps))
            missing_reduce = {k: v for k, v in VALID_XEMM_STATUS.items() if k != "reduce_position_only"}
            self.assertFalse(orch.status_schema_valid(XEMM_BOT, missing_reduce))
            self.assertFalse(
                orch.status_schema_valid(XEMM_BOT, {**VALID_XEMM_STATUS, "positions": "bogus"})
            )

    def test_read_status_rejects_surviving_fragment(self) -> None:
        import subprocess
        from unittest import mock

        with tempfile.TemporaryDirectory() as tmp:
            orch = self._orch(Path(tmp))
            fake = subprocess.CompletedProcess(args=[], returncode=0, stdout='{"tiny": 1}', stderr="")
            with mock.patch("orchestrator.subprocess.run", return_value=fake):
                self.assertIsNone(orch.read_status(TAKER_BOT))
            self.assertIn("status_invalid_payload", [k for k, _ in orch.recorded_events])

    def test_read_status_accepts_valid_payload(self) -> None:
        import json
        import subprocess
        from unittest import mock

        with tempfile.TemporaryDirectory() as tmp:
            orch = self._orch(Path(tmp))
            fake = subprocess.CompletedProcess(
                args=[], returncode=0, stdout=json.dumps(VALID_TAKER_STATUS), stderr=""
            )
            with mock.patch("orchestrator.subprocess.run", return_value=fake):
                status = orch.read_status(TAKER_BOT)
            self.assertIsNotNone(status)
            self.assertEqual("HYPE", status["market"])


class ReadyForTakerTests(unittest.TestCase):
    def test_near_flat_alone_is_not_ready_without_taker_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(
                Path(tmp),
                live=False,
                near_flat_notional_usd=Decimal("50"),
                resume_headroom_clips=Decimal("3"),
                resume_margin_clips=Decimal("3"),
            )
            position_status = {"positions": {"abs_position_notional_usd": "1"}}
            ready, details = orch.ready_for_taker(position_status, None)
            self.assertFalse(ready)
            self.assertEqual("taker_status_missing", details["ready_reason"])
            self.assertTrue(details["near_flat"])

    def test_near_flat_with_taker_status_still_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(
                Path(tmp),
                live=False,
                near_flat_notional_usd=Decimal("50"),
                resume_headroom_clips=Decimal("3"),
                resume_margin_clips=Decimal("3"),
            )
            position_status = {"positions": {"abs_position_notional_usd": "1"}}
            taker_status = {
                "desired_notional_usd": "13",
                "margin_buffer_usd": "0",
                "positions": {"headroom_notional_usd": "100"},
                "accounts": {"aster_available_usd": "1000", "lighter_available_usd": "1000"},
                "opportunities": [],
            }
            ready, details = orch.ready_for_taker(position_status, taker_status)
            self.assertTrue(ready)
            self.assertEqual("near_flat", details["ready_reason"])


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
