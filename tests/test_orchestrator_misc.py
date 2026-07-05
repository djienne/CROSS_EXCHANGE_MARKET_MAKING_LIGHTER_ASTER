#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator import TAKER_OBSERVER, Orchestrator, utc_now  # noqa: E402


def make_orch(state_dir: Path) -> Orchestrator:
    args = argparse.Namespace(
        market="HYPE",
        state_dir=state_dir,
        pnl_since="startup",
        max_loss_usdc=Decimal("15"),
        baseline_max_gap_hours=48.0,
        taker_observer_restart_sec=60,
    )
    orch = Orchestrator(args)
    orch.recorded_events = []
    orch.event = lambda kind, **details: orch.recorded_events.append((kind, details))
    return orch


class StubChild:
    def __init__(self, name: str, uptime_sec: float, code: int = 1):
        self.name = name
        self.started_at = utc_now() - timedelta(seconds=uptime_sec)
        self.log_path = Path("/tmp/stub.log")
        self._code = code

    def poll_exit(self) -> int:
        return self._code

    def is_running(self) -> bool:
        return False


def status(equity: str | None, bot: str) -> dict:
    accounts = {} if equity is None else {"total_equity_usd": equity}
    return {"bot": bot, "accounts": accounts}


class ObserverBackoffTests(unittest.TestCase):
    def test_long_healthy_run_resets_ladder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            orch.observer_exit_count = 5
            orch.children[TAKER_OBSERVER] = StubChild(TAKER_OBSERVER, uptime_sec=3600)
            orch.check_child_exits()
            self.assertEqual(1, orch.observer_exit_count)

    def test_crash_loop_still_escalates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            orch.observer_exit_count = 2
            orch.children[TAKER_OBSERVER] = StubChild(TAKER_OBSERVER, uptime_sec=30)
            orch.check_child_exits()
            self.assertEqual(3, orch.observer_exit_count)


class NotePnlSampleTests(unittest.TestCase):
    def test_equity_starvation_alarm_after_three_misses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            taker = status(None, "TAKER")  # status present, equity missing
            for _ in range(2):
                orch.note_pnl_sample(None, taker, None)
            self.assertEqual([], [k for k, _ in orch.recorded_events])
            orch.note_pnl_sample(None, taker, None)
            self.assertEqual(["equity_feed_starving"], [k for k, _ in orch.recorded_events])
            orch.note_pnl_sample(None, taker, None)  # only fires at exactly 3
            self.assertEqual(1, len(orch.recorded_events))
            orch.note_pnl_sample({"source_bot": "TAKER"}, taker, None)
            self.assertEqual(0, orch.equity_sample_failures)

    def test_no_starvation_count_without_any_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            orch.note_pnl_sample(None, None, None)
            self.assertEqual(0, orch.equity_sample_failures)

    def test_source_switch_emits_event_with_both_equities(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            orch.note_pnl_sample({"source_bot": "A"}, status("100", "A"), None)
            orch.note_pnl_sample({"source_bot": "B"}, status("100", "A"), status("101", "B"))
            switches = [d for k, d in orch.recorded_events if k == "pnl_source_switched"]
            self.assertEqual(1, len(switches))
            self.assertEqual("A", switches[0]["from_bot"])
            self.assertEqual("B", switches[0]["to_bot"])
            self.assertEqual(Decimal("100"), switches[0]["taker_equity_usd"])
            self.assertEqual(Decimal("101"), switches[0]["xemm_equity_usd"])

    def test_equity_divergence_event_throttled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            sample = {"source_bot": "XEMM"}
            far = (status("238.83", "TAKER"), status("230.00", "XEMM"))  # diff 8.83 >= 3
            orch.note_pnl_sample(sample, *far)
            orch.note_pnl_sample(sample, *far)
            events = [k for k, _ in orch.recorded_events if k == "equity_calc_divergence"]
            self.assertEqual(1, len(events))
            orch.last_divergence_event_at = utc_now() - timedelta(hours=1)
            orch.note_pnl_sample(sample, *far)
            events = [k for k, _ in orch.recorded_events if k == "equity_calc_divergence"]
            self.assertEqual(2, len(events))

    def test_no_divergence_event_when_close(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            orch.note_pnl_sample({"source_bot": "XEMM"}, status("238.83", "TAKER"), status("237.50", "XEMM"))
            self.assertEqual([], [k for k, _ in orch.recorded_events if k == "equity_calc_divergence"])


if __name__ == "__main__":
    unittest.main()
