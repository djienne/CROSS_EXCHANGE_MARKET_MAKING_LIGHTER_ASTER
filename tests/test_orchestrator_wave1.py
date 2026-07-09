#!/usr/bin/env python3
"""Tests for the 2026-07-09 Wave-1 orchestrator fixes: switch hysteresis, mid-tick
exit handling, crash-loop guard, incremental trade reads, and retention."""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator import (  # noqa: E402
    TAKER_BOT,
    XEMM_BOT,
    Orchestrator,
    PnlTracker,
    TradeTracker,
    iso,
    prune_old_logs,
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


class StubChild:
    def __init__(self, name: str, uptime_sec: float, code: int | None = 0):
        self.name = name
        self.started_at = utc_now() - timedelta(seconds=uptime_sec)
        self.log_path = Path("/tmp/stub.log")
        self._code = code

    def poll_exit(self) -> int | None:
        return self._code

    def is_running(self) -> bool:
        return self._code is None


class BaselineLoadFailureTests(unittest.TestCase):
    def test_corrupt_baseline_emits_loud_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            (state_dir / "orchestrator_baseline_HYPE.json").write_text("{corrupt", encoding="utf-8")
            events: list = []
            tracker = PnlTracker(make_args(state_dir), utc_now(), lambda kind, **d: events.append((kind, d)))
            self.assertIsNone(tracker.baseline_equity)
            self.assertIn("baseline_load_failed", [k for k, _ in events])


class ConfirmWindowTests(unittest.TestCase):
    def test_sustained_uses_monotonic_and_clears_on_switch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp), live=False)
            # Fresh key: not sustained yet.
            self.assertFalse(orch.sustained("taker_margin_limited", True, 90))
            # Backdate past the window: sustained.
            orch.condition_since["taker_margin_limited"] = time.monotonic() - 120
            self.assertTrue(orch.sustained("taker_margin_limited", True, 90))
            # A mode switch (dry-run ensure_bot) must clear ALL keys so the stale
            # timestamp cannot collapse the confirm window after the round-trip —
            # the 2026-07-07 13-switch flap-storm mechanism.
            orch.ensure_bot(XEMM_BOT, "test_switch", {})
            self.assertEqual({}, orch.condition_since)
            self.assertFalse(orch.sustained("taker_margin_limited", True, 90))

    def test_active_exit_clears_condition_windows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp), live=False)
            orch.condition_since["ready_for_taker"] = time.monotonic() - 900
            orch.active_bot = TAKER_BOT
            orch.active_taker_mode = "normal"
            orch.children[TAKER_BOT] = StubChild(TAKER_BOT, uptime_sec=3600, code=0)
            orch.check_child_exits()
            self.assertEqual({}, orch.condition_since)


class HysteresisTests(unittest.TestCase):
    def _status(self, headroom: str) -> dict:
        return {
            "desired_notional_usd": "13",
            "margin_buffer_usd": "0",
            "positions": {"headroom_notional_usd": headroom},
            "accounts": {"aster_available_usd": "1000", "lighter_available_usd": "1000"},
            "opportunities": [],
        }

    def test_band_between_switch_and_resume_flips_neither_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp))
            status = self._status("32.5")  # 2.5 clips of 13
            blocked, _ = orch.taker_margin_state(status, Decimal("2"), Decimal("2"))
            self.assertFalse(blocked, "2.5 clips is above the 2-clip block threshold")
            limited, _ = orch.taker_margin_state(status, Decimal("3"), Decimal("3"))
            self.assertTrue(limited, "2.5 clips is below the 3-clip resume threshold")


class EnsureBotSafetyTests(unittest.TestCase):
    def _live_orch(self, tmp: Path) -> Orchestrator:
        orch = make_orch(tmp, live=True)
        orch.halts = []
        orch.safe_halt = lambda reason, **d: (
            orch.halts.append((reason, d)),
            setattr(orch, "shutdown_requested", True),
            setattr(orch, "halted", True),
        )
        orch.started = []
        orch.start_bot = lambda bot, taker_mode="normal": orch.started.append(bot)
        orch.stop_observer = lambda reason: None
        return orch

    def test_mid_tick_nonzero_exit_halts_instead_of_silent_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = self._live_orch(Path(tmp))
            orch.active_bot = TAKER_BOT
            orch.active_taker_mode = "normal"
            # The child died with a breaker-trip exit code between check_child_exits
            # and ensure_bot (the status polls take seconds).
            orch.children[TAKER_BOT] = StubChild(TAKER_BOT, uptime_sec=3600, code=3)
            orch.ensure_bot(TAKER_BOT, "taker_active", {})
            self.assertEqual(["active_bot_exited_nonzero"], [r for r, _ in orch.halts])
            self.assertEqual([], orch.started, "a breaker trip must not be restarted")

    def test_sigkill_survivor_blocks_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = self._live_orch(Path(tmp))
            orch.active_bot = XEMM_BOT
            orch.stop_active = lambda reason, grace_sec=None: {"alive_after_sigkill": True}
            orch.ensure_bot(TAKER_BOT, "resume_taker", {})
            self.assertEqual(["bot_survived_sigkill"], [r for r, _ in orch.halts])
            self.assertEqual([], orch.started, "no second live writer over a survivor")

    def test_resume_requires_xemm_orders_clear(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = self._live_orch(Path(tmp))
            orch.active_bot = XEMM_BOT
            orch.stop_active = lambda reason, grace_sec=None: None
            orch.verify_xemm_orders_clear = lambda: (False, {"accounts": {"aster_open_orders": 1}})
            orch.ensure_bot(TAKER_BOT, "resume_taker", {})
            self.assertEqual(["xemm_orders_not_clear_on_resume"], [r for r, _ in orch.halts])
            self.assertEqual([], orch.started)

    def test_clean_resume_starts_taker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = self._live_orch(Path(tmp))
            orch.active_bot = XEMM_BOT
            orch.stop_active = lambda reason, grace_sec=None: None
            orch.verify_xemm_orders_clear = lambda: (True, {})
            orch.ensure_bot(TAKER_BOT, "resume_taker", {})
            self.assertEqual([], orch.halts)
            self.assertEqual([TAKER_BOT], orch.started)


class CrashLoopTests(unittest.TestCase):
    def test_three_short_clean_exits_halt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp), live=False)
            orch.halts = []
            orch.safe_halt = lambda reason, **d: (
                orch.halts.append(reason),
                setattr(orch, "shutdown_requested", True),
            )
            for i in range(3):
                orch.active_bot = TAKER_BOT
                orch.active_taker_mode = "normal"
                orch.children[TAKER_BOT] = StubChild(TAKER_BOT, uptime_sec=30, code=0)
                orch.check_child_exits()
            self.assertEqual(["active_bot_crash_loop"], orch.halts)

    def test_long_run_resets_the_ladder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp), live=False)
            orch.active_exit_count = 2
            orch.active_bot = TAKER_BOT
            orch.active_taker_mode = "normal"
            orch.children[TAKER_BOT] = StubChild(TAKER_BOT, uptime_sec=3600, code=0)
            orch.check_child_exits()
            self.assertEqual(1, orch.active_exit_count)


class HaltExitCodeTests(unittest.TestCase):
    def test_safe_halt_sets_halted_for_nonzero_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            orch = make_orch(Path(tmp), live=False)
            self.assertFalse(orch.halted)
            orch.safe_halt("test_reason")
            self.assertTrue(orch.halted)
            self.assertTrue(orch.shutdown_requested)


class TradeTrackerIncrementalTests(unittest.TestCase):
    def _row(self, n: int) -> dict:
        return {
            "timestamp": iso(utc_now()),
            "market": "HYPE",
            "aster_order_id": n,
            "lighter_client_order_index": n,
            "actual_net_usd": "0.5",
        }

    def test_poll_reads_only_appended_lines_and_survives_truncation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            trades_path = state_dir / "trades_HYPE.jsonl"
            trades_path.write_text(
                json.dumps(self._row(1)) + "\n" + json.dumps(self._row(2)) + "\n",
                encoding="utf-8",
            )
            args = make_args(state_dir, taker_trades=trades_path, backfill_existing_trades=True)
            tracker = TradeTracker(args, utc_now() - timedelta(hours=1), lambda kind, **d: None)
            tracker.read_xemm_trades = lambda: []

            self.assertEqual(2, len(tracker.poll()))
            self.assertEqual(trades_path.stat().st_size, tracker.taker_trades_offset)

            # Append one row: only IT is parsed (offset advances past it).
            with trades_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(self._row(3)) + "\n")
            self.assertEqual(1, len(tracker.poll()))
            self.assertEqual(trades_path.stat().st_size, tracker.taker_trades_offset)

            # A partially-written trailing line is NOT consumed yet.
            with trades_path.open("a", encoding="utf-8") as f:
                f.write('{"partial')
            self.assertEqual(0, len(tracker.poll()))
            with trades_path.open("a", encoding="utf-8") as f:
                f.write('": true}\n')

            # Truncation/rotation: re-read from zero; seen-keys dedup means no
            # double-count of the re-seen row.
            trades_path.write_text(json.dumps(self._row(1)) + "\n", encoding="utf-8")
            self.assertEqual(0, len(tracker.poll()))

            # Aggregates match what was ingested (3 real trades).
            summary = tracker.summary()
            self.assertEqual(3, summary["trades"])
            self.assertEqual(Decimal("1.5"), summary["net_pnl_usdc"])


class LogRetentionTests(unittest.TestCase):
    def test_prunes_old_logs_but_never_live_or_console(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            old = time.time() - 30 * 86400
            dead = state_dir / "orchestrator_xemm_HYPE_20260601T000000Z.log"
            live = state_dir / "orchestrator_taker_HYPE_20260601T000001Z.log"
            fresh = state_dir / "orchestrator_xemm_HYPE_20260709T000000Z.log"
            console = state_dir / "orchestrator_console_20260601T000000Z.log"
            for p in (dead, live, fresh, console):
                p.write_text("x", encoding="utf-8")
            for p in (dead, live, console):
                os.utime(p, (old, old))

            pruned = prune_old_logs(state_dir, "HYPE", 14.0, utc_now(), exclude={live})
            self.assertEqual([dead], [p for p, _ in pruned])
            self.assertTrue(live.exists(), "a live child's log must never be pruned")
            self.assertTrue(fresh.exists())
            self.assertTrue(console.exists(), "the console log is outside the glob")


if __name__ == "__main__":
    unittest.main()
