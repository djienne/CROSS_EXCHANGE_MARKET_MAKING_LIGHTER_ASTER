#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator import PnlTracker, utc_now  # noqa: E402


def make_args(state_dir: Path, pnl_since: str = "startup", gap_hours: float = 48.0) -> argparse.Namespace:
    return argparse.Namespace(
        market="HYPE",
        state_dir=state_dir,
        pnl_since=pnl_since,
        max_loss_usdc=Decimal("15"),
        baseline_max_gap_hours=gap_hours,
    )


def write_baseline(state_dir: Path, equity: str, baseline_age_hours: float, last_seen_age_hours: float | None = None, source_bot: str | None = None) -> Path:
    now = utc_now()
    body = {
        "market": "HYPE",
        "baseline_equity_usd": equity,
        "baseline_ts": (now - timedelta(hours=baseline_age_hours)).isoformat().replace("+00:00", "Z"),
    }
    if last_seen_age_hours is not None:
        body["last_seen_ts"] = (now - timedelta(hours=last_seen_age_hours)).isoformat().replace("+00:00", "Z")
    if source_bot is not None:
        body["source_bot"] = source_bot
    path = state_dir / "orchestrator_baseline_HYPE.json"
    path.write_text(json.dumps(body), encoding="utf-8")
    return path


def status(equity: str = "238.83", bot: str = "XEMM_LIGHTER_ASTER") -> dict:
    return {"bot": bot, "accounts": {"total_equity_usd": equity}}


class BaselineGapTests(unittest.TestCase):
    def test_old_format_stale_baseline_discarded(self) -> None:
        # Pre-fix files have no last_seen_ts; baseline_ts is the gap fallback.
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            path = write_baseline(state_dir, "244.64", baseline_age_hours=5 * 24)
            events: list = []
            tracker = PnlTracker(make_args(state_dir), utc_now(), lambda kind, **d: events.append((kind, d)))
            self.assertIsNone(tracker.baseline_equity)
            self.assertFalse(path.exists())
            self.assertEqual(["baseline_stale_discarded"], [k for k, _ in events])
            self.assertEqual(Decimal("244.64"), events[0][1]["baseline_equity_usd"])

    def test_recently_seen_baseline_kept_despite_old_arming(self) -> None:
        # Continuous supervision: an old baseline refreshed hourly must survive
        # restarts so a slow bleed keeps accumulating.
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            write_baseline(state_dir, "244.64", baseline_age_hours=10 * 24, last_seen_age_hours=1, source_bot="XEMM_LIGHTER_ASTER")
            events: list = []
            tracker = PnlTracker(make_args(state_dir), utc_now(), lambda kind, **d: events.append((kind, d)))
            self.assertEqual(Decimal("244.64"), tracker.baseline_equity)
            self.assertEqual("XEMM_LIGHTER_ASTER", tracker.baseline_source_bot)
            self.assertEqual([], events)

    def test_zero_gap_hours_disables_discard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            write_baseline(state_dir, "244.64", baseline_age_hours=30 * 24)
            tracker = PnlTracker(make_args(state_dir, gap_hours=0.0), utc_now(), lambda kind, **d: None)
            self.assertEqual(Decimal("244.64"), tracker.baseline_equity)

    def test_pnl_since_now_ignores_persisted_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            write_baseline(state_dir, "244.64", baseline_age_hours=1, last_seen_age_hours=0)
            tracker = PnlTracker(make_args(state_dir, pnl_since="now"), utc_now(), lambda kind, **d: None)
            self.assertIsNone(tracker.baseline_equity)

    def test_arming_persists_last_seen_and_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            tracker = PnlTracker(make_args(state_dir), utc_now(), lambda kind, **d: None)
            self.assertIsNotNone(tracker.record(status(), "XEMM_LIGHTER_ASTER"))
            body = json.loads((state_dir / "orchestrator_baseline_HYPE.json").read_text(encoding="utf-8"))
            self.assertEqual("238.83", body["baseline_equity_usd"])
            self.assertIn("last_seen_ts", body)
            self.assertEqual("XEMM_LIGHTER_ASTER", body["source_bot"])

    def test_last_seen_refreshes_after_an_hour(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            tracker = PnlTracker(make_args(state_dir), utc_now(), lambda kind, **d: None)
            tracker.record(status(), None)
            path = state_dir / "orchestrator_baseline_HYPE.json"
            first_seen = json.loads(path.read_text(encoding="utf-8"))["last_seen_ts"]
            tracker.record(status("238.90"), None)
            self.assertEqual(first_seen, json.loads(path.read_text(encoding="utf-8"))["last_seen_ts"])
            tracker.last_persist_ts = utc_now() - timedelta(hours=2)
            tracker.record(status("238.95"), None)
            refreshed = json.loads(path.read_text(encoding="utf-8"))
            self.assertGreater(refreshed["last_seen_ts"], first_seen)
            self.assertEqual("238.83", refreshed["baseline_equity_usd"])

    def test_restart_gap_event_fires_once_on_divergence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            write_baseline(state_dir, "244.64", baseline_age_hours=1, last_seen_age_hours=0, source_bot="TAKER")
            events: list = []
            tracker = PnlTracker(make_args(state_dir), utc_now(), lambda kind, **d: events.append((kind, d)))
            tracker.record(status("238.83"), None)  # |gap| = 5.81 >= 0.2 * 15 = 3
            tracker.record(status("238.80"), None)
            gap_events = [d for k, d in events if k == "baseline_restart_gap"]
            self.assertEqual(1, len(gap_events))
            self.assertEqual(Decimal("244.64"), gap_events[0]["baseline_equity_usd"])
            self.assertEqual("TAKER", gap_events[0]["baseline_source_bot"])
            # Warn-only: the loaded baseline must remain the anchor.
            self.assertEqual(Decimal("244.64"), tracker.baseline_equity)

    def test_no_restart_gap_event_when_close(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            write_baseline(state_dir, "239.00", baseline_age_hours=1, last_seen_age_hours=0)
            events: list = []
            tracker = PnlTracker(make_args(state_dir), utc_now(), lambda kind, **d: events.append((kind, d)))
            tracker.record(status("238.83"), None)
            self.assertEqual([], [k for k, _ in events if k == "baseline_restart_gap"])


if __name__ == "__main__":
    unittest.main()
