#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import combined_pnl  # noqa: E402


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows), encoding="utf-8")


class XemmSummaryTests(unittest.TestCase):
    def summarize(
        self,
        rows: list[dict],
        *,
        since: str = "2026-01-01T00:00:00Z",
        now: str = "2026-01-03T00:00:00Z",
        aster_fee_rate: Decimal = Decimal("0"),
        lighter_fee_rate: Decimal = Decimal("0"),
        include_untimestamped: bool = False,
        current_schema: bool = True,
    ) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "journal.jsonl"
            if current_schema:
                converted=[]
                for index,row in enumerate(rows):
                    row=dict(row)
                    d=dict(row.get("detail",{}))
                    logical=d.get("cloid","")
                    if row.get("kind")=="fill":
                        row["kind"]="maker_fill"
                        d={"logical_id":logical,"maker_side":"Sell" if d.get("side")=="BUY" else "Buy",
                            "qty":d.get("qty"),"px":d.get("avg_aster_px"),"fee_usd":"0","fee_complete":True,
                            "order_id":f"A-{logical}","trade_id":f"M-{index}","client_id":f"maker-{logical}"}
                    elif row.get("kind")=="hedge_fill":
                        row["kind"]="execution_trade"
                        d={"logical_id":logical,"attempt_id":logical,"venue":"lighter","side":d.get("side"),
                            "qty":d.get("qty"),"px":d.get("px"),"fee_usd":d.get("fee_usd"),
                            "order_id":f"L-{logical}","trade_id":f"H-{index}"}
                    row.update(schema_version=2,economic_status="confirmed",detail=d)
                    converted.append(row)
                rows=converted
            write_jsonl(path, rows)
            return combined_pnl.summarize_xemm_journal(
                path,
                "HYPE",
                aster_fee_rate,
                lighter_fee_rate,
                combined_pnl.parse_dt(since),
                combined_pnl.parse_dt(now),
                include_untimestamped,
            )

    def test_xemm_side_math_matches_hedge_side(self) -> None:
        rows = [
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0.1"}},
            {"timestamp": "2026-01-02T00:00:02Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "b", "side": "SELL", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:03Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "b", "side": "SELL", "qty": "1", "px": "101", "fee_usd": "0.2"}},
        ]
        out = self.summarize(rows)
        self.assertEqual(out["trades"], 2)
        self.assertEqual(out["gross_pnl_usdc"], Decimal("2"))
        self.assertEqual(out["lighter_fees_usdc"], Decimal("0.3"))
        self.assertEqual(out["net_pnl_usdc"], Decimal("1.7"))

    def test_actual_lighter_fee_is_not_double_counted_with_configured_rate(self) -> None:
        rows = [
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0.1"}},
        ]
        out = self.summarize(rows, lighter_fee_rate=Decimal("0.01"))
        self.assertEqual(out["lighter_callback_fees_usdc"], Decimal("0.1"))
        self.assertEqual(out["lighter_config_fallback_fees_usdc"], Decimal("0"))
        self.assertEqual(out["net_pnl_usdc"], Decimal("0.9"))

    def test_missing_venue_fee_remains_unknown_despite_configured_rate(self) -> None:
        rows = [
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "px": "99"}},
        ]
        out = self.summarize(rows, lighter_fee_rate=Decimal("0.01"))
        self.assertIsNone(out["lighter_callback_fees_usdc"])
        self.assertEqual(out["lighter_config_fallback_fees_usdc"], Decimal("0"))
        self.assertIsNone(out["net_pnl_usdc"])

    def test_mismatched_pair_keeps_matched_economics_and_exposes_residual(self) -> None:
        rows = [
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "2", "px": "99", "fee_usd": "0"}},
        ]
        out = self.summarize(rows)
        self.assertEqual(out["trades"], 1)
        self.assertEqual(out["qty_mismatches"], 1)
        self.assertEqual(out["net_pnl_usdc"], Decimal("1"))

    def test_timestamped_xemm_rows_are_filtered_by_since_now(self) -> None:
        rows = [
            {"timestamp": "2026-01-01T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "old", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-01T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "old", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0"}},
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "new", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "new", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0"}},
        ]
        out = self.summarize(rows, since="2026-01-02T00:00:00Z")
        self.assertEqual(out["trades"], 1)
        self.assertEqual(out["time_filtered_trades"], 1)

    def test_ts_ms_rows_are_windowed_not_policy_excluded(self) -> None:
        # Current-format journal rows carry ts_ms (epoch millis, stamped by JournalRecord).
        # They must window like any timestamped row — never fall into the untimestamped
        # policy that hides them from the default report.
        def ms(iso_ts: str) -> int:
            return int(combined_pnl.parse_dt(iso_ts).timestamp() * 1000)

        rows = [
            {"ts_ms": ms("2026-01-01T00:00:00Z"), "kind": "fill", "market": "HYPE", "detail": {"cloid": "old", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"ts_ms": ms("2026-01-01T00:00:01Z"), "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "old", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0"}},
            {"ts_ms": ms("2026-01-02T00:00:00Z"), "kind": "fill", "market": "HYPE", "detail": {"cloid": "new", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"ts_ms": ms("2026-01-02T00:00:01Z"), "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "new", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0"}},
        ]
        out = self.summarize(rows, since="2026-01-02T00:00:00Z")
        self.assertEqual(out["trades"], 1)
        self.assertEqual(out["time_filtered_trades"], 1)
        self.assertEqual(out["skipped_untimestamped_trades"], 0)
        self.assertEqual(out["untimestamped_trades"], 0)

    def test_legacy_untimestamped_xemm_rows_are_policy_controlled(self) -> None:
        rows = [
            {"kind": "fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "a", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0"}},
        ]
        strict = self.summarize(rows, include_untimestamped=False, current_schema=False)
        legacy = self.summarize(rows, include_untimestamped=True, current_schema=False)
        self.assertEqual(strict["trades"], 0)
        self.assertEqual(strict["skipped_untimestamped_trades"], 1)
        self.assertEqual(legacy["trades"], 1)
        self.assertEqual(legacy["untimestamped_trades"], 1)

    def test_malformed_xemm_row_is_skipped_not_fatal(self) -> None:
        rows = [
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "bad", "side": "BUY", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:00Z", "kind": "fill", "market": "HYPE", "detail": {"cloid": "good", "side": "BUY", "qty": "1", "avg_aster_px": "100"}},
            {"timestamp": "2026-01-02T00:00:01Z", "kind": "hedge_fill", "market": "HYPE", "detail": {"cloid": "good", "side": "BUY", "qty": "1", "px": "99", "fee_usd": "0"}},
        ]
        out = self.summarize(rows)
        self.assertEqual(out["malformed_rows"], 1)
        self.assertEqual(out["trades"], 1)

    def test_taker_summary_respects_upper_now_bound(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "taker.jsonl"
            write_jsonl(
                path,
                [
                    {"timestamp": "2026-01-02T00:00:00Z", "market": "HYPE", "actual_gross_usd": "1", "actual_fees_usd": "0", "actual_net_usd": "1"},
                    {"timestamp": "2026-01-04T00:00:00Z", "market": "HYPE", "actual_gross_usd": "1", "actual_fees_usd": "0", "actual_net_usd": "1"},
                ],
            )
            out = combined_pnl.summarize_taker(
                path,
                combined_pnl.parse_dt("2026-01-01T00:00:00Z"),
                combined_pnl.parse_dt("2026-01-03T00:00:00Z"),
                "HYPE",
            )
        self.assertEqual(out["trades"], 1)


if __name__ == "__main__":
    unittest.main()
