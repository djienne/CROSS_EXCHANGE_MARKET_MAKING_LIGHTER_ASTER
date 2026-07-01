#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import combined_pnl  # noqa: E402
import trade_history  # noqa: E402


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows), encoding="utf-8")


class TradeHistoryTests(unittest.TestCase):
    def open_db(self, path: Path) -> sqlite3.Connection:
        conn = trade_history.open_db(path)
        trade_history.init_db(conn)
        return conn

    def test_taker_ingest_uses_policy_fees_not_local_fee_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            db_path = root / "history.sqlite"
            write_jsonl(
                taker_path,
                [
                    {
                        "timestamp": "2026-01-02T00:00:00.123456789Z",
                        "market": "HYPE",
                        "direction": "SELL_ASTER_BUY_LIGHTER",
                        "qty": "1",
                        "actual_gross_usd": "999",
                        "actual_fees_usd": "999",
                        "actual_net_usd": "999",
                        "aster_fill": {"qty": "1", "vwap": "100", "notional": "100", "fee_usd": "999"},
                        "lighter_fill": {"qty": "1", "vwap": "99", "notional": "99", "fee_usd": "999"},
                        "aster_order_id": 1,
                        "lighter_client_order_index": 2,
                    }
                ],
            )
            write_jsonl(orch_path, [])
            with self.open_db(db_path) as conn:
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                report = trade_history.report_from_db(
                    conn,
                    market="HYPE",
                    since=combined_pnl.parse_dt("2026-01-01T00:00:00Z"),
                    now=combined_pnl.parse_dt("2026-01-03T00:00:00Z"),
                    db_path=db_path,
                )
            self.assertEqual(report["total"]["trades"], 1)
            self.assertEqual(report["total"]["gross_pnl_usdc"], Decimal("1"))
            self.assertEqual(report["total"]["policy_fees_usdc"], Decimal("0.04"))
            self.assertEqual(report["total"]["net_pnl_usdc"], Decimal("0.96"))

    def test_refresh_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            db_path = root / "history.sqlite"
            write_jsonl(
                taker_path,
                [
                    {
                        "timestamp": "2026-01-02T00:00:00Z",
                        "market": "HYPE",
                        "direction": "BUY_ASTER_SELL_LIGHTER",
                        "qty": "2",
                        "aster_fill": {"qty": "2", "vwap": "10", "notional": "20"},
                        "lighter_fill": {"qty": "2", "vwap": "11", "notional": "22"},
                        "aster_order_id": 10,
                        "lighter_client_order_index": 20,
                    }
                ],
            )
            write_jsonl(orch_path, [])
            with self.open_db(db_path) as conn:
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                trade_count = conn.execute("SELECT COUNT(*) FROM strategy_trades").fetchone()[0]
                fill_count = conn.execute("SELECT COUNT(*) FROM venue_fills").fetchone()[0]
            self.assertEqual(trade_count, 1)
            self.assertEqual(fill_count, 2)

    def test_xemm_orchestrator_ingest_uses_zero_policy_fees(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            db_path = root / "history.sqlite"
            write_jsonl(taker_path, [])
            write_jsonl(
                orch_path,
                [
                    {
                        "timestamp": "2026-01-02T00:00:00Z",
                        "key": "xemm:abc",
                        "bot": "XEMM_LIGHTER_ASTER",
                        "market": "HYPE",
                        "direction": "ASTER_MAKER_HEDGE_Sell",
                        "qty": "1",
                        "aster_px": "100",
                        "lighter_px": "99",
                        "gross_pnl_usdc": "999",
                        "fees_usdc": "999",
                        "net_pnl_usdc": "999",
                        "cloid": "abc",
                    }
                ],
            )
            with self.open_db(db_path) as conn:
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                report = trade_history.report_from_db(
                    conn,
                    market="HYPE",
                    since=combined_pnl.parse_dt("2026-01-01T00:00:00Z"),
                    now=combined_pnl.parse_dt("2026-01-03T00:00:00Z"),
                    db_path=db_path,
                )
            self.assertEqual(report["total"]["trades"], 1)
            self.assertEqual(report["total"]["gross_pnl_usdc"], Decimal("-1"))
            self.assertEqual(report["total"]["policy_fees_usdc"], Decimal("0"))
            self.assertEqual(report["total"]["net_pnl_usdc"], Decimal("-1"))

    def test_report_filters_by_time_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            db_path = root / "history.sqlite"
            base = {
                "market": "HYPE",
                "direction": "SELL_ASTER_BUY_LIGHTER",
                "qty": "1",
                "aster_fill": {"qty": "1", "vwap": "100", "notional": "100"},
                "lighter_fill": {"qty": "1", "vwap": "99", "notional": "99"},
            }
            old = dict(base, timestamp="2026-01-01T00:00:00Z", aster_order_id=1, lighter_client_order_index=1)
            new = dict(base, timestamp="2026-01-02T00:00:00Z", aster_order_id=2, lighter_client_order_index=2)
            write_jsonl(taker_path, [old, new])
            write_jsonl(orch_path, [])
            with self.open_db(db_path) as conn:
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                report = trade_history.report_from_db(
                    conn,
                    market="HYPE",
                    since=combined_pnl.parse_dt("2026-01-02T00:00:00Z"),
                    now=combined_pnl.parse_dt("2026-01-03T00:00:00Z"),
                    db_path=db_path,
                )
            self.assertEqual(report["total"]["trades"], 1)

    def test_report_time_window_uses_normalized_timestamp_not_text_sort(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            taker_path = root / "taker.jsonl"
            orch_path = root / "orchestrator.jsonl"
            db_path = root / "history.sqlite"
            base = {
                "market": "HYPE",
                "direction": "SELL_ASTER_BUY_LIGHTER",
                "qty": "1",
                "aster_fill": {"qty": "1", "vwap": "100", "notional": "100"},
                "lighter_fill": {"qty": "1", "vwap": "99", "notional": "99"},
            }
            boundary = dict(base, timestamp="2026-06-28T00:00:00.500000Z", aster_order_id=1, lighter_client_order_index=1)
            write_jsonl(taker_path, [boundary])
            write_jsonl(orch_path, [])
            with self.open_db(db_path) as conn:
                trade_history.refresh_lan(conn, market="HYPE", taker_trades=taker_path, orchestrator_trades=orch_path)
                report = trade_history.report_from_db(
                    conn,
                    market="HYPE",
                    since=combined_pnl.parse_dt("2026-06-28T00:00:00Z"),
                    now=combined_pnl.parse_dt("2026-06-28T00:00:01Z"),
                    db_path=db_path,
                )
            self.assertEqual(report["total"]["trades"], 1)


if __name__ == "__main__":
    unittest.main()
