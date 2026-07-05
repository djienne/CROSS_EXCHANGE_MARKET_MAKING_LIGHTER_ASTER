#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from orchestrator import extract_json_object  # noqa: E402


REPORT = {
    "bot": "XEMM_LIGHTER_ASTER",
    "accounts": {"total_equity_usd": "238.83", "aster_open_orders": 0},
    "positions": {"aster_qty": "-1.17"},
}
PRETTY = json.dumps(REPORT, indent=2)


class ExtractJsonTests(unittest.TestCase):
    def test_plain_json(self) -> None:
        self.assertEqual(REPORT, extract_json_object(PRETTY))

    def test_json_after_tracing_lines(self) -> None:
        stdout = (
            "2026-07-05T12:00:00.123456Z  INFO watchdog: feeds fresh\n"
            "2026-07-05T12:00:00.223456Z  WARN something odd\n" + PRETTY + "\n"
        )
        self.assertEqual(REPORT, extract_json_object(stdout))

    def test_tracing_line_with_braces_is_ignored(self) -> None:
        stdout = (
            '2026-07-05T12:00:00Z  INFO state {"gate": "open", "n": 3}\n'
            + PRETTY
            + '\n2026-07-05T12:00:01Z  INFO done {"ok": true}\n'
        )
        self.assertEqual(REPORT, extract_json_object(stdout))

    def test_tracing_line_inside_json_body(self) -> None:
        lines = PRETTY.splitlines()
        lines.insert(2, "2026-07-05T12:00:00Z  INFO noise landed mid-report")
        self.assertEqual(REPORT, extract_json_object("\n".join(lines)))

    def test_small_fragment_does_not_win_over_report(self) -> None:
        stdout = 'warn: skipping {"tiny": 1}\n' + PRETTY
        self.assertEqual(REPORT, extract_json_object(stdout))

    def test_no_json_raises(self) -> None:
        with self.assertRaises(ValueError):
            extract_json_object("2026-07-05T12:00:00Z  INFO nothing here\nplain text\n")

    def test_fragment_only_returned_as_fallback(self) -> None:
        self.assertEqual({"tiny": 1}, extract_json_object('warn: skipping {"tiny": 1} tail'))


if __name__ == "__main__":
    unittest.main()
