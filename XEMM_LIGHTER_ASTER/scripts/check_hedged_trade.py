#!/usr/bin/env python3
"""Journal summary using the shared versioned economic contract."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from economics import xemm_journal


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal", type=Path)
    parser.add_argument("--config", type=Path, help="Accepted for compatibility; configured fees cannot prove historical fees.")
    args = parser.parse_args()
    if not args.journal.exists():
        print(f"journal not found: {args.journal}")
        return 2
    markets = set()
    with args.journal.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                row = json.loads(line)
                if isinstance(row, dict) and row.get("market"):
                    markets.add(str(row["market"]))
            except json.JSONDecodeError:
                pass  # The shared parser counts malformed rows in its summary.
    count = 0
    for market in sorted(markets):
        result = xemm_journal(args.journal, market)
        count += len(result["trades"])
        print(f"{market}: logical_trades={len(result['trades'])} malformed_rows={result['malformed_rows']}")
        for trade in result["trades"][-10:]:
            net = trade["net_pnl_usdc"]
            print(f"{trade['key']} status={trade['economic_status']} matched_qty={trade['matched_qty']} "
                  f"residual_qty={trade['residual_qty']} execution_spread_usdc={trade.get('execution_spread_usdc')} "
                  f"net_usdc={net if net is not None else 'unknown'}")
    return 0 if count else 1


if __name__ == "__main__":
    raise SystemExit(main())
