#!/usr/bin/env python3
"""Summarize hedged Aster/Lighter rounds from the livebot journal.

Pairs `fill` records with `hedge_fill` records by `cloid` and prints the realized
cross-exchange edge. This is intentionally journal-only so it can be run while the
bot is still running:

    python3 scripts/check_hedged_trade.py runs/live-hype-smoke-journal.jsonl
"""

from __future__ import annotations

import argparse
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path


def dec(value, default="0"):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def load_config_fees(path: Path | None) -> tuple[Decimal, Decimal]:
    aster_maker = Decimal("0.0")
    lighter_taker = Decimal("0.0")
    if not path or not path.exists():
        return aster_maker, lighter_taker
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if "=" not in line:
            continue
        k, v = [part.strip() for part in line.split("=", 1)]
        v = v.strip('"')
        if k == "aster_maker_fee_bps":
            aster_maker = dec(v, aster_maker)
        elif k == "taker_fee_bps":
            lighter_taker = dec(v, lighter_taker)
    return aster_maker, lighter_taker


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize Aster/Lighter hedged rounds from a journal JSONL.")
    ap.add_argument("journal", type=Path)
    ap.add_argument("--config", type=Path, default=Path("config-live-lighter.toml"))
    args = ap.parse_args()

    aster_fee_bps, config_lighter_fee_bps = load_config_fees(args.config)
    fills = {}
    hedge_fills = {}
    counts = {}

    if not args.journal.exists():
        print(f"journal not found: {args.journal}")
        return 2

    with args.journal.open() as f:
        for line in f:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            kind = row.get("kind")
            counts[kind] = counts.get(kind, 0) + 1
            detail = row.get("detail") or {}
            cloid = detail.get("cloid")
            if not cloid:
                continue
            if kind == "fill":
                fills[cloid] = row
            elif kind == "hedge_fill":
                hedge_fills.setdefault(cloid, []).append(row)

    paired = []
    for cloid, fill in fills.items():
        hedges = hedge_fills.get(cloid, [])
        if not hedges:
            continue
        fd = fill["detail"]
        aster_px = dec(fd.get("avg_aster_px"))
        hedge_side = str(fd.get("side", "")).lower()
        target_qty = dec(fd.get("qty"))
        hedge_qty = sum(dec(h["detail"].get("qty")) for h in hedges)
        hedge_notional = sum(dec(h["detail"].get("qty")) * dec(h["detail"].get("px")) for h in hedges)
        fee_fields = [h["detail"].get("fee_usd") for h in hedges]
        has_callback_fees = all(v is not None for v in fee_fields)
        actual_lighter_fee_usd = sum(dec(v) for v in fee_fields)
        hedge_px = hedge_notional / hedge_qty if hedge_qty else Decimal("0")
        if aster_px <= 0 or hedge_px <= 0 or hedge_qty <= 0:
            continue
        if hedge_side == "sell":
            gross = hedge_px - aster_px
        else:
            gross = aster_px - hedge_px
        gross_bps = gross / aster_px * Decimal("10000")
        if has_callback_fees:
            lighter_fee_bps = actual_lighter_fee_usd / (hedge_qty * hedge_px) * Decimal("10000")
            fee_source = "callback"
        else:
            lighter_fee_bps = config_lighter_fee_bps
            fee_source = "config"
        net_bps = gross_bps - aster_fee_bps - lighter_fee_bps
        paired.append((
            fill.get("market"),
            cloid,
            hedge_side,
            target_qty,
            hedge_qty,
            aster_px,
            hedge_px,
            gross_bps,
            net_bps,
            actual_lighter_fee_usd,
            lighter_fee_bps,
            fee_source,
        ))

    print(f"journal: {args.journal}")
    print(f"records: fill={counts.get('fill', 0)} hedge_fill={counts.get('hedge_fill', 0)} paired={len(paired)}")
    print(f"configured fees: Aster maker {aster_fee_bps} bps + Lighter taker {config_lighter_fee_bps} bps")
    if not paired:
        print("no paired hedged rounds yet")
        return 1

    for market, cloid, side, target_qty, hedge_qty, aster_px, hedge_px, gross_bps, net_bps, fee_usd, fee_bps, fee_source in paired[-10:]:
        print(
            f"{market} {cloid[:12]} side={side:<4} target_qty={target_qty} hedge_qty={hedge_qty} "
            f"aster_px={aster_px} lighter_px={hedge_px} gross={gross_bps:+.2f}bps "
            f"lighter_fee={fee_usd} ({fee_bps:.2f}bps {fee_source}) net={net_bps:+.2f}bps"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
