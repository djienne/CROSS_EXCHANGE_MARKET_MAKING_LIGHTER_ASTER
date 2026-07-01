#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


SECONDS_PER_YEAR = Decimal(365 * 24 * 60 * 60)
DEFAULT_SINCE = "2026-06-28T00:00:00Z"
DEFAULT_INCLUDE_UNTIMESTAMPED_XEMM = False


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(raw: str) -> datetime:
    raw = raw.strip()
    if raw.lower() == "now":
        return utc_now()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    raw = re.sub(r"\.(\d{6})\d+(\+00:00)$", r".\1\2", raw)
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def dec(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return default


def dec_json(value: Decimal) -> str:
    return format(value.normalize(), "f")


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return dec_json(value)
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"{type(value)!r} is not JSON serializable")


def load_jsonl(path: Path):
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"warn: skipping invalid JSON in {path}:{line_no}: {exc}", file=sys.stderr)


def row_timestamp(row: dict[str, Any]) -> datetime | None:
    containers = [row]
    detail = row.get("detail")
    if isinstance(detail, dict):
        containers.append(detail)
    for container in containers:
        # ts_ms is the journal's wall-clock stamp (epoch milliseconds), written by the XEMM
        # bot's JournalRecord since 2026-07. Prefer it: it is unambiguous and always present
        # on current-format rows.
        raw_ms = container.get("ts_ms")
        if isinstance(raw_ms, (int, float)) and raw_ms > 0:
            try:
                return datetime.fromtimestamp(raw_ms / 1000.0, tz=timezone.utc)
            except (ValueError, OSError, OverflowError):
                pass
        for key in ("timestamp", "ts", "time", "created_at"):
            raw = container.get(key)
            if raw is None or raw == "":
                continue
            try:
                return parse_dt(str(raw))
            except (ValueError, TypeError):
                continue
    return None


def summarize_taker(path: Path, since: datetime, now: datetime, market: str) -> dict[str, Any]:
    out = {
        "trades": 0,
        "gross_pnl_usdc": Decimal("0"),
        "fees_usdc": Decimal("0"),
        "net_pnl_usdc": Decimal("0"),
        "path": path,
    }
    if not path.exists():
        return out
    for row in load_jsonl(path):
        if row.get("market") != market:
            continue
        try:
            ts = parse_dt(str(row.get("timestamp", "")))
        except (ValueError, TypeError) as exc:
            print(f"warn: skipping taker row with bad timestamp in {path}: {exc}", file=sys.stderr)
            continue
        if ts < since or ts > now:
            continue
        out["trades"] += 1
        out["gross_pnl_usdc"] += dec(row.get("actual_gross_usd"))
        out["fees_usdc"] += dec(row.get("actual_fees_usd"))
        out["net_pnl_usdc"] += dec(row.get("actual_net_usd"))
    return out


def parse_xemm_fee_bps(config_path: Path) -> tuple[Decimal, Decimal]:
    aster_maker = Decimal("0")
    lighter_taker = Decimal("0")
    if not config_path.exists():
        return aster_maker, lighter_taker
    pattern = re.compile(r"^\s*(aster_maker_fee_bps|taker_fee_bps)\s*=\s*[\"']?([^\"'#\s]+)")
    for line in config_path.read_text(encoding="utf-8").splitlines():
        match = pattern.search(line)
        if not match:
            continue
        if match.group(1) == "aster_maker_fee_bps":
            aster_maker = dec(match.group(2))
        elif match.group(1) == "taker_fee_bps":
            lighter_taker = dec(match.group(2))
    return aster_maker, lighter_taker


def detail_dec(detail: dict[str, Any], key: str) -> Decimal:
    value = detail.get(key)
    if value is None:
        raise ValueError(f"journal detail missing {key!r}")
    return dec(value)


def normalize_side(value: Any) -> str:
    side = str(value or "").strip().lower()
    if side == "buy":
        return "buy"
    if side == "sell":
        return "sell"
    return side


def summarize_xemm_journal(
    path: Path,
    market: str,
    aster_fee_rate: Decimal,
    lighter_fee_rate: Decimal,
    since: datetime,
    now: datetime,
    include_untimestamped: bool,
) -> dict[str, Any]:
    fills: dict[str, dict[str, Any]] = {}
    hedges: dict[str, dict[str, Any]] = {}
    out = {
        "path": path,
        "trades": 0,
        "gross_pnl_usdc": Decimal("0"),
        "aster_fees_usdc": Decimal("0"),
        "lighter_fees_usdc": Decimal("0"),
        "lighter_callback_fees_usdc": Decimal("0"),
        "lighter_config_fallback_fees_usdc": Decimal("0"),
        "fees_usdc": Decimal("0"),
        "net_pnl_usdc": Decimal("0"),
        "unmatched_fills": 0,
        "unmatched_hedges": 0,
        "qty_mismatches": 0,
        "malformed_rows": 0,
        "untimestamped_trades": 0,
        "skipped_untimestamped_trades": 0,
        "time_filtered_trades": 0,
    }
    for row in load_jsonl(path):
        try:
            if row.get("market") != market:
                continue
            detail = row.get("detail") or {}
            kind = row.get("kind")
            ts = row_timestamp(row)
            if kind == "fill":
                cloid = str(detail.get("cloid", ""))
                if not cloid:
                    continue
                fills[cloid] = {
                    "side": normalize_side(detail.get("side")),
                    "qty": detail_dec(detail, "qty"),
                    "aster_px": detail_dec(detail, "avg_aster_px"),
                    "timestamp": ts,
                }
            elif kind == "hedge_fill":
                cloid = str(detail.get("cloid", ""))
                if not cloid:
                    continue
                qty = detail_dec(detail, "qty")
                px = detail_dec(detail, "px")
                fee_raw = detail.get("fee_usd")
                side = normalize_side(detail.get("side"))
                hedge = hedges.setdefault(
                    cloid,
                    {
                        "side": side,
                        "qty": Decimal("0"),
                        "notional": Decimal("0"),
                        "fee_usd": Decimal("0"),
                        "fee_missing_notional": Decimal("0"),
                        "first_timestamp": ts,
                        "last_timestamp": ts,
                        "side_mismatch": False,
                    },
                )
                if hedge["side"] != side:
                    hedge["side_mismatch"] = True
                hedge["qty"] += qty
                hedge["notional"] += qty * px
                if fee_raw is None:
                    hedge["fee_missing_notional"] += qty * px
                else:
                    hedge["fee_usd"] += dec(fee_raw)
                if ts is not None:
                    if hedge["first_timestamp"] is None or ts < hedge["first_timestamp"]:
                        hedge["first_timestamp"] = ts
                    if hedge["last_timestamp"] is None or ts > hedge["last_timestamp"]:
                        hedge["last_timestamp"] = ts
        except (ValueError, TypeError, InvalidOperation) as exc:
            out["malformed_rows"] += 1
            print(f"warn: skipping malformed XEMM journal row in {path}: {exc}", file=sys.stderr)

    for cloid in sorted(set(fills) | set(hedges)):
        fill = fills.get(cloid)
        hedge = hedges.get(cloid)
        if fill and not hedge:
            out["unmatched_fills"] += 1
            continue
        if hedge and not fill:
            out["unmatched_hedges"] += 1
            continue
        if not fill or not hedge:
            continue
        if hedge["qty"] <= 0:
            out["qty_mismatches"] += 1
            continue
        trade_ts = max(
            [ts for ts in [fill.get("timestamp"), hedge.get("last_timestamp")] if ts is not None],
            default=None,
        )
        if trade_ts is None:
            if not include_untimestamped:
                out["skipped_untimestamped_trades"] += 1
                continue
            out["untimestamped_trades"] += 1
        elif trade_ts < since or trade_ts > now:
            out["time_filtered_trades"] += 1
            continue
        lighter_px = hedge["notional"] / hedge["qty"]
        if fill["qty"] != hedge["qty"] or fill["side"] != hedge["side"] or hedge.get("side_mismatch"):
            out["qty_mismatches"] += 1
            continue
        if fill["side"] == "buy":
            gross = fill["qty"] * (fill["aster_px"] - lighter_px)
        elif fill["side"] == "sell":
            gross = fill["qty"] * (lighter_px - fill["aster_px"])
        else:
            out["qty_mismatches"] += 1
            continue
        aster_fee = fill["qty"] * fill["aster_px"] * aster_fee_rate
        callback_fee = hedge["fee_usd"]
        fallback_fee = hedge["fee_missing_notional"] * lighter_fee_rate
        lighter_fee = callback_fee + fallback_fee
        net = gross - aster_fee - lighter_fee
        out["trades"] += 1
        out["gross_pnl_usdc"] += gross
        out["aster_fees_usdc"] += aster_fee
        out["lighter_fees_usdc"] += lighter_fee
        out["lighter_callback_fees_usdc"] += callback_fee
        out["lighter_config_fallback_fees_usdc"] += fallback_fee
        out["fees_usdc"] += aster_fee + lighter_fee
        out["net_pnl_usdc"] += net
    return out


def selected_xemm_journals(
    paths: list[Path],
    runs_dirs: list[Path],
    since: datetime,
    require_paths: bool = False,
) -> tuple[list[Path], list[Path]]:
    if paths:
        included = []
        skipped = []
        for raw_path in paths:
            path = raw_path.expanduser().resolve()
            if require_paths and not path.is_file():
                raise SystemExit(f"error: --xemm-journal file not found: {path}")
            if path.is_file() and (require_paths or path.stat().st_size > 0):
                included.append(path)
            else:
                skipped.append(path)
        return included, skipped
    included: list[Path] = []
    skipped: list[Path] = []
    seen: set[Path] = set()
    for runs_dir in runs_dirs:
        if not runs_dir.exists():
            continue
        for path in sorted(runs_dir.glob("*journal*.jsonl")):
            path = path.resolve()
            if path in seen:
                continue
            seen.add(path)
            if not path.is_file() or path.stat().st_size == 0:
                continue
            mtime = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
            if mtime >= since:
                included.append(path)
            else:
                skipped.append(path)
    return included, skipped


def latest_capital_from_state(path: Path, active_preference: str | None = None) -> tuple[Decimal | None, str | None]:
    if not path.exists():
        return None, None
    state = json.loads(path.read_text(encoding="utf-8"))
    accounts = state.get("accounts") or {}
    active = active_preference or state.get("active_bot")
    candidates = []
    if active == "XEMM_LIGHTER_ASTER":
        candidates.extend(["xemm", "taker"])
    elif active == "LIGHTER_ASTER_TAKER_ARB":
        candidates.extend(["taker", "xemm"])
    else:
        candidates.extend(["xemm", "taker"])
    for key in candidates:
        equity = dec((accounts.get(key) or {}).get("total_equity_usd"), Decimal("-1"))
        if equity > 0:
            return equity, f"{path}:{key}.total_equity_usd"
    return None, None


def projection(net: Decimal, capital: Decimal | None, since: datetime, now: datetime) -> dict[str, Any]:
    elapsed = Decimal(max(1, int((now - since).total_seconds())))
    out: dict[str, Any] = {
        "elapsed_seconds": elapsed,
        "elapsed_days": elapsed / Decimal(86400),
        "capital_usdc": capital,
        "window_return_pct": None,
        "simple_annualized_return_pct": None,
        "projected_cagr_pct": None,
    }
    if capital is None or capital <= 0:
        return out
    window_return = net / capital
    annual_factor = SECONDS_PER_YEAR / elapsed
    out["window_return_pct"] = window_return * Decimal(100)
    out["simple_annualized_return_pct"] = window_return * annual_factor * Decimal(100)
    base = 1.0 + float(window_return)
    if base > 0:
        out["projected_cagr_pct"] = Decimal(str((math.pow(base, float(annual_factor)) - 1.0) * 100.0))
    return out


def combine(args: argparse.Namespace) -> dict[str, Any]:
    since = parse_dt(args.since)
    now = parse_dt(args.now) if args.now else utc_now()
    taker = summarize_taker(args.taker_trades, since, now, args.market)
    aster_fee_bps, lighter_fee_bps = parse_xemm_fee_bps(args.xemm_config)
    aster_fee_rate = aster_fee_bps / Decimal(10_000)
    lighter_fee_rate = lighter_fee_bps / Decimal(10_000)
    journal_paths, skipped_journals = selected_xemm_journals(
        args.xemm_journal,
        args.xemm_runs_dir,
        since,
        args.require_xemm_journal_paths,
    )
    xemm_journals = [
        summarize_xemm_journal(
            path,
            args.market,
            aster_fee_rate,
            lighter_fee_rate,
            since,
            now,
            args.include_untimestamped_xemm,
        )
        for path in journal_paths
    ]
    xemm = {
        "trades": sum(j["trades"] for j in xemm_journals),
        "gross_pnl_usdc": sum((j["gross_pnl_usdc"] for j in xemm_journals), Decimal("0")),
        "aster_fees_usdc": sum((j["aster_fees_usdc"] for j in xemm_journals), Decimal("0")),
        "lighter_fees_usdc": sum((j["lighter_fees_usdc"] for j in xemm_journals), Decimal("0")),
        "fees_usdc": sum((j["fees_usdc"] for j in xemm_journals), Decimal("0")),
        "lighter_callback_fees_usdc": sum((j["lighter_callback_fees_usdc"] for j in xemm_journals), Decimal("0")),
        "lighter_config_fallback_fees_usdc": sum((j["lighter_config_fallback_fees_usdc"] for j in xemm_journals), Decimal("0")),
        "net_pnl_usdc": sum((j["net_pnl_usdc"] for j in xemm_journals), Decimal("0")),
        "unmatched_fills": sum(j["unmatched_fills"] for j in xemm_journals),
        "unmatched_hedges": sum(j["unmatched_hedges"] for j in xemm_journals),
        "qty_mismatches": sum(j["qty_mismatches"] for j in xemm_journals),
        "malformed_rows": sum(j["malformed_rows"] for j in xemm_journals),
        "untimestamped_trades": sum(j["untimestamped_trades"] for j in xemm_journals),
        "skipped_untimestamped_trades": sum(j["skipped_untimestamped_trades"] for j in xemm_journals),
        "time_filtered_trades": sum(j["time_filtered_trades"] for j in xemm_journals),
        "journals": xemm_journals,
        "skipped_journals": skipped_journals,
        "fee_bps": {
            "aster_maker": aster_fee_bps,
            "lighter_taker": lighter_fee_bps,
        },
        "journal_selection": args.xemm_journal_selection,
        "untimestamped_policy": args.xemm_untimestamped,
    }
    total = {
        "trades": taker["trades"] + xemm["trades"],
        "gross_pnl_usdc": taker["gross_pnl_usdc"] + xemm["gross_pnl_usdc"],
        "fees_usdc": taker["fees_usdc"] + xemm["fees_usdc"],
        "net_pnl_usdc": taker["net_pnl_usdc"] + xemm["net_pnl_usdc"],
    }
    capital = args.capital_usdc
    capital_source = "cli"
    if capital is None:
        capital, capital_source = latest_capital_from_state(args.orchestrator_state)
        if capital is None:
            fallback_state = args.taker_trades.parent / f"orchestrator_state_{args.market}.json"
            capital, capital_source = latest_capital_from_state(fallback_state)
    proj = projection(total["net_pnl_usdc"], capital, since, now)
    notes = [
        "PnL is realized completed-trade PnL only; unrealized mark-to-market is excluded.",
        "XEMM net uses configured Aster maker bps and actual Lighter journal fees; configured Lighter taker bps is only a fallback when a hedge_fill lacks fee_usd.",
        "By default XEMM includes only production live/orchestrator journals. Pass --xemm-journal for exact files or --xemm-runs-dir for an mtime-based scan.",
    ]
    if xemm["untimestamped_trades"]:
        notes.append(
            f"Included {xemm['untimestamped_trades']} XEMM trade(s) without wall-clock timestamps "
            "(journal rows written before the bot stamped ts_ms); these cannot be windowed inside the journal file."
        )
    if xemm["skipped_untimestamped_trades"]:
        notes.append(
            f"Skipped {xemm['skipped_untimestamped_trades']} XEMM trade(s) without wall-clock timestamps "
            "(journal rows written before the bot stamped ts_ms). Rebuild/restart the bot to get ts_ms on new rows; "
            "use --xemm-untimestamped include only for deliberate historical backfills."
        )
    if xemm["time_filtered_trades"]:
        notes.append(f"Filtered {xemm['time_filtered_trades']} timestamped XEMM trade(s) outside the requested window.")
    return {
        "market": args.market,
        "since": since,
        "now": now,
        "taker": taker,
        "xemm": xemm,
        "total": total,
        "projection": proj,
        "capital_source": capital_source,
        "notes": notes,
    }


def fmt_money(value: Decimal, signed: bool = True, places: int = 8) -> str:
    sign = "+" if signed else ""
    return f"{value:{sign}.{places}f}"


def fmt_pct(value: Decimal | None, places: int) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{places}f}%"


def capital_label(result: dict[str, Any]) -> str:
    capital = result["projection"]["capital_usdc"]
    if capital is None:
        return "n/a (pass --capital-usdc)"
    return str(capital)


def print_table(title: str, headers: list[str], rows: list[list[Any]], right_align: set[int] | None = None) -> None:
    right_align = right_align or set()
    text_rows = [[str(cell) for cell in row] for row in rows]
    widths = [
        max(len(headers[idx]), *(len(row[idx]) for row in text_rows))
        for idx in range(len(headers))
    ]

    def render_row(row: list[str]) -> str:
        cells = []
        for idx, cell in enumerate(row):
            if idx in right_align:
                cells.append(cell.rjust(widths[idx]))
            else:
                cells.append(cell.ljust(widths[idx]))
        return " | ".join(cells)

    print(title)
    print(render_row(headers))
    print("-+-".join("-" * width for width in widths))
    for row in text_rows:
        print(render_row(row))


def print_human(result: dict[str, Any]) -> None:
    p = result["projection"]
    xemm = result["xemm"]

    print_table(
        "PnL",
        ["Source", "Trades", "Gross USDC", "Fees USDC", "Net USDC", "Lighter Journal Fees"],
        [
            [
                "TAKER",
                result["taker"]["trades"],
                fmt_money(result["taker"]["gross_pnl_usdc"]),
                fmt_money(result["taker"]["fees_usdc"], signed=False),
                fmt_money(result["taker"]["net_pnl_usdc"]),
                "-",
            ],
            [
                "XEMM",
                xemm["trades"],
                fmt_money(xemm["gross_pnl_usdc"]),
                fmt_money(xemm["fees_usdc"], signed=False),
                fmt_money(xemm["net_pnl_usdc"]),
                fmt_money(xemm["lighter_callback_fees_usdc"], signed=False),
            ],
            [
                "TOTAL",
                result["total"]["trades"],
                fmt_money(result["total"]["gross_pnl_usdc"]),
                fmt_money(result["total"]["fees_usdc"], signed=False),
                fmt_money(result["total"]["net_pnl_usdc"]),
                "-",
            ],
        ],
        right_align={1, 2, 3, 4, 5},
    )
    print()
    print_table(
        "Projection",
        ["Metric", "Value"],
        [
            ["Market", result["market"]],
            ["Since UTC", iso(result["since"])],
            ["Now UTC", iso(result["now"])],
            ["Elapsed Days", f"{p['elapsed_days']:.6f}"],
            ["Capital USDC", capital_label(result)],
            ["Window Return", fmt_pct(p["window_return_pct"], 8)],
            ["Simple Annualized", fmt_pct(p["simple_annualized_return_pct"], 4)],
            ["Projected CAGR", fmt_pct(p["projected_cagr_pct"], 4)],
            ["XEMM Fee Bps", f"aster_maker={xemm['fee_bps']['aster_maker']} lighter_taker={xemm['fee_bps']['lighter_taker']}"],
        ],
    )
    print()
    if p["capital_usdc"] is not None:
        print(f"capital source: {result['capital_source']}")
    if xemm["journals"]:
        print("XEMM journals included:")
        for journal in xemm["journals"]:
            print(f"- {journal['path']}")
    if xemm["skipped_journals"]:
        print("XEMM journals skipped:")
        for path in xemm["skipped_journals"]:
            print(f"- {path}")
    if (
        result["xemm"]["unmatched_fills"]
        or result["xemm"]["unmatched_hedges"]
        or result["xemm"]["qty_mismatches"]
        or result["xemm"].get("malformed_rows")
        or result["xemm"].get("skipped_untimestamped_trades")
    ):
        print(
            "XEMM warnings: "
            f"unmatched_fills={result['xemm']['unmatched_fills']} "
            f"unmatched_hedges={result['xemm']['unmatched_hedges']} "
            f"qty_mismatches={result['xemm']['qty_mismatches']} "
            f"malformed_rows={result['xemm'].get('malformed_rows', 0)} "
            f"skipped_untimestamped_trades={result['xemm'].get('skipped_untimestamped_trades', 0)}"
        )
    print()
    print("notes:")
    for note in result["notes"]:
        print(f"- {note}")


def parse_args() -> argparse.Namespace:
    stack_root = Path(__file__).resolve().parent
    taker_root = stack_root / "LIGHTER_ASTER_TAKER_ARB"
    xemm_root = stack_root / "XEMM_LIGHTER_ASTER"
    parser = argparse.ArgumentParser(description="Combined realized PnL report for taker arb + XEMM.")
    parser.add_argument("--since", default=DEFAULT_SINCE, help=f"UTC/RFC3339 start time. Default: {DEFAULT_SINCE}.")
    parser.add_argument("--now", default=None, help="Override report end time. Defaults to current UTC time.")
    parser.add_argument("--market", default="HYPE")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--capital-usdc", type=Decimal, default=None, help="Capital denominator for projected CAGR. Defaults to latest active bot equity from orchestrator state.")
    parser.add_argument("--taker-trades", type=Path, default=None)
    parser.add_argument("--xemm-runs-dir", type=Path, action="append", default=None, help="Advanced: scan a directory of XEMM journal files by mtime. Repeatable.")
    parser.add_argument("--xemm-journal", type=Path, action="append", default=[], help="Explicit XEMM journal to include. Repeatable. Overrides default journal selection.")
    parser.add_argument("--xemm-config", type=Path, default=xemm_root / "config-live-lighter.toml")
    parser.add_argument(
        "--xemm-untimestamped",
        choices=["auto", "include", "exclude"],
        default="auto",
        help="How to handle legacy XEMM fill/hedge rows without wall-clock timestamps. auto follows the script default policy, currently exclude for the default recent window.",
    )
    parser.add_argument("--orchestrator-state", type=Path, default=None)
    args = parser.parse_args()
    explicit_xemm_journals = bool(args.xemm_journal)
    explicit_xemm_runs_dirs = args.xemm_runs_dir is not None
    if args.taker_trades is None:
        args.taker_trades = taker_root / f"runs/trades_{args.market}.jsonl"
    if args.xemm_runs_dir is None:
        args.xemm_runs_dir = []
    if not explicit_xemm_journals and not explicit_xemm_runs_dirs:
        args.xemm_journal = [
            stack_root / f"runs/orchestrator-xemm-{args.market}-journal.jsonl",
            xemm_root / f"runs/live-{args.market.lower()}-lighter-journal.jsonl",
        ]
        args.xemm_journal_selection = "default_production_journals"
        args.require_xemm_journal_paths = False
    elif explicit_xemm_journals:
        args.xemm_journal_selection = "explicit_journals"
        args.require_xemm_journal_paths = True
    else:
        args.xemm_journal_selection = "runs_dir_mtime_since"
        args.require_xemm_journal_paths = False
    if args.orchestrator_state is None:
        args.orchestrator_state = stack_root / f"runs/orchestrator_state_{args.market}.json"
    if args.xemm_untimestamped == "include":
        args.include_untimestamped_xemm = True
    elif args.xemm_untimestamped == "exclude":
        args.include_untimestamped_xemm = False
    else:
        args.include_untimestamped_xemm = DEFAULT_INCLUDE_UNTIMESTAMPED_XEMM if args.since == DEFAULT_SINCE else False
    return args


def main() -> int:
    args = parse_args()
    result = combine(args)
    if result["projection"]["projected_cagr_pct"] is None and result["projection"]["capital_usdc"] is None:
        print("warn: projected CAGR unavailable because capital could not be inferred", file=sys.stderr)
    if args.json:
        print(json.dumps(result, default=json_default, indent=2))
    else:
        print_human(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
