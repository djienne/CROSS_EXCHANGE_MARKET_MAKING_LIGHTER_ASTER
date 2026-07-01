#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from combined_pnl import DEFAULT_SINCE, dec, iso, json_default, latest_capital_from_state, parse_dt, projection, utc_now


TAKER_BOT = "LIGHTER_ASTER_TAKER_ARB"
XEMM_BOT = "XEMM_LIGHTER_ASTER"

ASTER_TAKER_FEE_RATE = Decimal("0.0004")
ASTER_MAKER_FEE_RATE = Decimal("0")
LIGHTER_FEE_RATE = Decimal("0")


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS strategy_trades (
    trade_key TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    strategy TEXT NOT NULL,
    bot TEXT NOT NULL,
    market TEXT NOT NULL,
    timestamp TEXT,
    timestamp_us INTEGER,
    direction TEXT,
    qty TEXT NOT NULL,
    gross_pnl_usdc TEXT NOT NULL,
    policy_fees_usdc TEXT NOT NULL,
    net_pnl_usdc TEXT NOT NULL,
    aster_fee_usdc TEXT NOT NULL,
    lighter_fee_usdc TEXT NOT NULL,
    aster_fee_rate TEXT NOT NULL,
    lighter_fee_rate TEXT NOT NULL,
    aster_order_id TEXT,
    lighter_client_order_index TEXT,
    cloid TEXT,
    aster_px TEXT,
    lighter_px TEXT,
    confirmation_status TEXT NOT NULL DEFAULT 'local_only',
    confirmed_at TEXT,
    source TEXT NOT NULL,
    source_path TEXT,
    source_line INTEGER,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_strategy_trades_market_ts
    ON strategy_trades(market, timestamp);

CREATE INDEX IF NOT EXISTS idx_strategy_trades_strategy_ts
    ON strategy_trades(strategy, timestamp);

CREATE TABLE IF NOT EXISTS venue_fills (
    fill_key TEXT PRIMARY KEY,
    trade_key TEXT NOT NULL,
    mode TEXT NOT NULL,
    venue TEXT NOT NULL,
    market TEXT NOT NULL,
    timestamp TEXT,
    timestamp_us INTEGER,
    side TEXT,
    qty TEXT NOT NULL,
    price TEXT NOT NULL,
    notional_usdc TEXT NOT NULL,
    liquidity TEXT NOT NULL,
    fee_rate TEXT NOT NULL,
    policy_fee_usdc TEXT NOT NULL,
    confirmation_status TEXT NOT NULL DEFAULT 'local_only',
    confirmed_at TEXT,
    external_trade_id TEXT,
    order_id TEXT,
    client_order_id TEXT,
    source TEXT NOT NULL,
    source_path TEXT,
    source_line INTEGER,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(trade_key) REFERENCES strategy_trades(trade_key) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_venue_fills_trade_key
    ON venue_fills(trade_key);

CREATE INDEX IF NOT EXISTS idx_venue_fills_venue_market_ts
    ON venue_fills(venue, market, timestamp);

CREATE TABLE IF NOT EXISTS sync_state (
    source TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    market TEXT NOT NULL,
    path TEXT NOT NULL,
    last_refresh_at TEXT NOT NULL,
    last_line_count INTEGER NOT NULL,
    last_mtime_ns INTEGER,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS reconciliation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    mode TEXT NOT NULL,
    market TEXT NOT NULL,
    severity TEXT NOT NULL,
    subject TEXT NOT NULL,
    detail TEXT NOT NULL
);
"""


@dataclass
class IngestStats:
    source: str
    path: Path
    read: int = 0
    upserted_trades: int = 0
    upserted_fills: int = 0
    skipped: int = 0
    missing: bool = False
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "path": self.path,
            "read": self.read,
            "upserted_trades": self.upserted_trades,
            "upserted_fills": self.upserted_fills,
            "skipped": self.skipped,
            "missing": self.missing,
            "error": self.error,
        }


def decimal_str(value: Decimal) -> str:
    return format(value.normalize(), "f")


def raw_json(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"))


def parse_timestamp(raw: Any) -> str | None:
    if raw is None or raw == "":
        return None
    return iso(parse_dt(str(raw)))


def timestamp_us(dt: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = dt.astimezone(timezone.utc) - epoch
    return ((delta.days * 86400 + delta.seconds) * 1_000_000) + delta.microseconds


def parse_timestamp_us(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    return timestamp_us(parse_dt(str(raw)))


def abs_fee(notional: Decimal, rate: Decimal) -> Decimal:
    return abs(notional) * rate


def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    migrate_schema(conn)


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row["name"] if isinstance(row, sqlite3.Row) else row[1]) for row in rows}


def migrate_schema(conn: sqlite3.Connection) -> None:
    for table, key_column in [("strategy_trades", "trade_key"), ("venue_fills", "fill_key")]:
        if "timestamp_us" not in table_columns(conn, table):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN timestamp_us INTEGER")
        rows = conn.execute(
            f"SELECT {key_column}, timestamp FROM {table} WHERE timestamp IS NOT NULL AND timestamp_us IS NULL"
        ).fetchall()
        for row in rows:
            try:
                ts_us = parse_timestamp_us(row["timestamp"])
            except (ValueError, TypeError):
                continue
            conn.execute(
                f"UPDATE {table} SET timestamp_us = ? WHERE {key_column} = ?",
                (ts_us, row[key_column]),
            )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_trades_market_ts_us ON strategy_trades(market, timestamp_us)")


def iter_jsonl(path: Path):
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield line_no, json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"warn: skipping invalid JSON in {path}:{line_no}: {exc}", file=sys.stderr)


def nested_dec(row: dict[str, Any], key: str, default: Decimal = Decimal("0")) -> Decimal:
    return dec(row.get(key), default)


def notional_from_fill(fill: dict[str, Any], qty: Decimal, px: Decimal) -> Decimal:
    if fill.get("notional") is not None:
        return dec(fill.get("notional"))
    return qty * px


def taker_sides(direction: Any) -> tuple[str | None, str | None]:
    direction_upper = str(direction or "").upper()
    if direction_upper == "SELL_ASTER_BUY_LIGHTER":
        return "sell", "buy"
    if direction_upper == "BUY_ASTER_SELL_LIGHTER":
        return "buy", "sell"
    return None, None


def xemm_sides_from_hedge(direction: Any) -> tuple[str | None, str | None]:
    direction_lower = str(direction or "").lower()
    if direction_lower.endswith("buy"):
        return "sell", "buy"
    if direction_lower.endswith("sell"):
        return "buy", "sell"
    return None, None


def taker_trade_from_row(
    row: dict[str, Any],
    *,
    mode: str,
    path: Path,
    line_no: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    aster_fill = row.get("aster_fill") or {}
    lighter_fill = row.get("lighter_fill") or {}
    if not isinstance(aster_fill, dict) or not isinstance(lighter_fill, dict):
        return None

    aster_order_id = row.get("aster_order_id")
    lighter_client_order_index = row.get("lighter_client_order_index")
    if aster_order_id is None or lighter_client_order_index is None:
        return None

    raw_timestamp = row.get("timestamp")
    timestamp = parse_timestamp(raw_timestamp)
    ts_us = parse_timestamp_us(raw_timestamp)
    market = str(row.get("market") or "")
    direction = row.get("direction")
    aster_side, lighter_side = taker_sides(direction)
    if not market or aster_side is None or lighter_side is None:
        return None

    qty = nested_dec(row, "qty")
    aster_px = nested_dec(aster_fill, "vwap")
    lighter_px = nested_dec(lighter_fill, "vwap")
    aster_notional = notional_from_fill(aster_fill, qty, aster_px)
    lighter_notional = notional_from_fill(lighter_fill, qty, lighter_px)
    if aster_side == "sell" and lighter_side == "buy":
        gross = aster_notional - lighter_notional
    elif aster_side == "buy" and lighter_side == "sell":
        gross = lighter_notional - aster_notional
    else:
        return None

    aster_fee = abs_fee(aster_notional, ASTER_TAKER_FEE_RATE)
    lighter_fee = Decimal("0")
    fees = aster_fee + lighter_fee
    net = gross - fees
    trade_key = f"taker:{aster_order_id}:{lighter_client_order_index}"
    now = iso(utc_now())
    source = "taker_local_ledger"
    raw = raw_json(row)

    trade = {
        "trade_key": trade_key,
        "mode": mode,
        "strategy": "TAKER",
        "bot": TAKER_BOT,
        "market": market,
        "timestamp": timestamp,
        "timestamp_us": ts_us,
        "direction": str(direction or ""),
        "qty": decimal_str(qty),
        "gross_pnl_usdc": decimal_str(gross),
        "policy_fees_usdc": decimal_str(fees),
        "net_pnl_usdc": decimal_str(net),
        "aster_fee_usdc": decimal_str(aster_fee),
        "lighter_fee_usdc": decimal_str(lighter_fee),
        "aster_fee_rate": decimal_str(ASTER_TAKER_FEE_RATE),
        "lighter_fee_rate": decimal_str(LIGHTER_FEE_RATE),
        "aster_order_id": str(aster_order_id),
        "lighter_client_order_index": str(lighter_client_order_index),
        "cloid": None,
        "aster_px": decimal_str(aster_px),
        "lighter_px": decimal_str(lighter_px),
        "confirmation_status": "local_only",
        "confirmed_at": None,
        "source": source,
        "source_path": str(path),
        "source_line": line_no,
        "raw_json": raw,
        "created_at": now,
        "updated_at": now,
    }
    fills = [
        {
            "fill_key": f"local:{trade_key}:aster:{aster_order_id}",
            "trade_key": trade_key,
            "mode": mode,
            "venue": "aster",
            "market": market,
            "timestamp": timestamp,
            "timestamp_us": ts_us,
            "side": aster_side,
            "qty": decimal_str(dec(aster_fill.get("qty"), qty)),
            "price": decimal_str(aster_px),
            "notional_usdc": decimal_str(aster_notional),
            "liquidity": "taker",
            "fee_rate": decimal_str(ASTER_TAKER_FEE_RATE),
            "policy_fee_usdc": decimal_str(aster_fee),
            "confirmation_status": "local_only",
            "confirmed_at": None,
            "external_trade_id": None,
            "order_id": str(aster_order_id),
            "client_order_id": None,
            "source": source,
            "source_path": str(path),
            "source_line": line_no,
            "raw_json": raw,
            "created_at": now,
            "updated_at": now,
        },
        {
            "fill_key": f"local:{trade_key}:lighter:{lighter_client_order_index}",
            "trade_key": trade_key,
            "mode": mode,
            "venue": "lighter",
            "market": market,
            "timestamp": timestamp,
            "timestamp_us": ts_us,
            "side": lighter_side,
            "qty": decimal_str(dec(lighter_fill.get("qty"), qty)),
            "price": decimal_str(lighter_px),
            "notional_usdc": decimal_str(lighter_notional),
            "liquidity": "unknown",
            "fee_rate": decimal_str(LIGHTER_FEE_RATE),
            "policy_fee_usdc": decimal_str(lighter_fee),
            "confirmation_status": "local_only",
            "confirmed_at": None,
            "external_trade_id": None,
            "order_id": None,
            "client_order_id": str(lighter_client_order_index),
            "source": source,
            "source_path": str(path),
            "source_line": line_no,
            "raw_json": raw,
            "created_at": now,
            "updated_at": now,
        },
    ]
    return trade, fills


def xemm_trade_from_orchestrator_row(
    row: dict[str, Any],
    *,
    mode: str,
    path: Path,
    line_no: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    if row.get("bot") != XEMM_BOT:
        return None
    trade_key = str(row.get("key") or "")
    if not trade_key:
        cloid = row.get("cloid")
        if cloid is None:
            return None
        trade_key = f"xemm:{cloid}"

    raw_timestamp = row.get("timestamp")
    timestamp = parse_timestamp(raw_timestamp)
    ts_us = parse_timestamp_us(raw_timestamp)
    market = str(row.get("market") or "")
    direction = row.get("direction")
    aster_side, lighter_side = xemm_sides_from_hedge(direction)
    if not market or aster_side is None or lighter_side is None:
        return None

    qty = nested_dec(row, "qty")
    aster_px = nested_dec(row, "aster_px")
    lighter_px = nested_dec(row, "lighter_px")
    if lighter_side == "buy":
        gross = qty * (aster_px - lighter_px)
    elif lighter_side == "sell":
        gross = qty * (lighter_px - aster_px)
    else:
        return None

    aster_notional = qty * aster_px
    lighter_notional = qty * lighter_px
    aster_fee = abs_fee(aster_notional, ASTER_MAKER_FEE_RATE)
    lighter_fee = abs_fee(lighter_notional, LIGHTER_FEE_RATE)
    fees = aster_fee + lighter_fee
    net = gross - fees
    now = iso(utc_now())
    source = "orchestrator_normalized_ledger"
    raw = raw_json(row)
    cloid = row.get("cloid")

    trade = {
        "trade_key": trade_key,
        "mode": mode,
        "strategy": "XEMM",
        "bot": XEMM_BOT,
        "market": market,
        "timestamp": timestamp,
        "timestamp_us": ts_us,
        "direction": str(direction or ""),
        "qty": decimal_str(qty),
        "gross_pnl_usdc": decimal_str(gross),
        "policy_fees_usdc": decimal_str(fees),
        "net_pnl_usdc": decimal_str(net),
        "aster_fee_usdc": decimal_str(aster_fee),
        "lighter_fee_usdc": decimal_str(lighter_fee),
        "aster_fee_rate": decimal_str(ASTER_MAKER_FEE_RATE),
        "lighter_fee_rate": decimal_str(LIGHTER_FEE_RATE),
        "aster_order_id": None,
        "lighter_client_order_index": None,
        "cloid": None if cloid is None else str(cloid),
        "aster_px": decimal_str(aster_px),
        "lighter_px": decimal_str(lighter_px),
        "confirmation_status": "local_only",
        "confirmed_at": None,
        "source": source,
        "source_path": str(path),
        "source_line": line_no,
        "raw_json": raw,
        "created_at": now,
        "updated_at": now,
    }
    fills = [
        {
            "fill_key": f"local:{trade_key}:aster:{cloid or line_no}",
            "trade_key": trade_key,
            "mode": mode,
            "venue": "aster",
            "market": market,
            "timestamp": timestamp,
            "timestamp_us": ts_us,
            "side": aster_side,
            "qty": decimal_str(qty),
            "price": decimal_str(aster_px),
            "notional_usdc": decimal_str(aster_notional),
            "liquidity": "maker",
            "fee_rate": decimal_str(ASTER_MAKER_FEE_RATE),
            "policy_fee_usdc": decimal_str(aster_fee),
            "confirmation_status": "local_only",
            "confirmed_at": None,
            "external_trade_id": None,
            "order_id": None,
            "client_order_id": None if cloid is None else str(cloid),
            "source": source,
            "source_path": str(path),
            "source_line": line_no,
            "raw_json": raw,
            "created_at": now,
            "updated_at": now,
        },
        {
            "fill_key": f"local:{trade_key}:lighter:{cloid or line_no}",
            "trade_key": trade_key,
            "mode": mode,
            "venue": "lighter",
            "market": market,
            "timestamp": timestamp,
            "timestamp_us": ts_us,
            "side": lighter_side,
            "qty": decimal_str(qty),
            "price": decimal_str(lighter_px),
            "notional_usdc": decimal_str(lighter_notional),
            "liquidity": "hedge",
            "fee_rate": decimal_str(LIGHTER_FEE_RATE),
            "policy_fee_usdc": decimal_str(lighter_fee),
            "confirmation_status": "local_only",
            "confirmed_at": None,
            "external_trade_id": None,
            "order_id": None,
            "client_order_id": None if cloid is None else str(cloid),
            "source": source,
            "source_path": str(path),
            "source_line": line_no,
            "raw_json": raw,
            "created_at": now,
            "updated_at": now,
        },
    ]
    return trade, fills


def upsert_row(conn: sqlite3.Connection, table: str, key_column: str, row: dict[str, Any], preserve: set[str]) -> None:
    columns = list(row.keys())
    placeholders = ", ".join("?" for _ in columns)
    assignments = ", ".join(
        f"{column}=excluded.{column}" for column in columns if column != key_column and column not in preserve
    )
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT({key_column}) DO UPDATE SET {assignments}"
    )
    conn.execute(sql, [row[column] for column in columns])


def upsert_trade(conn: sqlite3.Connection, trade: dict[str, Any]) -> None:
    upsert_row(
        conn,
        "strategy_trades",
        "trade_key",
        trade,
        preserve={"confirmation_status", "confirmed_at", "created_at"},
    )


def upsert_fill(conn: sqlite3.Connection, fill: dict[str, Any]) -> None:
    upsert_row(
        conn,
        "venue_fills",
        "fill_key",
        fill,
        preserve={"confirmation_status", "confirmed_at", "external_trade_id", "created_at"},
    )


def update_sync_state(conn: sqlite3.Connection, stats: IngestStats, *, mode: str, market: str) -> None:
    mtime_ns = None
    if stats.path.exists():
        mtime_ns = stats.path.stat().st_mtime_ns
    conn.execute(
        """
        INSERT INTO sync_state (source, mode, market, path, last_refresh_at, last_line_count, last_mtime_ns, last_error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source) DO UPDATE SET
            mode=excluded.mode,
            market=excluded.market,
            path=excluded.path,
            last_refresh_at=excluded.last_refresh_at,
            last_line_count=excluded.last_line_count,
            last_mtime_ns=excluded.last_mtime_ns,
            last_error=excluded.last_error
        """,
        (
            stats.source,
            mode,
            market,
            str(stats.path),
            iso(utc_now()),
            stats.read,
            mtime_ns,
            stats.error,
        ),
    )


def ingest_taker_trades(conn: sqlite3.Connection, path: Path, *, mode: str, market: str) -> IngestStats:
    stats = IngestStats("taker_local_ledger", path)
    if not path.exists():
        stats.missing = True
        update_sync_state(conn, stats, mode=mode, market=market)
        return stats

    for line_no, row in iter_jsonl(path):
        stats.read += 1
        if row.get("market") != market:
            stats.skipped += 1
            continue
        try:
            parsed = taker_trade_from_row(row, mode=mode, path=path, line_no=line_no)
        except Exception as exc:  # keep one malformed local line from blocking history refresh
            stats.skipped += 1
            stats.error = str(exc)
            continue
        if parsed is None:
            stats.skipped += 1
            continue
        trade, fills = parsed
        upsert_trade(conn, trade)
        stats.upserted_trades += 1
        for fill in fills:
            upsert_fill(conn, fill)
            stats.upserted_fills += 1
    update_sync_state(conn, stats, mode=mode, market=market)
    return stats


def ingest_orchestrator_xemm(conn: sqlite3.Connection, path: Path, *, mode: str, market: str) -> IngestStats:
    stats = IngestStats("orchestrator_normalized_ledger", path)
    if not path.exists():
        stats.missing = True
        update_sync_state(conn, stats, mode=mode, market=market)
        return stats

    for line_no, row in iter_jsonl(path):
        stats.read += 1
        if row.get("market") != market:
            stats.skipped += 1
            continue
        try:
            parsed = xemm_trade_from_orchestrator_row(row, mode=mode, path=path, line_no=line_no)
        except Exception as exc:
            stats.skipped += 1
            stats.error = str(exc)
            continue
        if parsed is None:
            stats.skipped += 1
            continue
        trade, fills = parsed
        upsert_trade(conn, trade)
        stats.upserted_trades += 1
        for fill in fills:
            upsert_fill(conn, fill)
            stats.upserted_fills += 1
    update_sync_state(conn, stats, mode=mode, market=market)
    return stats


def xemm_journal_row_ts(row: dict[str, Any]) -> tuple[str | None, int | None]:
    ts_ms = row.get("ts_ms")
    if isinstance(ts_ms, (int, float)) and ts_ms > 0:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return iso(dt), timestamp_us(dt)
    return None, None


def ingest_xemm_journal(conn: sqlite3.Connection, path: Path, *, mode: str, market: str) -> IngestStats:
    """Ingest XEMM trades directly from the bot journal (fill/hedge_fill pairs by cloid).

    The journal is bot-written, so trades that filled while the orchestrator was down are
    still captured, and `ts_ms` is the actual trade time — unlike the orchestrator ledger,
    whose rows are stamped at poll time. Runs AFTER the orchestrator-ledger ingestion so
    the journal's timestamps and actual hedge fees win on shared `xemm:{cloid}` keys.
    Legacy rows without ts_ms are left to the orchestrator-ledger source.
    """
    stats = IngestStats("xemm_journal", path)
    if not path.exists():
        stats.missing = True
        update_sync_state(conn, stats, mode=mode, market=market)
        return stats

    fills: dict[str, dict[str, Any]] = {}
    hedges: dict[str, dict[str, Any]] = {}
    for line_no, row in iter_jsonl(path):
        stats.read += 1
        if row.get("market") != market:
            stats.skipped += 1
            continue
        kind = row.get("kind")
        detail = row.get("detail") if isinstance(row.get("detail"), dict) else {}
        cloid = detail.get("cloid")
        if kind not in ("fill", "hedge_fill") or cloid is None:
            stats.skipped += 1
            continue
        timestamp, ts_us = xemm_journal_row_ts(row)
        if timestamp is None:
            stats.skipped += 1  # legacy row without ts_ms: orchestrator ledger covers it
            continue
        cloid = str(cloid)
        try:
            qty = Decimal(str(detail.get("qty", "0")))
        except Exception:
            stats.skipped += 1
            continue
        if kind == "fill":
            try:
                aster_px = Decimal(str(detail.get("avg_aster_px", "0")))
            except Exception:
                stats.skipped += 1
                continue
            fills[cloid] = {
                "qty": qty,
                "aster_px": aster_px,
                "side": str(detail.get("side", "")).lower(),
                "timestamp": timestamp,
                "ts_us": ts_us,
                "line_no": line_no,
                "raw": row,
            }
        else:
            try:
                px = Decimal(str(detail.get("px", "0")))
            except Exception:
                stats.skipped += 1
                continue
            fee_raw = detail.get("fee_usd")
            try:
                fee = Decimal(str(fee_raw)) if fee_raw is not None else None
            except Exception:
                fee = None
            agg = hedges.setdefault(
                cloid,
                {
                    "qty": Decimal("0"),
                    "notional": Decimal("0"),
                    "fee": Decimal("0"),
                    "fee_missing_notional": Decimal("0"),
                    "side": str(detail.get("side", "")).lower(),
                    "timestamp": timestamp,
                    "ts_us": ts_us,
                },
            )
            agg["qty"] += qty
            agg["notional"] += qty * px
            if fee is not None:
                agg["fee"] += fee
            else:
                agg["fee_missing_notional"] += qty * px
            if ts_us is not None and (agg["ts_us"] is None or ts_us > agg["ts_us"]):
                agg["timestamp"], agg["ts_us"] = timestamp, ts_us

    now = iso(utc_now())
    for cloid, fill in fills.items():
        hedge = hedges.get(cloid)
        if hedge is None or hedge["qty"] <= 0 or fill["qty"] != hedge["qty"] or fill["side"] != hedge["side"]:
            stats.skipped += 1
            continue
        lighter_px = hedge["notional"] / hedge["qty"]
        qty = fill["qty"]
        aster_px = fill["aster_px"]
        # Journal `side` is the HEDGE side: hedge buy means the maker leg sold on Aster.
        if fill["side"] == "buy":
            gross = qty * (aster_px - lighter_px)
            aster_side, lighter_side = "sell", "buy"
        elif fill["side"] == "sell":
            gross = qty * (lighter_px - aster_px)
            aster_side, lighter_side = "buy", "sell"
        else:
            stats.skipped += 1
            continue
        aster_notional = qty * aster_px
        lighter_notional = qty * lighter_px
        aster_fee = abs_fee(aster_notional, ASTER_MAKER_FEE_RATE)
        lighter_fee = hedge["fee"] + abs_fee(hedge["fee_missing_notional"], LIGHTER_FEE_RATE)
        fees = aster_fee + lighter_fee
        trade_key = f"xemm:{cloid}"
        timestamp = max(
            (t for t in [fill["timestamp"], hedge["timestamp"]] if t is not None),
            default=fill["timestamp"],
        )
        ts_us = max(
            (t for t in [fill["ts_us"], hedge["ts_us"]] if t is not None),
            default=fill["ts_us"],
        )
        raw = raw_json(fill["raw"])
        trade = {
            "trade_key": trade_key,
            "mode": mode,
            "strategy": "XEMM",
            "bot": XEMM_BOT,
            "market": market,
            "timestamp": timestamp,
            "timestamp_us": ts_us,
            "direction": fill["side"],
            "qty": decimal_str(qty),
            "gross_pnl_usdc": decimal_str(gross),
            "policy_fees_usdc": decimal_str(fees),
            "net_pnl_usdc": decimal_str(gross - fees),
            "aster_fee_usdc": decimal_str(aster_fee),
            "lighter_fee_usdc": decimal_str(lighter_fee),
            "aster_fee_rate": decimal_str(ASTER_MAKER_FEE_RATE),
            "lighter_fee_rate": decimal_str(LIGHTER_FEE_RATE),
            "aster_order_id": None,
            "lighter_client_order_index": None,
            "cloid": cloid,
            "aster_px": decimal_str(aster_px),
            "lighter_px": decimal_str(lighter_px),
            "confirmation_status": "local_only",
            "confirmed_at": None,
            "source": "xemm_journal",
            "source_path": str(path),
            "source_line": fill["line_no"],
            "raw_json": raw,
            "created_at": now,
            "updated_at": now,
        }
        fill_rows = []
        for venue, side, px, notional, liquidity, fee_rate, fee_usd in [
            ("aster", aster_side, aster_px, aster_notional, "maker", ASTER_MAKER_FEE_RATE, aster_fee),
            ("lighter", lighter_side, lighter_px, lighter_notional, "hedge", LIGHTER_FEE_RATE, lighter_fee),
        ]:
            fill_rows.append(
                {
                    "fill_key": f"local:{trade_key}:{venue}:{cloid}",
                    "trade_key": trade_key,
                    "mode": mode,
                    "venue": venue,
                    "market": market,
                    "timestamp": timestamp,
                    "timestamp_us": ts_us,
                    "side": side,
                    "qty": decimal_str(qty),
                    "price": decimal_str(px),
                    "notional_usdc": decimal_str(notional),
                    "liquidity": liquidity,
                    "fee_rate": decimal_str(fee_rate),
                    "policy_fee_usdc": decimal_str(fee_usd),
                    "confirmation_status": "local_only",
                    "confirmed_at": None,
                    "external_trade_id": None,
                    "order_id": None,
                    "client_order_id": cloid,
                    "source": "xemm_journal",
                    "source_path": str(path),
                    "source_line": fill["line_no"],
                    "raw_json": raw,
                    "created_at": now,
                    "updated_at": now,
                }
            )
        upsert_trade(conn, trade)
        stats.upserted_trades += 1
        for fill_row in fill_rows:
            upsert_fill(conn, fill_row)
            stats.upserted_fills += 1
    update_sync_state(conn, stats, mode=mode, market=market)
    return stats


def refresh_lan(
    conn: sqlite3.Connection,
    *,
    market: str,
    taker_trades: Path,
    orchestrator_trades: Path,
    xemm_journal: Path | None = None,
) -> list[IngestStats]:
    mode = "lan"
    stats = [
        ingest_taker_trades(conn, taker_trades, mode=mode, market=market),
        ingest_orchestrator_xemm(conn, orchestrator_trades, mode=mode, market=market),
    ]
    if xemm_journal is not None:
        # Last so the journal's real trade times + actual hedge fees win on shared keys.
        stats.append(ingest_xemm_journal(conn, xemm_journal, mode=mode, market=market))
    conn.commit()
    return stats


def empty_bucket(strategy: str) -> dict[str, Any]:
    return {
        "strategy": strategy,
        "trades": 0,
        "gross_pnl_usdc": Decimal("0"),
        "policy_fees_usdc": Decimal("0"),
        "net_pnl_usdc": Decimal("0"),
        "aster_fees_usdc": Decimal("0"),
        "lighter_fees_usdc": Decimal("0"),
        "local_only_trades": 0,
        "exchange_confirmed_trades": 0,
    }


def report_from_db(
    conn: sqlite3.Connection,
    *,
    market: str,
    since: datetime,
    now: datetime,
    db_path: Path,
    capital_usdc: Decimal | None = None,
    orchestrator_state: Path | None = None,
) -> dict[str, Any]:
    buckets = {"TAKER": empty_bucket("TAKER"), "XEMM": empty_bucket("XEMM")}
    confirmation_counts: dict[str, int] = {}
    rows = conn.execute(
        """
        SELECT strategy, gross_pnl_usdc, policy_fees_usdc, net_pnl_usdc,
               aster_fee_usdc, lighter_fee_usdc, confirmation_status
        FROM strategy_trades
        WHERE market = ? AND timestamp_us IS NOT NULL AND timestamp_us >= ? AND timestamp_us <= ?
        ORDER BY timestamp_us ASC, timestamp ASC, trade_key ASC
        """,
        (market, timestamp_us(since), timestamp_us(now)),
    ).fetchall()
    for row in rows:
        strategy = str(row["strategy"])
        bucket = buckets.setdefault(strategy, empty_bucket(strategy))
        status = str(row["confirmation_status"])
        confirmation_counts[status] = confirmation_counts.get(status, 0) + 1
        bucket["trades"] += 1
        bucket["gross_pnl_usdc"] += dec(row["gross_pnl_usdc"])
        bucket["policy_fees_usdc"] += dec(row["policy_fees_usdc"])
        bucket["net_pnl_usdc"] += dec(row["net_pnl_usdc"])
        bucket["aster_fees_usdc"] += dec(row["aster_fee_usdc"])
        bucket["lighter_fees_usdc"] += dec(row["lighter_fee_usdc"])
        if status == "exchange_confirmed":
            bucket["exchange_confirmed_trades"] += 1
        elif status == "local_only":
            bucket["local_only_trades"] += 1

    total = empty_bucket("TOTAL")
    for bucket in buckets.values():
        total["trades"] += bucket["trades"]
        total["gross_pnl_usdc"] += bucket["gross_pnl_usdc"]
        total["policy_fees_usdc"] += bucket["policy_fees_usdc"]
        total["net_pnl_usdc"] += bucket["net_pnl_usdc"]
        total["aster_fees_usdc"] += bucket["aster_fees_usdc"]
        total["lighter_fees_usdc"] += bucket["lighter_fees_usdc"]
        total["local_only_trades"] += bucket["local_only_trades"]
        total["exchange_confirmed_trades"] += bucket["exchange_confirmed_trades"]

    capital_source = "cli"
    capital = capital_usdc
    if capital is None and orchestrator_state is not None:
        capital, capital_source = latest_capital_from_state(orchestrator_state)
    proj = projection(total["net_pnl_usdc"], capital, since, now)
    return {
        "mode": "lan",
        "db": db_path,
        "market": market,
        "since": since,
        "now": now,
        "fee_policy": {
            "aster_taker": ASTER_TAKER_FEE_RATE,
            "aster_maker": ASTER_MAKER_FEE_RATE,
            "lighter": LIGHTER_FEE_RATE,
        },
        "by_strategy": [buckets[key] for key in sorted(buckets.keys())],
        "total": total,
        "confirmation_counts": confirmation_counts,
        "projection": proj,
        "capital_source": capital_source,
        "notes": [
            "LAN mode reads local bot artifacts only and makes no exchange API calls.",
            "Fees are recomputed from policy: Aster taker 0.04%, Aster maker 0%, Lighter 0%.",
            "confirmation_status stays local_only until exchange-history adapters confirm or repair rows.",
        ],
    }


def fmt_money(value: Decimal, signed: bool = True, places: int = 8) -> str:
    sign = "+" if signed else ""
    return f"{value:{sign}.{places}f}"


def fmt_pct(value: Decimal | None, places: int) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{places}f}%"


def print_table(title: str, headers: list[str], rows: list[list[Any]], right_align: set[int] | None = None) -> None:
    right_align = right_align or set()
    text_rows = [[str(cell) for cell in row] for row in rows]
    widths = [max(len(headers[idx]), *(len(row[idx]) for row in text_rows)) for idx in range(len(headers))]

    def render_row(row: list[str]) -> str:
        cells = []
        for idx, cell in enumerate(row):
            cells.append(cell.rjust(widths[idx]) if idx in right_align else cell.ljust(widths[idx]))
        return " | ".join(cells)

    print(title)
    print(render_row(headers))
    print("-+-".join("-" * width for width in widths))
    for row in text_rows:
        print(render_row(row))


def print_human(stats: list[IngestStats], report: dict[str, Any] | None) -> None:
    if stats:
        print_table(
            "Refresh",
            ["Source", "Rows", "Trades", "Fills", "Skipped", "Missing"],
            [
                [s.source, s.read, s.upserted_trades, s.upserted_fills, s.skipped, "yes" if s.missing else "no"]
                for s in stats
            ],
            right_align={1, 2, 3, 4},
        )
        print()
    if report is None:
        return
    p = report["projection"]
    print_table(
        "Trade History PnL",
        ["Source", "Trades", "Gross USDC", "Policy Fees", "Net USDC", "Local Only", "Confirmed"],
        [
            [
                bucket["strategy"],
                bucket["trades"],
                fmt_money(bucket["gross_pnl_usdc"]),
                fmt_money(bucket["policy_fees_usdc"], signed=False),
                fmt_money(bucket["net_pnl_usdc"]),
                bucket["local_only_trades"],
                bucket["exchange_confirmed_trades"],
            ]
            for bucket in report["by_strategy"]
        ]
        + [
            [
                "TOTAL",
                report["total"]["trades"],
                fmt_money(report["total"]["gross_pnl_usdc"]),
                fmt_money(report["total"]["policy_fees_usdc"], signed=False),
                fmt_money(report["total"]["net_pnl_usdc"]),
                report["total"]["local_only_trades"],
                report["total"]["exchange_confirmed_trades"],
            ]
        ],
        right_align={1, 2, 3, 4, 5, 6},
    )
    print()
    print_table(
        "Projection",
        ["Metric", "Value"],
        [
            ["Mode", report["mode"]],
            ["DB", report["db"]],
            ["Market", report["market"]],
            ["Since UTC", iso(report["since"])],
            ["Now UTC", iso(report["now"])],
            ["Elapsed Days", f"{p['elapsed_days']:.6f}"],
            ["Capital USDC", p["capital_usdc"] if p["capital_usdc"] is not None else "n/a"],
            ["Window Return", fmt_pct(p["window_return_pct"], 8)],
            ["Simple Annualized", fmt_pct(p["simple_annualized_return_pct"], 4)],
            ["Projected CAGR", fmt_pct(p["projected_cagr_pct"], 4)],
        ],
    )
    print()
    print("notes:")
    for note in report["notes"]:
        print(f"- {note}")


def parse_args() -> argparse.Namespace:
    stack_root = Path(__file__).resolve().parent
    taker_root = stack_root / "LIGHTER_ASTER_TAKER_ARB"
    parser = argparse.ArgumentParser(description="Canonical local trade-history DB and PnL report.")
    parser.add_argument("--mode", choices=["lan", "local"], default="lan", help="lan/local: local artifacts only; no exchange API calls.")
    parser.add_argument("--market", default="HYPE")
    parser.add_argument("--since", default=DEFAULT_SINCE, help=f"UTC/RFC3339 start time. Default: {DEFAULT_SINCE}.")
    parser.add_argument("--now", default=None, help="Override report end time. Defaults to current UTC time.")
    parser.add_argument("--db", type=Path, default=stack_root / "runs/trade_history.sqlite")
    parser.add_argument("--taker-trades", type=Path, default=None)
    parser.add_argument("--orchestrator-trades", type=Path, default=None)
    parser.add_argument("--xemm-journal", type=Path, default=None, help="XEMM bot journal (fill/hedge_fill pairs); the authoritative trade-time source.")
    parser.add_argument("--orchestrator-state", type=Path, default=None)
    parser.add_argument("--capital-usdc", type=Decimal, default=None)
    parser.add_argument("--no-refresh", action="store_true", help="Report existing DB contents without reading local ledgers first.")
    parser.add_argument("--refresh-only", action="store_true", help="Refresh the DB and skip the PnL report.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args()
    args.mode = "lan"
    if args.taker_trades is None:
        args.taker_trades = taker_root / f"runs/trades_{args.market}.jsonl"
    if args.orchestrator_trades is None:
        args.orchestrator_trades = stack_root / f"runs/orchestrator_trades_{args.market}.jsonl"
    if args.xemm_journal is None:
        args.xemm_journal = stack_root / f"runs/orchestrator-xemm-{args.market}-journal.jsonl"
    if args.orchestrator_state is None:
        args.orchestrator_state = stack_root / f"runs/orchestrator_state_{args.market}.json"
    return args


def main() -> int:
    args = parse_args()
    since = parse_dt(args.since)
    now = parse_dt(args.now) if args.now else utc_now()
    with open_db(args.db) as conn:
        init_db(conn)
        stats: list[IngestStats] = []
        if not args.no_refresh:
            stats = refresh_lan(
                conn,
                market=args.market,
                taker_trades=args.taker_trades,
                orchestrator_trades=args.orchestrator_trades,
                xemm_journal=args.xemm_journal,
            )
        report = None
        if not args.refresh_only:
            report = report_from_db(
                conn,
                market=args.market,
                since=since,
                now=now,
                db_path=args.db,
                capital_usdc=args.capital_usdc,
                orchestrator_state=args.orchestrator_state,
            )
    if args.json:
        print(json.dumps({"refresh": [s.as_dict() for s in stats], "report": report}, default=json_default, indent=2))
    else:
        print_human(stats, report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
