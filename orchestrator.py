#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


TAKER_BOT = "LIGHTER_ASTER_TAKER_ARB"
XEMM_BOT = "XEMM_LIGHTER_ASTER"
TAKER_OBSERVER = "LIGHTER_ASTER_TAKER_ARB_OBSERVER"
SECONDS_PER_YEAR = Decimal(365 * 24 * 60 * 60)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime | None = None) -> str:
    return (dt or utc_now()).isoformat().replace("+00:00", "Z")


def stamp(dt: datetime | None = None) -> str:
    return (dt or utc_now()).strftime("%Y%m%dT%H%M%SZ")


def prune_old_tapes(
    state_dir: Path, market: str, retention_days: float, now: datetime
) -> list[tuple[Path, int]]:
    """Delete finished XEMM research tapes (`--out` recordings) past retention.

    Tapes are only needed to replay a session; at ~100-200 MB/day they are the
    main long-term disk consumer after the results db. The active tape receives
    book snapshots continuously, so an mtime older than the window can only
    belong to a finished run. retention_days <= 0 keeps everything.
    Returns (path, size_bytes) for each deleted file.
    """
    if retention_days <= 0:
        return []
    cutoff = (now - timedelta(days=retention_days)).timestamp()
    pruned: list[tuple[Path, int]] = []
    for path in sorted(state_dir.glob(f"orchestrator_xemm_{market}_*.jsonl.zst")):
        try:
            st = path.stat()
            if st.st_mtime < cutoff:
                path.unlink()
                pruned.append((path, st.st_size))
        except OSError:
            continue
    return pruned


def parse_decimal(value: Any, default: Decimal | None = None) -> Decimal | None:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return default


def dec_or_zero(value: Any) -> Decimal:
    return parse_decimal(value, Decimal("0")) or Decimal("0")


def dec_to_json(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return dec_to_json(value)
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"{type(value)!r} is not JSON serializable")


def load_json_line(line: str, path: Path, line_no: int) -> dict[str, Any] | None:
    line = line.strip()
    if not line:
        return None
    try:
        value = json.loads(line)
    except json.JSONDecodeError as exc:
        print(f"warn: skipping invalid JSON in {path}:{line_no}: {exc}", file=sys.stderr)
        return None
    if not isinstance(value, dict):
        return None
    return value


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        json.dump(row, f, default=json_default, separators=(",", ":"))
        f.write("\n")
        f.flush()


def text_tail(value: Any, limit: int = 2000) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    text = str(value)
    return text[-limit:]


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, default=json_default, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def extract_json_object(stdout: str) -> dict[str, Any]:
    start = stdout.find("{")
    end = stdout.rfind("}")
    if start < 0 or end < start:
        raise ValueError(f"no JSON object in command output: {stdout[:300]!r}")
    return json.loads(stdout[start : end + 1])


def parse_since(raw: str, start: datetime) -> datetime:
    raw = raw.strip()
    if raw.lower() in {"startup", "now"}:
        return start
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        try:
            stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()
            if len(stat) > 2 and stat[2] == "Z":
                return False
        except Exception:
            pass
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def wait_process_exit(pid: int, timeout_sec: int) -> bool:
    deadline = time.monotonic() + max(0, timeout_sec)
    while time.monotonic() < deadline:
        if not process_alive(pid):
            return True
        time.sleep(0.2)
    return not process_alive(pid)


class LockFile:
    def __init__(self, path: Path):
        self.path = path
        self.fd: int | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(str(self.path), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            try:
                raw = os.read(self.fd, 128).decode("utf-8", errors="replace").strip()
                pid = int(raw.splitlines()[0])
            except Exception:
                pid = -1
            os.close(self.fd)
            self.fd = None
            raise SystemExit(f"orchestrator lock is active: {self.path} pid={pid}")
        os.ftruncate(self.fd, 0)
        os.write(self.fd, f"{os.getpid()}\n".encode())

    def release(self) -> None:
        if self.fd is not None:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None


class BotProcess:
    def __init__(self, name: str, cwd: Path, command: list[str], log_path: Path):
        self.name = name
        self.cwd = cwd
        self.command = command
        self.log_path = log_path
        self.proc: subprocess.Popen[bytes] | None = None
        self.log_file: Any = None
        self.started_at: datetime | None = None

    @property
    def pid(self) -> int | None:
        return self.proc.pid if self.proc and self.proc.poll() is None else None

    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self) -> None:
        if self.is_running():
            return
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_path.open("ab")
        self.proc = subprocess.Popen(
            self.command,
            cwd=str(self.cwd),
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.started_at = utc_now()

    def stop(self, grace_sec: int) -> dict[str, Any]:
        if self.proc is None:
            return {"name": self.name, "running": False}
        proc = self.proc
        if proc.poll() is not None:
            code = proc.returncode
            self._close_log()
            return {"name": self.name, "running": False, "exit_code": code}
        result: dict[str, Any] = {"name": self.name, "pid": proc.pid, "signal": "SIGINT"}
        try:
            os.killpg(proc.pid, signal.SIGINT)
        except ProcessLookupError:
            pass
        try:
            result["exit_code"] = proc.wait(timeout=grace_sec)
        except subprocess.TimeoutExpired:
            result["signal"] = "SIGTERM"
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                result["exit_code"] = proc.wait(timeout=max(3, grace_sec // 2))
            except subprocess.TimeoutExpired:
                result["signal"] = "SIGKILL"
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    result["exit_code"] = proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    result["exit_code"] = None
                    result["alive_after_sigkill"] = True
        self._close_log()
        return result

    def poll_exit(self) -> int | None:
        if self.proc is None:
            return None
        code = self.proc.poll()
        if code is not None:
            self._close_log()
        return code

    def _close_log(self) -> None:
        if self.log_file is not None:
            self.log_file.close()
            self.log_file = None


class TradeTracker:
    def __init__(self, args: argparse.Namespace, since: datetime, event):
        self.args = args
        self.market = args.market
        self.since = since
        self.event = event
        self.trades_path = args.state_dir / f"orchestrator_trades_{self.market}.jsonl"
        self.seen: set[str] = set()
        self.trades: list[dict[str, Any]] = []
        self.xemm_report_failures = 0
        self.load_existing_normalized()

    def load_existing_normalized(self) -> None:
        if not self.trades_path.exists():
            return
        with self.trades_path.open("r", encoding="utf-8") as f:
            for idx, line in enumerate(f, 1):
                row = load_json_line(line, self.trades_path, idx)
                if not row:
                    continue
                key = str(row.get("key", ""))
                if key:
                    self.seen.add(key)
                ts = self.row_ts(row)
                if ts is None or ts >= self.since:
                    self.trades.append(row)

    def prime_sources(self) -> None:
        if self.args.backfill_existing_trades:
            return
        for row in self.read_taker_trades():
            self.seen.add(self.taker_key(row))
        for trade in self.read_xemm_trades():
            self.seen.add(self.xemm_key(trade))

    def poll(self) -> list[dict[str, Any]]:
        new_rows = []
        for raw in self.read_taker_trades():
            key = self.taker_key(raw)
            if key in self.seen:
                continue
            ts = self.parse_ts(raw.get("timestamp")) or utc_now()
            if ts < self.since:
                self.seen.add(key)
                continue
            row = {
                "timestamp": iso(ts),
                "key": key,
                "bot": TAKER_BOT,
                "market": raw.get("market", self.market),
                "direction": raw.get("direction"),
                "qty": raw.get("qty"),
                "gross_pnl_usdc": raw.get("actual_gross_usd"),
                "fees_usdc": raw.get("actual_fees_usd"),
                "net_pnl_usdc": raw.get("actual_net_usd"),
                "net_pnl_bps": raw.get("actual_net_bps"),
                "aster_order_id": raw.get("aster_order_id"),
                "lighter_client_order_index": raw.get("lighter_client_order_index"),
                "final_aster_position": raw.get("final_aster_position"),
                "final_lighter_position": raw.get("final_lighter_position"),
            }
            self.record(row, new_rows)
        for trade in self.read_xemm_trades():
            key = self.xemm_key(trade)
            if key in self.seen:
                continue
            row = {
                "timestamp": iso(),
                "key": key,
                "bot": XEMM_BOT,
                "market": trade.get("market", self.market),
                "direction": f"ASTER_MAKER_HEDGE_{trade.get('hedge_side', '')}",
                "qty": trade.get("qty"),
                "gross_pnl_usdc": trade.get("gross_pnl"),
                "fees_usdc": dec_to_json(dec_or_zero(trade.get("aster_fee")) + dec_or_zero(trade.get("lighter_fee"))),
                "net_pnl_usdc": trade.get("net_pnl"),
                "net_pnl_bps": None,
                "cloid": trade.get("cloid"),
                "first_mono_ns": trade.get("first_mono_ns"),
                "last_mono_ns": trade.get("last_mono_ns"),
                "aster_px": trade.get("aster_px"),
                "lighter_px": trade.get("lighter_px"),
            }
            self.record(row, new_rows)
        if new_rows:
            self.event("trades_ingested", count=len(new_rows))
        return new_rows

    def record(self, row: dict[str, Any], out: list[dict[str, Any]]) -> None:
        self.seen.add(str(row["key"]))
        self.trades.append(row)
        append_jsonl(self.trades_path, row)
        out.append(row)

    def read_taker_trades(self) -> list[dict[str, Any]]:
        path = self.args.taker_trades
        if not path.exists():
            return []
        rows = []
        with path.open("r", encoding="utf-8") as f:
            for idx, line in enumerate(f, 1):
                row = load_json_line(line, path, idx)
                if row and row.get("market") in {None, self.market}:
                    rows.append(row)
        return rows

    def read_xemm_trades(self) -> list[dict[str, Any]]:
        journal = self.args.xemm_journal or inferred_xemm_journal(self.args.xemm_db)
        if not journal.exists():
            return []
        cmd = [
            str(self.args.xemm_bin),
            "--config",
            str(self.args.xemm_config),
            "live-report",
            "--db",
            str(self.args.xemm_db),
            "--journal",
            str(journal),
            "--market",
            self.market,
            # Window the journal scan to this tracker's PnL horizon: the journal grows
            # forever, and an unbounded scan eventually exceeds status_timeout_sec —
            # silently starving the realized-loss breaker of XEMM trades.
            "--since-ms",
            str(int(self.since.timestamp() * 1000)),
            "--json",
        ]
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(self.args.xemm_repo),
                text=True,
                capture_output=True,
                timeout=self.args.status_timeout_sec,
                check=True,
            )
            report = extract_json_object(proc.stdout)
        except Exception as exc:
            self.xemm_report_failures += 1
            self.event(
                "xemm_live_report_failed",
                error=str(exc),
                journal=str(journal),
                consecutive_failures=self.xemm_report_failures,
            )
            if self.xemm_report_failures == 3:
                # Escalate: the realized-loss breaker is now blind to XEMM trades. One
                # loud persistent event, not just per-failure noise.
                self.event(
                    "xemm_breaker_feed_starving",
                    consecutive_failures=self.xemm_report_failures,
                    journal=str(journal),
                )
            return []
        self.xemm_report_failures = 0
        return report.get("summary", {}).get("trades", []) or []

    def summary(self) -> dict[str, Any]:
        by_bot: dict[str, dict[str, Any]] = {}
        net_total = Decimal("0")
        gross_total = Decimal("0")
        fees_total = Decimal("0")
        wins = 0
        best: dict[str, Any] | None = None
        worst: dict[str, Any] | None = None
        for row in self.trades:
            bot = str(row.get("bot"))
            net = dec_or_zero(row.get("net_pnl_usdc"))
            gross = dec_or_zero(row.get("gross_pnl_usdc"))
            fees = dec_or_zero(row.get("fees_usdc"))
            net_total += net
            gross_total += gross
            fees_total += fees
            wins += int(net > 0)
            bucket = by_bot.setdefault(
                bot,
                {
                    "trades": 0,
                    "net_pnl_usdc": Decimal("0"),
                    "gross_pnl_usdc": Decimal("0"),
                    "fees_usdc": Decimal("0"),
                    "wins": 0,
                },
            )
            bucket["trades"] += 1
            bucket["net_pnl_usdc"] += net
            bucket["gross_pnl_usdc"] += gross
            bucket["fees_usdc"] += fees
            bucket["wins"] += int(net > 0)
            if best is None or net > dec_or_zero(best.get("net_pnl_usdc")):
                best = row
            if worst is None or net < dec_or_zero(worst.get("net_pnl_usdc")):
                worst = row
        total = len(self.trades)
        for bucket in by_bot.values():
            trades = Decimal(bucket["trades"])
            bucket["avg_net_pnl_usdc"] = bucket["net_pnl_usdc"] / trades if trades else None
            bucket["win_rate"] = Decimal(bucket["wins"]) / trades if trades else None
        return {
            "trades": total,
            "by_bot": by_bot,
            "net_pnl_usdc": net_total,
            "gross_pnl_usdc": gross_total,
            "fees_usdc": fees_total,
            "avg_net_pnl_usdc": net_total / Decimal(total) if total else None,
            "win_rate": Decimal(wins) / Decimal(total) if total else None,
            "best_trade": best,
            "worst_trade": worst,
        }

    @staticmethod
    def taker_key(row: dict[str, Any]) -> str:
        aster_id = row.get("aster_order_id")
        lighter_idx = row.get("lighter_client_order_index")
        if aster_id is not None or lighter_idx is not None:
            return f"taker:{aster_id}:{lighter_idx}"
        raw = json.dumps(row, default=str, sort_keys=True)
        return f"taker:fallback:{hashlib.sha256(raw.encode()).hexdigest()[:16]}"

    @staticmethod
    def xemm_key(row: dict[str, Any]) -> str:
        cloid = row.get("cloid")
        if cloid is not None:
            return f"xemm:{cloid}"
        raw = json.dumps(row, default=str, sort_keys=True)
        return f"xemm:fallback:{hashlib.sha256(raw.encode()).hexdigest()[:16]}"

    @staticmethod
    def row_ts(row: dict[str, Any]) -> datetime | None:
        return TradeTracker.parse_ts(row.get("timestamp"))

    @staticmethod
    def parse_ts(value: Any) -> datetime | None:
        if not value:
            return None
        raw = str(value)
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(raw).astimezone(timezone.utc)
        except ValueError:
            return None


class PnlTracker:
    def __init__(self, args: argparse.Namespace, since: datetime):
        self.args = args
        self.since = since
        self.samples_path = args.state_dir / f"orchestrator_pnl_{args.market}.jsonl"
        self.baseline_path = args.state_dir / f"orchestrator_baseline_{args.market}.json"
        self.baseline_equity: Decimal | None = None
        self.baseline_ts: datetime | None = None
        self.last_equity: Decimal | None = None
        self.last_sample: dict[str, Any] | None = None
        if args.pnl_since.lower() not in {"startup", "now"}:
            self.load_baseline()
        elif args.pnl_since.lower() == "startup":
            self.load_persisted_baseline()

    def load_baseline(self) -> None:
        if not self.samples_path.exists():
            return
        with self.samples_path.open("r", encoding="utf-8") as f:
            for idx, line in enumerate(f, 1):
                row = load_json_line(line, self.samples_path, idx)
                if not row:
                    continue
                ts = TradeTracker.parse_ts(row.get("timestamp"))
                equity = parse_decimal(row.get("total_equity_usd"))
                if ts is None or equity is None or ts < self.since:
                    continue
                self.baseline_ts = ts
                self.baseline_equity = equity
                break

    def load_persisted_baseline(self) -> None:
        if not self.baseline_path.exists():
            return
        try:
            row = json.loads(self.baseline_path.read_text(encoding="utf-8"))
        except Exception:
            return
        equity = parse_decimal(row.get("baseline_equity_usd"))
        ts = TradeTracker.parse_ts(row.get("baseline_ts"))
        if equity is not None and ts is not None:
            self.baseline_equity = equity
            self.baseline_ts = ts

    def persist_baseline(self) -> None:
        if self.baseline_equity is None or self.baseline_ts is None:
            return
        write_json_atomic(
            self.baseline_path,
            {
                "market": self.args.market,
                "baseline_equity_usd": dec_to_json(self.baseline_equity),
                "baseline_ts": iso(self.baseline_ts),
            },
        )

    def record(self, status: dict[str, Any] | None, active_bot: str | None) -> dict[str, Any] | None:
        if not status:
            return None
        accounts = status.get("accounts") or {}
        total_equity = parse_decimal(accounts.get("total_equity_usd"))
        if total_equity is None:
            return None
        now = utc_now()
        if self.baseline_equity is None:
            self.baseline_equity = total_equity
            self.baseline_ts = now
            self.persist_baseline()
        self.last_equity = total_equity
        row = {
            "timestamp": iso(now),
            "market": self.args.market,
            "active_bot": active_bot,
            "source_bot": status.get("bot") or active_bot,
            "total_equity_usd": total_equity,
            "baseline_equity_usd": self.baseline_equity,
            "equity_pnl_usdc": total_equity - self.baseline_equity,
            "aster_equity_usd": accounts.get("aster_equity_usd"),
            "lighter_equity_usd": accounts.get("lighter_equity_usd"),
            "total_available_usd": accounts.get("total_available_usd"),
        }
        self.last_sample = row
        append_jsonl(self.samples_path, row)
        return row

    def summary(self) -> dict[str, Any]:
        pnl = None
        annualized_return = None
        pnl_per_hour = None
        if self.baseline_equity is not None and self.last_equity is not None:
            pnl = self.last_equity - self.baseline_equity
            elapsed = Decimal(max(1, int((utc_now() - (self.baseline_ts or utc_now())).total_seconds())))
            pnl_per_hour = pnl * Decimal(3600) / elapsed
            if self.baseline_equity > 0:
                annualized_return = (pnl / self.baseline_equity) * (SECONDS_PER_YEAR / elapsed)
        return {
            "since": iso(self.since),
            "baseline_ts": iso(self.baseline_ts) if self.baseline_ts else None,
            "baseline_equity_usd": self.baseline_equity,
            "last_equity_usd": self.last_equity,
            "equity_pnl_usdc": pnl,
            "pnl_usdc_per_hour": pnl_per_hour,
            "annualized_return": annualized_return,
        }

    def breaker_reason(self, trade_summary: dict[str, Any]) -> str | None:
        max_loss = self.args.max_loss_usdc
        if max_loss <= 0:
            return None
        if self.baseline_equity is not None and self.last_equity is not None:
            pnl = self.last_equity - self.baseline_equity
            if pnl <= -max_loss:
                return f"equity_drawdown {pnl} <= -{max_loss}"
        realized = dec_or_zero(trade_summary.get("net_pnl_usdc"))
        if realized <= -max_loss:
            return f"realized_trade_pnl {realized} <= -{max_loss}"
        return None


class Orchestrator:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.start_time = utc_now()
        self.since = parse_since(args.pnl_since, self.start_time)
        self.lock = LockFile(args.state_dir / f"orchestrator_{args.market}.lock")
        self.active_bot: str | None = None
        self.active_taker_mode: str | None = None
        self.children: dict[str, BotProcess] = {}
        # Children that survived the SIGKILL wait (D-state): kept here so their eventual
        # exit is still reaped by the tick sweep instead of leaving a zombie for the
        # orchestrator's lifetime.
        self.zombies: list[BotProcess] = []
        self.switch_counts: dict[str, int] = {TAKER_BOT: 0, XEMM_BOT: 0}
        self.mode_started_at = self.start_time
        self.condition_since: dict[str, datetime] = {}
        self.observer_retry_after: datetime | None = None
        self.observer_exit_count = 0
        self.reduce_lease_id: str | None = None
        self.reduce_lease_started_at: datetime | None = None
        self.reduce_lease_expires_at: datetime | None = None
        self.reduce_lease_max_expires_at: datetime | None = None
        self.last_reduce_signal_at: datetime | None = None
        self.status_failures = 0
        self.status_backoff_until: dict[str, float] = {TAKER_BOT: 0.0, XEMM_BOT: 0.0}
        self.shutdown_requested = False
        self.breaker_starvation_warned = False
        self.last_tape_prune: datetime | None = None
        self.events_path = args.state_dir / f"orchestrator_events_{args.market}.jsonl"
        self.state_path = args.state_dir / f"orchestrator_state_{args.market}.json"
        self.stats_path = args.state_dir / f"orchestrator_stats_{args.market}.json"
        self.breaker_path = args.state_dir / f"orchestrator_breaker_{args.market}.json"
        self.trades = TradeTracker(args, self.since, self.event)
        self.pnl = PnlTracker(args, self.since)

    def event(self, kind: str, **details: Any) -> None:
        row = {"timestamp": iso(), "kind": kind, **details}
        append_jsonl(self.events_path, row)
        try:
            print(f"{row['timestamp']} {kind} {json.dumps(details, default=json_default, separators=(',', ':'))}", flush=True)
        except OSError:
            # A dead console pipe (e.g. the tee died) must never abort the caller —
            # especially the shutdown path, where an uncaught BrokenPipeError here
            # would exit before stop_observer/stop_active and orphan live bots
            # (observed 2026-07-02). The jsonl event above is the durable record.
            pass

    def run(self) -> None:
        self.lock.acquire()
        self.install_signals()
        try:
            self.event(
                "orchestrator_started",
                market=self.args.market,
                live=self.args.live,
                pnl_since=iso(self.since),
                state_dir=str(self.args.state_dir),
            )
            if self.breaker_path.exists():
                if not self.args.ack_breaker:
                    self.event("breaker_present_abort", path=str(self.breaker_path))
                    raise SystemExit(
                        f"orchestrator breaker is active: {self.breaker_path}; inspect it and rerun with --ack-breaker to archive it"
                    )
                archive = self.breaker_path.with_name(f"{self.breaker_path.name}.acked.{stamp()}")
                self.breaker_path.replace(archive)
                self.event("breaker_acknowledged", path=str(self.breaker_path), archived=str(archive))
            self.preflight_existing_bots()
            if self.shutdown_requested:
                return
            self.revoke_reduce_lease("startup")
            self.trades.prime_sources()
            next_status_poll = 0.0
            while not self.shutdown_requested:
                self.fast_tick()
                now_mono = time.monotonic()
                if now_mono >= next_status_poll:
                    self.tick()
                    if self.args.once:
                        break
                    next_status_poll = time.monotonic() + self.args.poll_sec
                time.sleep(max(0.05, self.args.fast_signal_poll_ms / 1000))
        finally:
            self.event("orchestrator_stopping", active_bot=self.active_bot)
            self.stop_observer("orchestrator_exit")
            self.stop_active("orchestrator_exit")
            self.lock.release()

    def install_signals(self) -> None:
        def handler(signum, _frame):
            self.event("signal_received", signal=signum)
            self.shutdown_requested = True

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def preflight_existing_bots(self) -> None:
        if self.args.allow_existing_writers:
            self.event("preflight_existing_bots_skipped", reason="allow_existing_writers")
            return
        processes = self.external_bot_processes()
        if not processes:
            self.event("preflight_existing_bots_clear")
            return
        should_kill = self.args.live or self.args.preflight_kill_existing
        if not should_kill:
            self.event("preflight_existing_bots_detected", action="observe_only", processes=processes)
            return
        self.event("preflight_existing_bots_terminating", processes=processes)
        results = [self.terminate_external_process(process) for process in processes]
        survivors = self.external_bot_processes()
        self.event("preflight_existing_bots_terminated", results=results, survivors=survivors)
        if survivors:
            self.safe_halt("preflight_existing_bots_survived", survivors=survivors)

    def fast_tick(self) -> None:
        if not self.args.live or self.shutdown_requested:
            return
        self.check_child_exits()
        if self.shutdown_requested:
            return
        signal_row = self.read_reduce_signal()
        if signal_row and self.active_bot == TAKER_BOT and self.active_taker_mode == "reduce":
            self.extend_reduce_lease("fresh_reduce_signal", signal_row)
        if signal_row and self.active_bot == XEMM_BOT:
            self.activate_reduce_arb(signal_row)

    def read_reduce_signal(self) -> dict[str, Any] | None:
        path = self.args.arb_signal_file
        if not path.exists():
            return None
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            self.event("reduce_signal_read_failed", path=str(path), error=str(exc))
            return None
        if not isinstance(row, dict) or row.get("status") != "confirmed":
            return None
        if row.get("market") != self.args.market:
            return None
        if int(row.get("samples") or 0) < self.args.reduce_burst_min_samples:
            return None
        ts = TradeTracker.parse_ts(row.get("timestamp"))
        if ts is None:
            return None
        age_ms = int((utc_now() - ts).total_seconds() * 1000)
        if age_ms < 0 or age_ms > self.args.reduce_signal_fresh_ms:
            return None
        row["age_ms"] = age_ms
        return row

    def activate_reduce_arb(self, signal_row: dict[str, Any]) -> None:
        self.event("reduce_burst_switch_start", signal=signal_row)
        self.stop_active("reduce_burst_signal", grace_sec=self.args.fast_xemm_stop_sec)
        ok, status = self.verify_xemm_orders_clear()
        if not ok:
            self.safe_halt("xemm_orders_not_clear_for_reduce_arb", xemm_status=status, signal=signal_row)
            return
        self.grant_reduce_lease("reduce_burst_signal", signal_row)
        child = self.children.pop(TAKER_OBSERVER, None)
        if child and child.is_running():
            self.children[TAKER_BOT] = child
            self.active_bot = TAKER_BOT
            self.active_taker_mode = "reduce"
            self.mode_started_at = utc_now()
            self.switch_counts[TAKER_BOT] = self.switch_counts.get(TAKER_BOT, 0) + 1
            self.event("observer_promoted_to_reduce_arb", pid=child.pid, lease_id=self.reduce_lease_id)
            return
        self.event("reduce_standby_missing_starting_cold", signal=signal_row)
        self.start_bot(TAKER_BOT, taker_mode="reduce")
        self.active_bot = TAKER_BOT
        self.active_taker_mode = "reduce"
        self.mode_started_at = utc_now()
        self.switch_counts[TAKER_BOT] = self.switch_counts.get(TAKER_BOT, 0) + 1

    def verify_xemm_orders_clear(self) -> tuple[bool, dict[str, Any] | None]:
        deadline = time.monotonic() + max(0.5, self.args.fast_xemm_stop_sec)
        last_status: dict[str, Any] | None = None
        while time.monotonic() <= deadline:
            last_status = self.read_status(XEMM_BOT)
            if self.xemm_orders_clear(last_status):
                return True, last_status
            time.sleep(0.25)
        return False, last_status

    @staticmethod
    def xemm_orders_clear(status: dict[str, Any] | None) -> bool:
        if not status:
            return False
        accounts = status.get("accounts") or {}
        try:
            aster_open = int(accounts.get("aster_open_orders") or 0)
            lighter_open = int(accounts.get("lighter_open_orders") or 0)
        except (TypeError, ValueError):
            return False
        return aster_open == 0 and lighter_open == 0

    def grant_reduce_lease(self, reason: str, signal_row: dict[str, Any] | None = None) -> None:
        now = utc_now()
        if self.reduce_lease_started_at is None or self.reduce_lease_max_expires_at is None:
            self.reduce_lease_started_at = now
            self.reduce_lease_max_expires_at = now + timedelta(seconds=self.args.reduce_lease_max_sec)
            self.reduce_lease_id = f"reduce-{self.args.market}-{stamp(now)}"
        desired_expiry = now + timedelta(seconds=self.args.reduce_lease_sec)
        self.reduce_lease_expires_at = min(desired_expiry, self.reduce_lease_max_expires_at)
        body = {
            "market": self.args.market,
            "mode": "reduce_only",
            "lease_id": self.reduce_lease_id,
            "issued_at": iso(now),
            "started_at": iso(self.reduce_lease_started_at),
            "expires_at": iso(self.reduce_lease_expires_at),
            "max_expires_at": iso(self.reduce_lease_max_expires_at),
            "reason": reason,
            "signal": signal_row,
        }
        write_json_atomic(self.args.arb_control_file, body)
        self.event(
            "reduce_lease_granted",
            reason=reason,
            lease_id=self.reduce_lease_id,
            expires_at=iso(self.reduce_lease_expires_at),
            max_expires_at=iso(self.reduce_lease_max_expires_at),
        )

    def extend_reduce_lease(self, reason: str, signal_row: dict[str, Any] | None = None) -> None:
        old_expiry = self.reduce_lease_expires_at
        now = utc_now()
        if self.reduce_lease_started_at is None or self.reduce_lease_max_expires_at is None:
            self.grant_reduce_lease(reason, signal_row)
            return
        desired_expiry = min(
            now + timedelta(seconds=self.args.reduce_lease_sec),
            self.reduce_lease_max_expires_at,
        )
        if old_expiry and desired_expiry <= old_expiry + timedelta(seconds=1):
            return
        self.reduce_lease_expires_at = desired_expiry
        body = {
            "market": self.args.market,
            "mode": "reduce_only",
            "lease_id": self.reduce_lease_id,
            "issued_at": iso(now),
            "started_at": iso(self.reduce_lease_started_at),
            "expires_at": iso(self.reduce_lease_expires_at),
            "max_expires_at": iso(self.reduce_lease_max_expires_at),
            "reason": reason,
            "signal": signal_row,
        }
        write_json_atomic(self.args.arb_control_file, body)
        self.event("reduce_lease_extended", reason=reason, expires_at=iso(self.reduce_lease_expires_at))

    def reduce_lease_active(self) -> bool:
        return self.reduce_lease_expires_at is not None and utc_now() < self.reduce_lease_expires_at

    def revoke_reduce_lease(self, reason: str) -> None:
        existed = self.args.arb_control_file.exists()
        self.args.arb_control_file.unlink(missing_ok=True)
        if existed or self.reduce_lease_id:
            self.event("reduce_lease_revoked", reason=reason, lease_id=self.reduce_lease_id)
        self.reduce_lease_id = None
        self.reduce_lease_started_at = None
        self.reduce_lease_expires_at = None
        self.reduce_lease_max_expires_at = None

    def tick(self) -> None:
        self.check_child_exits()
        self.prune_tapes_if_due()
        if self.shutdown_requested:
            return
        if self.args.live and not self.args.allow_existing_writers:
            writers = self.external_writers()
            if writers:
                self.safe_halt("external_live_writer_detected", writers=writers)
                return

        taker_status, xemm_status = self.poll_statuses()
        if not self.has_required_status(taker_status, xemm_status):
            self.status_failures += 1
            self.event("status_poll_failed", consecutive_failures=self.status_failures)
            if self.status_failures >= self.args.max_status_failures:
                self.safe_halt("status_unavailable")
            return
        self.status_failures = 0

        new_trades = self.trades.poll()
        if self.active_bot == TAKER_BOT and self.active_taker_mode == "reduce":
            if any(row.get("bot") == TAKER_BOT for row in new_trades):
                self.extend_reduce_lease("reduce_trade")
        stats = self.trades.summary()
        pnl_source = xemm_status or taker_status
        pnl_sample = self.pnl.record(pnl_source, self.active_bot)
        breaker = self.pnl.breaker_reason(stats)
        if breaker:
            self.safe_halt("pnl_breaker", breaker_reason=breaker, pnl_sample=pnl_sample)
            return
        if (
            not self.breaker_starvation_warned
            and stats.get("trades", 0) == 0
            and (utc_now() - self.start_time).total_seconds() > 60
        ):
            self.event("breaker_data_starvation", active_bot=self.active_bot, uptime_sec=int((utc_now() - self.start_time).total_seconds()))
            self.breaker_starvation_warned = True
        elif stats.get("trades", 0) > 0:
            self.breaker_starvation_warned = False

        decision = self.decide(taker_status, xemm_status)
        if decision["target"] == "SAFE_HALT":
            self.safe_halt(decision["reason"], **decision.get("details", {}))
            return
        self.ensure_bot(
            decision["target"],
            decision["reason"],
            decision.get("details", {}),
            decision.get("taker_mode", "normal"),
        )
        self.ensure_observer_for(decision["target"])
        self.write_state(taker_status, xemm_status, decision, stats)

    def prune_tapes_if_due(self) -> None:
        """Hourly sweep deleting research tapes past --tape-retention-days."""
        if self.args.tape_retention_days <= 0:
            return
        now = utc_now()
        if self.last_tape_prune and (now - self.last_tape_prune).total_seconds() < 3600:
            return
        self.last_tape_prune = now
        pruned = prune_old_tapes(
            self.args.state_dir, self.args.market, self.args.tape_retention_days, now
        )
        if pruned:
            self.event(
                "tapes_pruned",
                count=len(pruned),
                reclaimed_mb=round(sum(size for _, size in pruned) / 1048576, 1),
                retention_days=self.args.tape_retention_days,
                files=[p.name for p, _ in pruned],
            )

    def poll_statuses(self) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        taker_status: dict[str, Any] | None = None
        xemm_status: dict[str, Any] | None = None
        if self.active_bot == TAKER_BOT:
            taker_status = self.read_status(TAKER_BOT)
            if taker_status is None:
                xemm_status = self.read_status(XEMM_BOT, inactive=True)
        elif self.active_bot == XEMM_BOT:
            xemm_status = self.read_status(XEMM_BOT)
            taker_status = self.read_status(TAKER_BOT, inactive=True)
        else:
            taker_status = self.read_status(TAKER_BOT)
            if taker_status is None:
                xemm_status = self.read_status(XEMM_BOT, inactive=True)
        return taker_status, xemm_status

    def has_required_status(self, taker_status: dict[str, Any] | None, xemm_status: dict[str, Any] | None) -> bool:
        if self.active_bot == TAKER_BOT:
            return taker_status is not None
        if self.active_bot == XEMM_BOT:
            return xemm_status is not None
        return taker_status is not None or xemm_status is not None

    def read_status(self, bot: str, inactive: bool = False) -> dict[str, Any] | None:
        if inactive:
            backoff_until = self.status_backoff_until.get(bot, 0.0)
            if time.monotonic() < backoff_until:
                return None
        if bot == TAKER_BOT:
            cmd = [
                str(self.args.taker_bin),
                "--config",
                str(self.args.taker_config),
                "status",
                "--market",
                self.args.market,
                "--json",
            ]
            cwd = self.args.taker_repo
        else:
            cmd = [
                str(self.args.xemm_bin),
                "--config",
                str(self.args.xemm_config),
                "status",
                "--market",
                self.args.market,
                "--json",
            ]
            cwd = self.args.xemm_repo
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(cwd),
                text=True,
                capture_output=True,
                timeout=self.args.status_timeout_sec,
                check=False,
            )
            if proc.returncode != 0:
                self.event(
                    "status_command_failed",
                    bot=bot,
                    inactive=inactive,
                    exit_code=proc.returncode,
                    command=cmd,
                    stdout_tail=text_tail(proc.stdout),
                    stderr_tail=text_tail(proc.stderr),
                )
                self.apply_status_backoff(bot, inactive)
                return None
            status = extract_json_object(proc.stdout)
            status.setdefault("bot", bot)
            self.status_backoff_until[bot] = 0.0
            return status
        except subprocess.TimeoutExpired as exc:
            self.event(
                "status_command_failed",
                bot=bot,
                inactive=inactive,
                error="timeout",
                command=cmd,
                stdout_tail=text_tail(exc.stdout),
                stderr_tail=text_tail(exc.stderr),
            )
            self.apply_status_backoff(bot, inactive)
            return None
        except Exception as exc:
            self.event(
                "status_command_failed",
                bot=bot,
                inactive=inactive,
                error=str(exc),
                command=cmd,
            )
            self.apply_status_backoff(bot, inactive)
            return None

    def apply_status_backoff(self, bot: str, inactive: bool) -> None:
        if not inactive:
            return
        delay = max(0, int(self.args.inactive_status_backoff_sec))
        if delay == 0:
            return
        self.status_backoff_until[bot] = time.monotonic() + delay
        self.event(
            "inactive_status_backoff",
            bot=bot,
            retry_after=iso(utc_now() + timedelta(seconds=delay)),
            delay_sec=delay,
        )

    def decide(self, taker: dict[str, Any] | None, xemm: dict[str, Any] | None) -> dict[str, Any]:
        if xemm and xemm.get("reduce_position_only") is not True:
            return {
                "target": "SAFE_HALT",
                "reason": "xemm_reduce_position_only_disabled",
                "details": {"reduce_position_only": xemm.get("reduce_position_only")},
            }
        status = taker or xemm
        if not status:
            return {"target": "SAFE_HALT", "reason": "no_status", "details": {}}
        if self.active_bot == TAKER_BOT:
            if self.active_taker_mode == "reduce":
                details = {
                    **self.position_details(status),
                    "near_flat": self.near_flat(status),
                    "reduce_lease_id": self.reduce_lease_id,
                    "reduce_lease_expires_at": iso(self.reduce_lease_expires_at) if self.reduce_lease_expires_at else None,
                    "reduce_lease_max_expires_at": iso(self.reduce_lease_max_expires_at) if self.reduce_lease_max_expires_at else None,
                }
                if details["near_flat"]:
                    return {
                        "target": TAKER_BOT,
                        "reason": "reduce_lease_near_flat_resume_normal_taker",
                        "taker_mode": "normal",
                        "details": details,
                    }
                if not self.reduce_lease_active():
                    return {
                        "target": XEMM_BOT,
                        "reason": "reduce_lease_expired_resume_xemm",
                        "details": details,
                    }
                return {
                    "target": TAKER_BOT,
                    "reason": "reduce_lease_active",
                    "taker_mode": "reduce",
                    "details": details,
                }
            blocked, details = self.taker_blocked(taker)
            if self.sustained("taker_margin_limited", blocked, self.args.blocked_confirm_sec):
                return {"target": XEMM_BOT, "reason": "taker_margin_limited", "details": details}
            return {"target": TAKER_BOT, "reason": "taker_active", "taker_mode": "normal", "details": details}
        if self.active_bot == XEMM_BOT:
            ready, details = self.ready_for_taker(taker or xemm, taker)
            if self.sustained("ready_for_taker", ready, self.args.resume_confirm_sec):
                return {"target": TAKER_BOT, "reason": "resume_taker", "details": details}
            return {"target": XEMM_BOT, "reason": "xemm_reduce_only_active", "details": details}
        ready, details = self.ready_for_taker(taker or status, taker)
        if ready:
            return {"target": TAKER_BOT, "reason": "bootstrap_taker_preferred", "taker_mode": "normal", "details": details}
        return {"target": XEMM_BOT, "reason": "bootstrap_margin_limited_reduce_existing_position", "details": details}

    def taker_blocked(self, status: dict[str, Any] | None) -> tuple[bool, dict[str, Any]]:
        margin_limited, details = self.taker_margin_state(status)
        executable_reduce = bool(details.get("taker_executable_reduce"))
        blocked = margin_limited and not executable_reduce
        details["taker_blocked"] = blocked
        return blocked, details

    def taker_margin_state(self, status: dict[str, Any] | None) -> tuple[bool, dict[str, Any]]:
        if not status:
            return False, {"status": "missing", "taker_margin_limited": False}
        required = dec_or_zero(status.get("required_gross_edge_bps"))
        opportunities = status.get("opportunities") or []
        profitable = [
            o for o in opportunities
            if parse_decimal(o.get("gross_edge_bps")) is not None and dec_or_zero(o.get("gross_edge_bps")) >= required
        ]
        executable_reduce = [
            o for o in opportunities
            if o.get("limiting_reason") == "ok" and o.get("exposure_effect") == "reduce"
        ]
        executable_any = [o for o in opportunities if o.get("limiting_reason") == "ok"]
        profitable_increase = [o for o in profitable if o.get("exposure_effect") == "increase"]
        margin_binding = [
            o for o in profitable
            if o.get("limiting_reason") in {"headroom", "margin"}
        ]
        clip = self.clip(status)
        headroom = parse_decimal((status.get("positions") or {}).get("headroom_notional_usd"))
        accounts = status.get("accounts") or {}
        margin_buffer = dec_or_zero(status.get("margin_buffer_usd"))
        aster_available = parse_decimal(accounts.get("aster_available_usd"))
        lighter_available = parse_decimal(accounts.get("lighter_available_usd"))
        min_available = min(aster_available, lighter_available) if aster_available is not None and lighter_available is not None else None
        low_headroom = headroom is not None and headroom < clip * self.args.switch_headroom_clips
        low_margin = min_available is not None and (min_available - margin_buffer) < clip * self.args.switch_margin_clips
        margin_limited = bool(margin_binding) or low_headroom or low_margin
        return margin_limited, {
            "profitable": len(profitable),
            "profitable_increase": len(profitable_increase),
            "executable_any": len(executable_any),
            "executable_reduce": len(executable_reduce),
            "taker_executable_reduce": bool(executable_reduce),
            "margin_binding_reasons": [o.get("limiting_reason") for o in margin_binding],
            "headroom_notional_usd": headroom,
            "min_available_usd": min_available,
            "low_headroom": low_headroom,
            "low_margin": low_margin,
            "taker_margin_limited": margin_limited,
        }

    def ready_for_taker(self, position_status: dict[str, Any], taker_status: dict[str, Any] | None) -> tuple[bool, dict[str, Any]]:
        near_flat = self.near_flat(position_status)
        margin_limited = False
        margin_details: dict[str, Any] = {"status": "missing", "taker_margin_limited": False}
        if taker_status:
            margin_limited, margin_details = self.taker_margin_state(taker_status)
        executable_reduce = bool(margin_details.get("taker_executable_reduce"))
        ready = near_flat or executable_reduce or (taker_status is not None and not margin_limited)
        if near_flat:
            ready_reason = "near_flat"
        elif executable_reduce:
            ready_reason = "taker_executable_reduce"
        elif taker_status is not None and not margin_limited:
            ready_reason = "taker_margin_available"
        else:
            ready_reason = "taker_margin_limited"
        return ready, {
            **self.position_details(position_status),
            "near_flat": near_flat,
            "taker_executable_reduce": executable_reduce,
            "taker_margin_limited": margin_limited,
            "ready_for_taker": ready,
            "ready_reason": ready_reason,
            **margin_details,
        }

    def sustained(self, key: str, condition: bool, seconds: int) -> bool:
        if not condition:
            self.condition_since.pop(key, None)
            return False
        first = self.condition_since.setdefault(key, utc_now())
        return (utc_now() - first).total_seconds() >= seconds

    def near_flat(self, status: dict[str, Any]) -> bool:
        abs_position = parse_decimal((status.get("positions") or {}).get("abs_position_notional_usd"))
        if abs_position is None:
            return False
        threshold = self.args.near_flat_notional_usd
        if threshold <= 0:
            threshold = self.clip(status)
        return abs_position <= threshold

    def clip(self, status: dict[str, Any] | None) -> Decimal:
        if not status:
            return Decimal("13")
        return (
            parse_decimal(status.get("desired_notional_usd"))
            or parse_decimal(status.get("quote", {}).get("desired_notional"))
            or Decimal("13")
        )

    def position_details(self, status: dict[str, Any]) -> dict[str, Any]:
        positions = status.get("positions") or {}
        return {
            "aster_qty": positions.get("aster_qty"),
            "lighter_qty": positions.get("lighter_qty"),
            "abs_position_notional_usd": positions.get("abs_position_notional_usd"),
            "headroom_notional_usd": positions.get("headroom_notional_usd"),
        }

    def ensure_bot(self, target: str, reason: str, details: dict[str, Any], taker_mode: str = "normal") -> None:
        desired_taker_mode = taker_mode if target == TAKER_BOT else None
        if (
            self.active_bot == target
            and (not self.args.live or self.child_running(target))
            and (target != TAKER_BOT or self.active_taker_mode == desired_taker_mode)
        ):
            return
        if self.args.live:
            if target == TAKER_BOT:
                self.stop_observer(f"switch_to_{target}")
            self.stop_active(f"switch_to_{target}")
            self.start_bot(target, taker_mode=desired_taker_mode or "normal")
        else:
            self.event("dry_run_switch_decision", target=target, reason=reason, details=details)
        self.active_bot = target
        self.active_taker_mode = desired_taker_mode
        self.mode_started_at = utc_now()
        self.switch_counts[target] = self.switch_counts.get(target, 0) + 1

    def start_bot(self, bot: str, taker_mode: str = "normal") -> None:
        child = self.make_child(bot, taker_mode=taker_mode)
        child.start()
        self.children[bot] = child
        self.event("bot_started", bot=bot, taker_mode=taker_mode if bot == TAKER_BOT else None, pid=child.pid, log=str(child.log_path), command=child.command)

    def ensure_observer_for(self, active_target: str) -> None:
        if not self.args.taker_observer:
            self.stop_observer("observer_disabled")
            return
        if active_target != XEMM_BOT:
            self.stop_observer(f"active_{active_target}")
            return
        if not self.args.live:
            return
        if self.child_running(TAKER_OBSERVER):
            return
        if self.observer_retry_after and utc_now() < self.observer_retry_after:
            return
        child = self.make_taker_observer_child()
        child.start()
        self.children[TAKER_OBSERVER] = child
        self.event("observer_started", bot=TAKER_BOT, pid=child.pid, log=str(child.log_path), command=child.command)

    def stop_observer(self, reason: str) -> None:
        child = self.children.get(TAKER_OBSERVER)
        if child and child.is_running():
            result = child.stop(self.args.stop_grace_sec)
            self.event("observer_stopped", bot=TAKER_BOT, reason=reason, **result)
            self.track_if_unkilled(child, result)
        self.children.pop(TAKER_OBSERVER, None)

    def stop_active(self, reason: str, grace_sec: int | None = None) -> None:
        if not self.active_bot:
            return
        if self.active_bot == TAKER_BOT and self.active_taker_mode == "reduce":
            self.revoke_reduce_lease(reason)
        child = self.children.get(self.active_bot)
        if child and child.is_running():
            result = child.stop(grace_sec if grace_sec is not None else self.args.stop_grace_sec)
            self.event("bot_stopped", bot=self.active_bot, reason=reason, **result)
            self.track_if_unkilled(child, result)
        if self.active_bot:
            self.children.pop(self.active_bot, None)
        self.active_bot = None
        self.active_taker_mode = None

    def track_if_unkilled(self, child: BotProcess, stop_result: dict[str, Any]) -> None:
        if stop_result.get("alive_after_sigkill"):
            self.zombies.append(child)

    def reap_zombies(self) -> None:
        for child in list(self.zombies):
            code = child.poll_exit()
            if code is not None:
                self.event("zombie_reaped", bot=child.name, exit_code=code)
                self.zombies.remove(child)

    def child_running(self, bot: str) -> bool:
        child = self.children.get(bot)
        return bool(child and child.is_running())

    def check_child_exits(self) -> None:
        self.reap_zombies()
        for bot, child in list(self.children.items()):
            code = child.poll_exit()
            if code is not None:
                self.event("bot_exited", bot=bot, exit_code=code)
                self.children.pop(bot, None)
                was_active = self.active_bot == bot
                was_reduce_taker = bot == TAKER_BOT and self.active_taker_mode == "reduce"
                if was_active:
                    if was_reduce_taker:
                        self.revoke_reduce_lease("active_reduce_taker_exited")
                    self.active_bot = None
                    self.active_taker_mode = None
                    if code != 0:
                        self.safe_halt(
                            "active_bot_exited_nonzero",
                            bot=bot,
                            exit_code=code,
                            reduce_mode=was_reduce_taker,
                            log=str(child.log_path),
                        )
                        return
                if bot == TAKER_OBSERVER:
                    self.observer_exit_count += 1
                    retry_sec = min(
                        300,
                        self.args.taker_observer_restart_sec
                        * (2 ** min(self.observer_exit_count - 1, 3)),
                    )
                    self.observer_retry_after = utc_now() + timedelta(seconds=retry_sec)
                    self.event(
                        "observer_restart_delayed",
                        exit_code=code,
                        retry_sec=retry_sec,
                        consecutive_exits=self.observer_exit_count,
                    )

    def make_child(self, bot: str, taker_mode: str = "normal") -> BotProcess:
        log = self.args.state_dir / f"orchestrator_{bot.lower()}_{self.args.market}_{stamp()}.log"
        if bot == TAKER_BOT:
            if taker_mode == "reduce":
                cmd = self.reduce_taker_command()
            else:
                cmd = [
                    str(self.args.taker_bin),
                    "--config",
                    str(self.args.taker_config),
                    "run",
                    "--markets",
                    self.args.market,
                    *self.args.taker_arg,
                ]
            return BotProcess(bot, self.args.taker_repo, cmd, log)
        out = self.args.state_dir / f"orchestrator_xemm_{self.args.market}_{stamp()}.jsonl.zst"
        cmd = [
            str(self.args.xemm_bin),
            "--config",
            str(self.args.xemm_config),
            "livebot",
            "--mode",
            "live",
            "--markets",
            self.args.market,
            "--out",
            str(out),
            "--db",
            str(self.args.xemm_db),
            *self.args.xemm_arg,
        ]
        return BotProcess(bot, self.args.xemm_repo, cmd, log)

    def make_taker_observer_child(self) -> BotProcess:
        log = self.args.state_dir / f"orchestrator_taker_observer_{self.args.market}_{stamp()}.log"
        cmd = self.reduce_taker_command()
        return BotProcess(TAKER_OBSERVER, self.args.taker_repo, cmd, log)

    def reduce_taker_command(self) -> list[str]:
        cmd = [
            str(self.args.taker_bin),
            "--config",
            str(self.args.taker_config),
            "run",
            "--markets",
            self.args.market,
            "--exposure-filter",
            "reduce",
            "--control-file",
            str(self.args.arb_control_file),
            "--signal-file",
            str(self.args.arb_signal_file),
            "--reduce-cooldown-ms",
            str(self.args.reduce_cooldown_ms),
            "--reduce-signal-min-samples",
            str(self.args.reduce_burst_min_samples),
            "--reduce-signal-window-ms",
            str(self.args.reduce_burst_window_ms),
            *self.args.taker_observer_arg,
        ]
        return cmd

    def external_bot_processes(self) -> list[dict[str, Any]]:
        own_pids = {child.pid for child in self.children.values() if child.pid}
        try:
            proc = subprocess.run(["ps", "-eo", "pid=,pgid=,stat=,args="], text=True, capture_output=True, check=True, timeout=5)
        except Exception as exc:
            self.event("ps_failed", error=str(exc))
            return []
        processes = []
        for line in proc.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split(None, 3)
            if len(parts) < 4:
                continue
            pid_s, pgid_s, stat, args = parts
            try:
                pid = int(pid_s)
                pgid = int(pgid_s)
            except ValueError:
                continue
            if pid == os.getpid() or pid in own_pids or stat.startswith("Z"):
                continue
            argv = args.split()
            if not argv:
                continue
            executable = Path(argv[0]).name
            subcommand_args = f" {' '.join(argv[1:])} "
            if executable == "lighter_aster_taker_arb" and " run " in subcommand_args:
                if not process_matches_market(argv, self.args.market):
                    continue
                processes.append({"pid": pid, "pgid": pgid, "bot": TAKER_BOT, "args": args})
            elif executable == "xemm_lighter_aster" and " livebot " in subcommand_args:
                if not process_matches_market(argv, self.args.market):
                    continue
                processes.append({"pid": pid, "pgid": pgid, "bot": XEMM_BOT, "args": args})
        return processes

    def external_writers(self) -> list[dict[str, Any]]:
        writers = []
        for process in self.external_bot_processes():
            args = process["args"]
            if process["bot"] == TAKER_BOT and "--observe-only" not in args:
                writers.append(process)
            elif process["bot"] == XEMM_BOT and not (
                "--mode paper" in args or "--mode=paper" in args
            ):
                # The XEMM livebot falls back to the config's [live] mode when --mode is
                # absent, and the live config ships mode="live" — so only an explicit
                # paper flag proves a process is not a live writer.
                writers.append(process)
        return writers

    def terminate_external_process(self, process: dict[str, Any]) -> dict[str, Any]:
        pid = int(process["pid"])
        pgid = int(process.get("pgid") or pid)
        result: dict[str, Any] = {
            "pid": pid,
            "pgid": pgid,
            "bot": process.get("bot"),
            "signals": [],
            "alive": True,
        }
        use_group = pgid > 1 and pgid != os.getpgrp()
        signal_plan = [
            ("SIGINT", signal.SIGINT, self.args.stop_grace_sec),
            ("SIGTERM", signal.SIGTERM, max(3, self.args.stop_grace_sec // 2)),
            ("SIGKILL", signal.SIGKILL, 5),
        ]
        for name, sig, wait_sec in signal_plan:
            if not process_alive(pid):
                result["alive"] = False
                return result
            try:
                if use_group:
                    os.killpg(pgid, sig)
                    target = "process_group"
                else:
                    os.kill(pid, sig)
                    target = "process"
            except ProcessLookupError:
                result["alive"] = False
                return result
            result["signals"].append({"signal": name, "target": target})
            if wait_process_exit(pid, wait_sec):
                result["alive"] = False
                return result
        result["alive"] = process_alive(pid)
        return result

    def safe_halt(self, reason: str, **details: Any) -> None:
        self.event("safe_halt", reason=reason, details=details)
        self.stop_observer("safe_halt")
        self.stop_active("safe_halt")
        state = {
            "active": True,
            "triggered_at": iso(),
            "market": self.args.market,
            "reason": reason,
            "details": details,
            "pnl": self.pnl.summary(),
            "trades": self.trades.summary(),
        }
        write_json_atomic(self.breaker_path, state)
        self.write_state(None, None, {"target": "SAFE_HALT", "reason": reason, "details": details}, self.trades.summary())
        self.shutdown_requested = True

    def write_state(
        self,
        taker_status: dict[str, Any] | None,
        xemm_status: dict[str, Any] | None,
        decision: dict[str, Any],
        trade_stats: dict[str, Any],
    ) -> None:
        mode_age = int((utc_now() - self.mode_started_at).total_seconds())
        stats = {
            "timestamp": iso(),
            "market": self.args.market,
            "live": self.args.live,
            "active_bot": self.active_bot,
            "active_taker_mode": self.active_taker_mode,
            "observer": {
                "enabled": self.args.taker_observer,
                "running": self.child_running(TAKER_OBSERVER),
                "pid": self.children.get(TAKER_OBSERVER).pid if self.children.get(TAKER_OBSERVER) else None,
                "retry_after": iso(self.observer_retry_after) if self.observer_retry_after else None,
            },
            "reduce_lease": {
                "id": self.reduce_lease_id,
                "active": self.reduce_lease_active(),
                "started_at": iso(self.reduce_lease_started_at) if self.reduce_lease_started_at else None,
                "expires_at": iso(self.reduce_lease_expires_at) if self.reduce_lease_expires_at else None,
                "max_expires_at": iso(self.reduce_lease_max_expires_at) if self.reduce_lease_max_expires_at else None,
                "control_file": str(self.args.arb_control_file),
                "signal_file": str(self.args.arb_signal_file),
            },
            "mode_age_sec": mode_age,
            "switch_counts": self.switch_counts,
            "decision": decision,
            "pnl": self.pnl.summary(),
            "trades": trade_stats,
            "positions": {
                "taker": (taker_status or {}).get("positions"),
                "xemm": (xemm_status or {}).get("positions"),
            },
            "accounts": {
                "taker": (taker_status or {}).get("accounts"),
                "xemm": (xemm_status or {}).get("accounts"),
            },
        }
        write_json_atomic(self.state_path, stats)
        write_json_atomic(self.stats_path, stats)


def inferred_xemm_journal(db: Path) -> Path:
    stem = db.stem or "livebot"
    return db.parent / f"{stem}-journal.jsonl"


def process_matches_market(argv: list[str], market: str) -> bool:
    for i, arg in enumerate(argv):
        if arg == "--markets" and i + 1 < len(argv):
            return argv[i + 1].upper() == market.upper()
        if arg.startswith("--markets="):
            return arg.split("=", 1)[1].upper() == market.upper()
    return True


def positive_decimal(raw: str) -> Decimal:
    value = Decimal(raw)
    if value < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return value


def strictly_positive_decimal(raw: str) -> Decimal:
    value = Decimal(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return value


def existing_file_or_path(raw: str) -> Path:
    return Path(raw).expanduser().resolve()


def parse_args() -> argparse.Namespace:
    stack_root = Path(__file__).resolve().parent
    taker_root = stack_root / "LIGHTER_ASTER_TAKER_ARB"
    xemm_root = stack_root / "XEMM_LIGHTER_ASTER"
    parser = argparse.ArgumentParser(description="Supervise taker-arb and XEMM reduce-only bots.")
    parser.add_argument("--market", default="HYPE")
    parser.add_argument("--live", action="store_true", help="Actually start/stop child trading bots. Default only observes and writes decisions.")
    parser.add_argument("--allow-existing-writers", action="store_true", help="Do not halt if an unmanaged live writer is detected.")
    parser.add_argument("--preflight-kill-existing", action="store_true", help="Terminate existing taker run / XEMM livebot processes before polling status. Implied by --live.")
    parser.add_argument("--no-taker-observer", dest="taker_observer", action="store_false", help="Do not run taker arb in reduce-standby mode while XEMM is active.")
    parser.set_defaults(taker_observer=True)
    parser.add_argument("--taker-observer-restart-sec", type=int, default=60)
    parser.add_argument(
        "--tape-retention-days",
        type=float,
        default=7.0,
        help="Delete finished XEMM research tapes (--out *.jsonl.zst) older than this many days; 0 keeps everything.",
    )
    parser.add_argument("--poll-sec", type=int, default=15)
    parser.add_argument("--once", action="store_true", help="Run one status/decision cycle and exit.")
    parser.add_argument("--status-timeout-sec", type=int, default=25)
    parser.add_argument("--max-status-failures", type=int, default=3)
    parser.add_argument("--inactive-status-backoff-sec", type=int, default=60)
    parser.add_argument("--blocked-confirm-sec", type=int, default=90)
    parser.add_argument("--resume-confirm-sec", type=int, default=45)
    parser.add_argument("--stop-grace-sec", type=int, default=20)
    parser.add_argument("--fast-signal-poll-ms", type=int, default=250)
    parser.add_argument(
        "--reduce-signal-fresh-ms",
        type=int,
        default=60000,
        help="How long a confirmed reduce burst can arm the reduce-only taker mode. This is intentionally longer than the burst window so status polling cannot make the supervisor miss activation.",
    )
    parser.add_argument("--reduce-burst-min-samples", type=int, default=3)
    parser.add_argument("--reduce-burst-window-ms", type=int, default=2000)
    parser.add_argument("--reduce-lease-sec", type=int, default=180)
    parser.add_argument("--reduce-lease-max-sec", type=int, default=300)
    parser.add_argument("--fast-xemm-stop-sec", type=int, default=5)
    parser.add_argument("--reduce-cooldown-ms", type=int, default=5000)
    parser.add_argument("--near-flat-notional-usd", type=positive_decimal, default=Decimal("0"), help="0 means use the status desired-notional clip.")
    parser.add_argument("--switch-headroom-clips", type=positive_decimal, default=Decimal("2"))
    parser.add_argument("--switch-margin-clips", type=positive_decimal, default=Decimal("2"))
    parser.add_argument("--pnl-since", default="startup", help='RFC3339 timestamp, "startup", or "now".')
    parser.add_argument(
        "--max-loss-usdc",
        type=strictly_positive_decimal,
        default=Decimal("15"),
        help="Supervisor-level realized-loss backstop. Deliberately ABOVE the bot-level "
        "max_loss_usdc (10 in the live configs) so the bot breaker trips first and this "
        "remains a genuine second line of defense.",
    )
    parser.add_argument("--reset-breaker-baseline", action="store_true", help="Delete the persisted PnL baseline on startup so the equity-drawdown breaker re-anchors to current equity.")
    parser.add_argument("--ack-breaker", action="store_true", help="Archive an existing orchestrator breaker file after operator review and allow startup.")
    parser.add_argument("--backfill-existing-trades", action="store_true")
    parser.add_argument("--state-dir", type=existing_file_or_path, default=stack_root / "runs")
    parser.add_argument("--taker-repo", type=existing_file_or_path, default=taker_root)
    parser.add_argument("--xemm-repo", type=existing_file_or_path, default=xemm_root)
    parser.add_argument("--taker-bin", type=existing_file_or_path, default=taker_root / "target/release/lighter_aster_taker_arb")
    parser.add_argument("--xemm-bin", type=existing_file_or_path, default=xemm_root / "target/release/xemm_lighter_aster")
    parser.add_argument("--taker-config", type=existing_file_or_path, default=taker_root / "configs/live-hype.toml")
    parser.add_argument("--xemm-config", type=existing_file_or_path, default=xemm_root / "config-live-lighter.toml")
    parser.add_argument("--taker-trades", type=existing_file_or_path, default=None)
    parser.add_argument("--xemm-db", type=existing_file_or_path, default=None)
    parser.add_argument("--xemm-journal", type=existing_file_or_path, default=None)
    parser.add_argument("--arb-control-file", type=existing_file_or_path, default=None)
    parser.add_argument("--arb-signal-file", type=existing_file_or_path, default=None)
    parser.add_argument("--taker-arg", action="append", default=[], help="Extra arg appended to the taker run command. Repeat for multiple args.")
    parser.add_argument("--taker-observer-arg", action="append", default=[], help="Extra arg appended to the taker reduce-standby command. Repeat for multiple args.")
    parser.add_argument("--xemm-arg", action="append", default=[], help="Extra arg appended to the XEMM livebot command. Repeat for multiple args.")
    args = parser.parse_args()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    if args.reset_breaker_baseline:
        baseline_path = args.state_dir / f"orchestrator_baseline_{args.market}.json"
        baseline_path.unlink(missing_ok=True)
    if args.taker_trades is None:
        args.taker_trades = (taker_root / f"runs/trades_{args.market}.jsonl").resolve()
    if args.xemm_db is None:
        args.xemm_db = (args.state_dir / f"orchestrator-xemm-{args.market}.sqlite").resolve()
    if args.arb_control_file is None:
        args.arb_control_file = (args.state_dir / f"arb_reduce_control_{args.market}.json").resolve()
    if args.arb_signal_file is None:
        args.arb_signal_file = (args.state_dir / f"arb_reduce_signal_{args.market}.json").resolve()
    return args


def insecure_env_files(args: argparse.Namespace) -> list[str]:
    """Credential env files that are readable by group/other (mode should be 600)."""
    insecure = []
    for repo in (args.taker_repo, args.xemm_repo):
        for name in ("aster.env", "lighter.env"):
            path = Path(repo) / name
            if not path.exists():
                continue
            mode = path.stat().st_mode & 0o777
            if mode & 0o077:
                insecure.append(f"{path} (mode {mode:03o})")
    return insecure


def main() -> int:
    args = parse_args()
    missing = [p for p in [args.taker_bin, args.xemm_bin, args.taker_config, args.xemm_config] if not p.exists()]
    if missing:
        print("missing required path(s): " + ", ".join(str(p) for p in missing), file=sys.stderr)
        return 2
    if args.live:
        insecure = insecure_env_files(args)
        if insecure:
            print(
                "refusing --live: credential env file(s) readable by group/other "
                "(chmod 600 them first): " + ", ".join(insecure),
                file=sys.stderr,
            )
            return 2
    orch = Orchestrator(args)
    orch.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
