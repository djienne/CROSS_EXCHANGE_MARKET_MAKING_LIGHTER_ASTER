"""Record orchestrator.py decide() outcomes as a fixture for the Rust port.

Provenance of tests/fixtures/regime_decisions.json.zst (600 scenarios, 5054 steps, checked by
`controller::regime::tests::python_decisions_match`): this script run against orchestrator.py
and economics.py as of commit 7d47337, the last before c53fa6a retired the orchestrator. The
"about" text inside the fixture calls it the "scratch" script: it was written outside the repo
and committed unchanged except for this docstring. Rerunning it reproduces the fixture's JSON
byte for byte (checked 2026-09-25). In an empty directory, on Linux or macOS (orchestrator.py
imports fcntl; python:3.12-slim works):
    git -C <repo> show 7d47337:orchestrator.py > orchestrator.py
    git -C <repo> show 7d47337:economics.py > economics.py
    python3 <repo>/tests/fixtures/gen_regime_fixture.py OUT.json && zstd -19 OUT.json

Drives the REAL
Orchestrator.decide/taker_margin_state/ready_for_taker/sustained/near_flat/clip on a bare
instance with fake monotonic/UTC clocks. Each scenario is a sequence of steps; a step sets
the explicit state (active bot, taker mode, reduce lease), optionally clears the confirm
windows (what a mode switch does), advances the clocks and calls decide once.
"""
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

sys.path.insert(0, ".")
import orchestrator as orch  # noqa: E402

SEED = 20260923
rng = random.Random(SEED)
BASE_UTC = datetime(2026, 9, 23, 12, 0, 0, tzinfo=timezone.utc)


class Clock:
    mono = 1000.0
    utc = BASE_UTC


clock = Clock()
orch.time.monotonic = lambda: clock.mono
orch.utc_now = lambda: clock.utc

# ---------------------------------------------------------------- status pool
REASONS = ["ok", "headroom", "margin", "depth", "stale_book", "min_qty", "edge", None, 7]
EFFECTS = ["reduce", "increase", "flat", "unknown", None]


def dec_text(value):
    return format(Decimal(value).normalize(), "f")


def maybe(value, p_missing=0.08, p_null=0.05, p_garbage=0.03):
    """Return a JSON value or the sentinel "__missing__"."""
    r = rng.random()
    if r < p_missing:
        return "__missing__"
    if r < p_missing + p_null:
        return None
    if r < p_missing + p_null + p_garbage:
        return rng.choice(["abc", "", "NaN", "Infinity", True, [1], {"a": 1}])
    return value


def put(obj, key, value):
    if value != "__missing__":
        obj[key] = value


def number(value):
    """Mostly canonical decimal strings, sometimes JSON numbers (int or float)."""
    r = rng.random()
    if r < 0.8:
        return dec_text(value)
    if r < 0.9:
        return float(value)
    return int(Decimal(value)) if Decimal(value) == Decimal(value).to_integral_value() else float(value)


def rand_money(lo, hi, step="0.25"):
    s = Decimal(step)
    return Decimal(rng.randint(int(Decimal(lo) / s), int(Decimal(hi) / s))) * s


def opportunity(required):
    o = {}
    edge_choice = rng.random()
    if edge_choice < 0.25:
        edge = required  # boundary: profitable at exactly the required edge
    elif edge_choice < 0.3:
        edge = required - Decimal("0.0001")
    else:
        edge = rand_money("-5", "15", "0.5")
    put(o, "gross_edge_bps", maybe(number(edge)))
    put(o, "limiting_reason", maybe(rng.choice(REASONS), p_garbage=0))
    put(o, "exposure_effect", maybe(rng.choice(EFFECTS), p_garbage=0))
    return o


def taker_status(clip_hint):
    s = {}
    required = rng.choice([Decimal("6"), Decimal("8.5"), Decimal("0")])
    put(s, "required_gross_edge_bps", maybe(number(required)))
    n_opps = rng.choice([0, 1, 2, 2, 2, 3])
    put(s, "opportunities", maybe([opportunity(required) for _ in range(n_opps)], p_garbage=0))
    clip = clip_hint
    put(s, "desired_notional_usd", maybe(number(clip), p_garbage=0.02))
    positions = {}
    abs_pos = rng.choice([Decimal(0), clip, clip - Decimal("0.25"), clip + Decimal("0.25"), rand_money("0", "220")])
    put(positions, "abs_position_notional_usd", maybe(number(abs_pos)))
    headroom_mult = rng.choice([Decimal(2), Decimal(3), None])
    headroom = clip * headroom_mult if headroom_mult is not None and rng.random() < 0.3 else rand_money("0", "200")
    put(positions, "headroom_notional_usd", maybe(number(headroom)))
    put(positions, "aster_qty", number(rand_money("-9", "9", "0.01")))
    put(positions, "lighter_qty", number(rand_money("-9", "9", "0.01")))
    put(s, "positions", maybe(positions, p_garbage=0))
    buffer = rng.choice([Decimal(25), Decimal(26), Decimal(0)])
    put(s, "margin_buffer_usd", maybe(number(buffer)))
    accounts = {}
    for venue in ("aster_available_usd", "lighter_available_usd"):
        if rng.random() < 0.25:
            clips = rng.choice([Decimal(2), Decimal(3)])
            value = buffer + clip * clips  # boundary: exactly at the margin threshold
        else:
            value = rand_money("0", "220")
        put(accounts, venue, maybe(number(value)))
    put(s, "accounts", maybe(accounts, p_garbage=0))
    return s


def xemm_status(clip_hint):
    s = {}
    r = rng.random()
    if r < 0.9:
        s["reduce_position_only"] = True
    elif r < 0.94:
        s["reduce_position_only"] = False
    elif r < 0.97:
        s["reduce_position_only"] = "true"
    # else missing
    put(s, "desired_notional_usd", maybe(number(clip_hint), p_garbage=0.02))
    positions = {}
    abs_pos = rng.choice([Decimal(0), clip_hint, clip_hint + Decimal("0.25"), rand_money("0", "220")])
    put(positions, "abs_position_notional_usd", maybe(number(abs_pos)))
    put(positions, "headroom_notional_usd", maybe(number(rand_money("0", "200"))))
    put(s, "positions", maybe(positions, p_garbage=0))
    if rng.random() < 0.1:
        s["quote"] = {"desired_notional": number(rand_money("5", "30"))}
    return s


CLIPS = [Decimal(13), Decimal(13), Decimal(13), Decimal(0), Decimal("12.5")]
TAKER_POOL = [{}, {"desired_notional_usd": "13"}] + [taker_status(rng.choice(CLIPS)) for _ in range(400)]
XEMM_POOL = [{}, {"reduce_position_only": True}] + [xemm_status(rng.choice(CLIPS)) for _ in range(120)]

# ---------------------------------------------------------------- scenarios
THRESHOLD_SETS = [
    dict(blocked=90, resume=45, sh="2", sm="2", rh="3", rm="3", nf="0"),
    dict(blocked=90, resume=45, sh="2", sm="2", rh="3", rm="3", nf="0"),
    dict(blocked=30, resume=15, sh="1.5", sm="2.5", rh="4", rm="3.5", nf="20"),
    dict(blocked=0, resume=0, sh="2", sm="2", rh="3", rm="3", nf="0"),
]


def make_orchestrator(th):
    o = object.__new__(orch.Orchestrator)
    o.args = SimpleNamespace(
        blocked_confirm_sec=th["blocked"], resume_confirm_sec=th["resume"],
        switch_headroom_clips=Decimal(th["sh"]), switch_margin_clips=Decimal(th["sm"]),
        resume_headroom_clips=Decimal(th["rh"]), resume_margin_clips=Decimal(th["rm"]),
        near_flat_notional_usd=Decimal(th["nf"]),
    )
    o.condition_since = {}
    o.active_bot = None
    o.active_taker_mode = None
    o.reduce_lease_id = None
    o.reduce_lease_started_at = None
    o.reduce_lease_expires_at = None
    o.reduce_lease_max_expires_at = None
    return o


def to_json(value):
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, dict):
        return {k: to_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json(v) for v in value]
    return value


DETAIL_KEYS = ["near_flat", "ready_for_taker", "ready_reason", "taker_blocked", "taker_margin_limited",
               "low_headroom", "low_margin", "profitable", "profitable_increase", "executable_any",
               "executable_reduce", "taker_executable_reduce", "margin_binding_reasons",
               "headroom_notional_usd", "min_available_usd", "status", "reduce_position_only"]

STATES = [
    ("none", None),
    ("taker", "normal"),
    ("taker", "reduce"),
    ("xemm", None),
]


def pick_index(pool_len, none_p):
    return None if rng.random() < none_p else rng.randrange(pool_len)


def run_scenario(kind):
    th = rng.choice(THRESHOLD_SETS)
    o = make_orchestrator(th)
    clock.mono = 1000.0
    clock.utc = BASE_UTC
    steps = []
    state = rng.choice(STATES)
    lease_in = rng.choice([None, 0.0, 30.0, 180.0])
    sticky_t = pick_index(len(TAKER_POOL), 0.1)
    sticky_x = pick_index(len(XEMM_POOL), 0.3)
    n = rng.randint(3, 14)
    for i in range(n):
        clear = False
        if kind == "random" and rng.random() < 0.25:
            state = rng.choice(STATES)
            clear = rng.random() < 0.8
            lease_in = rng.choice([None, 0.0, 0.25, 30.0, 180.0, -1.0])
        dt = rng.choice([0.0, 0.25, 1.0, 5.0, 15.0, 15.0, 15.0, 30.0, 44.75, 45.0, 89.75, 90.0])
        if i == 0:
            dt = 0.0
        clock.mono += dt
        clock.utc += timedelta(seconds=dt)
        if kind == "sustain" and rng.random() < 0.8:
            t_idx, x_idx = sticky_t, sticky_x  # the same condition held across ticks
        else:
            t_idx, x_idx = pick_index(len(TAKER_POOL), 0.1), pick_index(len(XEMM_POOL), 0.3)
        if clear:
            o.condition_since.clear()
        o.active_bot = {"none": None, "taker": orch.TAKER_BOT, "xemm": orch.XEMM_BOT}[state[0]]
        o.active_taker_mode = state[1]
        if lease_in is None:
            o.reduce_lease_id = o.reduce_lease_expires_at = o.reduce_lease_max_expires_at = None
        else:
            # A lease keeps its absolute expiry across steps until the state changes.
            if o.reduce_lease_id is None or clear:
                o.reduce_lease_id = f"lease-{rng.randrange(1_000_000)}"
                o.reduce_lease_started_at = clock.utc
                o.reduce_lease_expires_at = clock.utc + timedelta(seconds=lease_in)
                o.reduce_lease_max_expires_at = clock.utc + timedelta(seconds=300)
        taker = TAKER_POOL[t_idx] if t_idx is not None else None
        xemm = XEMM_POOL[x_idx] if x_idx is not None else None
        decision = o.decide(taker, xemm)
        details = decision.get("details") or {}
        expected = {
            "target": decision["target"],
            "reason": decision["reason"],
            "taker_mode": decision.get("taker_mode"),
            "details": {k: to_json(details[k]) for k in DETAIL_KEYS if k in details},
        }
        lease = None
        if o.reduce_lease_expires_at is not None:
            lease = {
                "id": o.reduce_lease_id,
                "expires_at_offset": (o.reduce_lease_expires_at - BASE_UTC).total_seconds(),
                "max_expires_at_offset": (o.reduce_lease_max_expires_at - BASE_UTC).total_seconds(),
            }
        steps.append({
            "t": clock.mono - 1000.0,
            "active": state[0], "taker_mode": state[1], "clear": clear, "lease": lease,
            "taker": t_idx, "xemm": x_idx, "expected": expected,
        })
    return {"thresholds": th, "steps": steps}


scenarios = [run_scenario(rng.choice(["random", "sustain", "sustain"])) for _ in range(600)]
fixture = {
    "about": "orchestrator.py decide() outcomes; regenerate with scratch gen_regime_fixture.py (seed below)",
    "seed": SEED,
    "base_utc": BASE_UTC.isoformat().replace("+00:00", "Z"),
    "taker_pool": TAKER_POOL,
    "xemm_pool": XEMM_POOL,
    "scenarios": scenarios,
}
with open(sys.argv[1], "w", encoding="utf-8") as fh:
    json.dump(fixture, fh, separators=(",", ":"), sort_keys=True)
n_steps = sum(len(s["steps"]) for s in scenarios)
from collections import Counter  # noqa: E402
reasons = Counter(step["expected"]["reason"] for s in scenarios for step in s["steps"])
print(f"scenarios={len(scenarios)} steps={n_steps}")
for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
    print(f"  {count:5d} {reason}")
