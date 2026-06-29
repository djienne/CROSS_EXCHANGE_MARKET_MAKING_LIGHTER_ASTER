#!/usr/bin/env python
"""Read-only / low-risk Aster V3 probe + signing ORACLE for the Rust port.

Reference implementation of the Aster V3 "Pro" signing scheme (ABI-encode + EIP-191
personal_sign) used by the Rust `AsterSigner`. Doubles as:
  1. a live connectivity check (balance / open-orders / place+cancel), and
  2. the golden-vector oracle: `python aster_probe.py golden` prints the exact json_str,
     keccak digest, and signature for FIXED test inputs so the Rust unit test asserts
     byte-equality.

Credentials are read from ../aster.env (never hardcoded, never committed). ROLE MAPPING is
derived from the key, NOT the field names (the env field labels were re-edited and no longer
match roles): signer = address of private_key; user = whichever address field != signer.

Usage:
  python scripts/aster_probe.py balance
  python scripts/aster_probe.py open-orders [SYMBOL]
  python scripts/aster_probe.py place-cancel [SYMBOL]   # no-risk: post-only ~1.8% below bid, then cancel
  python scripts/aster_probe.py golden                  # deterministic oracle vector (no network)
"""
import json
import sys
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path

from eth_account import Account
from eth_account.messages import encode_defunct
from eth_abi import encode as abi_encode
from eth_utils import keccak, to_checksum_address

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

BASE = "https://fapi.asterdex.com"
ENV_PATH = Path(__file__).resolve().parent.parent / "aster.env"
HEADERS = {"Content-Type": "application/x-www-form-urlencoded", "User-Agent": "xemm-probe"}


def load_env(path: Path) -> dict:
    out = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def resolve_roles(env: dict):
    """signer = address of private_key; user = the address field that isn't the signer."""
    priv = env["private_key"]
    signer = Account.from_key(priv).address
    candidates = [env.get("wallet_address", ""), env.get("subaccount_address", "")]
    user = next((c for c in candidates if c and c.lower() != signer.lower()), signer)
    return user, signer, priv


def trim(params: dict) -> dict:
    trimmed = {}
    for k, v in params.items():
        if isinstance(v, bool):
            trimmed[k] = "true" if v else "false"
        elif isinstance(v, (dict, list)):
            trimmed[k] = json.dumps(v, separators=(",", ":"))
        else:
            trimmed[k] = str(v)
    return trimmed


def sign_v3(params: dict, user: str, signer: str, priv: str, nonce=None, timestamp=None):
    """Return (request_dict, json_str, digest_hex, signature_hex)."""
    if nonce is None:
        nonce = int(time.time() * 1e6)
    p = dict(params)
    p.setdefault("recvWindow", 50000)
    if timestamp is None:
        timestamp = int(time.time() * 1000)
    p.setdefault("timestamp", timestamp)
    trimmed = trim(p)
    json_str = json.dumps(trimmed, sort_keys=True, separators=(",", ":"))
    encoded = abi_encode(
        ["string", "address", "address", "uint256"],
        [json_str, to_checksum_address(user), to_checksum_address(signer), nonce],
    )
    digest = keccak(encoded)
    signed = Account.sign_message(encode_defunct(digest), private_key=priv)
    signature = signed.signature.hex()
    if not signature.startswith("0x"):
        signature = "0x" + signature
    req = dict(trimmed)
    req["nonce"] = nonce
    req["user"] = user
    req["signer"] = signer
    req["signature"] = signature
    return req, json_str, "0x" + digest.hex(), signature


def golden():
    priv = "0x0000000000000000000000000000000000000000000000000000000000000001"
    signer = Account.from_key(priv).address  # 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf
    user = "0x062903894bce55d4f80ee5931c46c77cd7881351"
    nonce = 1700000000000000
    params = {"symbol": "HYPEUSDT", "side": "BUY", "type": "LIMIT", "timeInForce": "GTX",
              "quantity": "0.3", "price": "40.0"}
    req, json_str, digest_hex, sig = sign_v3(params, user, signer, priv, nonce=nonce,
                                             timestamp=1700000000000)
    print("=== Aster V3 signing golden vector ===")
    print("signer_address:", signer)
    print("user_address  :", user)
    print("nonce         :", nonce)
    print("json_str      :", json_str)
    print("digest        :", digest_hex)
    print("signature     :", sig)


def public_get(path, **params):
    r = requests.get(BASE + path, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def fmt(d: Decimal) -> str:
    s = format(d.normalize(), "f")
    return s


def signed_get(path, params, roles):
    user, signer, priv = roles
    req, *_ = sign_v3(params, user, signer, priv)
    return requests.get(BASE + path, params=req, headers=HEADERS, timeout=10)


def signed_body(method, path, params, roles):
    user, signer, priv = roles
    req, *_ = sign_v3(params, user, signer, priv)
    return requests.request(method, BASE + path, data=req, headers=HEADERS, timeout=10)


def place_cancel(symbol, roles):
    info = public_get("/fapi/v3/exchangeInfo")
    sym = next(s for s in info["symbols"] if s["symbol"] == symbol)
    filt = {f["filterType"]: f for f in sym["filters"]}
    tick = Decimal(filt["PRICE_FILTER"]["tickSize"])
    step = Decimal(filt["LOT_SIZE"]["stepSize"])
    min_qty = Decimal(filt["LOT_SIZE"]["minQty"])
    min_notional = Decimal(filt.get("MIN_NOTIONAL", {}).get("notional", "5"))
    bt = public_get("/fapi/v1/ticker/bookTicker", symbol=symbol)
    bid = Decimal(str(bt["bidPrice"]))
    px = ((bid * Decimal("0.982")) / tick).to_integral_value(ROUND_DOWN) * tick  # 1.8% below bid (within +/-2% band)
    qty = (max(min_qty, (min_notional * Decimal("1.05")) / px) / step).to_integral_value(ROUND_UP) * step
    # Detect position mode: hedge mode wants LONG/SHORT, one-way wants BOTH.
    dual = signed_get("/fapi/v3/positionSide/dual", {}, roles).json()
    hedge_mode = bool(dual.get("dualSidePosition"))
    pos_side = "LONG" if hedge_mode else "BOTH"
    print(f"position mode: {'HEDGE (dual)' if hedge_mode else 'ONE-WAY'} -> positionSide={pos_side}")
    cid = "Xprobe-" + symbol[:6] + "-" + str(int(time.time()))[-7:]
    print(f"placing post-only BUY {fmt(qty)} {symbol} @ {fmt(px)} (bid={fmt(bid)}, notional=${float(px*qty):.2f}, cid={cid})")
    params = {"symbol": symbol, "side": "BUY", "type": "LIMIT", "timeInForce": "GTX",
              "quantity": fmt(qty), "price": fmt(px), "newClientOrderId": cid, "positionSide": pos_side}
    t0 = time.time()
    rp = signed_body("POST", "/fapi/v3/order", params, roles)
    place_ms = (time.time() - t0) * 1000
    print(f"PLACE  HTTP {rp.status_code} ({place_ms:.0f}ms): {rp.text[:300]}")
    body = rp.json() if rp.headers.get("content-type", "").startswith("application/json") else {}
    order_id = body.get("orderId")
    cparams = {"symbol": symbol}
    if order_id:
        cparams["orderId"] = order_id
    else:
        cparams["origClientOrderId"] = cid
    t1 = time.time()
    rc = signed_body("DELETE", "/fapi/v3/order", cparams, roles)
    cancel_ms = (time.time() - t1) * 1000
    print(f"CANCEL HTTP {rc.status_code} ({cancel_ms:.0f}ms): {rc.text[:300]}")
    oo = signed_get("/fapi/v3/openOrders", {"symbol": symbol}, roles)
    remaining = [o.get("clientOrderId") for o in oo.json()] if oo.status_code == 200 else oo.text
    print(f"OPEN ORDERS after cancel: {remaining}")
    print(f"--> RTT place={place_ms:.0f}ms cancel={cancel_ms:.0f}ms; clean={'YES' if cid not in (remaining or []) else 'NO'}")


def live(check: str, argv):
    if requests is None:
        print("requests not installed; cannot do live probe", file=sys.stderr)
        sys.exit(2)
    env = load_env(ENV_PATH)
    roles = resolve_roles(env)
    user, signer, priv = roles
    print(f"user={user} signer={signer}")
    if check == "balance":
        r = signed_get("/fapi/v3/balance", {}, roles)
        if r.status_code == 200:
            for row in r.json():
                if float(row.get("balance", 0)) != 0:
                    print(f"  {row['asset']}: balance={row['balance']} crossWallet={row.get('crossWalletBalance')}")
        else:
            print("HTTP", r.status_code, r.text[:300])
    elif check == "open-orders":
        params = {"symbol": argv[0]} if argv else {}
        r = signed_get("/fapi/v3/openOrders", params, roles)
        print("HTTP", r.status_code)
        print(json.dumps(r.json(), indent=2)[:3000])
    elif check == "place-cancel":
        place_cancel(argv[0] if argv else "HYPEUSDT", roles)
    else:
        print(f"unknown check: {check}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "golden":
        golden()
    else:
        live(cmd, sys.argv[2:])
