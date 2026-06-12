#!/usr/bin/env bash
# LIVE trader dashboard — reads the live-mode ledger written by the
# standalone executor at /data/bsc-meme-mev/trader_live/live_log.csv.
#
# Phase A (shadow): every row = a signed-but-not-broadcast tx. Status
# columns show "would-have" values useful for verifying the signing
# pipeline + limits behaviour before flipping broadcast=true.
#
# Phase B/C: same columns, broadcast=true rows reflect on-chain sends.
#
#   scripts/live-trader-report.sh              # refresh 15s
#   scripts/live-trader-report.sh 30           # refresh 30s
#   scripts/live-trader-report.sh 15 --all     # all-time scope
set -uo pipefail
exec python3 - "$@" <<'PY'
import csv, datetime, os, sys, time
from collections import defaultdict
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

ARGS = sys.argv[1:]
INTERVAL = next((int(a) for a in ARGS if a.isdigit()), 15)
ALL = "--all" in ARGS
LEDGER = "/data/bsc-meme-mev/trader_live/live_log.csv"

# Local TZ for on-screen times. Override with WATCH_TZ env var
# (e.g. WATCH_TZ=Asia/Singapore  /  WATCH_TZ=Asia/Tokyo  /  WATCH_TZ=UTC).
# Default UTC.
_TZ_NAME = os.environ.get("WATCH_TZ", "UTC")
TZ = ZoneInfo(_TZ_NAME) if (ZoneInfo and _TZ_NAME != "UTC") else datetime.timezone.utc
TZ_LABEL = _TZ_NAME if _TZ_NAME != "UTC" else "Z"


def today_utc():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")


def load_rows():
    if not os.path.exists(LEDGER):
        return []
    out = []
    with open(LEDGER) as f:
        for r in csv.DictReader(f):
            try:
                ts_ns = int(r["ts_unix_ns"])
            except (KeyError, ValueError):
                continue
            r["_ts_ns"] = ts_ns
            r["_day"] = datetime.datetime.fromtimestamp(
                ts_ns / 1e9, datetime.timezone.utc
            ).strftime("%Y-%m-%d")
            out.append(r)
    if not ALL:
        d = today_utc()
        out = [r for r in out if r["_day"] == d]
    return out


def render():
    rows = load_rows()
    now = datetime.datetime.now(TZ)
    scope = "ALL-TIME" if ALL else f"TODAY {today_utc()}"
    lines = []
    lines.append(
        f"\x1b[1mLIVE TRADER REPORT\x1b[0m  {now:%H:%M:%S} {TZ_LABEL}  "
        f"scope={scope}  (refresh {INTERVAL}s, Ctrl-C quit)"
    )
    lines.append("=" * 76)
    if not rows:
        # Pull current whitelist from limits.toml so the hint reflects reality
        wl = "(unknown)"
        try:
            import re as _re
            with open("/data/bsc-meme-mev/config/limits.toml") as _f:
                for _line in _f:
                    if _line.lstrip().startswith("kol_whitelist"):
                        m = _re.search(r"\[(.+?)\]", _line)
                        if m:
                            wl = "{" + m.group(1).replace('"', '').replace(" ", "") + "}"
                        break
        except Exception:
            pass
        lines.append(f"  (no events yet — waiting for a KOL public BUY of {wl})")
        return "\n".join(lines)

    # Aggregate
    phase_counts = defaultdict(int)
    skip_reasons = defaultdict(int)
    by_kol = defaultdict(
        lambda: {"signed": 0, "broadcast": 0, "skipped": 0, "bnb_in": 0.0}
    )
    total_signed = 0
    total_broadcast = 0
    total_skipped = 0
    total_bnb_committed = 0.0
    for r in rows:
        phase_counts[r.get("phase", "?")] += 1
        skip = r.get("limit_skip_reason", "")
        kol = r.get("kol_name", "?")
        bnb_in = float(int(r.get("bnb_in_wei", "0"))) / 1e18 if r.get("bnb_in_wei", "").isdigit() else 0.0
        if skip:
            total_skipped += 1
            by_kol[kol]["skipped"] += 1
            skip_reasons[skip] += 1
        else:
            total_signed += 1
            by_kol[kol]["signed"] += 1
            by_kol[kol]["bnb_in"] += bnb_in
            total_bnb_committed += bnb_in
            if r.get("broadcast", "").lower() == "true":
                total_broadcast += 1
                by_kol[kol]["broadcast"] += 1

    # phases summary
    lines.append(
        f"  phases:     "
        + "  ".join(f"{p}={n}" for p, n in sorted(phase_counts.items()))
    )
    lines.append(
        f"  signed:     {total_signed:>4}    broadcast: {total_broadcast:>4}    skipped: {total_skipped:>4}"
    )
    lines.append(f"  BNB committed (signed×size): {total_bnb_committed:.5f}")
    lines.append("")
    lines.append(f"  {'KOL':4}  {'signed':>6}  {'broadcast':>9}  {'skipped':>7}  {'bnb_in':>10}")
    lines.append("  " + "-" * 50)
    for kol, a in sorted(by_kol.items(), key=lambda kv: -kv[1]["signed"]):
        lines.append(
            f"  {kol:4}  {a['signed']:>6}  {a['broadcast']:>9}  {a['skipped']:>7}  {a['bnb_in']:>10.5f}"
        )

    if skip_reasons:
        lines.append("")
        lines.append("  skip reasons:")
        for r, n in sorted(skip_reasons.items(), key=lambda kv: -kv[1]):
            lines.append(f"    {n:>4}  {r}")

    # Last 10 events tape
    lines.append("")
    lines.append(f"  \x1b[1mlast 10 events:\x1b[0m")
    for r in rows[-10:]:
        ts = datetime.datetime.fromtimestamp(
            r["_ts_ns"] / 1e9, TZ
        ).strftime("%H:%M:%S")
        skip = r.get("limit_skip_reason", "")
        if skip:
            tag = f"\x1b[33mSKIP({skip})\x1b[0m"
        elif r.get("broadcast", "").lower() == "true":
            tag = "\x1b[32mBROADCAST\x1b[0m"
        else:
            tag = "\x1b[36mSHADOW\x1b[0m"
        tx = r.get("tx_hash", "")[:14]
        bnb_in = (
            float(int(r["bnb_in_wei"])) / 1e18
            if r.get("bnb_in_wei", "").isdigit()
            else 0.0
        )
        lines.append(
            f"  {ts}Z  {tag:<24}  kol={r.get('kol_name','?'):3}  "
            f"bnb={bnb_in:.4f}  tx={tx}…  phase={r.get('phase','?')}"
        )
    return "\n".join(lines)


try:
    while True:
        sys.stdout.write("\x1b[2J\x1b[H")
        sys.stdout.write(render() + "\n")
        sys.stdout.flush()
        time.sleep(INTERVAL)
except KeyboardInterrupt:
    pass
PY
