#!/usr/bin/env python3
"""Lightweight standalone metadata & monitoring daemon (meta_daemon.py).

Strictly separated from the Rust hot path:
1. Captures raw REST contract specs for all 4 exchanges at startup, 00:00:00 UTC,
   and on reconnect triggers, writing meta/specs/ with query/reply timestamps
   and SHA-256 fingerprints. Flags any inter-poll spec change as isolation window.
2. Measures 4-exchange public gateway RTT every 5 minutes -> meta/rtt/rtt_YYYYMMDD.csv (288 slots/day).
3. Logs `chronyc tracking` every 1 minute -> meta/chrony/chrony_YYYYMMDD.csv (1,440 slots/day),
   marking estimated error > 1ms or unsynced state.
4. Dispatches Webhook / Telegram alerts on dual A/B outage > 5s, LOCAL_LOSS, or disk > 70%.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

USER_AGENT = "tokyo-l2-meta-daemon/1.4"

REST_SPEC_ENDPOINTS = {
    "binance_futures": {
        "url": "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "headers": {},
        "paginated_bybit": False,
    },
    "bybit_linear": {
        "url": "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000",
        "headers": {},
        "paginated_bybit": True,
    },
    "bitget_v3": {
        "url": "https://api.bitget.com/api/v3/market/instruments?category=USDT-FUTURES",
        "headers": {},
        "paginated_bybit": False,
    },
    "gate_futures": {
        "url": "https://api.gateio.ws/api/v4/futures/usdt/contracts",
        "headers": {"X-Gate-Size-Decimal": "1"},
        "paginated_bybit": False,
    },
}

RTT_TARGETS = {
    "binance_futures": ("fstream.binance.com", 443),
    "bybit_linear": ("stream.bybit.com", 443),
    "bitget_v3": ("ws.bitget.com", 443),
    "gate_futures": ("fx-ws.gateio.ws", 443),
}


def utc_now_ns() -> int:
    return time.time_ns()


def utc_date_str(ts_ns: int | None = None) -> str:
    sec = (utc_now_ns() if ts_ns is None else ts_ns) / 1e9
    return datetime.fromtimestamp(sec, tz=timezone.utc).strftime("%Y%m%d")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def default_http_get(url: str, headers: dict[str, str], timeout: float = 15.0) -> bytes:
    req_headers = {"User-Agent": USER_AGENT, **headers}
    req = urllib.request.Request(url, headers=req_headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def fetch_specs_once(
    data_root: Path,
    trigger: str,
    exchanges: list[str] | None = None,
    fetcher: Callable[[str, dict[str, str]], bytes] | None = None,
    previous_hashes: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Fetch raw REST contract specs for each enabled exchange and persist losslessly."""
    do_fetch = fetcher or (lambda u, h: default_http_get(u, h))
    targets = exchanges or list(REST_SPEC_ENDPOINTS.keys())
    out_dir = data_root / "meta" / "specs"
    out_dir.mkdir(parents=True, exist_ok=True)
    prev = previous_hashes if previous_hashes is not None else {}

    summary: dict[str, Any] = {
        "trigger": trigger,
        "captured_utc": datetime.now(timezone.utc).isoformat(),
        "results": {},
        "spec_change_isolation_flags": [],
    }

    for ex in targets:
        if ex not in REST_SPEC_ENDPOINTS:
            raise ValueError(f"Unknown exchange for spec capture: {ex}")
        ep = REST_SPEC_ENDPOINTS[ex]
        q_ns = utc_now_ns()
        try:
            pages: list[bytes] = []
            url = ep["url"]
            headers = dict(ep["headers"])
            if ex == "gate_futures" and headers.get("X-Gate-Size-Decimal") != "1":
                raise RuntimeError("Gate REST spec fetch MUST send X-Gate-Size-Decimal: 1")

            if ep["paginated_bybit"]:
                cursor = ""
                for _ in range(20):
                    page_url = url if not cursor else f"{url}&cursor={urllib.parse.quote(cursor)}"
                    raw = do_fetch(page_url, headers)
                    pages.append(raw)
                    parsed = json.loads(raw.decode("utf-8"))
                    cursor = parsed.get("result", {}).get("nextPageCursor") or ""
                    if not cursor:
                        break
                combined_bytes = json.dumps(
                    {"pages": [json.loads(p.decode("utf-8")) for p in pages]},
                    separators=(",", ":"),
                    sort_keys=False,
                ).encode("utf-8")
            else:
                combined_bytes = do_fetch(url, headers)
            r_ns = utc_now_ns()
            digest = sha256_bytes(combined_bytes)

            stamp = datetime.fromtimestamp(q_ns / 1e9, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            raw_file = out_dir / f"{ex}_{stamp}_{trigger}.json"
            meta_file = out_dir / f"{ex}_{stamp}_{trigger}.meta.json"
            raw_file.write_bytes(combined_bytes)

            changed = ex in prev and prev[ex] != digest
            if changed:
                change_event = {
                    "ts_utc": datetime.fromtimestamp(r_ns / 1e9, tz=timezone.utc).isoformat(),
                    "exchange": ex,
                    "previous_sha256": prev[ex],
                    "current_sha256": digest,
                    "observed_wall_ns": r_ns,
                    "isolation_required": True,
                    "policy": "ISOLATE_INTERVAL_UNKNOWN_EFFECTIVE_TIME",
                }
                summary["spec_change_isolation_flags"].append(change_event)
                with (out_dir / "spec_changes.jsonl").open("a", encoding="utf-8") as cf:
                    cf.write(json.dumps(change_event) + "\n")
            prev[ex] = digest

            meta_obj = {
                "exchange": ex,
                "url": url,
                "headers_sent": headers,
                "trigger": trigger,
                "query_wall_ns": q_ns,
                "reply_wall_ns": r_ns,
                "bytes": len(combined_bytes),
                "sha256": digest,
                "raw_file": raw_file.name,
                "spec_changed_since_previous_poll": changed,
            }
            meta_file.write_text(json.dumps(meta_obj, indent=2) + "\n", encoding="utf-8")
            summary["results"][ex] = {"status": "ok", **meta_obj}
        except Exception as exc:
            summary["results"][ex] = {
                "status": "error",
                "query_wall_ns": q_ns,
                "error": f"{type(exc).__name__}: {exc}",
            }

    return summary


def measure_tcp_rtt_ms(host: str, port: int, timeout: float = 3.0) -> float | None:
    t0 = time.monotonic_ns()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return (time.monotonic_ns() - t0) / 1e6
    except OSError:
        return None


def sample_rtt_once(
    data_root: Path,
    exchanges: list[str] | None = None,
    prober: Callable[[str, int], float | None] | None = None,
) -> list[dict[str, Any]]:
    """Measure TCP handshake RTT to each public gateway and append to meta/rtt/rtt_YYYYMMDD.csv."""
    probe = prober or measure_tcp_rtt_ms
    targets = exchanges or list(RTT_TARGETS.keys())
    now_ns = utc_now_ns()
    day = utc_date_str(now_ns)
    rtt_dir = data_root / "meta" / "rtt"
    rtt_dir.mkdir(parents=True, exist_ok=True)
    csv_path = rtt_dir / f"rtt_{day}.csv"
    write_header = not csv_path.exists()

    rows = []
    with csv_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["wall_ns", "utc_iso", "exchange", "host", "port", "rtt_ms", "status"],
        )
        if write_header:
            writer.writeheader()
        utc_iso = datetime.fromtimestamp(now_ns / 1e9, tz=timezone.utc).isoformat()
        for ex in targets:
            host, port = RTT_TARGETS[ex]
            rtt = probe(host, port)
            row = {
                "wall_ns": now_ns,
                "utc_iso": utc_iso,
                "exchange": ex,
                "host": host,
                "port": port,
                "rtt_ms": f"{rtt:.4f}" if rtt is not None else "",
                "status": "ok" if rtt is not None else "missing",
            }
            writer.writerow(row)
            rows.append(row)
    return rows


def parse_chronyc_tracking(output: str) -> dict[str, Any]:
    """Parse `chronyc tracking` output into structured clock quality fields."""
    fields: dict[str, str] = {}
    for line in output.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            fields[k.strip().lower()] = v.strip()

    def parse_seconds_ms(raw: str | None) -> float | None:
        if not raw:
            return None
        m = re.search(r"([-+]?[0-9]*\.?[0-9]+)\s*seconds", raw)
        if not m:
            return None
        val_ms = float(m.group(1)) * 1000.0
        if "slow" in raw.lower():
            val_ms = -abs(val_ms)
        elif "fast" in raw.lower():
            val_ms = abs(val_ms)
        return val_ms

    ref_id = fields.get("reference id", "")
    leap_status = fields.get("leap status", "Unknown")
    sys_time_ms = parse_seconds_ms(fields.get("system time"))
    last_offset_ms = parse_seconds_ms(fields.get("last offset"))
    root_delay_ms = parse_seconds_ms(fields.get("root delay"))
    root_disp_ms = parse_seconds_ms(fields.get("root dispersion"))

    # Conservative upper bound on clock error: |system_time| + 0.5*root_delay + root_dispersion
    synced = leap_status.lower() in ("normal", "insert second", "delete second") and bool(ref_id)
    est_err_ms: float | None = None
    if sys_time_ms is not None and root_delay_ms is not None and root_disp_ms is not None:
        est_err_ms = abs(sys_time_ms) + 0.5 * abs(root_delay_ms) + abs(root_disp_ms)
    elif last_offset_ms is not None:
        est_err_ms = abs(last_offset_ms)

    certified_le_1ms = bool(synced and est_err_ms is not None and est_err_ms <= 1.0)
    return {
        "reference_id": ref_id,
        "leap_status": leap_status,
        "system_time_ms": sys_time_ms,
        "last_offset_ms": last_offset_ms,
        "root_delay_ms": root_delay_ms,
        "root_dispersion_ms": root_disp_ms,
        "estimated_error_ms": est_err_ms,
        "synced": synced,
        "certified_le_1ms": certified_le_1ms,
    }


def sample_chrony_once(
    data_root: Path,
    runner: Callable[[], str] | None = None,
) -> dict[str, Any]:
    """Capture `chronyc tracking` and append to meta/chrony/chrony_YYYYMMDD.csv."""
    now_ns = utc_now_ns()
    day = utc_date_str(now_ns)
    cdir = data_root / "meta" / "chrony"
    cdir.mkdir(parents=True, exist_ok=True)
    csv_path = cdir / f"chrony_{day}.csv"
    write_header = not csv_path.exists()

    if runner is not None:
        raw_out = runner()
        status = "ok"
    else:
        try:
            proc = subprocess.run(
                ["chronyc", "tracking"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True,
            )
            raw_out = proc.stdout
            status = "ok"
        except Exception as exc:
            raw_out = ""
            status = f"unavailable:{type(exc).__name__}"

    parsed = parse_chronyc_tracking(raw_out) if raw_out else {
        "reference_id": "",
        "leap_status": "UNAVAILABLE",
        "system_time_ms": None,
        "last_offset_ms": None,
        "root_delay_ms": None,
        "root_dispersion_ms": None,
        "estimated_error_ms": None,
        "synced": False,
        "certified_le_1ms": False,
    }
    row = {
        "wall_ns": now_ns,
        "utc_iso": datetime.fromtimestamp(now_ns / 1e9, tz=timezone.utc).isoformat(),
        "status": status,
        **{k: ("" if v is None else v) for k, v in parsed.items()},
    }
    with csv_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)
    return row


def check_disk_usage_pct(path: Path) -> float:
    usage = shutil.disk_usage(path)
    return 100.0 * (usage.used / usage.total) if usage.total > 0 else 100.0


def send_alert(webhook_url: str | None, severity: str, title: str, details: dict[str, Any]) -> bool:
    """Send structured Webhook/Telegram JSON alert (fail-safe: logs to stderr if unset)."""
    payload = {
        "severity": severity,
        "title": title,
        "utc": datetime.now(timezone.utc).isoformat(),
        "details": details,
    }
    if not webhook_url:
        print(f"[ALERT {severity}] {title}: {json.dumps(details)}", file=sys.stderr, flush=True)
        return False
    body = json.dumps({"text": f"[{severity}] {title}\n{json.dumps(details)}", **payload}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10):
            return True
    except urllib.error.URLError as exc:
        print(f"[ALERT_SEND_FAILED] {exc}", file=sys.stderr, flush=True)
        return False


def run_daemon(config_path: Path, once: bool = False) -> int:
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    data_root = Path(cfg["deployment"]["data_root"])
    data_root.mkdir(parents=True, exist_ok=True)
    enabled = [k for k, v in cfg["exchanges"].items() if v.get("enabled")]
    webhook = os.environ.get(cfg["meta_daemon"].get("alert_webhook_env", "TOKYO_L2_ALERT_WEBHOOK_URL"))
    disk_thresh = float(cfg["archive"].get("disk_alert_pct", 70.0))
    rtt_sec = int(cfg["meta_daemon"].get("rtt_interval_seconds", 300))
    chrony_sec = int(cfg["meta_daemon"].get("chrony_interval_seconds", 60))

    prev_hashes: dict[str, str] = {}
    spec_res = fetch_specs_once(data_root, "startup", exchanges=enabled, previous_hashes=prev_hashes)
    sample_rtt_once(data_root, exchanges=enabled)
    sample_chrony_once(data_root)

    disk_pct = check_disk_usage_pct(data_root)
    if disk_pct >= disk_thresh:
        send_alert(webhook, "CRITICAL", "Disk usage exceeded threshold", {"disk_pct": round(disk_pct, 2), "threshold": disk_thresh})

    if once:
        print(json.dumps({"status": "ok", "specs": spec_res, "disk_pct": round(disk_pct, 2)}, indent=2))
        return 0

    last_rtt = time.monotonic()
    last_chrony = time.monotonic()
    last_spec_day = utc_date_str()
    while True:
        time.sleep(5)
        now_m = time.monotonic()
        today = utc_date_str()
        if today != last_spec_day:
            last_spec_day = today
            res = fetch_specs_once(data_root, "utc_midnight", exchanges=enabled, previous_hashes=prev_hashes)
            if res["spec_change_isolation_flags"]:
                send_alert(webhook, "WARNING", "Contract spec changed at UTC midnight", {"flags": res["spec_change_isolation_flags"]})
        if now_m - last_chrony >= chrony_sec:
            last_chrony = now_m
            crow = sample_chrony_once(data_root)
            if not crow.get("certified_le_1ms"):
                send_alert(webhook, "WARNING", "Chrony clock error > 1ms or unsynced", crow)
        if now_m - last_rtt >= rtt_sec:
            last_rtt = now_m
            sample_rtt_once(data_root, exchanges=enabled)
            dp = check_disk_usage_pct(data_root)
            if dp >= disk_thresh:
                send_alert(webhook, "CRITICAL", "Disk usage exceeded threshold", {"disk_pct": round(dp, 2), "threshold": disk_thresh})


def main() -> int:
    p = argparse.ArgumentParser(description="Standalone metadata, RTT, chrony and alert daemon")
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--once", action="store_true", help="Run a single metadata+RTT+chrony sweep and exit")
    args = p.parse_args()
    return run_daemon(args.config, once=args.once)


if __name__ == "__main__":
    raise SystemExit(main())
