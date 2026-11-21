#!/usr/bin/env python3
"""Lightweight standalone metadata, health monitoring, Healthchecks & Telegram daemon (meta_daemon.py).

Strictly separated from the Rust hot path:
1. Captures raw REST contract specs for all 4 exchanges at startup, 00:00:00 UTC,
   and on reconnect triggers, writing meta/specs/ with query/reply timestamps
   and SHA-256 fingerprints. Flags any inter-poll spec change as isolation window.
2. Measures 4-exchange public gateway RTT every 5 minutes -> meta/rtt/rtt_YYYYMMDD.csv (288 slots/day).
3. Logs `chronyc tracking` every 1 minute -> meta/chrony/chrony_YYYYMMDD.csv (1,440 slots/day),
   marking estimated error > 1ms or unsynced state.
4. Evaluates collector & archive health strictly from lightweight metadata/signals ONLY
   (systemd status, incremental collector journal cursor, `.partial` mtime/size, small `.manifest.json`
   summaries, `meta/shutdown_*.json`, and read-only `archive_ledger.sqlite`).
   NEVER opens or decompresses `.tsv.zst` / `.tsv.zst.partial` payloads on EC2!
5. Dispatches deduplicated Telegram / Webhook alerts, automatic recovery notifications,
   hourly battle reports (explicitly separating archive status vs `PENDING` deep QA quality),
   and periodic fail-closed Healthchecks heartbeats with bounded timeouts & retries.
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
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Callable

USER_AGENT = "tokyo-l2-meta-daemon/1.4"
OUTAGE_ACK_REQUEST_DIRNAME = "outage_ack_requests"

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

DEFAULT_EXCHANGE_ORDER = (
    "binance_futures",
    "bybit_linear",
    "bitget_v3",
    "gate_futures",
)

ENDPOINT_TO_EXCHANGE = {
    "binance_public": "binance_futures",
    "binance_market": "binance_futures",
    "bybit_public": "bybit_linear",
    "bitget_public": "bitget_v3",
    "gate_public": "gate_futures",
}

# Secret redaction patterns (Telegram bot tokens & Healthchecks ping UUIDs)
_RE_TG_BOT_TOKEN = re.compile(r"(bot)\d{6,}:[A-Za-z0-9_-]{8,}")
_RE_HC_PING_UUID = re.compile(
    r"(https?://[^/\s]+/)(?:ping/)?([0-9a-fA-F]{8}-[0-9a-fA-F-]{13,}|[A-Za-z0-9_-]{16,})(/(?:fail|start|\d+))?"
)
_RE_BOOT_STARTED = re.compile(r"tokyo-l2-collector live engine started boot_id=(\S+)")
_RE_DUAL_OUTAGE_EP = re.compile(r"\b([a-z0-9_]+)\s+dual A/B outage exceeded", re.IGNORECASE)
_RE_CONNECTED_EP = re.compile(r"\bConnected\s+([a-z0-9_]+)\s+conn=(A|B)\b")
_RE_EP_ERROR = re.compile(
    r"(?:WS connect failed|Subscribe write error on|Resubscribing)\s+([a-z0-9_]+)\b"
)
_RE_GAP_END_JSON_EP = re.compile(r'"event"\s*:\s*"GAP_END".*?"endpoint"\s*:\s*"([a-z0-9_]+)"')
_RE_GAP_END_TEXT_EP = re.compile(
    r"(?:GAP_END(?:\s+endpoint=|\s+on\s+|\s+)|dual A/B outage recovered(?:\s+on\s+|\s+))([a-z0-9_]+)\b",
    re.IGNORECASE,
)
_RE_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def redact_secrets(text: str, extra_secrets: list[str] | None = None) -> str:
    """Scrub Telegram bot tokens, chat IDs, and Healthchecks ping UUIDs from logs/errors."""
    if not text:
        return ""
    out = str(text)
    for sec in extra_secrets or ():
        if sec and len(str(sec).strip()) >= 3:
            out = out.replace(str(sec).strip(), "***")
    out = _RE_TG_BOT_TOKEN.sub(r"\1***:***", out)
    out = _RE_HC_PING_UUID.sub(lambda m: f"{m.group(1)}***{m.group(3) or ''}", out)
    return out


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


@dataclass
class HttpDeliveryResult:
    ok: bool
    attempts: int
    status_code: int | None = None
    error: str | None = None


HttpSenderFn = Callable[[str, bytes, dict[str, str], float], tuple[int, bytes]]


def default_http_post(
    url: str,
    body: bytes,
    headers: dict[str, str],
    timeout: float = 8.0,
) -> tuple[int, bytes]:
    req_headers = {"User-Agent": USER_AGENT, **headers}
    req = urllib.request.Request(url, data=body, headers=req_headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        code = int(getattr(resp, "status", None) or resp.getcode() or 200)
        return code, resp.read()


def http_post_with_retry(
    url: str,
    body: bytes,
    *,
    headers: dict[str, str] | None = None,
    timeout_seconds: float = 8.0,
    max_attempts: int = 3,
    retry_backoff_seconds: float = 0.5,
    http_sender: HttpSenderFn | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    extra_secrets: list[str] | None = None,
) -> HttpDeliveryResult:
    """Bounded-timeout, finite-retry HTTP POST that NEVER raises or leaks secrets."""
    sender = http_sender or default_http_post
    attempts_limit = max(1, int(max_attempts))
    eff_headers = dict(headers or {})
    last_err = "unknown error"
    last_code: int | None = None

    for attempt in range(1, attempts_limit + 1):
        try:
            code, _resp = sender(url, body, eff_headers, timeout_seconds)
            last_code = int(code)
            if 200 <= last_code < 300:
                return HttpDeliveryResult(ok=True, attempts=attempt, status_code=last_code)
            last_err = f"HTTP {last_code}"
            if 400 <= last_code < 500 and last_code != 429:
                break
        except urllib.error.HTTPError as exc:
            last_code = int(exc.code)
            last_err = f"HTTPError {exc.code}: {exc.reason}"
            if 400 <= exc.code < 500 and exc.code != 429:
                break
        except Exception as exc:
            last_err = f"{type(exc).__name__}: {exc}"

        if attempt < attempts_limit and retry_backoff_seconds > 0:
            sleeper(retry_backoff_seconds * attempt)

    return HttpDeliveryResult(
        ok=False,
        attempts=min(attempt, attempts_limit),
        status_code=last_code,
        error=redact_secrets(last_err, extra_secrets=extra_secrets),
    )


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
        try:
            raw_out = runner()
            status = "ok"
        except Exception as exc:
            raw_out = ""
            status = f"unavailable:{type(exc).__name__}"
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


def ping_healthcheck(
    base_url: str | None,
    *,
    ok: bool,
    reason: str = "",
    timeout_seconds: float = 8.0,
    max_attempts: int = 3,
    retry_backoff_seconds: float = 0.5,
    http_sender: HttpSenderFn | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    extra_secrets: list[str] | None = None,
) -> bool:
    """Ping Healthchecks URL (`/` when healthy, `/fail` with concise redacted reason on failure).

    Strictly non-blocking & fail-safe: catches all network/HTTP failures, logs redacted warning to
    stderr, and returns bool without raising.
    """
    if not base_url or not base_url.strip():
        return False
    clean_url = base_url.strip().rstrip("/")
    target = clean_url if ok else f"{clean_url}/fail"
    secrets = [clean_url, *(extra_secrets or ())]
    safe_reason = redact_secrets(reason or ("ok" if ok else "failed"), extra_secrets=secrets)
    body = safe_reason.encode("utf-8")[:512]
    res = http_post_with_retry(
        target,
        body,
        headers={"Content-Type": "text/plain; charset=utf-8"},
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
        http_sender=http_sender,
        sleeper=sleeper,
        extra_secrets=secrets,
    )
    if not res.ok:
        print(
            f"[HC_PING_FAILED] ok={ok} attempts={res.attempts} error={res.error}",
            file=sys.stderr,
            flush=True,
        )
    return res.ok


class NotificationClient:
    """Fail-safe dispatcher for Telegram Bot API and/or generic alert webhooks."""

    def __init__(
        self,
        *,
        telegram_bot_token: str | None = None,
        telegram_chat_id: str | None = None,
        webhook_url: str | None = None,
        telegram_api_base: str = "https://api.telegram.org",
        timeout_seconds: float = 8.0,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 0.5,
        http_sender: HttpSenderFn | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.telegram_bot_token = (telegram_bot_token or "").strip() or None
        self.telegram_chat_id = (telegram_chat_id or "").strip() or None
        self.webhook_url = (webhook_url or "").strip() or None
        self.telegram_api_base = telegram_api_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.http_sender = http_sender
        self.sleeper = sleeper

    @property
    def configured(self) -> bool:
        has_tg = bool(self.telegram_bot_token and self.telegram_chat_id)
        return has_tg or bool(self.webhook_url)

    def _secrets(self) -> list[str]:
        return [s for s in (self.telegram_bot_token, self.telegram_chat_id, self.webhook_url) if s]

    def send_text(
        self,
        text: str,
        *,
        severity: str = "INFO",
        title: str = "",
        details: dict[str, Any] | None = None,
    ) -> bool:
        secrets = self._secrets()
        safe_text = redact_secrets(text, extra_secrets=secrets)
        if len(safe_text) > 3800:
            safe_text = safe_text[:3780] + "\n...(truncated)"

        if not self.configured:
            print(f"[NOTIFY {severity}] {title or safe_text.splitlines()[0]}", file=sys.stderr, flush=True)
            return False

        delivered_any = False
        all_ok = True

        if self.telegram_bot_token and self.telegram_chat_id:
            tg_url = f"{self.telegram_api_base}/bot{self.telegram_bot_token}/sendMessage"
            payload = json.dumps(
                {
                    "chat_id": self.telegram_chat_id,
                    "text": safe_text,
                    "disable_web_page_preview": True,
                },
                ensure_ascii=False,
            ).encode("utf-8")
            res = http_post_with_retry(
                tg_url,
                payload,
                headers={"Content-Type": "application/json; charset=utf-8"},
                timeout_seconds=self.timeout_seconds,
                max_attempts=self.max_attempts,
                retry_backoff_seconds=self.retry_backoff_seconds,
                http_sender=self.http_sender,
                sleeper=self.sleeper,
                extra_secrets=secrets,
            )
            if res.ok:
                delivered_any = True
            else:
                all_ok = False
                print(
                    f"[TELEGRAM_SEND_FAILED] attempts={res.attempts} error={res.error}",
                    file=sys.stderr,
                    flush=True,
                )

        if self.webhook_url:
            wh_payload = json.dumps(
                {
                    "severity": severity,
                    "title": redact_secrets(title, extra_secrets=secrets),
                    "utc": datetime.now(timezone.utc).isoformat(),
                    "text": safe_text,
                    "details": details or {},
                },
                ensure_ascii=False,
            ).encode("utf-8")
            res = http_post_with_retry(
                self.webhook_url,
                wh_payload,
                headers={"Content-Type": "application/json; charset=utf-8"},
                timeout_seconds=self.timeout_seconds,
                max_attempts=self.max_attempts,
                retry_backoff_seconds=self.retry_backoff_seconds,
                http_sender=self.http_sender,
                sleeper=self.sleeper,
                extra_secrets=secrets,
            )
            if res.ok:
                delivered_any = True
            else:
                all_ok = False
                print(
                    f"[WEBHOOK_SEND_FAILED] attempts={res.attempts} error={res.error}",
                    file=sys.stderr,
                    flush=True,
                )

        return delivered_any and all_ok


def send_alert(webhook_url: str | None, severity: str, title: str, details: dict[str, Any]) -> bool:
    """Backwards-compatible fail-safe webhook/stderr alert helper."""
    if not webhook_url:
        print(f"[ALERT {severity}] {title}: {redact_secrets(json.dumps(details))}", file=sys.stderr, flush=True)
        return False
    client = NotificationClient(webhook_url=webhook_url)
    return client.send_text(
        f"[{severity}] {title}\n{json.dumps(details, ensure_ascii=False)}",
        severity=severity,
        title=title,
        details=details,
    )


def default_systemd_status_probe(unit_name: str) -> dict[str, Any]:
    """Query systemd unit status without failing if systemd is absent."""
    props = (
        "LoadState,ActiveState,SubState,MainPID,NRestarts,"
        "ExecMainStatus,ExecMainCode,Result,ActiveEnterTimestampMonotonic"
    )
    try:
        proc = subprocess.run(
            ["systemctl", "show", unit_name, f"--property={props}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception as exc:
        return {"available": False, "unit": unit_name, "error": f"{type(exc).__name__}: {exc}"}

    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or f"rc={proc.returncode}").strip()
        return {"available": False, "unit": unit_name, "error": err}

    kv: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            kv[k.strip()] = v.strip()

    if kv.get("LoadState") in (None, "", "not-found"):
        return {
            "available": False,
            "unit": unit_name,
            "load_state": kv.get("LoadState", "unknown"),
            "error": "unit_not_found",
        }

    def _int_or_zero(val: str | None) -> int:
        try:
            return int(val or "0")
        except ValueError:
            return 0

    uptime_seconds: float | None = None
    active_mono_us = _int_or_zero(kv.get("ActiveEnterTimestampMonotonic"))
    if kv.get("ActiveState") == "active" and active_mono_us > 0:
        try:
            now_mono_us = int(time.clock_gettime(time.CLOCK_MONOTONIC) * 1_000_000)
            if now_mono_us >= active_mono_us:
                uptime_seconds = (now_mono_us - active_mono_us) / 1_000_000.0
        except Exception:
            uptime_seconds = None

    return {
        "available": True,
        "unit": unit_name,
        "load_state": kv.get("LoadState", ""),
        "active_state": kv.get("ActiveState", ""),
        "sub_state": kv.get("SubState", ""),
        "main_pid": _int_or_zero(kv.get("MainPID")),
        "n_restarts": _int_or_zero(kv.get("NRestarts")),
        "exec_main_status": _int_or_zero(kv.get("ExecMainStatus")),
        "exec_main_code": kv.get("ExecMainCode", ""),
        "result": kv.get("Result", ""),
        "uptime_seconds": uptime_seconds,
        "error": None,
    }


def default_journal_reader(
    unit_name: str,
    cursor: str | None = None,
    max_lines: int = 400,
) -> tuple[list[str], str | None]:
    """Incrementally read recent journal messages for a systemd unit using `--after-cursor`."""
    cmd = ["journalctl", "-u", unit_name, "--no-pager", "--output=json"]
    if cursor:
        cmd.extend(["--after-cursor", cursor, "--lines", str(max_lines)])
    else:
        cmd.extend(["--lines", str(min(max_lines, 100))])

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=6)
    except Exception:
        return [], cursor

    if proc.returncode != 0:
        # Fallback cleanly if stored cursor became stale / pruned from journal ring
        if cursor and "cursor" in (proc.stderr or "").lower():
            return default_journal_reader(unit_name, cursor=None, max_lines=100)
        return [], cursor

    messages: list[str] = []
    new_cursor = cursor
    for raw_line in proc.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            if obj.get("__CURSOR"):
                new_cursor = str(obj["__CURSOR"])
            msg = obj.get("MESSAGE")
            if isinstance(msg, str) and msg.strip():
                messages.append(msg.strip())
    return messages, new_cursor


def _extract_gap_end_endpoint(line: str) -> str | None:
    m1 = _RE_GAP_END_JSON_EP.search(line)
    if m1:
        return m1.group(1)
    m2 = _RE_GAP_END_TEXT_EP.search(line)
    if m2:
        return m2.group(1)
    return None


def inspect_collector_logs(lines: list[str]) -> dict[str, Any]:
    """Parse Rust collector log lines for per-endpoint outages/recoveries, LOCAL_LOSS, errors & boots."""
    dual_outages: list[str] = []
    local_losses: list[str] = []
    write_or_seal_errors: list[str] = []
    starts: list[str] = []
    clean_shutdowns: list[str] = []
    unclean_boot_transitions = 0
    saw_active_boot = False

    # Chronological per-endpoint tracking within this log batch:
    # A subsequent error or outage invalidates any earlier connect/recovery candidate in the same batch.
    endpoint_timeline: dict[str, dict[str, Any]] = {}

    def _ep_rec(ep: str) -> dict[str, Any]:
        return endpoint_timeline.setdefault(
            ep,
            {
                "saw_outage": False,
                "outage_messages": [],
                "connected_clean": False,
                "ws_connect_count": 0,
                "explicit_recovered": False,
                "saw_error_after_connect": False,
            },
        )

    for raw in lines:
        line = _RE_ANSI.sub("", str(raw)).strip()
        if not line:
            continue
        if "dual A/B outage exceeded" in line or (
            "dual A/B outage" in line and "recovered" not in line.lower()
        ):
            dual_outages.append(line)
            m_ep = _RE_DUAL_OUTAGE_EP.search(line)
            ep = m_ep.group(1) if m_ep else "unknown_endpoint"
            rec = _ep_rec(ep)
            rec["saw_outage"] = True
            rec["connected_clean"] = False
            rec["ws_connect_count"] = 0
            rec["explicit_recovered"] = False
            rec["outage_messages"].append(line)

        gap_ep = _extract_gap_end_endpoint(line)
        if gap_ep is not None:
            rec = _ep_rec(gap_ep)
            rec["explicit_recovered"] = True
            rec["saw_error_after_connect"] = False

        m_err = _RE_EP_ERROR.search(line)
        if m_err:
            ep = m_err.group(1)
            rec = _ep_rec(ep)
            rec["connected_clean"] = False
            rec["explicit_recovered"] = False
            rec["saw_error_after_connect"] = True

        m_conn = _RE_CONNECTED_EP.search(line)
        if m_conn:
            ep = m_conn.group(1)
            rec = _ep_rec(ep)
            rec["connected_clean"] = True
            rec["ws_connect_count"] = int(rec.get("ws_connect_count") or 0) + 1
            rec["saw_error_after_connect"] = False

        if "LOCAL_LOSS" in line:
            local_losses.append(line)
        if "Disk write error on " in line or "Failed sealing " in line:
            write_or_seal_errors.append(line)
        m_start = _RE_BOOT_STARTED.search(line)
        if m_start:
            if saw_active_boot:
                unclean_boot_transitions += 1
            saw_active_boot = True
            starts.append(m_start.group(1))
        if "Shutdown seal complete (" in line:
            saw_active_boot = False
            clean_shutdowns.append(line)

    return {
        "dual_outages": dual_outages,
        "endpoint_timeline": endpoint_timeline,
        "local_losses": local_losses,
        "write_or_seal_errors": write_or_seal_errors,
        "starts": starts,
        "clean_shutdowns": clean_shutdowns,
        "unclean_boot_transitions": unclean_boot_transitions,
    }


def _manifest_stat_summary(mpath: Path, rel_path: str) -> dict[str, Any] | None:
    """Read only small `.manifest.json` sidecar files (never `.tsv.zst` / `.partial`)."""
    try:
        st = mpath.stat()
        obj = json.loads(mpath.read_text(encoding="utf-8"))
        date_utc = str(obj.get("date_utc") or "")
        hour_utc = str(obj.get("hour_utc") or "").zfill(2)
        return {
            "rel_path": rel_path,
            "mtime_ns": int(st.st_mtime_ns),
            "mtime_s": float(st.st_mtime),
            "exchange": str(obj.get("exchange") or ""),
            "date_utc": date_utc,
            "hour_utc": hour_utc,
            "hour_key": f"{date_utc}T{hour_utc}",
            "boot_id": str(obj.get("boot_id") or ""),
            "bytes": int(obj.get("bytes") or 0),
            "data_lines": int(obj.get("data_lines") or 0),
            "data_escaped_lines": int(obj.get("data_escaped_lines") or 0),
            "meta_lines": int(obj.get("meta_lines") or 0),
            "local_loss_lines": int(obj.get("local_loss_lines") or 0),
            "gap_start_lines": int(obj.get("gap_start_lines") or 0),
            "switch_lines": int(obj.get("switch_lines") or 0),
        }
    except Exception:
        return None


def inspect_data_tree(
    data_root: Path,
    enabled_exchanges: list[str],
    *,
    now_s: float,
    stale_seconds: float,
    manifest_cache: dict[str, dict[str, Any]] | None = None,
    service_uptime_seconds: float | None = None,
) -> dict[str, Any]:
    """Lightweight metadata scan of `.partial` stat & `.manifest.json` summaries ONLY.

    Computes both the **previous complete UTC hour** window (`prev_hour_*`) and **cumulative**
    historical totals (`cumulative_*`), and strictly NEVER opens or decompresses `.tsv.zst` files.
    """
    cache = manifest_cache if manifest_cache is not None else {}
    raw_root = data_root / "raw"
    per_exchange: dict[str, Any] = {}
    stale_exchanges: list[str] = []
    manifests_with_local_loss: list[dict[str, Any]] = []
    in_startup_grace = (
        service_uptime_seconds is not None and service_uptime_seconds < min(float(stale_seconds), 90.0)
    )
    prev_hour_dt = datetime.fromtimestamp(now_s, tz=timezone.utc).replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(hours=1)
    prev_hour_key = prev_hour_dt.strftime("%Y-%m-%dT%H")

    seen_manifest_keys: set[str] = set()
    for ex in enabled_exchanges:
        ex_dir = raw_root / ex
        partials: list[tuple[float, int, str]] = []
        manifests: list[dict[str, Any]] = []

        if ex_dir.is_dir():
            for date_dir in sorted(ex_dir.iterdir()):
                if not date_dir.is_dir():
                    continue
                for entry in date_dir.iterdir():
                    name = entry.name
                    if name.endswith(".tsv.zst.partial"):
                        try:
                            st = entry.stat()
                            partials.append((float(st.st_mtime), int(st.st_size), entry.relative_to(data_root).as_posix()))
                        except OSError:
                            continue
                    elif name.endswith(".manifest.json"):
                        rel = entry.relative_to(data_root).as_posix()
                        seen_manifest_keys.add(rel)
                        try:
                            st = entry.stat()
                            cached = cache.get(rel)
                            if cached is None or cached.get("mtime_ns") != int(st.st_mtime_ns):
                                parsed = _manifest_stat_summary(entry, rel)
                                if parsed is not None:
                                    cache[rel] = parsed
                                    cached = parsed
                            if cached is not None:
                                manifests.append(cached)
                        except OSError:
                            continue

        manifests.sort(key=lambda m: (m["hour_key"], m["mtime_s"], m["rel_path"]))
        latest_partial = max(partials, key=lambda x: x[0]) if partials else None
        latest_manifest = manifests[-1] if manifests else None
        prev_hour_mfs = [m for m in manifests if m["hour_key"] == prev_hour_key]

        # Compare with recent *earlier* clean sealed hours. Aggregate split manifests
        # for one UTC hour before computing the reference, and never read zstd payloads.
        earlier_hours: dict[str, int] = {}
        for m in manifests:
            if (
                m["hour_key"] < prev_hour_key
                and m["local_loss_lines"] == 0
                and m["gap_start_lines"] == 0
            ):
                key = m["hour_key"]
                earlier_hours[key] = earlier_hours.get(key, 0) + (
                    m["data_lines"] + m["data_escaped_lines"]
                )
        reference_samples = [
            count for _, count in sorted(earlier_hours.items())[-48:] if count > 0
        ]
        reference_data_lines = (
            int(median(reference_samples)) if len(reference_samples) >= 3 else None
        )

        activity_candidates: list[float] = []
        if latest_partial is not None:
            activity_candidates.append(latest_partial[0])
        if latest_manifest is not None:
            activity_candidates.append(float(latest_manifest["mtime_s"]))
        latest_activity_s = max(activity_candidates) if activity_candidates else None
        activity_age_s = (now_s - latest_activity_s) if latest_activity_s is not None else None

        is_stale = False
        stale_reason: str | None = None
        if not in_startup_grace:
            if latest_activity_s is None:
                is_stale = True
                stale_reason = "no_partial_or_manifest"
            elif activity_age_s is not None and activity_age_s > stale_seconds:
                is_stale = True
                stale_reason = f"last_update_{int(activity_age_s)}s_ago>{int(stale_seconds)}s"

        if is_stale:
            stale_exchanges.append(ex)

        loss_mfs = [m for m in manifests if m["local_loss_lines"] > 0]
        manifests_with_local_loss.extend(loss_mfs)

        cum_data = sum(m["data_lines"] + m["data_escaped_lines"] for m in manifests)
        cum_meta = sum(m["meta_lines"] for m in manifests)
        cum_loss = sum(m["local_loss_lines"] for m in manifests)
        cum_gap = sum(m["gap_start_lines"] for m in manifests)
        cum_sw = sum(m["switch_lines"] for m in manifests)
        cum_bytes = sum(m["bytes"] for m in manifests)

        prev_data = sum(m["data_lines"] + m["data_escaped_lines"] for m in prev_hour_mfs)
        prev_meta = sum(m["meta_lines"] for m in prev_hour_mfs)
        prev_loss = sum(m["local_loss_lines"] for m in prev_hour_mfs)
        prev_gap = sum(m["gap_start_lines"] for m in prev_hour_mfs)
        prev_sw = sum(m["switch_lines"] for m in prev_hour_mfs)
        prev_bytes = sum(m["bytes"] for m in prev_hour_mfs)

        per_exchange[ex] = {
            "active_partial_count": len(partials),
            "latest_partial_path": latest_partial[2] if latest_partial else None,
            "latest_partial_bytes": latest_partial[1] if latest_partial else 0,
            "latest_partial_age_s": round(now_s - latest_partial[0], 1) if latest_partial else None,
            "last_activity_epoch_s": latest_activity_s,
            "last_activity_age_s": round(activity_age_s, 1) if activity_age_s is not None else None,
            "is_stale": is_stale,
            "stale_reason": stale_reason,
            "prev_hour_key": prev_hour_key,
            "prev_hour_manifest_count": len(prev_hour_mfs),
            "prev_hour_data_lines": prev_data,
            "reference_data_lines": reference_data_lines,
            "reference_hour_count": len(reference_samples),
            "prev_hour_meta_lines": prev_meta,
            "prev_hour_local_loss_lines": prev_loss,
            "prev_hour_gap_start_lines": prev_gap,
            "prev_hour_switch_lines": prev_sw,
            "prev_hour_sealed_bytes": prev_bytes,
            "latest_manifest_hour": latest_manifest["hour_key"] if latest_manifest else None,
            "latest_manifest_data_lines": latest_manifest["data_lines"] if latest_manifest else 0,
            "latest_manifest_bytes": latest_manifest["bytes"] if latest_manifest else 0,
            "cumulative_manifest_count": len(manifests),
            "cumulative_data_lines": cum_data,
            "cumulative_meta_lines": cum_meta,
            "cumulative_local_loss_lines": cum_loss,
            "cumulative_gap_start_lines": cum_gap,
            "cumulative_switch_lines": cum_sw,
            "cumulative_sealed_bytes": cum_bytes,
            # Backwards-compatible aliases
            "manifest_count": len(manifests),
            "total_data_lines": cum_data,
            "total_meta_lines": cum_meta,
            "total_local_loss_lines": cum_loss,
            "total_gap_start_lines": cum_gap,
            "total_switch_lines": cum_sw,
            "total_sealed_bytes": cum_bytes,
        }

    # Prune deleted manifests from cache if any
    for k in list(cache.keys()):
        if k not in seen_manifest_keys:
            cache.pop(k, None)

    return {
        "prev_hour_key": prev_hour_key,
        "per_exchange": per_exchange,
        "stale_exchanges": stale_exchanges,
        "manifests_with_local_loss": manifests_with_local_loss,
        "in_startup_grace": in_startup_grace,
    }


def assess_hourly_data_volume(
    data_tree: dict[str, Any], *, now_s: float, report_minute: int = 10
) -> dict[str, Any]:
    """Flag empty or extremely low previous-hour DATA even when META files keep growing.

    Heuristic: fewer than 10% of the median of up to 48 earlier, nonempty,
    gap-free sealed hours (at least three reference hours). Zero is always
    anomalous when an hourly manifest exists. This is a lightweight operational
    gate, not a substitute for per-symbol/channel deep QA.
    """
    dt = datetime.fromtimestamp(now_s, tz=timezone.utc)
    result: dict[str, Any] = {
        "hour_key": data_tree.get("prev_hour_key"),
        "ready": False,
        "affected": {},
    }
    if dt.minute < report_minute:
        return result
    per_ex = data_tree.get("per_exchange") or {}
    # Fresh P2 runs may not yet have finished their first hour.
    if not any(int(v.get("cumulative_manifest_count") or 0) > 0 for v in per_ex.values()):
        return result
    result["ready"] = True
    for ex, info in per_ex.items():
        sealed = int(info.get("prev_hour_manifest_count") or 0)
        lines = int(info.get("prev_hour_data_lines") or 0)
        baseline = info.get("reference_data_lines")
        why = None
        if sealed == 0:
            why = "上一完整小时缺少封口文件"
        elif lines == 0:
            why = "上一完整小时有效行情为零"
        elif baseline is not None and baseline > 0 and lines < baseline * 0.10:
            why = "有效行情低于近48个参考小时中位数的10%"
        if why:
            result["affected"][ex] = {
                "reason": why,
                "data_lines": lines,
                "manifest_count": sealed,
                "reference_data_lines": baseline,
            }
    return result


def inspect_stream_activity(
    data_root: Path, expected: set[tuple[str, str, str]], *, now_s: float, stale_seconds: float,
) -> dict[str, Any]:
    """Check timestamps of market frames written to disk, excluding META and control frames."""
    path = data_root / "meta" / "stream_activity.json"
    now_ns = int(now_s * 1_000_000_000)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        sampled = int(payload["sampled_wall_ns"])
        if not -2 <= (now_ns - sampled) / 1e9 <= min(60, stale_seconds):
            raise ValueError("stream activity sample is stale")
        streams = {
            (entry["exchange"], entry["symbol"], entry["channel"]):
                int(entry["last_written_wall_ns"])
            for entry in payload["streams"] if int(entry["written_frames"]) > 0
        }
        stale = [
            "/".join(stream) for stream in sorted(expected)
            if stream not in streams or not -2 <= (now_ns - streams[stream]) / 1e9 <= stale_seconds
        ]
        return {
            "healthy": not stale, "missing_or_stale": stale,
            "max_sampled_queue_depth": payload.get("max_sampled_queue_depth"),
        }
    except (OSError, ValueError, TypeError, KeyError) as exc:
        return {"healthy": False, "error": type(exc).__name__ + ": " + str(exc), "missing_or_stale": []}


def inspect_shutdown_audits(data_root: Path, seen_files: set[str] | None = None) -> list[dict[str, Any]]:
    """Read new `meta/shutdown_*.json` files emitted by `tokyo-l2-collector`."""
    meta_dir = data_root / "meta"
    if not meta_dir.is_dir():
        return []
    seen = seen_files if seen_files is not None else set()
    new_audits: list[dict[str, Any]] = []
    for fpath in sorted(meta_dir.glob("shutdown_*.json")):
        if fpath.name in seen:
            continue
        seen.add(fpath.name)
        try:
            obj = json.loads(fpath.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(obj, dict):
            obj["_file_name"] = fpath.name
            new_audits.append(obj)
    return new_audits


def inspect_archive_ledger(
    data_root: Path,
    *,
    now_s: float,
    archive_stale_minutes: float = 90.0,
) -> dict[str, Any]:
    """Read P1 `archive_ledger.sqlite` in strict read-only URI mode (never blocks archive_sync)."""
    db_path = data_root / "archive_ledger.sqlite"
    base: dict[str, Any] = {
        "exists": False,
        "status": "PENDING",
        "state_counts": {"SEALED": 0, "VERIFIED": 0, "PRUNED": 0},
        "per_exchange": {},
        "errored_partitions": [],
        "conflicted_partitions": [],
        "stale_sealed_partitions": [],
        "last_snapshot_utc": None,
        "recent_errors": [],
        "read_error": None,
    }
    if not db_path.is_file():
        return base

    uri = f"file:{urllib.parse.quote(str(db_path))}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=2.0)
        conn.row_factory = sqlite3.Row
    except Exception as exc:
        base["exists"] = True
        base["status"] = "FAILED"
        base["read_error"] = f"{type(exc).__name__}: {exc}"
        return base

    try:
        base["exists"] = True
        for row in conn.execute(
            "SELECT exchange, state, COUNT(*) AS cnt, COALESCE(SUM(local_bytes), 0) AS total_bytes "
            "FROM partitions GROUP BY exchange, state"
        ).fetchall():
            ex = str(row["exchange"])
            st = str(row["state"])
            cnt = int(row["cnt"])
            base["state_counts"][st] = base["state_counts"].get(st, 0) + cnt
            ex_map = base["per_exchange"].setdefault(ex, {"SEALED": 0, "VERIFIED": 0, "PRUNED": 0})
            ex_map[st] = cnt

        problem_rows = conn.execute(
            """
            SELECT rel_data_path, exchange, state, last_error_code, last_error_message,
                   last_error_at_utc, has_conflict, conflict_detail_json, sealed_at_utc
            FROM partitions
            WHERE has_conflict = 1 OR last_error_code IS NOT NULL OR state = 'SEALED'
            ORDER BY updated_at_utc DESC
            """
        ).fetchall()

        stale_cutoff_s = float(archive_stale_minutes) * 60.0
        for r in problem_rows:
            rd = dict(r)
            if rd["has_conflict"]:
                base["conflicted_partitions"].append(
                    {
                        "rel_data_path": rd["rel_data_path"],
                        "exchange": rd["exchange"],
                        "state": rd["state"],
                        "error_message": rd["last_error_message"],
                    }
                )
            elif rd["last_error_code"]:
                base["errored_partitions"].append(
                    {
                        "rel_data_path": rd["rel_data_path"],
                        "exchange": rd["exchange"],
                        "state": rd["state"],
                        "error_code": rd["last_error_code"],
                        "error_message": rd["last_error_message"],
                    }
                )
            elif rd["state"] == "SEALED" and rd.get("sealed_at_utc"):
                try:
                    sealed_dt = datetime.fromisoformat(str(rd["sealed_at_utc"]).replace("Z", "+00:00"))
                    if (now_s - sealed_dt.timestamp()) > stale_cutoff_s:
                        base["stale_sealed_partitions"].append(
                            {
                                "rel_data_path": rd["rel_data_path"],
                                "exchange": rd["exchange"],
                                "sealed_at_utc": rd["sealed_at_utc"],
                            }
                        )
                except ValueError:
                    pass

        snap_row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'last_snapshot_utc'"
        ).fetchone()
        if snap_row:
            base["last_snapshot_utc"] = str(snap_row["value"])

        if base["conflicted_partitions"]:
            base["status"] = "CONFLICT"
        elif base["errored_partitions"] or base["stale_sealed_partitions"]:
            base["status"] = "FAILED"
        elif base["state_counts"].get("SEALED", 0) > 0:
            base["status"] = "PENDING"
        elif base["state_counts"].get("VERIFIED", 0) + base["state_counts"].get("PRUNED", 0) > 0:
            base["status"] = "VERIFIED"
        else:
            base["status"] = "PENDING"
        return base
    except Exception as exc:
        base["status"] = "FAILED"
        base["read_error"] = f"{type(exc).__name__}: {exc}"
        return base
    finally:
        conn.close()


_ALERT_NAMES = {
    "collector_stopped": "采集程序停止",
    "collector_probe_failed": "采集状态无法检查",
    "collector_abnormal_restart": "采集程序异常重启",
    "collector_data_stale": "行情文件停止更新",
    "collector_data_volume_abnormal": "有效行情量异常",
    "collector_stream_stale": "行情频道未持续写入",
    "dual_ab_outage_gt_5s": "行情双连接中断",
    "local_loss_detected": "本地数据丢失记录",
    "disk_usage_high": "磁盘使用率过高",
    "disk_probe_failed": "磁盘状态无法检查",
    "clock_unsynced": "时钟同步异常",
    "archive_upload_or_verify_failed": "云端归档失败",
    "archive_ledger_conflict": "云端文件校验冲突",
    "lightsail_proxy_unavailable": "归档代理不可用",
}

_EXCHANGE_NAMES = {
    "binance_futures": "Binance",
    "bybit_linear": "Bybit",
    "bitget_v3": "Bitget",
    "gate_futures": "Gate",
}


def _display_time(now_s: float) -> str:
    dt_utc = datetime.fromtimestamp(now_s, tz=timezone.utc)
    dt_cn = dt_utc.astimezone(timezone(timedelta(hours=8)))
    return f"{dt_cn:%m-%d %H:%M} 北京时间 / {dt_utc:%H:%M} UTC"


def format_alert_message(
    *,
    alert_key: str,
    severity: str,
    summary: str,
    details: dict[str, Any],
    now_s: float,
    suppressed_count: int = 0,
    is_reminder: bool = False,
) -> str:
    """Human-readable operator message; raw diagnostics remain in monitor_state.json."""
    title = _ALERT_NAMES.get(alert_key, alert_key.replace("_", " "))
    prefix = "🔁 [持续告警]" if is_reminder else "🚨 [告警]"
    lines = [
        f"{prefix} 东京 L2｜{title}",
        f"时间：{_display_time(now_s)}",
        f"情况：{summary}",
    ]
    if alert_key == "collector_data_volume_abnormal":
        hour_key = str(details.get("hour_key") or "上一完整小时")
        lines.append(f"统计时段：{hour_key} UTC")
        for ex, item in list((details.get("affected") or {}).items())[:4]:
            name = _EXCHANGE_NAMES.get(ex, ex)
            count = int(item.get("data_lines") or 0)
            baseline = item.get("reference_data_lines")
            ref_text = f"；参考约{int(baseline):,}行/小时" if baseline is not None else ""
            lines.append(f"• {name}：{count:,} 行（{item.get('reason', '数量异常')}{ref_text}）")
        lines.append("影响：该小时暂不纳入有效研究样本，待深度 QA 核验。")
    elif alert_key == "dual_ab_outage_gt_5s":
        endpoints = details.get("pending_endpoints") or []
        if endpoints:
            lines.append("受影响端点：" + ", ".join(str(ep) for ep in endpoints[:6]))
        lines.append("恢复要求：确认真实 DATA 恢复后才能解除保守锁。")
    elif alert_key == "local_loss_detected":
        mf = details.get("manifest_local_loss") or []
        journal = details.get("journal_local_loss") or []
        write_errors = details.get("write_errors") or []
        if mf:
            lines.append("证据：" + "、".join(
                f"{_EXCHANGE_NAMES.get(str(m.get('exchange')), str(m.get('exchange')))} {int(m.get('local_loss_lines') or 0):,}条标记"
                for m in mf[:4]
            ))
        if journal or write_errors:
            lines.append(f"另有采集日志 {len(journal)} 条，写盘异常 {len(write_errors)} 条。")
        lines.append("注意：丢失标记行数不等于实际丢失的行情条数。")
    elif alert_key == "collector_data_stale":
        if details:
            lines.append("涉及交易所：" + "、".join(
                _EXCHANGE_NAMES.get(str(ex), str(ex)) for ex in list(details)[:4]
            ))
    elif alert_key == "disk_usage_high":
        lines.append(f"磁盘占用：{details.get('disk_pct', '?')}%（阈值 {details.get('threshold', '?')}%）")
    elif alert_key == "archive_ledger_conflict":
        lines.append("影响：远端文件完整性待核验，本地原件必须保留。")
    elif alert_key == "archive_upload_or_verify_failed":
        lines.append("影响：本次归档尚未通过远端核验，本地原件必须保留。")
    if suppressed_count:
        lines.append(f"期间相同告警已去重抑制 {suppressed_count} 次。")
    return "\n".join(lines)


def format_recovery_message(
    *,
    alert_key: str,
    summary: str,
    started_at_s: float,
    now_s: float,
    suppressed_count: int = 0,
    last_loss_detected_at_s: float | None = None,
) -> str:
    title = _ALERT_NAMES.get(alert_key, alert_key.replace("_", " "))
    dur = max(0, int(now_s - started_at_s))
    if alert_key == "local_loss_detected":
        quiet = "当前未检测到新增本地丢数或写盘异常。"
        if last_loss_detected_at_s is not None:
            quiet_seconds = max(0, int(now_s - last_loss_detected_at_s))
            quiet = f"过去 {quiet_seconds} 秒未检测到新增本地丢数或写盘异常。"
        return "\n".join([
            f"🟡 [无新增] 东京 L2｜{title}",
            f"时间：{_display_time(now_s)}",
            quiet,
            "历史丢数记录继续保留，受影响的数据仍需单独质检。",
        ])
    lines = [
        f"✅ [已恢复] 东京 L2｜{title}",
        f"时间：{_display_time(now_s)}",
        f"之前的异常条件已解除（持续约 {dur} 秒）。",
    ]
    if alert_key == "collector_data_volume_abnormal":
        lines.append("说明：最近完整小时的 DATA 已回到初筛阈值，仍需按币种和频道完成深度 QA。")
    if suppressed_count:
        lines.append(f"期间去重抑制 {suppressed_count} 次。")
    return "\n".join(lines)

def format_hourly_report(
    *,
    now_s: float,
    enabled_exchanges: list[str],
    collector_status: dict[str, Any],
    archive_status: dict[str, Any],
    data_tree: dict[str, Any],
    ledger_info: dict[str, Any],
    disk_pct: float | None,
    disk_thresh: float,
    chrony_row: dict[str, Any] | None,
    active_alerts: dict[str, dict[str, Any]],
) -> str:
    """Compact Telegram summary, with market DATA distinct from META file activity."""
    prev_key = str(data_tree.get("prev_hour_key") or "?")
    volume = data_tree.get("hourly_volume_assessment") or {}
    affected = volume.get("affected") or {}
    sc = ledger_info.get("state_counts") or {}
    if archive_status.get("available") and archive_status.get("active_state") == "failed":
        arch = "FAILED"
    else:
        arch = ledger_info.get("status", "PENDING")
    running = (
        collector_status.get("available")
        and collector_status.get("active_state") == "active"
        and collector_status.get("sub_state") == "running"
    )
    clock_good = chrony_row is not None and chrony_row.get("certified_le_1ms") is True
    err = (chrony_row or {}).get("estimated_error_ms")
    clock_str = f"{float(err):.3f} ms" if err is not None else "未知"
    disk_str = f"{disk_pct:.1f}%" if disk_pct is not None else "未知"
    current_alerts = list(active_alerts)
    historical_loss = data_tree.get("manifests_with_local_loss") or []
    previous_hour_loss = [item for item in historical_loss if item["hour_key"] == prev_key]
    if previous_hour_loss:
        overall = "🔴 上一完整小时有本地丢数，受影响分区待质检"
    elif affected or current_alerts:
        overall = "🔴 采集或数据异常，相关时段需核查"
    elif not volume.get("ready"):
        overall = "🟡 未有完整小时数据可供初筛"
    else:
        overall = "🟡 行情量初筛正常，深度 QA 待完成"
    lines = [
        "📊 东京 L2｜小时采集报告",
        f"发送：{_display_time(now_s)}",
        f"统计：{prev_key} UTC（上一完整小时）",
        overall,
        "",
        "📡 行情 DATA（有效行情消息行数）：",
    ]
    per_ex = data_tree.get("per_exchange") or {}
    for ex in enabled_exchanges:
        info = per_ex.get(ex) or {}
        n = int(info.get("prev_hour_data_lines") or 0)
        gap = int(info.get("prev_hour_gap_start_lines") or 0)
        loss = int(info.get("prev_hour_local_loss_lines") or 0)
        if ex in affected or loss > 0:
            mark = "🔴"
        elif not volume.get("ready"):
            mark = "⚪"
        else:
            mark = "▫️"
        label = _EXCHANGE_NAMES.get(ex, ex)
        lines.append(f"{mark} {label}: {n:,} 行 · GAP {gap} · LOSS {loss}")
    lines.extend([
        "",
        f"⚙️ 采集程序：{'运行中' if running else '异常/未核实'}",
        f"📦 云端归档：{arch}（只代表文件验证；V={sc.get('VERIFIED', 0)} / P={sc.get('PRUNED', 0)}）",
        f"💾 磁盘：{disk_str}（警戒线 {disk_thresh:.0f}%）",
        f"⏱ 时钟：{'正常' if clock_good else '异常/未核实'}（误差 {clock_str}）",
        "🔬 深度数据质量：PENDING（待按币种、频道核验）",
    ])
    if current_alerts:
        names = [_ALERT_NAMES.get(k, k.replace("_", " ")) for k in current_alerts[:5]]
        lines.append("🚨 当前告警：" + "、".join(names))
    elif affected:
        lines.append("🚨 当前告警：有效行情量异常")
    else:
        lines.append("✅ 当前没有监控告警；研究数据质量仍待验收")
    if previous_hour_loss:
        lines.append("受影响分区（上一完整小时）：")
        for item in previous_hour_loss[:5]:
            name = Path(item["rel_path"]).name
            lines.append(f"• {item['hour_key']} UTC · {name} · LOSS {item['local_loss_lines']}")
        if len(previous_hour_loss) > 5:
            lines.append(f"另有 {len(previous_hour_loss) - 5} 个分区，完整清单见质检报告。")
    if historical_loss:
        lines.append(f"📌 历史丢数证据（累计）：{len(historical_loss)} 个分区留有丢数标记，相关数据待质检。")
    lines.append("说明：META 文件持续更新不代表行情 DATA 正常。")
    return "\n".join(lines)

def submit_outage_ack_request(
    data_root: Path,
    endpoint: str | None = None,
    *,
    now_epoch_s: float | None = None,
) -> dict[str, Any]:
    """Queue an atomic operator outage-ack request without modifying monitor_state.json.

    NOTE: This submits an acknowledgement request only; it does NOT verify live market data flow.
    Only the running CollectorMonitor daemon owns writes to `monitor_state.json`.
    """
    now_s = time.time() if now_epoch_s is None else float(now_epoch_s)
    req_dir = Path(data_root) / "meta" / OUTAGE_ACK_REQUEST_DIRNAME
    req_dir.mkdir(parents=True, exist_ok=True)

    target = None if endpoint in (None, "", "ALL", "all") else str(endpoint)
    req_id = f"{time.time_ns()}_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    req_obj = {
        "schema_version": 1,
        "request_id": req_id,
        "requested_at_utc": datetime.fromtimestamp(now_s, tz=timezone.utc).isoformat(),
        "requested_at_epoch_s": now_s,
        "target": target or "ALL",
    }
    req_path = req_dir / f"req_{req_id}.json"
    tmp_path = req_dir / f".req_{req_id}.tmp.{os.getpid()}"
    tmp_path.write_text(json.dumps(req_obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp_path, req_path)

    # Best-effort read-only peek of monitor_state.json for immediate operator visibility only
    pending_snapshot: list[str] = []
    state_path = Path(data_root) / "meta" / "monitor_state.json"
    if state_path.is_file():
        try:
            snap = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(snap, dict) and isinstance(snap.get("dual_outage_endpoints"), dict):
                pending_snapshot = sorted(snap["dual_outage_endpoints"].keys())
        except Exception:
            pass

    return {
        "status": "queued",
        "request_id": req_id,
        "request_file": str(req_path),
        "target": req_obj["target"],
        "requested_at_utc": req_obj["requested_at_utc"],
        "pending_endpoints_snapshot": pending_snapshot,
        "warning": (
            "--ack-outage only queues acknowledgement to clear the conservative alert lock and "
            "does NOT verify live market data recovery; confirm live market frames first."
        ),
    }


class CollectorMonitor:
    """Stateful, offline-testable P3 monitor orchestrating dedup, recovery, HC & hourly reports."""

    def __init__(
        self,
        config_path: Path,
        *,
        notifier: NotificationClient | None = None,
        hc_collector_url: str | None = None,
        hc_archive_url: str | None = None,
        service_probe: Callable[[str], dict[str, Any]] | None = None,
        journal_reader: Callable[[str, str | None], tuple[list[str], str | None]] | None = None,
        spec_fetcher: Callable[[str, dict[str, str]], bytes] | None = None,
        rtt_prober: Callable[[str, int], float | None] | None = None,
        chrony_runner: Callable[[], str] | None = None,
        disk_checker: Callable[[Path], float] | None = None,
        proxy_probe: Callable[[str, int], dict[str, Any]] | None = None,
        hc_sender: HttpSenderFn | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        allow_notify: bool = True,
    ) -> None:
        self.config_path = config_path
        self.cfg = json.loads(config_path.read_text(encoding="utf-8"))
        self.data_root = Path(self.cfg["deployment"]["data_root"])
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.enabled_exchanges = [k for k, v in self.cfg["exchanges"].items() if v.get("enabled")]

        meta_cfg = self.cfg.get("meta_daemon", {})
        self.disk_thresh = float(self.cfg.get("archive", {}).get("disk_alert_pct", 70.0))
        self.rtt_interval_seconds = int(meta_cfg.get("rtt_interval_seconds", 300))
        self.chrony_interval_seconds = int(meta_cfg.get("chrony_interval_seconds", 60))
        self.health_poll_seconds = int(meta_cfg.get("health_poll_seconds", 30))
        self.collector_hc_interval_seconds = int(meta_cfg.get("collector_hc_interval_seconds", 300))
        self.collector_stale_seconds = int(meta_cfg.get("collector_stale_seconds", 300))
        self.require_stream_activity = bool(meta_cfg.get("require_stream_activity", False))
        self.expected_market_streams: set[tuple[str, str, str]] = set()
        if self.require_stream_activity:
            from data_coverage import expected_streams

            self.expected_market_streams = expected_streams(self.cfg)
        self.archive_stale_minutes = float(meta_cfg.get("archive_stale_minutes", 90.0))
        self.alert_cooldown_seconds = int(meta_cfg.get("alert_cooldown_seconds", 1800))
        self.restart_recovery_seconds = int(meta_cfg.get("restart_recovery_seconds", 600))
        self.outage_recovery_seconds = int(meta_cfg.get("outage_recovery_seconds", 60))
        self.loss_recovery_seconds = int(meta_cfg.get("loss_recovery_seconds", 300))
        self.dual_outage_recovery_mode = str(
            meta_cfg.get("dual_outage_recovery_mode", "conservative")
        ).strip().lower()
        if self.dual_outage_recovery_mode not in ("conservative", "explicit_gap_end"):
            self.dual_outage_recovery_mode = "conservative"
        self.hourly_report_enabled = bool(meta_cfg.get("hourly_report_enabled", True))
        self.hourly_report_minute_utc = int(meta_cfg.get("hourly_report_minute_utc", 10))
        self.collector_service_name = str(meta_cfg.get("collector_service_name", "tokyo-l2-collector.service"))
        self.archive_service_name = str(meta_cfg.get("archive_service_name", "tokyo-l2-archive.service"))
        self.proxy_service_name = str(meta_cfg.get("proxy_service_name", "tokyo-l2-lightsail-proxy.service"))

        self.allow_notify = allow_notify
        if notifier is not None:
            self.notifier = notifier
        else:
            tg_token = os.environ.get(meta_cfg.get("telegram_bot_token_env", "TOKYO_L2_TELEGRAM_BOT_TOKEN"))
            tg_chat_id = os.environ.get(meta_cfg.get("telegram_chat_id_env", "TOKYO_L2_TELEGRAM_CHAT_ID"))
            wh_url = os.environ.get(meta_cfg.get("alert_webhook_env", "TOKYO_L2_ALERT_WEBHOOK_URL"))
            self.notifier = NotificationClient(
                telegram_bot_token=tg_token,
                telegram_chat_id=tg_chat_id,
                webhook_url=wh_url,
                sleeper=sleeper,
            )

        self.hc_collector_url = (
            hc_collector_url
            if hc_collector_url is not None
            else os.environ.get(meta_cfg.get("healthchecks_collector_url_env", "TOKYO_L2_HEALTHCHECKS_COLLECTOR_URL"))
        )
        self.hc_archive_url = (
            hc_archive_url
            if hc_archive_url is not None
            else os.environ.get(meta_cfg.get("healthchecks_archive_url_env", "TOKYO_L2_HEALTHCHECKS_ARCHIVE_URL"))
        )

        self.service_probe = service_probe or default_systemd_status_probe
        self.journal_reader = journal_reader or default_journal_reader
        self.spec_fetcher = spec_fetcher
        self.rtt_prober = rtt_prober
        self.chrony_runner = chrony_runner
        self.disk_checker = disk_checker or check_disk_usage_pct
        self.proxy_probe = proxy_probe
        self.hc_sender = hc_sender
        self.sleeper = sleeper

        self.state_path = self.data_root / "meta" / "monitor_state.json"
        self.ack_requests_dir = self.data_root / "meta" / OUTAGE_ACK_REQUEST_DIRNAME
        self.manifest_cache: dict[str, dict[str, Any]] = {}
        self.last_chrony_row: dict[str, Any] | None = None
        self.last_rtt_rows: list[dict[str, Any]] = []
        self.last_spec_res: dict[str, Any] | None = None
        self.state = self._load_state()

    def acknowledge_dual_outage(
        self,
        endpoint: str | None = None,
        *,
        now_epoch_s: float | None = None,
    ) -> dict[str, Any]:
        """Manually acknowledge and clear conservative dual A/B outage state in-process."""
        now_s = time.time() if now_epoch_s is None else float(now_epoch_s)
        ep_outages = self.state.setdefault("dual_outage_endpoints", {})
        if not isinstance(ep_outages, dict):
            ep_outages = {}
            self.state["dual_outage_endpoints"] = ep_outages

        target = None if endpoint in (None, "", "ALL", "all") else str(endpoint)
        cleared: list[str] = []
        if target is None:
            cleared = sorted(ep_outages.keys())
            ep_outages.clear()
        elif target in ep_outages:
            ep_outages.pop(target, None)
            cleared = [target]

        # Leave active_alerts['dual_ab_outage_gt_5s'] intact until the next _update_alerts()
        # sweep so the daemon naturally dispatches the Telegram recovery message.
        self.state["last_outage_ack"] = {
            "ts_utc": datetime.fromtimestamp(now_s, tz=timezone.utc).isoformat(),
            "target": target or "ALL",
            "cleared_endpoints": cleared,
            "remaining_endpoints": sorted(ep_outages.keys()),
        }
        self._save_state()
        return {
            "status": "ok",
            **self.state["last_outage_ack"],
            "warning": "--ack-outage does NOT verify live market data flow; verify endpoint recovery separately.",
        }

    def _default_state(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "active_alerts": {},
            "journal_cursor": None,
            "last_collector_n_restarts": None,
            "last_collector_boot_id": None,
            "last_restart_event_epoch_s": None,
            "last_dual_outage_epoch_s": None,
            "dual_outage_endpoints": {},
            "processed_outage_ack_ids": [],
            "last_local_loss_epoch_s": None,
            "acknowledged_loss_manifests": [],
            "seen_shutdown_files": [],
            "spec_previous_hashes": {},
            "last_spec_day": None,
            "last_rtt_epoch_s": None,
            "last_chrony_epoch_s": None,
            "last_collector_hc_epoch_s": None,
            "last_hourly_report_hour": None,
            "last_hourly_volume_assessment": None,
        }

    def _load_state(self) -> dict[str, Any]:
        base = self._default_state()
        if not self.state_path.is_file():
            return base
        try:
            loaded = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                base.update(loaded)
        except Exception:
            pass
        return base

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_name(f"{self.state_path.name}.tmp.{os.getpid()}")
        payload = json.dumps(self.state, indent=2, ensure_ascii=False, sort_keys=True)
        # Defense-in-depth: guarantee secrets never enter monitor_state.json
        payload = redact_secrets(payload, extra_secrets=self._all_secrets())
        tmp.write_text(payload + "\n", encoding="utf-8")
        os.replace(tmp, self.state_path)

    def _all_secrets(self) -> list[str]:
        return [
            s
            for s in (
                self.notifier.telegram_bot_token,
                self.notifier.telegram_chat_id,
                self.notifier.webhook_url,
                self.hc_collector_url,
                self.hc_archive_url,
            )
            if s
        ]

    def _update_alerts(
        self,
        *,
        observed: dict[str, dict[str, Any]],
        now_s: float,
    ) -> dict[str, Any]:
        active: dict[str, dict[str, Any]] = self.state["active_alerts"]
        sent_alerts: list[str] = []
        sent_recoveries: list[str] = []
        suppressed: list[str] = []
        notify_failures: list[str] = []

        # 1. Process newly firing or ongoing conditions
        for key, cond in sorted(observed.items()):
            sev = str(cond["severity"])
            summary = str(cond["summary"])
            details = dict(cond.get("details") or {})
            entry = active.get(key)

            if entry is None:
                entry = {
                    "severity": sev,
                    "summary": summary,
                    "details": details,
                    "first_seen_epoch_s": now_s,
                    "last_notified_epoch_s": None,
                    "last_attempt_epoch_s": None,
                    "suppressed_count": 0,
                    "notified_once": False,
                }
                active[key] = entry
                should_send = True
                is_reminder = False
            else:
                entry["severity"] = sev
                entry["summary"] = summary
                entry["details"] = details
                last_notif = entry.get("last_notified_epoch_s")
                last_attempt = entry.get("last_attempt_epoch_s")
                if not entry.get("notified_once"):
                    # Retry initial alert after short bounded gap (60s) if prior delivery failed
                    should_send = last_attempt is None or (now_s - float(last_attempt)) >= 60.0
                    is_reminder = False
                elif last_notif is not None and (now_s - float(last_notif)) >= self.alert_cooldown_seconds:
                    should_send = True
                    is_reminder = True
                else:
                    should_send = False
                    is_reminder = False

            if should_send:
                msg = format_alert_message(
                    alert_key=key,
                    severity=sev,
                    summary=summary,
                    details=details,
                    now_s=now_s,
                    suppressed_count=int(entry.get("suppressed_count", 0)),
                    is_reminder=is_reminder,
                )
                entry["last_attempt_epoch_s"] = now_s
                if self.allow_notify and self.notifier.configured:
                    ok = self.notifier.send_text(msg, severity=sev, title=key, details=details)
                elif self.allow_notify:
                    # Stderr fallback still logs once and participates in dedup / recovery tracking.
                    self.notifier.send_text(msg, severity=sev, title=key, details=details)
                    ok = True
                else:
                    ok = True

                if ok:
                    entry["last_notified_epoch_s"] = now_s
                    entry["notified_once"] = True
                    entry["suppressed_count"] = 0
                    sent_alerts.append(key)
                else:
                    notify_failures.append(key)
            else:
                entry["suppressed_count"] = int(entry.get("suppressed_count", 0)) + 1
                suppressed.append(key)

        # 2. Process recovered conditions
        for key in sorted(list(active.keys())):
            if key in observed:
                continue
            prev_entry = active.pop(key)
            if prev_entry.get("notified_once"):
                rec_msg = format_recovery_message(
                    alert_key=key,
                    summary=f"先前异常({prev_entry.get('summary', '')})已清除",
                    started_at_s=float(prev_entry.get("first_seen_epoch_s") or now_s),
                    now_s=now_s,
                    suppressed_count=int(prev_entry.get("suppressed_count", 0)),
                    last_loss_detected_at_s=self.state.get("last_local_loss_epoch_s")
                    if key == "local_loss_detected" else None,
                )
                if self.allow_notify and self.notifier.configured:
                    if self.notifier.send_text(rec_msg, severity="RECOVERY", title=key):
                        sent_recoveries.append(key)
                    else:
                        notify_failures.append(f"recovery:{key}")
                else:
                    if self.allow_notify:
                        self.notifier.send_text(rec_msg, severity="RECOVERY", title=key)
                    sent_recoveries.append(key)

        return {
            "sent_alerts": sent_alerts,
            "sent_recoveries": sent_recoveries,
            "suppressed": suppressed,
            "notify_failures": notify_failures,
        }

    def _consume_outage_ack_requests(self, *, now_s: float) -> tuple[list[Path], list[dict[str, Any]]]:
        """Drain pending `--ack-outage` request files idempotently inside the single-writer monitor.

        Persists consumed `request_id`s inside `self.state['processed_outage_ack_ids']` before
        `_save_state()` runs, so a crash between `_save_state()` and `req_path.unlink()` never
        re-applies a previously processed acknowledgement (e.g. clearing newly discovered outages)
        upon daemon restart; already-processed request files are simply unlinked on the next sweep.
        """
        if not self.ack_requests_dir.is_dir():
            return [], []

        ep_outages = self.state.setdefault("dual_outage_endpoints", {})
        if not isinstance(ep_outages, dict):
            ep_outages = {}
            self.state["dual_outage_endpoints"] = ep_outages

        processed_raw = self.state.get("processed_outage_ack_ids")
        processed_list = list(processed_raw) if isinstance(processed_raw, list) else []
        processed_set = set(str(x) for x in processed_list)

        consumed_paths: list[Path] = []
        applied_summaries: list[dict[str, Any]] = []
        for req_path in sorted(self.ack_requests_dir.glob("req_*.json")):
            try:
                raw = json.loads(req_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(raw, dict):
                continue

            req_id = str(raw.get("request_id") or f"file:{req_path.name}")
            if req_id in processed_set:
                # Already persisted in a prior committed sweep; queue only for cleanup without re-applying.
                consumed_paths.append(req_path)
                continue

            raw_target = raw.get("target")
            target = None if raw_target in (None, "", "ALL", "all") else str(raw_target)
            cleared: list[str] = []
            if target is None:
                cleared = sorted(ep_outages.keys())
                ep_outages.clear()
            elif target in ep_outages:
                ep_outages.pop(target, None)
                cleared = [target]

            processed_set.add(req_id)
            processed_list.append(req_id)

            ack_record = {
                "ts_utc": datetime.fromtimestamp(now_s, tz=timezone.utc).isoformat(),
                "request_id": req_id,
                "requested_at_utc": raw.get("requested_at_utc"),
                "target": target or "ALL",
                "cleared_endpoints": cleared,
                "remaining_endpoints": sorted(ep_outages.keys()),
            }
            self.state["last_outage_ack"] = ack_record
            consumed_paths.append(req_path)
            applied_summaries.append(ack_record)

        self.state["processed_outage_ack_ids"] = processed_list[-500:]
        return consumed_paths, applied_summaries

    def run_sweep(
        self,
        *,
        now_epoch_s: float | None = None,
        run_specs: bool | None = None,
        run_rtt: bool | None = None,
        run_chrony: bool | None = None,
        force_collector_hc: bool = False,
        force_hourly_report: bool = False,
    ) -> dict[str, Any]:
        """Run one complete non-blocking monitoring, alert, Healthchecks & report sweep."""
        now_s = time.time() if now_epoch_s is None else float(now_epoch_s)
        today = utc_date_str(int(now_s * 1e9))
        dt_now = datetime.fromtimestamp(now_s, tz=timezone.utc)
        hour_key = dt_now.strftime("%Y-%m-%dT%H")

        # Sample the UTC slot before external network calls in this sweep.
        last_c = self.state.get("last_chrony_epoch_s")
        chrony_interval = max(1, self.chrony_interval_seconds)
        do_chrony = run_chrony if run_chrony is not None else (
            last_c is None or int(now_s // chrony_interval) != int(float(last_c) // chrony_interval)
        )
        if do_chrony:
            self.last_chrony_row = sample_chrony_once(self.data_root, runner=self.chrony_runner)
            self.state["last_chrony_epoch_s"] = now_s

        # Spec fetch on first startup, UTC midnight, or explicit request
        observed: dict[str, dict[str, Any]] = {}
        spec_change_alerts: list[str] = []
        last_spec_day = self.state.get("last_spec_day")
        do_specs = run_specs if run_specs is not None else (last_spec_day != today)
        if do_specs:
            trigger = "startup" if last_spec_day is None else "utc_midnight"
            prev_hashes = self.state.get("spec_previous_hashes") or {}
            self.last_spec_res = fetch_specs_once(
                self.data_root,
                trigger,
                exchanges=self.enabled_exchanges,
                fetcher=self.spec_fetcher,
                previous_hashes=prev_hashes,
            )
            self.state["spec_previous_hashes"] = prev_hashes
            self.state["last_spec_day"] = today
            if self.last_spec_res.get("spec_change_isolation_flags"):
                # A hash of the *entire* REST body changes with benign/dynamic fields.
                # Preserve raw before/after snapshots and isolation flags for audit, but
                # do not announce a verified contract change without field-level proof.
                spec_change_alerts.append("raw_spec_response_changed_unverified")

        # 3. RTT probe
        last_r = self.state.get("last_rtt_epoch_s")
        do_rtt = run_rtt if run_rtt is not None else (
            last_r is None or (now_s - float(last_r)) >= self.rtt_interval_seconds
        )
        if do_rtt:
            self.last_rtt_rows = sample_rtt_once(
                self.data_root, exchanges=self.enabled_exchanges, prober=self.rtt_prober
            )
            self.state["last_rtt_epoch_s"] = now_s

        # 4. Disk check (fail-closed on probe failure)
        disk_probe_error: str | None = None
        try:
            disk_pct: float | None = float(self.disk_checker(self.data_root))
        except Exception as exc:
            disk_pct = None
            disk_probe_error = f"{type(exc).__name__}: {exc}"

        # 5. Service & incremental journal probes (fail-closed if service_probe fails/raises)
        try:
            collector_status = self.service_probe(self.collector_service_name)
        except Exception as exc:
            collector_status = {
                "available": False,
                "unit": self.collector_service_name,
                "error": f"{type(exc).__name__}: {exc}",
            }
        try:
            archive_status = self.service_probe(self.archive_service_name)
        except Exception as exc:
            archive_status = {
                "available": False,
                "unit": self.archive_service_name,
                "error": f"{type(exc).__name__}: {exc}",
            }

        try:
            journal_lines, new_cursor = self.journal_reader(
                self.collector_service_name, self.state.get("journal_cursor")
            )
        except Exception:
            journal_lines, new_cursor = [], self.state.get("journal_cursor")
        if new_cursor != self.state.get("journal_cursor"):
            self.state["journal_cursor"] = new_cursor
        log_events = inspect_collector_logs(journal_lines)

        # If a live engine start happened cleanly or restarted in journal, record boot_id & trigger spec snapshot on reconnect
        if log_events["starts"]:
            latest_boot = log_events["starts"][-1]
            prev_boot = self.state.get("last_collector_boot_id")
            if prev_boot is not None and prev_boot != latest_boot and not do_specs:
                prev_hashes = self.state.get("spec_previous_hashes") or {}
                self.last_spec_res = fetch_specs_once(
                    self.data_root,
                    "collector_restart",
                    exchanges=self.enabled_exchanges,
                    fetcher=self.spec_fetcher,
                    previous_hashes=prev_hashes,
                )
                self.state["spec_previous_hashes"] = prev_hashes
                if self.last_spec_res.get("spec_change_isolation_flags"):
                    # Record raw hash changes without mislabeling them verified spec changes.
                    spec_change_alerts.append("raw_spec_response_changed_unverified")
            self.state["last_collector_boot_id"] = latest_boot

        # 6. Filesystem metadata inspect (.partial stat + .manifest.json + shutdown_*.json; ZERO .tsv.zst decompression!)
        data_tree = inspect_data_tree(
            self.data_root,
            self.enabled_exchanges,
            now_s=now_s,
            stale_seconds=float(self.collector_stale_seconds),
            manifest_cache=self.manifest_cache,
            service_uptime_seconds=collector_status.get("uptime_seconds"),
        )
        volume_now = assess_hourly_data_volume(
            data_tree, now_s=now_s, report_minute=self.hourly_report_minute_utc
        )
        if volume_now["ready"]:
            self.state["last_hourly_volume_assessment"] = volume_now
            data_tree["hourly_volume_assessment"] = volume_now
        else:
            # Keep the last evaluated complete-hour condition until the next
            # :10 window; do not briefly report healthy at every UTC hour boundary.
            data_tree["hourly_volume_assessment"] = (
                self.state.get("last_hourly_volume_assessment") or volume_now
            )
        seen_shutdowns = set(self.state.get("seen_shutdown_files") or ())
        new_shutdowns = inspect_shutdown_audits(self.data_root, seen_shutdowns)
        self.state["seen_shutdown_files"] = sorted(seen_shutdowns)[-200:]

        # 7. Read-only P1 Archive SQLite ledger inspect
        ledger_info = inspect_archive_ledger(
            self.data_root,
            now_s=now_s,
            archive_stale_minutes=self.archive_stale_minutes,
        )

        # --- Evaluate alert conditions ---
        collector_available = bool(collector_status.get("available"))
        collector_running_verified = bool(
            collector_available
            and collector_status.get("active_state") == "active"
            and collector_status.get("sub_state") not in ("failed", "dead", "auto-restart", "stop-sigterm", "exited")
        )

        # A. Collector probe unavailable vs stopped (fail-closed: probe error MUST trigger alert & block HC ok!)
        if not collector_available:
            observed["collector_probe_failed"] = {
                "severity": "CRITICAL",
                "summary": f"无法探测采集服务状态 ({collector_status.get('error') or 'systemctl_unavailable'})",
                "details": collector_status,
            }
        elif not collector_running_verified:
            observed["collector_stopped"] = {
                "severity": "CRITICAL",
                "summary": f"采集服务未运行 ({collector_status.get('active_state')}/{collector_status.get('sub_state')})",
                "details": collector_status,
            }

        # B. Collector abnormal restart
        restart_reasons: list[str] = []
        if collector_available:
            cur_restarts = int(collector_status.get("n_restarts") or 0)
            prev_restarts = self.state.get("last_collector_n_restarts")
            if prev_restarts is not None and cur_restarts > int(prev_restarts):
                restart_reasons.append(f"NRestarts {prev_restarts}->{cur_restarts}")
            self.state["last_collector_n_restarts"] = cur_restarts
            res_str = str(collector_status.get("result") or "")
            if res_str and res_str != "success":
                restart_reasons.append(f"systemd result={res_str}")
        if log_events["unclean_boot_transitions"] > 0:
            restart_reasons.append(f"unclean_boot_transitions={log_events['unclean_boot_transitions']}")
        for sd in new_shutdowns:
            if int(sd.get("failed_count") or 0) > 0:
                restart_reasons.append(f"shutdown_seal_failed={sd.get('_file_name')}")
            elif sd.get("shutdown_reason") not in ("SIGINT", "SIGTERM", "duration_elapsed"):
                restart_reasons.append(f"unexpected_shutdown_reason={sd.get('shutdown_reason')}")

        if restart_reasons:
            self.state["last_restart_event_epoch_s"] = now_s
        last_restart_s = self.state.get("last_restart_event_epoch_s")
        if restart_reasons or (
            last_restart_s is not None
            and "collector_abnormal_restart" in self.state.get("active_alerts", {})
            and (
                (now_s - float(last_restart_s)) < self.restart_recovery_seconds
                or "collector_stopped" in observed
            )
        ):
            prev_detail = self.state.get("active_alerts", {}).get("collector_abnormal_restart", {}).get("details", {})
            reasons_out = restart_reasons or prev_detail.get("reasons", ["cooldown_stabilizing"])
            observed["collector_abnormal_restart"] = {
                "severity": "CRITICAL",
                "summary": f"采集器异常重启/关闭: {', '.join(reasons_out)}",
                "details": {"reasons": reasons_out, "n_restarts": collector_status.get("n_restarts")},
            }

        # C. Data stale (long time without .partial / manifest update)
        if data_tree["stale_exchanges"]:
            stale_details = {
                ex: data_tree["per_exchange"][ex].get("stale_reason")
                for ex in data_tree["stale_exchanges"]
            }
            observed["collector_data_stale"] = {
                "severity": "CRITICAL",
                "summary": f"采集数据长期未更新: {', '.join(data_tree['stale_exchanges'])}",
                "details": stale_details,
            }

        # C2. META or .partial bytes must never masquerade as healthy market DATA.
        volume_info = data_tree.get("hourly_volume_assessment") or {}
        volume_bad = volume_info.get("affected") or {}
        if volume_bad:
            observed["collector_data_volume_abnormal"] = {
                "severity": "CRITICAL",
                "summary": f"上一完整小时 {len(volume_bad)} 家交易所 DATA 为零或显著低于参考量",
                "details": {
                    "hour_key": volume_info.get("hour_key"),
                    "affected": volume_bad,
                },
            }

        stream_activity = None
        if self.require_stream_activity:
            stream_activity = inspect_stream_activity(
                self.data_root, self.expected_market_streams, now_s=now_s,
                stale_seconds=float(self.collector_stale_seconds),
            )
            if not stream_activity["healthy"]:
                observed["collector_stream_stale"] = {
                    "severity": "CRITICAL",
                    "summary": "无法确认全部行情频道持续写入",
                    "details": stream_activity,
                }

        # D. Dual A/B outage > 5s (strict conservative endpoint tracking; NEVER clears from
        #    `Connected` handshake or sibling-shared `.partial` disk activity!)
        #    Single-writer ack order: drain queued `--ack-outage` request files FIRST, then process
        #    any new outage events from this sweep's journal batch so concurrent faults are never wiped.
        consumed_ack_paths, applied_outage_acks = self._consume_outage_ack_requests(now_s=now_s)
        ep_outages: dict[str, dict[str, Any]] = self.state.setdefault("dual_outage_endpoints", {})
        if not isinstance(ep_outages, dict):
            ep_outages = {}
            self.state["dual_outage_endpoints"] = ep_outages

        timeline: dict[str, dict[str, Any]] = log_events.get("endpoint_timeline") or {}
        for ep, ev in timeline.items():
            if ev.get("saw_outage"):
                self.state["last_dual_outage_epoch_s"] = now_s
                ep_outages[ep] = {
                    "outage_at_s": now_s,
                    "explicit_recovered_at_s": None,
                    "sample_message": (ev.get("outage_messages") or [""])[-1],
                    "diagnostics": {
                        "last_ws_connected_at_s": None,
                        "ws_connects_since_outage": int(ev.get("ws_connect_count") or 0),
                        "ws_errors_since_outage": 0,
                    },
                }
                continue
            if ep not in ep_outages:
                continue
            ep_rec = ep_outages[ep]
            diag = ep_rec.setdefault(
                "diagnostics",
                {"last_ws_connected_at_s": None, "ws_connects_since_outage": 0, "ws_errors_since_outage": 0},
            )
            if ev.get("ws_connect_count"):
                diag["ws_connects_since_outage"] = int(diag.get("ws_connects_since_outage") or 0) + int(
                    ev["ws_connect_count"]
                )
            if ev.get("explicit_recovered"):
                ep_rec["explicit_recovered_at_s"] = now_s
            elif ev.get("saw_error_after_connect"):
                ep_rec["explicit_recovered_at_s"] = None
                diag["last_ws_connected_at_s"] = None
                diag["ws_errors_since_outage"] = int(diag.get("ws_errors_since_outage") or 0) + 1
            elif ev.get("connected_clean"):
                # Recorded ONLY as diagnostic telemetry: WS handshake before subscribe NEVER proves
                # subscription success or live market data flow, so it NEVER clears `ep_outages`!
                diag["last_ws_connected_at_s"] = now_s

        # Auto-recovery ONLY if `dual_outage_recovery_mode == "explicit_gap_end"` AND logs contain
        # an explicit endpoint-level GAP_END event cleanly after the outage while collector is active.
        # Under the default `"conservative"` mode (matching current Rust binaries whose GAP_END is only
        # written inside `.tsv.zst`), zero automatic clearing occurs and operator clearance requires
        # `--ack-outage` after verification.
        if self.dual_outage_recovery_mode == "explicit_gap_end":
            for ep in list(ep_outages.keys()):
                ep_rec = ep_outages[ep]
                if ep_rec.get("explicit_recovered_at_s") is not None and collector_running_verified:
                    ep_outages.pop(ep, None)

        if ep_outages:
            pending_eps = sorted(ep_outages.keys())
            mode_note = (
                "保守模式(需人工确认恢复后执行 --ack-outage)"
                if self.dual_outage_recovery_mode == "conservative"
                else "需明确端点 GAP_END 恢复事件"
            )
            observed["dual_ab_outage_gt_5s"] = {
                "severity": "CRITICAL",
                "summary": f"检测到 A/B 双线中断超过 5 秒 (待恢复端点: {', '.join(pending_eps)}; {mode_note})",
                "details": {
                    "recovery_mode": self.dual_outage_recovery_mode,
                    "pending_endpoints": pending_eps,
                    "endpoint_states": ep_outages,
                    "recent_events": log_events["dual_outages"][-5:],
                },
            }

        # E. LOCAL_LOSS & disk write errors
        ack_loss = set(self.state.get("acknowledged_loss_manifests") or ())
        new_loss_manifests: list[dict[str, Any]] = []
        for m in data_tree["manifests_with_local_loss"]:
            key_m = f"{m['rel_path']}:{m['local_loss_lines']}"
            if key_m not in ack_loss:
                ack_loss.add(key_m)
                new_loss_manifests.append(m)
        self.state["acknowledged_loss_manifests"] = sorted(ack_loss)[-500:]

        for sd in new_shutdowns:
            for item in sd.get("sealed") or ():
                if int(item.get("local_loss_lines") or 0) > 0:
                    new_loss_manifests.append(
                        {"exchange": item.get("exchange"), "local_loss_lines": item.get("local_loss_lines"), "from": sd.get("_file_name")}
                    )

        has_new_loss = bool(log_events["local_losses"] or log_events["write_or_seal_errors"] or new_loss_manifests)
        if has_new_loss:
            self.state["last_local_loss_epoch_s"] = now_s
        last_loss_s = self.state.get("last_local_loss_epoch_s")
        if has_new_loss or (
            last_loss_s is not None
            and "local_loss_detected" in self.state.get("active_alerts", {})
            and (now_s - float(last_loss_s)) < self.loss_recovery_seconds
        ):
            observed["local_loss_detected"] = {
                "severity": "CRITICAL",
                "summary": "检测到 LOCAL_LOSS 或本地写盘异常",
                "details": {
                    "journal_local_loss": log_events["local_losses"][-5:],
                    "write_errors": log_events["write_or_seal_errors"][-5:],
                    "manifest_local_loss": new_loss_manifests[-5:],
                },
            }

        # F. Disk >= 70% or disk probe failed
        if disk_pct is None:
            observed["disk_probe_failed"] = {
                "severity": "CRITICAL",
                "summary": f"无法获取磁盘使用率 ({disk_probe_error})",
                "details": {"error": disk_probe_error},
            }
        elif disk_pct >= self.disk_thresh:
            observed["disk_usage_high"] = {
                "severity": "CRITICAL",
                "summary": f"磁盘使用率 {disk_pct:.1f}% 超过阈值 {self.disk_thresh:.0f}%",
                "details": {"disk_pct": round(disk_pct, 2), "threshold": self.disk_thresh},
            }

        # G. Clock out of sync or unavailable
        if self.last_chrony_row is None or not self.last_chrony_row.get("certified_le_1ms"):
            err_ms_val = self.last_chrony_row.get("estimated_error_ms") if self.last_chrony_row else None
            synced_val = self.last_chrony_row.get("synced") if self.last_chrony_row else False
            observed["clock_unsynced"] = {
                "severity": "WARNING",
                "summary": f"Chrony 时钟未通过 <=1ms 认证 (synced={synced_val}, err_ms={err_ms_val})",
                "details": self.last_chrony_row or {"error": "chrony_row_missing"},
            }

        # H. Archive upload / verification failure / stale SEALED / archive service failure
        archive_unit_failed = bool(archive_status.get("available") and archive_status.get("active_state") == "failed")
        if (
            ledger_info["errored_partitions"]
            or ledger_info["stale_sealed_partitions"]
            or ledger_info["read_error"]
            or archive_unit_failed
        ):
            observed["archive_upload_or_verify_failed"] = {
                "severity": "CRITICAL",
                "summary": (
                    f"归档上传或远端校验异常 (errors={len(ledger_info['errored_partitions'])}, "
                    f"stale_sealed={len(ledger_info['stale_sealed_partitions'])}, unit_failed={archive_unit_failed})"
                ),
                "details": {
                    "errored": ledger_info["errored_partitions"][:5],
                    "stale_sealed": ledger_info["stale_sealed_partitions"][:5],
                    "read_error": ledger_info["read_error"],
                    "archive_service_result": archive_status.get("result") if archive_unit_failed else None,
                },
            }

        # I. Archive ledger conflict
        if ledger_info["conflicted_partitions"]:
            observed["archive_ledger_conflict"] = {
                "severity": "CRITICAL",
                "summary": f"归档账本发现远程哈希/身份冲突 ({len(ledger_info['conflicted_partitions'])} 个分区)",
                "details": {"conflicts": ledger_info["conflicted_partitions"][:5]},
            }

        # J. Optional P5 Lightsail archive SOCKS5 tunnel check (strictly inactive while proxy mode == off)
        try:
            from lightsail_proxy import parse_proxy_url, probe_socks5_proxy, resolve_rclone_proxy_config

            p_mode, p_url = resolve_rclone_proxy_config()
            if p_mode in ("required", "fallback_direct") and p_url:
                _, p_host, p_port = parse_proxy_url(p_url)
                p_res = (self.proxy_probe or probe_socks5_proxy)(p_host, p_port)
                if not p_res.get("reachable"):
                    observed["lightsail_proxy_unavailable"] = {
                        "severity": "CRITICAL" if p_mode == "required" else "WARNING",
                        "summary": f"Lightsail 归档 SOCKS5 隧道不可达 ({p_url}; mode={p_mode})",
                        "details": {"proxy_url": p_url, "mode": p_mode, "probe": p_res},
                    }
        except Exception as exc:
            if os.environ.get("TOKYO_L2_ARCHIVE_PROXY_MODE", "off").strip().lower() not in ("", "off"):
                observed["lightsail_proxy_unavailable"] = {
                    "severity": "CRITICAL",
                    "summary": f"Lightsail 归档代理配置或探测失败 ({type(exc).__name__})",
                    "details": {"error": f"{type(exc).__name__}: {exc}"},
                }

        # --- Apply alert dedup & recovery notifications ---
        dispatch_res = self._update_alerts(observed=observed, now_s=now_s)

        # --- Collector Healthchecks low-frequency periodic ping (default 5 min) ---
        # STRICT POSITIVE PROOF REQUIRED: only report healthy if collector is verified running,
        # all enabled exchanges are verified fresh, clock is <=1ms certified, disk is under threshold,
        # and no blocking collector/probe alerts are active.
        last_hc_s = self.state.get("last_collector_hc_epoch_s")
        due_hc = force_collector_hc or (
            last_hc_s is None or (now_s - float(last_hc_s)) >= self.collector_hc_interval_seconds
        )
        hc_result: dict[str, Any] = {"due": due_hc, "sent": False, "healthy": None, "reason": ""}
        if due_hc:
            collector_unhealthy_keys = [
                k
                for k in (
                    "collector_stopped",
                    "collector_probe_failed",
                    "collector_abnormal_restart",
                    "collector_data_stale",
                    "collector_data_volume_abnormal",
                    "collector_stream_stale",
                    "dual_ab_outage_gt_5s",
                    "local_loss_detected",
                    "disk_usage_high",
                    "disk_probe_failed",
                    "clock_unsynced",
                )
                if k in self.state["active_alerts"]
            ]
            data_fresh_verified = bool(
                self.enabled_exchanges
                and not data_tree["stale_exchanges"]
                and all(
                    info.get("last_activity_age_s") is not None and not info.get("is_stale")
                    for info in data_tree["per_exchange"].values()
                )
            )
            clock_verified = bool(
                self.last_chrony_row is not None and self.last_chrony_row.get("certified_le_1ms") is True
            )
            disk_verified = bool(disk_pct is not None and disk_pct < self.disk_thresh)

            healthy = bool(
                collector_running_verified
                and data_fresh_verified
                and clock_verified
                and disk_verified
                and len(collector_unhealthy_keys) == 0
            )
            if healthy:
                reason = f"ok active_exchanges={len(self.enabled_exchanges)} disk={disk_pct:.1f}%"
            else:
                fail_parts: list[str] = []
                for k in collector_unhealthy_keys:
                    fail_parts.append(f"{k}:{self.state['active_alerts'][k].get('summary', '')}")
                if not collector_running_verified and "collector_stopped" not in collector_unhealthy_keys and "collector_probe_failed" not in collector_unhealthy_keys:
                    fail_parts.append("collector_running_unverified")
                if not data_fresh_verified and "collector_data_stale" not in collector_unhealthy_keys:
                    fail_parts.append("data_freshness_unverified")
                if not clock_verified and "clock_unsynced" not in collector_unhealthy_keys:
                    fail_parts.append("clock_certification_unverified")
                if not disk_verified and "disk_usage_high" not in collector_unhealthy_keys and "disk_probe_failed" not in collector_unhealthy_keys:
                    fail_parts.append("disk_usage_unverified")
                reason = "; ".join(fail_parts) or "collector_health_unverified"

            hc_result["healthy"] = healthy
            hc_result["reason"] = redact_secrets(reason, extra_secrets=self._all_secrets())
            if self.allow_notify and self.hc_collector_url:
                hc_result["sent"] = ping_healthcheck(
                    self.hc_collector_url,
                    ok=healthy,
                    reason=hc_result["reason"],
                    http_sender=self.hc_sender,
                    sleeper=self.sleeper,
                    extra_secrets=self._all_secrets(),
                )
            self.state["last_collector_hc_epoch_s"] = now_s

        # --- Hourly Telegram Battle Report (once per UTC hour after hourly archive window) ---
        last_hr_sent = self.state.get("last_hourly_report_hour")
        due_hourly = force_hourly_report or (
            self.hourly_report_enabled
            and last_hr_sent != hour_key
            and self.hourly_report_minute_utc <= dt_now.minute < min(
                60, self.hourly_report_minute_utc + 5
            )
        )
        hourly_res: dict[str, Any] = {"due": due_hourly, "sent": False, "hour_key": hour_key, "text": None}
        if due_hourly:
            report_text = format_hourly_report(
                now_s=now_s,
                enabled_exchanges=self.enabled_exchanges,
                collector_status=collector_status,
                archive_status=archive_status,
                data_tree=data_tree,
                ledger_info=ledger_info,
                disk_pct=disk_pct,
                disk_thresh=self.disk_thresh,
                chrony_row=self.last_chrony_row,
                active_alerts=self.state["active_alerts"],
            )
            hourly_res["text"] = report_text
            if self.allow_notify and self.notifier.configured:
                sent_hr = self.notifier.send_text(
                    report_text, severity="REPORT", title=f"Hourly Battle Report {hour_key}"
                )
                hourly_res["sent"] = sent_hr
                if sent_hr:
                    self.state["last_hourly_report_hour"] = hour_key
            elif not self.notifier.configured:
                self.state["last_hourly_report_hour"] = hour_key

        self._save_state()
        for req_path in consumed_ack_paths:
            try:
                req_path.unlink(missing_ok=True)
            except OSError:
                pass
        return {
            "status": "ok",
            "utc": dt_now.isoformat(),
            "disk_pct": round(disk_pct, 2) if disk_pct is not None else None,
            "collector_service": collector_status,
            "archive_service": archive_status,
            "chrony": self.last_chrony_row,
            "data_tree": data_tree,
            "archive_ledger": ledger_info,
            "stream_activity": stream_activity,
            "active_alerts": list(sorted(self.state["active_alerts"].keys())),
            "outage_acks_applied": applied_outage_acks,
            "alert_dispatch": dispatch_res,
            "spec_change_alerts": spec_change_alerts,
            "collector_healthcheck": hc_result,
            "hourly_report": hourly_res,
            "specs": self.last_spec_res,
        }


def notify_archive_healthcheck(
    result: dict[str, Any] | None,
    *,
    error: BaseException | None = None,
    base_url: str | None = None,
    http_sender: HttpSenderFn | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> bool:
    """Send fail-closed hourly archive Healthchecks ping (`/` on ok, `/fail` with concise reason)."""
    url = base_url if base_url is not None else os.environ.get("TOKYO_L2_HEALTHCHECKS_ARCHIVE_URL")
    if not url or not str(url).strip():
        return False

    if error is not None or result is None:
        err_str = f"{type(error).__name__}: {error}" if error is not None else "missing_archive_result"
        return ping_healthcheck(
            url,
            ok=False,
            reason=f"archive_exception: {err_str}",
            http_sender=http_sender,
            sleeper=sleeper,
        )

    proxy_meta = result.get("proxy") if isinstance(result.get("proxy"), dict) else None
    proxy_suffix = ""
    if proxy_meta and proxy_meta.get("used_route") in ("lightsail_proxy", "direct_fallback"):
        proxy_suffix = f" proxy={proxy_meta['used_route']}"

    if result.get("status") == "ok":
        reason = (
            f"ok verified={result.get('verified_count', 0)} "
            f"pruned={len(result.get('pruned_local_files', []))} "
            f"snap={bool(result.get('snapshot_remote_path'))}{proxy_suffix}"
        )
        return ping_healthcheck(
            url,
            ok=True,
            reason=reason,
            http_sender=http_sender,
            sleeper=sleeper,
        )

    conflicts = result.get("conflicts") or []
    failed = result.get("failed_items") or []
    head_items = "; ".join(str(x) for x in failed[:2])
    reason = f"archive_blocked conflicts={len(conflicts)} failed={len(failed)}{proxy_suffix}: {head_items}"
    return ping_healthcheck(
        url,
        ok=False,
        reason=reason,
        http_sender=http_sender,
        sleeper=sleeper,
    )


def run_daemon(
    config_path: Path,
    once: bool = False,
    *,
    send_hourly_report: bool = False,
    allow_notify: bool = True,
) -> int:
    monitor = CollectorMonitor(config_path, allow_notify=allow_notify)
    first = monitor.run_sweep(
        run_specs=True,
        run_rtt=True,
        run_chrony=True,
        force_collector_hc=True,
        force_hourly_report=send_hourly_report,
    )
    if once:
        safe_out = redact_secrets(
            json.dumps(first, indent=2, ensure_ascii=False, default=str),
            extra_secrets=monitor._all_secrets(),
        )
        print(safe_out)
        return 0

    poll_sec = max(5, int(monitor.health_poll_seconds))
    while True:
        time.sleep(poll_sec - time.time() % poll_sec)
        try:
            monitor.run_sweep()
        except Exception as exc:
            err = redact_secrets(f"{type(exc).__name__}: {exc}", extra_secrets=monitor._all_secrets())
            print(f"[META_DAEMON_SWEEP_ERROR] {err}", file=sys.stderr, flush=True)


def main() -> int:
    p = argparse.ArgumentParser(description="Standalone metadata, RTT, chrony, Telegram & Healthchecks daemon")
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--once", action="store_true", help="Run a single metadata+health sweep and exit")
    p.add_argument(
        "--send-hourly-report",
        action="store_true",
        help="Force sending the hourly Telegram battle report during --once",
    )
    p.add_argument(
        "--no-notify",
        action="store_true",
        help="Disable outbound Telegram/Webhook/Healthchecks HTTP requests during dry-run",
    )
    p.add_argument(
        "--ack-outage",
        nargs="?",
        const="ALL",
        default=None,
        help=(
            "Submit an acknowledgement request to clear the conservative dual A/B outage lock "
            "(optionally specify endpoint name, or ALL). NOTE: does NOT verify live market data flow; "
            "confirm live market frames first."
        ),
    )
    args = p.parse_args()
    if args.ack_outage is not None:
        cfg_obj = json.loads(args.config.read_text(encoding="utf-8"))
        data_root = Path(cfg_obj["deployment"]["data_root"])
        res = submit_outage_ack_request(data_root, None if args.ack_outage == "ALL" else args.ack_outage)
        print(json.dumps(res, indent=2, ensure_ascii=False))
        return 0
    return run_daemon(
        args.config,
        once=args.once,
        send_hourly_report=args.send_hourly_report,
        allow_notify=not args.no_notify,
    )


if __name__ == "__main__":
    raise SystemExit(main())
