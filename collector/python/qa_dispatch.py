"""Lightweight independent GitHub Actions workflow_dispatch notifier for verified Tokyo L2 windows.

Reads durable deduplicated outbox records from `meta/archive_ledger.sqlite` (and performs
crash-recovery discovery from synced `meta/archive_manifest_index.json`), then triggers
`tokyo-l2-incremental-qa.yml` via GitHub's REST `workflow_dispatch` endpoint.

Isolation guarantees:
  - Runs independently from `tokyo-l2-collector.service` and `tokyo-l2-archive.service`.
  - Only this notifier reads the fine-grained GitHub PAT (`Actions: write` on this repo).
  - GitHub timeouts, rate limits, 5xx errors, or token failures keep records PENDING with
    visible error state and bounded exponential backoff without blocking collection or archive.
  - Separately tracks `index_synced_at_utc`, `api_accepted_at_utc`, `workflow_run_started_at_utc`,
    and `qa_completed_at_utc`.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import http.client
import json
import os
from pathlib import Path
import socket
import ssl
import sys
import time
from typing import Any, Callable
from urllib.parse import urlparse

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from archive_sync import (  # noqa: E402
    LEDGER_DB_FILENAME,
    ArchiveLedger,
    discover_and_enqueue_qa_dispatches,
)

DEFAULT_GITHUB_API_BASE = "https://api.github.com"
DEFAULT_GITHUB_REPO = "Huhabubu/mm"
DEFAULT_WORKFLOW_FILE = "tokyo-l2-incremental-qa.yml"
DEFAULT_WORKFLOW_REF = "main"
DEFAULT_TOKEN_ENV = "TOKYO_L2_GITHUB_QA_DISPATCH_TOKEN"
DEFAULT_CONNECT_TIMEOUT_S = 5.0
DEFAULT_TOTAL_TIMEOUT_S = 15.0
DEFAULT_BASE_BACKOFF_S = 30.0
DEFAULT_MAX_BACKOFF_S = 900.0
DEFAULT_AUTH_BACKOFF_S = 300.0
DEFAULT_MAX_DISPATCHES_PER_RUN = 2


@dataclass(frozen=True)
class HttpDispatchResponse:
    status_code: int
    headers: dict[str, str]
    body: str


HttpSenderFn = Callable[
    [str, dict[str, str], bytes, float, float],
    HttpDispatchResponse,
]


def _iso_utc(epoch_s: float) -> str:
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sanitize_error(message: str, token: str | None = None) -> str:
    text = str(message or "").replace("\r", " ").replace("\n", " ").strip()
    if token:
        text = text.replace(token, "[REDACTED]")
    return text[:500]


class DispatchLockBusyError(RuntimeError):
    """Raised when another qa-dispatch process already holds the non-blocking lock."""


class _DispatchFileLock:
    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._fh: Any = None

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fh = self.lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            fh.close()
            raise DispatchLockBusyError(f"QA dispatcher lock busy: {self.lock_path}") from exc
        fh.seek(0)
        fh.truncate()
        fh.write(f"pid={os.getpid()}\n")
        fh.flush()
        self._fh = fh

    def release(self) -> None:
        if self._fh is not None:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            finally:
                self._fh.close()
                self._fh = None


def default_http_sender(
    url: str,
    headers: dict[str, str],
    body: bytes,
    connect_timeout_s: float,
    total_timeout_s: float,
) -> HttpDispatchResponse:
    """Send an HTTP(S) POST with separate connect timeout and overall wall-clock deadline."""
    parsed = urlparse(url)
    if parsed.scheme not in ("https", "http") or not parsed.hostname:
        raise ValueError(f"Unsupported GitHub API URL: {url}")

    connect_t = max(0.1, float(connect_timeout_s))
    total_t = max(connect_t, float(total_timeout_s))
    deadline = time.monotonic() + total_t
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    conn: http.client.HTTPConnection
    if parsed.scheme == "https":
        ctx = ssl.create_default_context()
        conn = http.client.HTTPSConnection(parsed.hostname, port, timeout=connect_t, context=ctx)
    else:
        conn = http.client.HTTPConnection(parsed.hostname, port, timeout=connect_t)

    try:
        conn.connect()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Total timeout ({total_t}s) exceeded after connect")
        if conn.sock is not None:
            conn.sock.settimeout(remaining)
        conn.request("POST", path, body=body, headers=headers)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Total timeout ({total_t}s) exceeded before response")
        if conn.sock is not None:
            conn.sock.settimeout(remaining)
        resp = conn.getresponse()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Total timeout ({total_t}s) exceeded before body read")
        if conn.sock is not None:
            conn.sock.settimeout(remaining)
        raw_body = resp.read(65536)
        hdrs = {k.lower(): v for k, v in resp.getheaders()}
        return HttpDispatchResponse(
            status_code=int(resp.status),
            headers=hdrs,
            body=raw_body.decode("utf-8", errors="replace"),
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _compute_retry_delay_s(
    *,
    attempt_index: int,
    base_backoff_s: float,
    max_backoff_s: float,
    response: HttpDispatchResponse | None = None,
    now_s: float,
    min_delay_s: float = 0.0,
) -> float:
    exp_delay = float(base_backoff_s) * (2 ** max(0, int(attempt_index)))
    delay = max(float(min_delay_s), min(float(max_backoff_s), exp_delay))
    if response is not None:
        retry_after = response.headers.get("retry-after")
        if retry_after:
            try:
                delay = max(delay, min(float(max_backoff_s), float(retry_after)))
            except ValueError:
                pass
        rl_reset = response.headers.get("x-ratelimit-reset")
        if rl_reset and response.headers.get("x-ratelimit-remaining") == "0":
            try:
                wait_reset = max(1.0, float(rl_reset) - now_s)
                delay = max(delay, min(float(max_backoff_s), wait_reset))
            except ValueError:
                pass
    return delay


def _classify_http_failure(
    response: HttpDispatchResponse,
    *,
    token: str | None = None,
) -> tuple[str, str, bool]:
    """Return `(error_code, sanitized_message, is_auth_error)`."""
    code = response.status_code
    body_msg = response.body
    try:
        parsed = json.loads(response.body) if response.body.strip() else {}
        if isinstance(parsed, dict) and parsed.get("message"):
            body_msg = str(parsed["message"])
    except ValueError:
        pass
    clean_msg = _sanitize_error(body_msg or f"HTTP {code}", token=token)

    is_rate_limit = (
        code == 429
        or (
            code == 403
            and (
                response.headers.get("x-ratelimit-remaining") == "0"
                or "retry-after" in response.headers
                or "rate limit" in clean_msg.lower()
            )
        )
    )
    if is_rate_limit:
        return "github_rate_limited", f"HTTP {code}: {clean_msg}", False
    if code == 401:
        return "github_token_invalid", f"HTTP 401: {clean_msg}", True
    if code == 403:
        return "github_token_forbidden", f"HTTP 403: {clean_msg}", True
    if 500 <= code <= 599:
        return f"github_http_{code}", f"HTTP {code}: {clean_msg}", False
    return f"github_request_rejected_{code}", f"HTTP {code}: {clean_msg}", False


def build_workflow_dispatch_payload(
    record: dict[str, Any],
    *,
    ref: str = DEFAULT_WORKFLOW_REF,
    extra_inputs: dict[str, str] | None = None,
) -> dict[str, Any]:
    inputs: dict[str, str] = {
        "run_id": str(record["run_id"]),
        "target_window_utc": str(record["target_window_utc"]),
        "archive_revision": str(record["archive_revision"]),
    }
    if extra_inputs:
        for k, v in extra_inputs.items():
            if v is not None and str(v) != "":
                inputs[str(k)] = str(v)
    return {
        "ref": str(ref),
        "inputs": inputs,
    }


def dispatch_pending_qa(
    sealed_root: Path,
    *,
    repo: str | None = None,
    workflow_file: str | None = None,
    ref: str | None = None,
    token: str | None = None,
    token_env: str = DEFAULT_TOKEN_ENV,
    api_base: str = DEFAULT_GITHUB_API_BASE,
    run_id_filter: str | None = None,
    partition_minutes_filter: int | None = None,
    max_dispatches: int = DEFAULT_MAX_DISPATCHES_PER_RUN,
    max_age_hours: float | None = None,
    connect_timeout_s: float = DEFAULT_CONNECT_TIMEOUT_S,
    total_timeout_s: float = DEFAULT_TOTAL_TIMEOUT_S,
    base_backoff_s: float = DEFAULT_BASE_BACKOFF_S,
    max_backoff_s: float = DEFAULT_MAX_BACKOFF_S,
    auth_backoff_s: float = DEFAULT_AUTH_BACKOFF_S,
    extra_inputs: dict[str, str] | None = None,
    now_epoch_s: float | None = None,
    http_sender: HttpSenderFn | None = None,
    recover_from_index: bool = True,
    require_token: bool = False,
) -> dict[str, Any]:
    """Discover any missed COMPLETE windows from synced index and dispatch due outbox items."""
    now_s = time.time() if now_epoch_s is None else float(now_epoch_s)
    now_iso = _iso_utc(now_s)
    eff_repo = (repo or os.environ.get("TOKYO_L2_GITHUB_REPO") or DEFAULT_GITHUB_REPO).strip()
    eff_workflow = (workflow_file or os.environ.get("TOKYO_L2_GITHUB_QA_WORKFLOW") or DEFAULT_WORKFLOW_FILE).strip()
    eff_ref = (ref or os.environ.get("TOKYO_L2_GITHUB_QA_REF") or DEFAULT_WORKFLOW_REF).strip()
    eff_token = token if token is not None else os.environ.get(token_env, "")
    eff_token = str(eff_token or "").strip()
    sender = http_sender or default_http_sender

    lock = _DispatchFileLock(sealed_root / "meta" / ".qa_dispatch.lock")
    try:
        lock.acquire()
    except DispatchLockBusyError as exc:
        return {
            "status": "busy",
            "phase": "qa_dispatch",
            "checked_at_utc": now_iso,
            "error": str(exc),
        }

    try:
        ledger_path = sealed_root / LEDGER_DB_FILENAME
        with ArchiveLedger(ledger_path) as ledger:
            recovery: dict[str, Any] = {
                "index_synced": False,
                "enqueued": [],
                "already_queued": [],
                "skipped_not_complete": [],
            }
            if recover_from_index:
                recovery = discover_and_enqueue_qa_dispatches(
                    sealed_root,
                    ledger,
                    now_epoch_s=now_s,
                    run_id_filter=run_id_filter,
                    partition_minutes_filter=partition_minutes_filter,
                    discovery_source="index_recovery",
                    max_age_hours=max_age_hours,
                )

            all_pending = ledger.list_qa_dispatches(
                states=("PENDING",),
                run_id=run_id_filter,
                newest_first=True,
            )
            due_pending = ledger.list_qa_dispatches(
                states=("PENDING",),
                run_id=run_id_filter,
                due_before_epoch_s=now_s,
                newest_first=True,
            )

            if not eff_token:
                status = "blocked" if require_token else "disabled_no_token"
                return {
                    "status": status,
                    "phase": "qa_dispatch",
                    "checked_at_utc": now_iso,
                    "repo": eff_repo,
                    "workflow": eff_workflow,
                    "ref": eff_ref,
                    "token_configured": False,
                    "token_env": token_env,
                    "recovery": recovery,
                    "pending_count": len(all_pending),
                    "due_count": len(due_pending),
                    "accepted": [],
                    "retried": [],
                    "auth_errors": [],
                }

            endpoint_url = (
                f"{api_base.rstrip('/')}/repos/{eff_repo}/actions/workflows/{eff_workflow}/dispatches"
            )
            headers = {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {eff_token}",
                "Content-Type": "application/json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "tokyo-l2-qa-dispatcher/1.0",
            }

            accepted: list[dict[str, Any]] = []
            retried: list[dict[str, Any]] = []
            auth_errors: list[dict[str, Any]] = []
            rejected_errors: list[dict[str, Any]] = []

            to_send = due_pending[: max(0, int(max_dispatches))]
            for rec in to_send:
                payload = build_workflow_dispatch_payload(
                    rec,
                    ref=eff_ref,
                    extra_inputs=extra_inputs,
                )
                body_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
                attempt_ts_s = time.time() if now_epoch_s is None else now_s
                attempt_iso = _iso_utc(attempt_ts_s)

                try:
                    resp = sender(
                        endpoint_url,
                        headers,
                        body_bytes,
                        float(connect_timeout_s),
                        float(total_timeout_s),
                    )
                except (TimeoutError, socket.timeout) as exc:
                    delay_s = _compute_retry_delay_s(
                        attempt_index=int(rec["attempt_count"]),
                        base_backoff_s=base_backoff_s,
                        max_backoff_s=max_backoff_s,
                        now_s=attempt_ts_s,
                    )
                    err_msg = _sanitize_error(f"{type(exc).__name__}: {exc}", token=eff_token)
                    ledger.mark_qa_dispatch_retry(
                        int(rec["id"]),
                        next_attempt_at_epoch_s=attempt_ts_s + delay_s,
                        http_status=None,
                        error_code="github_timeout",
                        error_message=err_msg,
                        now_iso=attempt_iso,
                    )
                    ledger.record_event(
                        ts_utc=attempt_iso,
                        run_id=str(rec["run_id"] or "qa_dispatch"),
                        event_type="QA_DISPATCH_RETRY_SCHEDULED",
                        detail={
                            "target_window_utc": rec["target_window_utc"],
                            "archive_revision": rec["archive_revision"],
                            "error_code": "github_timeout",
                            "retry_in_seconds": round(delay_s, 1),
                        },
                    )
                    retried.append(
                        {
                            "id": rec["id"],
                            "run_id": rec["run_id"],
                            "target_window_utc": rec["target_window_utc"],
                            "archive_revision": rec["archive_revision"],
                            "error_code": "github_timeout",
                            "error_message": err_msg,
                            "http_status": None,
                            "next_attempt_at_utc": _iso_utc(attempt_ts_s + delay_s),
                        }
                    )
                    continue
                except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
                    delay_s = _compute_retry_delay_s(
                        attempt_index=int(rec["attempt_count"]),
                        base_backoff_s=base_backoff_s,
                        max_backoff_s=max_backoff_s,
                        now_s=attempt_ts_s,
                    )
                    err_msg = _sanitize_error(f"{type(exc).__name__}: {exc}", token=eff_token)
                    ledger.mark_qa_dispatch_retry(
                        int(rec["id"]),
                        next_attempt_at_epoch_s=attempt_ts_s + delay_s,
                        http_status=None,
                        error_code="github_network_error",
                        error_message=err_msg,
                        now_iso=attempt_iso,
                    )
                    ledger.record_event(
                        ts_utc=attempt_iso,
                        run_id=str(rec["run_id"] or "qa_dispatch"),
                        event_type="QA_DISPATCH_RETRY_SCHEDULED",
                        detail={
                            "target_window_utc": rec["target_window_utc"],
                            "archive_revision": rec["archive_revision"],
                            "error_code": "github_network_error",
                            "retry_in_seconds": round(delay_s, 1),
                        },
                    )
                    retried.append(
                        {
                            "id": rec["id"],
                            "run_id": rec["run_id"],
                            "target_window_utc": rec["target_window_utc"],
                            "archive_revision": rec["archive_revision"],
                            "error_code": "github_network_error",
                            "error_message": err_msg,
                            "http_status": None,
                            "next_attempt_at_utc": _iso_utc(attempt_ts_s + delay_s),
                        }
                    )
                    continue

                if 200 <= resp.status_code < 300:
                    ledger.mark_qa_dispatch_accepted(
                        int(rec["id"]),
                        accepted_at_utc=attempt_iso,
                        http_status=resp.status_code,
                        now_iso=attempt_iso,
                    )
                    ledger.record_event(
                        ts_utc=attempt_iso,
                        run_id=str(rec["run_id"] or "qa_dispatch"),
                        event_type="QA_DISPATCH_ACCEPTED",
                        detail={
                            "target_window_utc": rec["target_window_utc"],
                            "archive_revision": rec["archive_revision"],
                            "ref": eff_ref,
                            "workflow": eff_workflow,
                            "http_status": resp.status_code,
                        },
                    )
                    accepted.append(
                        {
                            "id": rec["id"],
                            "run_id": rec["run_id"],
                            "target_window_utc": rec["target_window_utc"],
                            "archive_revision": rec["archive_revision"],
                            "index_synced_at_utc": rec["index_synced_at_utc"],
                            "api_accepted_at_utc": attempt_iso,
                            "qa_completed_at_utc": None,
                            "http_status": resp.status_code,
                            "ref": eff_ref,
                            "workflow": eff_workflow,
                        }
                    )
                else:
                    err_code, err_msg, is_auth_err = _classify_http_failure(resp, token=eff_token)
                    delay_s = _compute_retry_delay_s(
                        attempt_index=int(rec["attempt_count"]),
                        base_backoff_s=base_backoff_s,
                        max_backoff_s=max_backoff_s,
                        response=resp,
                        now_s=attempt_ts_s,
                        min_delay_s=auth_backoff_s if is_auth_err else 0.0,
                    )
                    ledger.mark_qa_dispatch_retry(
                        int(rec["id"]),
                        next_attempt_at_epoch_s=attempt_ts_s + delay_s,
                        http_status=resp.status_code,
                        error_code=err_code,
                        error_message=err_msg,
                        now_iso=attempt_iso,
                    )
                    ledger.record_event(
                        ts_utc=attempt_iso,
                        run_id=str(rec["run_id"] or "qa_dispatch"),
                        event_type="QA_DISPATCH_AUTH_ERROR" if is_auth_err else "QA_DISPATCH_RETRY_SCHEDULED",
                        detail={
                            "target_window_utc": rec["target_window_utc"],
                            "archive_revision": rec["archive_revision"],
                            "http_status": resp.status_code,
                            "error_code": err_code,
                            "retry_in_seconds": round(delay_s, 1),
                        },
                    )
                    entry_err = {
                        "id": rec["id"],
                        "run_id": rec["run_id"],
                        "target_window_utc": rec["target_window_utc"],
                        "archive_revision": rec["archive_revision"],
                        "http_status": resp.status_code,
                        "error_code": err_code,
                        "error_message": err_msg,
                        "next_attempt_at_utc": _iso_utc(attempt_ts_s + delay_s),
                    }
                    retried.append(entry_err)
                    if is_auth_err:
                        auth_errors.append(entry_err)
                        # Stop hammering GitHub with an invalid/expired token in this tick.
                        break
                    if 400 <= resp.status_code < 500 and err_code.startswith("github_request_rejected_"):
                        rejected_errors.append(entry_err)

            remaining_pending = ledger.list_qa_dispatches(
                states=("PENDING",),
                run_id=run_id_filter,
                newest_first=True,
            )
            accepted_total = ledger.list_qa_dispatches(
                states=("ACCEPTED",),
                run_id=run_id_filter,
                newest_first=True,
            )
            completed_total = ledger.list_qa_dispatches(
                states=("QA_COMPLETED",),
                run_id=run_id_filter,
                newest_first=True,
            )

            if auth_errors:
                overall_status = "auth_error"
            elif retried or rejected_errors:
                overall_status = "retry_scheduled"
            else:
                overall_status = "ok"

            return {
                "status": overall_status,
                "phase": "qa_dispatch",
                "checked_at_utc": now_iso,
                "repo": eff_repo,
                "workflow": eff_workflow,
                "ref": eff_ref,
                "token_configured": True,
                "recovery": recovery,
                "attempted_count": len(to_send),
                "accepted_count": len(accepted),
                "retried_count": len(retried),
                "auth_error_count": len(auth_errors),
                "rejected_error_count": len(rejected_errors),
                "accepted": accepted,
                "retried": retried,
                "auth_errors": auth_errors,
                "rejected_errors": rejected_errors,
                "outbox_counts": {
                    "PENDING": len(remaining_pending),
                    "ACCEPTED": len(accepted_total),
                    "QA_COMPLETED": len(completed_total),
                },
            }
    finally:
        lock.release()


def record_qa_completion(
    sealed_root: Path,
    *,
    run_id: str,
    target_window_utc: str,
    archive_revision: str,
    qa_result_status: str,
    qa_completed_at_utc: str | None = None,
    workflow_run_id: int | None = None,
    workflow_run_url: str | None = None,
    workflow_run_started_at_utc: str | None = None,
    now_epoch_s: float | None = None,
) -> dict[str, Any]:
    """Record QA completion separately from API acceptance on an existing outbox entry."""
    now_s = time.time() if now_epoch_s is None else float(now_epoch_s)
    now_iso = _iso_utc(now_s)
    completed_iso = qa_completed_at_utc or now_iso
    ledger_path = sealed_root / LEDGER_DB_FILENAME
    with ArchiveLedger(ledger_path) as ledger:
        updated = ledger.mark_qa_dispatch_completed(
            run_id=run_id,
            target_window_utc=target_window_utc,
            archive_revision=archive_revision,
            qa_result_status=qa_result_status,
            qa_completed_at_utc=completed_iso,
            workflow_run_id=workflow_run_id,
            workflow_run_url=workflow_run_url,
            workflow_run_started_at_utc=workflow_run_started_at_utc,
            now_iso=now_iso,
        )
        row = ledger.get_qa_dispatch(
            run_id=run_id,
            target_window_utc=target_window_utc,
            archive_revision=archive_revision,
        )
        if updated:
            ledger.record_event(
                ts_utc=now_iso,
                run_id=run_id or "qa_dispatch",
                event_type="QA_DISPATCH_COMPLETED",
                detail={
                    "target_window_utc": target_window_utc,
                    "archive_revision": archive_revision,
                    "qa_result_status": qa_result_status,
                    "workflow_run_id": workflow_run_id,
                    "workflow_run_url": workflow_run_url,
                },
            )
    return {
        "updated": updated,
        "record": row,
    }


def get_dispatch_status(
    sealed_root: Path,
    *,
    run_id_filter: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """Return human- and machine-readable outbox status without touching credentials."""
    ledger_path = sealed_root / LEDGER_DB_FILENAME
    if not ledger_path.is_file():
        return {
            "status": "no_ledger",
            "ledger_db_path": str(ledger_path),
            "counts": {"PENDING": 0, "ACCEPTED": 0, "QA_COMPLETED": 0, "FAILED_PERMANENT": 0},
            "pending": [],
            "accepted": [],
            "completed": [],
        }
    with ArchiveLedger(ledger_path) as ledger:
        pending = ledger.list_qa_dispatches(states=("PENDING",), run_id=run_id_filter, newest_first=True)
        accepted = ledger.list_qa_dispatches(states=("ACCEPTED",), run_id=run_id_filter, newest_first=True)
        completed = ledger.list_qa_dispatches(states=("QA_COMPLETED",), run_id=run_id_filter, newest_first=True)
        failed = ledger.list_qa_dispatches(states=("FAILED_PERMANENT",), run_id=run_id_filter, newest_first=True)
    return {
        "status": "ok",
        "ledger_db_path": str(ledger_path),
        "counts": {
            "PENDING": len(pending),
            "ACCEPTED": len(accepted),
            "QA_COMPLETED": len(completed),
            "FAILED_PERMANENT": len(failed),
        },
        "pending": pending[:limit],
        "accepted": accepted[:limit],
        "completed": completed[:limit],
        "failed_permanent": failed[:limit],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Independent lightweight GitHub Actions QA workflow dispatcher for verified Tokyo L2 windows"
    )
    parser.add_argument("--dir", type=Path, required=True, help="Sealed archive root directory (e.g. /data/tokyo_l2)")
    parser.add_argument("--config", type=Path, default=None, help="Optional collector config for partition_minutes/run_id")
    parser.add_argument("--run-id", default=None, help="Optional collection batch run_id filter")
    parser.add_argument("--repo", default=None, help="Target GitHub repo (default: Huhabubu/mm)")
    parser.add_argument("--workflow", default=None, help="Workflow filename (default: tokyo-l2-incremental-qa.yml)")
    parser.add_argument("--ref", default=None, help="Git ref to dispatch (production default: main)")
    parser.add_argument("--token-env", default=DEFAULT_TOKEN_ENV, help="Env var holding fine-grained GitHub PAT")
    parser.add_argument("--max-dispatches", type=int, default=DEFAULT_MAX_DISPATCHES_PER_RUN)
    parser.add_argument("--max-age-hours", type=float, default=None, help="Optional max window age for index recovery")
    parser.add_argument("--connect-timeout", type=float, default=DEFAULT_CONNECT_TIMEOUT_S)
    parser.add_argument("--total-timeout", type=float, default=DEFAULT_TOTAL_TIMEOUT_S)
    parser.add_argument("--base-backoff", type=float, default=DEFAULT_BASE_BACKOFF_S)
    parser.add_argument("--max-backoff", type=float, default=DEFAULT_MAX_BACKOFF_S)
    parser.add_argument("--require-token", action="store_true", help="Exit non-zero if dispatch token is unset")
    parser.add_argument("--status-only", action="store_true", help="Print outbox status JSON without sending requests")
    args = parser.parse_args(argv)

    run_id_filter = args.run_id
    pm_filter: int | None = None
    if args.config is not None and args.config.is_file():
        from data_coverage import configured_partition_minutes, resolve_effective_run_id

        cfg_obj = json.loads(args.config.read_text(encoding="utf-8"))
        pm_filter = configured_partition_minutes(cfg_obj)
        run_id_filter = resolve_effective_run_id(cfg_obj, args.run_id)

    if args.status_only:
        res = get_dispatch_status(args.dir, run_id_filter=run_id_filter)
        print(json.dumps(res, indent=2, ensure_ascii=False))
        return 0

    res = dispatch_pending_qa(
        args.dir,
        repo=args.repo,
        workflow_file=args.workflow,
        ref=args.ref,
        token_env=args.token_env,
        run_id_filter=run_id_filter,
        partition_minutes_filter=pm_filter,
        max_dispatches=args.max_dispatches,
        max_age_hours=args.max_age_hours,
        connect_timeout_s=args.connect_timeout,
        total_timeout_s=args.total_timeout,
        base_backoff_s=args.base_backoff,
        max_backoff_s=args.max_backoff,
        require_token=args.require_token,
    )
    print(json.dumps(res, indent=2, ensure_ascii=False))
    if res["status"] in ("ok", "disabled_no_token", "busy"):
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
