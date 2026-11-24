#!/usr/bin/env python3
"""Kaggle environment probe: resources, dependencies, Drive secrets/rclone access.

Writes /kaggle/working/probe_report.json after every section. Never prints or
writes secret values; rclone credentials live only under /tmp and are removed.
"""
from __future__ import annotations

import importlib.metadata as md
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.request
import zipfile

WORK = Path("/kaggle/working")
REPORT = WORK / "probe_report.json"
RCLONE_VERSION = "v1.68.2"
PROBE_DATE = "2026-11-23"
# Kaggle secrets service can be transiently unreachable early in a Batch run; retry with backoff.
SECRET_RETRY_DELAYS_S = (5, 10, 20)
report: dict = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


def save() -> None:
    REPORT.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")


def section(name):
    def deco(fn):
        t0 = time.time()
        try:
            report[name] = {"ok": True, **(fn() or {})}
        except Exception as exc:
            report[name] = {"ok": False, "error": f"{type(exc).__name__}: {exc}",
                            "trace": traceback.format_exc()[-3000:]}
        report[name]["elapsed_s"] = round(time.time() - t0, 2)
        save()
        print(f"SECTION {name} ok={report[name]['ok']} {report[name]['elapsed_s']}s", flush=True)
        return fn
    return deco


def _read(path: str) -> str | None:
    try:
        return Path(path).read_text().strip()
    except Exception:
        return None


def _meminfo() -> dict:
    out = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        k, v = line.split(":", 1)
        if k in ("MemTotal", "MemAvailable", "SwapTotal"):
            out[k + "_GiB"] = round(int(v.split()[0]) / 1024 ** 2, 2)
    return out


@section("system")
def _system():
    model = next((l.split(":", 1)[1].strip() for l in Path("/proc/cpuinfo").read_text().splitlines()
                  if l.startswith("model name")), None)
    disks = {}
    for p in ("/kaggle/working", "/tmp", "/kaggle/input", "/"):
        if os.path.exists(p):
            u = shutil.disk_usage(p)
            disks[p] = {"total_GiB": round(u.total / 1024 ** 3, 1), "free_GiB": round(u.free / 1024 ** 3, 1)}
    return {"python": sys.version, "platform": platform.platform(), "cpu_count": os.cpu_count(),
            "sched_affinity": len(os.sched_getaffinity(0)), "cpu_model": model,
            "cgroup_cpu_max": _read("/sys/fs/cgroup/cpu.max"),
            "cgroup_memory_max": _read("/sys/fs/cgroup/memory.max"),
            "meminfo": _meminfo(), "disk": disks}


@section("deps")
def _deps():
    cmd = [sys.executable, "-m", "pip", "install", "-q", "hftbacktest==2.4.4", "zstandard==0.23.0"]
    r = subprocess.run(cmd, capture_output=True, text=True)
    vers = {}
    for name in ("hftbacktest", "numba", "numpy", "zstandard", "llvmlite", "pandas"):
        try:
            vers[name] = md.version(name)
        except md.PackageNotFoundError:
            vers[name] = None
    return {"pip_rc": r.returncode, "pip_tail": (r.stdout + r.stderr)[-2000:], "versions": vers}


@section("hftbacktest_smoke")
def _hbt():
    import numpy as np
    from numba import njit
    from hftbacktest import (BacktestAsset, ROIVectorMarketDepthBacktest, BUY_EVENT, SELL_EVENT,
                             DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT, TRADE_EVENT, EXCH_EVENT, LOCAL_EVENT,
                             GTX, LIMIT)
    from hftbacktest.types import event_dtype

    t0 = 1_700_000_000_000_000_000
    rows = []
    snap = DEPTH_SNAPSHOT_EVENT | EXCH_EVENT | LOCAL_EVENT
    rows.append((snap | BUY_EVENT, t0, t0 + 1_000_000, 100.0, 5.0, 0, 0, 0.0))
    rows.append((snap | SELL_EVENT, t0, t0 + 1_000_000, 100.1, 5.0, 0, 0, 0.0))
    for i in range(1, 200):
        ts = t0 + i * 50_000_000
        rows.append((DEPTH_EVENT | EXCH_EVENT | LOCAL_EVENT | BUY_EVENT, ts, ts + 1_000_000,
                     100.0, float(4 + i % 3), 0, 0, 0.0))
        if i % 10 == 0:
            rows.append((TRADE_EVENT | EXCH_EVENT | LOCAL_EVENT | SELL_EVENT, ts, ts + 1_000_000,
                         100.0, 1.0, 0, 0, 0.0))
    data = np.array(rows, dtype=event_dtype)
    assert data["exch_ts"].dtype == np.int64 and data["local_ts"].dtype == np.int64

    asset = (BacktestAsset().data([data]).linear_asset(1.0)
             .constant_order_latency(1_000_000, 1_000_000)
             .power_prob_queue_model(2.0).no_partial_fill_exchange()
             .trading_value_fee_model(0.0, 0.0).tick_size(0.1).lot_size(1.0)
             .roi_lb(90.0).roi_ub(110.0))
    hbt = ROIVectorMarketDepthBacktest([asset])

    @njit
    def loop(hbt):
        steps = 0
        submitted = False
        best_bid = 0.0
        while hbt.elapse(100_000_000) == 0:
            steps += 1
            depth = hbt.depth(0)
            best_bid = depth.best_bid
            if not submitted and steps == 2:
                hbt.submit_buy_order(0, 1, 100.0, 1.0, GTX, LIMIT, False)
                submitted = True
        return steps, best_bid, hbt.position(0)

    steps, best_bid, pos = loop(hbt)
    hbt.close()
    return {"steps": int(steps), "best_bid": float(best_bid), "position": float(pos), "rows": int(len(data))}


RCLONE = Path("/tmp/rclone_bin/rclone")
AUTH = Path(tempfile.mkdtemp(prefix="tokyo_l2_auth_"))


@section("rclone_install")
def _rclone():
    zpath = Path("/tmp/rclone.zip")
    urls = [f"https://github.com/rclone/rclone/releases/download/{RCLONE_VERSION}/rclone-{RCLONE_VERSION}-linux-amd64.zip",
            f"https://downloads.rclone.org/{RCLONE_VERSION}/rclone-{RCLONE_VERSION}-linux-amd64.zip"]
    errors = []
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "curl/8.5.0"})
            with urllib.request.urlopen(req, timeout=120) as r:
                zpath.write_bytes(r.read())
            break
        except Exception as exc:
            errors.append(f"{url.split('/')[2]}: {type(exc).__name__}: {exc}")
    else:
        raise RuntimeError("; ".join(errors))
    RCLONE.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zpath) as z:
        member = next(m for m in z.namelist() if m.endswith("/rclone"))
        RCLONE.write_bytes(z.read(member))
    RCLONE.chmod(0o755)
    ver = subprocess.run([str(RCLONE), "version"], capture_output=True, text=True).stdout.splitlines()[0]
    return {"version": ver, "source": url.split("/")[2], "earlier_errors": errors}


def _exc_chain(exc: BaseException) -> str:
    """Type/message of the exception and its causes (no secret values are ever in these)."""
    parts, seen = [], set()
    while exc is not None and id(exc) not in seen and len(parts) < 4:
        seen.add(id(exc))
        parts.append(f"{type(exc).__name__}: {str(exc)[:240]}")
        exc = exc.__cause__ or exc.__context__
    return " <- ".join(parts)


def _secrets_service_diag() -> dict:
    """Where the secrets client would connect, and whether that host is reachable. No tokens."""
    import socket
    import urllib.parse
    out: dict = {}
    try:
        import kaggle_secrets
        out["client_module"] = getattr(kaggle_secrets, "__file__", None)
    except Exception as exc:
        out["client_module_error"] = _exc_chain(exc)
    base = os.environ.get("KAGGLE_URL_BASE", "")
    host = urllib.parse.urlparse(base).hostname or ""
    out["url_base_host"] = host
    out["url_base_scheme"] = urllib.parse.urlparse(base).scheme
    if host:
        try:
            out["dns"] = sorted({ai[4][0] for ai in socket.getaddrinfo(host, 443)})[:4]
        except Exception as exc:
            out["dns_error"] = _exc_chain(exc)
        try:
            t0 = time.time()
            with socket.create_connection((host, 443), timeout=10):
                out["tcp443_s"] = round(time.time() - t0, 3)
        except Exception as exc:
            out["tcp443_error"] = _exc_chain(exc)
    return out


@section("secrets_and_drive")
def _drive():
    # Presence only (never values) of the env the Kaggle secrets client depends on.
    report["secrets_env_present"] = {k: bool(os.environ.get(k)) for k in
                                     ("KAGGLE_USER_SECRETS_TOKEN", "KAGGLE_URL_BASE", "KAGGLE_KERNEL_RUN_TYPE",
                                      "KAGGLE_DATA_PROXY_TOKEN", "KAGGLE_IAP_TOKEN",
                                      "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy")}
    report["kernel_run_type"] = os.environ.get("KAGGLE_KERNEL_RUN_TYPE")
    report["secrets_service"] = _secrets_service_diag()
    from kaggle_secrets import UserSecretsClient
    got = {}
    attempts_log = {}
    for name in ("TOKYO_L2_QA_GDRIVE_SA_JSON", "TOKYO_L2_QA_GDRIVE_FOLDER_ID"):
        got[name] = ""
        tries = []
        for delay in (0, *SECRET_RETRY_DELAYS_S):
            if delay:
                time.sleep(delay)
            t0 = time.time()
            try:
                got[name] = UserSecretsClient().get_secret(name) or ""
                tries.append({"after_wait_s": delay, "ok": bool(got[name]), "s": round(time.time() - t0, 2)})
                if got[name]:
                    break
            except Exception as exc:
                tries.append({"after_wait_s": delay, "ok": False, "s": round(time.time() - t0, 2),
                              "error": _exc_chain(exc)})
        attempts_log[name] = tries
        if not got[name]:
            report.setdefault("secret_errors", {})[name] = tries[-1].get("error", "empty value")
    report["secret_attempts"] = attempts_log
    save()
    present = {k: bool(v) for k, v in got.items()}
    if not all(present.values()):
        raise RuntimeError(f"secrets not readable after {1 + len(SECRET_RETRY_DELAYS_S)} attempts: {present}")
    sa = json.loads(got["TOKYO_L2_QA_GDRIVE_SA_JSON"])
    os.umask(0o077)
    sa_path = AUTH / "sa.json"
    sa_path.write_text(json.dumps(sa))
    conf = AUTH / "rclone.conf"
    conf.write_text(f"[qa]\ntype = drive\nscope = drive.readonly\nservice_account_file = {sa_path}\n"
                    f"root_folder_id = {got['TOKYO_L2_QA_GDRIVE_FOLDER_ID']}\n")
    env = dict(os.environ, RCLONE_CONFIG=str(conf))

    def rc(*a, timeout=300):
        r = subprocess.run([str(RCLONE), *a], capture_output=True, text=True, env=env, timeout=timeout)
        if r.returncode != 0:
            raise RuntimeError(f"rclone {a[0]} rc={r.returncode}: {r.stderr[-500:]}")
        return r.stdout

    meta = rc("lsf", "qa:meta", "--max-depth", "1").split()
    out = {"secrets_present": present, "sa_type": sa.get("type"), "meta_entries": len(meta),
           "has_archive_manifest_index": "archive_manifest_index.json" in meta}
    for ex in ("binance_futures", "gate_futures"):
        names = rc("lsf", f"qa:raw/{ex}/{PROBE_DATE}", "--files-only").split()
        out[f"raw_{ex}_{PROBE_DATE}"] = {
            "data_files": sum(n.endswith(".tsv.zst") for n in names),
            "manifests": sum(n.endswith(".manifest.json") for n in names)}
    t0 = time.time()
    dst = Path("/tmp/probe_dl")
    rc("copyto", "qa:meta/archive_manifest_index.json", str(dst / "archive_manifest_index.json"), timeout=900)
    size = (dst / "archive_manifest_index.json").stat().st_size
    dt = time.time() - t0
    out["index_download"] = {"bytes": size, "seconds": round(dt, 2), "MBps": round(size / 1e6 / max(dt, 1e-3), 2)}
    shutil.rmtree(dst, ignore_errors=True)
    out["drive_listed"] = True
    return out


shutil.rmtree(AUTH, ignore_errors=True)
report["auth_dir_removed"] = not AUTH.exists()
report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
# Every section must pass AND Drive must actually have been listed; unread secrets are a failure.
report["all_ok"] = (all(v.get("ok") for v in report.values() if isinstance(v, dict) and "ok" in v)
                    and bool(report.get("secrets_and_drive", {}).get("drive_listed")))
save()
print("PROBE_DONE all_ok=", report["all_ok"], flush=True)
sys.exit(0 if report["all_ok"] else 3)
