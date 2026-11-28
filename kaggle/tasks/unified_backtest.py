#!/usr/bin/env python3
"""Unified, config-driven Tokyo L2 backtest & five-stage research runner.

Eliminates hardcoded exchange/symbol/date/3-model constraints from the entry point:
  - Reads a unified JSON experiment config (e.g. `configs/tokyo_l2_20261123_gate_btc.json`).
  - Downloads and converts raw data ONCE per `(date, symbol, exchange)` via existing
    `tokyo_l2_bt_fetch.py` + `tokyo_l2_bt_stream_convert.py`, with fail-closed verified
    cache reuse (`tokyo_l2_bt_cache.py`).
  - Runs all configured models (up to all 6 via `scripts/core_glft_engine.py`) and
    Stages 1, 2, 3 (shadow), 4 (locked/pending), 5A, 5B, 5C (locked/pending) on the
    single converted dataset.
  - Writes machine-readable JSON, CSV, and Markdown reports to `--out-dir` (`/kaggle/working`),
    and removes all temporary raw/converted market data and credential files in `finally:`.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import gc
import json
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import time
import traceback
import urllib.request
import zipfile
from typing import Any

WORK = Path("/kaggle/working")
REMOTE = "qa:"
RCLONE_VERSION = "v1.68.2"
RCLONE_DIR = Path("/tmp/rclone_bin")
AUTH = Path("/tmp/tokyo_l2_auth_unified")
SCRATCH = Path("/tmp/tokyo_l2_unified")
CODE = Path(os.environ.get("TOKYO_L2_CODE_ROOT", "/tmp/tokyo_l2_code"))
DEFAULT_COLLECTOR_CONFIG = CODE / "collector" / "specs" / "config.trial_15m.json"
QA_DAYS = CODE / "repo" / "kaggle" / "qa_days"
QA_PROTOCOL = CODE / "repo" / "kaggle" / "qa_protocol"
CRED_FILES = {
    "TOKYO_L2_QA_GDRIVE_SA_JSON": "tokyo_l2_gdrive_sa.json",
    "TOKYO_L2_QA_GDRIVE_FOLDER_ID": "tokyo_l2_gdrive_folder_id.txt",
}

NS = 1_000_000_000
DAY_NS = 86_400 * NS


def _ensure_sys_path() -> None:
    for p in (
        CODE / "repo" / "scripts",
        CODE / "collector" / "python",
        Path(__file__).resolve().parents[2] / "scripts",
    ):
        s = str(p)
        if p.exists() and s not in sys.path:
            sys.path.insert(0, s)


def peak_rss_mb() -> float:
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def du(path: Path) -> int:
    p = Path(path)
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0


def setup_kaggle_env() -> dict[str, Any]:
    r = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "hftbacktest==2.4.4", "zstandard==0.23.0"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError("pip install failed: " + (r.stdout + r.stderr)[-1500:])

    exe = RCLONE_DIR / "rclone"
    if not exe.exists():
        blob, errors = None, []
        for url in (
            f"https://github.com/rclone/rclone/releases/download/{RCLONE_VERSION}/rclone-{RCLONE_VERSION}-linux-amd64.zip",
            f"https://downloads.rclone.org/{RCLONE_VERSION}/rclone-{RCLONE_VERSION}-linux-amd64.zip",
        ):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "curl/8.5.0"})
                with urllib.request.urlopen(req, timeout=120) as resp:
                    blob = resp.read()
                break
            except Exception as exc:
                errors.append(url.split("/")[2] + f": {type(exc).__name__}: {exc}")
        if blob is None:
            raise RuntimeError("; ".join(errors))
        zpath = Path("/tmp/rclone_unified.zip")
        zpath.write_bytes(blob)
        RCLONE_DIR.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zpath) as z:
            member = next(m for m in z.namelist() if m.endswith("/rclone"))
            exe.write_bytes(z.read(member))
        exe.chmod(0o755)
        zpath.unlink()
    os.environ["PATH"] = str(RCLONE_DIR) + os.pathsep + os.environ.get("PATH", "")

    root = Path("/kaggle/input")
    found = {}
    for key, fname in CRED_FILES.items():
        hits = sorted(p for p in root.rglob(fname) if p.is_file()) if root.is_dir() else []
        if hits:
            found[key] = hits[0]
    if len(found) != len(CRED_FILES):
        raise RuntimeError("credential dataset files missing: " + json.dumps({k: k in found for k in CRED_FILES}))
    sa = json.loads(found["TOKYO_L2_QA_GDRIVE_SA_JSON"].read_text(encoding="utf-8"))
    folder = found["TOKYO_L2_QA_GDRIVE_FOLDER_ID"].read_text(encoding="utf-8").strip()
    old_umask = os.umask(0o077)
    try:
        AUTH.mkdir(parents=True, exist_ok=True)
        sa_path = AUTH / "sa.json"
        sa_path.write_text(json.dumps(sa), encoding="utf-8")
        conf = AUTH / "rclone.conf"
        conf.write_text(
            "[qa]\ntype = drive\nscope = drive.readonly\n"
            f"service_account_file = {sa_path}\nroot_folder_id = {folder}\n",
            encoding="utf-8",
        )
    finally:
        os.umask(old_umask)
    os.environ["RCLONE_CONFIG"] = str(conf)

    import hftbacktest
    import numpy as np
    import zstandard as zstd

    return {
        "hftbacktest": getattr(hftbacktest, "__version__", "2.4.4"),
        "numpy": np.__version__,
        "zstandard": zstd.__version__,
        "rclone_version": subprocess.check_output(["rclone", "version"], text=True).splitlines()[0],
    }


def _classify_qa_interval(status: str, reason: str) -> str:
    st = str(status or "").lower()
    rs = str(reason or "").lower()
    if st == "known_gap" or rs in ("depth_unavailable", "local_loss", "gap_start"):
        return "confirmed_data_gap"
    return "continuity_unverified"


def _audit_staged_protocol(
    cands_by_window: dict[str, list[dict[str, Any]]],
    run_id: str,
    signal_exchange: str,
    making_exchange: str,
    base_symbol: str,
) -> dict[str, Any]:
    """Verify staged 96-window protocol QA reports by rules digest, archive_revision, and input hashes."""
    from tokyo_l2_exchange_adapters import strategy_channels, to_base_symbol

    prov_path = QA_PROTOCOL / "provenance.json"
    if not prov_path.is_file():
        return {"present": False, "status": "not_staged", "strategy_channel_intervals": []}
    provenance = json.loads(prov_path.read_text(encoding="utf-8"))
    expected_rules_sha = str(provenance.get("expected_protocol_rules_sha") or "").strip()
    expected_revs = dict(provenance.get("window_archive_revisions") or {})
    allowed_channels = strategy_channels(signal_exchange, making_exchange)

    windows = sorted(cands_by_window.keys())
    statuses: dict[str, int] = {}
    checker_commits: set[str] = set()
    rules_digests: set[str] = set()
    missing_reports: list[str] = []
    rules_mismatches: list[str] = []
    revision_mismatches: list[str] = []
    input_mismatches: list[str] = []
    strategy_intervals: list[dict[str, Any]] = []
    strategy_affected_windows: set[str] = set()

    for w in windows:
        fname = w.replace(":", "") + ".json"
        path = QA_PROTOCOL / fname
        if not path.is_file():
            missing_reports.append(w)
            continue
        q = json.loads(path.read_text(encoding="utf-8"))
        st = str(q.get("status") or "unknown")
        statuses[st] = statuses.get(st, 0) + 1
        checker = str(q.get("checker") or "")
        if "/" in checker:
            c_sha, r_sha = checker.split("/", 1)
            checker_commits.add(c_sha)
            rules_digests.add(r_sha)
        else:
            r_sha = ""
        if not expected_rules_sha or r_sha != expected_rules_sha:
            rules_mismatches.append(w)
        exp_rev = expected_revs.get(w)
        if not exp_rev or q.get("archive_revision") != exp_rev:
            revision_mismatches.append(w)
        expected_inputs = {(c["exchange"], c["data_sha256"], c["manifest_sha256"]) for c in cands_by_window[w]}
        actual_inputs = {(p["exchange"], p["data_sha256"], p["manifest_sha256"]) for p in q.get("target_inputs", [])}
        if not expected_inputs or not expected_inputs.issubset(actual_inputs) or (run_id and q.get("run_id") != run_id):
            input_mismatches.append(w)
        for market in q.get("market_availability", []):
            ex = market.get("exchange")
            ch = market.get("channel")
            msym = market.get("symbol")
            if msym and to_base_symbol(msym) == base_symbol and (ex, ch) in allowed_channels:
                for iv in market.get("intervals", []):
                    if iv.get("status") != "verified_usable":
                        s_ns = int(iv["start_wall_ns"])
                        e_ns = int(iv["end_wall_ns"]) if iv.get("end_wall_ns") is not None else None
                        strategy_intervals.append({
                            "window_id": w,
                            "exchange": ex,
                            "channel": ch,
                            "qa_status": iv.get("status"),
                            "reason": iv.get("reason"),
                            "category": _classify_qa_interval(iv.get("status"), iv.get("reason")),
                            "start_wall_ns": s_ns,
                            "end_wall_ns": e_ns,
                            "duration_ms": round((e_ns - s_ns) / 1e6, 3) if e_ns is not None else None,
                        })
                        strategy_affected_windows.add(w)

    binding_ok = (
        not missing_reports and not rules_mismatches
        and not revision_mismatches and not input_mismatches
        and len(windows) == 96
    )
    return {
        "present": True,
        "windows_checked": len(windows) - len(missing_reports),
        "expected_windows": len(windows),
        "missing_reports": missing_reports,
        "expected_protocol_rules_sha": expected_rules_sha,
        "observed_checker_commits": sorted(checker_commits),
        "observed_rules_digests": sorted(rules_digests),
        "rules_digest_mismatches": rules_mismatches,
        "archive_revision_mismatches": revision_mismatches,
        "input_hash_mismatches": input_mismatches,
        "evidence_binding_verified": binding_ok,
        "production_qa_window_statuses_all_exchanges": statuses,
        "strategy_channels_affected_windows": sorted(strategy_affected_windows),
        "strategy_channel_intervals": strategy_intervals,
        "stage3_strict_qa_passed": (
            binding_ok and not strategy_intervals and statuses.get("passed") == len(windows)
        ),
    }


def _inspect_anomalies_and_book(
    hbt_path: Path,
    stream_report: dict[str, Any],
    protocol_intervals: list[dict[str, Any]],
    signal_exchange: str,
    making_exchange: str,
    day_start_ns: int,
    day_end_ns: int,
) -> tuple[dict[str, Any], list[tuple[int, int]], float, float]:
    """Extract interruption episodes, ROI price bounds, and book-recovery diagnostics once."""
    import numpy as np
    from hftbacktest import DEPTH_CLEAR_EVENT, DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT, TRADE_EVENT

    with np.load(hbt_path, allow_pickle=False) as z:
        data = z["data"]
    ev_kind = data["ev"].astype(np.uint64) & np.uint64(0xFF)
    px = data["px"]
    valid_px = px[np.isfinite(px) & (px > 0)]
    if not len(valid_px):
        raise ValueError("HBT array has no valid positive prices")
    roi_low = float(valid_px.min())
    roi_high = float(valid_px.max())

    raw_intervals: list[tuple[int, int, str]] = []
    for iv in protocol_intervals or []:
        s_ns = int(iv["start_wall_ns"])
        e_ns = int(iv["end_wall_ns"]) if iv.get("end_wall_ns") is not None else s_ns
        if e_ns < s_ns:
            e_ns = s_ns
        raw_intervals.append((s_ns, e_ns, f"protocol:{iv.get('exchange')}:{iv.get('channel')}:{iv.get('reason')}"))
    for ex_key, gap_list in (stream_report.get("gaps") or {}).items():
        for g in gap_list or []:
            s = g.get("start_wall_ns")
            e = g.get("end_wall_ns")
            s_ns = int(s) if s is not None else day_start_ns
            e_ns = int(e) if e is not None else day_end_ns
            if e_ns < s_ns:
                e_ns = s_ns
            raw_intervals.append((s_ns, e_ns, f"gap:{ex_key}:{g.get('reason', '')}"))
    for iv in stream_report.get("depth_unavailable_intervals") or []:
        s = int(iv.get("start_wall_ns") or day_start_ns)
        e_raw = iv.get("end_wall_ns")
        e = int(e_raw) if (e_raw is not None and int(e_raw) >= s) else day_end_ns
        raw_intervals.append((s, e, f"depth_unavailable:{iv.get('reason', '')}"))
    for iv in stream_report.get("unrebuildable_depth_intervals") or []:
        s = int(iv.get("start_wall_ns") or day_start_ns)
        e_raw = iv.get("end_wall_ns")
        e = int(e_raw) if (e_raw is not None and int(e_raw) >= s) else s
        raw_intervals.append((s, e, f"unrebuildable:{iv.get('reason', '')}"))

    raw_intervals.sort(key=lambda x: (x[0], x[1]))
    merged: list[dict[str, Any]] = []
    for s, e, tag in raw_intervals:
        if merged and s <= merged[-1]["end_ns"] + 5_000_000:
            merged[-1]["end_ns"] = max(merged[-1]["end_ns"], e)
            merged[-1]["sources"].append(tag)
        else:
            merged.append({"start_ns": s, "end_ns": e, "sources": [tag]})

    episodes = [(m["start_ns"], m["end_ns"]) for m in merged]
    diag = {
        "hbt_total_rows": int(len(data)),
        "hbt_depth_rows": int(np.sum(ev_kind == DEPTH_EVENT)),
        "hbt_snapshot_rows": int(np.sum(ev_kind == DEPTH_SNAPSHOT_EVENT)),
        "hbt_clear_rows": int(np.sum(ev_kind == DEPTH_CLEAR_EVENT)),
        "hbt_trade_rows": int(np.sum(ev_kind == TRADE_EVENT)),
        "roi_low": roi_low,
        "roi_high": roi_high,
        "episode_count": len(episodes),
        "episodes": [
            {
                "index": i,
                "start_ns": m["start_ns"],
                "end_ns": m["end_ns"],
                "duration_ms": round((m["end_ns"] - m["start_ns"]) / 1e6, 3),
                "sources": m["sources"],
            }
            for i, m in enumerate(merged)
        ],
        "switches": stream_report.get("switches") or {},
        "trade_dedup": stream_report.get("trade_dedup") or {},
        "exch_ahead_of_local": stream_report.get("exch_ahead_of_local") or {},
        "order_diagnostics": stream_report.get("order_diagnostics") or {},
        "signal_exchange": signal_exchange,
        "making_exchange": making_exchange,
    }
    del data
    gc.collect()
    return diag, episodes, roi_low, roi_high


def _fetch_and_convert_drive_day(
    *,
    cfg: dict[str, Any],
    date_utc: str,
    collector_config_path: Path,
    cache_input_dir: Path | None,
    day_scratch: Path,
) -> dict[str, Any]:
    """Discover, verify QA/spec, check conversion cache, or download + stream-convert ONCE."""
    import hourly_incremental_qa as archive
    from data_coverage import effective_config_hash
    from tokyo_l2_bt_fetch import (
        WINDOWS_PER_DAY_15M,
        _safe_rel,
        day_start_s,
        fetch_meta,
        find_day_report,
        integrity_gate,
    )
    from tokyo_l2_exchange_adapters import (
        contract_spec_digest,
        load_live_contract_from_meta,
        normalize_contract_spec,
    )
    from tokyo_l2_bt_cache import (
        compute_cache_key,
        converter_code_sha256,
        source_partitions_digest,
        verify_cache_dir,
        write_cache_manifest,
    )
    from tokyo_l2_bt_stream_convert import StreamConverter, frozen_sort_key

    sig_ex = cfg["signal_exchange"]
    mak_ex = cfg["making_exchange"]
    exchanges = (sig_ex, mak_ex)
    base_sym = cfg["base_symbol"]
    mak_native = cfg["making_native_symbol"]
    day_start_sec = day_start_s(date_utc)
    day_start_ns = day_start_sec * NS
    day_end_ns = day_start_ns + DAY_NS

    collector_cfg = json.loads(collector_config_path.read_text(encoding="utf-8"))
    cfg_hash = effective_config_hash(collector_cfg)

    qa_day_info: dict[str, Any] = {"present": False}
    run_id = ""
    if QA_DAYS.is_dir():
        try:
            rpath = find_day_report(QA_DAYS, date_utc, None)
            day_doc = json.loads(rpath.read_text(encoding="utf-8"))
            run_id = str(day_doc["run_id"])
            gate = integrity_gate(day_doc, exchanges, cfg_hash, date_utc)
            qa_day_info = {
                "present": True,
                "report_file": rpath.name,
                "run_id": run_id,
                "production_qa_status_all_exchanges": day_doc.get("qa_status"),
                "strategy_exchanges_gate": gate,
            }
        except Exception as exc:
            qa_day_info = {"present": False, "error": f"{type(exc).__name__}: {exc}"}

    # Discover verified Drive partitions
    candidates, disc_mode, warnings, _, index_run_id = archive._discover_verified_candidates(
        REMOTE, collector_cfg, since_date=date_utc, since_start_s=day_start_sec,
        run_id_filter=(run_id or None), download_retries=3, sleeper=time.sleep,
    )
    if disc_mode != "archive_manifest_index":
        raise RuntimeError(f"verified archive index unavailable: mode={disc_mode}, warnings={warnings}")
    chosen = [c for c in candidates if c["exchange"] in exchanges and c["date_utc"] == date_utc]
    win_counts = Counter((c["exchange"], c["window_id"]) for c in chosen)
    if any(n != 1 for n in win_counts.values()):
        raise RuntimeError("duplicate verified partitions in archive index")
    per_ex = Counter(c["exchange"] for c in chosen)
    for ex in exchanges:
        if per_ex[ex] != WINDOWS_PER_DAY_15M:
            raise RuntimeError(f"{ex}: found {per_ex[ex]} verified windows for {date_utc}, need {WINDOWS_PER_DAY_15M}")

    cands_by_window: dict[str, list[dict[str, Any]]] = {}
    for c in chosen:
        cands_by_window.setdefault(str(c["window_id"]), []).append(c)
    protocol_audit = _audit_staged_protocol(cands_by_window, run_id, sig_ex, mak_ex, base_sym)

    # Fetch meta (chrony + specs) once
    meta_root = day_scratch / "meta_root"
    meta_root.mkdir(parents=True, exist_ok=True)
    meta_files = fetch_meta(REMOTE, date_utc, exchanges, meta_root)

    # Resolve contract spec (live snapshot or explicit config override)
    cspec_cfg = cfg["contract_spec"]
    if cspec_cfg["mode"] == "explicit":
        contract_spec = normalize_contract_spec(cspec_cfg["explicit"])
        contract_provenance = {"mode": "explicit", "spec_sha256": contract_spec_digest(contract_spec)}
    else:
        live = load_live_contract_from_meta(
            REMOTE, meta_root / "meta", mak_ex, mak_native, day_start_ns,
            quote_tick_mult=int(cspec_cfg.get("quote_tick_mult", 1)),
            hbt_tick_size_override=cspec_cfg.get("hbt_tick_size_override"),
        )
        contract_spec = live["contract"]
        contract_provenance = {
            "mode": "auto_live",
            "meta_file": live["meta_file"],
            "reply_wall_ns": live["reply_wall_ns"],
            "raw_sha256": live["raw_sha256"],
            "spec_sha256": live["spec_sha256"],
        }

    # Compute deterministic conversion cache key
    scripts_dir = Path(sys.modules["tokyo_l2_bt_stream_convert"].__file__).resolve().parent
    conv_sha = converter_code_sha256([
        scripts_dir / "tokyo_l2_bt_stream_convert.py",
        scripts_dir / "tokyo_l2_bt_convert.py",
        scripts_dir / "tokyo_l2_exchange_adapters.py",
    ])
    src_sha = source_partitions_digest(chosen)
    conv_ver = str(cfg["cache"]["converter_version"])
    spec_sha = contract_spec_digest(contract_spec)
    cache_key = compute_cache_key(
        signal_exchange=sig_ex, making_exchange=mak_ex, base_symbol=base_sym,
        date_utc=date_utc, source_sha256=src_sha, converter_sha256=conv_sha,
        converter_version=conv_ver, contract_spec_sha256=spec_sha,
    )

    # Try verified cache reuse first (fail-closed)
    converted_dir = day_scratch / "converted" / cache_key
    cache_hit = False
    cache_reason = "cache_disabled"
    cache_manifest: dict[str, Any] | None = None
    if cfg["cache"].get("reuse_converted", True):
        probe_dirs = []
        if cache_input_dir is not None:
            probe_dirs.extend([Path(cache_input_dir) / cache_key, Path(cache_input_dir)])
        if cfg["cache"].get("cache_input_dir"):
            cdir = Path(cfg["cache"]["cache_input_dir"])
            probe_dirs.extend([cdir / cache_key, cdir])
        probe_dirs.append(converted_dir)
        cache_reason = "cache_miss"
        for cand_dir in probe_dirs:
            ok, reason, man = verify_cache_dir(
                cand_dir,
                signal_exchange=sig_ex, making_exchange=mak_ex, base_symbol=base_sym,
                date_utc=date_utc, source_sha256=src_sha, converter_sha256=conv_sha,
                converter_version=conv_ver, contract_spec_sha256=spec_sha,
            )
            if ok and man is not None:
                converted_dir = cand_dir
                cache_hit = True
                cache_reason = reason
                cache_manifest = man
                print("CACHE_HIT", date_utc, cache_key, str(converted_dir), flush=True)
                break
            if reason != "missing_cache_manifest":
                cache_reason = reason

    download_s = 0.0
    convert_s = 0.0
    total_bytes = sum(int(c.get("data_bytes") or 0) for c in chosen)

    if cache_hit:
        stream_report = json.loads((converted_dir / "stream_report.json").read_text(encoding="utf-8"))
    else:
        converted_dir.mkdir(parents=True, exist_ok=True)
        state_dir = day_scratch / "state"
        parts_dir = state_dir / "parts"
        raw_stage = day_scratch / "raw_stage"
        state_dir.mkdir(parents=True, exist_ok=True)
        parts_dir.mkdir(parents=True, exist_ok=True)
        raw_stage.mkdir(parents=True, exist_ok=True)

        conv = StreamConverter(
            mak_native, config=collector_cfg, coverage_date=date_utc, making_exchange=mak_ex
        )
        batch_windows = int(cfg["execution"].get("batch_windows", 8))
        dl_workers = int(cfg["execution"].get("download_workers", 8))
        allow_gaps = bool(cfg["execution"].get("diagnostic_allow_known_gaps", True))

        by_window: dict[str, list[dict[str, Any]]] = {}
        for c in chosen:
            by_window.setdefault(str(c["window_id"]), []).append(c)
        ordered_windows = sorted(by_window.keys())

        def download_one(c: dict[str, Any]) -> tuple[str, Path, Path, tuple]:
            rel_m = _safe_rel(c["source_manifest"], "raw")
            rel_d = _safe_rel(c["source_data"], "raw")
            if str(rel_d) != str(rel_m).removesuffix(".manifest.json") + ".tsv.zst":
                raise ValueError("index data and manifest paths mismatch")
            mpath = raw_stage / rel_m
            dpath = raw_stage / rel_d
            mpath.parent.mkdir(parents=True, exist_ok=True)
            archive._download_with_retry(REMOTE, str(rel_m), mpath, retries=3, sleeper=time.sleep)
            archive._download_with_retry(REMOTE, str(rel_d), dpath, retries=3, sleeper=time.sleep)
            mf = json.loads(mpath.read_bytes())
            archive._verify_three_way_index_manifest_data(
                cand=c, manifest_path=mpath, manifest=mf, data_path=dpath, run_id_filter=(run_id or None)
            )
            return str(c["exchange"]), dpath, mpath, frozen_sort_key(mf, dpath)

        for b_start in range(0, len(ordered_windows), batch_windows):
            batch_wids = ordered_windows[b_start : b_start + batch_windows]
            batch_items = [c for wid in batch_wids for c in by_window[wid]]
            t_dl = time.time()
            with ThreadPoolExecutor(max_workers=dl_workers) as pool:
                downloaded = list(pool.map(download_one, batch_items))
            download_s += time.time() - t_dl
            downloaded.sort(key=lambda x: (x[3][0], x[0] != sig_ex, x[3]))

            t_cv = time.time()
            for _ex, dpath, mpath, _sk in downloaded:
                conv.process_partition(dpath, mpath, parts_dir, progress_every=0)
                if dpath.exists():
                    dpath.unlink()
            conv.checkpoint(state_dir)
            convert_s += time.time() - t_cv
            print(
                "BATCH_DONE", date_utc,
                f"windows={b_start + len(batch_wids)}/{len(ordered_windows)}",
                f"dl_s={download_s:.1f}", f"cv_s={convert_s:.1f}",
                f"rss_mb={peak_rss_mb()}",
                flush=True,
            )

        t_fin = time.time()
        stream_report = conv.finalize(
            parts_dir, converted_dir, meta_root / "meta", date_utc,
            diagnostic_allow_known_gaps=allow_gaps,
        )
        convert_s += time.time() - t_fin
        if stream_report["status"] not in ("ok", "ok_with_known_gaps"):
            raise RuntimeError(
                f"conversion blocked ({stream_report['status']}): {stream_report.get('failed_items')}"
            )
        cache_manifest = write_cache_manifest(
            converted_dir,
            signal_exchange=sig_ex, making_exchange=mak_ex, base_symbol=base_sym,
            date_utc=date_utc, source_sha256=src_sha, converter_sha256=conv_sha,
            converter_version=conv_ver, contract_spec_sha256=spec_sha,
            stream_report=stream_report,
        )
        shutil.rmtree(state_dir, ignore_errors=True)
        shutil.rmtree(raw_stage, ignore_errors=True)
        gc.collect()

    arts = {a["kind"]: converted_dir / a["path"] for a in stream_report.get("artifacts", [])}
    return {
        "date_utc": date_utc,
        "day_start_ns": day_start_ns,
        "day_end_ns": day_end_ns,
        "contract_spec": contract_spec,
        "contract_provenance": contract_provenance,
        "converted_dir": converted_dir,
        "artifacts": arts,
        "stream_report": stream_report,
        "cache": {
            "cache_hit": cache_hit,
            "reason": cache_reason,
            "cache_key": cache_key,
            "source_sha256": src_sha,
            "converter_sha256": conv_sha,
            "contract_spec_sha256": spec_sha,
            "manifest": cache_manifest,
        },
        "qa_and_data": {
            "index_effective_run_id": index_run_id,
            "partitions_verified": len(chosen),
            "compressed_bytes_total": total_bytes,
            "compressed_gib": round(total_bytes / (2 ** 30), 3),
            "download_elapsed_s": round(download_s, 1),
            "convert_elapsed_s": round(convert_s, 1),
            "meta_files_count": len(meta_files),
            "qa_day_report": qa_day_info,
            "protocol_audit": protocol_audit,
            "stream_status": stream_report.get("status"),
            "stream_failed_items": stream_report.get("failed_items"),
            "stream_peak_rss_mb": stream_report.get("peak_rss_mb"),
        },
    }


def _load_converted_arrays(artifacts: dict[str, Path]) -> dict[str, Any]:
    import numpy as np

    with np.load(artifacts["binance_bbo"], allow_pickle=False) as zb:
        bbo_local_ns = zb["local_ts_ns"].copy()
        bbo_mid = zb["mid_px"].copy()
    with np.load(artifacts["follower_l1_bbo"], allow_pickle=False) as zl:
        maker_l1_local_ns = zl["local_ts_ns"].copy()
        maker_l1_exch_ns = zl["exch_ts_ns"].copy()
        maker_l1_mid = zl["mid_px"].copy()
    return {
        "hbt_path": artifacts["follower_l2_hbt"],
        "bbo_local_ns": bbo_local_ns,
        "bbo_mid": bbo_mid,
        "maker_l1_local_ns": maker_l1_local_ns,
        "maker_l1_exch_ns": maker_l1_exch_ns,
        "maker_l1_mid": maker_l1_mid,
    }


def run_unified_experiment(
    config_path: Path,
    *,
    out_dir: Path = WORK,
    cache_input_dir: Path | None = None,
    collector_config_path: Path = DEFAULT_COLLECTOR_CONFIG,
    skip_kaggle_setup: bool = False,
) -> dict[str, Any]:
    _ensure_sys_path()
    from tokyo_l2_experiment_config import load_experiment_config
    from tokyo_l2_research_stages import run_models_on_arrays
    from tokyo_l2_report_writer import write_unified_reports

    cfg = load_experiment_config(config_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    report: dict[str, Any] = {
        "task": "tokyo_l2_unified_backtest",
        "status": "started",
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "config_path": str(config_path),
        "config": cfg,
        "setup": {},
        "days": {},
        "summary": {},
    }

    try:
        if cfg["data_source"] == "google_drive_tokyo_l2" and not skip_kaggle_setup:
            report["setup"] = setup_kaggle_env()

        stage4_lock = None
        if cfg["stage4"].get("lock_file"):
            stage4_lock = json.loads(_resolve_config_path(cfg["stage4"]["lock_file"]).read_text(encoding="utf-8"))
        stage5c_lock = None
        if cfg["stage5c"].get("lock_file"):
            stage5c_lock = json.loads(_resolve_config_path(cfg["stage5c"]["lock_file"]).read_text(encoding="utf-8"))

        SCRATCH.mkdir(parents=True, exist_ok=True)
        for date_utc in cfg["dates"]["eval_dates"]:
            t_day = time.time()
            day_scratch = SCRATCH / f"{date_utc}_{cfg['making_exchange']}_{cfg['base_symbol']}"
            day_scratch.mkdir(parents=True, exist_ok=True)
            try:
                if cfg["data_source"] != "google_drive_tokyo_l2":
                    raise NotImplementedError(
                        "Tardis live download stays on frozen legacy scripts; pass converted arrays "
                        "to tokyo_l2_research_stages.run_models_on_arrays for Tardis parity runs."
                    )
                prep = _fetch_and_convert_drive_day(
                    cfg=cfg,
                    date_utc=date_utc,
                    collector_config_path=collector_config_path,
                    cache_input_dir=cache_input_dir,
                    day_scratch=day_scratch,
                )
                anom_diag, episodes, roi_low, roi_high = _inspect_anomalies_and_book(
                    prep["artifacts"]["follower_l2_hbt"],
                    prep["stream_report"],
                    prep["qa_and_data"]["protocol_audit"].get("strategy_channel_intervals") or [],
                    cfg["signal_exchange"],
                    cfg["making_exchange"],
                    prep["day_start_ns"],
                    prep["day_end_ns"],
                )
                arrays = _load_converted_arrays(prep["artifacts"])
                research = run_models_on_arrays(
                    config=cfg,
                    date_utc=date_utc,
                    hbt_path=arrays["hbt_path"],
                    bbo_local_ns=arrays["bbo_local_ns"],
                    bbo_mid=arrays["bbo_mid"],
                    maker_l1_local_ns=arrays["maker_l1_local_ns"],
                    maker_l1_exch_ns=arrays["maker_l1_exch_ns"],
                    maker_l1_mid=arrays["maker_l1_mid"],
                    contract_spec=prep["contract_spec"],
                    day_start_ns=prep["day_start_ns"],
                    day_end_ns=prep["day_end_ns"],
                    roi_low=roi_low,
                    roi_high=roi_high,
                    episodes=episodes,
                    stage4_lock=stage4_lock,
                    stage5c_lock=stage5c_lock,
                )
                report["days"][date_utc] = {
                    "elapsed_s": round(time.time() - t_day, 1),
                    "peak_rss_mb": peak_rss_mb(),
                    "contract_provenance": prep["contract_provenance"],
                    "cache": prep["cache"],
                    "qa_and_data": prep["qa_and_data"],
                    "anomalies": anom_diag,
                    "research": research,
                }
            finally:
                # Immediately delete per-day raw & converted scratch data so disk stays clean
                shutil.rmtree(day_scratch, ignore_errors=True)
                gc.collect()

        report["status"] = "complete"
        report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        report["summary"] = {
            "unified_backtest_complete": True,
            "experiment_name": cfg["experiment_name"],
            "signal_exchange": cfg["signal_exchange"],
            "making_exchange": cfg["making_exchange"],
            "base_symbol": cfg["base_symbol"],
            "eval_dates": cfg["dates"]["eval_dates"],
            "models_configured": cfg["models"],
            "stages_configured": cfg["stages"],
            "stage4_authorized": bool(cfg["stage4"].get("authorized", False)),
            "stage5c_authorized": bool(cfg["stage5c"].get("authorized", False)),
            "disk_free_gib": {
                p: round(shutil.disk_usage(p).free / (2 ** 30), 2)
                for p in ("/kaggle/working", "/tmp") if Path(p).exists()
            },
        }
        report["written_reports"] = write_unified_reports(report, out_dir)
        print("UNIFIED_COMPLETE", json.dumps(report["summary"], ensure_ascii=False), flush=True)
        return report
    except Exception as exc:
        report["status"] = "failed"
        report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        report["error"] = f"{type(exc).__name__}: {exc}"[:2000]
        report["trace"] = traceback.format_exc()[-4000:]
        (out_dir / "unified_report.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8"
        )
        raise
    finally:
        shutil.rmtree(AUTH, ignore_errors=True)
        shutil.rmtree(SCRATCH, ignore_errors=True)


def _resolve_config_path(raw: str) -> Path:
    p = Path(raw)
    if p.is_file():
        return p
    for base in (CODE / "repo", Path(__file__).resolve().parents[2]):
        cand = base / raw
        if cand.is_file():
            return cand
    raise FileNotFoundError(f"experiment config not found: {raw}")


def _write_batch_summary(batch_report: dict[str, Any], out_dir: Path) -> None:
    from tokyo_l2_report_writer import render_batch_markdown_summary

    (out_dir / "batch_summary.json").write_text(
        json.dumps(batch_report, indent=2, ensure_ascii=False, default=str) + "\n",
        encoding="utf-8",
    )
    (out_dir / "batch_summary.md").write_text(
        render_batch_markdown_summary(batch_report),
        encoding="utf-8",
    )


def run_batch_experiments(
    batch_config_path: Path,
    *,
    out_dir: Path = WORK,
    collector_config_path: Path = DEFAULT_COLLECTOR_CONFIG,
    skip_kaggle_setup: bool = False,
) -> dict[str, Any]:
    """Run multiple same-day exchange/symbol experiments sequentially in ONE Kaggle job.

    Downloads & scans the signal exchange (Binance) 96 partitions ONCE via
    `MultiSymbolBinanceBBOExtractor` to produce shared per-symbol BBO files,
    then sequentially runs follower-only conversion (`StreamConverter(..., include_leader=False)`)
    + full model/stage research pipeline for each experiment, writing flat prefixed
    outputs (`{slug}__*`) and updating `batch_summary.json` after each pair.
    """
    _ensure_sys_path()
    from tokyo_l2_experiment_config import load_experiment_config
    from tokyo_l2_report_writer import write_unified_reports

    batch_spec = json.loads(Path(batch_config_path).read_text(encoding="utf-8"))
    batch_name = str(batch_spec["batch_name"])
    date_utc = str(batch_spec["date_utc"])
    sig_ex = str(batch_spec.get("signal_exchange") or "binance_futures")
    shared_symbols = sorted({str(s).upper() for s in batch_spec["shared_leader_symbols"]})
    exec_cfg = dict(batch_spec.get("execution") or {})
    batch_windows = int(exec_cfg.get("batch_windows", 8))
    dl_workers = int(exec_cfg.get("download_workers", 8))
    allow_gaps = bool(exec_cfg.get("diagnostic_allow_known_gaps", True))

    entries = list(batch_spec["experiments"])
    if not entries:
        raise ValueError("batch config has no experiments")
    seen_slugs: set[str] = set()
    loaded_entries: list[tuple[str, Path, dict[str, Any]]] = []
    for item in entries:
        slug = str(item["slug"]).strip()
        if not slug or slug in seen_slugs:
            raise ValueError(f"invalid or duplicate experiment slug: {slug!r}")
        seen_slugs.add(slug)
        cpath = _resolve_config_path(str(item["config"]))
        cfg = load_experiment_config(cpath)
        if cfg["data_source"] != "google_drive_tokyo_l2":
            raise ValueError(f"{slug}: batch mode requires google_drive_tokyo_l2")
        if cfg["signal_exchange"] != sig_ex:
            raise ValueError(f"{slug}: signal_exchange {cfg['signal_exchange']} != {sig_ex}")
        if cfg["dates"]["eval_dates"] != [date_utc]:
            raise ValueError(f"{slug}: eval_dates {cfg['dates']['eval_dates']} != [{date_utc}]")
        if cfg["base_symbol"] not in shared_symbols:
            raise ValueError(f"{slug}: base_symbol {cfg['base_symbol']} not in shared_leader_symbols {shared_symbols}")
        loaded_entries.append((slug, cpath, cfg))

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    t_batch = time.time()

    batch_report: dict[str, Any] = {
        "task": "tokyo_l2_unified_batch_backtest",
        "batch_name": batch_name,
        "batch_config_path": str(batch_config_path),
        "date_utc": date_utc,
        "signal_exchange": sig_ex,
        "shared_leader_symbols": shared_symbols,
        "status": "started",
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "setup": {},
        "shared_leader": {},
        "experiments": {},
        "summary": {
            "all_experiments_complete": False,
            "experiments_total": len(loaded_entries),
            "experiments_completed": 0,
            "experiments_failed": 0,
            "stage4_authorized": any(bool(cfg["stage4"].get("authorized", False)) for _, _, cfg in loaded_entries),
            "stage5c_authorized": any(bool(cfg["stage5c"].get("authorized", False)) for _, _, cfg in loaded_entries),
        },
    }
    _write_batch_summary(batch_report, out_dir)

    try:
        if not skip_kaggle_setup:
            batch_report["setup"] = setup_kaggle_env()

        import hourly_incremental_qa as archive
        from data_coverage import effective_config_hash
        from tokyo_l2_bt_fetch import (
            WINDOWS_PER_DAY_15M,
            _safe_rel,
            day_start_s,
            fetch_meta,
            find_day_report,
            integrity_gate,
        )
        from tokyo_l2_bt_stream_convert import (
            MultiSymbolBinanceBBOExtractor,
            StreamConverter,
            frozen_sort_key,
        )
        from tokyo_l2_exchange_adapters import (
            contract_spec_digest,
            load_live_contract_from_meta,
            normalize_contract_spec,
        )
        from tokyo_l2_research_stages import run_models_on_arrays

        collector_cfg = json.loads(Path(collector_config_path).read_text(encoding="utf-8"))
        cfg_hash = effective_config_hash(collector_cfg)
        day_start_sec = day_start_s(date_utc)
        day_start_ns = day_start_sec * NS
        day_end_ns = day_start_ns + DAY_NS

        # Load staged QA day report ONCE
        qa_day_doc: dict[str, Any] | None = None
        qa_day_file = ""
        run_id = ""
        if QA_DAYS.is_dir():
            rpath = find_day_report(QA_DAYS, date_utc, None)
            qa_day_file = rpath.name
            qa_day_doc = json.loads(rpath.read_text(encoding="utf-8"))
            run_id = str(qa_day_doc["run_id"])

        # Discover verified Drive partitions ONCE
        candidates, disc_mode, warnings, _, index_run_id = archive._discover_verified_candidates(
            REMOTE, collector_cfg, since_date=date_utc, since_start_s=day_start_sec,
            run_id_filter=(run_id or None), download_retries=3, sleeper=time.sleep,
        )
        if disc_mode != "archive_manifest_index":
            raise RuntimeError(f"verified archive index unavailable: mode={disc_mode}, warnings={warnings}")

        all_exchanges = tuple(sorted({sig_ex, *(cfg["making_exchange"] for _, _, cfg in loaded_entries)}))
        SCRATCH.mkdir(parents=True, exist_ok=True)
        shared_meta_root = SCRATCH / "shared_meta_root"
        shared_meta_root.mkdir(parents=True, exist_ok=True)
        meta_files = fetch_meta(REMOTE, date_utc, all_exchanges, shared_meta_root)

        def download_one(c: dict[str, Any], raw_stage: Path) -> tuple[str, Path, Path, tuple]:
            rel_m = _safe_rel(c["source_manifest"], "raw")
            rel_d = _safe_rel(c["source_data"], "raw")
            if str(rel_d) != str(rel_m).removesuffix(".manifest.json") + ".tsv.zst":
                raise ValueError("index data and manifest paths mismatch")
            mpath = raw_stage / rel_m
            dpath = raw_stage / rel_d
            mpath.parent.mkdir(parents=True, exist_ok=True)
            archive._download_with_retry(REMOTE, str(rel_m), mpath, retries=3, sleeper=time.sleep)
            archive._download_with_retry(REMOTE, str(rel_d), dpath, retries=3, sleeper=time.sleep)
            mf = json.loads(mpath.read_bytes())
            archive._verify_three_way_index_manifest_data(
                cand=c, manifest_path=mpath, manifest=mf, data_path=dpath, run_id_filter=(run_id or None)
            )
            return str(c["exchange"]), dpath, mpath, frozen_sort_key(mf, dpath)

        # ---- Phase 1: Single-pass shared Binance @bookTicker extraction ----
        leader_cands = [c for c in candidates if c["exchange"] == sig_ex and c["date_utc"] == date_utc]
        lwin_counts = Counter(c["window_id"] for c in leader_cands)
        if any(n != 1 for n in lwin_counts.values()) or len(leader_cands) != WINDOWS_PER_DAY_15M:
            raise RuntimeError(
                f"{sig_ex}: expected {WINDOWS_PER_DAY_15M} unique verified windows for {date_utc}, found {len(leader_cands)}"
            )
        leader_cands.sort(key=lambda c: str(c["window_id"]))
        leader_raw_stage = SCRATCH / "leader_raw_stage"
        leader_parts_dir = SCRATCH / "leader_parts"
        shared_leader_out = SCRATCH / "shared_leader_out"
        leader_raw_stage.mkdir(parents=True, exist_ok=True)
        leader_parts_dir.mkdir(parents=True, exist_ok=True)
        shared_leader_out.mkdir(parents=True, exist_ok=True)

        extractor = MultiSymbolBinanceBBOExtractor(
            shared_symbols, config=collector_cfg, coverage_date=date_utc
        )
        l_dl_s = 0.0
        l_cv_s = 0.0
        for b_start in range(0, len(leader_cands), batch_windows):
            batch_items = leader_cands[b_start : b_start + batch_windows]
            t_dl = time.time()
            with ThreadPoolExecutor(max_workers=dl_workers) as pool:
                downloaded = list(pool.map(lambda c: download_one(c, leader_raw_stage), batch_items))
            l_dl_s += time.time() - t_dl
            downloaded.sort(key=lambda x: x[3])
            t_cv = time.time()
            for _ex, dpath, mpath, _sk in downloaded:
                extractor.process_partition(dpath, mpath, leader_parts_dir)
                if dpath.exists():
                    dpath.unlink()
            l_cv_s += time.time() - t_cv
            print(
                "SHARED_LEADER_BATCH", date_utc,
                f"windows={b_start + len(batch_items)}/{len(leader_cands)}",
                f"dl_s={l_dl_s:.1f}", f"cv_s={l_cv_s:.1f}",
                f"rss_mb={peak_rss_mb()}",
                flush=True,
            )

        t_fin = time.time()
        leader_report = extractor.finalize(
            leader_parts_dir, shared_leader_out, shared_meta_root / "meta", date_utc,
            diagnostic_allow_known_gaps=allow_gaps,
        )
        l_cv_s += time.time() - t_fin
        shutil.rmtree(leader_parts_dir, ignore_errors=True)
        shutil.rmtree(leader_raw_stage, ignore_errors=True)
        gc.collect()
        if leader_report["status"] not in ("ok", "ok_with_known_gaps"):
            raise RuntimeError(
                f"shared leader extraction blocked ({leader_report['status']}): {leader_report.get('failed_items')}"
            )
        batch_report["shared_leader"] = {
            "status": leader_report["status"],
            "failed_items": leader_report.get("failed_items"),
            "partitions_verified": len(leader_cands),
            "compressed_gib": round(sum(int(c.get("data_bytes") or 0) for c in leader_cands) / (2 ** 30), 3),
            "download_elapsed_s": round(l_dl_s, 1),
            "convert_elapsed_s": round(l_cv_s, 1),
            "artifacts": leader_report["artifacts"],
            "switches": leader_report.get("switches"),
            "gaps": leader_report.get("gaps"),
            "meta_files_count": len(meta_files),
        }
        _write_batch_summary(batch_report, out_dir)
        print("SHARED_LEADER_DONE", json.dumps({
            "status": leader_report["status"],
            "dl_s": round(l_dl_s, 1), "cv_s": round(l_cv_s, 1),
            "rows": {s: a["rows"] for s, a in leader_report["artifacts"].items()},
        }), flush=True)

        # ---- Phase 2: Sequential per-pair follower-only convert + research ----
        for slug, cpath, cfg in loaded_entries:
            t_pair = time.time()
            mak_ex = cfg["making_exchange"]
            base_sym = cfg["base_symbol"]
            mak_native = cfg["making_native_symbol"]
            pair_scratch = SCRATCH / f"pair_{slug}"
            pair_scratch.mkdir(parents=True, exist_ok=True)
            print("PAIR_START", slug, f"{sig_ex}->{mak_ex}", base_sym, date_utc, flush=True)

            try:
                # Per-pair truthful QA day gate + 96-window protocol audit
                qa_day_info: dict[str, Any] = {"present": False}
                if qa_day_doc is not None:
                    gate = integrity_gate(qa_day_doc, (sig_ex, mak_ex), cfg_hash, date_utc)
                    qa_day_info = {
                        "present": True,
                        "report_file": qa_day_file,
                        "run_id": run_id,
                        "production_qa_status_all_exchanges": qa_day_doc.get("qa_status"),
                        "strategy_exchanges_gate": gate,
                    }

                follower_cands = [c for c in candidates if c["exchange"] == mak_ex and c["date_utc"] == date_utc]
                fwin_counts = Counter(c["window_id"] for c in follower_cands)
                if any(n != 1 for n in fwin_counts.values()) or len(follower_cands) != WINDOWS_PER_DAY_15M:
                    raise RuntimeError(
                        f"{mak_ex}: expected {WINDOWS_PER_DAY_15M} unique verified windows for {date_utc}, found {len(follower_cands)}"
                    )
                follower_cands.sort(key=lambda c: str(c["window_id"]))

                cands_by_window: dict[str, list[dict[str, Any]]] = {}
                for c in leader_cands + follower_cands:
                    cands_by_window.setdefault(str(c["window_id"]), []).append(c)
                protocol_audit = _audit_staged_protocol(cands_by_window, run_id, sig_ex, mak_ex, base_sym)

                # Resolve contract spec for this making exchange + symbol
                cspec_cfg = cfg["contract_spec"]
                if cspec_cfg["mode"] == "explicit":
                    contract_spec = normalize_contract_spec(cspec_cfg["explicit"])
                    contract_provenance = {"mode": "explicit", "spec_sha256": contract_spec_digest(contract_spec)}
                else:
                    live = load_live_contract_from_meta(
                        REMOTE, shared_meta_root / "meta", mak_ex, mak_native, day_start_ns,
                        quote_tick_mult=int(cspec_cfg.get("quote_tick_mult", 1)),
                        hbt_tick_size_override=cspec_cfg.get("hbt_tick_size_override"),
                    )
                    contract_spec = live["contract"]
                    contract_provenance = {
                        "mode": "auto_live",
                        "meta_file": live["meta_file"],
                        "reply_wall_ns": live["reply_wall_ns"],
                        "raw_sha256": live["raw_sha256"],
                        "spec_sha256": live["spec_sha256"],
                    }

                # Download & convert follower partitions ONLY (reuse shared Binance BBO)
                f_raw_stage = pair_scratch / "raw_stage"
                f_state_dir = pair_scratch / "state"
                f_parts_dir = f_state_dir / "parts"
                f_converted_dir = pair_scratch / "converted"
                for d in (f_raw_stage, f_state_dir, f_parts_dir, f_converted_dir):
                    d.mkdir(parents=True, exist_ok=True)

                conv = StreamConverter(
                    mak_native, config=collector_cfg, coverage_date=date_utc,
                    making_exchange=mak_ex, include_leader=False,
                )
                p_batch_win = int(cfg["execution"].get("batch_windows", batch_windows))
                p_dl_workers = int(cfg["execution"].get("download_workers", dl_workers))
                p_allow_gaps = bool(cfg["execution"].get("diagnostic_allow_known_gaps", allow_gaps))
                f_dl_s = 0.0
                f_cv_s = 0.0
                for b_start in range(0, len(follower_cands), p_batch_win):
                    batch_items = follower_cands[b_start : b_start + p_batch_win]
                    t_dl = time.time()
                    with ThreadPoolExecutor(max_workers=p_dl_workers) as pool:
                        downloaded = list(pool.map(lambda c: download_one(c, f_raw_stage), batch_items))
                    f_dl_s += time.time() - t_dl
                    downloaded.sort(key=lambda x: x[3])
                    t_cv = time.time()
                    for _ex, dpath, mpath, _sk in downloaded:
                        conv.process_partition(dpath, mpath, f_parts_dir, progress_every=0)
                        if dpath.exists():
                            dpath.unlink()
                    conv.checkpoint(f_state_dir)
                    f_cv_s += time.time() - t_cv
                    print(
                        "PAIR_BATCH_DONE", slug,
                        f"windows={b_start + len(batch_items)}/{len(follower_cands)}",
                        f"dl_s={f_dl_s:.1f}", f"cv_s={f_cv_s:.1f}",
                        f"rss_mb={peak_rss_mb()}",
                        flush=True,
                    )

                t_fin = time.time()
                follower_stream_report = conv.finalize(
                    f_parts_dir, f_converted_dir, shared_meta_root / "meta", date_utc,
                    diagnostic_allow_known_gaps=p_allow_gaps,
                )
                f_cv_s += time.time() - t_fin
                shutil.rmtree(f_state_dir, ignore_errors=True)
                shutil.rmtree(f_raw_stage, ignore_errors=True)
                gc.collect()
                if follower_stream_report["status"] not in ("ok", "ok_with_known_gaps"):
                    raise RuntimeError(
                        f"{slug}: follower conversion blocked ({follower_stream_report['status']}): "
                        f"{follower_stream_report.get('failed_items')}"
                    )

                merged_stream_report = {
                    **follower_stream_report,
                    "gaps": {
                        **(leader_report.get("gaps") or {}),
                        **(follower_stream_report.get("gaps") or {}),
                    },
                    "switches": {
                        **(leader_report.get("switches") or {}),
                        **(follower_stream_report.get("switches") or {}),
                    },
                    "exch_ahead_of_local": {
                        **(follower_stream_report.get("exch_ahead_of_local") or {}),
                        "binance_bbo": int((leader_report.get("exch_ahead_of_local") or {}).get(base_sym, 0)),
                    },
                    "shared_leader": {
                        "status": leader_report["status"],
                        "failed_items": leader_report.get("failed_items"),
                        "artifact": leader_report["artifacts"][base_sym],
                    },
                }

                arts = {a["kind"]: f_converted_dir / a["path"] for a in follower_stream_report.get("artifacts", [])}
                arts["binance_bbo"] = shared_leader_out / leader_report["artifacts"][base_sym]["path"]

                anom_diag, episodes, roi_low, roi_high = _inspect_anomalies_and_book(
                    arts["follower_l2_hbt"],
                    merged_stream_report,
                    protocol_audit.get("strategy_channel_intervals") or [],
                    sig_ex,
                    mak_ex,
                    day_start_ns,
                    day_end_ns,
                )
                arrays = _load_converted_arrays(arts)

                stage4_lock = (
                    json.loads(_resolve_config_path(cfg["stage4"]["lock_file"]).read_text(encoding="utf-8"))
                    if cfg["stage4"].get("lock_file") else None
                )
                stage5c_lock = (
                    json.loads(_resolve_config_path(cfg["stage5c"]["lock_file"]).read_text(encoding="utf-8"))
                    if cfg["stage5c"].get("lock_file") else None
                )
                research = run_models_on_arrays(
                    config=cfg,
                    date_utc=date_utc,
                    hbt_path=arrays["hbt_path"],
                    bbo_local_ns=arrays["bbo_local_ns"],
                    bbo_mid=arrays["bbo_mid"],
                    maker_l1_local_ns=arrays["maker_l1_local_ns"],
                    maker_l1_exch_ns=arrays["maker_l1_exch_ns"],
                    maker_l1_mid=arrays["maker_l1_mid"],
                    contract_spec=contract_spec,
                    day_start_ns=day_start_ns,
                    day_end_ns=day_end_ns,
                    roi_low=roi_low,
                    roi_high=roi_high,
                    episodes=episodes,
                    stage4_lock=stage4_lock,
                    stage5c_lock=stage5c_lock,
                )

                f_bytes = sum(int(c.get("data_bytes") or 0) for c in follower_cands)
                pair_elapsed = round(time.time() - t_pair, 1)
                pair_report: dict[str, Any] = {
                    "task": "tokyo_l2_unified_backtest",
                    "batch_slug": slug,
                    "batch_name": batch_name,
                    "status": "complete",
                    "started_utc": batch_report["started_utc"],
                    "finished_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "config_path": str(cpath),
                    "config": cfg,
                    "setup": batch_report["setup"],
                    "days": {
                        date_utc: {
                            "elapsed_s": pair_elapsed,
                            "peak_rss_mb": peak_rss_mb(),
                            "contract_provenance": contract_provenance,
                            "cache": {
                                "cache_hit": False,
                                "reason": "shared_leader_batch_mode",
                                "shared_leader_artifact": leader_report["artifacts"][base_sym],
                            },
                            "qa_and_data": {
                                "index_effective_run_id": index_run_id,
                                "partitions_verified": len(leader_cands) + len(follower_cands),
                                "follower_partitions_verified": len(follower_cands),
                                "follower_compressed_bytes_total": f_bytes,
                                "follower_compressed_gib": round(f_bytes / (2 ** 30), 3),
                                "follower_download_elapsed_s": round(f_dl_s, 1),
                                "follower_convert_elapsed_s": round(f_cv_s, 1),
                                "shared_leader_download_elapsed_s": round(l_dl_s, 1),
                                "shared_leader_convert_elapsed_s": round(l_cv_s, 1),
                                "meta_files_count": len(meta_files),
                                "qa_day_report": qa_day_info,
                                "protocol_audit": protocol_audit,
                                "stream_status": follower_stream_report.get("status"),
                                "stream_failed_items": follower_stream_report.get("failed_items"),
                                "shared_leader_status": leader_report.get("status"),
                                "shared_leader_failed_items": leader_report.get("failed_items"),
                                "stream_peak_rss_mb": follower_stream_report.get("peak_rss_mb"),
                            },
                            "anomalies": anom_diag,
                            "research": research,
                        }
                    },
                    "summary": {
                        "unified_backtest_complete": True,
                        "experiment_name": cfg["experiment_name"],
                        "signal_exchange": sig_ex,
                        "making_exchange": mak_ex,
                        "base_symbol": base_sym,
                        "eval_dates": cfg["dates"]["eval_dates"],
                        "models_configured": cfg["models"],
                        "stages_configured": cfg["stages"],
                        "stage4_authorized": bool(cfg["stage4"].get("authorized", False)),
                        "stage5c_authorized": bool(cfg["stage5c"].get("authorized", False)),
                    },
                }
                written = write_unified_reports(pair_report, out_dir, prefix=f"{slug}__")
                pair_report["written_reports"] = written

                s3 = research.get("stage3") or {}
                batch_report["experiments"][slug] = {
                    "status": "complete",
                    "experiment_name": cfg["experiment_name"],
                    "signal_exchange": sig_ex,
                    "making_exchange": mak_ex,
                    "base_symbol": base_sym,
                    "making_native_symbol": mak_native,
                    "elapsed_s": pair_elapsed,
                    "follower_download_elapsed_s": round(f_dl_s, 1),
                    "follower_convert_elapsed_s": round(f_cv_s, 1),
                    "contract_spec": contract_spec,
                    "stream_status": follower_stream_report.get("status"),
                    "stream_failed_items": follower_stream_report.get("failed_items"),
                    "qa_day_gate_passed": (qa_day_info.get("strategy_exchanges_gate") or {}).get("passed"),
                    "protocol_affected_windows_count": len(protocol_audit.get("strategy_channels_affected_windows") or []),
                    "anomaly_episode_count": anom_diag.get("episode_count"),
                    "stage3_parity_verified": s3.get("parity_verified"),
                    "stage3_category_counts": s3.get("category_counts"),
                    "stage4_status": (research.get("stage4") or {}).get("status"),
                    "stage5c_status": (research.get("stage5c") or {}).get("status"),
                    "stage5a_table": (research.get("stage5a") or {}).get("table") or [],
                    "written_reports": written,
                }
                batch_report["summary"]["experiments_completed"] += 1
                print("PAIR_COMPLETE", slug, f"elapsed_s={pair_elapsed}", flush=True)
            except Exception as exc:
                batch_report["experiments"][slug] = {
                    "status": "failed",
                    "experiment_name": cfg["experiment_name"],
                    "signal_exchange": sig_ex,
                    "making_exchange": mak_ex,
                    "base_symbol": base_sym,
                    "elapsed_s": round(time.time() - t_pair, 1),
                    "error": f"{type(exc).__name__}: {exc}"[:2000],
                    "trace": traceback.format_exc()[-4000:],
                }
                batch_report["summary"]["experiments_failed"] += 1
                print("PAIR_FAILED", slug, f"{type(exc).__name__}: {exc}", flush=True)
            finally:
                shutil.rmtree(pair_scratch, ignore_errors=True)
                gc.collect()
                _write_batch_summary(batch_report, out_dir)

        all_ok = (
            batch_report["summary"]["experiments_completed"] == batch_report["summary"]["experiments_total"]
            and batch_report["summary"]["experiments_failed"] == 0
        )
        batch_report["status"] = "complete" if all_ok else "failed"
        batch_report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        batch_report["elapsed_s"] = round(time.time() - t_batch, 1)
        batch_report["summary"]["all_experiments_complete"] = all_ok
        batch_report["summary"]["disk_free_gib"] = {
            p: round(shutil.disk_usage(p).free / (2 ** 30), 2)
            for p in ("/kaggle/working", "/tmp") if Path(p).exists()
        }
        _write_batch_summary(batch_report, out_dir)
        print("BATCH_COMPLETE", json.dumps(batch_report["summary"], ensure_ascii=False), flush=True)
        if not all_ok:
            failed_slugs = [k for k, v in batch_report["experiments"].items() if v.get("status") != "complete"]
            raise RuntimeError(f"batch run incomplete; failed experiments: {failed_slugs}")
        return batch_report
    except Exception as exc:
        if batch_report.get("status") != "failed":
            batch_report["status"] = "failed"
            batch_report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            batch_report["error"] = f"{type(exc).__name__}: {exc}"[:2000]
            batch_report["trace"] = traceback.format_exc()[-4000:]
            _write_batch_summary(batch_report, out_dir)
        raise
    finally:
        shutil.rmtree(AUTH, ignore_errors=True)
        shutil.rmtree(SCRATCH, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Unified Tokyo L2 backtest & five-stage research runner")
    p.add_argument("--config", default="configs/tokyo_l2_20261123_gate_btc.json",
                   help="path to unified experiment config JSON")
    p.add_argument("--batch-config", default="",
                   help="optional path to multi-pair batch config JSON (shares single Binance BBO pass)")
    p.add_argument("--out-dir", type=Path, default=WORK, help="output directory for summary reports")
    p.add_argument("--cache-input-dir", type=Path, default=None,
                   help="optional read-only private cache directory with cache_manifest.json")
    p.add_argument("--collector-config", type=Path, default=DEFAULT_COLLECTOR_CONFIG)
    args = p.parse_args(argv)
    if args.batch_config:
        bpath = _resolve_config_path(args.batch_config)
        run_batch_experiments(
            bpath,
            out_dir=args.out_dir,
            collector_config_path=args.collector_config,
        )
        return 0
    cfg_path = _resolve_config_path(args.config)
    run_unified_experiment(
        cfg_path,
        out_dir=args.out_dir,
        cache_input_dir=args.cache_input_dir,
        collector_config_path=args.collector_config,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
