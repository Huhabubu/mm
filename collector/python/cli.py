#!/usr/bin/env python3
"""Unified CLI entrypoint for AWS Tokyo L2 Collector (SPEC_v1.4 Section 6.3).

Four standardized subcommands:
  preflight  --config CONFIG --offline
  collect    --config CONFIG --run-id ID [--allow-live-run]
  archive    --dir SEALED [--verify-remote] [--remote REMOTE] [--prune-hours H]
  qa-convert --manifest VERIFIED --out DIR [--meta-dir DIR] [--allow-unverified-local]
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

PYTHON_DIR = Path(__file__).resolve().parent
COLLECTOR_DIR = PYTHON_DIR.parent
REPO_ROOT = COLLECTOR_DIR.parent

if str(PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(PYTHON_DIR))

from archive_sync import RcloneBackend, RemoteObjectInfo, sync_sealed_directory  # noqa: E402
from qa_convert import run_qa_convert  # noqa: E402


def _blocked(phase: str, failed_items: list[str], minimal_fixes: list[str]) -> dict[str, Any]:
    return {
        "status": "blocked",
        "phase": phase,
        "failed_items": failed_items,
        "minimal_fixes": minimal_fixes,
    }


def _find_or_build_rust_binary() -> Path:
    env_bin = os.environ.get("TOKYO_L2_COLLECTOR_BIN")
    if env_bin and Path(env_bin).is_file():
        return Path(env_bin)
    for rel in ("target/release/tokyo-l2-collector", "target/debug/tokyo-l2-collector"):
        cand = COLLECTOR_DIR / rel
        if cand.is_file():
            return cand
    # Auto-build debug binary once if not yet built
    proc = subprocess.run(
        ["cargo", "build", "--manifest-path", str(COLLECTOR_DIR / "Cargo.toml")],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"cargo build failed:\n{proc.stderr}")
    built = COLLECTOR_DIR / "target/debug/tokyo-l2-collector"
    if not built.is_file():
        raise RuntimeError(f"Expected binary not found after cargo build: {built}")
    return built


class _LocalMemoryRemoteBackend:
    """In-memory mock remote backend used strictly inside offline preflight smoke verification."""

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}

    def stat_remote(self, remote_rel_path: str) -> RemoteObjectInfo:
        blob = self.store.get(remote_rel_path)
        if blob is None:
            return RemoteObjectInfo(exists=False)
        return RemoteObjectInfo(
            exists=True,
            remote_id=f"mem:{hashlib.md5(remote_rel_path.encode()).hexdigest()[:12]}",
            size=len(blob),
            md5=hashlib.md5(blob).hexdigest(),
            sha256=hashlib.sha256(blob).hexdigest(),
        )

    def copy_file(self, local_path: Path, remote_rel_path: str, *, immutable: bool = True) -> None:
        self.store[remote_rel_path] = local_path.read_bytes()

    def download_file(self, remote_rel_path: str, local_dest: Path) -> None:
        local_dest.parent.mkdir(parents=True, exist_ok=True)
        local_dest.write_bytes(self.store[remote_rel_path])

    def readback_sha256(self, remote_rel_path: str) -> str:
        return hashlib.sha256(self.store[remote_rel_path]).hexdigest()


def _build_preflight_smoke_fixture(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Build a small deterministic offline fixture matching whichever exchanges are enabled in CONFIG."""
    base_wall = 1_796_256_000_000_000_000  # 2026-12-03T00:00:00Z
    base_ms = base_wall // 1_000_000
    events: list[dict[str, Any]] = []
    ex = cfg.get("exchanges", {})

    if ex.get("binance_futures", {}).get("enabled"):
        events.extend(
            [
                {
                    "op": "frame",
                    "recv_wall_ns": base_wall + 1_000_000,
                    "recv_mono_ns": 1_000_000,
                    "endpoint": "binance_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "stream": "btcusdt@bookTicker",
                            "data": {
                                "e": "bookTicker",
                                "u": 1001,
                                "s": "BTCUSDT",
                                "b": "95000.0",
                                "B": "4.2",
                                "a": "95000.1",
                                "A": "3.8",
                                "T": base_ms,
                                "E": base_ms,
                            },
                        }
                    ),
                },
                {
                    "op": "frame",
                    "recv_wall_ns": base_wall + 2_000_000,
                    "recv_mono_ns": 2_000_000,
                    "endpoint": "binance_public",
                    "conn_id": "B",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "stream": "btcusdt@bookTicker",
                            "data": {
                                "e": "bookTicker",
                                "u": 1001,
                                "s": "BTCUSDT",
                                "b": "95000.0",
                                "B": "4.2",
                                "a": "95000.1",
                                "A": "3.8",
                                "T": base_ms,
                                "E": base_ms,
                            },
                        }
                    ),
                },
                {
                    "op": "frame",
                    "recv_wall_ns": base_wall + 3_000_000,
                    "recv_mono_ns": 3_000_000,
                    "endpoint": "binance_market",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "stream": "btcusdt@aggTrade",
                            "data": {
                                "e": "aggTrade",
                                "E": base_ms + 2,
                                "a": 99001,
                                "s": "BTCUSDT",
                                "p": "95000.1",
                                "q": "0.25",
                                "f": 500,
                                "l": 502,
                                "T": base_ms + 2,
                                "m": False,
                            },
                        }
                    ),
                },
            ]
        )

    if ex.get("gate_futures", {}).get("enabled"):
        events.extend(
            [
                {
                    "op": "frame",
                    "recv_wall_ns": base_wall + 1_500_000,
                    "recv_mono_ns": 1_500_000,
                    "endpoint": "gate_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "time": base_ms // 1000,
                            "time_ms": base_ms,
                            "channel": "futures.obu",
                            "event": "update",
                            "result": {
                                "t": base_ms,
                                "full": True,
                                "s": "ob.BTC_USDT.50",
                                "u": 500,
                                "b": [["95000.0", "1.5"], ["94999.9", "2.5"]],
                                "a": [["95000.1", "0.8"], ["95000.2", "3.1"]],
                            },
                        }
                    ),
                },
                {
                    "op": "frame",
                    "recv_wall_ns": base_wall + 5_000_000,
                    "recv_mono_ns": 5_000_000,
                    "endpoint": "gate_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "time": base_ms // 1000,
                            "time_ms": base_ms + 4,
                            "channel": "futures.obu",
                            "event": "update",
                            "result": {
                                "t": base_ms + 4,
                                "full": False,
                                "s": "ob.BTC_USDT.50",
                                "U": 501,
                                "u": 501,
                                "b": [["95000.0", "2.1"]],
                                "a": [],
                            },
                        }
                    ),
                },
                {
                    "op": "frame",
                    "recv_wall_ns": base_wall + 6_000_000,
                    "recv_mono_ns": 6_000_000,
                    "endpoint": "gate_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "time": base_ms // 1000,
                            "time_ms": base_ms + 5,
                            "channel": "futures.trades",
                            "event": "update",
                            "result": [
                                {
                                    "id": 77701,
                                    "contract": "BTC_USDT",
                                    "size": "0.3",
                                    "price": "95000.1",
                                    "create_time_ms": base_ms + 5,
                                    "is_internal": False,
                                }
                            ],
                        }
                    ),
                },
                {
                    "op": "frame",
                    "recv_wall_ns": base_wall + 7_000_000,
                    "recv_mono_ns": 7_000_000,
                    "endpoint": "gate_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "time": base_ms // 1000,
                            "time_ms": base_ms + 6,
                            "channel": "futures.book_ticker",
                            "event": "update",
                            "result": {
                                "t": base_ms + 6,
                                "u": 888,
                                "s": "BTC_USDT",
                                "b": "95000.0",
                                "B": "2.1",
                                "a": "95000.1",
                                "A": "0.8",
                            },
                        }
                    ),
                },
            ]
        )

    if ex.get("bybit_linear", {}).get("enabled"):
        events.extend(
            [
                {
                    "op": "frame",
                    "recv_wall_ns": base_wall + 1_600_000,
                    "recv_mono_ns": 1_600_000,
                    "endpoint": "bybit_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "topic": "orderbook.50.BTCUSDT",
                            "type": "snapshot",
                            "ts": base_ms,
                            "cts": base_ms,
                            "data": {
                                "s": "BTCUSDT",
                                "b": [["95000.0", "1.2"]],
                                "a": [["95000.1", "1.4"]],
                                "u": 100,
                                "seq": 1000,
                            },
                        }
                    ),
                },
                {
                    "op": "frame",
                    "recv_wall_ns": base_wall + 5_500_000,
                    "recv_mono_ns": 5_500_000,
                    "endpoint": "bybit_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "topic": "publicTrade.BTCUSDT",
                            "type": "snapshot",
                            "ts": base_ms + 5,
                            "data": [
                                {
                                    "T": base_ms + 5,
                                    "s": "BTCUSDT",
                                    "S": "Buy",
                                    "v": "0.1",
                                    "p": "95000.1",
                                    "i": "bybit-tr-1",
                                    "BT": False,
                                    "RPI": False,
                                    "seq": 1001,
                                }
                            ],
                        }
                    ),
                },
            ]
        )

    if ex.get("bitget_v3", {}).get("enabled"):
        events.extend(
            [
                {
                    "op": "frame",
                    "recv_wall_ns": base_wall + 1_700_000,
                    "recv_mono_ns": 1_700_000,
                    "endpoint": "bitget_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "action": "snapshot",
                            "arg": {"instType": "usdt-futures", "channel": "books50", "instId": "BTCUSDT"},
                            "data": [
                                {
                                    "a": [["95000.1", "2.0"]],
                                    "b": [["95000.0", "2.2"]],
                                    "ts": str(base_ms),
                                    "seq": 5000,
                                    "pseq": 0,
                                }
                            ],
                            "ts": base_ms,
                        }
                    ),
                },
                {
                    "op": "frame",
                    "recv_wall_ns": base_wall + 5_700_000,
                    "recv_mono_ns": 5_700_000,
                    "endpoint": "bitget_public",
                    "conn_id": "A",
                    "epoch": 1,
                    "payload": json.dumps(
                        {
                            "action": "update",
                            "arg": {"instType": "usdt-futures", "channel": "books50", "instId": "BTCUSDT"},
                            "data": [
                                {
                                    "a": [["95000.1", "1.7"]],
                                    "b": [],
                                    "ts": str(base_ms + 5),
                                    "seq": 5001,
                                    "pseq": 5000,
                                }
                            ],
                            "ts": base_ms + 5,
                        }
                    ),
                },
            ]
        )

    return events


def cmd_preflight(args: argparse.Namespace) -> int:
    failed: list[str] = []
    fixes: list[str] = []

    if not args.offline:
        rep = _blocked(
            "preflight",
            ["Online preflight requires explicit P2 AWS Tokyo authorization"],
            ["Run `preflight --config CONFIG --offline` during P0/P1"],
        )
        print(json.dumps(rep, indent=2, ensure_ascii=False))
        return 2

    # 1. Verify pinned Python dependencies
    try:
        hbt_ver = importlib.metadata.version("hftbacktest")
        if hbt_ver != "2.4.4":
            failed.append(f"hftbacktest version mismatch: found {hbt_ver}, required 2.4.4")
            fixes.append("Install hftbacktest==2.4.4 in .venv")
    except Exception as exc:
        failed.append(f"hftbacktest not installed: {exc}")
        fixes.append("Install hftbacktest==2.4.4 in .venv")
        hbt_ver = "missing"

    # 2. Verify P0 spec files exist and parse cleanly
    spec_files = [
        COLLECTOR_DIR / "specs" / "protocol_matrix.json",
        COLLECTOR_DIR / "specs" / "instrument_map.json",
        COLLECTOR_DIR / "specs" / "research_bindings.json",
    ]
    spec_hashes: dict[str, str] = {}
    for sf in spec_files:
        if not sf.is_file():
            failed.append(f"Missing P0 artifact: {sf}")
            fixes.append(f"Create {sf.name} under collector/specs/")
        else:
            try:
                parsed = json.loads(sf.read_text(encoding="utf-8"))
                spec_hashes[sf.name] = hashlib.sha256(sf.read_bytes()).hexdigest()
                if sf.name == "research_bindings.json":
                    sha = parsed.get("frozen_research_sha")
                    if sha is not None and not (isinstance(sha, str) and len(sha) == 40):
                        failed.append(f"Invalid frozen_research_sha in research_bindings.json: {sha!r}")
                        fixes.append("Set frozen_research_sha to null (pre-P3) or a valid 40-char git SHA")
            except Exception as exc:
                failed.append(f"Invalid JSON in {sf.name}: {exc}")
                fixes.append(f"Fix JSON syntax in {sf}")

    # 3. Validate config file and run Rust preflight
    if not args.config.is_file():
        failed.append(f"Config file not found: {args.config}")
        fixes.append("Pass a valid --config path (e.g. collector/specs/config.example.json)")
        print(json.dumps(_blocked("preflight", failed, fixes), indent=2, ensure_ascii=False))
        return 2

    cfg = json.loads(args.config.read_text(encoding="utf-8"))

    try:
        rust_bin = _find_or_build_rust_binary()
    except Exception as exc:
        failed.append(str(exc))
        fixes.append("Ensure Rust toolchain is installed and `cargo build` succeeds")
        print(json.dumps(_blocked("preflight", failed, fixes), indent=2, ensure_ascii=False))
        return 2

    rust_preflight = subprocess.run(
        [str(rust_bin), "preflight", "--config", str(args.config), "--offline"],
        capture_output=True,
        text=True,
    )
    if rust_preflight.returncode != 0:
        failed.append(f"Rust preflight failed (exit={rust_preflight.returncode}): {rust_preflight.stdout.strip()}")
        fixes.append("Fix config protocol violations reported by Rust preflight")
        print(json.dumps(_blocked("preflight", failed, fixes), indent=2, ensure_ascii=False))
        return 2
    rust_preflight_json = json.loads(rust_preflight.stdout)

    # 4. Run offline closed-loop deterministic smoke verification (replay-fixture -> archive -> qa-convert)
    smoke_result: dict[str, Any] = {}
    if not failed:
        with tempfile.TemporaryDirectory(prefix="tokyo_l2_preflight_") as tmpdir:
            tmp = Path(tmpdir)
            fixture_path = tmp / "smoke.jsonl"
            sealed_dir = tmp / "sealed"
            out_dir = tmp / "converted"
            smoke_events = _build_preflight_smoke_fixture(cfg)
            fixture_path.write_text("\n".join(json.dumps(e) for e in smoke_events) + "\n", encoding="utf-8")

            rp = subprocess.run(
                [
                    str(rust_bin),
                    "replay-fixture",
                    "--config",
                    str(args.config),
                    "--fixture",
                    str(fixture_path),
                    "--out",
                    str(sealed_dir),
                    "--boot-id",
                    "preflight_smoke_1",
                ],
                capture_output=True,
                text=True,
            )
            if rp.returncode != 0:
                failed.append(f"Offline replay-fixture failed: {rp.stdout} {rp.stderr}")
                fixes.append("Inspect Rust replay-fixture error")
            else:
                backend = _LocalMemoryRemoteBackend()
                arch = sync_sealed_directory(sealed_dir, backend=backend, verify_remote=True)
                if arch["status"] != "ok":
                    failed.append(f"Offline archive_sync failed: {arch.get('failed_items')}")
                    fixes.append("Inspect archive_sync verification")
                else:
                    ledger = Path(arch["ledger_path"])
                    qa = run_qa_convert(ledger, out_dir, strict_zero_loss=True)
                    if qa["status"] != "ok":
                        failed.append(f"Offline qa_convert failed: {qa.get('failed_items')}")
                        fixes.append("Inspect qa_convert output")
                    else:
                        smoke_result = {
                            "sealed_partitions": arch["verified_count"],
                            "eligible_asset_days": qa["eligible_asset_days"],
                            "execution_unverified_asset_days": qa["execution_unverified_asset_days"],
                            "artifacts_count": len(qa["artifacts"]),
                        }

    if failed:
        print(json.dumps(_blocked("preflight", failed, fixes), indent=2, ensure_ascii=False))
        return 2

    out = {
        "status": "ok",
        "phase": "preflight_offline",
        "config": str(args.config),
        "hftbacktest_version": hbt_ver,
        "rust_binary": str(rust_bin),
        "rust_preflight": rust_preflight_json,
        "p0_spec_sha256": spec_hashes,
        "offline_closed_loop_smoke": smoke_result,
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


def cmd_collect(args: argparse.Namespace) -> int:
    env_auth = os.environ.get("TOKYO_L2_LIVE_AUTHORIZED", "").lower() in ("1", "true")
    if not args.allow_live_run and not env_auth:
        rep = _blocked(
            "collect",
            ["Live WS collection is blocked until client authorizes P2/P3 execution"],
            ["Pass --allow-live-run or set TOKYO_L2_LIVE_AUTHORIZED=1 ONLY after AWS Tokyo P2/P3 approval"],
        )
        print(json.dumps(rep, indent=2, ensure_ascii=False))
        return 3

    from data_coverage import resolve_effective_run_id

    try:
        cfg_obj = json.loads(args.config.read_text(encoding="utf-8"))
        eff_run_id = resolve_effective_run_id(cfg_obj, getattr(args, "run_id", None), require=True)
    except Exception as exc:
        print(
            json.dumps(
                _blocked(
                    "collect",
                    [str(exc)],
                    ["Align TOKYO_L2_RUN_ID in /etc/tokyo-l2/live.env, deployment.run_id in config.json, and --run-id"],
                ),
                indent=2,
            )
        )
        return 2

    rust_bin = _find_or_build_rust_binary()
    cmd = [
        str(rust_bin),
        "collect",
        "--config",
        str(args.config),
        "--run-id",
        str(eff_run_id),
        "--allow-live-run",
    ]
    if args.duration_secs is not None:
        cmd.extend(["--duration-secs", str(args.duration_secs)])
    proc = subprocess.run(cmd)
    return int(proc.returncode)


def cmd_archive(args: argparse.Namespace) -> int:
    from data_coverage import (
        configured_partition_minutes,
        enabled_exchanges,
        resolve_cutover_window_utc,
        resolve_effective_run_id,
    )
    from meta_daemon import notify_archive_healthcheck

    proxy_url = getattr(args, "proxy_url", None)
    proxy_mode = getattr(args, "proxy_mode", None)
    if proxy_url is not None or proxy_mode is not None:
        backend = RcloneBackend(args.remote, proxy_url=proxy_url, proxy_mode=proxy_mode)
    else:
        backend = RcloneBackend(args.remote)
    result: dict[str, Any] | None = None
    try:
        archive_config = getattr(args, "config", None)
        cfg_obj: dict[str, Any] | None = None
        expected = None
        partition_minutes = 60
        seal_wait_seconds = 0.0
        seal_poll_interval = 1.0
        if archive_config:
            cfg_obj = json.loads(archive_config.read_text(encoding="utf-8"))
            expected = set(enabled_exchanges(cfg_obj))
            partition_minutes = configured_partition_minutes(cfg_obj)
            arch_cfg = cfg_obj.get("archive", {})
            seal_wait_seconds = float(arch_cfg.get("seal_wait_seconds", 0.0))
            seal_poll_interval = float(arch_cfg.get("seal_poll_interval_seconds", 1.0))
        if getattr(args, "seal_wait_seconds", None) is not None:
            seal_wait_seconds = float(args.seal_wait_seconds)
        effective_run_id = resolve_effective_run_id(cfg_obj, getattr(args, "run_id", None))
        cutover_window_utc = resolve_cutover_window_utc(cfg_obj, getattr(args, "cutover_window", None))
        result = sync_sealed_directory(
            args.dir,
            backend=backend,
            verify_remote=args.verify_remote,
            prune_older_than_hours=args.prune_hours,
            upload_ledger_snapshot=not args.no_snapshot,
            expected_hourly_exchanges=expected,
            partition_minutes=partition_minutes,
            expected_run_id=effective_run_id,
            cutover_window_utc=cutover_window_utc,
            seal_wait_seconds=seal_wait_seconds,
            seal_poll_interval_seconds=seal_poll_interval,
        )
    except Exception as exc:
        if not getattr(args, "no_healthchecks", False):
            notify_archive_healthcheck(None, error=exc)
        err_rep = _blocked(
            "archive",
            [f"{type(exc).__name__}: {exc}"],
            ["Inspect stderr / archive logs / Lightsail SOCKS5 tunnel; local source files remain preserved."],
        )
        print(json.dumps(err_rep, indent=2, ensure_ascii=False))
        return 2

    if not getattr(args, "no_healthchecks", False):
        notify_archive_healthcheck(result)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["status"] == "ok" else 2


def cmd_qa_convert(args: argparse.Namespace) -> int:
    remote_backend = RcloneBackend(args.remote) if args.remote else None
    try:
        coverage_config = (
            json.loads(args.coverage_config.read_text(encoding="utf-8"))
            if args.coverage_config else None
        )
    except (OSError, ValueError) as exc:
        print(json.dumps(_blocked("qa_convert", [str(exc)], ["Supply a readable collector configuration"])))
        return 2
    result = run_qa_convert(
        args.manifest,
        args.out,
        meta_dir=args.meta_dir,
        allow_unverified_local=args.allow_unverified_local,
        strict_zero_loss=args.strict_zero_loss,
        remote_backend=remote_backend,
        restore_cache_dir=args.restore_cache_dir,
        coverage_config=coverage_config,
        coverage_date_utc=args.date_utc,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["status"] == "ok" else 2


def cmd_qa_dispatch(args: argparse.Namespace) -> int:
    from data_coverage import configured_partition_minutes, resolve_effective_run_id
    from qa_dispatch import dispatch_pending_qa, get_dispatch_status

    run_id_filter = args.run_id
    pm_filter: int | None = None
    if args.config is not None and args.config.is_file():
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
        qa_state_dir=args.qa_state_dir,
        qa_state_branch=args.qa_state_branch,
        accepted_stale_timeout_s=args.accepted_stale_timeout,
        require_protocol_report=args.require_protocol_report,
        expected_checker_commit=args.expected_checker_commit,
        expected_protocol_checker=args.expected_protocol_checker,
        reconcile_accepted=not args.no_reconcile,
        require_token=args.require_token,
    )
    print(json.dumps(res, indent=2, ensure_ascii=False))
    return 0 if res["status"] in ("ok", "disabled_no_token", "busy") else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tokyo-l2-cli",
        description="Unified CLI for AWS Tokyo L2 Collector & Offline QA Pipeline (SPEC_v1.4)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_pre = sub.add_parser("preflight", help="Offline protocol, config, P0 artifact, and smoke gatekeeper")
    p_pre.add_argument("--config", type=Path, required=True)
    p_pre.add_argument("--offline", action="store_true", help="Run strictly offline without network calls")
    p_pre.set_defaults(func=cmd_preflight)

    p_col = sub.add_parser("collect", help="Run live dual-active collector (guarded for P2/P3)")
    p_col.add_argument("--config", type=Path, required=True)
    p_col.add_argument("--run-id", default=None)
    p_col.add_argument("--allow-live-run", action="store_true")
    p_col.add_argument("--duration-secs", type=int, default=None, help="Optional clean internal run duration in seconds")
    p_col.set_defaults(func=cmd_collect)

    p_arc = sub.add_parser("archive", help="Verify sealed partitions and upload to single Google Drive Pro")
    p_arc.add_argument("--dir", type=Path, required=True)
    p_arc.add_argument("--config", type=Path, help="Require verified partitions for all configured exchanges in the previous UTC window")
    p_arc.add_argument("--verify-remote", action="store_true")
    p_arc.add_argument("--remote", default="gdrive_main:tokyo_l2_data")
    p_arc.add_argument("--prune-hours", type=float, default=None)
    p_arc.add_argument("--no-snapshot", action="store_true", help="Skip uploading consistent SQLite ledger snapshot")
    p_arc.add_argument("--no-healthchecks", action="store_true", help="Skip Healthchecks ping after archive run")
    p_arc.add_argument("--run-id", default=None, help="Optional collection batch run_id filter")
    p_arc.add_argument("--cutover-window", default=None, help="Optional first full acceptance UTC window (YYYY-MM-DDTHH:MMZ)")
    p_arc.add_argument("--seal-wait-seconds", type=float, default=None, help="Override bounded async seal wait")
    p_arc.add_argument("--proxy-url", default=None, help="Optional SOCKS5 proxy URL scoped ONLY to rclone")
    p_arc.add_argument(
        "--proxy-mode",
        choices=("off", "required", "fallback_direct"),
        default=None,
        help="Optional archive proxy mode overriding TOKYO_L2_ARCHIVE_PROXY_MODE",
    )
    p_arc.set_defaults(func=cmd_archive)

    p_qa = sub.add_parser("qa-convert", help="Run QA gatekeeper and HBT 2.4.4 converter")
    p_qa.add_argument("--manifest", type=Path, required=True)
    p_qa.add_argument("--out", type=Path, required=True)
    p_qa.add_argument("--meta-dir", type=Path, default=None)
    p_qa.add_argument("--coverage-config", type=Path, help="Collector configuration for complete-day coverage")
    p_qa.add_argument("--date-utc", help="UTC day (YYYY-MM-DD) to certify with --coverage-config")
    p_qa.add_argument("--allow-unverified-local", action="store_true")
    p_qa.add_argument("--strict-zero-loss", action="store_true")
    p_qa.add_argument("--remote", default=None, help="Optional rclone remote root to restore PRUNED partitions")
    p_qa.add_argument("--restore-cache-dir", type=Path, default=None, help="Optional cache dir for restored PRUNED files")
    p_qa.set_defaults(func=cmd_qa_convert)

    p_disp = sub.add_parser("qa-dispatch", help="Dispatch pending verified archive windows to GitHub Actions QA")
    p_disp.add_argument("--dir", type=Path, required=True)
    p_disp.add_argument("--config", type=Path, default=None)
    p_disp.add_argument("--run-id", default=None)
    p_disp.add_argument("--repo", default=None)
    p_disp.add_argument("--workflow", default=None)
    p_disp.add_argument("--ref", default=None)
    p_disp.add_argument("--token-env", default="TOKYO_L2_GITHUB_QA_DISPATCH_TOKEN")
    p_disp.add_argument("--max-dispatches", type=int, default=2)
    p_disp.add_argument("--max-age-hours", type=float, default=None)
    p_disp.add_argument("--connect-timeout", type=float, default=5.0)
    p_disp.add_argument("--total-timeout", type=float, default=15.0)
    p_disp.add_argument("--base-backoff", type=float, default=30.0)
    p_disp.add_argument("--max-backoff", type=float, default=900.0)
    p_disp.add_argument("--qa-state-dir", type=Path, default=None)
    p_disp.add_argument("--qa-state-branch", default=None)
    p_disp.add_argument("--accepted-stale-timeout", type=float, default=None)
    p_disp.add_argument("--require-protocol-report", action="store_true", default=None)
    p_disp.add_argument("--expected-checker-commit", default=None)
    p_disp.add_argument("--expected-protocol-checker", default=None)
    p_disp.add_argument("--no-reconcile", action="store_true")
    p_disp.add_argument("--require-token", action="store_true")
    p_disp.add_argument("--status-only", action="store_true")
    p_disp.set_defaults(func=cmd_qa_dispatch)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
