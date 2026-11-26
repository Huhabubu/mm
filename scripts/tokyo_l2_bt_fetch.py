#!/usr/bin/env python3
"""Fetch one complete UTC day of verified Tokyo L2 partitions for selected exchanges.

Read-only. Nothing here writes to Drive or to the QA state branch.

Reused from the frozen collector code (collector/python, same commit as the
15-minute QA checker):
- hourly_incremental_qa._discover_verified_candidates: discovery only from the
  trusted remotely verified archive index (never scans unverified raw/).
- hourly_incremental_qa._download_with_retry / _list_meta_files: rclone access.
- hourly_incremental_qa._verify_three_way_index_manifest_data: index/manifest/data
  SHA-256, size, window and run_id consistency for every partition.
- data_coverage.effective_config_hash: proves the config used here is the one the
  15-minute QA used for that day.

The output layout is the qa_convert directory mode:
<out>/raw/<exchange>/<date>/<partition>.{manifest.json,tsv.zst} and <out>/meta/.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import tempfile
import time
from typing import Any

WINDOWS_PER_DAY_15M = 96
DEFAULT_EXCHANGES = ("binance_futures", "gate_futures")
# Binance is the leader signal: only its @bookTicker BBO is used. A recorded
# Binance depth unavailability does not touch any input of the backtest.
BINANCE_TOLERATED_PROBLEM = re.compile(r"^depth unavailable events recorded: \d+$")
BLOCKING_DAY_LISTS = (
    "missing_windows", "overdue_windows", "pending_windows",
    "archived_pending_qa_windows", "review_windows",
)


def day_start_s(date_utc: str) -> int:
    return int(datetime.strptime(date_utc, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def find_day_report(qa_days_dir: Path, date_utc: str, run_id: str | None = None) -> Path:
    hits = sorted(Path(qa_days_dir).glob(f"*/{date_utc}_p15m.day.qa.json"))
    if run_id:
        hits = [h for h in hits if h.parent.name == run_id]
    if len(hits) != 1:
        raise ValueError(
            f"need exactly one 15m QA day report for {date_utc} (run_id={run_id or 'auto'}), "
            f"found {[str(h) for h in hits]}; pass --run-id")
    return hits[0]


def _mentions_exchange(item: Any, exchange: str) -> bool:
    if isinstance(item, dict):
        return item.get("exchange") == exchange
    return exchange in json.dumps(item, ensure_ascii=False, default=str)


def integrity_gate(day: dict[str, Any], exchanges: tuple[str, ...], config_hash: str, date_utc: str) -> dict[str, Any]:
    """Per-exchange integrity verdict from the 15-minute QA day report.

    The day-level qa_status can be 'failed' because of an exchange that is not
    used (e.g. Bitget); only the requested exchanges are judged here.
    """
    problems: list[str] = []
    tolerated: list[dict[str, Any]] = []
    if day.get("report_level") != "day" or day.get("date_utc") != date_utc:
        problems.append("day report does not describe the requested UTC date")
    if int(day.get("partition_minutes") or 0) != 15:
        problems.append("only 15-minute partitions are supported")
    if day.get("effective_config_sha256") != config_hash:
        problems.append(
            f"collector config hash {config_hash} != QA effective_config_sha256 {day.get('effective_config_sha256')}")
    if int(day.get("expected_windows_count") or 0) != WINDOWS_PER_DAY_15M or \
            int(day.get("observed_windows_count") or 0) != WINDOWS_PER_DAY_15M:
        problems.append("day is not a complete 96-window UTC day")
    for key in BLOCKING_DAY_LISTS:
        if day.get(key):
            problems.append(f"{key} not empty: {day.get(key)}")
    if day.get("clock_certification_passed") is not True:
        problems.append("chrony clock certification (<=1 ms) did not pass for the day")
    for key in ("cross_window_sequence_issues", "startup_stub_segments"):
        for item in day.get(key) or ():
            if any(_mentions_exchange(item, ex) for ex in exchanges):
                problems.append(f"{key}: {item}")
    for item in day.get("damaged_partitions") or ():
        ex = item.get("exchange")
        if ex not in exchanges:
            continue
        probs = [str(p) for p in item.get("problems") or ()]
        if (ex == "binance_futures" and probs
                and all(BINANCE_TOLERATED_PROBLEM.match(p) for p in probs)
                and not int(item.get("local_loss_events") or 0)
                and not int(item.get("gap_start_events") or 0)):
            tolerated.append({"exchange": ex, "window_id": item.get("window_id"), "problems": probs,
                              "reason": "Binance depth not used; leader input is @bookTicker only"})
        else:
            problems.append(f"damaged partition {ex} {item.get('window_id')}: {probs}")
    return {
        "status": "passed" if not problems else "failed",
        "exchanges": list(exchanges),
        "problems": problems,
        "tolerated_partitions": tolerated,
        "day_run_id": day.get("run_id"),
        "day_qa_status_all_exchanges": day.get("qa_status"),
        "checker_commit_sha": day.get("checker_commit_sha"),
    }


def _safe_rel(rel: str, root: str) -> PurePosixPath:
    p = PurePosixPath(rel)
    if p.is_absolute() or ".." in p.parts or not p.parts or p.parts[0] != root:
        raise ValueError(f"unsafe path in verified index/listing: {rel}")
    return p


def fetch_partitions(remote, config, date_utc, run_id, exchanges, out: Path, workers=4):
    import hourly_incremental_qa as archive

    start = day_start_s(date_utc)
    candidates, mode, warnings, _, index_run_id = archive._discover_verified_candidates(
        remote, config, since_date=date_utc, since_start_s=start, run_id_filter=run_id,
        download_retries=3, sleeper=time.sleep,
    )
    if mode != "archive_manifest_index":
        raise ValueError(f"verified archive index unavailable (discovery_mode={mode}, warnings={warnings})")
    chosen = [c for c in candidates if c["exchange"] in exchanges and c["date_utc"] == date_utc]
    windows = Counter((c["exchange"], c["window_id"]) for c in chosen)
    if any(n != 1 for n in windows.values()):
        raise ValueError("duplicate verified partitions for one exchange window")
    per_exchange = Counter(c["exchange"] for c in chosen)
    for ex in exchanges:
        if per_exchange[ex] != WINDOWS_PER_DAY_15M:
            raise ValueError(f"{ex}: {per_exchange[ex]} verified windows for {date_utc}, need {WINDOWS_PER_DAY_15M}")

    def one(c):
        rel_m = _safe_rel(c["source_manifest"], "raw")
        rel_d = _safe_rel(c["source_data"], "raw")
        if str(rel_d) != str(rel_m).removesuffix(".manifest.json") + ".tsv.zst":
            raise ValueError("index data and manifest paths do not match")
        mpath, dpath = out / rel_m, out / rel_d
        mpath.parent.mkdir(parents=True, exist_ok=True)
        archive._download_with_retry(remote, str(rel_m), mpath, retries=3, sleeper=time.sleep)
        archive._download_with_retry(remote, str(rel_d), dpath, retries=3, sleeper=time.sleep)
        manifest = json.loads(mpath.read_bytes())
        archive._verify_three_way_index_manifest_data(
            cand=c, manifest_path=mpath, manifest=manifest, data_path=dpath, run_id_filter=run_id,
        )
        return {"exchange": c["exchange"], "window_id": c["window_id"],
                "manifest_sha256": c["manifest_sha256"], "data_sha256": c["data_sha256"],
                "data_bytes": c["data_bytes"]}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        verified = sorted(pool.map(one, chosen), key=lambda x: (x["exchange"], x["window_id"]))
    return verified, index_run_id


def fetch_meta(remote, date_utc, exchanges, out: Path) -> list[str]:
    """Chrony certification CSV and spec-change metadata read by qa_convert."""
    import hourly_incremental_qa as archive

    names = archive._list_meta_files(remote)
    chrony = f"chrony/chrony_{date_utc.replace('-', '')}.csv"
    if chrony not in names:
        raise ValueError(f"missing meta/{chrony}")
    wanted = [chrony]
    if "specs/spec_changes.jsonl" in names:
        wanted.append("specs/spec_changes.jsonl")
    wanted += sorted(
        n for n in names
        if n.startswith("specs/") and n.endswith(".meta.json")
        and PurePosixPath(n).parent == PurePosixPath("specs")
        and any(PurePosixPath(n).name.startswith(ex + "_") for ex in exchanges)
    )
    for name in wanted:
        rel = _safe_rel("meta/" + name, "meta")
        target = out / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        archive._download_with_retry(remote, str(rel), target, retries=3, sleeper=time.sleep)
    return wanted


def gate_live_contract(remote, meta_dir: Path, gate_symbol: str, start_ns: int) -> dict[str, Any]:
    """Gate instrument snapshot already observed by the collector at day start.

    Same selection and digest rule as load_btc_specs in scripts/tokyo_l2_replay_qa.py
    (main), generalized to one Gate symbol.
    """
    import hourly_incremental_qa as archive

    metas = []
    for path in sorted((meta_dir / "specs").glob("gate_futures_*.meta.json")):
        meta = json.loads(path.read_text(encoding="utf-8"))
        if meta.get("exchange") == "gate_futures" and int(meta.get("reply_wall_ns") or 0) <= start_ns:
            metas.append((int(meta["reply_wall_ns"]), path.name, meta))
    if not metas:
        raise ValueError("no Gate instrument snapshot observed before the day start")
    reply_ns, meta_name, meta = max(metas, key=lambda x: (x[0], x[1]))
    raw_name = meta["raw_file"]
    if Path(raw_name).name != raw_name or not raw_name.startswith("gate_futures_"):
        raise ValueError("invalid instrument snapshot path")
    with tempfile.TemporaryDirectory() as tmp:
        raw_path = Path(tmp) / raw_name
        archive._download_with_retry(remote, "meta/specs/" + raw_name, raw_path, retries=3, sleeper=time.sleep)
        raw = raw_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != meta["sha256"]:
        raise ValueError("instrument snapshot digest mismatch")
    row = next(x for x in json.loads(raw) if x.get("name") == gate_symbol)
    keys = ("name", "type", "order_price_round", "quanto_multiplier", "order_size_min",
            "order_size_round", "enable_decimal", "maker_fee_rate", "taker_fee_rate")
    return {"contract": {k: row.get(k) for k in keys}, "meta_file": meta_name,
            "reply_wall_ns": reply_ns, "raw_sha256": meta["sha256"]}


def live_contract(
    remote: str | None,
    meta_dir: Path,
    exchange: str,
    native_symbol: str,
    start_ns: int,
    *,
    quote_tick_mult: int = 1,
    hbt_tick_size_override: float | None = None,
) -> dict[str, Any]:
    """Generic instrument snapshot loader for Gate, Bybit, and Bitget."""
    from tokyo_l2_exchange_adapters import load_live_contract_from_meta

    return load_live_contract_from_meta(
        remote, meta_dir, exchange, native_symbol, start_ns,
        quote_tick_mult=quote_tick_mult, hbt_tick_size_override=hbt_tick_size_override,
    )


def main() -> int:
    p = argparse.ArgumentParser(description="Fetch one verified Tokyo L2 UTC day (read-only)")
    p.add_argument("--remote", required=True, help="read-only rclone remote root, e.g. bt:")
    p.add_argument("--config", type=Path, required=True, help="collector config used by the 15m QA")
    p.add_argument("--qa-days", type=Path, required=True, help="tokyo-l2-qa-state qa_reports/days directory")
    p.add_argument("--date-utc", required=True)
    p.add_argument("--run-id", default="")
    p.add_argument("--exchanges", default=",".join(DEFAULT_EXCHANGES))
    p.add_argument("--gate-symbol", required=True, help="e.g. BTC_USDT")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--workers", type=int, default=4)
    args = p.parse_args()

    from data_coverage import effective_config_hash

    exchanges = tuple(x.strip() for x in args.exchanges.split(",") if x.strip())
    config = json.loads(args.config.read_text(encoding="utf-8"))
    report_path = find_day_report(args.qa_days, args.date_utc, args.run_id or None)
    day = json.loads(report_path.read_text(encoding="utf-8"))
    run_id = args.run_id or str(day["run_id"])
    args.out.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {"date_utc": args.date_utc, "run_id": run_id,
                              "day_report": str(report_path), "status": "started"}
    # Integrity verdict first: a failed day costs no Drive bandwidth.
    result["integrity"] = integrity_gate(day, exchanges, effective_config_hash(config), args.date_utc)
    try:
        if result["integrity"]["status"] != "passed":
            result["status"] = "blocked_integrity"
            return 2
        partitions, index_run_id = fetch_partitions(
            args.remote, config, args.date_utc, run_id, exchanges, args.out, args.workers)
        result["index_effective_run_id"] = index_run_id
        result["partitions"] = partitions
        result["compressed_bytes"] = {ex: sum(int(x["data_bytes"] or 0) for x in partitions if x["exchange"] == ex)
                                      for ex in exchanges}
        result["meta_files"] = fetch_meta(args.remote, args.date_utc, exchanges, args.out)
        if "gate_futures" in exchanges:
            result["gate_live_contract"] = gate_live_contract(
                args.remote, args.out / "meta", args.gate_symbol, day_start_s(args.date_utc) * 1_000_000_000)
        result["status"] = "ok"
        return 0
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        (args.out / "fetch_report.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print("FETCH_REPORT", result["status"], json.dumps(result.get("integrity"), ensure_ascii=False), flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
