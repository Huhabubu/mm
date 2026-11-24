"""Read-only incident audit for Tokyo L2 QA alerts.

Downloads verified archive partitions through the read-only QA remote, checks
index/manifest/data identity, and writes summary-only evidence: META event
timelines, interruption durations, affected streams, the existing partition
checker result on the same files, and the age of the last same-connection BTC
depth snapshot that an hourly follower replay would need. Raw frames are never
written to the output.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
import io
import json
from pathlib import Path, PurePosixPath
import shutil
import tempfile
import time

import zstandard as zstd

import data_coverage as coverage
import hourly_incremental_qa as archive
from tokyo_l2_protocol_qa import BITGET, BYBIT, GATE, depth_event, symbol_key

NS = 1_000_000_000
NL = chr(10)
SILENCE_NS = 2 * NS
DETAIL_EVENTS = {
    "GAP_START", "GAP_END", "SWITCH", "STANDBY_DISCONNECT", "DEPTH_UNAVAILABLE",
    "DEPTH_RECOVERED", "SNAPSHOT_BRIDGE_RETRY", "ROTATION_DEFERRED", "LOCAL_LOSS",
    "LATE_FRAME_REJECTED",
}
WAIT_EVENTS = {"DEPTH_UNAVAILABLE", "SNAPSHOT_BRIDGE_RETRY"}
BTC_MARKERS = {"gate_futures": "ob.BTC_USDT.50", "bybit_linear": "orderbook.50.BTCUSDT"}


def window_start(text):
    return int(datetime.strptime(text, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc).timestamp())


def iso(ns):
    return datetime.fromtimestamp(ns / NS, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def raw_lines(path):
    with path.open("rb") as fh, zstd.ZstdDecompressor().stream_reader(fh) as reader:
        for line in io.TextIOWrapper(reader, encoding="utf-8"):
            wall, _mono, conn, epoch, _seq, kind, payload = line.rstrip(NL).split(chr(9), 6)
            yield int(wall), conn, int(epoch), kind, payload


def load(kind, payload):
    obj = json.loads(payload)
    if kind == "DATA_ESCAPED" and isinstance(obj, str):
        obj = json.loads(obj)
    return obj if isinstance(obj, dict) else {}


def scalars(obj):
    return {k: v for k, v in obj.items() if v is None or isinstance(v, (str, int, float, bool))}


def is_depth(exchange, channel):
    return coverage.classify_channel(exchange, channel) == coverage.CHANNEL_CATEGORY_DEPTH


def checker_summary(result):
    keep = {}
    for key, value in result.items():
        if value is None or isinstance(value, (str, int, float, bool)):
            keep[key] = value
        elif key in ("problems", "structural_problems", "planned_depth_unavailable_intervals") and isinstance(value, list):
            keep[key] = value[:60]
    return keep


class Archive:
    def __init__(self, remote, config, run_id, earliest, tmp):
        self.remote, self.config, self.run_id, self.tmp = remote, config, run_id, tmp
        self.pm = coverage.configured_partition_minutes(config)
        self.step = self.pm * 60
        self.bytes_downloaded = 0
        day = datetime.fromtimestamp(earliest, timezone.utc).date().isoformat()
        candidates, discovery, _x, _y, batch = archive._discover_verified_candidates(
            remote, config, since_date=day, since_start_s=earliest, run_id_filter=run_id,
            download_retries=3, sleeper=time.sleep, state_dir=None,
        )
        if discovery != "archive_manifest_index":
            raise SystemExit(f"verified archive index unavailable: {discovery}")
        self.index_batch = batch
        self.by_key = defaultdict(list)
        for item in candidates:
            if item.get("run_id") == run_id:
                self.by_key[(item["exchange"], int(item["planned_start_s"]))].append(item)

    def wid(self, start):
        return coverage.window_descriptor(start, self.pm)["window_id"]

    def items(self, exchange, start):
        return sorted(self.by_key.get((exchange, start), []), key=lambda item: item["source_manifest"])

    @contextmanager
    def opened(self, item):
        rel = PurePosixPath(item["source_manifest"])
        data = PurePosixPath(item["source_data"])
        if rel.is_absolute() or rel.parts[0] != "raw" or ".." in rel.parts:
            raise ValueError("invalid path in verified index")
        if str(data) != str(rel).removesuffix(".manifest.json") + ".tsv.zst":
            raise ValueError("index data and manifest paths do not match")
        dest = Path(tempfile.mkdtemp(dir=self.tmp))
        try:
            mf_path, data_path = dest / rel.name, dest / data.name
            archive._download_with_retry(self.remote, str(rel), mf_path, retries=3, sleeper=time.sleep)
            archive._download_with_retry(self.remote, str(data), data_path, retries=3, sleeper=time.sleep)
            manifest = json.loads(mf_path.read_bytes())
            archive._verify_three_way_index_manifest_data(
                cand=item, manifest_path=mf_path, manifest=manifest, data_path=data_path, run_id_filter=self.run_id,
            )
            self.bytes_downloaded += data_path.stat().st_size
            print(f"verified {rel.name}", flush=True)
            yield manifest, data_path
        finally:
            shutil.rmtree(dest, ignore_errors=True)


def silences_over(silences, start, end, keep=None):
    found = []
    for stream, spans in silences.items():
        if keep is not None and not keep(stream):
            continue
        longest = 0
        for s0, s1, _c0, _c1 in spans:
            if min(s1, end) > max(s0, start):
                longest = max(longest, s1 - s0)
        if longest:
            found.append({"stream": stream, "silence_seconds": round(longest / NS, 3)})
    return sorted(found, key=lambda item: -item["silence_seconds"])


def audit_exchange(arc, exchange, starts):
    events, silences, last, seen = [], defaultdict(list), {}, set()
    frames, meta_counts, checker = Counter(), {}, {}
    previous = None
    for start in sorted(starts):
        wid = arc.wid(start)
        if previous is None or start != previous + arc.step:
            last = {}
        previous = start
        items = arc.items(exchange, start)
        checker[wid] = []
        if not items:
            checker[wid].append({"missing_verified_file": True})
            previous = None
            continue
        counts = Counter()
        for item in items:
            with arc.opened(item) as (manifest, path):
                result = archive.check_partition(path, manifest, arc.config)
                checker[wid].append({"file": item["source_manifest"], "boot_id": manifest.get("boot_id"),
                                     **checker_summary(result)})
                for wall, conn, epoch, kind, payload in raw_lines(path):
                    obj = load(kind, payload)
                    if kind == "META":
                        event = str(obj.get("event") or "")
                        counts[event] += 1
                        if event in DETAIL_EVENTS:
                            events.append({"window_id": wid, "wall_ns": wall, "wall_utc": iso(wall),
                                           "line_conn": conn, "line_epoch": epoch, "meta": scalars(obj)})
                        continue
                    for _ex, sym, channel in coverage._data_streams(exchange, obj):
                        stream = f"{symbol_key(str(sym))}/{channel}"
                        seen.add(stream)
                        frames[f"{wid}|{conn}:{epoch}|{channel}"] += 1
                        prior = last.get(stream)
                        if prior is not None and wall - prior[0] >= SILENCE_NS:
                            silences[stream].append((prior[0], wall, prior[1], f"{conn}:{epoch}"))
                        last[stream] = (wall, f"{conn}:{epoch}")
        meta_counts[wid] = dict(counts)

    events.sort(key=lambda e: e["wall_ns"])
    gaps, open_gaps, waits, open_waits = [], {}, [], {}
    for e in events:
        m = e["meta"]
        event = m.get("event")
        endpoint = str(m.get("endpoint") or exchange)
        if event == "GAP_START":
            gap = {"endpoint": endpoint, "window_id": e["window_id"], "start_utc": e["wall_utc"],
                   "start_ns": e["wall_ns"], "primary": m.get("primary"), "standby_state": m.get("standby_state"),
                   "reason": m.get("reason"), "end_utc": None, "end_ns": None, "duration_seconds": None,
                   "recovered_conn": None}
            gaps.append(gap)
            open_gaps[endpoint] = gap
        elif event == "GAP_END" and endpoint in open_gaps:
            gap = open_gaps.pop(endpoint)
            gap.update(end_utc=e["wall_utc"], end_ns=e["wall_ns"], recovered_conn=m.get("recovered_conn"),
                       previous_primary=m.get("previous_primary"),
                       duration_seconds=round((e["wall_ns"] - gap["start_ns"]) / NS, 6))
        elif event in WAIT_EVENTS:
            symbol = symbol_key(str(m.get("symbol") or "ALL"))
            key = (endpoint, symbol)
            if key in open_waits:
                open_waits.pop(key)["outcome"] = "superseded"
            since = int(m.get("unavailable_since_wall_ns") or e["wall_ns"])
            wait = {"event": event, "endpoint": endpoint, "symbol": symbol, "window_id": e["window_id"],
                    "start_utc": iso(since), "start_ns": since, "conn_id": m.get("conn_id") or m.get("conn"),
                    "epoch": m.get("epoch"), "reason": m.get("reason"), "planned": m.get("planned") is True,
                    "outcome": "open_at_audit_end", "end_ns": None, "end_utc": None, "duration_seconds": None}
            waits.append(wait)
            open_waits[key] = wait
        elif event == "DEPTH_RECOVERED":
            key = (endpoint, symbol_key(str(m.get("symbol") or "")))
            wait = open_waits.pop(key, None)
            if wait is not None:
                duration = int(m.get("duration_ns") or (e["wall_ns"] - wait["start_ns"]))
                wait.update(outcome="recovered", end_ns=wait["start_ns"] + duration, end_utc=e["wall_utc"],
                            duration_seconds=round(duration / NS, 6))

    for gap in gaps:
        end = gap["end_ns"] or gap["start_ns"] + 60 * NS
        gap["silent_streams"] = silences_over(silences, gap["start_ns"], end)
    for wait in waits:
        end = wait["end_ns"] or wait["start_ns"] + arc.step * NS
        sym = wait["symbol"]
        found = silences_over(
            silences, wait["start_ns"], max(end, wait["start_ns"] + 1),
            keep=lambda s, sym=sym: s.split("/", 1)[0] == sym and is_depth(exchange, s.split("/", 1)[1]),
        )
        wait["depth_silence_seconds"] = found[0]["silence_seconds"] if found else 0.0
    switches = [
        {"window_id": e["window_id"], "wall_utc": e["wall_utc"],
         **{k: e["meta"].get(k) for k in ("endpoint", "from", "to", "reason")}}
        for e in events if e["meta"].get("event") == "SWITCH"
    ]
    disconnects = [
        {"window_id": e["window_id"], "wall_utc": e["wall_utc"],
         **{k: e["meta"].get(k) for k in ("endpoint", "conn", "reason")}}
        for e in events if e["meta"].get("event") == "STANDBY_DISCONNECT"
    ]
    return {"checker": checker, "meta_counts": meta_counts, "gaps": gaps, "waits": waits,
            "switches": switches, "standby_disconnects": disconnects, "streams_observed": sorted(seen),
            "frames_by_window_connection_channel": dict(frames),
            "events": [e for e in events if e["meta"].get("event") != "DEPTH_RECOVERED"][:400]}


def anchor_search(arc, exchange, target, max_back):
    marker = BTC_MARKERS[exchange]

    def scan(start):
        info = {"window_id": arc.wid(start), "files": 0, "bytes": 0, "snapshots": 0, "boot_id": None,
                "first_snapshot": None, "last_snapshot_by_connection": {}, "first_delta": None,
                "delta_connections": Counter(), "meta": Counter()}
        for item in arc.items(exchange, start):
            info["files"] += 1
            info["bytes"] += int(item.get("data_bytes") or 0)
            with arc.opened(item) as (manifest, path):
                boot = manifest.get("boot_id") or manifest["file_name"]
                info["boot_id"] = manifest.get("boot_id")
                for wall, conn, epoch, kind, payload in raw_lines(path):
                    if kind == "META":
                        event = load(kind, payload).get("event")
                        if event in DETAIL_EVENTS:
                            info["meta"][event] += 1
                        continue
                    if marker not in payload:
                        continue
                    depth = depth_event(exchange, boot, conn, epoch, kind, load(kind, payload))
                    if depth is None or depth.symbol != "BTCUSDT":
                        continue
                    ckey = "|".join(str(x) for x in depth.connection)
                    point = {"wall_utc": iso(wall), "wall_ns": wall, "connection": ckey, "seq": depth.seq}
                    if depth.mode == "snapshot":
                        info["snapshots"] += 1
                        info["first_snapshot"] = info["first_snapshot"] or point
                        info["last_snapshot_by_connection"][ckey] = point
                    else:
                        info["delta_connections"][ckey] += 1
                        info["first_delta"] = info["first_delta"] or point
        return info

    target_info = scan(target)
    active = (target_info["first_delta"] or {}).get("connection")
    result = {"exchange": exchange, "target_window": arc.wid(target), "active_connection": active,
              "target": target_info, "found": False, "windows": [], "stop_reason": "max_windows_reached"}
    first_snap, first_delta = target_info["first_snapshot"], target_info["first_delta"]
    if first_snap and (not first_delta or first_snap["wall_ns"] < first_delta["wall_ns"]):
        result.update(found=True, anchor_utc=first_snap["wall_utc"], age_seconds=0, windows_back=0,
                      bytes_to_anchor=0, stop_reason="snapshot_inside_target")
        return result
    total = 0
    for back in range(1, max_back + 1):
        start = target - back * arc.step
        if not arc.items(exchange, start):
            result["stop_reason"] = f"archive_window_missing:{arc.wid(start)}"
            break
        info = scan(start)
        total += info["bytes"]
        result["windows"].append(info)
        hit = info["last_snapshot_by_connection"].get(active)
        if hit:
            result.update(found=True, anchor_utc=hit["wall_utc"], windows_back=back, bytes_to_anchor=total,
                          age_seconds=round((target * NS - hit["wall_ns"]) / NS, 3), stop_reason="anchor_found")
            break
    return result


def cell(value):
    return "" if value is None else str(value).replace("|", "/").replace(NL, " ")


def trading_event(exchange, obj):
    """Same trade extraction as tokyo_l2_replay_qa.trading_event, without its replay dependencies."""
    if exchange == GATE and obj.get("channel") == "futures.trades" and obj.get("event") == "update":
        data = obj.get("result", [])
        return data if isinstance(data, list) else [data]
    if exchange == BYBIT and obj.get("topic", "").startswith("publicTrade."):
        return obj.get("data", [])
    arg = obj.get("arg") or {}
    if exchange == BITGET and (arg.get("topic") or arg.get("channel")) == "publicTrade":
        return obj.get("data", [])
    return []


def trade_symbol(trade, obj):
    arg = obj.get("arg") or {}
    return symbol_key(str(trade.get("s") or trade.get("contract") or arg.get("symbol") or arg.get("instId")
                          or obj.get("topic", "").split(".")[-1]))


def trade_duplicates(arc, exchange, start):
    """Trade ids seen more than once in one window, with the connection and frame action of each copy."""
    seen, meta = defaultdict(list), []
    lo, hi = start * NS, (start + arc.step) * NS
    for item in arc.items(exchange, start):
        with arc.opened(item) as (_manifest, path):
            for wall, conn, epoch, kind, payload in raw_lines(path):
                if wall < lo or wall >= hi:
                    continue
                obj = load(kind, payload)
                if kind == "META":
                    if obj.get("event") in DETAIL_EVENTS and len(meta) < 60:
                        meta.append({"wall_utc": iso(wall), "line_conn": f"{conn}:{epoch}", **scalars(obj)})
                    continue
                for trade in trading_event(exchange, obj):
                    tid = trade.get("id") if exchange == GATE else trade.get("i") or trade.get("tradeId")
                    if tid is None:
                        continue
                    fields = {k: trade.get(k) for k in ("price", "size", "side", "ts", "p", "v", "S", "T",
                                                        "create_time_ms") if k in trade}
                    seen[(trade_symbol(trade, obj), str(tid))].append({
                        "wall_utc": iso(wall), "wall_ns": wall, "conn": f"{conn}:{epoch}",
                        "action": obj.get("action") or obj.get("type") or obj.get("event"), "fields": fields,
                    })
    dups = []
    for (sym, tid), occ in sorted(seen.items()):
        if len(occ) > 1:
            dups.append({
                "symbol": sym, "trade_id": tid, "count": len(occ),
                "same_connection": len({o["conn"] for o in occ}) == 1,
                "identical_fields": len({json.dumps(o["fields"], sort_keys=True) for o in occ}) == 1,
                "gap_ms": round((occ[-1]["wall_ns"] - occ[0]["wall_ns"]) / 1e6, 3),
                "occurrences": [{k: v for k, v in o.items() if k != "wall_ns"} for o in occ[:6]],
            })
    return {"exchange": exchange, "window_id": arc.wid(start), "trade_copies": sum(len(v) for v in seen.values()),
            "unique_trade_ids": len(seen), "duplicate_count": len(dups), "duplicates": dups[:50],
            "meta_events": meta}


def row(values):
    return "| " + " | ".join(cell(v) for v in values) + " |"


def render(report):
    src = report["source_summary"]
    lines = [
        "# Tokyo L2 QA incident audit (read-only)", "",
        f"- run_id `{report['run_id']}`, archive index batch `{report['index_batch']}`, verified bytes read {report['bytes_downloaded']}",
        f"- evidence summary checker `{src.get('checker_commit_sha')}`, completed `{src.get('completed_at_utc')}`, damaged partitions {len(src.get('damaged_partitions') or [])}",
        "", "## GAP_START interruptions", "",
        row(["exchange", "endpoint", "start UTC", "end UTC", "duration s", "primary", "standby state", "reason",
             "recovered conn", "silent streams (s)"]),
        row(["---"] * 10),
    ]
    audits = report["event_audit"]
    for ex, audit in audits.items():
        for gap in audit["gaps"]:
            silent = ", ".join(f"{s['stream']}={s['silence_seconds']}" for s in gap["silent_streams"][:12])
            lines.append(row([ex, gap["endpoint"], gap["start_utc"], gap["end_utc"], gap["duration_seconds"],
                              gap["primary"], gap["standby_state"], gap["reason"], gap["recovered_conn"], silent]))
    lines += ["", "## Switches and standby disconnects", "", row(["exchange", "window", "UTC", "event", "detail"]),
              row(["---"] * 5)]
    for ex, audit in audits.items():
        for s in audit["switches"]:
            lines.append(row([ex, s["window_id"], s["wall_utc"], "SWITCH", f"{s['endpoint']} {s['from']}->{s['to']} {s['reason']}"]))
        for s in audit["standby_disconnects"]:
            lines.append(row([ex, s["window_id"], s["wall_utc"], "STANDBY_DISCONNECT", f"{s['endpoint']} {s['conn']} {s['reason']}"]))
    lines += ["", "## Depth unavailable and snapshot-bridge waits", "",
              row(["exchange", "window", "event", "reason", "planned", "outcome", "count", "symbols", "connection",
                   "max duration s", "max depth silence s"]),
              row(["---"] * 11)]
    for ex, audit in audits.items():
        groups = defaultdict(list)
        for w in audit["waits"]:
            groups[(w["window_id"], w["event"], str(w["reason"]), w["planned"], w["outcome"])].append(w)
        for key in sorted(groups, key=lambda k: tuple(str(x) for x in k)):
            ws = groups[key]
            durations = [w["duration_seconds"] for w in ws if w["duration_seconds"] is not None]
            conns = sorted({f"{w['conn_id']}:{w['epoch']}" for w in ws})
            lines.append(row([ex, *key, len(ws), ",".join(sorted({w["symbol"] for w in ws})), ",".join(conns),
                              max(durations) if durations else None, max(w["depth_silence_seconds"] for w in ws)]))
    lines += ["", "## Existing partition checker on the same files", "",
              row(["exchange", "window", "qa_status", "gap_start", "depth_unavailable", "problems"]), row(["---"] * 6)]
    for ex, audit in audits.items():
        for wid, items in sorted(audit["checker"].items()):
            for item in items:
                status = "missing" if item.get("missing_verified_file") else item.get("qa_status")
                problems = "; ".join(str(p) for p in item.get("problems") or [])
                lines.append(row([ex, wid, status, item.get("gap_start_events"), item.get("depth_unavailable_events"),
                                  problems[:700]]))
    lines += ["", "## Last same-connection BTC depth snapshot before replay target", "",
              row(["exchange", "target", "active connection", "found", "anchor UTC", "age s", "windows back",
                   "bytes to anchor", "stop reason"]),
              row(["---"] * 9)]
    for a in report["anchor_search"]:
        lines.append(row([a["exchange"], a["target_window"], a["active_connection"], a["found"], a.get("anchor_utc"),
                          a.get("age_seconds"), a.get("windows_back"), a.get("bytes_to_anchor"), a.get("stop_reason")]))
    lines += ["", "## Duplicate trade ids inside the target window", "",
              row(["exchange", "window", "trade copies", "unique ids", "duplicates", "symbol", "trade id",
                   "same connection", "identical fields", "gap ms", "copies (conn@UTC action)"]),
              row(["---"] * 11)]
    for d in report.get("trade_duplicates") or []:
        head = [d["exchange"], d["window_id"], d["trade_copies"], d["unique_trade_ids"], d["duplicate_count"]]
        if not d["duplicates"]:
            lines.append(row(head + [None] * 6))
        for dup in d["duplicates"]:
            copies = "; ".join(f"{o['conn']}@{o['wall_utc']} {o['action']}" for o in dup["occurrences"])
            lines.append(row(head + [dup["symbol"], dup["trade_id"], dup["same_connection"],
                                     dup["identical_fields"], dup["gap_ms"], copies]))
    for d in report.get("trade_duplicates") or []:
        if d["duplicates"] and d["meta_events"]:
            lines += ["", f"META events in {d['exchange']} {d['window_id']}:", ""]
            lines += [f"- {m['wall_utc']} {m['line_conn']} {m.get('event')} "
                      + " ".join(f"{k}={v}" for k, v in m.items() if k not in ("wall_utc", "line_conn", "event"))
                      for m in d["meta_events"]]
    return NL.join(lines) + NL


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--cutover", required=True)
    parser.add_argument("--summary", required=True, help="read-only copy of qa_reports/latest_run_summary.json")
    parser.add_argument("--out", required=True)
    parser.add_argument("--anchor-target", action="append", default=None)
    parser.add_argument("--anchor-max-windows", type=int, default=52)
    parser.add_argument("--trade-dup-target", action="append", default=None)
    parser.add_argument("--skip-anchor-search", action="store_true")
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    summary = json.loads(Path(args.summary).read_text(encoding="utf-8-sig"))
    damaged = summary.get("historical_damaged_partitions") or []
    tmp = Path(tempfile.mkdtemp(prefix="tokyo-audit-"))
    try:
        arc = Archive(args.remote, config, args.run_id, window_start(args.cutover), tmp)
        per_exchange = defaultdict(set)
        for item in damaged:
            start = window_start(item["window_id"])
            per_exchange[item["exchange"]].update({start, start + arc.step})
        report = {
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "run_id": args.run_id, "index_batch": arc.index_batch,
            "source_summary": {
                "checker_commit_sha": summary.get("checker_commit_sha"),
                "completed_at_utc": summary.get("completed_at_utc"),
                "damaged_partitions": [
                    {k: item.get(k) for k in ("exchange", "window_id", "gap_start_events", "problems")}
                    for item in damaged
                ],
            },
            "event_audit": {},
            "anchor_search": [],
            "trade_duplicates": [],
        }
        for target in args.trade_dup_target or ["2026-11-24T12:00Z"]:
            for exchange in ("bitget_v3", "gate_futures", "bybit_linear"):
                report["trade_duplicates"].append(trade_duplicates(arc, exchange, window_start(target)))
        if not args.skip_anchor_search:
            for exchange in sorted(per_exchange):
                report["event_audit"][exchange] = audit_exchange(arc, exchange, per_exchange[exchange])
            for target in args.anchor_target or ["2026-11-24T10:00Z"]:
                for exchange in ("gate_futures", "bybit_linear"):
                    report["anchor_search"].append(
                        anchor_search(arc, exchange, window_start(target), args.anchor_max_windows))
        report["bytes_downloaded"] = arc.bytes_downloaded
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "incident_audit.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    (out / "incident_audit.md").write_text(render(report), encoding="utf-8")


if __name__ == "__main__":
    main()
