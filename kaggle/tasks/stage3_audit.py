"""Independent raw-message and protocol evidence for the two-partition acceptance run.

No strategy changes. Never exports raw payloads, prices, credentials or full trade IDs.
"""
from collections import Counter
import hashlib
import io
import json
from pathlib import Path

import numpy as np
import zstandard as zstd
from hftbacktest import (BUY_EVENT, SELL_EVENT, EXCH_EVENT, LOCAL_EVENT, DEPTH_EVENT,
                         DEPTH_SNAPSHOT_EVENT, TRADE_EVENT)

EVENT = np.dtype([("kind", "<u8"), ("side", "<u8"), ("exch", "<i8"), ("local", "<i8"),
                  ("px", "<f8"), ("qty", "<f8"), ("flags", "<i8")])
QUOTE_KEYS = ("local_ts_ns", "exch_ts_ns", "bid_px", "bid_qty", "ask_px", "ask_qty", "mid_px")


def error_stats(a, b):
    if len(a) != len(b):
        return {"aligned": False, "counts": [len(a), len(b)]}
    delta = np.abs(np.asarray(a, dtype=np.int64) - np.asarray(b, dtype=np.int64))
    return {"aligned": True, "count": len(delta), "p50_abs_ns": float(np.percentile(delta, 50)) if len(delta) else 0,
            "p99_abs_ns": float(np.percentile(delta, 99)) if len(delta) else 0,
            "max_abs_ns": int(delta.max()) if len(delta) else 0,
            "nonzero_fraction": float(np.mean(delta != 0)) if len(delta) else 0}


def rows_equal(expected, actual):
    expected, actual = np.asarray(expected, dtype=EVENT), np.asarray(actual, dtype=EVENT)
    if len(expected) != len(actual):
        return False
    return bool(np.array_equal(np.sort(expected, order=EVENT.names),
                               np.sort(actual, order=EVENT.names)))


def projected_events(h, kinds, direction):
    rows = h[((h["ev"] & direction) != 0) & np.isin(h["ev"] & 255, kinds)]
    out = np.empty(len(rows), dtype=EVENT)
    out["kind"], out["side"] = rows["ev"] & 255, rows["ev"] & (BUY_EVENT | SELL_EVENT)
    out["exch"], out["local"], out["px"], out["qty"], out["flags"] = (
        rows["exch_ts"], rows["local_ts"], rows["px"], rows["qty"], rows["ival"])
    return out


def raw_records(part):
    mf = json.loads(Path(part["manifest"]).read_text())
    with open(part["data"], "rb") as f, zstd.ZstdDecompressor().stream_reader(f) as reader:
        for line_no, line in enumerate(io.TextIOWrapper(reader, encoding="utf-8"), 1):
            cols = line.rstrip("\n").split("\t", 6)
            if len(cols) != 7:
                raise ValueError("malformed raw TSV")
            wall, conn, epoch, kind, payload = int(cols[0]), cols[2], int(cols[3]), cols[5], cols[6]
            # Filtering is independent of the converter. META remains visible for connection resets.
            if kind != "META" and "BTC" not in payload:
                continue
            obj = json.loads(payload)
            if kind == "DATA_ESCAPED" and isinstance(obj, str):
                obj = json.loads(obj)
            yield wall, (mf["boot_id"], conn, epoch), kind, obj, line_no


def quote_row(wall, obj, row, leader):
    ts = row.get("T") or row.get("E") if leader else row.get("t") or obj.get("time_ms")
    exch = int(ts or (wall // 1_000_000)) * 1_000_000
    bid, ask = float(row["b"]), float(row["a"])
    return (wall, exch, bid, float(row["B"]), ask, float(row["A"]), (bid + ask) * 0.5)


def scan(parts):
    quotes = {"binance_bbo": [], "gate_l1_bbo": []}
    trades, copies, conflicts = {}, 0, 0
    depths, trace, recoveries, pre = [], [], [], []
    anchor = None
    pending = None
    first_snapshot = None
    chain_errors = 0
    for part in parts:
        leader = part["exchange"] == "binance_futures"
        for wall, conn, kind, obj, line_no in raw_records(part):
            if kind == "META":
                if leader or str(obj.get("symbol") or "ALL") not in ("ALL", "BTC", "BTCUSDT", "BTC_USDT"):
                    continue
                event = obj.get("event")
                if event in ("SWITCH", "GAP_START", "LOCAL_LOSS", "DEPTH_UNAVAILABLE"):
                    anchor = None
                    pending = None
                    if len(trace) < 40:
                        trace.append({"kind": event, "wall_ns": wall, "connection": conn[1:],
                                      "slot": part["slot"], "line": line_no})
                continue
            if leader:
                row = obj.get("data") if isinstance(obj.get("data"), dict) else obj
                if row.get("s") == "BTCUSDT" and (row.get("e") == "bookTicker" or
                        str(obj.get("stream", "")).endswith("@bookTicker")):
                    quotes["binance_bbo"].append(quote_row(wall, obj, row, True))
                continue
            if obj.get("event") != "update":
                continue
            channel, row = obj.get("channel"), obj.get("result")
            if channel == "futures.trades":
                for tr in row if isinstance(row, list) else [row]:
                    if tr.get("contract") != "BTC_USDT":
                        continue
                    copies += 1
                    key = str(tr["id"])
                    # Preserve raw distinctions, including internal trades, without float timestamp conversion.
                    signature = tuple(tr.get(k) for k in (
                        "contract", "id", "price", "size", "create_time_ms", "create_time", "is_internal"))
                    exch = int(tr.get("create_time_ms") or obj.get("time_ms") or wall // 1_000_000) * 1_000_000
                    size = float(tr["size"])
                    event = (TRADE_EVENT, BUY_EVENT if size >= 0 else SELL_EVENT, exch, wall,
                             float(tr["price"]), abs(size), 1 if tr.get("is_internal", False) else 0)
                    if key in trades and trades[key][0] != signature:
                        conflicts += 1
                    if key not in trades or wall < trades[key][1][3]:
                        trades[key] = (signature, event)
                continue
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("s", ""))
            if channel == "futures.book_ticker" and symbol == "BTC_USDT":
                quotes["gate_l1_bbo"].append(quote_row(wall, obj, row, False))
            if channel != "futures.obu" or symbol.lower() != "ob.btc_usdt.400":
                continue
            full, u, parent = bool(row.get("full", False)), row.get("u"), row.get("U")
            if first_snapshot is None and not full:
                pre.append(wall)
                continue
            if full:
                if first_snapshot is None:
                    first_snapshot = wall
                anchor = (conn, u)
                pending = {"snapshot_wall_ns": wall, "snapshot_seq": u, "connection": list(conn[1:])}
                recoveries.append(pending)
            elif anchor is None or anchor[0] != conn or parent != anchor[1] + 1 or u < parent:
                chain_errors += 1
                anchor = None
                continue
            else:
                anchor = (conn, u)
                if pending is not None:
                    pending.update(confirmation_wall_ns=wall, confirming_U=parent, confirming_u=u,
                                   confirmation_lag_ns=wall - pending["snapshot_wall_ns"])
                    pending = None
            if len(trace) < 40:
                trace.append({"kind": "snapshot" if full else "delta", "wall_ns": wall,
                              "U": parent, "u": u, "connection": conn[1:], "slot": part["slot"],
                              "line": line_no})
            exch = int(row.get("t") or obj.get("time_ms") or wall // 1_000_000) * 1_000_000
            for levels, side in ((row.get("b", []), BUY_EVENT), (row.get("a", []), SELL_EVENT)):
                for px, qty in levels:
                    depths.append((DEPTH_SNAPSHOT_EVENT if full else DEPTH_EVENT, side, exch, wall,
                                   float(px), float(qty), 0))
    return {"quotes": quotes, "trades": trades, "copies": copies, "conflicts": conflicts,
            "depths": depths, "trace": trace, "recoveries": recoveries,
            "pre": pre, "chain_errors": chain_errors}


def protocol_evidence(folder, windows, parts, run_id, checker, rules_sha=None, expected_archive_revisions=None):
    reports, blocked, problems = [], [], []
    expected_checker = f"{checker}/{rules_sha}" if rules_sha else str(checker or "")
    has_rules_digest = "/" in expected_checker and bool(expected_checker.split("/", 1)[1])
    for window in windows:
        hits = list(Path(folder).glob(window.replace(":", "") + ".json"))
        if len(hits) != 1:
            problems.append("protocol_report_missing:" + window)
            continue
        q = json.loads(hits[0].read_text())
        current = has_rules_digest and str(q.get("checker", "")) == expected_checker
        expected = {(p["exchange"], p["data_sha256"], p["manifest_sha256"]) for p in parts
                    if p["window_id"] == window}
        actual = {(p["exchange"], p["data_sha256"], p["manifest_sha256"]) for p in q.get("target_inputs", [])}
        bound = bool(expected) and expected.issubset(actual) and q.get("run_id") == run_id and q.get("window_id") == window
        expected_rev = (expected_archive_revisions or {}).get(window) if expected_archive_revisions is not None else None
        if expected_archive_revisions is not None:
            rev_ok = bool(expected_rev) and q.get("archive_revision") == expected_rev
        else:
            rev_ok = bool(q.get("archive_revision"))
        gate_replay = (q.get("replay") or {}).get("gate_futures", {})
        passed = current and bound and rev_ok and q.get("status") == "passed" and q.get("protocol_status") == "passed" \
            and gate_replay.get("status") not in ("blocked", "failed")
        reports.append({"window_id": window, "checker": q.get("checker"), "expected_checker": expected_checker,
                        "current_checker": current, "completed_at_utc": q.get("completed_at_utc"),
                        "archive_revision": q.get("archive_revision"), "expected_archive_revision": expected_rev,
                        "archive_revision_matched": rev_ok, "bound_to_raw_inputs": bound,
                        "status": q.get("status"), "protocol_status": q.get("protocol_status"),
                        "gate_replay": gate_replay, "passed": passed})
        if not current:
            problems.append("protocol_checker_stale:" + window)
        if not rev_ok:
            problems.append("protocol_archive_revision_mismatch:" + window)
        if not bound:
            problems.append("protocol_inputs_unmatched:" + window)
        if not passed:
            problems.append("protocol_not_accepted:" + window)
        for market in q.get("market_availability", []):
            required = (market.get("exchange") == "gate_futures" and market.get("symbol") == "BTCUSDT"
                        and market.get("channel") in ("futures.obu", "futures.trades")) or (
                        market.get("exchange") == "binance_futures" and market.get("symbol") == "BTCUSDT"
                        and market.get("channel") in ("bookTicker", "book_ticker", "@bookTicker"))
            if required:
                for iv in market.get("intervals", []):
                    if iv.get("status") != "verified_usable":
                        blocked.append({**iv, "exchange": market["exchange"], "channel": market["channel"]})
    return {"passed": not problems and len(reports) == len(windows), "problems": problems,
            "reports": reports, "execution_unavailable_intervals": blocked}


def audit(parts, new_out, old_out, names, new_report, old_calendar, protocol):
    raw = scan(parts)
    checks, quote_reports = {}, {}
    for name, rows in raw["quotes"].items():
        arrs = {k: np.array([r[i] for r in rows], dtype=np.int64 if i < 2 else np.float64)
                for i, k in enumerate(QUOTE_KEYS)}
        order = np.argsort(arrs["local_ts_ns"], kind="stable")
        arrs = {k: v[order] for k, v in arrs.items()}
        with np.load(Path(new_out) / names[name]) as z:
            new = dict(z)
        checks[name + "_raw_exact"] = all(k in new and np.array_equal(new[k], v) for k, v in arrs.items())
        item = {"rows": len(rows), "new_vs_raw": {k: error_stats(new[k], arrs[k]) for k in QUOTE_KEYS[:2]}}
        old_path = Path(old_out) / names[name]
        if old_path.exists():
            with np.load(old_path) as z:
                old = dict(z)
            same_values = all(k in old and np.array_equal(old[k], arrs[k]) for k in QUOTE_KEYS[2:])
            rounded = all(k in old and np.array_equal(old[k], arrs[k].astype(np.float64).astype(np.int64))
                          for k in QUOTE_KEYS[:2])
            item.update(old_vs_new={k: error_stats(old[k], new[k]) for k in QUOTE_KEYS[:2]},
                        old_values_match_raw=same_values, old_timestamps_equal_float64_rounding=rounded)
            checks[name + "_old_difference_explained"] = same_values and rounded
        else:
            item["old_missing"] = True
            checks[name + "_old_difference_explained"] = name.startswith("gate_")
        quote_reports[name] = item
    with np.load(Path(new_out) / names["gate_l2_hbt"]) as z:
        hbt = z["data"]
    expected_trades = [v[1] for v in raw["trades"].values()]
    for direction, label in ((EXCH_EVENT, "exchange"), (LOCAL_EVENT, "local")):
        checks["trades_" + label + "_exact"] = rows_equal(expected_trades, projected_events(hbt, [TRADE_EVENT], direction))
        checks["depth_" + label + "_exact"] = rows_equal(raw["depths"], projected_events(
            hbt, [DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT], direction))
    checks["raw_trade_ids_nonconflicting"] = raw["conflicts"] == 0
    checks["raw_depth_sequence"] = raw["chain_errors"] == 0
    pre = raw["pre"]
    excluded = new_report["initialization_exclusion"]
    checks["initial_exclusion_exact"] = excluded == {"frames": len(pre),
        "first_local_ts": pre[0] if pre else None, "last_local_ts": pre[-1] if pre else None}
    confirmation = new_report.get("execution_unavailable_intervals", [])
    checks["recovery_confirmation_exact"] = bool(raw["recoveries"]) and all(
        any(iv.get("snapshot_wall_ns") == r["snapshot_wall_ns"] and
            iv.get("end_wall_ns") == r.get("confirmation_wall_ns") and
            iv.get("confirmation_lag_ns") == r.get("confirmation_lag_ns") for iv in confirmation)
        for r in raw["recoveries"]) and all("confirmation_wall_ns" in r for r in raw["recoveries"])
    depth_gaps = [iv for iv in protocol["execution_unavailable_intervals"]
                  if iv.get("exchange") == "gate_futures" and iv.get("channel") == "futures.obu"]
    checks["qa_recovery_confirmation_exact"] = bool(depth_gaps) and all(any(
        r.get("confirmation_wall_ns") == iv.get("end_wall_ns") for r in raw["recoveries"]) for iv in depth_gaps)
    gate_cal = [r for r in old_calendar if r.get("exchange") == "gate_futures"]
    old_intervals = [iv for r in gate_cal for iv in r.get("unrebuildable_depth_intervals") or []]
    # Old exclusion is explained only if ALL offending frames are exactly the missing-history prefix.
    excluded_frames = sum(int(iv.get("excluded_frames", 0)) for iv in old_intervals)
    checks["old_gate_exclusion_explained"] = bool(gate_cal) and bool(pre) and excluded_frames == len(pre) and all(
        iv.get("reason") == "missing_same_connection_anchor" and
        pre[0] <= int(iv["start_wall_ns"]) <= int(iv["end_wall_ns"]) <= pre[-1] for iv in old_intervals)
    ids_hash = hashlib.sha256(json.dumps(sorted(raw["trades"]), separators=(",", ":")).encode()).hexdigest()
    return {"passed": all(checks.values()), "checks": checks, "quote_timestamps": quote_reports,
            "gate_depth": {"raw_chain_errors": raw["chain_errors"], "expected_event_rows": len(raw["depths"]),
                           "trace": raw["trace"], "recoveries": raw["recoveries"], "old_calendar": gate_cal},
            "gate_trades": {"raw_copies": raw["copies"], "unique_ids": len(raw["trades"]),
                            "removed_copies": raw["copies"] - len(raw["trades"]), "conflicts": raw["conflicts"],
                            "unique_ids_sha256": ids_hash, "earliest_receive_events_exact":
                            checks["trades_exchange_exact"] and checks["trades_local_exact"],
                            "note": "HBT does not retain public trade IDs; compare multiplicities and every emitted field"},
            "initialization_exclusion": excluded,
            "old_new_difference_explained": all(v for k, v in checks.items()
                                               if "old_difference" in k or k == "old_gate_exclusion_explained")}
