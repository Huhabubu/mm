"""One-time independent provenance audit of Binance 2026-10-15 BTC hour 12.
Downloads only two hour files; publishes aggregate diagnostics without raw trades.
"""
import collections
import datetime as dt
import json
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
from qa_cryptohftdata_oct_2026 import download

OUT = Path("reports/chd_binance_20261015_anomaly_audit.json")
DAY, HOUR = "2026-10-15", "12"
FOCUS_NS = 1792067432488662594


def iso(ns):
    return dt.datetime.fromtimestamp(int(ns) / 1e9, dt.timezone.utc).isoformat(timespec="milliseconds")


def audit_trades(path):
    f = pq.ParquetFile(path)
    available = set(f.schema_arrow.names)
    keys = [x for x in ["trade_id", "received_time", "event_time", "trade_time",
                        "order_type", "price", "quantity"] if x in available]
    data = f.read(columns=keys).to_pydict()
    count = len(data["trade_id"])
    zeros = []
    na = 0
    exceptional = 0
    ids = data["trade_id"]
    nonincrement = 0
    gaps = []
    for i in range(count):
        typ = data.get("order_type", [None] * count)[i]
        pr, qty = data["price"][i], data["quantity"][i]
        p0 = float(pr) <= 0 or float(qty) <= 0
        if p0:
            if typ == "NA" and float(pr) == 0 and float(qty) == 0:
                na += 1
            else:
                exceptional += 1
                if len(zeros) < 10:
                    zeros.append({"trade_id": ids[i], "order_type": typ, "price": str(pr), "quantity": str(qty)})
        if i:
            diff = int(ids[i]) - int(ids[i - 1])
            if diff <= 0:
                nonincrement += 1
            elif diff > 1:
                gaps.append({
                    "prev_id": ids[i - 1], "next_id": ids[i], "missing_id_count": diff - 1,
                    "prev_received": iso(data["received_time"][i - 1]),
                    "next_received": iso(data["received_time"][i]),
                    "gap_seconds": round((int(data["received_time"][i]) - int(data["received_time"][i - 1])) / 1e9, 6),
                })
    gaps.sort(key=lambda x: -x["missing_id_count"])
    return dict(rows=count, zero_type_NA=na, zero_other=exceptional,
                zero_other_examples=zeros, id_nonincreasing=nonincrement,
                id_gap_events=len(gaps), id_missing_total=sum(x["missing_id_count"] for x in gaps),
                largest_id_gaps=gaps[:20], time_window_note="ID gaps are candidates until reconciled with official exchange archives.")


def audit_book(path):
    import numpy as np
    import pyarrow.compute as pc
    f = pq.ParquetFile(path)
    columns = [x for x in ["received_time", "event_time", "event_type", "first_update_id",
                             "final_update_id", "prev_final_update_id", "last_update_id"] if x in f.schema_arrow.names]
    events = []
    signature_prev = None
    prev_final = None
    previous_was_snapshot = False
    checked = broken = snapshot_count = back_count = snapshot_bridges = 0
    broken_examples = []
    adjacent_gap_max = (0, None)
    last_t = None

    for batch in f.iter_batches(batch_size=120_000, columns=columns):
        data = {name: batch.column(i) for i, name in enumerate(columns)}
        t = data["received_time"].to_numpy(zero_copy_only=False)
        ty = np.asarray(data["event_type"].to_pylist(), dtype=object)
        fseq = pc.fill_null(data["final_update_id"], -1).to_numpy(zero_copy_only=False)
        firstseq = pc.fill_null(data["first_update_id"], -1).to_numpy(zero_copy_only=False)
        pu = pc.fill_null(data["prev_final_update_id"], -1).to_numpy(zero_copy_only=False)
        if len(t) == 0:
            continue
        change = np.r_[True, (t[1:] != t[:-1]) | (ty[1:] != ty[:-1]) |
                       (fseq[1:] != fseq[:-1]) | (pu[1:] != pu[:-1])]
        indices = np.flatnonzero(change)
        if signature_prev == (int(t[0]), ty[0], int(fseq[0]), int(pu[0])):
            indices = indices[1:]
        if not len(indices):
            continue
        evt_times = t[indices]
        prev_times = np.r_[np.array([last_t], dtype=np.int64), evt_times] if last_t is not None else evt_times
        if len(prev_times) > 1:
            ds = np.diff(prev_times)
            back_count += int(np.count_nonzero(ds < 0))
            maximum = int(np.max(ds))
            if maximum > adjacent_gap_max[0]:
                idx = int(np.argmax(ds))
                adjacent_gap_max = (maximum, (int(prev_times[idx]), int(prev_times[idx + 1])))
        last_t = int(evt_times[-1])
        for i in indices:
            timestamp = int(t[i])
            etype = ty[i]
            seq = int(fseq[i])
            prev = int(pu[i])
            if abs(timestamp - FOCUS_NS) <= 16_000_000_000 and len(events) < 5000:
                events.append({"t": iso(timestamp), "received_ns": timestamp,
                               "type": etype, "first": int(firstseq[i]), "final": seq, "prev": prev})
            if etype == "snapshot":
                snapshot_count += 1
                prev_final = seq
                previous_was_snapshot = True
            elif etype == "update":
                if prev_final is not None and prev != -1:
                    checked += 1
                    bridge = previous_was_snapshot and int(firstseq[i]) <= prev_final <= seq
                    if bridge and prev != prev_final:
                        snapshot_bridges += 1
                    elif prev != prev_final:
                        broken += 1
                        if len(broken_examples) < 16:
                            broken_examples.append({
                                "received": iso(timestamp),
                                "previous_final": prev_final,
                                "received_previous_final": prev,
                                "current_final": seq,
                            })
                prev_final = seq
                previous_was_snapshot = False
        k = int(indices[-1])
        signature_prev = (int(t[k]), ty[k], int(fseq[k]), int(pu[k]))

    gap_start, gap_end = adjacent_gap_max[1] if adjacent_gap_max[1] else (None, None)
    return dict(rows=f.metadata.num_rows, snapshots=snapshot_count,
                chain_pairs_checked=checked, chain_breaks=broken,
                valid_snapshot_bridges=snapshot_bridges,
                chain_break_examples=broken_examples,
                received_backwards=back_count,
                largest_receive_gap_seconds=round(adjacent_gap_max[0] / 1e9, 6),
                largest_receive_gap={"from": iso(gap_start), "to": iso(gap_end)} if gap_start is not None else None,
                focus_events=events[:100] + events[-100:] if len(events) > 200 else events,
                focus_events_total=len(events))


def main():
    OUT.parent.mkdir(exist_ok=True, parents=True)
    r = {"date": DAY, "hour": HOUR, "venue": "binance_futures", "symbol": "BTCUSDT",
         "focus_received_ns": FOCUS_NS, "focus_utc": iso(FOCUS_NS)}
    with tempfile.TemporaryDirectory() as tmp:
        for kind, fn in (("trades", audit_trades), ("orderbook", audit_book)):
            key = f"binance_futures/{DAY}/{HOUR}/BTCUSDT_{kind}.parquet"
            path = Path(tmp) / f"{kind}.parquet"
            try:
                d = download(key, path)
                r[kind] = {**d, **fn(path), "status": "ok"}
            except Exception as ex:
                r[kind] = {"status": "error", "error": str(ex)}
            OUT.write_text(json.dumps(r, indent=2, ensure_ascii=False), encoding="utf-8")
            print(json.dumps({kind: {k: v for k, v in r[kind].items() if k in
                  ("status", "rows", "zero_type_NA", "zero_other", "id_missing_total", "chain_breaks", "largest_receive_gap_seconds", "error")}}, ensure_ascii=False), flush=True)
    assert all(r[k]["status"] == "ok" for k in ("trades", "orderbook"))


if __name__ == "__main__":
    main()
