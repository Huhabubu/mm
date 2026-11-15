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
    f = pq.ParquetFile(path)
    columns = [x for x in ["received_time", "event_time", "event_type", "first_update_id",
                             "final_update_id", "prev_final_update_id", "last_update_id"] if x in f.schema_arrow.names]
    events = []
    signatures_prev = None
    prev_final = None
    checked = 0
    broken = 0
    broken_examples = []
    snapshot_count = 0
    adjacent_gap_max = (0, None)
    last_t = None
    back_count = 0
    for batch in f.iter_batches(batch_size=100_000, columns=columns):
        rows = batch.to_pydict()
        n = len(rows["received_time"])
        for i in range(n):
            record = {x: rows[x][i] for x in columns}
            signature = (record["received_time"], record["event_type"],
                         record.get("final_update_id"), record.get("prev_final_update_id"))
            if signature == signatures_prev:
                continue
            signatures_prev = signature
            timestamp = record["received_time"]
            etype = record["event_type"]
            if last_t is not None:
                d = int(timestamp) - int(last_t)
                if d < 0:
                    back_count += 1
                if d > adjacent_gap_max[0]:
                    adjacent_gap_max = (d, (last_t, timestamp))
            last_t = timestamp
            if abs(timestamp - FOCUS_NS) <= 30_000_000_000 and len(events) < 5000:
                events.append({
                    "t": iso(timestamp),
                    "received_ns": timestamp,
                    "event_time": record.get("event_time"),
                    "type": etype,
                    "first": record.get("first_update_id"),
                    "final": record.get("final_update_id"),
                    "prev": record.get("prev_final_update_id"),
                    "last": record.get("last_update_id"),
                })
            if etype == "snapshot":
                snapshot_count += 1
                prev_final = record.get("final_update_id")
            elif etype == "update":
                pu = record.get("prev_final_update_id")
                if prev_final is not None and pu is not None:
                    checked += 1
                    if pu != prev_final:
                        broken += 1
                        if len(broken_examples) < 16:
                            broken_examples.append({
                                "received": iso(timestamp),
                                "previous_final": prev_final,
                                "received_previous_final": pu,
                                "current_final": record.get("final_update_id"),
                                "current_first": record.get("first_update_id"),
                            })
                prev_final = record.get("final_update_id")
    # Keep a compact window around the largest time gap and snapshot boundaries.
    focus_events = events
    if len(events) > 160:
        focus_events = events[:80] + events[-80:]
    gap_start, gap_end = adjacent_gap_max[1] if adjacent_gap_max[1] else (None, None)
    return dict(rows=f.metadata.num_rows, snapshots=snapshot_count,
                chain_pairs_checked=checked, chain_breaks=broken,
                chain_break_examples=broken_examples,
                received_backwards=back_count,
                largest_receive_gap_seconds=round(adjacent_gap_max[0] / 1e9, 6),
                largest_receive_gap={"from": iso(gap_start), "to": iso(gap_end)} if gap_start else None,
                focus_events=focus_events,
                focus_events_reduced=len(events) > 160)


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
