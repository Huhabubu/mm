"""Inspect Bybit Oct 24 02 UTC local receive-time reversal and exchange times."""
import datetime as dt
import json
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
from qa_cryptohftdata_oct_2026 import download

OUT = Path("reports/chd_bybit_20261024_02_time_reversal.json")
KEY = "bybit/2026-10-24/02/BTCUSDT_trades.parquet"


def iso(ns):
    return dt.datetime.fromtimestamp(int(ns) / 1e9, dt.timezone.utc).isoformat(timespec="milliseconds")


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as folder:
        p = Path(folder) / "data.parquet"
        meta = download(KEY, p)
        file = pq.ParquetFile(p)
        cols = [x for x in ("received_time", "event_time", "trade_time", "trade_id") if x in file.schema_arrow.names]
        d = file.read(columns=cols).to_pydict()
        received = d["received_time"]
        abnormal = []
        for i in range(1, len(received)):
            delta = (received[i] - received[i - 1]) / 1e9
            if delta < -1 or delta > 60:
                neighbors = []
                for j in range(max(0, i - 2), min(len(received), i + 3)):
                    recv = int(received[j])
                    e = d.get("event_time", [None] * len(received))[j]
                    trade_t = d.get("trade_time", [None] * len(received))[j]
                    neighbors.append({
                        "row_index": j,
                        "received_utc": iso(recv),
                        "event_time_ms": e,
                        "trade_time_ms": trade_t,
                        "recv_minus_trade_ms": round(recv / 1e6 - trade_t, 3) if trade_t is not None else None,
                        "trade_id": str(d["trade_id"][j]) if "trade_id" in d else None,
                    })
                abnormal.append({"delta_received_seconds": round(delta, 6), "row_index": i, "neighbors": neighbors})
        report = {"file": KEY, "bytes": meta["download_bytes"], "rows": len(received),
                  "file_fields": file.schema_arrow.names,
                  "receive_anomalies_over_60s_or_backwards_1s": abnormal,
                  "note": "Event/Trade times are exchange timestamps. Negative receive ordering may indicate backfill/replay or publication-order issues; observation alone cannot determine cause."}
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"rows": report["rows"], "anomalies": len(abnormal),
                      "samples": abnormal[:3]}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
