"""Follow up new Binance BTC anomalies found by the daily 25-day sample."""
import json
import tempfile
from pathlib import Path

import qa_chd_binance_20261015 as audit
from qa_cryptohftdata_oct_2026 import download

CASES = [
    ("2026-10-20", "11", 1792494208812318975),
    ("2026-10-23", "08", 1792744228304523076),
]
OUT = Path("reports/chd_binance_oct_followup.json")


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    report = {"source": "CryptoHFTData direct hourly Parquet files", "cases": []}
    for day, hour, focus in CASES:
        audit.FOCUS_NS = focus
        case = {"day": day, "hour": hour, "focus_ns": focus}
        with tempfile.TemporaryDirectory() as temp:
            for kind, fn in (("trades", audit.audit_trades), ("orderbook", audit.audit_book)):
                key = f"binance_futures/{day}/{hour}/BTCUSDT_{kind}.parquet"
                file = Path(temp) / f"{kind}.parquet"
                try:
                    meta = download(key, file)
                    case[kind] = {**meta, **fn(file), "status": "ok"}
                except Exception as err:
                    case[kind] = {"status": "error", "error": f"{type(err).__name__}: {err}"}
                OUT.write_text(json.dumps(report | {"pending_case": case}, indent=2), encoding="utf-8")
        report["cases"].append(case)
        OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps({"day": day,
                          "trade_gap_ids": case["trades"].get("id_missing_total"),
                          "book_chain_breaks": case["orderbook"].get("chain_breaks"),
                          "book_snapshot_bridges": case["orderbook"].get("valid_snapshot_bridges"),
                          "book_gap_seconds": case["orderbook"].get("largest_receive_gap_seconds")}), flush=True)
    assert all(case[k]["status"] == "ok" for case in report["cases"] for k in ("trades", "orderbook"))


if __name__ == "__main__":
    main()
