"""Daily rotating-hour content audit of four BTC perpetual feeds, plus Bybit reconnect window."""
import argparse
import datetime as dt
import json
import tempfile
from pathlib import Path

from qa_cryptohftdata_oct_2026 import VENUES, download, scan

START = dt.date(2026, 10, 5)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--segment", type=int, required=True, choices=range(5))
    args = p.parse_args()
    report_path = Path(f"reports/chd_oct2026_daily_content_part{args.segment}.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = {"segment": args.segment,
              "method": "Pre-fixed rotating one-hour daily sample for all four venues, plus Bybit 02:00 UTC trades",
              "limitation": "One-hour-per-day sample and one extra Bybit window cannot certify remaining hours.",
              "files": []}
    for i in range(args.segment * 5, args.segment * 5 + 5):
        date = (START + dt.timedelta(days=i)).isoformat()
        hour = (i * 7 + 2) % 24
        checks = [(v, sym, typ, hour) for v, sym in VENUES.items() for typ in ("trades", "orderbook")]
        if hour != 2:
            checks.append(("bybit", "BTCUSDT", "trades", 2))
        for venue, symbol, kind, check_hour in checks:
            key = f"{venue}/{date}/{check_hour:02d}/{symbol}_{kind}.parquet"
            item = {"key": key, "date": date, "hour": check_hour, "venue": venue, "kind": kind}
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    path = Path(tmp) / "hour.parquet"
                    item.update(download(key, path))
                    item.update(scan(path, venue, kind, date, f"{check_hour:02d}"))
                    item["status"] = "readable"
            except Exception as err:
                item["status"] = "error"
                item["error"] = f"{type(err).__name__}: {err}"
            report["files"].append(item)
            report_path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
            print(json.dumps({k: item.get(k) for k in
                              ("key", "status", "rows", "received_backwards",
                               "max_receive_gap_s", "sequence_gap_candidates",
                               "binance_trade_id_gap_candidates", "error")}, ensure_ascii=False), flush=True)
    failures = sum(r["status"] != "readable" for r in report["files"])
    print(f"Files inspected: {len(report['files'])}, unreadable: {failures}; saved {report_path}")
    if failures:
        raise RuntimeError(f"{failures} content QA files were unreadable; see saved artifact.")


if __name__ == "__main__":
    main()
