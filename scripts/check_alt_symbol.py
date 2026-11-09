"""Check one free monthly native BBO day exists for a new cross-exchange symbol.

All raw files stay in the Actions runner. Only aggregate quality metrics persist.
"""
import argparse
import csv
import gzip
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from compare_bbo_feeds import scan
from inspect_tardis import download


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", default="2026-11-01")
    parser.add_argument("--asset", default="ENA")
    parser.add_argument("--binance-symbol", default="ENAUSDT")
    parser.add_argument("--okx-symbol", default="ENA-USDT-SWAP")
    parser.add_argument("--data-dir", default="data/raw")
    parser.add_argument("--report-dir", default="reports")
    args = parser.parse_args()

    date = datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if date.day != 1:
        parser.error("Public free datasets require first day of month")
    start = int(date.timestamp() * 1_000_000)
    end = int((date + timedelta(days=1)).timestamp() * 1_000_000)

    report = {"status": "started", "date": args.day, "asset": args.asset, "venues": {}}
    path = Path(args.report_dir) / f"{args.asset.lower()}_native_bbo_preflight_{args.day}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        for venue, exchange, symbol in (
            ("binance", "binance-futures", args.binance_symbol),
            ("okx", "okex-swap", args.okx_symbol),
        ):
            dest = Path(args.data_dir) / args.day / f"{exchange}_{symbol}_book_ticker.csv.gz"
            info = download(exchange, symbol, "book_ticker", args.day, dest)
            with gzip.open(dest, "rt", newline="", encoding="utf-8") as source:
                reader = csv.DictReader(source)
                first = next(reader, None)
            if first is None or first.get("symbol") != symbol or first.get("exchange") != exchange:
                raise ValueError(f"Unexpected symbol/exchange in {dest}: {first}")
            detail = scan(dest, start, end)
            if detail["rows"] < 100 or detail["invalid_rows"] or detail["local_backwards"]:
                raise ValueError(f"Failed quality gate for {symbol}: {detail}")
            report["venues"][venue] = {
                "symbol": symbol,
                "compressed_bytes": info["compressed_bytes"],
                "sha256_gzip": info["sha256_gzip"],
                "scan": detail,
            }
            print(f"{venue}: {symbol}, {detail['rows']:,} native BBO events, "
                  f"{info['compressed_bytes']:,} compressed bytes", flush=True)
        report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Saved {path}; status={report['status']}", flush=True)


if __name__ == "__main__":
    main()
