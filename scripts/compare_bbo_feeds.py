"""Compare native BBO book_ticker with L2-derived quotes on one free Tardis day.

Download normalized Tardis CSVs to a temporary GitHub Actions runner. Publish
aggregate QA only: no individual market-data observations or file attachments.
"""
import argparse
import csv
import gzip
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

from inspect_tardis import Sample, download

FEEDS = {"binance": ("binance-futures", "BTCUSDT"),
         "okx": ("okex-swap", "BTC-USDT-SWAP")}
KINDS = ("quotes", "book_ticker")
STEP_US = 10_000  # 10ms, enough for a first feed-level QA comparison


def iter_bbo(path):
    """Preserve native event order; discard malformed/crossed quotes only for alignment."""
    with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"timestamp", "local_timestamp", "bid_price", "ask_price",
                    "bid_amount", "ask_amount", "exchange", "symbol"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns in {path}: {sorted(missing)}")
        for row in reader:
            try:
                t = int(row["local_timestamp"])
                bid, ask = float(row["bid_price"]), float(row["ask_price"])
                bid_amount, ask_amount = float(row["bid_amount"]), float(row["ask_amount"])
            except (ValueError, KeyError, TypeError):
                continue
            if (all(math.isfinite(v) for v in (bid, ask, bid_amount, ask_amount))
                and 0 < bid < ask and bid_amount >= 0 and ask_amount >= 0):
                yield t, (bid + ask) * 0.5


def scan(path, day_start, day_end):
    """One-pass completeness and cadence statistics, in collection-timestamp order."""
    rows = valid = invalid = local_backwards = exchange_backwards = 0
    same_local = changes = mid_changes = gaps_over_1s = 0
    first = last = prior_t = prior_e = prior_bbo = prior_mid = None
    local_gaps, capture_offset = Sample(seed=42), Sample(seed=43)
    with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"timestamp", "local_timestamp", "bid_price", "ask_price",
                    "bid_amount", "ask_amount", "exchange", "symbol"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns in {path}: {sorted(missing)}")
        for row in reader:
            rows += 1
            try:
                t, exch = int(row["local_timestamp"]), int(row["timestamp"])
                bid, ask = float(row["bid_price"]), float(row["ask_price"])
                b_amt, a_amt = float(row["bid_amount"]), float(row["ask_amount"])
            except (KeyError, ValueError, TypeError):
                invalid += 1
                continue
            if first is None:
                first = t
            last = t
            if not (day_start <= t < day_end):
                raise ValueError(f"Timestamp outside selected UTC day: {t} in {path}")
            if prior_t is not None:
                gap = t - prior_t
                if gap < 0:
                    local_backwards += 1
                elif gap == 0:
                    same_local += 1
                else:
                    local_gaps.add(gap)
                    if gap > 1_000_000:
                        gaps_over_1s += 1
            if prior_e is not None and exch < prior_e:
                exchange_backwards += 1
            prior_t, prior_e = t, exch
            capture_offset.add(t - exch)
            if (not all(math.isfinite(v) for v in (bid, ask, b_amt, a_amt))
                or bid <= 0 or bid >= ask or b_amt < 0 or a_amt < 0):
                invalid += 1
                continue
            valid += 1
            bbo = (bid, ask, b_amt, a_amt)
            mid = (bid + ask) / 2
            if prior_bbo is not None and bbo != prior_bbo:
                changes += 1
            if prior_mid is not None and mid != prior_mid:
                mid_changes += 1
            prior_bbo, prior_mid = bbo, mid
    if not rows or not valid:
        raise ValueError(f"No valid rows in {path}")
    if local_backwards:
        raise ValueError(f"Local timestamps not monotonic in {path} ({local_backwards})")
    return {
        "rows": rows, "valid_rows": valid, "invalid_rows": invalid,
        "local_backwards": local_backwards, "exchange_timestamp_backwards": exchange_backwards,
        "same_local_timestamp": same_local, "bbo_changes": changes,
        "mid_changes": mid_changes, "positive_gap_over_1s": gaps_over_1s,
        "first_local_us": first, "last_local_us": last,
        "local_gap_us_approx": local_gaps.result(),
        "capture_minus_exchange_us_approx": capture_offset.result(),
    }


def compare_on_grid(native_path, l2_path, start_us, end_us):
    """Two feeds of ONE venue, both past-only at 10ms grid. For QA only."""
    it = {"native": iter(iter_bbo(native_path)),
          "l2": iter(iter_bbo(l2_path))}
    pending = {k: next(v, None) for k, v in it.items()}
    current = {"native": None, "l2": None}
    counts = {"grid": 0, "both_present": 0, "different_mid": 0,
              "absolute_diff_above_0_1bps": 0, "age_native_over_100ms": 0,
              "age_l2_over_100ms": 0}
    abs_mid_diff_bps = Sample(seed=41)
    age_native = Sample(seed=44)
    age_l2 = Sample(seed=45)
    for t in range(start_us, end_us, STEP_US):
        counts["grid"] += 1
        for k in it:
            while pending[k] is not None and pending[k][0] <= t:
                current[k] = pending[k]
                pending[k] = next(it[k], None)
        a, b = current["native"], current["l2"]
        if a is None or b is None:
            continue
        counts["both_present"] += 1
        delta = abs(a[1] - b[1]) / a[1] * 1e4
        abs_mid_diff_bps.add(delta)
        age_native.add(t - a[0])
        age_l2.add(t - b[0])
        if delta > 1e-8:
            counts["different_mid"] += 1
        if delta > 0.1:
            counts["absolute_diff_above_0_1bps"] += 1
        if t - a[0] > 100_000:
            counts["age_native_over_100ms"] += 1
        if t - b[0] > 100_000:
            counts["age_l2_over_100ms"] += 1
    return {"counts": counts,
            "different_mid_pct": round(100 * counts["different_mid"] / max(1, counts["both_present"]), 3),
            "abs_mid_diff_bps_approx": abs_mid_diff_bps.result(),
            "native_age_us_approx": age_native.result(),
            "l2_age_us_approx": age_l2.result()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", default="2026-11-01")
    parser.add_argument("--data-dir", default="data/raw")
    parser.add_argument("--report-dir", default="reports")
    args = parser.parse_args()
    d = datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if d.day != 1:
        parser.error("Free Tardis datasets available on day 01")
    start, end = int(d.timestamp()*1e6), int((d + timedelta(days=1)).timestamp()*1e6)
    report = {"utc_day": args.day, "status": "started", "venues": {},
              "cautions": [
                  "book_ticker is native BBO and quotes is L2 reconstructed; channel cadence can differ.",
                  "Tardis collectors sharing a city does not guarantee same arrival paths or latencies.",
                  "Same-venue quote ages measured at a fixed 10ms grid are not execution latency.",
                  "No raw observations published; only aggregate QA results.",
              ]}
    try:
        for name, (exchange, symbol) in FEEDS.items():
            files = {}
            result = {"files": {}, "comparison": None}
            report["venues"][name] = result
            for kind in KINDS:
                path = Path(args.data_dir) / args.day / f"{exchange}_{symbol}_{kind}.csv.gz"
                print(f"Download {name}/{kind}", flush=True)
                info = download(exchange, symbol, kind, args.day, path)
                files[kind] = path
                result["files"][kind] = {
                    "compressed_bytes": info["compressed_bytes"], "sha256": info["sha256_gzip"],
                    "scan": scan(path, start, end),
                }
                print(f"  {name}/{kind}: {result['files'][kind]['scan']['rows']:,} rows", flush=True)
            result["comparison"] = compare_on_grid(files["book_ticker"], files["quotes"], start, end)
            print(f"{name} native-vs-L2 disagreement "
                  f"{result['comparison']['different_mid_pct']}%", flush=True)
        report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        dest = Path(args.report_dir) / f"native_bbo_vs_l2_{args.day}.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Saved {dest} status={report['status']}", flush=True)
        for name, info in report["venues"].items():
            if info["comparison"]:
                print(f"summary {name} {json.dumps(info['comparison'], separators=(',', ':'))}", flush=True)


if __name__ == "__main__":
    main()
