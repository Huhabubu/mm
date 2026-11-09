"""Download and inspect one free monthly Tardis day without republishing market data.

No third-party dependencies; reads compressed CSV in constant memory.
All timestamps are UTC epoch microseconds. The 10ms alignment is QA only.
"""
import argparse
import csv
import gzip
import hashlib
import json
import random
import shutil
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path


FEEDS = {
    "binance": ("binance-futures", "BTCUSDT"),
    "okx": ("okex-swap", "BTC-USDT-SWAP"),
}
KINDS = ("quotes", "trades")
BASE_URL = "https://datasets.tardis.dev/v1"
MAX_FILE_BYTES = 2_500_000_000


class Sample:
    """Deterministic reservoir sample for approximate quantiles."""
    def __init__(self, limit=20000, seed=13):
        self.limit = limit
        self.rng = random.Random(seed)
        self.values = []
        self.count = 0

    def add(self, value):
        self.count += 1
        if len(self.values) < self.limit:
            self.values.append(value)
        else:
            i = self.rng.randrange(self.count)
            if i < self.limit:
                self.values[i] = value

    def result(self):
        if not self.values:
            return {"sampled": 0, "observed": self.count}
        ordered = sorted(self.values)
        return {
            "observed": self.count,
            "sampled": len(ordered),
            "p50": ordered[int(0.50 * (len(ordered) - 1))],
            "p90": ordered[int(0.90 * (len(ordered) - 1))],
            "p99": ordered[int(0.99 * (len(ordered) - 1))],
        }


def download(exchange, symbol, kind, day, target):
    yyyy, mm, dd = day.split("-")
    url = f"{BASE_URL}/{exchange}/{kind}/{yyyy}/{mm}/{dd}/{symbol}.csv.gz"
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()  # Never trust an incomplete file from an earlier attempt.
    for attempt in range(1, 4):
        temp = target.with_suffix(target.suffix + ".part")
        digest = hashlib.sha256()
        size = 0
        try:
            request = urllib.request.Request(
                url, headers={"User-Agent": "mm-research-data-qa/1.0"}
            )
            with urllib.request.urlopen(request, timeout=120) as response, temp.open("wb") as out:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > MAX_FILE_BYTES:
                        raise RuntimeError(f"Download exceeded {MAX_FILE_BYTES} bytes: {url}")
                    if shutil.disk_usage(target.parent).free < 500_000_000:
                        raise RuntimeError("Remaining runner disk space under 500MB")
                    out.write(chunk)
                    digest.update(chunk)
            if size < 30:
                raise RuntimeError(f"Empty or suspicious compressed response ({size} bytes): {url}")
            temp.replace(target)
            return {"url": url, "compressed_bytes": size, "sha256_gzip": digest.hexdigest()}
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError, RuntimeError) as exc:
            temp.unlink(missing_ok=True)
            if isinstance(exc, urllib.error.HTTPError) and 400 <= exc.code < 500:
                raise RuntimeError(f"Tardis HTTP {exc.code}: {url}") from exc
            if attempt == 3:
                raise RuntimeError(f"Download failed after 3 attempts: {url}: {exc}") from exc
            print(f"retry {attempt}/3 {url}: {exc}", flush=True)
            time.sleep(3 * attempt)


def inspect_csv(path, kind, start_us, end_us):
    required = {"exchange", "symbol", "timestamp", "local_timestamp"}
    required |= {"bid_price", "ask_price", "bid_amount", "ask_amount"} if kind == "quotes" else {"price", "amount", "side"}
    counts = {
        "rows": 0, "parse_errors": 0, "outside_utc_day": 0,
        "local_timestamp_backwards": 0, "same_local_timestamp": 0,
        "exchange_timestamp_backwards": 0, "negative_capture_delay": 0,
        "invalid_quote_or_trade": 0, "crossed_or_locked_quote": 0,
        "identical_adjacent_quotes": 0, "over_1s_local_gap": 0,
    }
    delay = Sample()
    gaps = Sample()
    first_local = last_local = prev_local = prev_exchange = None
    prev_bbo = None
    symbols = set()
    with gzip.open(path, "rt", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path}: missing CSV columns {sorted(missing)}")
        for row in reader:
            counts["rows"] += 1
            symbols.add(row["symbol"])
            try:
                local = int(row["local_timestamp"])
                exch = int(row["timestamp"])
                if kind == "quotes":
                    bid = float(row["bid_price"])
                    ask = float(row["ask_price"])
                    bid_amt = float(row["bid_amount"])
                    ask_amt = float(row["ask_amount"])
                    valid = bid > 0 and ask > 0 and bid_amt >= 0 and ask_amt >= 0
                    if valid and bid >= ask:
                        counts["crossed_or_locked_quote"] += 1
                    bbo = (bid, ask, bid_amt, ask_amt)
                    if prev_bbo == bbo:
                        counts["identical_adjacent_quotes"] += 1
                    prev_bbo = bbo
                else:
                    price = float(row["price"])
                    amount = float(row["amount"])
                    valid = price > 0 and amount > 0 and row["side"] in ("buy", "sell")
                if not valid:
                    counts["invalid_quote_or_trade"] += 1
            except (ValueError, TypeError, KeyError):
                counts["parse_errors"] += 1
                continue
            if first_local is None:
                first_local = local
            last_local = local
            if not start_us <= local < end_us:
                counts["outside_utc_day"] += 1
            if prev_local is not None:
                gap = local - prev_local
                if gap < 0:
                    counts["local_timestamp_backwards"] += 1
                elif gap == 0:
                    counts["same_local_timestamp"] += 1
                else:
                    gaps.add(gap)
                    if gap > 1_000_000:
                        counts["over_1s_local_gap"] += 1
            if prev_exchange is not None and exch < prev_exchange:
                counts["exchange_timestamp_backwards"] += 1
            delta = local - exch
            delay.add(delta)
            if delta < 0:
                counts["negative_capture_delay"] += 1
            prev_local, prev_exchange = local, exch
            if counts["rows"] % 1_000_000 == 0:
                print(f"inspected {path.name}: {counts['rows']:,} rows", flush=True)
    if not counts["rows"]:
        raise RuntimeError(f"CSV contained no data: {path}")
    return {
        "counts": counts, "first_local_timestamp_us": first_local,
        "last_local_timestamp_us": last_local,
        "symbols_seen": sorted(symbols), "capture_delay_us_approx": delay.result(),
        "positive_local_gap_us_approx": gaps.result(),
        "quantiles_are_reservoir_estimates": True,
    }


def quotes(path):
    """Yield valid quotes, in original file order, keyed by collection arrival time."""
    with gzip.open(path, "rt", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                t = int(row["local_timestamp"])
                bid = float(row["bid_price"])
                ask = float(row["ask_price"])
                if bid > 0 and ask > bid:
                    yield t, bid, ask
            except (KeyError, ValueError, TypeError):
                continue


def align_quality(binance_path, okx_path, start_us, end_us, step_us=10_000, freshness_us=2_000_000):
    """Full-day past-only as-of alignment on a fixed grid, retaining aggregate QA only."""
    streams = {name: iter(quotes(path)) for name, path in
               (("binance", binance_path), ("okx", okx_path))}
    pending = {name: next(stream, None) for name, stream in streams.items()}
    latest = {"binance": None, "okx": None}
    thresholds_us = (20_000, 50_000, 100_000, 250_000, 500_000, 2_000_000)
    n = {"grid_points": 0, "both_seen": 0, "both_fresh": 0,
         "missing_initial_side": 0, "stale_either_side": 0,
         "both_fresh_by_threshold_us": {str(v): 0 for v in thresholds_us}}
    ages = {"binance": Sample(seed=1), "okx": Sample(seed=2)}
    relative_mid_bps = Sample(seed=3)
    for t in range(start_us, end_us, step_us):
        n["grid_points"] += 1
        for name in ("binance", "okx"):
            while pending[name] is not None and pending[name][0] <= t:
                latest[name] = pending[name]
                pending[name] = next(streams[name], None)
        b, o = latest["binance"], latest["okx"]
        if b is None or o is None:
            n["missing_initial_side"] += 1
            continue
        n["both_seen"] += 1
        age_b, age_o = t - b[0], t - o[0]
        for threshold in thresholds_us:
            if age_b <= threshold and age_o <= threshold:
                n["both_fresh_by_threshold_us"][str(threshold)] += 1
        if age_b > freshness_us or age_o > freshness_us:
            n["stale_either_side"] += 1
            continue
        n["both_fresh"] += 1
        ages["binance"].add(age_b)
        ages["okx"].add(age_o)
        mb, mo = (b[1] + b[2]) / 2, (o[1] + o[2]) / 2
        relative_mid_bps.add((mb / mo - 1) * 10000)
    return {
        "method": f"past-only as-of by Tardis local_timestamp, {step_us/1000:g}ms grid; QA not trading signal",
        "step_us": step_us, "max_quote_age_us": freshness_us, "counts": n,
        "coverage_pct": round(100 * n["both_fresh"] / max(1, n["grid_points"]), 4),
        "age_binance_us_approx": ages["binance"].result(),
        "age_okx_us_approx": ages["okx"].result(),
        "binance_minus_okx_mid_bps_approx": relative_mid_bps.result(),
        "warning": "Different Tardis collection sites are not a common trading-system clock; "
                   "no causal lead-lag inference from this grid.",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", default="2026-11-01", help="UTC YYYY-MM-01")
    parser.add_argument("--data-dir", default="data/raw")
    parser.add_argument("--report-dir", default="reports")
    args = parser.parse_args()
    date = datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if date.day != 1:
        parser.error("Free Tardis dataset requests are restricted to day 01")
    start_us = int(date.timestamp() * 1_000_000)
    end_us = int((date + timedelta(days=1)).timestamp() * 1_000_000)
    raw = Path(args.data_dir)
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"data_qa_{args.day}.json"
    report = {"day_utc": args.day, "status": "started", "datasets": {}, "alignment": None}
    try:
        for name, (exchange, symbol) in FEEDS.items():
            report["datasets"][name] = {}
            for kind in KINDS:
                path = raw / args.day / f"{exchange}_{symbol}_{kind}.csv.gz"
                print(f"Downloading {exchange} {symbol} {kind}", flush=True)
                info = download(exchange, symbol, kind, args.day, path)
                info["inspection"] = inspect_csv(path, kind, start_us, end_us)
                report["datasets"][name][kind] = info
                print(f"Checked {name}/{kind}: {info['inspection']['counts']['rows']:,} rows", flush=True)
        for name in FEEDS:
            data = report["datasets"][name]["quotes"]["inspection"]
            if data["counts"]["local_timestamp_backwards"] or data["counts"]["parse_errors"]:
                raise RuntimeError(f"{name}: cannot align quote data with nonmonotonic/invalid timestamps")
        report["alignment"] = align_quality(
            raw / args.day / "binance-futures_BTCUSDT_quotes.csv.gz",
            raw / args.day / "okex-swap_BTC-USDT-SWAP_quotes.csv.gz",
            start_us, end_us,
        )
        report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Report: {report_path}; status={report['status']}", flush=True)
        if report["alignment"]:
            print(json.dumps(report["alignment"], indent=2), flush=True)


if __name__ == "__main__":
    main()
