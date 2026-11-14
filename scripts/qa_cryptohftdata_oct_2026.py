"""Sample CryptoHFTData archive QA. Aggregate metrics only; raw market data is deleted."""
import argparse
import hashlib
import json
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

CASES = [
    ("2026-10-05", "02"),
    ("2026-10-15", "12"),
    ("2026-10-25", "23"),
]
VENUES = {
    "binance_futures": "BTCUSDT",
    "okx_futures": "BTC-USDT-SWAP",
    "bybit": "BTCUSDT",
    "bitget_futures": "BTCUSDT",
}
KINDS = ("trades", "orderbook")
MAX_DOWNLOAD_BYTES = 900_000_000
REPORT = Path("reports/chd_oct_2026_btc_sample_qa.json")


def sequence_gap(venue, prior_id, current_id, declared_prior):
    """Check only venue-specific message-level links that the feed actually supplies."""
    if prior_id is None or current_id is None:
        return False
    if venue == "bybit":
        return current_id != prior_id + 1
    if venue == "bitget_futures" and declared_prior is not None:
        return declared_prior != prior_id
    return False


def download(key, path):
    url = "https://api.cryptohftdata.com/v1/download?file=" + urllib.parse.quote(key, safe="/")
    last_error = None
    for attempt in range(3):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "mm-research-data-qa/1.0"})
            with urllib.request.urlopen(request, timeout=90) as response, path.open("wb") as out:
                length = response.headers.get("Content-Length")
                if length and int(length) > MAX_DOWNLOAD_BYTES:
                    raise RuntimeError(f"File exceeds safety limit: {length} bytes")
                digest = hashlib.sha256()
                size = 0
                while True:
                    block = response.read(1024 * 1024)
                    if not block:
                        break
                    size += len(block)
                    if size > MAX_DOWNLOAD_BYTES:
                        raise RuntimeError("File exceeded 900 MB safety limit")
                    out.write(block)
                    digest.update(block)
                if size == 0:
                    raise RuntimeError("Downloaded empty response")
                return {"download_bytes": size, "sha256": digest.hexdigest()}
        except urllib.error.HTTPError as exc:
            last_error = f"HTTP {exc.code}: {exc.reason}"
            if exc.code in (400, 401, 403, 404):
                raise RuntimeError(last_error) from exc
            time.sleep(2 ** attempt)
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = str(exc)
            time.sleep(2 ** attempt)
    raise RuntimeError(last_error or "Unable to download")


def scan(path, venue, kind, day, hour):
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    available = pf.schema_arrow.names
    needed = ["received_time", "event_time", "price", "quantity"]
    if kind == "orderbook":
        needed += ["event_type", "side", "final_update_id", "prev_final_update_id"]
    else:
        needed += ["trade_id"]
    columns = [c for c in needed if c in available]
    if "received_time" not in columns:
        raise ValueError("Missing received_time field")

    hour_start = int(datetime.fromisoformat(f"{day}T{hour}:00:00+00:00").timestamp() * 1_000_000_000)
    hour_end = hour_start + 3_600_000_000_000
    result = {
        "rows": 0, "events": 0, "snapshot_events": 0,
        "missing_expected_columns": sorted(set(needed) - set(columns)),
        "received_backwards": 0, "received_outside_hour": 0,
        "invalid_price_or_quantity": 0, "max_receive_gap_s": 0.0,
        "receive_gaps_over_1s": 0, "receive_gaps_over_10s": 0,
        "receive_gaps_over_60s": 0, "sequence_pairs_checked": 0,
        "sequence_gap_candidates": 0, "binance_trade_id_gap_candidates": 0,
        "binance_trade_ids_skipped": 0, "initial_book_snapshot_observed": False,
        "received_min_ns": None, "received_max_ns": None,
    }
    prior_t = None
    prior_message = None
    prior_seq = None
    prior_trade_id = None
    first_book_event_seen = False

    for batch in pf.iter_batches(batch_size=30_000, columns=columns):
        data = batch.to_pydict()
        for idx, t in enumerate(data["received_time"]):
            result["rows"] += 1
            if t is None:
                result["received_outside_hour"] += 1
                continue
            t = int(t)
            result["received_min_ns"] = t if result["received_min_ns"] is None else min(result["received_min_ns"], t)
            result["received_max_ns"] = t if result["received_max_ns"] is None else max(result["received_max_ns"], t)
            if t < hour_start or t >= hour_end:
                result["received_outside_hour"] += 1

            if "price" in data and "quantity" in data:
                side = data.get("side", [None] * len(data["received_time"]))[idx]
                if side != "noop":
                    try:
                        if float(data["price"][idx]) <= 0 or float(data["quantity"][idx]) < 0:
                            result["invalid_price_or_quantity"] += 1
                    except (TypeError, ValueError):
                        result["invalid_price_or_quantity"] += 1

            if kind == "orderbook":
                event_type = data.get("event_type", [None] * len(data["received_time"]))[idx]
                seq = data.get("final_update_id", [None] * len(data["received_time"]))[idx]
                declared = data.get("prev_final_update_id", [None] * len(data["received_time"]))[idx]
                signature = (t, event_type, seq, declared)
                if signature == prior_message:
                    continue
                prior_message = signature
                if not first_book_event_seen:
                    result["initial_book_snapshot_observed"] = event_type == "snapshot"
                    first_book_event_seen = True
                if event_type == "snapshot":
                    result["snapshot_events"] += 1
                    prior_seq = seq
                elif event_type == "update" and seq is not None:
                    if prior_seq is not None:
                        if venue in ("bybit", "bitget_futures"):
                            result["sequence_pairs_checked"] += 1
                            if sequence_gap(venue, int(prior_seq), int(seq), None if declared is None else int(declared)):
                                result["sequence_gap_candidates"] += 1
                    prior_seq = seq
            else:
                if venue == "binance_futures" and "trade_id" in data:
                    try:
                        trade_id = int(data["trade_id"][idx])
                        if prior_trade_id is not None and trade_id > prior_trade_id + 1:
                            result["binance_trade_id_gap_candidates"] += trade_id - prior_trade_id - 1
                        prior_trade_id = trade_id
                    except (TypeError, ValueError):
                        result["binance_trade_ids_skipped"] += 1

            result["events"] += 1
            if prior_t is not None:
                if t < prior_t:
                    result["received_backwards"] += 1
                else:
                    delta = (t - prior_t) / 1_000_000_000
                    result["max_receive_gap_s"] = max(result["max_receive_gap_s"], delta)
                    for threshold, name in ((1, "receive_gaps_over_1s"), (10, "receive_gaps_over_10s"), (60, "receive_gaps_over_60s")):
                        if delta > threshold:
                            result[name] += 1
            prior_t = t

    result["max_receive_gap_s"] = round(result["max_receive_gap_s"], 6)
    result["parquet_rows_match_metadata"] = result["rows"] == pf.metadata.num_rows
    return result


def self_test():
    assert not sequence_gap("bybit", 100, 101, None)
    assert sequence_gap("bybit", 100, 103, None)
    assert not sequence_gap("bitget_futures", 100, 500, 100)
    assert sequence_gap("bitget_futures", 100, 500, 99)
    assert not sequence_gap("okx_futures", 100, 103, None)
    print("Sequence-link checks: PASS")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return

    REPORT.parent.mkdir(exist_ok=True, parents=True)
    output = {
        "scope": "BTCUSDT perpetual futures, three dates, one UTC hour each, all four venues, trades and L2",
        "samples": CASES,
        "caveat": "Sample QA cannot certify 25 complete days; gaps between received events may reflect inactivity.",
        "files": [],
    }
    for day, hour in CASES:
        for venue, symbol in VENUES.items():
            for kind in KINDS:
                key = f"{venue}/{day}/{hour}/{symbol}_{kind}.parquet"
                row = {"key": key, "venue": venue, "day": day, "hour": hour, "kind": kind}
                with tempfile.TemporaryDirectory() as tmp:
                    path = Path(tmp) / "raw.parquet"
                    try:
                        row.update(download(key, path))
                        row.update(scan(path, venue, kind, day, hour))
                        row["status"] = "readable"
                    except Exception as exc:
                        row["status"] = "error"
                        row["error"] = f"{type(exc).__name__}: {exc}"
                output["files"].append(row)
                REPORT.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
                print(json.dumps({k: row.get(k) for k in ("key", "status", "rows", "events", "error")}, ensure_ascii=False), flush=True)
    successes = sum(r["status"] == "readable" for r in output["files"])
    print(f"Read {successes}/{len(output['files'])} hourly files; report: {REPORT}")


if __name__ == "__main__":
    main()
