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
    """Use vectorized batches to avoid per-level Python loops in L2 files."""
    import numpy as np
    import pyarrow.parquet as pq
    import pyarrow.compute as pc

    pf = pq.ParquetFile(path)
    available = set(pf.schema_arrow.names)
    expected = ["received_time", "event_time", "price", "quantity"]
    if kind == "orderbook":
        expected += ["event_type", "side", "final_update_id", "prev_final_update_id"]
    else:
        expected += ["trade_id"]
    columns = [c for c in expected if c in available]
    if "received_time" not in columns:
        raise ValueError("Missing received_time field")

    start_ns = int(datetime.fromisoformat(
        f"{day}T{hour}:00:00+00:00").timestamp() * 1_000_000_000)
    stop_ns = start_ns + 3_600_000_000_000
    result = dict(
        rows=0, events=0, snapshot_events=0,
        missing_expected_columns=sorted(set(expected) - available),
        received_backwards=0, received_outside_hour=0,
        invalid_price_or_quantity=0, max_receive_gap_s=0.,
        receive_gaps_over_1s=0, receive_gaps_over_10s=0,
        receive_gaps_over_60s=0, sequence_pairs_checked=0,
        sequence_gap_candidates=0, binance_trade_id_gap_candidates=0,
        binance_trade_ids_skipped=0, initial_book_snapshot_observed=False,
        received_min_ns=None, received_max_ns=None,
        anomalies=[],
    )
    previous_t = None
    previous_signature = None
    previous_seq = None
    previous_trade_id = None
    first_book_message = True

    def audit_receive_times(ts):
        nonlocal previous_t
        if len(ts) == 0:
            return
        result["events"] += len(ts)
        prior = np.r_[np.array([previous_t], dtype=np.int64), ts] if previous_t is not None else ts
        if len(prior) > 1:
            ds = np.diff(prior)
            result["received_backwards"] += int(np.count_nonzero(ds < 0))
            positives = ds[ds >= 0]
            if positives.size:
                result["max_receive_gap_s"] = max(result["max_receive_gap_s"], float(np.max(positives)) / 1e9)
                for label, ns in (("receive_gaps_over_1s", 1e9),
                                  ("receive_gaps_over_10s", 10e9),
                                  ("receive_gaps_over_60s", 60e9)):
                    result[label] += int(np.count_nonzero(positives > ns))
            flagged = np.flatnonzero((ds < 0) | (ds > 10_000_000_000))
            for j in flagged[: max(0, 12 - len(result["anomalies"]))]:
                result["anomalies"].append({
                    "previous_received_ns": int(prior[j]),
                    "current_received_ns": int(prior[j + 1]),
                    "gap_seconds": round(int(ds[j]) / 1e9, 6)
                })
        previous_t = int(ts[-1])

    for batch in pf.iter_batches(batch_size=100_000, columns=columns):
        b = {name: batch.column(i) for i, name in enumerate(columns)}
        ts = b["received_time"].to_numpy(zero_copy_only=False)
        if np.any(np.isnan(ts)) if ts.dtype.kind == "f" else False:
            raise ValueError("Null received_time")
        ts = ts.astype(np.int64, copy=False)
        result["rows"] += len(ts)
        result["received_outside_hour"] += int(np.count_nonzero(
            (ts < start_ns) | (ts >= stop_ns)))
        if len(ts):
            lo, hi = int(np.min(ts)), int(np.max(ts))
            result["received_min_ns"] = lo if result["received_min_ns"] is None else min(result["received_min_ns"], lo)
            result["received_max_ns"] = hi if result["received_max_ns"] is None else max(result["received_max_ns"], hi)

        if "price" in b and "quantity" in b:
            prices = b["price"].to_numpy(zero_copy_only=False).astype(np.float64)
            quantities = b["quantity"].to_numpy(zero_copy_only=False).astype(np.float64)
            valid = np.isfinite(prices) & np.isfinite(quantities) & (prices > 0) & (quantities >= 0)
            if kind == "orderbook" and "side" in b:
                sides = np.asarray(b["side"].to_pylist(), dtype=object)
                valid |= (sides == "noop")
            result["invalid_price_or_quantity"] += int(np.count_nonzero(~valid))

        if kind == "trades":
            audit_receive_times(ts)
            if venue == "binance_futures" and "trade_id" in b:
                ids = b["trade_id"].to_numpy(zero_copy_only=False)
                try:
                    ids = ids.astype(np.int64)
                    all_ids = np.r_[np.array([previous_trade_id], dtype=np.int64), ids] if previous_trade_id is not None else ids
                    if len(all_ids) > 1:
                        gaps = np.diff(all_ids)
                        result["binance_trade_id_gap_candidates"] += int(np.sum(gaps[gaps > 1] - 1))
                    if len(ids):
                        previous_trade_id = int(ids[-1])
                except (ValueError, TypeError):
                    result["binance_trade_ids_skipped"] += len(ids)
            continue

        # One order-book update is repeated across many price-level rows.
        # Reduce first, then perform timestamp and venue sequence checks.
        events = np.asarray(b["event_type"].to_pylist(), dtype=object) if "event_type" in b else np.full(len(ts), None)
        seq = pc.fill_null(b["final_update_id"], -1).to_numpy(zero_copy_only=False) if "final_update_id" in b else np.full(len(ts), -1)
        prev = pc.fill_null(b["prev_final_update_id"], -1).to_numpy(zero_copy_only=False) if "prev_final_update_id" in b else np.full(len(ts), -1)
        if not len(ts):
            continue
        boundaries = np.r_[True, (ts[1:] != ts[:-1]) | (events[1:] != events[:-1]) |
                           (seq[1:] != seq[:-1]) | (prev[1:] != prev[:-1])]
        chosen = np.flatnonzero(boundaries)
        if previous_signature == (int(ts[0]), events[0], seq[0], prev[0]):
            chosen = chosen[1:]
        if not chosen.size:
            continue
        event_times = ts[chosen]
        audit_receive_times(event_times)
        result["snapshot_events"] += int(np.count_nonzero(events[chosen] == "snapshot"))
        if first_book_message:
            result["initial_book_snapshot_observed"] = bool(events[chosen[0]] == "snapshot")
            first_book_message = False
        for index in chosen:
            event = events[index]
            sid = seq[index]
            declared = prev[index]
            if event == "snapshot":
                previous_seq = sid
            elif event == "update":
                if previous_seq is not None and venue in ("bybit", "bitget_futures"):
                    result["sequence_pairs_checked"] += 1
                    if sequence_gap(venue, int(previous_seq), int(sid), int(declared)):
                        result["sequence_gap_candidates"] += 1
                previous_seq = sid
        last = chosen[-1]
        previous_signature = (int(ts[last]), events[last], seq[last], prev[last])

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
