"""Cross-check three CryptoHFTData trade-ID gaps against Binance official daily archive.

Source: Binance public data futures/um/daily/trades/BTCUSDT.
Download is bounded, CSV is streamed, no exchange rows are published.
"""
import csv
import hashlib
import json
import re
import tempfile
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

DAY = "2026-10-15"
SYMBOL = "BTCUSDT"
INTERVALS = [(8175325806, 8175335361), (8175335391, 8175340434), (8175381515, 8175383867)]
BASE = f"https://data.binance.vision/data/futures/um/daily/"
MAX_ZIP_BYTES = 850 * 1024 * 1024
OUTPUT = Path("reports/chd_binance_20261015_exchange_crosscheck.json")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)


def acquire(kind, target):
    url = f"{BASE}{kind}/{SYMBOL}/{SYMBOL}-{kind}-{DAY}.zip"
    req = urllib.request.Request(url, headers={"User-Agent": "mm-research-crosscheck/1.0", "Range": "bytes=0-0"})
    meta = {"url": url}
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            meta.update({"probe_code": r.status, "probe_content_range": r.headers.get("Content-Range"),
                         "probe_length": r.headers.get("Content-Length")})
            match = re.fullmatch(r"bytes 0-0/([0-9]+)", r.headers.get("Content-Range", ""))
            estimate = int(match.group(1)) if match else None
            if estimate is not None and estimate > MAX_ZIP_BYTES:
                meta["status"] = "skipped_too_large"
                return meta
    except Exception as ex:
        meta.update({"status": "probe_failed", "error": str(ex)})
        return meta
    try:
        req2 = urllib.request.Request(url, headers={"User-Agent": "mm-research-crosscheck/1.0"})
        with urllib.request.urlopen(req2, timeout=90) as r, target.open("wb") as out:
            total = 0
            digest = hashlib.sha256()
            while True:
                b = r.read(4 * 1024 * 1024)
                if not b:
                    break
                total += len(b)
                if total > MAX_ZIP_BYTES:
                    raise RuntimeError("Daily archive exceeds download safety cap")
                out.write(b)
                digest.update(b)
            meta.update({"status": "downloaded", "bytes": total, "sha256": digest.hexdigest()})
    except Exception as ex:
        meta.update({"status": "download_failed", "error": str(ex)})
    return meta


def inspect(path, kind):
    found = [0] * len(INTERVALS)
    first_match = [None] * len(INTERVALS)
    last_match = [None] * len(INTERVALS)
    total = 0
    with zipfile.ZipFile(path) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise RuntimeError(f"Unexpected ZIP members: {z.namelist()[:15]}")
        with z.open(names[0]) as handle:
            import io
            reader = csv.reader(io.TextIOWrapper(handle, encoding="utf-8", newline=""))
            for row in reader:
                if not row:
                    continue
                try:
                    if kind == "trades":
                        lower = upper = int(row[0])
                    else:
                        lower, upper = int(row[3]), int(row[4])
                except (IndexError, TypeError, ValueError):
                    continue
                total += 1
                for j, (start, end) in enumerate(INTERVALS):
                    overlap_lo, overlap_hi = max(start, lower), min(end, upper)
                    if overlap_lo <= overlap_hi:
                        # For aggTrades ranges, count covered trade IDs, not aggregate rows.
                        qty = overlap_hi - overlap_lo + 1
                        found[j] += qty
                        if first_match[j] is None:
                            first_match[j] = (lower, upper)
                        last_match[j] = (lower, upper)
    return {"archive_csv_records": total, "overlap": [
        {"start_id": a, "end_id": b, "gap_size": b - a + 1,
         "exchange_market_id_overlap": found[i],
         "first_matching_row_id_span": first_match[i],
         "last_matching_row_id_span": last_match[i]}
        for i, (a, b) in enumerate(INTERVALS)]}


def main():
    report = {"day_utc": DAY, "venue": "binance_futures", "symbol": SYMBOL,
              "method": "Cross-compare CryptoHFTData missing ID intervals with Binance official daily archive",
              "note": "AggTrades uses [firstTradeId,lastTradeId] envelopes; range overlap is not proof that every enclosed ID is an individual market trade.",
              "archives": {}}
    with tempfile.TemporaryDirectory() as work:
        for kind in ("trades", "aggTrades"):
            path = Path(work) / f"{kind}.zip"
            entry = acquire(kind, path)
            if entry["status"] == "downloaded":
                try:
                    entry.update(inspect(path, kind))
                except Exception as exc:
                    entry["inspect_error"] = str(exc)
            report["archives"][kind] = entry
            OUTPUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            print(json.dumps({kind: entry}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
