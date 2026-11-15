"""25-day availability manifest for CryptoHFTData BTC perpetuals on four venues.

Uses HTTP byte-range GET only (one byte per file) because the archive's
download endpoint returns 404 to HEAD even for extant objects.
This is a coverage check, not orderbook/trade completeness validation.
"""
import argparse
import datetime as dt
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

START = dt.date(2026, 10, 5)
VENUES = {
    "binance_futures": "BTCUSDT",
    "okx_futures": "BTC-USDT-SWAP",
    "bybit": "BTCUSDT",
    "bitget_futures": "BTCUSDT",
}
TYPES = ("orderbook", "trades")
BASE = "https://api.cryptohftdata.com/v1/download?file="
RANGE_RE = re.compile(r"^bytes 0-0/([0-9]+)$")


def inspect(key):
    url = BASE + urllib.parse.quote(key, safe="/")
    for attempt in range(4):
        req = urllib.request.Request(
            url, headers={"Range": "bytes=0-0", "User-Agent": "mm-chd-coverage-202610/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=35) as response:
                content_range = response.headers.get("Content-Range", "")
                match = RANGE_RE.match(content_range)
                if response.status == 206 and match and int(match.group(1)) > 0:
                    response.read(1)
                    return {"status": "present", "size_bytes": int(match.group(1))}
                return {"status": "unknown", "code": response.status,
                        "content_range": content_range[:100]}
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return {"status": "missing", "code": 404}
            if exc.code == 429 or exc.code >= 500:
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                try:
                    wait = float(retry_after)
                except (ValueError, TypeError):
                    wait = min(30, 3 * (attempt + 1))
                time.sleep(min(60, max(3, wait)))
                continue
            return {"status": "unknown", "code": exc.code, "error": str(exc.reason)[:140]}
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt < 3:
                time.sleep(2 * (attempt + 1))
                continue
            return {"status": "unknown", "error": str(exc)[:140]}
    return {"status": "unknown", "error": "Retries exhausted"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--segment", type=int, required=True, choices=range(5))
    args = parser.parse_args()
    path = Path(f"reports/chd_oct2026_manifest_part{args.segment}.json")
    path.parent.mkdir(exist_ok=True, parents=True)
    output = {"segment": args.segment, "type": "range_get_1_byte_existence_only",
              "days": [], "rows": []}
    for offset in range(args.segment * 5, args.segment * 5 + 5):
        day = (START + dt.timedelta(days=offset)).isoformat()
        output["days"].append(day)
        for hour in range(24):
            for venue, symbol in VENUES.items():
                for kind in TYPES:
                    key = f"{venue}/{day}/{hour:02d}/{symbol}_{kind}.parquet"
                    result = inspect(key)
                    output["rows"].append({"key": key, "day": day, "hour": hour,
                                           "venue": venue, "type": kind, **result})
                    time.sleep(1.25)  # Under the 60 requests/minute/IP anonymous limit
            path.write_text(json.dumps(output, ensure_ascii=False), encoding="utf-8")
        tally = {s: sum(r["status"] == s and r["day"] == day for r in output["rows"])
                 for s in ("present", "missing", "unknown")}
        print(f"{day}: {tally}", flush=True)
    print(f"Saved {len(output['rows'])} file statuses to {path}", flush=True)


if __name__ == "__main__":
    main()
