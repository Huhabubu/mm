"""Probe HEAD semantics and hour availability before whole-month metadata scan."""
import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE = "https://api.cryptohftdata.com/v1/download?file="
CASES = [
    "binance_futures/2026-10-05/02/BTCUSDT_trades.parquet",
    "binance_futures/2026-10-05/02/BTCUSDT_orderbook.parquet",
    "okx_futures/2026-10-05/02/BTC-USDT-SWAP_trades.parquet",
    "bybit/2026-10-05/02/BTCUSDT_trades.parquet",
    "bitget_futures/2026-10-05/02/BTCUSDT_trades.parquet",
    "binance_futures/2026-10-05/02/NOT_A_REAL_SYMBOL_trades.parquet",
]
OUT = Path("reports/chd_head_probe.json")
OUT.parent.mkdir(parents=True, exist_ok=True)
rows = []
for key in CASES:
    row = {"path": key}
    url = BASE + urllib.parse.quote(key, safe="/")
    for method in ("HEAD", "GET"):
        req = urllib.request.Request(url, headers={"User-Agent": "mm-data-qa-probe/1.0",
                             "Range": "bytes=0-0"} if method == "GET" else
                             {"User-Agent": "mm-data-qa-probe/1.0"}, method=method)
        try:
            with urllib.request.urlopen(req, timeout=40) as r:
                row[method] = {"status": r.status, "type": r.headers.get("Content-Type"),
                               "length": r.headers.get("Content-Length"),
                               "content_range": r.headers.get("Content-Range"),
                               "final_url_host": urllib.parse.urlparse(r.url).hostname}
                if method == "GET":
                    r.read(1)
        except urllib.error.HTTPError as ex:
            row[method] = {"status": ex.code, "error": str(ex.reason)}
        except Exception as ex:
            row[method] = {"error": str(ex)}
    rows.append(row)
    OUT.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(row, ensure_ascii=False), flush=True)
