"""Aggregate independent 5-day archive manifests. No raw market data included."""
import collections
import datetime as dt
import json
from pathlib import Path

PARTS = Path("manifest_parts")
OUT = Path("reports")
OUT.mkdir(exist_ok=True, parents=True)
all_rows = []
for i in range(5):
    matches = list(PARTS.glob(f"**/chd_oct2026_manifest_part{i}.json"))
    if len(matches) != 1:
        raise RuntimeError(f"Expected exactly one report for part {i}, found {len(matches)}")
    content = json.loads(matches[0].read_text())
    if len(content["rows"]) != 960:
        raise RuntimeError(f"Unexpected item count in part {i}: {len(content['rows'])}")
    all_rows += content["rows"]
if len(all_rows) != 4800 or len(set(r["key"] for r in all_rows)) != 4800:
    raise RuntimeError("Missing or duplicated hourly file keys")
counts = collections.Counter(r["status"] for r in all_rows)
by_venue = {}
for venue in sorted(set(r["venue"] for r in all_rows)):
    sub = [r for r in all_rows if r["venue"] == venue]
    by_venue[venue] = dict(collections.Counter(r["status"] for r in sub))
by_day = {}
for day in sorted(set(r["day"] for r in all_rows)):
    sub = [r for r in all_rows if r["day"] == day]
    by_day[day] = {"counts": dict(collections.Counter(r["status"] for r in sub)),
                   "complete_8_stream_hours": sum(
                       all(r["status"] == "present" for r in sub if r["hour"] == hr)
                       for hr in range(24))}
report = {
    "scope": "BTCUSDT perpetual, Binance OKX Bybit Bitget, 2026-10-05 to 2026-10-29 UTC",
    "method": "one-byte HTTP range GET existence plus Content-Range total object size",
    "limitation": "This checks file presence only. Valid file presence does not establish full event completeness, sequence integrity or synchronized timestamps.",
    "files_checked": len(all_rows), "counts": dict(counts),
    "by_venue": by_venue, "by_day": by_day,
    "missing": [r for r in all_rows if r["status"] == "missing"],
    "unknown": [r for r in all_rows if r["status"] == "unknown"],
}
(OUT / "chd_oct2026_manifest_summary.json").write_text(
    json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
table = ["# CryptoHFTData 2026-10-05 to 10-29 BTC hourly file presence",
         "", "Range GET existence only; no claim of orderbook/trade completeness.",
         "", f"Total: {len(all_rows)} files; statuses: {dict(counts)}", "",
         "| Date UTC | Present | Missing | Unknown | Fully covered hours (8 streams) |",
         "|---|---:|---:|---:|---:|"]
for day, item in by_day.items():
    c = item["counts"]
    table.append(f"| {day} | {c.get('present',0)} | {c.get('missing',0)} | {c.get('unknown',0)} | {item['complete_8_stream_hours']}/24 |")
if report["missing"] or report["unknown"]:
    table.extend(["", "## Missing and inconclusive files", ""])
    for row in report["missing"] + report["unknown"]:
        table.append(f"- {row['status']}: {row['key']} ({row.get('error',row.get('code',''))})")
(OUT / "chd_oct2026_manifest_summary.md").write_text("\n".join(table) + "\n", encoding="utf-8")
print(json.dumps({"counts": report["counts"], "by_venue": report["by_venue"],
                  "days_all_24h_covered": sum(d["complete_8_stream_hours"] == 24 for d in by_day.values()),
                  "missing_examples": report["missing"][:10],
                  "unknown_examples": report["unknown"][:10]}, indent=2))
