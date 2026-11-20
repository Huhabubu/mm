"""R1 Gate/Binance six-date capture-lag audit. No trading or HBT full-day batch.

Tardis normalized CSV timestamps are microseconds. Report local_timestamp -
timestamp as capture lag (clock + feed publication + transit), NOT one-way
network latency. Reuse the Stage4A 100-us histogram and percentile convention.
"""
import argparse
import csv
from datetime import datetime, timezone
import gzip
import io
import json
from pathlib import Path
import time
import urllib.error
import urllib.request

from audit_cross_exchange_4a import LagStats, url_for

SYMBOLS = ("BTC", "ETH", "SOL", "DOGE")
DAYS = tuple(f"2026-{m:02d}-01" for m in range(6,12))
COLUMNS = {"timestamp", "local_timestamp", "exchange", "symbol"}


def audit_lag(exchange, symbol, kind, day, opener=urllib.request.urlopen):
    """Stream an entire UTC day; one quotes/BBO CSV row per recorded event."""
    url = url_for(exchange, kind, day, symbol)
    begin = int(datetime.strptime(day, "%Y-%m-%d").replace(
        tzinfo=timezone.utc).timestamp() * 1_000_000)
    end = begin + 86_400_000_000
    stats = LagStats()
    rows = 0
    backwards = 0
    wrong_source = 0
    previous = -1
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent":"mm-gate-capture-lag-r1/1"})
            response = opener(req, timeout=120)
            break
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
            if attempt == 2:
                raise
            time.sleep(2+attempt)
    with response:
        with gzip.GzipFile(fileobj=response) as zipped:
            with io.TextIOWrapper(zipped, encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream)
                if not COLUMNS.issubset(set(reader.fieldnames or [])):
                    raise ValueError(f"Missing source time columns: {url}")
                for row in reader:
                    local = int(row["local_timestamp"])
                    if local >= end:
                        break
                    if local < begin:
                        continue
                    if local < previous:
                        backwards += 1
                    previous = local
                    if row["exchange"] != exchange or row["symbol"] != symbol:
                        wrong_source += 1
                    exch = int(row["timestamp"])
                    if exch <= 0:
                        raise ValueError("Invalid exchange timestamp in "+url)
                    hour = min(23, (local-begin)//3_600_000_000)
                    stats.add(local-exch, int(hour))
                    rows += 1
                    if rows % 4_000_000 == 0:
                        print("GATE_LAG_PROGRESS",day,exchange,symbol,rows,flush=True)
    if not rows or backwards or wrong_source:
        raise ValueError(f"Invalid or empty market time series: rows={rows}, "
                         f"backwards={backwards}, wrong_source={wrong_source}")
    return {"kind":kind,"source":url,"rows":rows,"lag":stats.result(),
            "local_backwards":backwards,"wrong_source":wrong_source}


def run(symbol, day, dest):
    if symbol not in SYMBOLS or day not in DAYS:
        raise ValueError("Out-of-sample Gate R1 audit symbol/day")
    gate_symbol = symbol + "_USDT"
    binance_symbol = symbol + "USDT"
    # Book-depth channel classification is inherited from the Stage4A data
    # audit and explicitly NOT independently verified from raw WS packets.
    mode = ("100ms / 20-level snapshot-derived L2"
            if day < "2026-11-01" else "native incremental L2")
    data = {"status":"started", "symbol":symbol, "day":day,
            "gate_depth_mode_stage4a_classification":mode,
            "gate_depth_raw_ws_independently_verified":False,
            "clock_definition":"Tardis local_timestamp - timestamp, normalized CSV microseconds",
            "timing_caveat":"capture lag is NOT pure exchange-to-client network latency",
            "source_gate":"gate-io-futures quotes (L2 reconstructed BBO)",
            "source_binance":"binance-futures native book_ticker",
            "gate":audit_lag("gate-io-futures",gate_symbol,"quotes",day),
            "binance":audit_lag("binance-futures",binance_symbol,"book_ticker",day)}
    data["delta_gate_minus_binance_ms"] = {
        k: (round(data["gate"]["lag"][k]-data["binance"]["lag"][k],4)
            if isinstance(data["gate"]["lag"][k], (float,int))
            and isinstance(data["binance"]["lag"][k], (float,int)) else None)
        for k in ("p50_ms","p95_ms","p99_ms")}
    data["status"]="complete"
    dest=Path(dest)
    dest.parent.mkdir(parents=True,exist_ok=True)
    dest.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print("GATE_R1_LAG",symbol,day,
          "gate",data["gate"]["lag"]["p50_ms"],
          data["gate"]["lag"]["p95_ms"],data["gate"]["lag"]["p99_ms"],
          "binance",data["binance"]["lag"]["p50_ms"],
          data["binance"]["lag"]["p95_ms"],data["binance"]["lag"]["p99_ms"],
          flush=True)
    return data


def aggregate(folder, output):
    import csv as csv_module
    reports={}
    for p in Path(folder).rglob("gate_r1_lag_*.json"):
        d=json.loads(p.read_text(encoding="utf-8"))
        key=(d["symbol"],d["day"])
        if key in reports:
            raise ValueError(f"Duplicate audit: {key}")
        if d["status"]!="complete":
            raise ValueError(f"Incomplete audit: {key}")
        reports[key]=d
    fields=["symbol","day","depth_mode","gate_rows","binance_rows",
            "gate_p50_ms","gate_p95_ms","gate_p99_ms",
            "binance_p50_ms","binance_p95_ms","binance_p99_ms",
            "delta_p50_ms","delta_p95_ms","delta_p99_ms",
            "gate_negative_lag_rows","binance_negative_lag_rows"]
    rows=[]
    for sym in SYMBOLS:
        for day in DAYS:
            d=reports.get((sym,day))
            if d is None:
                raise ValueError(f"Missing Gate 6-day latency evidence {sym}/{day}")
            ga=d["gate"]["lag"];bi=d["binance"]["lag"]
            row={"symbol":sym,"day":day,"depth_mode":d["gate_depth_mode_stage4a_classification"],
                 "gate_rows":d["gate"]["rows"],"binance_rows":d["binance"]["rows"],
                 "gate_negative_lag_rows":ga["n_negative"],
                 "binance_negative_lag_rows":bi["n_negative"]}
            for field in ("p50","p95","p99"):
                row[f"gate_{field}_ms"]=ga[f"{field}_ms"]
                row[f"binance_{field}_ms"]=bi[f"{field}_ms"]
                row[f"delta_{field}_ms"]=d["delta_gate_minus_binance_ms"][f"{field}_ms"]
            rows.append(row)
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    with (output/"gate_latency_24_asset_days.csv").open("w",newline="",encoding="utf-8") as f:
        writer=csv_module.DictWriter(f,fieldnames=fields)
        writer.writeheader();writer.writerows(rows)
    lines=[
        "# Gate 4币×6日采集延迟 R1",
        "",
        "口径：Tardis local_timestamp − timestamp；P50/P95/P99，单位毫秒；"
        "负值事件单列，百分位按4A相同100微秒直方图统计。",
        "Gate使用 L2 重建 quotes，Binance使用原生 book_ticker；"
        "不同市场的采集差异不能直接解释成单程网络延迟。",
        "前5日使用Stage4A标记的100ms/20档快照；11-01标记为原生增量；"
        "仅有规范化数据，不能独立证明原始WS频道。",
        "",
        "|币|日|Gate模式|Gate P50|P95|P99|Binance P50|P95|P99|ΔP50|ΔP95|ΔP99|",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        fields=(r["symbol"],r["day"],
                "snapshot" if r["day"]<"2026-11-01" else "incremental")
        nums=[r[p] for p in (
            "gate_p50_ms","gate_p95_ms","gate_p99_ms",
            "binance_p50_ms","binance_p95_ms","binance_p99_ms",
            "delta_p50_ms","delta_p95_ms","delta_p99_ms")]
        lines.append("|"+"|".join(map(str,fields))+("|"+
                     "|".join(str(x) for x in nums)+"|"))
    (output/"gate_latency_24_asset_days.md").write_text(
        "\n".join(lines)+"\n",encoding="utf-8")
    print("GATE_R1_24_ROW_TABLE",flush=True)
    print("\n".join(lines),flush=True)
    return rows


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--symbol",choices=SYMBOLS)
    p.add_argument("--day",choices=DAYS)
    p.add_argument("--out")
    p.add_argument("--aggregate-root")
    args=p.parse_args()
    if args.aggregate_root:
        if not args.out: p.error("--out required")
        aggregate(args.aggregate_root,args.out)
    else:
        if not args.symbol or not args.day or not args.out:
            p.error("--symbol --day --out required")
        run(args.symbol,args.day,args.out)
