"""Read-only 1.645-sigma Gate BTC official-HBT fill quality / native quote markout.

Never changes strategy, order matching, fee, or calibration.
Quote clock: Tardis Gate quotes LOCAL receipt timestamp, in microseconds.
Fill clock: HBT local observation timestamp, in nanoseconds.
For the entry quote require quote_time < fill_observed_ns (strict).
For future horizons require quote_time <= fill_observed_ns + horizon_ns.
Both endpoints must be <=500ms old for the primary 'fresh' result.
"""
import argparse
import csv
import gzip
import io
import json
import math
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
import numpy as np

DAYS = tuple(f"2026-{m:02d}-01" for m in range(6, 12))
MODELS = ("glft", "both_1p0")
HORIZONS = (100, 500, 1000)
FRESH_NS = 500_000_000
ROOT = "https://datasets.tardis.dev/v1/gate-io-futures/quotes"
GROUPS = ("all", "buy", "sell")


def read_quotes(day, url=None):
    year, month, daynum = day.split("-")
    url = url or f"{ROOT}/{year}/{month}/{daynum}/BTC_USDT.csv.gz"
    t, m = [], []
    seen = 0
    equal = 0
    prev = -1
    with urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "mm-stage2-markout/1.0"}),
            timeout=240) as source:
        with gzip.GzipFile(fileobj=source) as zipped:
            with io.TextIOWrapper(zipped, encoding="utf-8", newline="") as text:
                r = csv.DictReader(text)
                needed = {"local_timestamp", "timestamp", "bid_price", "ask_price"}
                if not needed.issubset(set(r.fieldnames or ())):
                    raise ValueError(f"Gate native quote columns missing: {needed}")
                for row in r:
                    us = int(row["local_timestamp"])
                    exch = int(row["timestamp"])
                    bid, ask = float(row["bid_price"]), float(row["ask_price"])
                    if (us < prev or exch > us or exch <= 0
                            or not (math.isfinite(bid) and math.isfinite(ask)
                                    and 0 < bid < ask)):
                        raise ValueError(f"Invalid quote clock/price on {day}")
                    ns = us * 1000
                    value = (bid + ask) * 0.5
                    if us == prev:
                        m[-1] = value  # keep last state of same-microsecond batch
                        equal += 1
                    else:
                        t.append(ns)
                        m.append(value)
                    prev = us
                    seen += 1
    if not t:
        raise ValueError("Empty Gate quotes")
    ts = np.asarray(t, dtype=np.int64)
    mid = np.asarray(m, dtype=np.float64)
    base = int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1e9)
    if ts[0] > base + 5_000_000_000 or ts[-1] < base + 86_370_000_000_000:
        raise ValueError(f"Incomplete Gate native quote day: {day}")
    gaps = np.diff(ts)
    if len(gaps) and np.max(gaps) > 30_000_000_000:
        raise ValueError(f"Unexplained Gate quote gap over 30s on {day}")
    return ts, mid, {
        "source": url, "raw_rows": seen, "unique_microsecond_states": len(ts),
        "same_microsecond_rows": equal,
        "first_quote_ns": int(ts[0]), "last_quote_ns": int(ts[-1]),
        "max_quote_gap_ms": float(np.max(gaps) / 1e6) if len(gaps) else 0.0,
    }


def weighted_p10_and_es10(x, weights):
    """A notional-weighted lower-tail quantile and exactly 10%-mass expected shortfall."""
    order = np.argsort(x, kind="stable")
    xs = x[order]
    w = weights[order]
    total = float(np.sum(w))
    if not total > 0:
        raise ValueError("Missing positive notional weights")
    cutoff = total * 0.10
    cum = np.cumsum(w)
    k = int(np.searchsorted(cum, cutoff, side="left"))
    q10 = float(xs[min(k, len(xs)-1)])
    previous = float(cum[k-1]) if k else 0.0
    tail_weighted_sum = float(np.dot(xs[:k], w[:k]) + xs[k] * (cutoff-previous))
    return q10, tail_weighted_sum/cutoff


def stats(x, w):
    if not len(x):
        return None
    if not (np.isfinite(x).all() and np.isfinite(w).all()
            and np.all(w > 0)):
        raise ValueError("Nonfinite markout/notional")
    n = len(x)
    k = max(1, math.ceil(0.10*n))
    lower = np.sort(x, kind="stable")[:k]
    wq, wes = weighted_p10_and_es10(x, w)
    return {
        "mean_bps": float(np.mean(x)),
        "notional_weighted_mean_bps": float(np.average(x, weights=w)),
        "p10_bps": float(np.quantile(x, 0.1)),
        "worst10_mean_bps": float(np.mean(lower)),
        "notional_weighted_p10_bps": wq,
        "notional_weighted_worst10_mean_bps": wes,
        "negative_fraction": float(np.mean(x < 0)),
        "notional_weighted_negative_fraction": float(np.sum(w[x < 0])/np.sum(w)),
        "count": n,
        "notional_usdt": float(np.sum(w)),
    }


def asof_quotes(times, prices, queries, inclusive, last_valid_ns):
    idx = np.searchsorted(times, queries, side="right" if inclusive else "left") - 1
    valid = (idx >= 0) & (queries < last_valid_ns)
    safe = np.maximum(idx, 0)
    mid = np.where(valid, prices[safe], np.nan)
    age = np.where(valid, queries - times[safe], -1)
    return mid, age, valid


def label_fills(fill, ts, mid, day_end_ns, ctval):
    required = {"exchange_timestamp_ns","local_observed_ns","side","exec_price","exec_qty"}
    if set(fill) != required:
        raise ValueError(f"Official fill fields mismatch: {set(fill)}")
    t = np.asarray(fill["local_observed_ns"], dtype=np.int64)
    ex = np.asarray(fill["exchange_timestamp_ns"], dtype=np.int64)
    side = np.asarray(fill["side"], dtype=np.int64)
    price = np.asarray(fill["exec_price"], dtype=float)
    qty = np.asarray(fill["exec_qty"], dtype=float)
    if (np.any(ex > t) or np.any(t[1:] < t[:-1])
            or np.any((side != 1) & (side != -1))
            or np.any(qty <= 0) or np.any(price <= 0)
            or not (np.isfinite(qty).all() and np.isfinite(price).all())):
        raise ValueError("Invalid or unsorted official fills")
    px0, age0, valid0 = asof_quotes(ts, mid, t, False, day_end_ns)
    notional = price * qty * ctval
    capture = side * (px0-price)/px0 * 10000
    result = dict(
        exchange_timestamp_ns=ex, local_observed_ns=t,
        side=side, exec_price=price, exec_qty=qty,
        fill_notional_usdt=notional,
        gate_quote_mid_at_observation=px0,
        quote_age_at_observation_ms=age0/1e6,
        capture_halfspread_bps=capture,
    )
    flags = {}
    for h in HORIZONS:
        future = t + h * 1_000_000
        mh, ageh, validh = asof_quotes(ts, mid, future, True, day_end_ns)
        asof = valid0 & validh
        fresh = asof & (age0 <= FRESH_NS) & (ageh <= FRESH_NS)
        markout = side * (mh-px0)/px0 * 10000
        result[f"markout_{h}ms_bps"] = markout
        result[f"quote_age_{h}ms_ms"] = ageh/1e6
        result[f"asof_valid_{h}ms"] = asof
        result[f"fresh_{h}ms"] = fresh
        flags[h] = (asof, fresh)
    return result, flags


def model_day(model, fill, trace, ts, mid, day_end_ns, ctval):
    annotated, flags = label_fills(fill, ts, mid, day_end_ns, ctval)
    n = len(annotated["side"])
    if n != int(trace["trading"]["engine_fills"]):
        raise ValueError(f"Official HBT count mismatch: {model}")
    if n and not (annotated["local_observed_ns"][0] >= day_end_ns-86_000_000_000_000
                  and annotated["local_observed_ns"][-1] < day_end_ns):
        raise ValueError(f"Unexpected fill window: {model}")
    groups = {}
    for direction in GROUPS:
        mask = (np.ones(n, bool) if direction == "all" else
                annotated["side"] == (1 if direction == "buy" else -1))
        count = int(mask.sum())
        w = annotated["fill_notional_usdt"]
        capture_ok = mask & np.isfinite(annotated["capture_halfspread_bps"])
        capture = stats(annotated["capture_halfspread_bps"][capture_ok], w[capture_ok])
        horizons = {}
        for h in HORIZONS:
            asof, fresh = flags[h]
            valid = mask & fresh & np.isfinite(annotated[f"markout_{h}ms_bps"])
            x = annotated[f"markout_{h}ms_bps"][valid]
            weights = w[valid]
            horizons[str(h)] = {
                "all_fills": count,
                "asof_count": int(np.count_nonzero(mask & asof)),
                "fresh_count": int(np.count_nonzero(valid)),
                "fresh_fraction": float(np.count_nonzero(valid)/count) if count else None,
                "fresh": stats(x, weights),
                "capture_on_same_fresh_fills": stats(
                    annotated["capture_halfspread_bps"][valid], weights),
            }
        groups[direction] = {
            "fills": count, "capture": capture,
            "horizons": horizons
        }
    return annotated, {
        "model": model,
        "hbt_fills": n,
        "buy_fills": int(np.sum(annotated["side"] == 1)),
        "sell_fills": int(np.sum(annotated["side"] == -1)),
        "total_fill_notional_usdt": float(np.sum(annotated["fill_notional_usdt"])),
        "observed_minus_exchange_ms": {
            "min": float(np.min((annotated["local_observed_ns"]-annotated["exchange_timestamp_ns"])/1e6)) if n else None,
            "max": float(np.max((annotated["local_observed_ns"]-annotated["exchange_timestamp_ns"])/1e6)) if n else None,
        },
        "groups": groups,
    }


def read_archive_case(root, day, model):
    loc = root / "recorder" / day / model / "fills.npz"
    with np.load(loc, allow_pickle=False) as d:
        return {k: d[k].copy() for k in d.files}


def one_day(day, case_dir, out_dir, quotes_url=None):
    report = json.loads((case_dir / f"{day}_1430m.json").read_text())
    if (report.get("day") != day or report.get("status") != "complete"
        or report.get("minutes_traded") != 1430
        or report.get("experimental_trigger_sigma_multiple") != 1.645
        or report.get("binance_extra_visible_delay_ms") != 2.5
        or abs(report["scenario"]["maker_fee_rate"]-0.0002) > 1e-12):
        raise ValueError(f"Wrong HBT simulation scenario: {day}")
    ts, mid, data_qa = read_quotes(day, url=quotes_url)
    end_ns = int((datetime.strptime(day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
                  + timedelta(days=1)).timestamp()*1e9)
    out_dir.mkdir(parents=True, exist_ok=True)
    by_model = {}
    for model in MODELS:
        fill = read_archive_case(case_dir,day,model)
        annotated, summary = model_day(
            model, fill, report["variants"][model], ts, mid, end_ns,
            float(report["scenario"]["ctval"]))
        by_model[model] = summary
        np.savez_compressed(out_dir / f"{day}_{model}_annotated.npz", **annotated)
    output = {
        "day": day, "status": "complete",
        "definition": "signed(buy=+1,sell=-1)*(future Gate mid - entry Gate mid)/entry Gate mid*10000",
        "start": "HBT local fill observation; Gate local_timestamp quote strictly earlier",
        "future": "last Gate local_timestamp quote at or before local observation + horizon",
        "freshness_primary_max_age_ms": 500,
        "source_quote_qa": data_qa,
        "models": by_model,
    }
    (out_dir/f"{day}_quality.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2)+"\n",encoding="utf-8")
    print("GATE_STAGE2_DAY_OK", day, data_qa["raw_rows"],
          {m: by_model[m]["hbt_fills"] for m in MODELS},flush=True)


def aggregate(root, out_dir):
    reports = []
    for day in DAYS:
        paths = list(root.rglob(day+"_quality.json"))
        if len(paths)!=1:
            raise ValueError(f"Expected exactly one stage2 result for {day}: {len(paths)}")
        r = json.loads(paths[0].read_text())
        if r["status"] != "complete" or r["day"] != day or set(r["models"]) != set(MODELS):
            raise ValueError(f"Incomplete stage2 day: {day}")
        reports.append(r)
    rows = []
    for r in reports:
        for model in MODELS:
            for direction, group in r["models"][model]["groups"].items():
                for horizon, data in group["horizons"].items():
                    s = data["fresh"]
                    rows.append({
                        "date":r["day"],"model":model,"side":direction,"horizon_ms":int(horizon),
                        "all_fills":data["all_fills"],"fresh_fills":data["fresh_count"],
                        "coverage":data["fresh_fraction"],
                        "capture_bps":data["capture_on_same_fresh_fills"]["mean_bps"] if data["capture_on_same_fresh_fills"] else None,
                        "mean_markout_bps":s["mean_bps"] if s else None,
                        "notional_weighted_mean_bps":s["notional_weighted_mean_bps"] if s else None,
                        "p10_bps":s["p10_bps"] if s else None,
                        "worst10_mean_bps":s["worst10_mean_bps"] if s else None,
                        "weighted_p10_bps":s["notional_weighted_p10_bps"] if s else None,
                        "weighted_worst10_mean_bps":s["notional_weighted_worst10_mean_bps"] if s else None,
                        "negative_fraction":s["negative_fraction"] if s else None,
                        "notional_weighted_negative_fraction":s["notional_weighted_negative_fraction"] if s else None,
                    })
    out_dir.mkdir(parents=True,exist_ok=True)
    with (out_dir/"per_day_quality.csv").open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader();w.writerows(rows)
    metrics = ("coverage","capture_bps","mean_markout_bps",
               "notional_weighted_mean_bps","p10_bps","worst10_mean_bps",
               "weighted_p10_bps","weighted_worst10_mean_bps",
               "negative_fraction","notional_weighted_negative_fraction")
    summary = {}
    for model in MODELS:
        summary[model]={}
        for direction in GROUPS:
            summary[model][direction]={}
            for h in HORIZONS:
                group = [x for x in rows if x["model"]==model and x["side"]==direction
                         and x["horizon_ms"]==h]
                if len(group)!=6:
                    raise ValueError(f"Missing six independent daily observations {model} {h}")
                summary[model][direction][str(h)] = {
                    "days":6, "valid_days":sum(x["mean_markout_bps"] is not None for x in group),
                    "total_fills":sum(x["all_fills"] for x in group),
                    "total_fresh_fills":sum(x["fresh_fills"] for x in group),
                    **{"equal_day_mean_"+key: (float(np.mean([x[key] for x in group if x[key] is not None]))
                        if any(x[key] is not None for x in group) else None)
                       for key in metrics},
                }
    result={"status":"complete","six_exploratory_dates":DAYS,
            "primary":"each day equal-weighted, quote freshness<=500ms both endpoints; spread capture on same eligible fill set",
            "note":"Descriptive fill conditional distributions, not causal effect or PnL attribution",
            "results":summary}
    (out_dir/"summary.json").write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n")
    lines=["# Gate BTC 1.645σ 双边β1：阶段②成交质量（六日探索性）","",
           "基于官方HBT实际成交回报时刻及Tardis Gate原生quotes行情。主结果要求起点/终点盘口年龄均≤500ms；捕获半价差与Markout使用同一批符合条件的成交，否则排除并报告覆盖率。未修改撮合/策略/阈值。", "",
           "| 模型 | 方向 | 期限 | 每日有效笔数均值 | 覆盖率 | 捕获半价差(bps) | Markout(bps) | P10 | 最差10%均值 | 负向占比 |",
           "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for model in MODELS:
        for direction in GROUPS:
            for h in HORIZONS:
                z=summary[model][direction][str(h)]
                f=lambda key,dec=3: ("NA" if z["equal_day_mean_"+key] is None else
                                     f'{z["equal_day_mean_"+key]:.{dec}f}')
                lines.append(f'| {model} | {direction} | {h}ms | {z["total_fresh_fills"]/6:.1f} | '
                             f'{f("coverage",3)} | {f("capture_bps")} | '
                             f'{f("mean_markout_bps")} | {f("p10_bps")} | '
                             f'{f("worst10_mean_bps")} | {f("negative_fraction",3)} |')
    lines.extend(["","负向Markout表示买后下跌或卖后上涨。报告另有逐日CSV及名义额加权均值、P10、最差10%均值和负向比例；0变化计入非负。P10按逐日10%分位点计算，最差10%均值按精确排序后的最差ceil(0.1N)笔计算；名义额加权尾部使用10%权重质量。所有指标仅是成交后价格变化，不与收益分解中的库存盈亏相加。","",
                  "跨所2.5ms为假设，资金费率、主动期末平仓成本未模拟；日期已用于研发探索，不能声称样本外显著性。",""])
    (out_dir/"report.md").write_text("\n".join(lines),encoding="utf-8")
    print("GATE_STAGE2_AGGREGATE_OK",flush=True)


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--mode",choices=("day","aggregate"),required=True)
    p.add_argument("--day")
    p.add_argument("--case-dir")
    p.add_argument("--quotes-url",help="Optional testing/local gz URL")
    p.add_argument("--input")
    p.add_argument("--out",required=True)
    args=p.parse_args()
    if args.mode=="day":
        if not (args.day in DAYS and args.case_dir):
            p.error("day mode requires valid --day and --case-dir")
        one_day(args.day,Path(args.case_dir),Path(args.out),args.quotes_url)
    else:
        if not args.input:
            p.error("aggregate mode requires --input")
        aggregate(Path(args.input),Path(args.out))
