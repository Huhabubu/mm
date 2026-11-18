"""Read-only 1.645-sigma Gate BTC official-HBT fill quality / native quote markout.

Never changes strategy, order matching, fee, or calibration.
Quote clock: Tardis Gate quotes LOCAL receipt timestamp, in microseconds.
Fill clock: HBT local observation timestamp, in nanoseconds.
For the entry quote require quote_time < fill_observed_ns (strict).
For future horizons require quote_time <= fill_observed_ns + horizon_ns.
Legacy capture/drift use both endpoints <=500ms old. Primary total markout
uses a fresh future quote; old labels remain available.
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
PRIMARY_HORIZONS = (500, 1000)
QUOTE_AGE_BUCKETS = ("lt_100ms", "100_to_250ms")
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


def quote_age_bucket_mask(age_ms, bucket):
    """Age of visible entry quote at the instant of local fill observation."""
    if bucket == "lt_100ms":
        return (age_ms >= 0) & (age_ms < 100)
    if bucket == "100_to_250ms":
        return (age_ms >= 100) & (age_ms <= 250)
    raise ValueError(f"Unknown quote-age bucket: {bucket}")


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
        markout = side * (mh-px0)/px0 * 10000  # legacy entry-mid drift
        total_markout = side * (mh-price)/price * 10000
        total_fresh = validh & (ageh >= 0) & (ageh <= FRESH_NS)
        future_quote_updated = validh & ((future - ageh) > t)
        result[f"markout_{h}ms_bps"] = markout
        result[f"total_markout_{h}ms_bps"] = total_markout
        result[f"total_fresh_{h}ms"] = total_fresh
        result[f"future_quote_updated_{h}ms"] = future_quote_updated
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
            total_eligible = (mask & annotated[f"total_fresh_{h}ms"]
                              & np.isfinite(annotated[f"total_markout_{h}ms_bps"]))
            updated = total_eligible & annotated[f"future_quote_updated_{h}ms"]
            age = annotated["quote_age_at_observation_ms"]
            strata = {}
            for bucket in QUOTE_AGE_BUCKETS:
                subset = total_eligible & quote_age_bucket_mask(age, bucket)
                strata[bucket] = {
                    "fills": int(subset.sum()),
                    "total_markout": stats(
                        annotated[f"total_markout_{h}ms_bps"][subset], w[subset]),
                }
            horizons[str(h)] = {
                "total_fresh_count": int(total_eligible.sum()),
                "total_fresh_fraction": float(total_eligible.sum()/count) if count else None,
                "total_markout": stats(
                    annotated[f"total_markout_{h}ms_bps"][total_eligible], w[total_eligible]),
                "future_quote_updated_count": int(updated.sum()),
                "total_markout_after_quote_update": stats(
                    annotated[f"total_markout_{h}ms_bps"][updated], w[updated]),
                "quote_age_strata": strata,
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


def depth_visibility_diagnostic(day, model, witness_dir, fill):
    """Order-placement queue-depth diagnostic, only with independently recorded L2.

    Frozen Stage2 fills + native best-bid/best-ask quotes do NOT include order
    submissions or the 20th/400th level. Never infer unobservable depth from them.
    """
    source = ("20_level_snapshots" if day < "2026-11-01"
              else "400_level_initial_plus_native_deltas")
    if witness_dir is None:
        return {
            "status": "unavailable_missing_order_placement_l2_witness",
            "depth_source": source,
            "orders_outside_visible_fraction": None,
            "fills_outside_visible_fraction": None,
        }
    path = Path(witness_dir) / f"{day}_{model}_depth_witness.npz"
    if not path.exists():
        raise FileNotFoundError(f"Missing requested depth witness: {path}")
    fields = {
        "order_side", "order_price_tick", "order_last_visible_tick",
        "order_submit_local_ns", "order_book_local_ns",
        "fill_order_side", "fill_order_price_tick", "fill_order_last_visible_tick",
        "fill_order_submit_local_ns", "fill_order_book_local_ns",
        "fill_exchange_timestamp_ns",
    }
    with np.load(path, allow_pickle=False) as data:
        if not fields.issubset(data.files):
            raise ValueError(f"Missing depth witness arrays: {fields-set(data.files)}")
        w = {key: data[key] for key in fields}
    for prefix in ("order", "fill_order"):
        n = len(w[prefix+"_side"])
        names = (prefix+"_side", prefix+"_price_tick", prefix+"_last_visible_tick",
                 prefix+"_submit_local_ns", prefix+"_book_local_ns")
        if any(len(w[name]) != n for name in names):
            raise ValueError(f"Misaligned {prefix} depth witness arrays")
        if np.any((w[prefix+"_side"] != 1) & (w[prefix+"_side"] != -1)):
            raise ValueError("Invalid depth witness side")
        if (not np.isfinite(w[prefix+"_price_tick"]).all()
                or not np.isfinite(w[prefix+"_last_visible_tick"]).all()
                or np.any(w[prefix+"_book_local_ns"] > w[prefix+"_submit_local_ns"])):
            raise ValueError("Nonfinite or future-information L2 witness")
    if (len(w["fill_order_side"]) != len(fill["side"])
            or len(w["fill_exchange_timestamp_ns"]) != len(fill["side"])
            or not np.array_equal(w["fill_order_side"], fill["side"])
            or not np.array_equal(w["fill_exchange_timestamp_ns"], fill["exchange_timestamp_ns"])
            or np.any(w["fill_order_submit_local_ns"] > fill["local_observed_ns"])):
        raise ValueError("Depth witness differs from frozen official fill order/clock")
    def beyond(prefix):
        side = w[prefix+"_side"]
        price = w[prefix+"_price_tick"]
        edge = w[prefix+"_last_visible_tick"]
        return np.where(side == 1, price < edge, price > edge)
    a, b = beyond("order"), beyond("fill_order")
    return {
        "status": "complete",
        "reference": "same-side last visible level at original order submission time",
        "depth_source": source,
        "orders": len(a), "fills": len(b),
        "orders_outside_visible_count": int(a.sum()),
        "fills_outside_visible_count": int(b.sum()),
        "orders_outside_visible_fraction": float(a.mean()) if len(a) else None,
        "fills_outside_visible_fraction": float(b.mean()) if len(b) else None,
    }


def read_archive_case(root, day, model):
    loc = root / "recorder" / day / model / "fills.npz"
    with np.load(loc, allow_pickle=False) as d:
        return {k: d[k].copy() for k in d.files}


def one_day(day, case_dir, out_dir, quotes_url=None, depth_witness_dir=None):
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
        summary["depth_visibility"] = depth_visibility_diagnostic(day, model, depth_witness_dir, fill)
        by_model[model] = summary
        np.savez_compressed(out_dir / f"{day}_{model}_annotated.npz", **annotated)
    output = {
        "day": day, "status": "complete",
        "definition": "signed(buy=+1,sell=-1)*(future Gate mid - official HBT execution price)/execution price*10000",
        "legacy_definition": "signed(buy=+1,sell=-1)*(future Gate mid - entry Gate mid)/entry Gate mid*10000",
        "core_horizons_ms": PRIMARY_HORIZONS,
        "total_markout_primary_future_max_age_ms": 500,
        "start": "HBT local fill observation; Gate local_timestamp quote strictly earlier",
        "future": "last Gate local_timestamp quote at or before local observation + horizon",
        "freshness_legacy_both_endpoints_max_age_ms": 500,
        "source_quote_qa": data_qa,
        "depth_witness_required_for_actual_queue_position": True,
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
                         "total_fresh_fills":data["total_fresh_count"],
                         "total_coverage":data["total_fresh_fraction"],
                         "total_markout_bps":(data["total_markout"]["mean_bps"]
                             if data["total_markout"] else None),
                         "total_markout_updated_bps":(
                             data["total_markout_after_quote_update"]["mean_bps"]
                             if data["total_markout_after_quote_update"] else None),
                         "quote_age_lt100_fills":data["quote_age_strata"]["lt_100ms"]["fills"],
                         "quote_age_lt100_total_bps":(
                             data["quote_age_strata"]["lt_100ms"]["total_markout"]["mean_bps"]
                             if data["quote_age_strata"]["lt_100ms"]["total_markout"] else None),
                         "quote_age_100to250_fills":data["quote_age_strata"]["100_to_250ms"]["fills"],
                         "quote_age_100to250_total_bps":(
                             data["quote_age_strata"]["100_to_250ms"]["total_markout"]["mean_bps"]
                             if data["quote_age_strata"]["100_to_250ms"]["total_markout"] else None),
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
    metrics = ("total_coverage","total_markout_bps","total_markout_updated_bps",
               "quote_age_lt100_total_bps","quote_age_100to250_total_bps",
               "coverage","capture_bps","mean_markout_bps",
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
                    "days":6, "valid_days":sum(x["total_markout_bps"] is not None for x in group),
                    "total_fills":sum(x["all_fills"] for x in group),
                    "total_fresh_fills":sum(x["fresh_fills"] for x in group),
                     "total_future_fresh_fills":sum(x["total_fresh_fills"] for x in group),
                     "quote_age_lt100_fills":sum(x["quote_age_lt100_fills"] for x in group),
                     "quote_age_100to250_fills":sum(x["quote_age_100to250_fills"] for x in group),
                    **{"equal_day_mean_"+key: (float(np.mean([x[key] for x in group if x[key] is not None]))
                        if any(x[key] is not None for x in group) else None)
                       for key in metrics},
                }
    result={"status":"complete","six_exploratory_dates":DAYS,
            "primary":"execution-price total markout; equal-weighted by day; 500/1000ms core; future quote age<=500ms",
             "legacy":"entry-mid capture and markout, both endpoint quotes age<=500ms",
            "note":"Descriptive fill conditional distributions, not causal effect or PnL attribution",
            "results":summary}
    (out_dir/"summary.json").write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n")
    lines=["# Gate BTC 1.645σ 双边β1：阶段②成交质量（六日探索性）","",
           "主口径是实际成交价到未来Gate中间价的Total Markout。要求未来报价年龄≤500ms，核心500/1000ms；报告入场时报价年龄分层及未来至少发生一次报价刷新的子样本。旧Capture与旧Markout另存供审计。未修改撮合、策略及手续费。", "",
           "| 模型 | 方向 | 期限 | 有效笔数/日 | 主覆盖率 | Total Markout(bps) | 报价更新后(bps) | <100ms报价(bps) | 100–250ms报价(bps) | 旧Capture(bps) | 旧Markout(bps) |",
           "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for model in MODELS:
        for direction in GROUPS:
            for h in HORIZONS:
                z=summary[model][direction][str(h)]
                f=lambda key,dec=3: ("NA" if z["equal_day_mean_"+key] is None else
                                     f'{z["equal_day_mean_"+key]:.{dec}f}')
                lines.append(f'| {model} | {direction} | {h}ms | {z["total_future_fresh_fills"]/6:.1f} | '
                             f'{f("total_coverage",3)} | {f("total_markout_bps")} | '
                             f'{f("total_markout_updated_bps")} | {f("quote_age_lt100_total_bps")} | '
                             f'{f("quote_age_100to250_total_bps")} | {f("capture_bps")} | '
                             f'{f("mean_markout_bps")} |')
    lines.extend(["","总Markout以成交价为零点，负值为成交后未来中间价相对成交价的不利估值；尚未扣Maker费，不是净收益。旧Capture和Markout保留用于旧结果复核；各口径有效成交集合可能不同，不能直接混加。逐日CSV仍包含历史的尾部风险指标。","",
                  "跨所2.5ms为假设，资金费率、主动期末平仓成本未模拟；日期已用于研发探索，不能声称样本外显著性。",""])
    (out_dir/"report.md").write_text("\n".join(lines),encoding="utf-8")
    print("GATE_STAGE2_AGGREGATE_OK",flush=True)


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--mode",choices=("day","aggregate"),required=True)
    p.add_argument("--day")
    p.add_argument("--case-dir")
    p.add_argument("--quotes-url",help="Optional testing/local gz URL")
    p.add_argument("--depth-witness-dir",help="Optional instrumented order + L2 witnesses; missing entries fail closed")
    p.add_argument("--input")
    p.add_argument("--out",required=True)
    args=p.parse_args()
    if args.mode=="day":
        if not (args.day in DAYS and args.case_dir):
            p.error("day mode requires valid --day and --case-dir")
        one_day(args.day,Path(args.case_dir),Path(args.out),args.quotes_url,
                Path(args.depth_witness_dir) if args.depth_witness_dir else None)
    else:
        if not args.input:
            p.error("aggregate mode requires --input")
        aggregate(Path(args.input),Path(args.out))
