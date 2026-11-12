"""Audit signed execution edge, inventory MTM, 100/500ms markout from saved HBT fills.

Input: archived official hftbacktest Recorder and fill arrays from the 2.5ms
multivenue run. Market midpoint: the matching venue's Tardis native
book_ticker, at *local* capture timestamp. No matching engine is written.

Accounting identity (with initial zero inventory and cash):
  gross = sum(sign*qty*ctval*(mid_at_fill_observation - fill_px))
        + integral(position_before_quote_update*ctval*d(native_mid))
        + final_position*ctval*(HBT_final_mid-native_final_mid)
The exchange's actual matching/fees are read only from HBT results.

The 'execution' edge uses the local fill recognition time, and may include
a little price movement after exchange fill; report that limitation.
"""
import argparse
import csv
import gzip
import io
import json
import math
import statistics
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

BASE = "https://datasets.tardis.dev/v1"
EXPECTED = ("glft", "glft_binance", "glft_local_peak", "glft_binance_local_peak")
HORIZONS_NS = (100_000_000, 500_000_000)
MAX_BBO_AGE_NS = 500_000_000  # prespecified quote-freshness diagnostic only
UA = {"User-Agent": "mm-hbt-recorder-markout-research/1.0"}


def native_bbo(exchange, symbol, day, end_ns):
    """Use the publicly available historical *target venue* native BBO.

    Limit to ~first 60min, preserving original row order; never use a quote
    after a fill observation as that observation's current midpoint.
    """
    yyyy, mm, dd = day.split("-")
    url = f"{BASE}/{exchange}/book_ticker/{yyyy}/{mm}/{dd}/{symbol}.csv.gz"
    times = []
    mids = []
    bad = 0
    prev = -1
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA),
                                timeout=120) as source:
        with io.TextIOWrapper(gzip.GzipFile(fileobj=source), encoding="utf-8",
                              newline="") as stream:
            reader = csv.DictReader(stream)
            required = {"local_timestamp", "bid_price", "ask_price", "exchange", "symbol"}
            if not required.issubset(reader.fieldnames or []):
                raise ValueError(f"Invalid Tardis BBO headers {url}")
            for row in reader:
                t = int(row["local_timestamp"]) * 1000
                if t > end_ns + 5_000_000_000:
                    break
                if t < prev:
                    raise ValueError(f"Native BBO local timestamps not increasing: {url}")
                prev = t
                if row["exchange"] != exchange or row["symbol"] != symbol:
                    raise ValueError(f"Wrong venue/symbol in {url}")
                bid, ask = float(row["bid_price"]), float(row["ask_price"])
                if not (math.isfinite(bid) and math.isfinite(ask) and 0 < bid < ask):
                    bad += 1
                    continue
                times.append(t)
                mids.append((bid + ask) / 2.0)
    if not times or bad:
        raise ValueError(f"Invalid native BBO: {url}, count={len(times)}, bad={bad}")
    ts = np.asarray(times, dtype=np.int64)
    px = np.asarray(mids, dtype=np.float64)
    return ts, px


def bbo_at(ts, px, times):
    ix = np.searchsorted(ts, times, side="right") - 1
    mid = np.full(times.shape, np.nan, np.float64)
    age = np.full(times.shape, -1, np.int64)
    valid = ix >= 0
    mid[valid] = px[ix[valid]]
    age[valid] = times[valid] - ts[ix[valid]]
    return mid, age


def mean_or_none(seq):
    return float(statistics.mean(seq)) if seq else None


def file_for(case_dir, path):
    relative = Path(path)
    if relative.parts and relative.parts[0] == "reports":
        relative = Path(*relative.parts[1:])
    out = case_dir / relative
    if not out.is_file():
        raise FileNotFoundError(f"Missing saved recorder/fill: {out}")
    return out


def compute_attribution(rec, fills, trader, ctval, bbo_ns, bbo_mid):
    """Conserve exact gross HBT P&L; calculate independently timed markouts."""
    if ctval <= 0:
        raise ValueError("Invalid contract value")
    if len(rec) < 29000:
        raise ValueError("Incomplete Recorder samples")
    if np.any(np.diff(rec["timestamp"]) <= 0):
        raise ValueError("Recorder timestamps must strictly increase")

    obs = fills["local_observed_ns"].astype(np.int64)
    exch = fills["exchange_timestamp_ns"].astype(np.int64)
    side = fills["side"].astype(np.int64)
    qty = fills["exec_qty"].astype(np.float64)
    fill_px = fills["exec_price"].astype(np.float64)
    n = len(obs)
    if n != trader["engine_fills"]:
        raise ValueError("Official fills missing")
    if not np.all((side == 1) | (side == -1)):
        raise ValueError("Official fill side is not +/-1")
    if (np.any(qty <= 0) or np.any(~np.isfinite(qty)) or
            np.any(fill_px <= 0) or np.any(~np.isfinite(fill_px))):
        raise ValueError("Bad official fill price/qty")

    # Sort by when the local HBT strategy first knows the official fill.
    order = np.argsort(obs, kind="stable")
    obs, exch, side, qty, fill_px = (
        obs[order], exch[order], side[order], qty[order], fill_px[order])
    first_sample, end = int(rec["timestamp"][0]), int(rec["timestamp"][-1])
    if len(obs) and (np.min(obs) < first_sample - 100_000_000 or
                     np.max(obs) > end + 1):
        raise ValueError("Fill falls outside the recorder time range")
    if len(obs) and np.any(obs < exch):
        raise ValueError("Local fill observation before recorded exchange event")
    latency = obs - exch

    # Explicit check: the saved fills must reproduce the official HBT ledger.
    signed_qty = side * qty
    pos_from_fills = float(np.sum(signed_qty))
    balance_from_fills = float(-np.sum(signed_qty * qty * 0 + signed_qty * fill_px * ctval))
    tol_bal = 3e-7 + abs(float(trader["balance_usdt"])) * 1e-10
    if abs(pos_from_fills - float(trader["end_position_contracts"])) > 2e-7:
        raise ValueError(f"Fill position doesn't reconcile: {pos_from_fills} vs {trader['end_position_contracts']}")
    if abs(balance_from_fills - float(trader["balance_usdt"])) > tol_bal:
        raise ValueError(f"Fill cash ledger doesn't reconcile: {balance_from_fills} vs {trader['balance_usdt']}")
    gross = float(trader["end_net_marked_equity_usdt"] + trader["fee_usdt"])
    if abs(float(trader["balance_usdt"]) +
           ctval * float(trader["end_position_contracts"]) *
           float(trader["end_mid_usdt_per_btc"]) - gross) > 1e-7:
        raise ValueError("Official HBT gross PnL identity failed")
    rpos, rbal, rfee = [float(rec[x][-1]) for x in ("position", "balance", "fee")]
    if abs(rbal-float(trader["balance_usdt"])) > tol_bal or abs(rpos-pos_from_fills) > 2e-7:
        raise ValueError("Final Recorder differs from official HBT ledger")
    if abs(rfee-float(trader["fee_usdt"])) > tol_bal:
        raise ValueError("Final Recorder fee differs from official HBT fee")
    end_mid = float(trader["end_mid_usdt_per_btc"])
    if abs(float(rec["price"][-1]) - end_mid) > 5e-7:
        raise ValueError("Final Recorder midpoint != official HBT midpoint")

    ix_end = int(np.searchsorted(bbo_ns, end, side="right"))
    if ix_end < 2:
        raise ValueError("No native BBO up to recorder end")
    ts = bbo_ns[:ix_end]
    mid = bbo_mid[:ix_end]
    mid_obs, age_obs = bbo_at(ts, mid, obs)
    if n and (np.any(~np.isfinite(mid_obs)) or np.any(age_obs < 0)):
        raise ValueError("No native BBO before one or more fill observations")

    notional = qty * ctval * mid_obs
    edge_each = signed_qty * ctval * (mid_obs - fill_px)
    edge = float(np.sum(edge_each))
    # First BBO entry starts with a zero strategy position, no opening holding PnL.
    quote_t = ts[1:]
    q_before_revision = np.r_[0., np.cumsum(signed_qty)][
        np.searchsorted(obs, quote_t, side="left")]
    holding_each = q_before_revision * ctval * np.diff(mid)
    holding = float(np.sum(holding_each))
    terminal_mid_correction = float(pos_from_fills * ctval * (end_mid - mid[-1]))
    attributed = edge + holding + terminal_mid_correction
    residual = gross - attributed
    if abs(residual) > 0.00015:
        raise ValueError(
            f"Attribution fails to conserve HBT gross: gross={gross}, "
            f"edge={edge}, holding={holding}, terminal={terminal_mid_correction}, residual={residual}")

    markouts = {}
    annotations = {
        "local_observed_ns":obs, "exchange_timestamp_ns":exch,
        "side":side, "exec_qty":qty, "exec_price":fill_px,
        "native_mid_at_observation":mid_obs, "native_bbo_age_ns":age_obs,
        "instant_exec_edge_usdt":edge_each
    }
    for horizon in HORIZONS_NS:
        future = obs + horizon
        valid_time = future <= end
        mid_future, age_future = bbo_at(ts, mid, np.minimum(future, end))
        good = valid_time & np.isfinite(mid_future) & np.isfinite(mid_obs)
        fresh = (good & (age_obs <= MAX_BBO_AGE_NS) &
                 (age_future <= MAX_BBO_AGE_NS))
        signed_price_move = side * (mid_future-mid_obs)
        dollar_move = signed_qty * ctval * (mid_future-mid_obs)
        bps = 1e4 * signed_price_move / mid_obs
        notional_good = float(np.sum(notional[good]))
        notional_fresh = float(np.sum(notional[fresh]))
        name = f"{horizon//1_000_000}ms"
        markouts[name] = {
            "all_valid_n":int(np.count_nonzero(good)),
            "fresh_n":int(np.count_nonzero(fresh)),
            "fresh_weighted_bps":(float(np.sum(dollar_move[fresh]) / notional_fresh * 1e4)
                                  if notional_fresh == 0 else None),
            "all_valid_weighted_bps":(float(np.sum(dollar_move[good]) / notional_good * 1e4)
                                       if notional_good else None),
            "fresh_negative_fraction":(float(np.mean(bps[fresh] < 0))
                                       if np.any(fresh) else None),
            "fresh_signed_dollar_move":float(np.sum(dollar_move[fresh])),
            "fresh_notional_usdt":notional_fresh,
            "all_valid_notional_usdt":notional_good,
            "fresh_mean_bps":(float(np.mean(bps[fresh])) if np.any(fresh) else None),
            "fresh_by_side_bps":{
                "buy":(float(np.mean(bps[fresh & (side == 1)]))
                       if np.any(fresh & (side == 1)) else None),
                "sell":(float(np.mean(bps[fresh & (side == -1)]))
                        if np.any(fresh & (side == -1)) else None)}
        }
        annotations[f"future_mid_{name}"] = mid_future
        annotations[f"future_age_ns_{name}"] = age_future
        annotations[f"signed_markout_bps_{name}"] = bps
        annotations[f"fresh_{name}"] = fresh

    result = {
        "fill_n":n,
        "gross_usdt":gross,
        "fees_usdt":float(trader["fee_usdt"]),
        "net_usdt":float(trader["end_net_marked_equity_usdt"]),
        "execution_edge_at_observation_usdt":edge,
        "inventory_native_bbo_mark_usdt":holding,
        "terminal_hbt_bbo_mid_adjust_usdt":terminal_mid_correction,
        "reconciliation_residual_usdt":residual,
        "notional_filled_usdt":float(np.sum(notional)),
        "execution_edge_bps_of_notional":float(edge/np.sum(notional)*1e4) if n else None,
        "fee_bps_of_notional":float(trader["fee_usdt"]/np.sum(notional)*1e4) if n else None,
        "native_bbo_stale_gt_500ms_at_observation":int(np.count_nonzero(age_obs > MAX_BBO_AGE_NS)),
        "native_bbo_age_p50_ms":(float(np.median(age_obs)/1e6) if n else None),
        "local_observation_minus_exchange_time_p50_ms":(
            float(np.median(latency)/1e6) if n else None),
        "markout":markouts,
        "end_position_units":pos_from_fills,
        "end_market_mid_usdt_per_coin":end_mid,
        "end_native_bbo_mid_usdt_per_coin":float(mid[-1]),
    }
    return result, annotations


def analyse_case(case_dir, output, fields):
    paths = sorted(case_dir.glob("*_50m.json"))
    if len(paths) != 1:
        raise ValueError(f"Expected one case JSON in {case_dir}: {paths}")
    r = json.loads(paths[0].read_text(encoding="utf-8"))
    spec = r["market"]
    venue = "okx" if spec["exchange"] == "okex-swap" else "bitget"
    coin, day, delay = spec["coin"], r["day"], float(r["extra_binance_delay_ms"])
    if delay != 2.5:
        raise ValueError(f"Unexpected delay {delay} in {case_dir}")
    case = f"{venue}_{coin}_{day}"
    if r["status"] != "complete":
        return {"case":case,"status":r["status"],"reason":r.get("calibration_diagnostic")}
    models = r["variants"]
    if set(models) != set(EXPECTED):
        raise ValueError(f"Wrong models in {case}")
    start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()*1e9)
    end = start + 3600*1_000_000_000
    ts, mids = native_bbo(spec["exchange"], spec["symbol"], day, end)
    summaries={}
    for model in EXPECTED:
        item = models[model]
        recorder_meta = item["recorder"]
        with np.load(file_for(case_dir,recorder_meta["file"])) as z:
            rec = z["0"].copy()
        with np.load(file_for(case_dir,recorder_meta["fill_file"])) as z:
            fills = {k:z[k].copy() for k in z.files}
        values, annotated = compute_attribution(
            rec, fills, item["trading"], float(spec["ctval"]), ts, mids)
        summaries[model] = values
        annot_path=output/"annotated"/case/(model+".npz")
        annot_path.parent.mkdir(parents=True,exist_ok=True)
        # Store per-fill quality metrics, not full source BBO or depth.
        np.savez_compressed(annot_path, **annotated)
        if values["fill_n"] and values["markout"]["100ms"]["fresh_n"] == 0:
            print("WARNING no fresh 100ms BBO after fills",case,model,flush=True)
    for model in EXPECTED:
        print("CASE_RESULT",case,model,
              "GROSS",summaries[model]["gross_usdt"],
              "EDGE",summaries[model]["execution_edge_at_observation_usdt"],
              "HOLD",summaries[model]["inventory_native_bbo_mark_usdt"],
              "FEE",summaries[model]["fees_usdt"],
              "M100",summaries[model]["markout"]["100ms"]["fresh_weighted_bps"],
              "M500",summaries[model]["markout"]["500ms"]["fresh_weighted_bps"],
              flush=True)
    return {"case":case,"status":"complete","market":venue,"coin":coin,
            "day":day,"delay_ms":delay, "models":summaries}


def aggregate(cases):
    result = {}
    complete = [x for x in cases if x["status"]=="complete"]
    missing = [x for x in cases if x["status"]!="complete"]
    rows=[]
    for venue in ("okx","bitget"):
        for coin in ("BTC","SOL"):
            matching=sorted((x for x in complete if x["market"]==venue and x["coin"]==coin),
                            key=lambda x:x["day"])
            for model in EXPECTED:
                r=[x["models"][model] for x in matching]
                if not r:continue
                def mean(k):
                    return float(statistics.mean(x[k] for x in r))
                def bps(h):
                    valid=[x["markout"][h]["fresh_weighted_bps"] for x in r
                           if x["markout"][h]["fresh_weighted_bps"] is not None]
                    return mean_or_none(valid)
                rows.append({"market":venue+" "+coin,"venue":venue,"coin":coin,
                             "model":model,"valid_days":len(r),
                             "gross_mean_usdt":mean("gross_usdt"),
                             "edge_mean_usdt":mean("execution_edge_at_observation_usdt"),
                             "hold_mean_usdt":mean("inventory_native_bbo_mark_usdt"),
                             "terminal_correction_mean_usdt":mean("terminal_hbt_bbo_mid_adjust_usdt"),
                             "reconciliation_mean_usdt":mean("reconciliation_residual_usdt"),
                             "fees_mean_usdt":mean("fees_usdt"),
                             "net_mean_usdt":mean("net_usdt"),
                             "filled_notional_mean_usdt":mean("notional_filled_usdt"),
                             "fill_count_mean":mean("fill_n"),
                             "edge_bps_of_filled_notional":(
                                1e4 * sum(x["execution_edge_at_observation_usdt"] for x in r)/
                                      sum(x["notional_filled_usdt"] for x in r)
                                if sum(x["notional_filled_usdt"] for x in r) else None),
                             "markout100_fresh_weighted_bps":bps("100ms"),
                             "markout500_fresh_weighted_bps":bps("500ms"),
                             "markout100_fresh_n":sum(x["markout"]["100ms"]["fresh_n"] for x in r),
                             "markout500_fresh_n":sum(x["markout"]["500ms"]["fresh_n"] for x in r),
                             "fill_total":sum(x["fill_n"] for x in r)})
    result={"summary_rows":rows, "cases":cases, "missing":missing,
            "count_cases":len(cases),"count_complete":len(complete),
            "method":"Official HBT fill ledger + target native BBO, local-observed fill clock; "
                     "midpoint mark changes, fees and exact HBT balance reconciliation."}
    return result


def render_md(results):
    z=["# 2.5ms四市场 GLFT 做市亏损：成交边际、库存持有与手续费审计",
       "",
       "**口径：每个有效日期50分钟交易的等权平均，金额USDT/50min。** "
       "各日期取2026年6—11月每月1日、前10min预热、25ms历史对照不混算。"
       "收集的官方HBT成交、Recorder及目标交易所原生Tardis BBO都在回测结束后进行只读复核。",
       "",
       "## 一、损益桥接（官方真实模拟结果，加总可核对）", "",
       "| 市场 | 策略 | 有效日 | 即时成交边际 | 库存价格变化 | 期末盘口口径校正 | 毛收益 | 手续费 | 净收益 |",
       "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for x in results["summary_rows"]:
        z.append(f"| {x['market']} | {x['model']} | {x['valid_days']} "
                 f"| {x['edge_mean_usdt']:+.4f} | {x['hold_mean_usdt']:+.4f} "
                 f"| {x['terminal_correction_mean_usdt']:+.4f} | {x['gross_mean_usdt']:+.4f} "
                 f"| {x['fees_mean_usdt']:.4f} | {x['net_mean_usdt']:+.4f} |")
    z+=["", "公式（标记 m 为目标市场原生BBO中间价）：",
        "逐笔成交的即时边际 = 买入数量×(m−成交价) + 卖出数量×(成交价−m)，再乘合约面值；"
        "库存损益 = 每次目标市场BBO更新前持仓×中间价变化×合约面值；"
        "期末校正 = 期末持仓×(官方HBT末价−原生BBO末价)×合约面值。",
        "**即时边际以成交被本地策略观测到的时刻定价，可能包含成交到回报期间的价格变化；"
        "库存项包含持仓期间的全部市场价格变化。** 这是恒等式归因，不代表单笔交易获利。"
        "每一场景均核对成交重建的现金/库存与官方HBT完全一致、毛收益误差低于0.00015USDT。",
        "",
        "## 二、成交后价格变化：100ms 与 500ms", "",
        "| 市场 | 策略 | 有效天数 | 成交总笔数 | 即时成交优势(bps) | 100ms方向性中间价变化(bps) | 500ms方向性中间价变化(bps) |",
        "|---|---|---:|---:|---:|---:|---:|"]
    for x in results["summary_rows"]:
        def fmt(v):return f"{v:+.3f}" if v is not None else "不可估计"
        z.append(f"| {x['market']} | {x['model']} | {x['valid_days']} "
                 f"| {x['fill_total']} | {fmt(x['edge_bps_of_filled_notional'])} "
                 f"| {fmt(x['markout100_fresh_weighted_bps'])} "
                 f"| {fmt(x['markout500_fresh_weighted_bps'])} |")
    z+=["", "口径：在每笔模拟成交**本地可见回报时刻**取目标交易所原生BBO中间价，"
        "再向后100ms和500ms取截止该时刻已经到达的原生BBO；买单乘+1，卖单乘−1，"
        "负值表示不利方向。仅保留成交点及未来点BBO采集年龄≤500ms的记录。"
        "每日期先按成交名义金额加权计算bps，再对有效日期等权平均。"
        "该markout是反向选择的观察性代理量，不能独立证明取消的订单原本会有负markout。",
        "",
        "## 三、特殊和统计限制","",
        "- 未能在预热10min内满足GLFT成交强度拟合要求的OKX SOL历史日期按无效案例保留，"
        "不能当作零收益纳入平均。",
        "- 即时成交边际、库存损益、手续费三项是会计恒等式。"
        "成交后100/500ms方向性价格变化是**另外的诊断指标，不能再次叠加到总收益上**。",
        "- 本地回报时钟的即时BBO，可能相对于交易所撮合时刻有响应延迟。"
        "独立存储了交易所撮合时钟与本地观察时钟，未来可审核二者差异；"
        "若真实Tardis跨所时间戳不一致，结果仍会受到采集时钟误差影响。",
        "- 50分钟样本来自已探索月首日，统计量不代表全天/样本外/实盘；手续费等级是情景设定，"
        "资金费率、期末实际平仓滑点、限频与成交排队乐观性仍需检查。",
        "- 原生Recorder NPZ和官方模拟成交NPZ保留在原始GitHub Action #43198245510；"
        "本审计仅新增已对齐的逐笔标注NPZ和聚合摘要，无新造撮合器。"]
    return "\n".join(z)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",default="gathered")
    p.add_argument("--out",default="reports/markout_attribution_2p5ms")
    a=p.parse_args()
    out=Path(a.out)
    out.mkdir(parents=True,exist_ok=True)
    cases=[]
    for folder in sorted(Path(a.source).glob("recorder-*-2p5ms-50min")):
        cases.append(analyse_case(folder,out,EXPECTED))
    if len(cases)!=24:
        raise RuntimeError(f"Expected 24 target market-day artifacts, found {len(cases)}")
    summary=aggregate(cases)
    if summary["count_complete"]!=20:
        raise RuntimeError(f"Unexpected fit count {summary['count_complete']}")
    (out/"summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    (out/"report.md").write_text(render_md(summary),encoding="utf-8")
    with (out/"by_market_model.csv").open("w",encoding="utf-8",newline="") as stream:
        writer=csv.DictWriter(stream,fieldnames=list(summary["summary_rows"][0]))
        writer.writeheader()
        writer.writerows(summary["summary_rows"])
    print("AUDIT_COMPLETE",summary["count_complete"],"OF",summary["count_cases"],flush=True)
    for x in summary["summary_rows"]:
        print("AGG",x["market"],x["model"],"DAYS",x["valid_days"],
              "GROSS",x["gross_mean_usdt"],"EDGE",x["edge_mean_usdt"],
              "HOLD",x["hold_mean_usdt"],"FEE",x["fees_mean_usdt"],
              "NET",x["net_mean_usdt"],"M100",x["markout100_fresh_weighted_bps"],
              "M500",x["markout500_fresh_weighted_bps"],flush=True)


if __name__ == "__main__":
    main()
