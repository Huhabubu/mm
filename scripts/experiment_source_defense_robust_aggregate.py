"""Step3 six-date sequential 3-level GLFT admission using earlier-date priors only.

Input is compact sufficient HBT tape statistics (100ms crossing-distance counts),
never test-period fills. The original 5-level baseline is reported unchanged.
Training extension 20min moves hypothetical trade start to 00:20 UTC.
"""
import argparse
import json
import math
from pathlib import Path

from experiment_source_defense_robust_fit import research_fit
from g0_glft_rolling import glft_coefficients

COINS=("SOL","DOGE","XRP","1000PEPE")
VENUES=("okex-swap","bitget-futures")
DAYS=("2026-06-01","2026-07-01","2026-08-01",
      "2026-09-01","2026-10-01","2026-11-01")



def accepted_chronological_fits(rows,field,variant):
    """Only an already quote-valid earlier UTC day may supply the next prior."""
    history_k=None
    prior_date=None
    out={}
    for r in sorted(rows,key=lambda z:z["day"]):
        day=r["day"]
        fit=research_fit(r[field],prior_k=history_k)
        fit["day"]=day
        fit["variant"]=variant
        fit["prior_source_day"]=(prior_date if fit.get("causal_prior_used")
                                  else None)
        if fit["valid"]:
            q=r["order_notional_scenario"]
            half,skew=glft_coefficients(
                fit["A_per_s"],fit["k_per_tick"],
                fit["sigma_tick_per_sqrt_s"],
                q["gamma_code"],q["order_qty"])
            fit["half_spread_ticks"]=float(half)
            fit["inventory_skew_ticks_per_contract"]=float(skew)
            if not (math.isfinite(half) and math.isfinite(skew) and
                    0<half<=5000 and skew>0):
                fit["valid"]=False
                fit["reason"]="invalid_or_extreme_glft_quote_after_research_fit"
        if fit["valid"]:
            history_k=fit["k_per_tick"]
            prior_date=day
        out[day]=fit
    return out

def produce(cases,pepe_checks):
    indexed={}
    for r in cases:
        key=(r["coin"],r["venue"],r["day"])
        if key in indexed:raise ValueError("Duplicate coin/venue/day")
        indexed[key]=r
    outputs={"groups":{},"pepe_unit_checks":pepe_checks,
             "limits":["Live contract specs are not historical contract spec records",
                       "20min warmup shifts proposed start to UTC00:20",
                       "All month-first dates previously explored",
                       "Original>=5 estimator and 3-level prior-shrink research model must be distinguished",
                       "1000PEPE Binance quote price divided by 1000 before any lead-lag comparison",
                       "No orders, fills, PnL or source-response lead-lag inferred"]}
    lines=["# Step3 六日期：原GLFT vs 因果先验收缩 3档拟合（BTC之外四币种）",
           "", "**只读官方HBT前10/20min数据，不做任何策略成交/全量防御收益回测。**",
           "原始5档校准口径完全不变；研究3档要求严格更早日期可用的k先验、K范围[0.01,5]/tick、"
           "≥10个独立100ms穿越时间窗、至少一半有效波动率样本，以及可计算GLFT半价差。",
           "20min方案实际必须延后开仓到UTC00:20，所以与原00:10–01:00交易窗不直接可比。",
           "", "|币种|市场|完整资料日|原5档10min|研究3档10min因果prior|研究3档20min因果prior|50min最小原生成交数|",
           "|---|---|---:|---:|---:|---:|---:|"]
    for coin in COINS:
      for venue in VENUES:
        group=f"{coin}_{venue}"
        subset=[indexed.get((coin,venue,d)) for d in DAYS]
        valid_rows=[r for r in subset if r is not None and r["status"]=="stats_complete"]
        fits10=accepted_chronological_fits(valid_rows,"sufficient_stats_10min","10min")
        fits20=accepted_chronological_fits(valid_rows,"sufficient_stats_20min","20min")
        per_day=[]
        for day,r in zip(DAYS,subset):
            if r is None or r["status"]!="stats_complete":
                per_day.append({"day":day,"status":"missing_or_failed",
                                "reason":r.get("error") if r else "report_missing",
                                "original_valid":False,
                                "robust_10min_valid":False,
                                "robust_20min_valid":False})
                continue
            o=r["original_10min_fit"]
            t=fits10[day]
            v=fits20[day]
            per_day.append({"day":day,"status":"stats_complete",
                            "original_valid":bool(o["valid"]),
                            "original_k":o["k"],"n_levels10":o["positive_levels"],
                            "n_levels20":r["sufficient_stats_20min"]["positive_distance_levels"],
                            "crossing_intervals10":o["crossing_intervals"],
                            "robust_10min_valid":bool(t["valid"]),
                            "robust_20min_valid":bool(v["valid"]),
                            "robust_10min_fit":t,"robust_20min_fit":v,
                            "trades_50min":r["trade_activity_original_00_10_to_01_00"].get("trades_50min"),
                            "contract_source_current_only":True})
        base=sum(x["original_valid"] for x in per_day)
        robust10=sum(x["robust_10min_valid"] for x in per_day)
        robust20=sum(x["robust_20min_valid"] for x in per_day)
        counts=[x.get("trades_50min") for x in per_day]
        counts=[x for x in counts if x is not None]
        result={"full_stats_days":len(valid_rows),"original_10min_valid_days":base,
                "research_10min_causal_prior_valid_days":robust10,
                "research_20min_causal_prior_valid_days":robust20,
                "minimum_50min_target_trade_rows":min(counts) if counts else None,
                "date_results":per_day,
                "six_of_six_original":base==6,
                "six_of_six_robust10":robust10==6,
                "six_of_six_robust20":robust20==6,
                "historical_spec_verified":False,
                "binance_source_100ms_alpha_verified":False}
        outputs["groups"][group]=result
        lines.append(f"|{coin}|{venue}|{len(valid_rows)}/6|{base}/6|"
                     f"{robust10}/6|{robust20}/6|"
                     f"{min(counts) if counts else 'N/A'}|")
    lines+=["","## 解释限制","",
            "- 本轮成功的6/6（若有）仅表示在规定的历史预热段及参数边界内可以计算候选A/k/sigma，"
            "并不证明这六天真的都能满足正式做市策略的交易可行性。",
            "- 相同币种之前有效日期提供的k先验经过严格日历排序；任何失败日无合格更早先验都会继续失败。",
            "- 3档参数统计来自累积成交距离阈值，阈值间有依赖性；收缩可稳定数值但不能构造新的独立成交。",
            "- 每日原生BBO历史价格精度与当前公开合约规格一致只能作为兼容性检查，历史费率/合约元数据尚未验证。",
            "- 1000PEPE价格必须将Binance原生1000PEPEUSDT报价除以1000、数量乘以1000后比较，"
            "但当前检查不涵盖实际主动成交方向。",
            "- 研究策略单日验收未通过所有红线，因此此处没有运行六日防御收益回测。"]
    return outputs,"\n".join(lines)+"\n"


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--source",required=True)
    p.add_argument("--pepe-units",required=True)
    p.add_argument("--out",required=True)
    args=p.parse_args()
    files=sorted(Path(args.source).rglob("*.json"))
    cases=[json.loads(x.read_text(encoding="utf8")) for x in files]
    if len(cases)!=len(COINS)*len(VENUES)*len(DAYS):
        raise ValueError(f"Expected 48 dated fit reports, found {len(cases)}")
    unit_files=sorted(Path(args.pepe_units).rglob("*.json"))
    checks=[json.loads(x.read_text(encoding="utf8")) for x in unit_files]
    if len(checks)!=2 or not all(x.get("normalization_scale_check_pass_lt_5pct") for x in checks):
        raise ValueError("1000PEPE source price scaling not validated on both venues")
    data,md=produce(cases,checks)
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    (out/"step3_fit_admission.json").write_text(
        json.dumps(data,indent=2,ensure_ascii=False),encoding="utf8")
    (out/"step3_fit_admission.md").write_text(md,encoding="utf8")
    print(md)


if __name__=="__main__":
    main()
