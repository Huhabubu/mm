"""Single predefined historical April 2026 Gate HBT holdout, no model refit/tuning."""
import argparse
import json
from pathlib import Path

def aggregate(src,out):
    p=json.loads(Path(src).read_text(encoding="utf-8"))
    if (p.get("status")!="complete" or p.get("holdout_date")!="2026-04-01"
        or p["selected_mode"]!=p["strategy_selection_lock"]["selected_mode"]
        or not p["first_time_reading_april_data_after_mode_locked"]
        or not p["historical_holdout_not_forward_out_of_sample"]):
        raise ValueError("April HBT outcome protocol mismatch")
    selected=p["selected_mode"]
    base=p["models"]["glft"]["trading"]
    both=p["models"]["both_1p0"]["trading"]
    pick=p["models"][selected]["trading"]
    def summary(name):
        x=p["models"][name]
        t=x["trading"]
        h=x["quality"]["groups"]["all"]["horizons"]["100"]
        return {
          "net_usdt":t["end_net_marked_equity_usdt"],
          "maker_fee_usdt":t["fee_usdt"],
          "fills":t["engine_fills"],
          "notional_usdt":t["trading_value_usdt"],
          "mean_abs_inventory_contracts":t["mean_abs_inventory_contracts"],
          "rms_inventory_contracts":t["rms_inventory_contracts"],
          "max_abs_inventory_contracts":t["max_abs_position_contracts"],
          "cancel_requests":t["cancel_requests"],
          "markout_100ms_bps":h["fresh"]["mean_bps"],
          "markout_100ms_worst10_bps":h["fresh"]["worst10_mean_bps"],
          "markout_100ms_fresh_quote_fraction":h["fresh_fraction"],
        }
    s={name:summary(name) for name in p["models"]}
    result={
      "status":"complete","date":"2026-04-01",
      "selected_from_june_nov_before_april_read":selected,
      "model_results":s,
      "selected_minus_glft_net_usdt":pick["end_net_marked_equity_usdt"]-base["end_net_marked_equity_usdt"],
      "selected_minus_bilateral_net_usdt":pick["end_net_marked_equity_usdt"]-both["end_net_marked_equity_usdt"],
      "one_historical_date_only":True,
      "chronologically_backward_selection_using_later_training":True,
      "claims":"untouched historical robustness only, not forward chronological sample",
      "limits":["April 2026 contract verified by current metadata not historic archival spec",
                "No funding, final forced unwind, actual cross-venue clock agreement"]
    }
    out=Path(out)
    out.mkdir(parents=True,exist_ok=True)
    (out/"summary.json").write_text(json.dumps(result,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    lines=["# 阶段⑤C：冻结模型2026-04-01独立历史日期回测","",
           f"在读取任何4月行情之前，使用6—11月探索性六日的预先固定选择规则选中 **{selected}**。",
           "4月早于5月训练及6—11月模型筛选日期，只构成此前未在该项目分析过的历史稳健性检验，**不可称严格前向样本外**。","",
           "| 官方HBT指标 | "+" | ".join(s)+" |",
           "|---|"+"|".join("---:" for _ in s)+"|"]
    for field,label in [
        ("net_usdt","Maker+0.02%含费净权益 USDT"),
        ("maker_fee_usdt","实际手续费 USDT"),
        ("fills","成交笔数"),
        ("notional_usdt","名义成交额 USDT"),
        ("mean_abs_inventory_contracts","平均绝对库存张数"),
        ("rms_inventory_contracts","库存RMS张数"),
        ("max_abs_inventory_contracts","最大绝对库存张数"),
        ("cancel_requests","撤单请求数"),
        ("markout_100ms_bps","100ms方向Markout bps"),
        ("markout_100ms_worst10_bps","100ms最差10% Markout bps"),
        ("markout_100ms_fresh_quote_fraction","100ms有效报价覆盖率")
    ]:
        lines.append("| "+label+" | "+" | ".join(f"{s[k][field]:,.4f}" for k in s)+" |")
    lines.extend(["",f"选择模式相对原GLFT含费收益差 **{result['selected_minus_glft_net_usdt']:+.4f} USDT**，相对双边β1 **{result['selected_minus_bilateral_net_usdt']:+.4f} USDT**。","",
         "该独立历史日期样本数量=1，不得报告显著性或外推普遍盈利。资金费率、期末主动清算、手续费实际等级及历史合约规则、跨所同步偏差仍有剩余风险。"])
    (out/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")
    print("STAGE5C_REPORT",json.dumps({k:v["net_usdt"] for k,v in s.items()}),flush=True)

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--input",required=True)
    p.add_argument("--out",required=True)
    a=p.parse_args()
    aggregate(a.input,a.out)
