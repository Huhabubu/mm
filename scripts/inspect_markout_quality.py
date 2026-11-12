"""Read-only quality diagnostics from already verified 2.5ms attribution JSON."""
import argparse
import json
from collections import defaultdict
from pathlib import Path


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--input",required=True)
    parser.add_argument("--out",required=True)
    a=parser.parse_args()
    data=json.loads(Path(a.input).read_text(encoding="utf-8"))
    groups=defaultdict(list)
    for case in data["cases"]:
        if case["status"]!="complete":continue
        for model,d in case["models"].items():
            groups[(case["market"],case["coin"],model)].append((case["day"],d))

    lines=["# 2.5ms 做市成交质量核验：报价新鲜度、负向占比与成交强度","",""]
    lines+=["| 市场 | 模型 | 日数 | 总成交 | 100ms有效数 | 100ms负向占比 | 500ms有效数 | 500ms负向占比 | 成交时BBO年龄中位数(ms) | 成交可见回报−撮合时间中位数(ms) |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for (venue,coin,model),group in sorted(groups.items()):
        total_n=sum(x["fill_n"] for day,x in group)
        out={}
        for h in ("100ms","500ms"):
            n=sum(x["markout"][h]["fresh_n"] for day,x in group)
            neg=sum(x["markout"][h]["fresh_n"]*x["markout"][h]["fresh_negative_fraction"]
                    for day,x in group if x["markout"][h]["fresh_negative_fraction"] is not None)
            out[h]=(n,neg/n if n else None)
        quote_age=sum(x["fill_n"]*x["native_bbo_age_p50_ms"] for day,x in group
                      if x["native_bbo_age_p50_ms"] is not None)/total_n if total_n else None
        latency=sum(x["fill_n"]*x["local_observation_minus_exchange_time_p50_ms"]
                    for day,x in group
                    if x["local_observation_minus_exchange_time_p50_ms"] is not None)/total_n if total_n else None
        f=lambda x:f"{x:.3f}" if x is not None else "NA"
        lines.append(f"| {venue} {coin} | {model} | {len(group)} | {total_n} "
                     f"| {out['100ms'][0]} | {f(out['100ms'][1])} "
                     f"| {out['500ms'][0]} | {f(out['500ms'][1])} "
                     f"| {f(quote_age)} | {f(latency)} |")
        if model in ("glft","glft_binance"):
            print("QUALITY",venue,coin,model,
                  "FILLS",total_n,
                  "M100_VALID",out["100ms"][0],
                  "M100_NEGATIVE_FRACTION",out["100ms"][1],
                  "M500_VALID",out["500ms"][0],
                  "M500_NEGATIVE_FRACTION",out["500ms"][1],
                  "BBO_AGE_MS",quote_age,
                  "HBT_OBSERVATION_LAG_MS",latency)
    Path(a.out).parent.mkdir(parents=True,exist_ok=True)
    Path(a.out).write_text("\n".join(lines)+"\n",encoding="utf-8")


if __name__=="__main__":
    main()
