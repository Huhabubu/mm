"""Prepare a descriptive report from *actually reprocessed* frozen Gate Stage2/5B days."""
import argparse
import csv
import json
from pathlib import Path

DAYS = tuple(f"2026-{m:02d}-01" for m in range(6, 12))
CATEGORIES = ("threatened", "inactive_or_no_effect", "opposite")
HORIZONS = (100, 500, 1000)
FIELDS = ("total_markout_bps", "updated_quote_total_markout_bps",
          "entry_quote_lt100_total_markout_bps",
          "entry_quote_100to250_total_markout_bps")


def read_records(path):
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def number(row, key):
    v = row.get(key)
    return float(v) if v not in ("", None) else None


def mean(values):
    seq = [v for v in values if v is not None]
    return sum(seq)/len(seq) if seq else None


def fmt(value, places=4):
    return "NA" if value is None else f"{value:+.{places}f}"


def compare_paired(rows, horizon, field):
    by_key = {(r["day"], r["class"], int(r["horizon_ms"])): r for r in rows}
    pairs = []
    for day in DAYS:
        a = number(by_key[(day, "threatened", horizon)], field)
        b = number(by_key[(day, "inactive_or_no_effect", horizon)], field)
        if a is not None and b is not None:
            pairs.append(a-b)
    return len(pairs), mean(pairs), sum(v < 0 for v in pairs)


def render(stage2_dir, stage5b_dir):
    s2 = json.loads((stage2_dir/"summary.json").read_text(encoding="utf-8"))
    s5 = json.loads((stage5b_dir/"summary.json").read_text(encoding="utf-8"))
    rows = read_records(stage5b_dir/"stage5b_same_fill_by_day.csv")
    if (len(rows) != 54 or len(set((r["day"], r["class"], r["horizon_ms"])
                                   for r in rows)) != 54
            or set(r["day"] for r in rows) != set(DAYS)
            or set(r["class"] for r in rows) != set(CATEGORIES)):
        raise ValueError("Missing or duplicated Stage5B six-day results")
    if s2["status"] != "complete" or s5["status"] != "complete":
        raise ValueError("Incomplete Stage2 or Stage5B aggregate")
    lines = [
        "# Gate BTC 六日冻结成交：Stage 2 / Stage 5B Total Markout 更新",
        "",
        "只重算冻结HBT成交后的Gate原生报价估值，六日期每日独立计算、再等权平均。",
        "Total Markout = 成交方向 × (未来可见Gate中间价 − 官方实际成交价) / 实际成交价 × 10000 (bps)。",
        "买入方向+1，卖出方向−1。未来报价年龄≤500ms；100ms为补充窗口，500/1000ms为核心窗口。",
        "future_quote_updated：未来截点使用的最后一条报价必须严格晚于本地收到成交回报时刻。",
        "报价年龄分层按成交回报瞬间最后一条可见报价时间计算，主口径使用同一未来报价条件。",
        "所有Markout均为成交价至未来中间价的短期估值；仅明确命名的列扣除Maker 2bps，均非完整策略净权益。",
        "",
        "## Stage 2：原GLFT与双边Binance模型（所有实际成交）",
        "",
        "| 模型 | 窗口 | 六日日等权Total (bps) | 更新后子集 (bps) | 报价年龄<100ms (bps) | 100–250ms (bps) | 未来有效笔数(六日合计) |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for model in ("glft", "both_1p0"):
        for h in HORIZONS:
            v = s2["results"][model]["all"][str(h)]
            lines.append(
                f'| {model} | {h}ms | {fmt(v["equal_day_mean_total_markout_bps"])}'
                f' | {fmt(v["equal_day_mean_total_markout_updated_bps"])}'
                f' | {fmt(v["equal_day_mean_quote_age_lt100_total_bps"])}'
                f' | {fmt(v["equal_day_mean_quote_age_100to250_total_bps"])}'
                f' | {v["total_future_fresh_fills"]} |')
    lines += [
        "",
        "## Stage 5B：原GLFT同笔成交，按原Stage 3影子信号分类",
        "",
        "| 分类 | 窗口 | 总Markout (bps) | 扣Maker 2bps后的中间价估值 (bps) | 更新后 (bps) | 报价<100ms (bps) | 报价100–250ms (bps) | 有效笔数/日 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for cat in CATEGORIES:
        for h in HORIZONS:
            z = s5["groups"][cat][str(h)]
            lines.append(
                f'| {cat} | {h}ms | {fmt(z["total_markout_bps"])}'
                f' | {fmt(z["total_markout_minus_maker2bps_bps"])}'
                f' | {fmt(z["updated_quote_total_markout_bps"])}'
                f' | {fmt(z["entry_quote_lt100_total_markout_bps"])}'
                f' | {fmt(z["entry_quote_100to250_total_markout_bps"])}'
                f' | {z["total_fresh_fills"]:.1f} |')
    lines += [
        "",
        "## 受威胁与未激活组：严格逐日配对差值",
        "",
        "差值=当日threatened均值−同日inactive_or_no_effect均值，负值说明受威胁组更不利。",
        "只比较两组该字段均有真实有效样本的日期；负值天数是描述性一致性指标。",
        "",
        "| 窗口 | 样本层 | 可配对日期 | 日等权差值 (bps) | 负差天数 |",
        "|---|---|---:|---:|---:|",
    ]
    labels = ("全体未来有效", "未来已刷新", "入场报价<100ms", "入场报价100–250ms")
    for h in HORIZONS:
        for field, label in zip(FIELDS, labels):
            n, diff, neg = compare_paired(rows, h, field)
            lines.append(f"| {h}ms | {label} | {n}/6 | {fmt(diff)} | {neg}/{n} |")
    lines += [
        "",
        "## 深度可见性",
        "",
        "仅有冻结成交价与BBO报价，缺少订单提交时对应的逐档L2与委托记录；无法准确计算当时第20档之外的挂单/成交比例。",
        "该指标保留为uncomputable，严禁视为0或视为11月全深度队列已经得到验证。",
        "",
        "## 审计说明",
        "",
        "Stage 2使用原GLFT及双边信号策略各自的冻结官方成交；Stage 5B只使用原GLFT同笔成交及原Stage3威胁标签。",
        "旧Capture/旧Markout仍保存在各CSV/JSON与逐笔NPZ中供审计。类别间是成交条件样本对比，不能推出撤单的因果效果。",
        "这六个研究日均曾被用于实验设计与探索，不能标作独立样本外结果。",
        "",
    ]
    return "\n".join(lines)


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--stage2",type=Path,required=True)
    p.add_argument("--stage5b",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    args=p.parse_args()
    report=render(args.stage2,args.stage5b)
    args.out.parent.mkdir(parents=True,exist_ok=True)
    args.out.write_text(report,encoding="utf-8")
    print("GATE_REAL_STAGE2_STAGE5B_REPORT_OK",len(report))


if __name__=="__main__":
    main()
