"""Aggregate fixed-date native BBO diagnostics; day-equal means, no tick pooling."""
import argparse
import json
from pathlib import Path

HORIZONS = ("5", "10", "25", "50")
VARIANTS = ("g60", "g300", "raw")


def summarize(files):
    rows = [json.loads(Path(f).read_text(encoding="utf-8")) for f in files]
    rows.sort(key=lambda r: (r["coin"], r["day"]))
    out = {"per_day": rows, "by_coin": {}}
    lines = [
        "# Binance→OKX 基差滞后：60秒 / 300秒 / 原始价差真实对照",
        "",
        "**只读预测诊断**。单位为bps，正值表示预测方向与未来OKX中间价变动同向；"
        "并非做市成交收益、费用或逆向选择损失。",
        "2026年6—11月每月首日UTC00:10—01:00，100ms决策，原生BBO，主延迟2.5ms。",
        "6个日期都曾用于研发探索，不能作为新的盲测。",
        "",
    ]
    for coin in ("BTC", "SOL"):
        group = [r for r in rows if r["coin"] == coin]
        if not group:
            continue
        lines += [
            f"## {coin}: {len(group)}/6 个日期报告",
            "",
            "| 日期 | 三模型共同有效时点 | 有效率 | 近300秒基线但60秒偏移≥2档 | 残差符号冲突 |",
            "|---|---:|---:|---:|---:|",
        ]
        result = {"days": len(group), "horizons": {}}
        for r in group:
            d = r["diagnostics"]
            lines.append(
                f"| {r['day']} | {d['joint_valid_100ms_points']:,} "
                f"| {d['joint_valid_fraction']:.2%} "
                f"| {d['near_300s_but_strong_60s_points']:,} "
                f"| {d['opposite_60s_vs_300s_residual_points']:,} |"
            )
        lines += [
            "",
            "**每一日期先求均值，再对有效日期等权平均：**",
            "",
            "| 未来期限 | 60秒信号 | 300秒信号 | 原始价差 | 60秒−300秒 | 有效日 | 含价格变化样本总数 |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
        for h in HORIZONS:
            valid_rows = [
                r for r in group
                if r["future_okx"][h]["joint_valid_points"] > 0
                and all(r["future_okx"][h]["variants"][k][
                    "mean_signed_future_bps_all_valid"] is not None
                    for k in VARIANTS)
            ]
            avg = {}
            for v in VARIANTS:
                avg[v] = (
                    sum(r["future_okx"][h]["variants"][v][
                        "mean_signed_future_bps_all_valid"] for r in valid_rows)
                    / len(valid_rows) if valid_rows else None)
            result["horizons"][h] = {
                "date_equal_mean_signed_bps": avg,
                "valid_days": len(valid_rows),
                "nonzero_future_move_points": sum(
                    r["future_okx"][h]["nonzero_future_price_moves"]
                    for r in valid_rows),
            }
            if valid_rows:
                delta = avg["g60"] - avg["g300"]
                lines.append(
                    f"| {h}ms | {avg['g60']:+.6f} | {avg['g300']:+.6f} "
                    f"| {avg['raw']:+.6f} | {delta:+.6f} "
                    f"| {len(valid_rows)}/{len(group)} "
                    f"| {result['horizons'][h]['nonzero_future_move_points']:,} |"
                )
            else:
                lines.append(f"| {h}ms | N/A | N/A | N/A | N/A | 0/{len(group)} | 0 |")
        lines += [
            "",
            "注意：接近300秒均值是代理事件；它不能直接证明60秒信号错误。"
            "原生BBO价格预测的改善也不能直接证明maker成交质量改善。",
            "",
        ]
        out["by_coin"][coin] = result
    return out, "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    files = sorted(Path(args.input_dir).glob("*.json"))
    if not files:
        raise RuntimeError("No per-date JSON outputs found")
    stats, report = summarize(files)
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    (output / "summary.json").write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
    (output / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
