"""Plot date-specific causal gap bins from aggregate-only research results."""
import argparse
import json
from pathlib import Path


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="reports/event_gap_regression_exploratory.json")
    p.add_argument("--out-dir", default="reports")
    args = p.parse_args()

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    report = json.loads(Path(args.input).read_text(encoding="utf-8"))
    if report.get("status") != "complete":
        raise RuntimeError("No completed gap diagnostic to plot")
    alpha = report["fit"]["alpha_usdt_per_btc"]
    beta = report["fit"]["beta"]
    output = Path(args.out_dir)
    output.mkdir(parents=True, exist_ok=True)
    for day in ("2026-09-01", "2026-10-01", "2026-11-01"):
        for source in ("combined", "okx_trigger", "binance_trigger"):
            bins = report["scores"][day][source]["bin_summary"]
            x = [b["mean_gap_usdt_per_btc"] for b in bins]
            y = [b["mean_future_change_usdt_per_btc"] for b in bins]
            fig, ax = plt.subplots(figsize=(7.5, 4.5))
            ax.plot(x, y, marker="o", label="Realized mean OKX move")
            ax.plot(x, [alpha + beta * val for val in x],
                    linestyle="--", label="Training OLS prediction")
            ax.axhline(0, linewidth=0.8)
            ax.axvline(0, linewidth=0.8)
            ax.set_xlabel("Bin mean gap (USDT / BTC)")
            ax.set_ylabel(f"OKX mid change after {report['tau_ms']}ms (USDT / BTC)")
            ax.set_title(f"{day} | {source} | exploratory (previously inspected day)")
            ax.legend()
            fig.tight_layout()
            target = output / f"event_gap_bins_{day}_{source}.png"
            fig.savefig(target, dpi=160)
            plt.close(fig)
            print("plot:", target)


if __name__ == "__main__":
    main()
