"""Tardis Binance/Gate BTC event-union midpoint plots; causal backward-as-of."""
import argparse
import csv
import gzip
import io
import json
import math
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np

ROOT = "https://datasets.tardis.dev/v1"


def timestamp_us(day, hms):
    d = datetime.strptime(day + " " + hms, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(d.timestamp()) * 1_000_000


def read_mid(exchange, kind, symbol, day, stop_us):
    y, m, d = day.split("-")
    url = f"{ROOT}/{exchange}/{kind}/{y}/{m}/{d}/{symbol}.csv.gz"
    stamps, mids = [], []
    n_rows = n_ties = 0
    prev = -1
    req = urllib.request.Request(url, headers={"User-Agent": "Huhabubu-mm-mid-plot/1.0"})
    with urllib.request.urlopen(req, timeout=180) as resp:
        with gzip.GzipFile(fileobj=resp) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                required = {"local_timestamp", "bid_price", "ask_price"}
                if not required.issubset(set(reader.fieldnames or [])):
                    raise ValueError(f"Missing columns in {url}: {required - set(reader.fieldnames or [])}")
                for row in reader:
                    t = int(row["local_timestamp"])
                    if t >= stop_us:
                        break
                    if t < prev:
                        raise ValueError("Nonmonotonic local_timestamp in " + url)
                    bid, ask = float(row["bid_price"]), float(row["ask_price"])
                    if not (math.isfinite(bid) and math.isfinite(ask) and 0 < bid < ask):
                        raise ValueError(f"Invalid BBO in {url} at {t}")
                    mid = (bid + ask) / 2.0
                    n_rows += 1
                    # Equal collector-microsecond timestamps have no provable cross-venue order.
                    # Use the final state at that instant; do not invent sub-microsecond ordering.
                    if t == prev:
                        mids[-1] = mid
                        n_ties += 1
                    else:
                        stamps.append(t)
                        mids.append(mid)
                    prev = t
    if not stamps:
        raise ValueError("Empty Tardis feed: " + url)
    return np.asarray(stamps, np.int64), np.asarray(mids, np.float64), {
        "url": url, "raw_rows": n_rows, "unique_local_timestamps": len(stamps),
        "same_timestamp_extra_rows": n_ties,
    }


def align(bt, bm, gt, gm, start_us, stop_us):
    # Use ALL event timestamps from BOTH feeds; high-frequency-only master
    # would omit Gate-only changes. Backward as-of never reads a future quote.
    times = np.union1d(bt, gt)
    times = times[(times >= start_us) & (times < stop_us)]
    bi = np.searchsorted(bt, times, side="right") - 1
    gi = np.searchsorted(gt, times, side="right") - 1
    keep = (bi >= 0) & (gi >= 0)
    times, bi, gi = times[keep], bi[keep], gi[keep]
    if len(times) == 0:
        raise ValueError("No overlap between the two feeds")
    assert np.all(bt[bi] <= times) and np.all(gt[gi] <= times)
    assert np.all(np.diff(times) > 0)
    return times, bm[bi], gm[gi]


def draw(day, start_us, stop_us, t, b, g, target):
    minutes = (t - start_us) / 60_000_000.0
    gap = b - g
    fig, (ax, bottom) = plt.subplots(2, 1, figsize=(15, 7.5), sharex=True,
        gridspec_kw={"height_ratios": [3.2, 1.25]}, constrained_layout=True)
    ax.plot(minutes, b, color="blue", linewidth=0.8, drawstyle="steps-post",
            label="Binance mid (native BBO)", zorder=3)
    ax.plot(minutes, g, color="red", linewidth=0.8, drawstyle="steps-post",
            label="Gate mid (Tardis reconstructed quotes)", zorder=3)
    ax.fill_between(minutes, b, g, step="post", color="yellow", alpha=0.62,
                    label="Price difference", zorder=2)
    ax.set_ylabel("BTC mid-price (USDT)")
    ax.legend(loc="upper left", ncol=3, fontsize=8)
    ax.grid(alpha=.2)
    bottom.fill_between(minutes, gap, 0, step="post", color="yellow", alpha=.78)
    bottom.plot(minutes, gap, color="#a88700", linewidth=.45, drawstyle="steps-post")
    bottom.axhline(0, color="black", linewidth=.7, alpha=.6)
    bottom.set_ylabel("Binance - Gate (USDT)")
    bottom.grid(alpha=.2)
    bottom.set_xlim(0, (stop_us - start_us) / 60_000_000)
    start_dt = datetime.fromtimestamp(start_us / 1e6, tz=timezone.utc)
    stop_dt = datetime.fromtimestamp(stop_us / 1e6, tz=timezone.utc)
    bottom.xaxis.set_major_formatter(FuncFormatter(
        lambda x, _: (start_dt + timedelta(minutes=float(x))).strftime("%H:%M")))
    bottom.set_xlabel("Tardis collector local_timestamp (UTC)")
    fig.suptitle(
        f"BTC-USDT perpetual mid-prices: Binance (blue) / Gate (red) | "
        f"{day} {start_dt:%H:%M}–{stop_dt:%H:%M} UTC\n"
        "Every unique event timestamp from either venue; backward as-of only",
        fontsize=12)
    fig.savefig(target, dpi=150, facecolor="white")
    plt.close(fig)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", nargs="+",
                   default=["2026-09-01", "2026-10-01", "2026-11-01"])
    p.add_argument("--start", default="00:10:00")
    p.add_argument("--minutes", type=int, default=50)
    p.add_argument("--out", default="reports/visualizations/binance_gate_mids")
    args = p.parse_args()
    if not 1 <= args.minutes <= 60:
        p.error("minutes must be 1–60")
    root = Path(args.out)
    root.mkdir(parents=True, exist_ok=True)
    summaries = []
    for day in args.days:
        start = timestamp_us(day, args.start)
        stop = start + args.minutes * 60 * 1_000_000
        print("START", day, args.start, args.minutes, flush=True)
        bt, bm, bi = read_mid("binance-futures", "book_ticker", "BTCUSDT", day, stop)
        print("BINANCE", day, json.dumps(bi), flush=True)
        gt, gm, gi = read_mid("gate-io-futures", "quotes", "BTC_USDT", day, stop)
        print("GATE", day, json.dumps(gi), flush=True)
        times, b, g = align(bt, bm, gt, gm, start, stop)
        filename = f"binance_gate_BTC_{day}_{args.minutes}min.png"
        draw(day, start, stop, times, b, g, root / filename)
        meta = {"day": day, "start_utc": args.start,
                "duration_minutes": args.minutes,
                "binance": bi, "gate": gi,
                "aligned_event_timestamps": len(times),
                "min_raw_gap_usdt": float(np.min(b - g)),
                "max_raw_gap_usdt": float(np.max(b - g)),
                "assumed_extra_transport_delay_ms": 0,
                "plot": filename}
        print("PLOT_OK", json.dumps(meta), flush=True)
        summaries.append(meta)
    (root / "summary.json").write_text(json.dumps(summaries, indent=2) + "\n")
    header = (
        "# Binance / Gate event-aligned mid-price plots\n\n"
        f"BTC USDT perpetuals, Tardis 1st-of-month free data, starting {args.start} UTC, "
        f"{args.minutes} minutes for each date.\n\n"
        "**Blue**: Binance native `book_ticker` midpoint. **Red**: Gate Tardis "
        "`quotes` midpoint reconstructed from L2. **Yellow**: raw difference "
        "between the two mid-prices (top shaded area and bottom gap panel).\n\n"
        "The time axis uses Tardis collector `local_timestamp` (UTC). "
        "Take the union of **all event timestamps from both exchanges**. "
        "At each timestamp take the latest available price at or before that time "
        "(backward as-of). Rows sharing a microsecond timestamp are collapsed "
        "to the final quote at that timestamp, so no inter-venue ordering is "
        "asserted inside a microsecond. No future quotes, no transport delay "
        "adjustment, and no 60-second basis subtraction. This is descriptive "
        "visualization, not a trading backtest.\n\n"
    )
    header += "\n".join(f"- [{x['day']} — {args.minutes} minutes]({x['plot']})" for x in summaries) + "\n"
    (root / "README.md").write_text(header, encoding="utf-8")


if __name__ == "__main__":
    main()
