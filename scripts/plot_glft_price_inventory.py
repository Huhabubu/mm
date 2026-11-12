"""Draw the two-panel GLFT price/quotes and inventory from official HBT replay.

Only a predeclared read-only probe is added; the strategy and HBT matching
code are untouched. Emit PNG and aggregate audit JSON, never raw L2/fills.
"""
import argparse
import csv
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from check_okx_hft_data import SCHEMAS, extract_window
from cross_venue_event_probe import read_binance_bbo, visible_timestamps
from g0_glft_baseline import GAMMA, LOT, POSITION_CAP, TICK
from g0_glft_rolling import asset_for
from g0_glft_event_driven import run_event_glft, summarize
from run_beta_one_direct_gap import read_local_bbo, causal_60s_basis

NS = 1_000_000_000
EXPECTED_2026_11 = {
    "beta_0": {"fills": 73, "net_usdt": -1.6772189200000975},
    "beta_1": {"fills": 61, "net_usdt": -1.4199367800000393},
}


def build_plot_trace(raw, probe_ns, local_bbo_ns, local_bbo_mid):
    """Local past-known BBO plus locally cancellable orders and inventory."""
    probes = np.asarray(raw[34][4], dtype=np.float64)
    if probes.shape != (len(probe_ns), 7):
        raise RuntimeError("Missing or malformed official HBT probe rows")
    if not np.all(np.isfinite(probes[:, 6])):
        raise RuntimeError("Inventory probe did not execute at every timestamp")
    if np.max(np.abs(probes[:, 6])) > POSITION_CAP + 1e-9:
        raise RuntimeError("Position exceeds inventory cap")

    # Strict past quote only; flag >250ms age as missing rather than
    # silently carrying a stale quote across a gap.
    indices = np.searchsorted(local_bbo_ns, probe_ns, side="left") - 1
    safe = np.maximum(indices, 0)
    quote_age_ns = probe_ns - local_bbo_ns[safe]
    valid = (indices >= 0) & (quote_age_ns >= 0) & (quote_age_ns <= 250_000_000)
    mid = np.where(valid, local_bbo_mid[safe], np.nan)

    bid = probes[:, 0] * TICK
    ask = probes[:, 1] * TICK
    bid[~np.isfinite(probes[:, 0])] = np.nan
    ask[~np.isfinite(probes[:, 1])] = np.nan
    if not np.any(np.isfinite(mid)):
        raise RuntimeError("No valid OKX mid-price samples")
    if not (np.any(np.isfinite(bid)) and np.any(np.isfinite(ask))):
        raise RuntimeError("No locally cancellable bid/ask orders observed")
    return {
        "minutes": (probe_ns - (probe_ns[0] - NS)) / (60 * NS),
        "mid": mid, "bid": bid, "ask": ask,
        "inventory": probes[:, 6],
        "valid_mid_fraction": float(np.mean(valid)),
        "bid_fraction": float(np.mean(np.isfinite(bid))),
        "ask_fraction": float(np.mean(np.isfinite(ask))),
    }


def draw_two_panel(trace, name, day, output, shared_y):
    fig, (top, bottom) = plt.subplots(
        2, 1, figsize=(14, 8), sharex=True,
        gridspec_kw={"height_ratios": [2.1, 1.0], "hspace": 0.12},
    )
    x = trace["minutes"]
    top.plot(x, trace["mid"], label="OKX mid-price", linewidth=1.35)
    top.plot(x, trace["bid"], label="Locally cancellable bid", linewidth=0.85, alpha=0.85)
    top.plot(x, trace["ask"], label="Locally cancellable ask", linewidth=0.85, alpha=0.85)
    top.set_ylabel("USDT per BTC")
    top.set_ylim(*shared_y)
    top.legend(loc="upper left", ncol=3, fontsize=9)
    top.grid(alpha=0.2)
    top.set_title(f"GLFT {name} | OKX BTC-USDT-SWAP | {day} UTC", loc="left", fontsize=13)
    top.ticklabel_format(style="plain", axis="y", useOffset=False)

    bottom.step(x, trace["inventory"], where="post", linewidth=1.2, label="Inventory")
    bottom.axhline(0, linestyle="--", linewidth=0.9)
    # The actual path is within about 0.4 contracts here; plotting
    # the full +/-2 contract hard cap would obscure inventory movements.
    # Zoom the inventory axis using measured values, never rescale the data.
    observed_limit = float(np.nanmax(np.abs(trace["inventory"])))
    display_limit = min(POSITION_CAP, max(0.5, observed_limit * 1.20))
    bottom.set_ylabel("Inventory (contracts)")
    bottom.set_xlabel("Minutes after trade start (00:10 UTC)")
    bottom.set_ylim(-display_limit, display_limit)
    bottom.text(0.99, 0.93, "Hard cap: +/-2 contracts (off-scale)",
                transform=bottom.transAxes, ha="right", va="top", fontsize=9)
    bottom.set_xlim(0, 50)
    bottom.grid(alpha=0.2)
    fig.text(
        0.5, 0.015,
        "1s read-only probes; displayed orders are locally cancellable, not exchange-ACK timestamps.",
        ha="center", va="bottom", fontsize=9,
    )
    fig.subplots_adjust(bottom=0.10)
    fig.savefig(output, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", default="2026-11-01")
    parser.add_argument("--report-dir", default="reports")
    args = parser.parse_args()
    if datetime.strptime(args.day, "%Y-%m-%d").day != 1:
        parser.error("Tardis free date must be the first of a month")

    output = Path(args.report_dir)
    output.mkdir(parents=True, exist_ok=True)
    day_start_us = int(datetime.strptime(args.day, "%Y-%m-%d")
                       .replace(tzinfo=timezone.utc).timestamp()) * 1_000_000
    first_trade_ns = day_start_us * 1000 + 600 * NS
    # One-second local-state probes on a deterministic UTC grid.
    # Training engine origin can be a few milliseconds after midnight.
    probe_ns = np.arange(first_trade_ns + NS, first_trade_ns + 3000 * NS,
                         NS, dtype=np.int64)
    metadata = {"day_utc": args.day, "trade_window_utc": "00:10-01:00",
                "time_sample_seconds": 1, "binance_extra_delay_ms": 25,
                "source": "Tardis plus official hftbacktest 2.4.4",
                "probe_only_no_matching_change": True,
                "local_probe": "cancellable orders only",
                "variants": {}}
    traces = {}

    from hftbacktest import ROIVectorMarketDepthBacktest
    from hftbacktest.data.utils import tardis
    from hftbacktest.data.validation import validate_event_order
    with tempfile.TemporaryDirectory(prefix="glft_classic_plot_") as folder:
        folder = Path(folder)
        until_us = day_start_us + 3600 * 1_000_000 + 500_000
        paths = {}
        counts = {}
        for kind in SCHEMAS:
            paths[kind] = folder / f"{kind}.csv"
            counts[kind] = extract_window(kind, args.day, until_us,
                                         paths[kind], max_rows=2_500_000)
        npz = folder / "fused.npz"
        fused = tardis.convert_fuse(
            str(paths["trades"]), str(paths["incremental_book_L2"]),
            str(paths["book_ticker"]), tick_size=TICK, lot_size=LOT,
            output_filename=str(npz),
            ss_buffer_size=max(1000, counts["incremental_book_L2"]["snapshot_rows"] + 100),
            snapshot_mode="process",
        )
        validate_event_order(fused)
        metadata["fused_event_count"] = int(len(fused))
        del fused
        ot, om = read_local_bbo(paths["book_ticker"])
        b_local, bm = read_binance_bbo(args.day, until_us)
        bt = visible_timestamps(b_local, 25)
        basis_time, basis_value = causal_60s_basis(ot, om, bt, bm)
        low, high = float(np.min(om)) - 1000, float(np.max(om)) + 1000

        for name, beta in (("beta_0", 0.0), ("beta_1", 1.0)):
            engine = ROIVectorMarketDepthBacktest([asset_for(npz, low, high)])
            try:
                raw = run_event_glft(
                    engine, bt, bm, 50 * 600,
                    rolling=True, gamma_code=GAMMA, read_binance=True,
                    beta=beta, okx_native_ns=ot, okx_native_mid=om,
                    basis_grid_ns=basis_time, basis_values=basis_value,
                    probe_ns=probe_ns,
                )
                account = summarize(raw, 50, True, beta=beta)
                traces[name] = build_plot_trace(raw, probe_ns, ot, om)
                trade = account["trading"]
                if args.day == "2026-11-01":
                    expected = EXPECTED_2026_11[name]
                    if trade["engine_fills"] != expected["fills"]:
                        raise RuntimeError(f"{name}: fill count changed by plotting")
                    if not math.isclose(trade["end_net_marked_equity_usdt"],
                                        expected["net_usdt"], abs_tol=1e-7):
                        raise RuntimeError(f"{name}: PnL changed by plotting")
                metadata["variants"][name] = {
                    "fills": trade["engine_fills"],
                    "end_net_marked_equity_usdt": trade["end_net_marked_equity_usdt"],
                    "final_inventory_contracts": trade["end_position_contracts"],
                    "probe_count": len(probe_ns),
                    "valid_mid_fraction": traces[name]["valid_mid_fraction"],
                    "local_bid_fraction": traces[name]["bid_fraction"],
                    "local_ask_fraction": traces[name]["ask_fraction"],
                }
            finally:
                engine.close()

    all_prices = np.concatenate([
        traces[k][c][np.isfinite(traces[k][c])]
        for k in traces for c in ("mid", "bid", "ask")
    ])
    low_p, high_p = float(all_prices.min()), float(all_prices.max())
    pad = max((high_p - low_p) * 0.04, 0.5)
    for name, trace in traces.items():
        filename = output / f"classic_glft_{args.day}_{name}_50m.png"
        draw_two_panel(trace, name, args.day, filename,
                       (low_p - pad, high_p + pad))
        metadata["variants"][name]["chart_png"] = filename.name
        print("Figure:", filename, flush=True)

    filename = output / f"classic_glft_{args.day}_audit.json"
    filename.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8")
    print("Verified chart audit:", filename, flush=True)


if __name__ == "__main__":
    main()
