"""Binance top-5 snapshot price inputs: ordinary mid and raw crossed-quantity VAMP5.

Tardis book_snapshot_5 comes from L2 reconstruction, not native book_ticker.
Both prices are extracted from exactly the SAME snapshot rows / timestamps.
Use no lookahead. Price units are USDT per BTC or USDT per SOL.
"""
import csv
import gzip
import io
import math
import urllib.request
from array import array

import numpy as np

from inspect_tardis import BASE_URL


def raw_vamp5(asks_px, asks_qty, bids_px, bids_qty):
    """The five-level cross-side quantity-weighted price, with no clipping."""
    if not (len(asks_px) == len(asks_qty) == len(bids_px) == len(bids_qty) == 5):
        raise ValueError("VAMP5 requires exactly five price/quantity levels per side")
    if any(not math.isfinite(v) or v <= 0.0
           for seq in (asks_px, asks_qty, bids_px, bids_qty) for v in seq):
        raise ValueError("VAMP5 requires positive finite prices and quantities")
    if not bids_px[0] < asks_px[0]:
        raise ValueError("Crossed source book")
    if any(asks_px[i] <= asks_px[i - 1] or bids_px[i] >= bids_px[i - 1]
           for i in range(1, 5)):
        raise ValueError("Unsorted or duplicate five-level book prices")
    den = sum(asks_qty) + sum(bids_qty)
    if not (den > 0.0 and math.isfinite(den)):
        raise ValueError("No valid order-book total size")
    return (sum(asks_px[i] * bids_qty[i] + bids_px[i] * asks_qty[i]
                for i in range(5)) / den)


def read_binance_snapshot5(day, end_us, symbol, opener=urllib.request.urlopen):
    """Return local ns, snapshot BBO midpoint, raw VAMP5, and data-quality audit.

    Reject invalid top-5 rows (fail closed), rather than silently substituting
    an unweighted price. Stop streaming at a bounded UTC minute cutoff.
    """
    yy, mm, dd = day.split("-")
    url = (f"{BASE_URL}/binance-futures/book_snapshot_5/"
           f"{yy}/{mm}/{dd}/{symbol}.csv.gz")
    request = urllib.request.Request(
        url, headers={"User-Agent": "mm-binance-vamp5-audit/1.0"})
    ts, mids, weighted = array("q"), array("d"), array("d")
    previous = -1
    min_delta = math.inf
    max_delta = -math.inf
    out_of_spread = 0
    changed_top_price = 0
    prev_mid = math.nan
    with opener(request, timeout=180) as response:
        with gzip.GzipFile(fileobj=response) as compressed:
            with io.TextIOWrapper(compressed, encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream)
                required = {"local_timestamp"}
                for i in range(5):
                    for side in ("asks", "bids"):
                        required.add(f"{side}[{i}].price")
                        required.add(f"{side}[{i}].amount")
                missing = required - set(reader.fieldnames or [])
                if missing:
                    raise ValueError(f"Missing top-5 source columns: {sorted(missing)}")
                for row in reader:
                    t = int(row["local_timestamp"])
                    if t >= end_us:
                        break
                    if t < previous:
                        raise ValueError("Decreasing top-5 Binance local timestamp")
                    previous = t
                    try:
                        ap = [float(row[f"asks[{i}].price"]) for i in range(5)]
                        aq = [float(row[f"asks[{i}].amount"]) for i in range(5)]
                        bp = [float(row[f"bids[{i}].price"]) for i in range(5)]
                        bq = [float(row[f"bids[{i}].amount"]) for i in range(5)]
                        v = raw_vamp5(ap, aq, bp, bq)
                    except (ValueError, TypeError, KeyError) as exc:
                        raise ValueError(f"Invalid top-5 snapshot at {t}: {exc}") from exc
                    mid = (ap[0] + bp[0]) / 2.0
                    if not math.isfinite(v) or not math.isfinite(mid):
                        raise ValueError("Nonfinite derived Binance signal")
                    delta = v - mid
                    min_delta = min(min_delta, delta)
                    max_delta = max(max_delta, delta)
                    if v < bp[0] or v > ap[0]:
                        out_of_spread += 1
                    if math.isfinite(prev_mid) and mid != prev_mid:
                        changed_top_price += 1
                    prev_mid = mid
                    ts.append(t * 1000)
                    mids.append(mid)
                    weighted.append(v)
    if not ts:
        raise ValueError("No five-level Binance snapshots available")
    return (
        np.asarray(ts, dtype=np.int64),
        np.asarray(mids, dtype=np.float64),
        np.asarray(weighted, dtype=np.float64),
        {
            "source": "binance-futures/book_snapshot_5",
            "rows": len(ts),
            "changed_best_mid_rows": changed_top_price,
            "raw_vamp_minus_mid_min_usdt": min_delta,
            "raw_vamp_minus_mid_max_usdt": max_delta,
            "raw_vamp_outside_best_spread_count": out_of_spread,
            "raw_vamp_outside_best_spread_fraction": out_of_spread / len(ts),
            "same_event_clock_for_mid_and_vamp": True,
            "source_url": url,
        }
    )
