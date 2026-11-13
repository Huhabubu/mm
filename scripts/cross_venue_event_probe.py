"""Causal external Binance feed next to official hftbacktest OKX event replay.

This module schedules events only. Matching, queue position, fill and cancellation
execution remain exclusively inside hftbacktest 2.4.4. All clocks use epoch ns.
"""
import csv
import gzip
import io
import urllib.request
from array import array

import numpy as np

from inspect_tardis import BASE_URL


def read_binance_bbo(day, end_us, opener=urllib.request.urlopen, symbol="BTCUSDT"):
    """Read bounded free Tardis native book_ticker; preserve microsecond row order."""
    yyyy, mm, dd = day.split("-")
    url = f"{BASE_URL}/binance-futures/book_ticker/{yyyy}/{mm}/{dd}/{symbol}.csv.gz"
    ts = array("q")
    mids = array("d")
    previous = -1
    request = urllib.request.Request(url, headers={"User-Agent": "mm-cross-venue-event-probe/1.0"})
    with opener(request, timeout=120) as response:
        with gzip.GzipFile(fileobj=response) as zipped:
            with io.TextIOWrapper(zipped, encoding="utf-8", newline="") as stream:
                for row in csv.DictReader(stream):
                    t = int(row["local_timestamp"])
                    if t >= end_us:
                        break
                    if t < previous:
                        raise ValueError("Binance BBO local timestamps are decreasing")
                    previous = t
                    bid, ask = float(row["bid_price"]), float(row["ask_price"])
                    if not (0 < bid < ask):
                        raise ValueError("Invalid Binance bid/ask")
                    ts.append(t * 1000)
                    mids.append((bid + ask) / 2)
    if not ts:
        raise ValueError("No Binance BBO before end_us")
    return np.asarray(ts, dtype=np.int64), np.asarray(mids, dtype=np.float64)


def visible_timestamps(local_ns, extra_delay_ms=25):
    """Use measured collection time plus an *assumed* signal transport delay.

    For a positive simulated extra delay, one ns after the reported
    microsecond avoids ambiguous ties with OKX at the same resolution.
    Zero-delay experiments keep exact local timestamps; when equal, the
    event loop consumes HBT target events before Binance rows.
    Original row order, including equal timestamps, stays.
    """
    if extra_delay_ms < 0:
        raise ValueError("negative extra delay")
    local_ns = np.asarray(local_ns, dtype=np.int64)
    if local_ns.ndim != 1 or (len(local_ns) > 1 and np.any(np.diff(local_ns) < 0)):
        raise ValueError("external timestamps must be sorted")
    return local_ns + int(extra_delay_ms * 1_000_000) + (1 if extra_delay_ms > 0 else 0)


def replay_union(engine, binance_ts_ns, binance_mid, stop_ns, on_event, max_events=2_000_000):
    """Advance official engine to the earliest OKX feed, order reply, or BBO.

    on_event(kind, timestamp_ns, last_binance_mid) may submit/cancel OKX orders.
    Reaching the external clock itself does NOT imply any cancel is effective.
    A timeout is RELATIVE to engine.current_timestamp, not an absolute date.
    """
    bt = np.asarray(binance_ts_ns, dtype=np.int64)
    bm = np.asarray(binance_mid, dtype=np.float64)
    if bt.ndim != 1 or bt.shape != bm.shape:
        raise ValueError("external feed shape mismatch")
    if len(bt) > 1 and np.any(np.diff(bt) < 0):
        raise ValueError("external feed not sorted")
    if not np.all(np.isfinite(bm)):
        raise ValueError("nonfinite Binance midpoint")
    if engine.elapse(0) != 0:
        raise RuntimeError("Unable to initialize OKX replay")
    now = int(engine.current_timestamp)
    if now >= stop_ns:
        raise ValueError("stop before first OKX event")
    idx = int(np.searchsorted(bt, now, side="left"))
    # Events before engine's first event are observable, but still subject to age guards.
    latest = float(bm[idx - 1]) if idx else np.nan
    counters = {"okx_feed": 0, "order_reply": 0, "binance_feed": 0,
                "timeouts": 0, "first_ns": now, "last_ns": now}
    handled = 0
    while now < stop_ns:
        if handled >= max_events:
            raise RuntimeError("Joint replay event cap exceeded")
        due = min(stop_ns, int(bt[idx]) if idx < len(bt) else stop_ns)
        if due <= now and idx < len(bt):
            # Never batch future rows with the same timestamp: update one by one.
            latest = float(bm[idx])
            idx += 1
            counters["binance_feed"] += 1
            on_event("binance_feed", now, latest)
            handled += 1
            continue
        delta = due - now
        if delta <= 0:
            break
        status = int(engine.wait_next_feed(True, int(delta)))
        advanced = int(engine.current_timestamp)
        if advanced < now or advanced > due:
            raise RuntimeError("Official replay clock violated requested deadline")
        if status == 1:
            break
        now = advanced
        counters["last_ns"] = now
        if status == 2:
            counters["okx_feed"] += 1
            on_event("okx_feed", now, latest)
        elif status == 3:
            counters["order_reply"] += 1
            on_event("order_reply", now, latest)
        elif status == 0:
            counters["timeouts"] += 1
            if now < due:
                raise RuntimeError("Unadvanced replay timeout")
        else:
            raise RuntimeError(f"Unexpected hftbacktest status {status}")
        handled += 1
    counters["last_ns"] = now
    counters["external_consumed"] = idx
    return counters
