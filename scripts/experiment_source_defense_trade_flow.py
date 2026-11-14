"""Causal Binance taker-side trade-flow confirmation for June pilot.

Tardis normalized 'side' is LIQUIDITY TAKER side. CSV timestamp order is
capture order, local_timestamp is when provider received the row. Add the
same extra 2.5ms visibility latency as for Binance book_ticker, and enforce
strictly visible t < now in every HBT decision. No exchange timestamp used
to claim earlier information availability. Dataset may be absent: fail closed.
"""
import csv
import gzip
import io
import math
import urllib.request
from array import array

import numpy as np
from numba import njit

from inspect_tardis import BASE_URL
from cross_venue_event_probe import visible_timestamps


def read_binance_taker_trades(day, end_us, symbol,
                             extra_delay_ms=2.5, max_rows=3_500_000,
                             opener=urllib.request.urlopen):
    """Tardis trade 'amount'*'price' yields quote USD notional (including 1000PEPE)."""
    yyyy, mm, dd=day.split("-")
    url=f"{BASE_URL}/binance-futures/trades/{yyyy}/{mm}/{dd}/{symbol}.csv.gz"
    req=urllib.request.Request(url,headers={"User-Agent":"mm-sparse-defense-flow/1.0"})
    ts=array("q")
    signed=array("d")
    absolute=array("d")
    buy=sell=unknown=0
    previous=-1
    with opener(req,timeout=120) as response:
        with io.TextIOWrapper(gzip.GzipFile(fileobj=response),encoding="utf-8",newline="") as f:
            reader=csv.DictReader(f)
            needed={"local_timestamp","side","price","amount","symbol","exchange"}
            if not needed.issubset(set(reader.fieldnames or [])):
                raise ValueError(f"Missing normalized Binance taker trade columns: {needed}")
            for row in reader:
                at=int(row["local_timestamp"])
                if at>=end_us:break
                if at<previous:
                    raise ValueError("Binance trade local_timestamp reversed; cannot use causal prefix sums")
                previous=at
                if row["exchange"]!="binance-futures" or row["symbol"]!=symbol:
                    raise ValueError("Wrong trade dataset")
                side=row["side"]
                px=float(row["price"])
                qty=float(row["amount"])
                if (not math.isfinite(px) or not math.isfinite(qty) or
                        px<=0 or qty<=0):
                    raise ValueError("Invalid taker price/quantity")
                if side=="buy":
                    sg=1.
                    buy+=1
                elif side=="sell":
                    sg=-1.
                    sell+=1
                elif side=="unknown":
                    sg=0.
                    unknown+=1
                else:
                    raise ValueError("Unknown aggressor side "+side)
                notional=px*qty
                ts.append(at*1000)
                signed.append(sg*notional)
                absolute.append(notional)
                if len(ts)>max_rows:
                    raise RuntimeError("Too many Binance trades for bounded June-1 window")
    if not ts:
        raise ValueError("No Binance trades; disable trade confirmation explicitly")
    visible=visible_timestamps(np.asarray(ts,dtype=np.int64),extra_delay_ms)
    net=np.empty(len(ts)+1,dtype=np.float64)
    total=np.empty(len(ts)+1,dtype=np.float64)
    net[0]=total[0]=0.
    net[1:]=np.cumsum(np.asarray(signed,dtype=np.float64))
    total[1:]=np.cumsum(np.asarray(absolute,dtype=np.float64))
    if np.any(np.diff(visible)<0):
        raise ValueError("Non-monotone visible Binance trades")
    return visible,net,total,{"trade_rows":len(ts),"taker_buy":buy,
        "taker_sell":sell,"unknown_taker_side":unknown,"first_visible_ns":int(visible[0]),
        "last_visible_ns":int(visible[-1]),
        "extra_transport_delay_ms":float(extra_delay_ms),
        "trade_amount_unit":"native Binance symbol; notional=price*amount USDT",
        "trade_side":"Tardis taker/aggressor side; not maker side",
        "data_source_url":url}


@njit
def causal_taker_window(now_ns, visible_ns, signed_prefix, notional_prefix,
                        lookback_ns, direction, min_signed_ratio=.35,
                        min_total_notional_usdt=100.):
    """Return accepted, signed_ratio, gross_notional and net_notional.

    Strictly earlier visible trades only; no future trade labels.
    Ratio uses total *all-side* taker notional to reject sparse/mixed flow.
    """
    if (len(visible_ns)==0 or lookback_ns<=0 or direction==0 or
            len(signed_prefix)!=len(visible_ns)+1 or
            len(notional_prefix)!=len(visible_ns)+1):
        return False,0.,0.,0.
    end=np.searchsorted(visible_ns,now_ns,side="left")
    begin=np.searchsorted(visible_ns,now_ns-lookback_ns,side="left")
    if begin>=end:
        return False,0.,0.,0.
    gross=notional_prefix[end]-notional_prefix[begin]
    net=signed_prefix[end]-signed_prefix[begin]
    if gross<=0.:
        return False,0.,gross,net
    signed_ratio=net/gross
    accept=(gross>=min_total_notional_usdt and
            signed_ratio*direction>=min_signed_ratio)
    return accept,signed_ratio,gross,net
