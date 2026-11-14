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
                        min_total_notional_usdt=100.,
                        min_directional_net_notional_usdt=0.):
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
            signed_ratio*direction>=min_signed_ratio and
            net*direction>=min_directional_net_notional_usdt)
    return accept,signed_ratio,gross,net



@njit
def causal_pretrade_directional_p80(visible_ns,signed_prefix,notional_prefix,
                                   start_ns,stop_ns,lookback_ns=100_000_000,
                                   sample_ns=100_000_000,quantile=.8):
    """Calibrate positive directional net taker notional *only* in warmup.

    Each sample at its own strictly past-visible 100ms end; separate positive
    buy/sell net-dollar values. Quantile on positive windows avoids zero-P80
    degeneracy when a quiet symbol has few signed trades; report sample sizes.
    Return threshold for buy/sell, counts, and all warmup grid observations.
    """
    if not (0<quantile<1 and lookback_ns>0 and sample_ns>0 and
            stop_ns>start_ns+lookback_ns):
        raise ValueError("Bad strictly causal taker training window")
    n=int((stop_ns-start_ns)//sample_ns)
    buys=np.empty(n,np.float64)
    sells=np.empty(n,np.float64)
    count_b=count_s=valid=0
    for j in range(1,n+1):
        t=start_ns+j*sample_ns
        if t>stop_ns:
            break
        hi=np.searchsorted(visible_ns,t,side="left")
        lo=np.searchsorted(visible_ns,t-lookback_ns,side="left")
        net=signed_prefix[hi]-signed_prefix[lo]
        gross=notional_prefix[hi]-notional_prefix[lo]
        if gross<=0:
            continue
        valid+=1
        if net>0:
            buys[count_b]=net
            count_b+=1
        elif net<0:
            sells[count_s]=-net
            count_s+=1
    if count_b<20 or count_s<20:
        raise ValueError("Too few independent pretrade directional taker windows")
    b=np.sort(buys[:count_b])
    ss=np.sort(sells[:count_s])
    qb=b[int(quantile*(count_b-1))]
    qs=ss[int(quantile*(count_s-1))]
    if qb<=0 or qs<=0:
        raise ValueError("Nonpositive high-intensity taker threshold")
    return qb,qs,count_b,count_s,valid
