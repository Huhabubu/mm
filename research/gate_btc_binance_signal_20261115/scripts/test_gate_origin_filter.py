"""Focused tests: continuous-time 600s moments and Gate Binance-origin gate."""
import numpy as np

from gate_basis_weighted_moments import NS, exact_moments_5s
from gate_origin_glft_strategy import gate_origin_filter
from run_gate_btc_origin_glft import variants,scenario
from gate_l2_snapshot_adapter import normalized_gate_l2_rows


def run():
    bt=np.array([0,100*NS,605*NS,700*NS],dtype=np.int64)
    bm=np.array([100.,102.,105.,105.])
    gt=np.array([0,700*NS],dtype=np.int64)
    gm=np.array([98.,98.])
    time,avg,sd,coverage=exact_moments_5s(bt,bm,gt,gm,600*NS,610*NS)
    assert len(time)==2 and np.allclose(coverage,1.)
    assert abs(avg[0]-(2*100+4*500)/600)<1e-10,avg
    assert abs(avg[1]-(2*95+4*505)/600)<1e-10,avg
    assert abs(sd[0]-np.sqrt((4*100+16*500)/600-avg[0]**2))<1e-9
    changed=bm.copy();changed[2]=10000.
    _,av2,_,_=exact_moments_5s(bt,changed,gt,gm,600*NS,610*NS)
    assert avg[0]==av2[0] and avg[1]==av2[1],(avg,av2)
    # Normal band records anchors, not only a price gap signal.
    step=lambda g,s,b,m,baseb,basem,has,active,changed,tie: gate_origin_filter(
        g,s,b,m,baseb,basem,has,active,changed,tie)
    r=step(.4,1.,100.,99.,np.nan,np.nan,False,False,False,False)
    assert r[0]==0. and r[1]==100. and r[2]==99. and r[3] and not r[4]
    # Binance up 3, Gate up .2: triggered and activates.
    q=step(3.,1.,103.,99.2,r[1],r[2],r[3],r[4],True,False)
    assert q[0]==3. and q[4] and q[7]==1,q
    # Same active episode, Gate-only quote update rechecks but can remain on.
    q2=step(2.4,1.,103.,99.8,q[1],q[2],q[3],q[4],False,False)
    assert q2[0]==2.4 and q2[4],q2
    # Gate-only motion from normal must NOT first activate Binance origin.
    no=step(3.,1.,100.,96.,100.,99.,True,False,False,False)
    assert no[0]==0. and not no[4],no
    # Binance price-size-only update cannot activate.
    no2=step(3.,1.,103.,99.,100.,99.,True,False,False,False)
    assert no2[0]==0.,no2
    # Coincident target feed blocks origin classification.
    tied=step(3.,1.,103.,99.2,100.,99.,True,False,True,True)
    assert tied[0]==0. and tied[8]==1,tied
    # Gate is the larger magnitude mover: reject even with Binance same sign.
    gate_dominates=step(3.,1.,101.,105.,100.,99.,True,False,True,False)
    assert gate_dominates[0]==0. and not gate_dominates[4]
    # New 90% band: 1.645 sigma activates, keeping 1sigma anchor unchanged.
    just_below = step(1.64,1.,101.64,99.,100.,99.,True,False,True,False)
    assert just_below[0]==0. and just_below[1]==100. and not just_below[4]
    lower_band = step(1.80,1.,101.80,99.,100.,99.,True,False,True,False)
    assert lower_band[0]==1.80 and lower_band[4] and lower_band[7]==1,lower_band
    # The same 1.80 sigma event would fail the original 1.96 sigma trigger.
    no_binance = step(1.80,1.,101.80,99.,100.,99.,True,False,False,False)
    assert no_binance[0]==0. and not no_binance[4]
    still_normal = step(.95,1.,100.95,99.,100.,99.,True,True,False,False)
    assert still_normal[0]==0. and not still_normal[4] and still_normal[1]==100.95
    # No change to a 1.0 sigma freeze threshold.
    # Between 1sigma and 1.645sigma: frozen anchors but no signal.
    warm=step(1.5,1.,101.,99.,100.,99.,True,False,True,False)
    assert warm[1]==100. and warm[2]==99. and warm[0]==0.
    # Mean/sigma can change while anchors remain frozen.
    sigma_changed=step(3.,2.,103.,99.,100.,99.,True,False,True,False)
    assert sigma_changed[0]==0. and sigma_changed[1]==100.
    # Return normal band refreshes anchors and clears active.
    reset=step(.3,1.,103.,100.5,q[1],q[2],q[3],q[4],False,False)
    assert reset[0]==0. and reset[1]==103. and reset[2]==100.5 and not reset[4]
    # Downside Binance-leads loss, input in same absolute USDT/BTC units.
    down=step(-3.,1.,97.,98.,100.,99.,True,False,True,False)
    assert down[0]==-3. and down[4],down
    for g in (float("nan"),float("inf")):
        invalid=step(g,1.,103.,99.,100.,99.,True,True,True,False)
        assert invalid[0]==0. and not invalid[4]
    models=variants()
    assert len(models)==9 and models[0]==("glft",0.,0)
    assert len({x[0] for x in models})==9
    spec={"order_price_round":"0.1","order_size_min":1,"quanto_multiplier":".0001",
          "maker_fee_rate":"-0.0001","taker_fee_rate":"0.00075"}
    c=scenario(78000,spec)
    assert c["qty"]==10 and c["position_cap"]==200
    assert abs(c["gamma_code"]-.0005)<1e-12
    assert c["maker_fee_rate"]==0.0002 and c["taker_fee_rate"]==0.0005
    assert c["api_maker_fee_rate"]==-0.0001
    def level(ts,side,price,amount,snapshot):
        return {"local_timestamp":str(ts),"timestamp":str(ts-100),
                "exchange":"gate-io-futures","symbol":"BTC_USDT",
                "is_snapshot":snapshot,"side":side,"price":str(price),"amount":str(amount)}
    complete=[
        level(1000,"bid",100,10,"true"),level(1000,"ask",101,15,"true"),
        level(2000,"bid",100,12,"true"),level(2000,"ask",102,11,"true")]
    m={"l2_format":None,"source_snapshot_groups":0,"source_l2_rows":0,
       "emitted_l2_rows":0,"removed_l2_levels":0}
    normalized=list(normalized_gate_l2_rows(iter(complete),m))
    assert m["l2_format"]=="per_message_full_snapshots_rewritten_to_deltas",m
    assert m["source_l2_rows"]==4 and len(normalized)==5,m
    assert normalized[0]["is_snapshot"]=="true"
    assert normalized[1]["is_snapshot"]=="true"
    assert sorted((r["side"],float(r["price"]),float(r["amount"]))
                  for r in normalized[2:])==[
                    ("ask",101.,0.),("ask",102.,11.),("bid",100.,12.)]
    native=[level(1000,"bid",100,10,"true"),level(1000,"ask",101,15,"true"),
            level(2000,"bid",100,12,"false")]
    m2=m.copy()
    assert list(normalized_gate_l2_rows(iter(native),m2))==native
    assert m2["l2_format"]=="initial_snapshot_plus_native_deltas"
    print("GATE_BASIS_ORIGIN_SIGNAL_TESTS_OK",len(models),"variants")


if __name__=="__main__":
    run()
