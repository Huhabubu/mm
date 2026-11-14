"""Synthetic deterministic read-only attribution checks; not market outcomes."""
import unittest
import numpy as np

from experiment_source_defense_step1_diagnostic import (
    fill_group_summary, position_valuation_proxy, prefill_binance_gap_buckets,
)
from experiment_source_defense_pilot import classify_opportunities


def fills(ts,side,marks):
    t=np.asarray(ts,np.int64)
    s=np.asarray(side,np.int8)
    v=np.asarray(marks,float)
    d={"exchange_timestamp_ns":t,"local_observed_ns":t+1,
       "side_sign":s,"exec_qty":np.ones(len(t))*2,
       "exec_price":np.ones(len(t))*100.}
    for h in (100,500,1000,5000):
        d[f"markout_{h}ms_bps"]=v.copy()
        d[f"fresh_{h}ms"]=np.ones(len(t),bool)
    return d


class AttributionTests(unittest.TestCase):
    def test_added_is_variant_only_not_net_fills(self):
        g=fills([10,20,30],[1,-1,1],[-3,-1,2])
        b=fills([10,30,40],[1,1,-1],[-2,2,-4])
        dropped,kept=classify_opportunities(g,b)
        added,_=classify_opportunities(b,g)
        self.assertEqual((int(dropped.sum()),int(kept.sum()),int(added.sum())),
                         (1,2,1))
        z=fill_group_summary(b,added,1.)
        self.assertEqual(z["fill_count"],1)
        self.assertEqual(z["notional_usdt"],200.)
        self.assertAlmostEqual(z["by_horizon"]["100ms"]["mean_markout_bps"],-4.)
        self.assertAlmostEqual(z["by_horizon"]["100ms"]["markout_dollar_exposure_proxy"],-.08)

    def test_inventory_price_path_and_terminal(self):
        f=fills([10,20],[1,-1],[0,0])
        ot=np.array([0,10,15,20,25],np.int64)
        om=np.array([100.,100.,102.,102.,103.])
        z=position_valuation_proxy(f,ot,om,1.,25)
        self.assertEqual(z["terminal_inventory_contracts"],0.)
        self.assertAlmostEqual(z["inventory_holding_mtm_drift_usdt_proxy"],4.)

    def test_bps_g0_buckets_are_exhaustive(self):
        # Artificial visible Binance +1 and reference gap about 1bps; quote
        # sequence intentionally small. No future labels used for selection.
        base=5_000_000_000
        g=fills([base,base+100_000_000],[1,-1],[-1.,.5])
        bt=np.array([base-300_000_000,base-130_000_000,
                     base-2_000_000,base+98_000_000],np.int64)
        bm=np.array([100.,100.,100.01,100.01])
        ot=np.array([base-250_000_000,base-100_000_000,
                     base-3_000_000,base+97_000_000],np.int64)
        om=np.array([100.,100.,100.,100.])
        grid=np.array([base-10_000_000,base+90_000_000],np.int64)
        ema=np.array([0.,0.])
        z=prefill_binance_gap_buckets(g,ot,om,bt,bm,grid,ema,.01)
        self.assertEqual(sum(x["fills"] for x in z["buckets"]),len(g["side_sign"]))
        self.assertIn("未触发",[x["bin"] for x in z["buckets"]])
        self.assertEqual(z["asof_clock"].split(";")[0],"exchange_fill_timestamp_minus_1ms")


if __name__=="__main__":
    unittest.main()
