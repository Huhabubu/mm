"""Focused deterministic assertions for pure-Tokyo exact-order cancellation audit."""
import unittest
import numpy as np

from experiment_tokyo_4h import (
    exact_ack_attribution,DAYS,MARKETS,TRADE_STEPS,DELAY_MS
)


def filled(t=100_000_000):
    z={"exchange_timestamp_ns":np.array([t],np.int64),
       "local_observed_ns":np.array([t+2_000_000],np.int64),
       "side_sign":np.array([1],np.int8),
       "exec_price":np.array([10.00]),
       "exec_qty":np.array([1.]),
       "tick_size":.01,
       "spread_capture_bps":np.array([1.])}
    for h in (100,500,1000):
        z[f"fresh_{h}ms"]=np.array([True])
        z[f"markout_{h}ms_bps"]=np.array([-2.])
    return z


def empty():
    z={"exchange_timestamp_ns":np.empty(0,np.int64),
       "side_sign":np.empty(0,np.int8),
       "exec_price":np.empty(0),
       "exec_qty":np.empty(0),
       "spread_capture_bps":np.empty(0)}
    for h in (100,500,1000):
        z[f"fresh_{h}ms"]=np.empty(0,bool)
        z[f"markout_{h}ms_bps"]=np.empty(0)
    return z


def trace(fill_oid=True,source=False,submit_time=10_000_000,
          ack=51_000_000,req=50_000_000,oid=2000):
    x=np.array([oid],np.int64)
    q=np.array([1.])
    z=np.empty(0,np.int64)
    if source:
        return (z,x,np.array([1000],np.int64),q,np.array([ack],np.int64),
                x,np.array([submit_time],np.int64),q)
    return (x,z,z,np.empty(0),z,x,np.array([submit_time],np.int64),q)


def episode(req=50_000_000):
    return (np.array([20_000_000],np.int64),
            np.array([80_000_000],np.int64),
            np.array([-1],np.int64),
            np.array([req],np.int64),
            np.array([-1],np.int64))


class HBTOrderAckTests(unittest.TestCase):
    def test_happy_path_needs_exact_lineage_and_ack_before_fill(self):
        res=exact_ack_attribution(filled(),empty(),trace(),
                                  trace(source=True),episode())
        self.assertEqual(res["strict_same_submit_order_id_cancel_ACK_before_G0_fill"]["count"],1)
        self.assertEqual(res["G0_only_not_strictly_attributed"]["count"],0)

    def test_late_ack_does_not_become_exact_avoided(self):
        r=exact_ack_attribution(filled(),empty(),trace(),
                    trace(source=True,ack=101_000_000),episode())
        self.assertEqual(r["strict_same_submit_order_id_cancel_ACK_before_G0_fill"]["count"],0)

    def test_reused_price_order_id_without_matching_submit_rejected(self):
        r=exact_ack_attribution(filled(),empty(),trace(),
                trace(source=True,submit_time=11_000_000),episode())
        self.assertEqual(r["strict_same_submit_order_id_cancel_ACK_before_G0_fill"]["count"],0)

    def test_ack_request_outside_defense_rejected(self):
        r=exact_ack_attribution(filled(),empty(),trace(),
                    trace(source=True,req=90_000_000,ack=91_000_000),
                    episode(req=90_000_000))
        self.assertEqual(r["strict_same_submit_order_id_cancel_ACK_before_G0_fill"]["count"],0)

    def test_no_out_of_scope_calibration_or_test_dates(self):
        self.assertEqual(DAYS,("2026-09-01","2026-10-01","2026-11-01"))
        self.assertEqual(len(MARKETS),4)
        self.assertEqual(TRADE_STEPS,144000)
        self.assertEqual(DELAY_MS,(0.,1.,2.5))


if __name__=="__main__":
    unittest.main()
