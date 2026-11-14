"""G0-only causal EMA re-use: strict past, bounded age, no fake stale label."""
import unittest
import numpy as np

from experiment_source_defense_step1_diagnostic import prefill_binance_gap_buckets


class PepeMissingLabelTests(unittest.TestCase):
    def build(self):
        g0={"exchange_timestamp_ns":np.array([1_000_000_000],np.int64)}
        for h in (100,500,1000):
            g0[f"fresh_{h}ms"]=np.array([True])
            g0[f"markout_{h}ms_bps"]=np.array([-2.])
        ot=np.array([900_000_000],np.int64)
        om=np.array([100.])
        bt=np.array([780_000_000,900_000_000],np.int64)
        bm=np.array([100.1,101.])
        grid=np.array([800_000_000,900_000_000],np.int64)
        basis=np.array([.1,np.nan])
        return g0,ot,om,bt,bm,grid,basis

    def test_latest_nan_can_use_only_bounded_prior_finite_ema(self):
        args=self.build()
        original=prefill_binance_gap_buckets(*args,.01)
        repaired=prefill_binance_gap_buckets(
            *args,.01,asof_ema=True,ema_max_age_ns=250_000_000)
        self.assertEqual(original["buckets"][0]["fills"],1)
        self.assertEqual(repaired["buckets"][0]["fills"],0)
        self.assertEqual(sum(x["fills"] for x in repaired["buckets"]),1)

    def test_stale_ema_is_rejected_even_with_more_recent_source_quotes(self):
        args=self.build()
        x=prefill_binance_gap_buckets(
            *args,.01,asof_ema=True,ema_max_age_ns=100_000_000)
        self.assertEqual(x["buckets"][0]["fills"],1)

    def test_default_archive_semantics_unchanged(self):
        args=self.build()
        old=prefill_binance_gap_buckets(*args,.01)
        explicitly_old=prefill_binance_gap_buckets(
            *args,.01,freshness_ns=250_000_000,asof_ema=False)
        self.assertEqual(old,explicitly_old)


if __name__=="__main__":
    unittest.main()
