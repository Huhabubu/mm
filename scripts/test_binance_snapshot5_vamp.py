"""Toy-only tests of raw five-level VAMP and actual Tardis CSV input wiring."""
import csv
import gzip
import io
import unittest

import numpy as np

from binance_snapshot5_vamp import raw_vamp5, read_binance_snapshot5
from run_beta_one_direct_gap import causal_60s_basis
from cross_venue_event_probe import visible_timestamps


class BinanceVamp5Tests(unittest.TestCase):
    def test_cross_quantity_weighted_formula_and_unclipped_value(self):
        asks=[100.01,100.02,100.03,100.04,100.05]
        bids=[99.99,99.98,99.97,99.96,99.95]
        aq=[1.]*5
        bq=[1.,1.,1.,1.,100.]
        price=raw_vamp5(asks,aq,bids,bq)
        expected=(sum(a*x+b*y for a,b,x,y in zip(asks,bids,bq,aq))/
                  (sum(aq)+sum(bq)))
        self.assertAlmostEqual(price,expected)
        self.assertGreater(price,asks[0])
        self.assertAlmostEqual((asks[0]+bids[0])/2.,100.)

    def test_balanced_symmetric_levels_equal_mid(self):
        asks=[101.,102.,103.,104.,105.]
        bids=[99.,98.,97.,96.,95.]
        q=[1.,2.,3.,4.,5.]
        self.assertAlmostEqual(raw_vamp5(asks,q,bids,q),100.)

    def test_bad_depth_fails_closed(self):
        with self.assertRaises(ValueError):
            raw_vamp5([100.01]*5,[1.]*5,[99.99]*5,[1.]*5)
        with self.assertRaises(ValueError):
            raw_vamp5([100.01+i*.01 for i in range(5)],
                      [1.]*5,[99.99-i*.01 for i in range(5)],
                      [1.,2.,0.,4.,5.])

    def _fake_csv(self):
        columns=["local_timestamp"]
        for i in range(5):
            columns += [f"asks[{i}].price",f"asks[{i}].amount",
                        f"bids[{i}].price",f"bids[{i}].amount"]
        out=io.StringIO()
        writer=csv.DictWriter(out,fieldnames=columns)
        writer.writeheader()
        for timestamp,qty in ((1000000,1.),(1000000,2.),(1000100,3.)):
            row={"local_timestamp":timestamp}
            for i in range(5):
                row[f"asks[{i}].price"]=100.01+.01*i
                row[f"bids[{i}].price"]=99.99-.01*i
                row[f"asks[{i}].amount"]=1.
                row[f"bids[{i}].amount"]=qty if i==4 else 1.
            writer.writerow(row)
        return gzip.compress(out.getvalue().encode("utf-8"))

    def test_csv_schema_identical_event_clock_and_existing_delay(self):
        data=self._fake_csv()
        class Response:
            def __enter__(self):return io.BytesIO(data)
            def __exit__(self,*args):pass
        captured=[]
        def fake_opener(request,timeout=0):
            captured.append(request.full_url)
            return Response()
        t,mid,vamp,detail=read_binance_snapshot5(
            "2026-06-01",2000000,"SOLUSDT",opener=fake_opener)
        np.testing.assert_array_equal(t,[1000000000,1000000000,1000100000])
        np.testing.assert_array_equal(mid,[100.,100.,100.])
        self.assertEqual(len(vamp),len(mid))
        self.assertNotEqual(vamp[1],vamp[0])
        np.testing.assert_array_equal(
            visible_timestamps(t,2.5)-t,[2500001]*3)
        self.assertEqual(detail["rows"],3)
        self.assertTrue(detail["same_event_clock_for_mid_and_vamp"])
        self.assertIn("/book_snapshot_5/2026/06/01/SOLUSDT.csv.gz",captured[0])


if __name__=="__main__":
    unittest.main()
