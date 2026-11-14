"""Deterministic engine-path test: t=0 submit, t=2ms cancellable Ask danger."""
import unittest
from types import SimpleNamespace

from hftbacktest import BUY, SELL
from experiment_source_defense_engine import MIN_REQUOTE_NS, cancel_if_needed


class AuditEngine:
    def __init__(self):
        self.calls=[]
        self.clock_ns=0
    def cancel(self, asset, order_id, wait):
        self.calls.append((self.clock_ns, asset, order_id, wait))


class EmergencyCancelTests(unittest.TestCase):
    def test_ask_sent_at_0_cancel_immediately_on_upward_signal_at_2ms(self):
        engine=AuditEngine()
        submitted_ns=0
        ask=SimpleNamespace(side=SELL, order_id=101, price_tick=1000,
                            cancellable=True)
        engine.clock_ns=2_000_000
        self.assertLess(engine.clock_ns-submitted_ns,MIN_REQUOTE_NS)
        # Exercise the very same helper invoked by the Numba HBT event loop.
        result=cancel_if_needed.py_func(engine,ask,engine.clock_ns,submitted_ns,
                                        1050,True,False)
        self.assertEqual(result,2)
        self.assertEqual(engine.calls,[(2_000_000,0,101,False)])

    def test_ordinary_price_change_still_waits_twenty_ms(self):
        engine=AuditEngine()
        bid=SimpleNamespace(side=BUY, order_id=102, price_tick=900,
                            cancellable=True)
        engine.clock_ns=2_000_000
        self.assertEqual(cancel_if_needed.py_func(engine,bid,engine.clock_ns,0,
                                                  890,False,False),0)
        self.assertEqual(engine.calls,[])
        engine.clock_ns=20_000_000
        self.assertEqual(cancel_if_needed.py_func(engine,bid,engine.clock_ns,0,
                                                  890,False,False),1)
        self.assertEqual(engine.calls,[(20_000_000,0,102,False)])

    def test_pending_cancel_not_sent_twice(self):
        engine=AuditEngine()
        ask=SimpleNamespace(side=SELL, order_id=103, price_tick=1000,
                            cancellable=False)
        engine.clock_ns=2_000_000
        self.assertEqual(cancel_if_needed.py_func(engine,ask,engine.clock_ns,0,
                                                  1040,True,True),0)
        self.assertEqual(engine.calls,[])

    def test_defensive_order_never_moved_inward(self):
        engine=AuditEngine()
        ask=SimpleNamespace(side=SELL, order_id=104, price_tick=1090,
                            cancellable=True)
        engine.clock_ns=2_000_000
        self.assertEqual(cancel_if_needed.py_func(engine,ask,engine.clock_ns,0,
                                                  1050,True,False),0)
        self.assertEqual(engine.calls,[])

    def test_downward_bid_defense_emergency(self):
        engine=AuditEngine()
        bid=SimpleNamespace(side=BUY, order_id=105, price_tick=900,
                            cancellable=True)
        engine.clock_ns=2_000_000
        self.assertEqual(cancel_if_needed.py_func(engine,bid,engine.clock_ns,0,
                                                  850,True,False),2)
        self.assertEqual(engine.calls,[(2_000_000,0,105,False)])


if __name__=="__main__":
    unittest.main()
