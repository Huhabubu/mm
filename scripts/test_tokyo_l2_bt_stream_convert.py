"""Deterministic synthetic checks for scripts/tokyo_l2_bt_stream_convert.py.

Runs on GitHub Actions with the frozen collector modules (collector/python @ COLLECTOR_SHA) and
hftbacktest 2.4.4 on PYTHONPATH. No network and no real market data.
"""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

import numpy as np
import zstandard as zstd
from hftbacktest import (DEPTH_CLEAR_EVENT, DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT, EXCH_EVENT, LOCAL_EVENT,
                         TRADE_EVENT)

import tokyo_l2_bt_stream_convert as sc

DATE = "2026-11-23"
YMD = DATE.replace("-", "")
NS = sc.NS
D0 = int(datetime(2026, 11, 23, tzinfo=timezone.utc).timestamp()) * NS
P = 15 * 60 * NS
ODD = 123_456_789  # sub-millisecond nanoseconds that float64 cannot keep at ~1.8e18 ns
TAB, NL = "\t", "\n"


def ms(wall):
    return wall // 1_000_000 - 2


def bn_bbo(wall, bid, ask, sym="BTCUSDT"):
    return {"stream": sym.lower() + "@bookTicker",
            "data": {"e": "bookTicker", "s": sym, "b": str(bid), "B": "1.5", "a": str(ask), "A": "2.5",
                     "T": ms(wall), "E": ms(wall)}}


def obu(wall, u, parent=None, bids=(), asks=(), sym="BTC_USDT"):
    res = {"t": ms(wall), "s": "ob." + sym + ".400", "u": u,
           "b": [[str(p), str(q)] for p, q in bids], "a": [[str(p), str(q)] for p, q in asks]}
    if parent is None:
        res["full"] = True
    else:
        res["U"] = parent
    return {"channel": "futures.obu", "event": "update", "result": res}


def trades(wall, *items, cms=None):
    cms = ms(wall) if cms is None else cms
    return {"channel": "futures.trades", "event": "update", "time_ms": ms(wall),
            "result": [{"contract": c, "id": i, "price": str(px), "size": sz, "create_time_ms": cms}
                       for c, i, px, sz in items]}


def ticker(wall):
    return {"channel": "futures.book_ticker", "event": "update",
            "result": {"s": "BTC_USDT", "t": ms(wall), "b": "100.0", "B": 5, "a": "100.5", "A": 4}}


def meta(event, **kw):
    return {"event": event, "symbol": "ALL", **kw}


class Writer:
    """One exchange's sealed partitions with a continuous rx_seq and receive clock (one boot)."""

    def __init__(self, root, exchange):
        self.root, self.exchange = Path(root), exchange
        self.rx, self.lines, self.first_rx = 0, [], None

    def add(self, wall, payload, kind="DATA", conn="A", epoch=1):
        self.rx += 1
        self.first_rx = self.rx if self.first_rx is None else self.first_rx
        raw = json.dumps(payload, separators=(",", ":"))
        if kind == "DATA_ESCAPED":
            raw = json.dumps(raw)
        self.lines.append(TAB.join(str(v) for v in (wall, wall - D0 + NS, conn, epoch, self.rx, kind, raw)))

    def seal(self, minute):
        name = f"{self.exchange}_{DATE}T00{minute:02d}_15m_boot1.tsv.zst"
        folder = self.root / "raw" / self.exchange / DATE
        folder.mkdir(parents=True, exist_ok=True)
        blob = zstd.ZstdCompressor().compress((NL.join(self.lines) + NL).encode())
        (folder / name).write_bytes(blob)
        mf = {"exchange": self.exchange, "date_utc": DATE, "hour_utc": "00", "minute_utc": f"{minute:02d}",
              "partition_minutes": 15, "file_name": name, "boot_id": "boot1",
              "start_wall_ns": D0 + minute * 60 * NS, "first_rx_seq": self.first_rx,
              "bytes": len(blob), "sha256": hashlib.sha256(blob).hexdigest()}
        (folder / name.replace(".tsv.zst", ".manifest.json")).write_text(json.dumps(mf), encoding="utf-8")
        self.lines, self.first_rx = [], None


def two_partitions(root, next_parent=107):
    """BTC 00:00-00:15 and 00:15-00:30; the Gate book is anchored once, in the first partition."""
    bn, gt = Writer(root, sc.LEADER), Writer(root, sc.FOLLOWER)
    bn.add(D0 + NS + ODD, bn_bbo(D0 + NS + ODD, 100.1, 100.2))
    bn.add(D0 + 2 * NS, bn_bbo(D0 + 2 * NS, 3000.1, 3000.2, "ETHUSDT"))
    bn.add(D0 + 3 * NS, {"stream": "btcusdt@depth@0ms", "data": {"e": "depthUpdate", "s": "BTCUSDT"}})
    bn.add(D0 + 4 * NS, {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "s": "BTCUSDT"}})
    bn.add(D0 + 5 * NS, meta("REST_SNAPSHOT", symbol="BTCUSDT"), kind="META")
    bn.add(D0 + 800 * NS, bn_bbo(D0 + 800 * NS, 100.2, 100.3))
    bn.seal(0)
    bn.add(D0 + P + NS + ODD, bn_bbo(D0 + P + NS + ODD, 100.3, 100.4))
    bn.seal(15)

    gt.add(D0 + NS, obu(D0 + NS, 95, 90, bids=[(100.0, 1)]))
    gt.add(D0 + 2 * NS + ODD, obu(D0 + 2 * NS + ODD, 100, bids=[(100.0, 5), (99.5, 3)],
                                  asks=[(100.5, 4), (101.0, 2)]))
    gt.add(D0 + 3 * NS, obu(D0 + 3 * NS, 104, 101, bids=[(100.0, 0), (99.8, 1)]))
    gt.add(D0 + 3 * NS + NS // 2, obu(D0 + 3 * NS + NS // 2, 7, bids=[(3000.0, 1)], asks=[(3001.0, 1)],
                                      sym="ETH_USDT"))
    gt.add(D0 + 4 * NS, trades(D0 + 4 * NS, ("BTC_USDT", 1, 100.2, 3), ("ETH_USDT", 2, 3000.5, 1)))
    gt.add(D0 + 5 * NS, trades(D0 + 5 * NS, ("ETH_USDT", 3, 3000.6, 1), ("BTC_USDT", 4, 100.3, -2)))
    gt.add(D0 + 6 * NS, ticker(D0 + 6 * NS))
    gt.add(D0 + 7 * NS, obu(D0 + 7 * NS, 106, 105, asks=[(100.5, 6)]), kind="DATA_ESCAPED")
    gt.seal(0)
    gt.add(D0 + P + NS + ODD, obu(D0 + P + NS + ODD, 110, next_parent, bids=[(99.9, 2)]))
    gt.add(D0 + P + 2 * NS, trades(D0 + P + 2 * NS, ("BTC_USDT", 5, 100.4, 1)))
    gt.seal(15)


def gate_trades_case(root, tail):
    bn = Writer(root, sc.LEADER)
    bn.add(D0 + NS, bn_bbo(D0 + NS, 100.1, 100.2))
    bn.seal(0)
    gt = Writer(root, sc.FOLLOWER)
    gt.add(D0 + NS, obu(D0 + NS, 100, bids=[(100.0, 5)], asks=[(100.5, 4)]))
    gt.add(D0 + 2 * NS, trades(D0 + 2 * NS, ("BTC_USDT", 1, 100.2, 3), ("BTC_USDT", 2, 100.3, 1)))
    tail(gt)
    gt.seal(0)


def handover(gt, price=100.3):
    gt.add(D0 + 3 * NS, meta("SWITCH"), kind="META", conn="B")
    gt.add(D0 + 3 * NS + NS // 2, trades(D0 + 3 * NS + NS // 2, ("BTC_USDT", 2, price, 1),
                                         ("BTC_USDT", 3, 100.4, 1), cms=ms(D0 + 2 * NS)), conn="B")


def repeat_same_conn(gt):
    gt.add(D0 + 12 * NS, trades(D0 + 12 * NS, ("BTC_USDT", 1, 100.2, 3), cms=ms(D0 + 2 * NS)))


def exch_rows(hbt):
    return hbt[(hbt["ev"].astype(np.uint64) & np.uint64(EXCH_EVENT)) != 0]


def exch_kinds(hbt):
    rows = exch_rows(hbt)
    return Counter(int(k) for k in (rows["ev"].astype(np.uint64) & np.uint64(0xFF)).tolist())


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="tl2_stream_"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_conv(self, fetched, tag, finalize=True):
        out = self.tmp / f"out_{tag}"
        args = ["--fetched", str(fetched), "--gate-symbol", "BTC_USDT", "--state", str(self.tmp / f"state_{tag}"),
                "--label-date", DATE]
        if finalize:
            args += ["--finalize", "--out", str(out)]
        rc = sc.main(args)
        report = json.loads((out / "stream_report.json").read_text(encoding="utf-8")) if finalize else None
        return rc, report, out

    def load_hbt(self, out):
        with np.load(out / f"gate_futures_l2_BTC_{YMD}.npz") as z:
            return z["data"]


class TwoPartitionTests(Base):
    def test_book_continues_across_partition_boundary(self):
        two_partitions(self.tmp / "f")
        rc, rep, out = self.run_conv(self.tmp / "f", "a")
        self.assertEqual((rc, rep["status"], rep["failed_items"]), (0, "ok", []))
        hbt = self.load_hbt(out)
        kinds = exch_kinds(hbt)
        self.assertEqual(kinds[DEPTH_CLEAR_EVENT], 1)
        self.assertEqual(kinds[DEPTH_SNAPSHOT_EVENT], 4)
        self.assertEqual(kinds[DEPTH_EVENT], 4)
        self.assertEqual(kinds[TRADE_EVENT], 3)
        trade_rows = exch_rows(hbt)
        trade_rows = trade_rows[(trade_rows["ev"].astype(np.uint64) & np.uint64(0xFF)) == TRADE_EVENT]
        self.assertEqual(sorted(trade_rows["px"].tolist()), [100.2, 100.3, 100.4])
        gate_parts = [p for p in rep["parts"] if p["exchange"] == sc.FOLLOWER]
        self.assertEqual(len(gate_parts), 2)
        self.assertEqual(gate_parts[1]["book_before"], gate_parts[0]["book_after"])
        before = gate_parts[1]["book_before"]
        self.assertEqual((before["anchor"]["seq"], before["best_bid"], before["best_ask"]), (106, 99.8, 100.5))
        self.assertFalse([k for k in gate_parts[1]["rows"]["hbt_types"] if k.split("|")[0] in ("snapshot", "clear")])
        self.assertEqual(rep["initialization_exclusion"]["frames"], 1)

    def test_timestamps_stay_int64_and_events_stay_ordered(self):
        two_partitions(self.tmp / "f")
        rc, _, out = self.run_conv(self.tmp / "f", "a")
        self.assertEqual(rc, 0)
        hbt = self.load_hbt(out)
        with np.load(out / f"binance_bbo_BTC_{YMD}.npz") as z:
            bbo_ts = z["local_ts_ns"]
        self.assertEqual(hbt["local_ts"].dtype, np.dtype(np.int64))
        self.assertEqual(hbt["exch_ts"].dtype, np.dtype(np.int64))
        self.assertEqual(bbo_ts.dtype, np.dtype(np.int64))
        self.assertEqual(bbo_ts.tolist(), [D0 + NS + ODD, D0 + 800 * NS, D0 + P + NS + ODD])
        local_ts = set(hbt["local_ts"].tolist())
        self.assertIn(D0 + 2 * NS + ODD, local_ts)
        self.assertIn(D0 + P + NS + ODD, local_ts)
        self.assertNotEqual(int(np.float64(D0 + NS + ODD)), D0 + NS + ODD)
        ev = hbt["ev"].astype(np.uint64)
        for flag, col in ((LOCAL_EVENT, "local_ts"), (EXCH_EVENT, "exch_ts")):
            ts = hbt[col][(ev & np.uint64(flag)) != 0]
            self.assertTrue(bool(np.all(np.diff(ts) >= 0)), col)

    def test_filters_other_symbols_and_binance_non_bbo_streams(self):
        two_partitions(self.tmp / "f")
        rc, rep, _ = self.run_conv(self.tmp / "f", "a")
        self.assertEqual(rc, 0)
        lead, gate = rep["records"][sc.LEADER], rep["records"][sc.FOLLOWER]
        self.assertEqual((lead["bbo"], lead["dropped_prefilter"], lead["meta"]), (3, 3, 1))
        self.assertEqual(len(rep["binance_depth_meta"]), 1)
        self.assertEqual((gate["obu_pre_first_snapshot"], gate["obu_snapshot"], gate["obu_delta"],
                          gate["trade_frames"], gate["gate_mixed_symbol_trade_messages"], gate["l1"],
                          gate["dropped_prefilter"], gate["trades_emitted"]), (1, 1, 3, 3, 2, 1, 1, 3))

    def test_sequence_break_at_boundary_blocks_instead_of_reanchoring(self):
        two_partitions(self.tmp / "f", next_parent=108)
        rc, rep, _ = self.run_conv(self.tmp / "f", "b")
        self.assertEqual((rc, rep["status"]), (2, "blocked"))
        self.assertTrue(any(f.startswith("gate_depth_unrebuildable") for f in rep["failed_items"]))
        self.assertFalse(rep["artifacts"])

    def test_checkpoint_resume_matches_single_run(self):
        full = self.tmp / "f"
        two_partitions(full)
        first = self.tmp / "first_half"
        shutil.copytree(full, first)
        for f in list(first.rglob("*T0015*")):
            f.unlink()
        self.assertEqual(self.run_conv(first, "r", finalize=False)[0], 0)
        rc_r, rep_r, out_r = self.run_conv(full, "r")
        rc_s, rep_s, out_s = self.run_conv(full, "s")
        self.assertEqual((rc_r, rc_s), (0, 0))
        self.assertEqual(len(rep_r["parts"]), 4)
        self.assertEqual(rep_r["records"], rep_s["records"])
        self.assertEqual([a["path"] for a in rep_r["artifacts"]], [a["path"] for a in rep_s["artifacts"]])
        for art in rep_s["artifacts"]:
            with np.load(out_r / art["path"]) as zr, np.load(out_s / art["path"]) as zs:
                self.assertEqual(sorted(zr.files), sorted(zs.files))
                for key in zs.files:
                    np.testing.assert_array_equal(zr[key], zs[key])


class IntegrityTests(Base):
    def test_tampered_partition_fails_and_keeps_earlier_checkpoint(self):
        f = self.tmp / "f"
        two_partitions(f)
        bad = next(f.rglob(f"{sc.FOLLOWER}_*T0015*.tsv.zst"))
        line = TAB.join(str(v) for v in (D0 + P + NS, NS, "A", 1, 99, "DATA", "{}"))
        bad.write_bytes(zstd.ZstdCompressor().compress((line + NL).encode()))
        with self.assertRaisesRegex(ValueError, "mismatch"):
            self.run_conv(f, "t")
        prog = json.loads((self.tmp / "state_t" / sc.PROGRESS_FILE).read_text(encoding="utf-8"))
        self.assertEqual(len(prog["parts"]), 3)

    def test_partitions_must_arrive_in_order(self):
        f = self.tmp / "f"
        two_partitions(f)
        gate = [p for p in sc.discover_partitions(f) if p[0] == sc.FOLLOWER]
        conv = sc.StreamConverter("BTC_USDT")
        conv.process_partition(gate[1][1], gate[1][2], self.tmp / "parts")
        with self.assertRaisesRegex(ValueError, "not after"):
            conv.process_partition(gate[0][1], gate[0][2], self.tmp / "parts")


class TradeDedupTests(Base):
    def test_identical_copy_on_new_connection_after_switch_is_overlap(self):
        gate_trades_case(self.tmp / "f", handover)
        rc, rep, out = self.run_conv(self.tmp / "f", "h")
        self.assertEqual((rc, rep["status"]), (0, "ok"))
        self.assertEqual(rep["trade_dedup"].get("connection_overlap", 0), 1)
        self.assertEqual(exch_kinds(self.load_hbt(out))[TRADE_EVENT], 3)

    def test_same_connection_repeat_blocks(self):
        gate_trades_case(self.tmp / "f", repeat_same_conn)
        rc, rep, _ = self.run_conv(self.tmp / "f", "d")
        self.assertEqual((rc, rep["status"]), (2, "blocked"))
        self.assertEqual(rep["trade_dedup"].get("duplicate_trade_id", 0), 1)
        self.assertIn("gate_unexplained_duplicate_trades:1", rep["failed_items"])

    def test_conflicting_copy_blocks(self):
        gate_trades_case(self.tmp / "f", lambda gt: handover(gt, price=100.9))
        rc, rep, _ = self.run_conv(self.tmp / "f", "c")
        self.assertEqual((rc, rep["status"]), (2, "blocked"))
        self.assertEqual(rep["trade_dedup"].get("conflicting_duplicate_trade", 0), 1)


class RowSinkTests(unittest.TestCase):
    def test_chunks_keep_int64_and_order(self):
        sink = sc.RowSink(np.dtype([("t", "<i8")]), chunk_rows=3)
        vals = [D0 + i * ODD + 1 for i in range(7)]
        for v in vals:
            sink.append((v,))
        self.assertEqual(len(sink), 7)
        self.assertEqual(sink.drain()["t"].tolist(), vals)
        self.assertTrue(sink.is_drained())
        self.assertEqual(len(sink), 7)


if __name__ == "__main__":
    unittest.main()