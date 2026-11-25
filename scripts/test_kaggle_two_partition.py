"""Stage-3 acceptance failures and independent raw audit, run only on Actions."""
import base64
from copy import deepcopy
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
from hftbacktest import BUY_EVENT, TRADE_EVENT
import tokyo_l2_bt_stream_convert as sc
from test_tokyo_l2_bt_stream_convert import two_partitions, DATE, D0, NS

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "kaggle" / "tasks"))
sys.path.insert(0, str(ROOT / "kaggle"))
import two_partition as tp
import stage3_audit as audit
import launcher


def passing_stages():
    results = {
        "fetch": {"integrity": {"status": "passed"}, "sample_integrity": {"status": "passed"},
                  "protocol": {"passed": True, "problems": []}},
        "convert_new": {"single_report": {"status": "ok"}},
        "compare": {"resume_identical": True, "old_new_identical": False,
                    "hbt_detail": {"new": {"validate_event_order": "ok"}}},
        "raw_audit": {"passed": True, "old_new_difference_explained": True, "checks": {"raw": True}},
        "glft_replay": {"passed": True},
    }
    return {name: {"ok": True, "result": results.get(name, {})} for name in tp.STAGES}


class AcceptanceTests(unittest.TestCase):
    def test_all_required_gates_and_fail_closed_for_missing_stages(self):
        good = passing_stages()
        self.assertTrue(tp.acceptance(good)["stage3_accepted"])
        for name in tp.STAGES:
            with self.subTest(stage=name):
                bad = deepcopy(good)
                del bad[name]
                self.assertFalse(tp.acceptance(bad)["stage3_accepted"])
        mutations = [
            ("fetch", lambda r: r["sample_integrity"].update(status="failed")),
            ("fetch", lambda r: r["protocol"].update(passed=False)),
            ("raw_audit", lambda r: r.update(passed=False)),
            ("raw_audit", lambda r: r.update(old_new_difference_explained=False)),
            ("glft_replay", lambda r: r.update(passed=False)),
            ("convert_new", lambda r: r["single_report"].update(status="blocked")),
            ("compare", lambda r: r.update(resume_identical=False)),
        ]
        for name, mutate in mutations:
            bad = deepcopy(good)
            mutate(bad[name]["result"])
            self.assertFalse(tp.acceptance(bad)["stage3_accepted"], name)

    def test_day_integrity_and_stage4_authorization_are_separate(self):
        stages = passing_stages()
        stages["fetch"]["result"]["integrity"]["status"] = "failed"
        verdict = tp.acceptance(stages)
        self.assertTrue(verdict["stage3_accepted"])
        self.assertFalse(verdict["full_day_integrity_passed"])
        self.assertFalse(verdict["stage4_authorized"])

    def test_glft_exception_is_not_success(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(tp, "REPORT", Path(tmp) / "r.json"), \
                patch.object(tp, "WORK", Path(tmp)), patch.object(tp, "SCRATCH", Path(tmp)), \
                patch.object(tp.shutil, "disk_usage", return_value=shutil.disk_usage(tmp)), \
                patch.object(tp, "report", {"stages": {}}):
            def broken():
                raise ModuleNotFoundError("missing dependency")
            self.assertFalse(tp.stage("glft_replay", broken)["ok"])

    def test_child_failure_propagates(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "result.json"
            with patch.object(tp, "child_new", return_value={"rc": 2}):
                self.assertEqual(tp.child_main("new", "{}", str(path)), 2)
            self.assertFalse(json.loads(path.read_text())["ok"])
            with patch.object(tp, "SCRATCH", Path(tmp)):
                def failed(cmd, **kwargs):
                    Path(cmd[-1]).write_text(json.dumps({"ok": True, "rc": 0}))
                    return subprocess.CompletedProcess(cmd, 7)
                with patch.object(tp.subprocess, "run", side_effect=failed):
                    with self.assertRaisesRegex(RuntimeError, "rc=7"):
                        tp.run_child("new", {})

    def test_raw_event_comparison_preserves_multiplicity_and_earliest_time(self):
        a = (TRADE_EVENT, BUY_EVENT, 100, 105, 1.0, 2.0, 0)
        b = (TRADE_EVENT, BUY_EVENT, 100, 109, 1.0, 2.0, 0)
        self.assertTrue(audit.rows_equal([a, b], [b, a]))
        self.assertFalse(audit.rows_equal([a, b], [a, a]))
        self.assertFalse(audit.rows_equal([a], [b]))

    def test_nanosecond_stats(self):
        ts = np.array([1795392000414187372, 1795392000427894177], dtype=np.int64)
        old = ts.astype(np.float64).astype(np.int64)
        self.assertGreater(audit.error_stats(old, ts)["max_abs_ns"], 0)
        self.assertEqual(audit.error_stats(ts, ts)["nonzero_fraction"], 0)

    def test_protocol_stale_failed_or_wrong_inputs_cannot_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            window = "2026-11-23T00:00Z"
            path = Path(tmp) / "2026-11-23T0000Z.json"
            part = {"exchange": "gate_futures", "window_id": window, "data_sha256": "raw",
                    "manifest_sha256": "manifest"}
            q = {"run_id": "run", "window_id": window, "checker": "current/rules", "status": "passed",
                 "protocol_status": "passed", "target_inputs": [part], "market_availability": []}
            path.write_text(json.dumps(q))
            self.assertTrue(audit.protocol_evidence(tmp, [window], [part], "run", "current")["passed"])
            for update in ({"checker": "old/rules"}, {"status": "data_failed"},
                           {"target_inputs": []}, {"run_id": "wrong"},
                           {"replay": {"gate_futures": {"status": "blocked"}}}):
                path.write_text(json.dumps({**q, **update}))
                self.assertFalse(audit.protocol_evidence(tmp, [window], [part], "run", "current")["passed"])

    def test_actual_packaged_import_closure(self):
        job = json.loads((ROOT / "kaggle" / "job.json").read_text())
        import qa_convert
        collector = Path(qa_convert.__file__).resolve().parents[2]
        bundle, _ = launcher.build_bundle(ROOT, collector, job["files"])
        with tempfile.TemporaryDirectory() as tmp:
            with tarfile.open(fileobj=io.BytesIO(bundle), mode="r:gz") as tf:
                tf.extractall(tmp, filter="data")
            env = dict(os.environ, PYTHONPATH=os.pathsep.join([
                str(Path(tmp) / "repo" / "scripts"), str(Path(tmp) / "collector" / "python")]))
            r = subprocess.run([sys.executable, "-c",
                                "from core_glft_engine import run_core_glft; from qa_convert import run_offline_e2e_hbt_replay"],
                               cwd=tmp, env=env, capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)

    def test_notebook_task_failure_preserves_status_and_exits_nonzero(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w:gz") as tf:
                content = b"raise SystemExit(3)\n"
                info = tarfile.TarInfo("repo/kaggle/tasks/fail.py")
                info.size = len(content)
                tf.addfile(info, io.BytesIO(content))
            job = {"task": "kaggle/tasks/fail.py", "args": [], "git_sha": "test"}
            cell = launcher.CELL_TEMPLATE.replace("__JOB__", repr(json.dumps(job))).replace(
                "__BUNDLE__", base64.b64encode(buf.getvalue()).decode())
            cell = cell.replace('root = "/tmp/tokyo_l2_code"', "root = " + repr(str(tmp / "code")))
            cell = cell.replace('work = "/kaggle/working"', "work = " + repr(str(tmp / "output")))
            # Mock only the external secret client, not the notebook's subprocess/exit handling.
            prefix = "import sys,types\nm=types.ModuleType('kaggle_secrets')\n"
            prefix += "m.UserSecretsClient=lambda: types.SimpleNamespace(get_secret=lambda name: 'fixture')\n"
            prefix += "sys.modules['kaggle_secrets']=m\n"
            r = subprocess.run([sys.executable, "-c", prefix + cell], capture_output=True, text=True)
            self.assertNotEqual(r.returncode, 0)
            self.assertEqual(json.loads((tmp / "output" / "job_status.json").read_text())["returncode"], 3)


class RawAuditIntegration(unittest.TestCase):
    def test_raw_depth_quotes_trades_and_recovery_against_converter(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fetched, out, old = root / "f", root / "out", root / "old"
            two_partitions(fetched)
            rc = sc.main(["--fetched", str(fetched), "--gate-symbol", "BTC_USDT",
                          "--state", str(root / "state"), "--label-date", DATE, "--finalize", "--out", str(out)])
            self.assertEqual(rc, 0)
            report = json.loads((out / "stream_report.json").read_text())
            parts = []
            for ex, data, manifest, _ in sc.discover_partitions(fetched):
                parts.append({"exchange": ex, "data": str(data), "manifest": str(manifest),
                              "slot": 0 if "T0000" in data.name else 1})
            old.mkdir()
            names = tp.artifact_names()
            for key in ("binance_bbo", "gate_l1_bbo"):
                with np.load(out / names[key]) as z:
                    values = dict(z)
                for ts in ("local_ts_ns", "exch_ts_ns"):
                    values[ts] = values[ts].astype(np.float64).astype(np.int64)
                np.savez_compressed(old / names[key], **values)
            calendar = [{"exchange": "gate_futures", "unrebuildable_depth_intervals": [
                {"reason": "missing_same_connection_anchor", "excluded_frames": 1,
                 "start_wall_ns": D0 + NS, "end_wall_ns": D0 + NS}]}]
            protocol = {"execution_unavailable_intervals": [
                {"exchange": "gate_futures", "channel": "futures.obu", "end_wall_ns": D0 + 3 * NS}]}
            result = audit.audit(parts, out, old, names, report, calendar, protocol)
            self.assertTrue(result["passed"], result["checks"])
            self.assertEqual(result["gate_trades"]["unique_ids"], 3)


if __name__ == "__main__":
    unittest.main()
