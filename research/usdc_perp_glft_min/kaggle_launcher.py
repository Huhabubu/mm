#!/usr/bin/env python3
"""Build the private Kaggle notebook for the USDC-M perp GLFT minimal run.

Runs in GitHub Actions only. One code cell unpacks a code bundle, installs hftbacktest 2.4.4,
runs kaggle_task.py with stdout tee'd to run.log and writes job_status.json. No secrets embedded.
"""
import argparse
import base64
import hashlib
import io
import json
import tarfile
from pathlib import Path

TASK = "research/usdc_perp_glft_min/kaggle_task.py"
FILES = (TASK, "research/usdc_perp_glft_min/glft_core.py", "research/usdc_perp_glft_min/origin_glft.py",
         "research/usdc_perp_glft_min/basis_moments.py", "research/usdc_perp_glft_min/event_study.py",
         "research/usdc_perp_glft_min/wall_study.py", "research/usdc_perp_glft_min/sweep_study.py",
         "scripts/check_okx_hft_data.py",
         "scripts/inspect_tardis.py", "scripts/tardis_segmented_hbt.py")
HBT_VERSION = "2.4.4"

CELL = r'''
import base64, io, json, os, subprocess, sys, tarfile, time
JOB = json.loads(__JOB__)
root = "/tmp/usdc_glft_code"
os.makedirs(root, exist_ok=True)
with tarfile.open(fileobj=io.BytesIO(base64.b64decode("__BUNDLE__")), mode="r:gz") as tf:
    try:
        tf.extractall(root, filter="data")
    except TypeError:
        tf.extractall(root)
work = "/kaggle/working"
os.makedirs(work, exist_ok=True)
with open(os.path.join(work, "job_manifest.json"), "w") as fh:
    json.dump(JOB, fh, indent=2)
t0 = time.time()
pip = subprocess.run([sys.executable, "-m", "pip", "install", "-q", "hftbacktest==" + JOB["hbt_version"]],
                     capture_output=True, text=True)
ver = subprocess.run([sys.executable, "-c",
                      "import importlib.metadata as m, numba, numpy; print(m.version('hftbacktest'), numba.__version__, numpy.__version__)"],
                     capture_output=True, text=True)
env = dict(os.environ, PYTHONUNBUFFERED="1",
           PYTHONPATH=os.pathsep.join([root + "/research/usdc_perp_glft_min", root + "/scripts"]))
rc = 90
with open(os.path.join(work, "run.log"), "w") as log:
    log.write("PIP rc=%d\n%s\n%s\n" % (pip.returncode, pip.stdout[-3000:], pip.stderr[-3000:]))
    log.write("VERSIONS hftbacktest numba numpy: " + ver.stdout + ver.stderr + "\n")
    log.flush()
    if pip.returncode == 0:
        cmd = [sys.executable, "-u", os.path.join(root, JOB["task"]), *JOB["args"]]
        proc = subprocess.Popen(cmd, cwd=work, env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout:
            sys.stdout.write(line)
            log.write(line)
            log.flush()
        rc = proc.wait()
status = {"returncode": rc, "pip_returncode": pip.returncode, "versions": ver.stdout.strip(),
          "elapsed_s": round(time.time() - t0, 1),
          "finished_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          "git_sha": JOB["git_sha"], "bundle_sha256": JOB["bundle_sha256"], "task": JOB["task"], "args": JOB["args"]}
with open(os.path.join(work, "job_status.json"), "w") as fh:
    json.dump(status, fh, indent=2)
print("JOB_STATUS", json.dumps(status))
if rc != 0:
    raise RuntimeError(f"task failed with return code {rc}; reports preserved")
'''


def build_bundle(repo: Path) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for rel in FILES:
            path = repo / rel
            if not path.is_file():
                raise FileNotFoundError(rel)
            tf.add(path, arcname=rel)
    return buf.getvalue()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nb-dir", type=Path, required=True)
    ap.add_argument("--repo", type=Path, required=True)
    ap.add_argument("--kernel-id", required=True, help="owner/slug")
    ap.add_argument("--git-sha", required=True)
    ap.add_argument("--day", default="2026-11-01")
    ap.add_argument("--hours", default="24")
    ap.add_argument("--chunk-minutes", default="60")
    args = ap.parse_args()
    owner, slug = args.kernel_id.split("/")
    bundle = build_bundle(args.repo)
    job = {"task": TASK, "args": ["--day", args.day, "--hours", str(args.hours),
                                  "--chunk-minutes", str(args.chunk_minutes)],
           "git_sha": args.git_sha, "hbt_version": HBT_VERSION,
           "bundle_sha256": hashlib.sha256(bundle).hexdigest(), "bundle_files": list(FILES)}
    cell = (CELL.replace("__JOB__", repr(json.dumps(job)))
            .replace("__BUNDLE__", base64.b64encode(bundle).decode("ascii")))
    nb = {"cells": [{"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [],
                     "source": cell.lstrip("\n")}],
          "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                       "language_info": {"name": "python"}},
          "nbformat": 4, "nbformat_minor": 4}
    args.nb_dir.mkdir(parents=True, exist_ok=True)
    code_file = f"{slug}.ipynb"
    (args.nb_dir / code_file).write_text(json.dumps(nb), encoding="utf-8")
    meta = {"id": f"{owner}/{slug}", "title": slug, "code_file": code_file, "language": "python",
            "kernel_type": "notebook", "is_private": True, "enable_gpu": False, "enable_tpu": False,
            "enable_internet": True, "dataset_sources": [], "competition_sources": [],
            "kernel_sources": [], "model_sources": []}
    (args.nb_dir / "kernel-metadata.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print("NOTEBOOK_BUILT", json.dumps({k: job[k] for k in ("task", "args", "git_sha", "hbt_version", "bundle_sha256")}),
          "bundle_bytes", len(bundle), "kernel_id", meta["id"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
