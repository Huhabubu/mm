#!/usr/bin/env python3
"""Build a new version of the EXISTING private Kaggle notebook from a job spec.

Runs inside GitHub Actions only. The notebook gets one code cell that unpacks a
code bundle (this branch's kaggle/ + listed scripts + frozen collector code) and
runs the job's task with stdout tee'd to /kaggle/working/run.log. Nothing secret
is embedded: Drive credentials are read on Kaggle via the notebook's own
attached User Secrets.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
from pathlib import Path
import tarfile

CELL_TEMPLATE = r'''
import base64, io, json, os, subprocess, sys, tarfile, time
JOB = json.loads(__JOB__)
BUNDLE = "__BUNDLE__"
root = "/tmp/tokyo_l2_code"
os.makedirs(root, exist_ok=True)
with tarfile.open(fileobj=io.BytesIO(base64.b64decode(BUNDLE)), mode="r:gz") as tf:
    try:
        tf.extractall(root, filter="data")
    except TypeError:
        tf.extractall(root)
work = "/kaggle/working"
os.makedirs(work, exist_ok=True)
with open(os.path.join(work, "job_manifest.json"), "w") as fh:
    json.dump(JOB, fh, indent=2)
# Secrets precheck in the notebook kernel process itself (the task runs in a child process).
# Records presence/error only, never values; distinguishes kernel-vs-subprocess env issues.
pre = {"run_type": os.environ.get("KAGGLE_KERNEL_RUN_TYPE"), "checks": {}}
for _name in ("TOKYO_L2_QA_GDRIVE_FOLDER_ID",):
    _tries = []
    for _delay in (0, 5, 10, 20):
        if _delay:
            time.sleep(_delay)
        try:
            from kaggle_secrets import UserSecretsClient
            _ok = bool(UserSecretsClient().get_secret(_name))
            _tries.append({"after_wait_s": _delay, "ok": _ok})
            if _ok:
                break
        except Exception as _exc:
            _tries.append({"after_wait_s": _delay, "ok": False, "error": f"{type(_exc).__name__}: {str(_exc)[:200]}"})
    pre["checks"][_name] = _tries
with open(os.path.join(work, "secrets_precheck.json"), "w") as fh:
    json.dump(pre, fh, indent=2)
print("SECRETS_PRECHECK", json.dumps(pre))
env = dict(os.environ, PYTHONUNBUFFERED="1", TOKYO_L2_CODE_ROOT=root,
           PYTHONPATH=os.pathsep.join([root + "/collector/python", root + "/repo/scripts", root + "/repo"]))
cmd = [sys.executable, "-u", os.path.join(root, "repo", JOB["task"]), *JOB.get("args", [])]
t0 = time.time()
with open(os.path.join(work, "run.log"), "w") as log:
    proc = subprocess.Popen(cmd, cwd=work, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1)
    for line in proc.stdout:
        sys.stdout.write(line)
        log.write(line)
        log.flush()
    rc = proc.wait()
status = {"returncode": rc, "elapsed_s": round(time.time() - t0, 1),
          "finished_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
          "git_sha": JOB.get("git_sha"), "collector_sha": JOB.get("collector_sha"), "task": JOB["task"]}
with open(os.path.join(work, "job_status.json"), "w") as fh:
    json.dump(status, fh, indent=2)
print("JOB_STATUS", json.dumps(status))
if rc != 0:
    raise RuntimeError(f"Kaggle task failed with return code {rc}; diagnostic reports preserved")
'''


def _add_tree(tf: tarfile.TarFile, src: Path, arc: str) -> int:
    n = 0
    for path in sorted(src.rglob("*") if src.is_dir() else [src]):
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc":
            rel = path.relative_to(src) if src.is_dir() else Path(path.name)
            tf.add(path, arcname=f"{arc}/{rel.as_posix()}" if src.is_dir() else arc)
            n += 1
    return n


def build_bundle(repo: Path, collector: Path, files: list[str]) -> tuple[bytes, int]:
    buf = io.BytesIO()
    n = 0
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        n += _add_tree(tf, repo / "kaggle", "repo/kaggle")
        for rel in files:
            if rel.startswith("kaggle/"):
                continue
            p = repo / rel
            if not p.is_file():
                raise FileNotFoundError(rel)
            n += _add_tree(tf, p, f"repo/{rel}")
        n += _add_tree(tf, collector / "collector" / "python", "collector/python")
        n += _add_tree(tf, collector / "collector" / "specs", "collector/specs")
    return buf.getvalue(), n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", type=Path, required=True)
    ap.add_argument("--nb-dir", type=Path, required=True, help="dir from `kaggle kernels pull -m`")
    ap.add_argument("--repo", type=Path, required=True)
    ap.add_argument("--collector", type=Path, required=True)
    ap.add_argument("--git-sha", required=True)
    ap.add_argument("--collector-sha", required=True)
    ap.add_argument("--cred-dataset", default="",
                    help="private Kaggle dataset slug (owner/slug) holding the Drive credential files; "
                         "attached via dataset_sources because API-pushed runs cannot read User Secrets")
    args = ap.parse_args()

    job = json.loads(args.job.read_text(encoding="utf-8"))
    if not str(job.get("task", "")).startswith(("kaggle/", "scripts/")):
        raise ValueError("job.task must be a repo path under kaggle/ or scripts/")
    bundle, n_files = build_bundle(args.repo, args.collector, list(job.get("files", [])) + [job["task"]])
    run_job = {"task": job["task"], "args": [str(a) for a in job.get("args", [])],
               "git_sha": args.git_sha, "collector_sha": args.collector_sha,
               "bundle_sha256": hashlib.sha256(bundle).hexdigest(), "bundle_files": n_files}
    cell = (CELL_TEMPLATE.replace("__JOB__", repr(json.dumps(run_job)))
            .replace("__BUNDLE__", base64.b64encode(bundle).decode("ascii")))
    nb = {"cells": [{"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [],
                     "source": cell.lstrip("\n")}],
          "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                       "language_info": {"name": "python"}},
          "nbformat": 4, "nbformat_minor": 4}

    meta_path = args.nb_dir / "kernel-metadata.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    old = meta.get("code_file") or ""
    code_file = old if old.endswith(".ipynb") else "tokyo-l2-glft.ipynb"
    for p in args.nb_dir.iterdir():
        if p.is_file() and p.name != "kernel-metadata.json":
            p.unlink()
    (args.nb_dir / code_file).write_text(json.dumps(nb), encoding="utf-8")
    meta.update(code_file=code_file, language="python", kernel_type="notebook", is_private=True,
                enable_gpu=False, enable_tpu=False, enable_internet=True)
    if args.cred_dataset:
        sources = list(meta.get("dataset_sources") or [])
        if args.cred_dataset not in sources:
            sources.append(args.cred_dataset)
        meta["dataset_sources"] = sources
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print("NOTEBOOK_BUILT", json.dumps({k: run_job[k] for k in ("task", "args", "git_sha", "collector_sha",
                                                                 "bundle_sha256", "bundle_files")}),
          "bundle_bytes", len(bundle), "kernel_id", meta.get("id"), "dataset_sources", meta.get("dataset_sources"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
