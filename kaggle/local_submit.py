#!/usr/bin/env python3
"""Prepare an existing private Kaggle notebook from a clean Git snapshot.
Only --submit starts execution. Uses locally configured Kaggle credentials.
"""
from __future__ import annotations
import argparse
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

COLLECTOR_SHA = "78f7a69ac070cdb1d823e93a871b0801c4a1e3b7"

def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args])

def archive(root, ref, dest, paths):
    if not paths:
        return
    dest.mkdir(parents=True, exist_ok=True)
    blob = git(root, "archive", "--format=tar", ref, "--", *paths)
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:") as tf:
        tf.extractall(dest, filter="data")

def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

def selected_dates(repo, job):
    args = list(job.get("args", []))
    if "--batch-config" in args:
        batch = read_json(repo / args[args.index("--batch-config") + 1])
        paths = [x["config"] for x in batch["experiments"]]
    else:
        paths = [args[args.index("--config") + 1] if "--config" in args else "configs/tokyo_l2_20261123_gate_btc.json"]
    dates = set()
    for p in paths:
        cfg = read_json(repo / p)
        for role in ("eval_dates", "train_dates", "holdout_dates"):
            dates.update(cfg.get("dates", {}).get(role, []))
    if not dates:
        raise ValueError("Explicit experiment dates required")
    return sorted(dates)

def stage_qa(root, qa_ref, rules_ref, repo, scratch, dates):
    qa_sha = git(root, "rev-parse", qa_ref).decode().strip()
    rules_sha = git(root, "rev-parse", rules_ref).decode().strip()
    qa_paths = git(root, "ls-tree", "-r", "--name-only", qa_sha, "--", "qa_reports").decode().splitlines()
    selected = [p for p in qa_paths if any(d in p for d in dates) and
                p.startswith(("qa_reports/days/", "qa_reports/windows/", "qa_reports/protocol/"))]
    qa = scratch / "qa_state"
    archive(root, qa_sha, qa, selected)
    lines = []
    for name in ("protocol", "replay", "recurring"):
        p = f"scripts/tokyo_l2_{name}_qa.py"
        content = git(root, "show", f"{rules_sha}:{p}")
        lines.append(f"{hashlib.sha256(content).hexdigest()}  workflow_code/{p}\n")
    provenance = {"qa_state_sha": qa_sha, "qa_rules_main_sha": rules_sha,
                  "expected_protocol_rules_sha": hashlib.sha256("".join(lines).encode()).hexdigest(),
                  "window_archive_revisions": {}, "protocol_files": [], "missing_protocol_windows": []}
    for d in dates:
        hits = sorted((qa / "qa_reports/days").glob(f"*/{d}_p15m.day.qa.json"))
        if len(hits) != 1:
            raise ValueError(f"need exactly one QA day report for {d}; found {len(hits)}")
        day = read_json(hits[0])
        run_id = str(day["run_id"])
        dst = repo / "kaggle/qa_days" / hits[0].parent.name / hits[0].name
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(hits[0], dst)
        for h in range(24):
            for m in (0, 15, 30, 45):
                w = f"{d}T{h:02d}{m:02d}Z"
                whits = sorted((qa / "qa_reports/windows").glob(f"*/{d}/{w}__*.window.qa.json"))
                whits = [p for p in whits if str(read_json(p).get("run_id") or run_id) == run_id]
                phits = sorted((qa / "qa_reports/protocol").glob(f"*/15m/{w}.json"))
                phits = [p for p in phits if str(read_json(p).get("run_id") or run_id) == run_id]
                if len(whits) != 1 or len(phits) != 1:
                    provenance["missing_protocol_windows"].append(w)
                    continue
                data = read_json(whits[0])
                if not data.get("window_id") or not data.get("archive_revision"):
                    provenance["missing_protocol_windows"].append(w)
                    continue
                provenance["window_archive_revisions"][data["window_id"]] = data["archive_revision"]
                pdst = repo / "kaggle/qa_protocol" / phits[0].name
                pdst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(phits[0], pdst)
                provenance["protocol_files"].append(str(phits[0].relative_to(qa)))
    write_json(repo / "kaggle/qa_protocol/provenance.json", provenance)
    return provenance

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--job", type=Path, default=Path("kaggle/job.json"))
    p.add_argument("--notebook", required=True)
    p.add_argument("--cred-dataset", required=True)
    p.add_argument("--nb-dir", type=Path, required=True)
    p.add_argument("--qa-ref", default="origin/tokyo-l2-qa-state")
    p.add_argument("--rules-ref", default="origin/main")
    p.add_argument("--submit", action="store_true")
    args = p.parse_args()
    root = Path(git(Path.cwd(), "rev-parse", "--show-toplevel").decode().strip())
    head = git(root, "rev-parse", "HEAD").decode().strip()
    job = read_json(args.job.resolve())
    if job.get("task") != "kaggle/tasks/unified_backtest.py":
        raise ValueError("only unified research task supported")
    if args.nb_dir.exists() and any(args.nb_dir.iterdir()):
        raise ValueError("--nb-dir must be empty")
    args.nb_dir.mkdir(parents=True, exist_ok=True)
    nb_dir = args.nb_dir.resolve()
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()
    if "/" not in args.cred_dataset or "/" not in args.notebook:
        raise ValueError("use exact owner/slug identifiers")
    hits = [d for d in api.dataset_list(mine=True, search=args.cred_dataset.split("/",1)[1])
            if str(getattr(d, "ref", "")) == args.cred_dataset]
    if len(hits) != 1 or getattr(hits[0], "is_private", getattr(hits[0], "isPrivate", None)) is not True:
        raise RuntimeError("cannot confirm credential dataset is private")
    subprocess.run(["kaggle", "kernels", "pull", args.notebook, "-p", str(nb_dir), "-m"], check=True)
    meta = read_json(nb_dir / "kernel-metadata.json")
    if meta.get("is_private") is not True or meta.get("id") != args.notebook:
        raise RuntimeError("cannot confirm exact existing private notebook")
    with tempfile.TemporaryDirectory(prefix="paper-kaggle-") as tmp:
        scratch = Path(tmp)
        repo = scratch / "repo"
        collector = scratch / "collector"
        archive(root, head, repo, ["scripts", "configs", "kaggle"])
        archive(root, COLLECTOR_SHA, collector, ["collector/python", "collector/specs"])
        for f in job.get("files", []):
            path = (repo / f).resolve()
            if not path.is_relative_to(repo.resolve()) or not path.is_file():
                raise ValueError(f"invalid/uncommitted job input: {f}")
        dates = selected_dates(repo, job)
        prov = stage_qa(root, args.qa_ref, args.rules_ref, repo, scratch, dates)
        print("QA snapshot:", prov["qa_state_sha"], "missing protocol windows:", len(prov["missing_protocol_windows"]))
        local_job = scratch / "job.json"
        write_json(local_job, job)
        subprocess.run([sys.executable, str(repo / "kaggle/launcher.py"), "--job", str(local_job),
                        "--nb-dir", str(nb_dir), "--repo", str(repo), "--collector", str(collector),
                        "--git-sha", head, "--collector-sha", COLLECTOR_SHA,
                        "--cred-dataset", args.cred_dataset], check=True)
    if args.submit:
        status = subprocess.check_output(["kaggle", "kernels", "status", args.notebook], text=True).lower()
        if "running" in status or "queued" in status:
            raise RuntimeError("notebook already running; refusing to replace")
        subprocess.run(["kaggle", "kernels", "push", "-p", str(nb_dir)], check=True)
    else:
        print("Prepared only; no Kaggle job submitted.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
