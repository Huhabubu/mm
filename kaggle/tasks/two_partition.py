#!/usr/bin/env python3
'''Stage 3 on Kaggle: two adjacent real partitions, frozen converter vs streaming converter.

BTC, 2026-11-23 UTC windows 00:00-00:15 and 00:15-00:30: Binance bookTicker leader + Gate L2 follower.
Read-only on Drive. Raw partitions stay under /tmp and are removed at the end; only JSON reports go to
/kaggle/working. Drive credentials come from the private dataset under /kaggle/input and are never printed.

Old side: frozen collector qa_convert.run_qa_convert with the input narrowing of
scripts/tokyo_l2_bt_convert.py, called without the complete-day coverage gate (that gate needs all 96
windows; it is not relaxed anywhere, only not invoked for this 2-window sample).
New side: scripts/tokyo_l2_bt_stream_convert.py in sample mode, once in one pass and once as
partition 1 -> checkpoint -> a new process resumes with partition 2.
'''
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import argparse
import json
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import time
import traceback
import urllib.request
import zipfile

WORK = Path('/kaggle/working')
REPORT = WORK / 'two_partition_report.json'
DATE = '2026-11-23'
GATE_SYMBOL = 'BTC_USDT'
EXCHANGES = ('binance_futures', 'gate_futures')
REMOTE = 'qa:'
RCLONE_VERSION = 'v1.68.2'
RCLONE_DIR = Path('/tmp/rclone_bin')
AUTH = Path('/tmp/tokyo_l2_auth_tp')
SCRATCH = Path('/tmp/tokyo_l2_tp')
CODE = Path(os.environ.get('TOKYO_L2_CODE_ROOT', '/tmp/tokyo_l2_code'))
CONFIG = CODE / 'collector' / 'specs' / 'config.trial_15m.json'
QA_DAYS = CODE / 'repo' / 'kaggle' / 'qa_days'
QA_PROTOCOL = CODE / 'repo' / 'kaggle' / 'qa_protocol'
CRED_FILES = {'TOKYO_L2_QA_GDRIVE_SA_JSON': 'tokyo_l2_gdrive_sa.json',
              'TOKYO_L2_QA_GDRIVE_FOLDER_ID': 'tokyo_l2_gdrive_folder_id.txt'}
WINDOW_NS = 900 * 1_000_000_000
BOOK_STEP_NS = 100_000_000
SAMPLE_WINDOWS = ('2026-11-23T00:00Z', '2026-11-23T00:15Z')
STAGES = ('setup', 'fetch', 'convert_old', 'convert_new', 'compare', 'book_rebuild', 'raw_audit', 'glft_replay')
# Download concurrency is not re-measured; result of the earlier stage-3 run, kept for the record.
PRIOR_THROUGHPUT = {'source': 'Actions run 45551773265 (commit 6c748e8), artifact kaggle-45551773265',
                    'workers': [1, 2, 4, 8], 'MBps': [1.8, 3.9, 6.4, 11.5], 'recommended_workers': 8,
                    'rerun': False}
NL = chr(10)
NLB = bytes([10])

report: dict = {'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'date_utc': DATE,
                'gate_symbol': GATE_SYMBOL, 'windows': ['00:00-00:15', '00:15-00:30'], 'stages': {}}
CTX: dict = {}


def peak_rss_mb() -> float:
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def du(path: Path) -> int:
    path = Path(path)
    return sum(p.stat().st_size for p in path.rglob('*') if p.is_file()) if path.exists() else 0


def save() -> None:
    WORK.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')


def stage(name, fn):
    t0 = time.time()
    print('STAGE', name, 'start', flush=True)
    entry: dict = {}
    try:
        entry['result'] = fn()
        entry['ok'] = True
    except Exception as exc:
        entry.update(ok=False, error=f'{type(exc).__name__}: {exc}'[:2000], trace=traceback.format_exc()[-4000:])
    entry.update(elapsed_s=round(time.time() - t0, 1), parent_peak_rss_mb=peak_rss_mb(), scratch_bytes=du(SCRATCH),
                 disk_free_gib={p: round(shutil.disk_usage(p).free / 2 ** 30, 2) for p in ('/kaggle/working', '/tmp')})
    report['stages'][name] = entry
    save()
    print('STAGE', name, 'ok=' + str(entry['ok']), str(entry['elapsed_s']) + 's', flush=True)
    return entry


def setup():
    r = subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', 'hftbacktest==2.4.4', 'zstandard==0.23.0'],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError('pip install failed: ' + (r.stdout + r.stderr)[-1500:])
    exe = RCLONE_DIR / 'rclone'
    if not exe.exists():
        blob, errors = None, []
        for url in (f'https://github.com/rclone/rclone/releases/download/{RCLONE_VERSION}/rclone-{RCLONE_VERSION}-linux-amd64.zip',
                    f'https://downloads.rclone.org/{RCLONE_VERSION}/rclone-{RCLONE_VERSION}-linux-amd64.zip'):
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'curl/8.5.0'})
                with urllib.request.urlopen(req, timeout=120) as resp:
                    blob = resp.read()
                break
            except Exception as exc:
                errors.append(url.split('/')[2] + f': {type(exc).__name__}: {exc}')
        if blob is None:
            raise RuntimeError('; '.join(errors))
        zpath = Path('/tmp/rclone_tp.zip')
        zpath.write_bytes(blob)
        RCLONE_DIR.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zpath) as z:
            member = next(m for m in z.namelist() if m.endswith('/rclone'))
            exe.write_bytes(z.read(member))
        exe.chmod(0o755)
        zpath.unlink()
    os.environ['PATH'] = str(RCLONE_DIR) + os.pathsep + os.environ.get('PATH', '')
    root = Path('/kaggle/input')
    found = {}
    for key, fname in CRED_FILES.items():
        hits = sorted(p for p in root.rglob(fname) if p.is_file()) if root.is_dir() else []
        if hits:
            found[key] = hits[0]
    if len(found) != len(CRED_FILES):
        raise RuntimeError('credential dataset files missing: ' + json.dumps({k: k in found for k in CRED_FILES}))
    sa = json.loads(found['TOKYO_L2_QA_GDRIVE_SA_JSON'].read_text(encoding='utf-8'))
    folder = found['TOKYO_L2_QA_GDRIVE_FOLDER_ID'].read_text(encoding='utf-8').strip()
    old_umask = os.umask(0o077)
    AUTH.mkdir(parents=True, exist_ok=True)
    sa_path = AUTH / 'sa.json'
    sa_path.write_text(json.dumps(sa))
    conf = AUTH / 'rclone.conf'
    conf.write_text('[qa]' + NL + 'type = drive' + NL + 'scope = drive.readonly' + NL
                    + f'service_account_file = {sa_path}' + NL + f'root_folder_id = {folder}' + NL)
    os.umask(old_umask)
    os.environ['RCLONE_CONFIG'] = str(conf)
    import importlib.metadata as md
    versions = {n: md.version(n) for n in ('hftbacktest', 'numba', 'numpy', 'zstandard')}
    ver = subprocess.run(['rclone', 'version'], capture_output=True, text=True).stdout.splitlines()[0]
    return {'versions': versions, 'rclone': ver, 'cpu_count': os.cpu_count(), 'credential_source': 'private_dataset'}


def count_lines(path: Path) -> dict:
    import zstandard as zstd
    lines = size = 0
    with open(path, 'rb') as fh:
        reader = zstd.ZstdDecompressor().stream_reader(fh, read_across_frames=True)
        while True:
            chunk = reader.read(1 << 24)
            if not chunk:
                break
            lines += chunk.count(NLB)
            size += len(chunk)
    return {'raw_lines': lines, 'raw_uncompressed_bytes': size}


def download_one(c: dict, out: Path, run_id: str) -> dict:
    import hourly_incremental_qa as archive
    import tokyo_l2_bt_fetch as F
    rel_m = F._safe_rel(c['source_manifest'], 'raw')
    rel_d = F._safe_rel(c['source_data'], 'raw')
    if str(rel_d) != str(rel_m).removesuffix('.manifest.json') + '.tsv.zst':
        raise ValueError('index data and manifest paths do not match')
    mpath, dpath = out / rel_m, out / rel_d
    mpath.parent.mkdir(parents=True, exist_ok=True)
    archive._download_with_retry(REMOTE, str(rel_m), mpath, retries=3, sleeper=time.sleep)
    archive._download_with_retry(REMOTE, str(rel_d), dpath, retries=3, sleeper=time.sleep)
    manifest = json.loads(mpath.read_bytes())
    archive._verify_three_way_index_manifest_data(cand=c, manifest_path=mpath, manifest=manifest,
                                                  data_path=dpath, run_id_filter=run_id)
    return {'exchange': c['exchange'], 'window_id': c['window_id'], 'data_sha256': c['data_sha256'],
            'data_bytes': int(c['data_bytes'] or 0), 'manifest_sha256': c['manifest_sha256'],
            'manifest': str(mpath), 'data': str(dpath)}


def fetch():
    import hourly_incremental_qa as archive
    from data_coverage import effective_config_hash, normalize_manifest_window
    import tokyo_l2_bt_fetch as F
    config = json.loads(CONFIG.read_text(encoding='utf-8'))
    day_path = F.find_day_report(QA_DAYS, DATE)
    day = json.loads(day_path.read_text(encoding='utf-8'))
    run_id = str(day['run_id'])
    integrity = F.integrity_gate(day, EXCHANGES, effective_config_hash(config), DATE)
    start_s = F.day_start_s(DATE)
    cands, mode, warnings, _, index_run_id = archive._discover_verified_candidates(
        REMOTE, config, since_date=DATE, since_start_s=start_s, run_id_filter=run_id,
        download_retries=3, sleeper=time.sleep)
    if mode != 'archive_manifest_index':
        raise ValueError(f'verified archive index unavailable: mode={mode} warnings={warnings}')
    per_ex = {ex: sorted((c for c in cands if c['exchange'] == ex and c['date_utc'] == DATE),
                         key=lambda c: str(c['window_id'])) for ex in EXCHANGES}
    fetched = SCRATCH / 'fetched'
    selected = [(ex, slot, c) for ex in EXCHANGES for slot, c in enumerate(per_ex[ex][:2])]
    if len(selected) != 4 or any(len({c['window_id'] for c in per_ex[ex][:2]}) != 2 for ex in EXCHANGES):
        raise ValueError('need exactly two distinct adjacent partitions per exchange')
    # Keep the previously selected concurrency; do not benchmark or fetch extra windows.
    with ThreadPoolExecutor(max_workers=8) as pool:
        infos = list(pool.map(lambda item: download_one(item[2], fetched, run_id), selected))
    parts = []
    for (ex, slot, c), info in zip(selected, infos):
        mf = json.loads(Path(info['manifest']).read_text(encoding='utf-8'))
        planned = int(normalize_manifest_window(mf, fallback_file_name=Path(info['data']).name)['planned_start_wall_ns'])
        expect = start_s * 1_000_000_000 + slot * WINDOW_NS
        if planned != expect or info['window_id'] != SAMPLE_WINDOWS[slot]:
            raise ValueError(f'{ex} slot {slot}: wrong sample window')
        info.update(planned_start_wall_ns=planned, slot=slot, **count_lines(Path(info['data'])))
        parts.append(info)
        print('FETCHED', ex, slot, info['data_bytes'], info['raw_lines'], flush=True)
    from stage3_audit import protocol_evidence
    provenance = json.loads((QA_PROTOCOL / 'provenance.json').read_text())
    protocol = protocol_evidence(
        QA_PROTOCOL, SAMPLE_WINDOWS, parts, run_id, day['checker_commit_sha'],
        rules_sha=provenance.get('expected_protocol_rules_sha'),
        expected_archive_revisions=provenance.get('window_archive_revisions'),
    )
    sample_day = dict(day)
    for key in (*F.BLOCKING_DAY_LISTS, 'cross_window_sequence_issues', 'startup_stub_segments', 'damaged_partitions'):
        sample_day[key] = [v for v in day.get(key, []) if any(w in json.dumps(v) for w in SAMPLE_WINDOWS)]
    sample_integrity = F.integrity_gate(sample_day, EXCHANGES, effective_config_hash(config), DATE)
    CTX.update(protocol=protocol, qa_provenance=provenance)
    meta = F.fetch_meta(REMOTE, DATE, EXCHANGES, fetched)
    contract = F.gate_live_contract(REMOTE, fetched / 'meta', GATE_SYMBOL, start_s * 1_000_000_000)
    CTX.update(run_id=run_id, per_ex=per_ex, fetched=fetched, parts=parts, contract=contract['contract'],
               day_start_ns=start_s * 1_000_000_000)
    return {'day_report': day_path.name, 'run_id': run_id, 'integrity': integrity,
            'sample_integrity': sample_integrity, 'protocol': protocol, 'qa_provenance': provenance,
            'index_effective_run_id': index_run_id, 'verified_windows': {ex: len(v) for ex, v in per_ex.items()},
            'parts': [{k: v for k, v in p.items() if k not in ('manifest', 'data')} for p in parts],
            'meta_files': meta, 'gate_live_contract': contract}


def child_old(spec: dict) -> dict:
    import qa_convert
    import tokyo_l2_bt_convert as oc
    stats = {'records_in': 0, 'records_kept': 0, 'gate_mixed_symbol_trade_messages': 0,
             'prefilter_rx_seq_violations': 0}
    oc.install_filter(qa_convert, GATE_SYMBOL, stats)
    fetched = Path(spec['fetched'])
    res = qa_convert.run_qa_convert(fetched, Path(spec['out']), meta_dir=fetched / 'meta', strict_zero_loss=True)
    return {'status': res.get('status'), 'failed_items': res.get('failed_items'), 'input_narrowing': stats,
            'result_keys': sorted(res)}


def child_new(spec: dict) -> dict:
    import tokyo_l2_bt_stream_convert as sc
    return {'rc': sc.main(list(spec['argv']))}


def child_main(kind: str, spec_json: str, result_path: str) -> int:
    t0 = time.time()
    out: dict = {'kind': kind}
    try:
        out.update((child_old if kind == 'old' else child_new)(json.loads(spec_json)))
        out['ok'] = out.get('rc', 0) == 0 and out.get('status', 'ok') == 'ok'
    except Exception as exc:
        out.update(ok=False, error=f'{type(exc).__name__}: {exc}'[:2000], trace=traceback.format_exc()[-4000:])
    out.update(elapsed_s=round(time.time() - t0, 1), peak_rss_mb=peak_rss_mb())
    Path(result_path).write_text(json.dumps(out, default=str), encoding='utf-8')
    return 0 if out['ok'] else 2


def run_child(kind: str, spec: dict, timeout: int = 5400) -> dict:
    res_path = SCRATCH / f'child_{kind}_{time.time_ns()}.json'
    cmd = [sys.executable, '-u', str(Path(__file__).resolve()), '--child', kind, '--spec', json.dumps(spec),
           '--result', str(res_path)]
    r = subprocess.run(cmd, timeout=timeout)
    out = json.loads(res_path.read_text(encoding='utf-8')) if res_path.exists() else \
        {'ok': False, 'error': 'child exited without a result file'}
    out['returncode'] = r.returncode
    if r.returncode != 0 or not out.get('ok') or out.get('rc', 0) != 0:
        raise RuntimeError(f'{kind} child failed rc={r.returncode}: ' + str(out.get('error')))
    return out


def convert_old():
    out = SCRATCH / 'old'
    r = run_child('old', {'fetched': str(CTX['fetched']), 'out': str(out)})
    CTX['old_out'] = out
    r['files'] = sorted(p.name for p in out.glob('*')) if out.exists() else []
    cal = out / 'eligibility_calendar.json'
    if cal.exists():
        keys = ('exchange', 'symbol', 'date_utc', 'eligibility_status', 'depth_rebuild_status', 'unverified_reasons',
                'unrebuildable_depth_intervals', 'unrebuildable_depth_reasons')
        r['calendar'] = [{k: row.get(k) for k in keys} for row in json.loads(cal.read_text(encoding='utf-8'))
                         if isinstance(row, dict) and row.get('symbol') == 'BTC']
    return r


def convert_new():
    fetched = CTX['fetched']
    w0 = SCRATCH / 'fetched_w0'
    for p in CTX['parts']:
        if p['slot'] == 0:
            for src in (Path(p['manifest']), Path(p['data'])):
                dst = w0 / src.relative_to(fetched)
                dst.parent.mkdir(parents=True, exist_ok=True)
                os.symlink(src, dst)
    meta, gs = fetched / 'meta', GATE_SYMBOL
    s1, s2 = SCRATCH / 'state_resume', SCRATCH / 'state_single'
    n1, n2 = SCRATCH / 'new_resume', SCRATCH / 'new_single'
    fin = ['--label-date', DATE, '--meta', str(meta), '--finalize']
    runs = {
        'resume_part1': run_child('new', {'argv': ['--fetched', str(w0), '--gate-symbol', gs, '--state', str(s1)]}),
        'resume_part2_finalize': run_child('new', {'argv': ['--fetched', str(fetched), '--gate-symbol', gs,
                                                            '--state', str(s1), *fin, '--out', str(n1)]}),
        'single_finalize': run_child('new', {'argv': ['--fetched', str(fetched), '--gate-symbol', gs,
                                                      '--state', str(s2), *fin, '--out', str(n2)]}),
    }
    CTX.update(new_out=n2, new_resume_out=n1)
    rep = json.loads((n2 / 'stream_report.json').read_text(encoding='utf-8'))
    CTX['new_report'] = rep
    keys = ('status', 'failed_items', 'mode', 'records', 'trade_dedup', 'hbt_event_types', 'hbt_time_range',
            'order_diagnostics', 'exch_ahead_of_local', 'special_flag_counts', 'initialization_exclusion',
            'switches', 'gaps', 'local_loss', 'binance_depth_meta', 'depth_unavailable_intervals',
            'execution_unavailable_intervals',
            'unrebuildable_depth_intervals', 'eligibility', 'artifacts', 'parts', 'peak_rss_mb', 'finalize_s',
            'out_bytes')
    resumed = json.loads((n1 / 'stream_report.json').read_text(encoding='utf-8'))
    return {'runs': runs, 'single_report': {k: rep.get(k) for k in keys},
            'resume_report_status': resumed.get('status'), 'state_bytes': {'resume': du(s1), 'single': du(s2)}}


def artifact_names() -> dict:
    from tokyo_l2_bt_convert import native_symbols
    base = native_symbols(GATE_SYMBOL)[0]
    ymd = DATE.replace('-', '')
    return {'binance_bbo': f'binance_bbo_{base}_{ymd}.npz', 'gate_l2_hbt': f'gate_futures_l2_{base}_{ymd}.npz',
            'gate_l2_edges': f'gate_futures_l2_edges_{base}_{ymd}.npz', 'gate_l1_bbo': f'gate_futures_l1_{base}_{ymd}.npz'}


def load_npz(path: Path) -> dict:
    import numpy as np
    from hftbacktest import event_dtype
    with np.load(path) as z:
        out = {k: z[k] for k in z.files}
    d = out.get('data')
    if d is not None and d.dtype.fields == event_dtype.fields and d.dtype.itemsize == event_dtype.itemsize:
        out['data'] = d.view(event_dtype)
    return out


def same(x, y) -> bool:
    import numpy as np
    if x.shape != y.shape or x.dtype.descr != y.dtype.descr:
        return False
    return np.ascontiguousarray(x).tobytes() == np.ascontiguousarray(y).tobytes()


def cmp_npz(a, b) -> dict:
    if a is None or b is None:
        return {'_artifact': {'equal': False, 'missing': 'old' if a is None else 'new'}}
    out = {}
    for key in sorted(set(a) | set(b)):
        if key not in a or key not in b:
            out[key] = {'equal': False, 'missing': 'old' if key not in a else 'new'}
            continue
        x, y = a[key], b[key]
        out[key] = {'len': [int(len(x)), int(len(y))], 'dtype': [str(x.dtype), str(y.dtype)], 'equal': same(x, y)}
    return out


def row_hashes(h):
    import numpy as np
    u = np.ascontiguousarray(h).view(np.uint64).reshape(len(h), h.dtype.itemsize // 8)
    acc = np.full(len(h), np.uint64(1469598103934665603), dtype=np.uint64)
    with np.errstate(over='ignore'):
        for j in range(u.shape[1]):
            acc = (acc ^ u[:, j]) * np.uint64(1099511628211)
    return acc


def hbt_side(h) -> dict:
    import numpy as np
    from hftbacktest import EXCH_EVENT, LOCAL_EVENT
    from hftbacktest.data.validation import validate_event_order
    import tokyo_l2_bt_stream_convert as sc
    ev, lt, et = h['ev'], h['local_ts'], h['exch_ts']
    loc = (ev & np.uint64(LOCAL_EVENT)) != 0
    exc = (ev & np.uint64(EXCH_EVENT)) != 0
    try:
        validate_event_order(h)
        val = 'ok'
    except Exception as e:
        val = f'{type(e).__name__}: {e}'[:500]

    def frac(mask):
        return float(mask.mean()) if len(mask) else None
    return {'rows': int(len(h)), 'dtypes': {f: str(h.dtype[f]) for f in ('exch_ts', 'local_ts')},
            'event_types': sc.event_histogram(h), 'validate_event_order': val,
            'local_rows_local_ts_nondecreasing': bool((np.diff(lt[loc]) >= 0).all()),
            'exch_rows_exch_ts_nondecreasing': bool((np.diff(et[exc]) >= 0).all()),
            'exch_after_local_rows': int(((et > lt) & loc & exc).sum()),
            'local_ts_mod256_zero_frac': frac(lt % 256 == 0), 'local_ts_mod1000_zero_frac': frac(lt % 1000 == 0),
            'exch_ts_mod1e6_zero_frac': frac(et % 1_000_000 == 0),
            'first_local_ts': int(lt.min()) if len(h) else None, 'last_local_ts': int(lt.max()) if len(h) else None}


def hbt_detail(old, new) -> dict:
    import numpy as np
    import tokyo_l2_bt_stream_convert as sc
    out = {'new': hbt_side(new), 'old': hbt_side(old) if old is not None else None}
    if old is None:
        return out
    ho, hn = row_hashes(old), row_hashes(new)
    only_old, only_new = ~np.isin(ho, hn), ~np.isin(hn, ho)
    diff = {'rows_only_in_old': int(only_old.sum()), 'rows_only_in_new': int(only_new.sum()),
            'only_old_event_types': sc.event_histogram(old[only_old]),
            'only_new_event_types': sc.event_histogram(new[only_new]),
            'only_old_samples': old[only_old][:5].tolist(), 'only_new_samples': new[only_new][:5].tolist()}
    if len(old) == len(new) and np.array_equal(old['ev'], new['ev']) and len(old):
        diff['aligned_max_abs_diff'] = {
            'exch_ts': int(np.abs(old['exch_ts'] - new['exch_ts']).max()),
            'local_ts': int(np.abs(old['local_ts'] - new['local_ts']).max()),
            'px': float(np.nanmax(np.abs(old['px'] - new['px']))),
            'qty': float(np.nanmax(np.abs(old['qty'] - new['qty'])))}
    out['diff'] = diff
    return out


def compare():
    names = artifact_names()
    out: dict = {'resume_vs_single': {}, 'old_vs_new': {}}
    arrays = {}
    for kind, name in names.items():
        single = load_npz(CTX['new_out'] / name)
        out['resume_vs_single'][kind] = cmp_npz(load_npz(CTX['new_resume_out'] / name), single)
        old_path = CTX['old_out'] / name
        old = load_npz(old_path) if old_path.exists() else None
        out['old_vs_new'][kind] = cmp_npz(old, single)
        arrays[kind] = (old, single)
    out['resume_identical'] = all(v.get('equal') for d in out['resume_vs_single'].values() for v in d.values())
    out['old_new_identical'] = all(v.get('equal') for d in out['old_vs_new'].values() for v in d.values())
    out['hbt_detail'] = hbt_detail((arrays['gate_l2_hbt'][0] or {}).get('data'), arrays['gate_l2_hbt'][1]['data'])
    for kind in ('binance_bbo', 'gate_l1_bbo', 'gate_l2_edges'):
        o, n = arrays[kind]
        out[kind + '_rows'] = {'old': len(next(iter(o.values()))) if o else None, 'new': len(next(iter(n.values())))}
    return out


_BOOK_LOOP = None


def get_book_loop():
    global _BOOK_LOOP
    if _BOOK_LOOP is None:
        from numba import njit

        @njit
        def loop(hbt, start_ns, step, ts, vals):
            if hbt.elapse(1) != 0:
                return 0
            delta = start_ns - hbt.current_timestamp
            if delta > 0:
                if hbt.elapse(delta) != 0:
                    return 0
            k = 0
            while k < ts.shape[0]:
                d = hbt.depth(0)
                ts[k] = hbt.current_timestamp
                vals[k, 0] = d.best_bid
                vals[k, 1] = d.best_ask
                vals[k, 2] = d.bid_qty_at_tick(d.best_bid_tick)
                vals[k, 3] = d.ask_qty_at_tick(d.best_ask_tick)
                k += 1
                if hbt.elapse(step) != 0:
                    break
            return k
        _BOOK_LOOP = loop
    return _BOOK_LOOP


def book_samples(path: Path, tick: float, start_ns: int, end_ns: int):
    import numpy as np
    from hftbacktest import BacktestAsset, ROIVectorMarketDepthBacktest
    with np.load(path) as z:
        px = z['data']['px']
    good = px[(px > 0) & np.isfinite(px)]
    asset = (BacktestAsset().data([str(path)]).linear_asset(1.0).constant_order_latency(1_000_000, 1_000_000)
             .power_prob_queue_model(2.0).no_partial_fill_exchange().trading_value_fee_model(0.0, 0.0)
             .tick_size(tick).lot_size(1e-8).roi_lb(max(tick, float(good.min()) - 1000 * tick))
             .roi_ub(float(good.max()) + 1000 * tick))
    hbt = ROIVectorMarketDepthBacktest([asset])
    n = int((end_ns - start_ns) // BOOK_STEP_NS) + 1
    ts = np.zeros(n, dtype=np.int64)
    vals = np.full((n, 4), np.nan)
    try:
        k = get_book_loop()(hbt, start_ns, BOOK_STEP_NS, ts, vals)
    finally:
        hbt.close()
    return ts[:k], vals[:k]


def book_summary(ts, vals, boundary: int) -> dict:
    import numpy as np
    bid, ask = vals[:, 0], vals[:, 1]
    fin = np.isfinite(bid) & np.isfinite(ask) & (bid > 0) & (ask > 0) & (bid < 1e12) & (ask < 1e12)
    valid = fin & (ask > bid)
    first = int(np.argmax(valid)) if valid.any() else None
    res = {'samples': int(len(ts)), 'first_ts': int(ts[0]) if len(ts) else None,
           'last_ts': int(ts[-1]) if len(ts) else None, 'valid_samples': int(valid.sum()),
           'first_valid_ts': int(ts[first]) if first is not None else None,
           'invalid_after_first_valid': int((~valid[first:]).sum()) if first is not None else None,
           'crossed_or_locked_samples': int((fin & (ask <= bid)).sum())}
    if first is not None and len(ts) > first + 1:
        mid = np.where(valid, (bid + ask) / 2, np.nan)
        jumps = np.abs(np.diff(mid))
        tail = jumps[first:]
        tail = tail[np.isfinite(tail)]
        near_pair = (np.abs(ts[1:] - boundary) <= 1_000_000_000) & np.isfinite(jumps)
        res['mid_jump_p99'] = float(np.percentile(tail, 99)) if len(tail) else None
        res['mid_jump_max'] = float(tail.max()) if len(tail) else None
        res['mid_jump_max_boundary_pm1s'] = float(jumps[near_pair].max()) if near_pair.any() else None
        near = np.abs(ts - boundary) <= 3_000_000_000
        res['boundary_samples_every_500ms'] = [[int(t), float(b), float(a)] for t, b, a in
                                               zip(ts[near][::5], bid[near][::5], ask[near][::5])]
    return res


def boundary_events(h, boundary: int) -> dict:
    import numpy as np
    from hftbacktest import DEPTH_CLEAR_EVENT, DEPTH_SNAPSHOT_EVENT
    kind = h['ev'] & np.uint64(0xFF)
    lt = h['local_ts']
    near = np.abs(lt - boundary) <= 5_000_000_000
    is_snap = kind == np.uint64(DEPTH_SNAPSHOT_EVENT)
    is_clear = kind == np.uint64(DEPTH_CLEAR_EVENT)
    return {'snapshot_rows_total': int(is_snap.sum()), 'clear_rows_total': int(is_clear.sum()),
            'snapshot_rows_boundary_pm5s': int((is_snap & near).sum()),
            'clear_rows_boundary_pm5s': int((is_clear & near).sum()),
            'snapshot_local_ts_unique': [int(x) for x in np.unique(lt[is_snap])[:20]]}


def book():
    import numpy as np
    names = artifact_names()
    tick = float(CTX['contract']['order_price_round'])
    day0 = CTX['day_start_ns']
    boundary = day0 + WINDOW_NS
    start, end = day0 + 1_000_000_000, day0 + 2 * WINDOW_NS
    out: dict = {'tick_size': tick, 'step_ns': BOOK_STEP_NS, 'boundary_ns': boundary}
    samples = {}
    for side, d in (('old', CTX['old_out']), ('new', CTX['new_out'])):
        path = d / names['gate_l2_hbt']
        if not path.exists():
            out[side] = {'missing': True}
            continue
        ts, vals = book_samples(path, tick, start, end)
        samples[side] = (ts, vals)
        out[side] = {**book_summary(ts, vals, boundary), **boundary_events(load_npz(path)['data'], boundary)}
    if 'old' in samples and 'new' in samples:
        (to, vo), (tn, vn) = samples['old'], samples['new']
        if len(to) == len(tn) and np.array_equal(to, tn):
            rows = (~((vo == vn) | (np.isnan(vo) & np.isnan(vn)))).any(axis=1)
            out['old_vs_new'] = {'same_grid': True, 'mismatched_samples': int(rows.sum()),
                                 'first_mismatch_ts': int(tn[rows][0]) if rows.any() else None}
        else:
            out['old_vs_new'] = {'same_grid': False, 'old_samples': int(len(to)), 'new_samples': int(len(tn))}
    return out


def raw_audit():
    from stage3_audit import audit
    old = report['stages']['convert_old']['result']
    return audit(CTX['parts'], CTX['new_out'], CTX['old_out'], artifact_names(),
                 CTX['new_report'], old.get('calendar', []), CTX['protocol'])


def glft():
    import qa_convert
    names = artifact_names()
    tick = float(CTX['contract']['order_price_round'])
    # Preserve the existing stage-3 replay parameters. This is the frozen input-equivalence
    # replay (600 s calibration + 120 s trading), not a new paper-profitability experiment.
    intervals = list(CTX['new_report']['execution_unavailable_intervals'])
    intervals += CTX['protocol']['execution_unavailable_intervals']
    r = qa_convert.run_offline_e2e_hbt_replay(
        CTX['new_out'] / names['gate_l2_hbt'], CTX['new_out'] / names['binance_bbo'],
        tick_size=tick, depth_unavailable_intervals=intervals)
    m = r['metrics']
    audit = r['gap_execution_audit']
    fills = len(m.get('fill_ts_ns') or [])
    accepted = (r.get('status') == 'ok' and r.get('execution_status') == 'verified'
                and int(m.get('decisions', 0)) > 0 and int(m.get('submits', 0)) > 0
                and int(m.get('sample_count', 0)) == 7200 and bool(r['diagnostics'][5])
                and bool(r.get('ledger_sha256')))
    return {'passed': accepted, 'status': r.get('status'), 'execution_status': r.get('execution_status'),
            'ledger_sha256': r.get('ledger_sha256'), 'fills': fills,
            't0_ns': r.get('t0_ns'), 't_end_ns': r.get('t_end_ns'),
            'metrics': {k: v for k, v in m.items() if not k.startswith('fill_')},
            'diagnostics': r['diagnostics'],
            'gap_execution_audit': {k: v for k, v in audit.items() if k not in ('fills', 'labeled_fills')},
            'params': {'tick_size': tick, 'other_parameters': 'frozen run_offline_e2e_hbt_replay defaults',
                       'trade_steps': 1200, 'note': 'input-equivalence replay, not paper profitability'},
            'accounting_policy': 'retain diagnostic ledger; any unverified fill rejects entire replay',
            'execution_unavailable_intervals': intervals}


def acceptance(st):
    def res(name):
        return (st.get(name) or {}).get('result') or {}
    stage_ok = {name: (st.get(name) or {}).get('ok') is True for name in STAGES}
    fetched, raw, cmp_, replay = res('fetch'), res('raw_audit'), res('compare'), res('glft_replay')
    new_rep = res('convert_new').get('single_report') or {}
    diagnostic = all(stage_ok.values())
    sample = (fetched.get('sample_integrity', {}).get('status') == 'passed'
              and fetched.get('protocol', {}).get('passed') is True
              and raw.get('passed') is True and new_rep.get('status') == 'ok')
    conversion = (cmp_.get('resume_identical') is True
                  and (cmp_.get('hbt_detail', {}).get('new') or {}).get('validate_event_order') == 'ok'
                  and (cmp_.get('old_new_identical') is True or raw.get('old_new_difference_explained') is True))
    stage3 = diagnostic and sample and conversion and replay.get('passed') is True
    return {'diagnostic_execution_complete': diagnostic, 'sample_quality_passed': sample,
            'conversion_checks_passed': conversion, 'glft_replay_passed': replay.get('passed') is True,
            'full_day_integrity_passed': fetched.get('integrity', {}).get('status') == 'passed',
            'stage3_accepted': stage3, 'stage4_authorized': False,
            'stage_ok': stage_ok, 'protocol_blockers': fetched.get('protocol', {}).get('problems', []),
            'raw_audit_failed_checks': [k for k, v in raw.get('checks', {}).items() if not v]}


def summarize() -> None:
    report['summary'] = acceptance(report['stages'])
    report['all_ok'] = report['summary']['stage3_accepted']
    report['prior_download_throughput'] = PRIOR_THROUGHPUT


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--child')
    ap.add_argument('--spec')
    ap.add_argument('--result')
    a = ap.parse_args()
    if a.child:
        return child_main(a.child, a.spec, a.result)
    SCRATCH.mkdir(parents=True, exist_ok=True)
    try:
        if stage('setup', setup)['ok'] and stage('fetch', fetch)['ok']:
            stage('convert_old', convert_old)
            if stage('convert_new', convert_new)['ok']:
                stage('compare', compare)
                stage('book_rebuild', book)
                stage('raw_audit', raw_audit)
                stage('glft_replay', glft)
    finally:
        shutil.rmtree(AUTH, ignore_errors=True)
        report['auth_dir_removed'] = not AUTH.exists()
        report['scratch_bytes_before_cleanup'] = du(SCRATCH)
        shutil.rmtree(SCRATCH, ignore_errors=True)
        report['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        summarize()
        save()
    print('TWO_PARTITION_DONE all_ok=' + str(report['all_ok']), json.dumps(report['summary'], default=str), flush=True)
    return 0 if report['all_ok'] else 3


if __name__ == '__main__':
    raise SystemExit(main())
