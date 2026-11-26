#!/usr/bin/env python3
"""Full-day (24h UTC) Tokyo L2 diagnostic backtest on Kaggle: GLFT, both_1p0, risk_side_pause.

Runs inside the existing private Kaggle notebook via kaggle/launcher.py.
- Uses existing streaming converter (scripts/tokyo_l2_bt_stream_convert.py) with official
  hftbacktest==2.4.4 data types, event ordering, matching, and queue model.
- Self-collected data latency & order-op parameters (explicitly enforced & verified at runtime):
  * Binance extra signal visibility delay = 0 ms (raw local receipt ns, no +2.5ms, no exch ts).
  * Gate market data uses raw local receipt ns.
  * Simulated order entry latency = 1 ms, order response latency = 1 ms (2 ms RTT assumption).
  * Same-side requote cooldown = 0 ms (min_requote_ns = 0; preserves ACK/cancellable/GTX/cap).
- Three frozen models share identical data, identical calibration witness start, and identical
  effective trading session (600 s warmup + 1430 min continuous trading; no 96-window stitching).
- Reports zero-fee gross PnL as primary metric and +2 bps study maker fee as supplementary.
- Preserves truthful production QA statuses; does not modify production QA or authorize Stage 4.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import gc
import hashlib
import json
import math
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
REPORT = WORK / 'full_day_report.json'
DATE = '2026-11-23'
GATE_SYMBOL = 'BTC_USDT'
EXCHANGES = ('binance_futures', 'gate_futures')
REMOTE = 'qa:'
RCLONE_VERSION = 'v1.68.2'
RCLONE_DIR = Path('/tmp/rclone_bin')
AUTH = Path('/tmp/tokyo_l2_auth_fd')
SCRATCH = Path('/tmp/tokyo_l2_fd')
CODE = Path(os.environ.get('TOKYO_L2_CODE_ROOT', '/tmp/tokyo_l2_code'))
CONFIG = CODE / 'collector' / 'specs' / 'config.trial_15m.json'
QA_DAYS = CODE / 'repo' / 'kaggle' / 'qa_days'
QA_PROTOCOL = CODE / 'repo' / 'kaggle' / 'qa_protocol'
CRED_FILES = {'TOKYO_L2_QA_GDRIVE_SA_JSON': 'tokyo_l2_gdrive_sa.json',
              'TOKYO_L2_QA_GDRIVE_FOLDER_ID': 'tokyo_l2_gdrive_folder_id.txt'}

NS = 1_000_000_000
WINDOW_NS = 900 * NS
DAY_NS = 86_400 * NS
TRAIN_MINUTES = 10
TRADE_MINUTES = 1430
TRADE_STEPS = TRADE_MINUTES * 600
BATCH_WINDOWS = 8
DOWNLOAD_WORKERS = 8

# Frozen Gate BTC research parameters (from run_gate_btc_origin_glft.py / configs/contract_specs.json)
TARGET_NOTIONAL_USDT = 80.0
RISK_AVERSION_PER_USDT = 50.0
INVENTORY_MULTIPLE = 20
STUDY_MAKER_FEE_RATE = 0.0002
STUDY_TAKER_FEE_RATE = 0.0005

# Self-collected Tokyo L2 explicit latency & cooldown parameters
BINANCE_EXTRA_VISIBLE_DELAY_NS = 0
ORDER_ENTRY_LATENCY_NS = 1_000_000
ORDER_RESPONSE_LATENCY_NS = 1_000_000
MIN_REQUOTE_NS = 0

MODELS = (('glft', 0.0, 0), ('both_1p0', 1.0, 1), ('risk_side_pause', 1.0, 3))
STAGES = ('setup', 'discover_and_qa', 'fetch_and_convert', 'anomaly_and_book', 'backtest_models')
STRATEGY_CHANNELS = {
    ('binance_futures', 'bookTicker'),
    ('binance_futures', 'book_ticker'),
    ('binance_futures', '@bookTicker'),
    ('gate_futures', 'futures.book_ticker'),
    ('gate_futures', 'futures.obu'),
    ('gate_futures', 'futures.trades'),
}

report: dict = {
    'task': 'full_day_glft_diagnostic',
    'started_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    'date_utc': DATE,
    'gate_symbol': GATE_SYMBOL,
    'signal_exchange': 'binance_futures',
    'making_exchange': 'gate_futures',
    'stages': {},
}
CTX: dict = {}


def peak_rss_mb() -> float:
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def du(path: Path) -> int:
    path = Path(path)
    return sum(p.stat().st_size for p in path.rglob('*') if p.is_file()) if path.exists() else 0


def save() -> None:
    WORK.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')


def stage(name: str, fn):
    t0 = time.time()
    print('STAGE', name, 'start', flush=True)
    entry: dict = {}
    try:
        entry['result'] = fn()
        entry['ok'] = True
    except Exception as exc:
        entry.update(ok=False, error=f'{type(exc).__name__}: {exc}'[:2000],
                     trace=traceback.format_exc()[-4000:])
    entry.update(
        elapsed_s=round(time.time() - t0, 1),
        parent_peak_rss_mb=peak_rss_mb(),
        scratch_bytes=du(SCRATCH),
        disk_free_gib={p: round(shutil.disk_usage(p).free / 2 ** 30, 2)
                       for p in ('/kaggle/working', '/tmp') if Path(p).exists()},
    )
    report['stages'][name] = entry
    save()
    print('STAGE', name, 'ok=' + str(entry['ok']), str(entry['elapsed_s']) + 's', flush=True)
    return entry


def setup() -> dict:
    r = subprocess.run([sys.executable, '-m', 'pip', 'install', '-q',
                        'hftbacktest==2.4.4', 'zstandard==0.23.0'],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError('pip install failed: ' + (r.stdout + r.stderr)[-1500:])
    exe = RCLONE_DIR / 'rclone'
    if not exe.exists():
        blob, errors = None, []
        for url in (
            f'https://github.com/rclone/rclone/releases/download/{RCLONE_VERSION}/rclone-{RCLONE_VERSION}-linux-amd64.zip',
            f'https://downloads.rclone.org/{RCLONE_VERSION}/rclone-{RCLONE_VERSION}-linux-amd64.zip',
        ):
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'curl/8.5.0'})
                with urllib.request.urlopen(req, timeout=120) as resp:
                    blob = resp.read()
                break
            except Exception as exc:
                errors.append(url.split('/')[2] + f': {type(exc).__name__}: {exc}')
        if blob is None:
            raise RuntimeError('; '.join(errors))
        zpath = Path('/tmp/rclone_fd.zip')
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
        raise RuntimeError('credential dataset files missing: '
                           + json.dumps({k: k in found for k in CRED_FILES}))
    sa = json.loads(found['TOKYO_L2_QA_GDRIVE_SA_JSON'].read_text(encoding='utf-8'))
    folder = found['TOKYO_L2_QA_GDRIVE_FOLDER_ID'].read_text(encoding='utf-8').strip()
    old_umask = os.umask(0o077)
    AUTH.mkdir(parents=True, exist_ok=True)
    sa_path = AUTH / 'sa.json'
    sa_path.write_text(json.dumps(sa))
    conf = AUTH / 'rclone.conf'
    conf.write_text('[qa]\ntype = drive\nscope = drive.readonly\n'
                    + f'service_account_file = {sa_path}\nroot_folder_id = {folder}\n')
    os.umask(old_umask)
    os.environ['RCLONE_CONFIG'] = str(conf)
    import importlib.metadata as md
    versions = {n: md.version(n) for n in ('hftbacktest', 'numba', 'numpy', 'zstandard')}
    ver = subprocess.run(['rclone', 'version'], capture_output=True, text=True).stdout.splitlines()[0]
    mem_gib = None
    try:
        for line in Path('/proc/meminfo').read_text().splitlines():
            if line.startswith('MemTotal:'):
                mem_gib = round(int(line.split()[1]) / (1024 * 1024), 2)
                break
    except Exception:
        pass
    return {'versions': versions, 'rclone': ver, 'cpu_count': os.cpu_count(),
            'mem_total_gib': mem_gib, 'credential_source': 'private_dataset'}


def classify_qa_interval(status: str, reason: str) -> str:
    st = str(status or '').lower()
    rs = str(reason or '').lower()
    if st == 'known_gap' or rs in ('depth_unavailable', 'local_loss', 'gap_start'):
        return 'confirmed_data_gap'
    return 'continuity_unverified'


def audit_full_day_protocol_reports(
    folder: Path,
    cands_by_window: dict[str, list[dict]],
    run_id: str,
    provenance: dict,
) -> dict:
    """Verify all 96 protocol QA reports by rules digest, archive_revision, and input hashes.

    Does not reject valid reports solely because an unrelated commit (e.g. docs) changed the
    checker commit prefix before '/'; enforces rules digest + archive_revision + target_inputs.
    Preserves truthful production QA statuses without altering them.
    """
    expected_rules_sha = str(provenance.get('expected_protocol_rules_sha') or '').strip()
    expected_revs = dict(provenance.get('window_archive_revisions') or {})
    windows = sorted(cands_by_window.keys())
    statuses: dict[str, int] = {}
    checker_commits: set[str] = set()
    rules_digests: set[str] = set()
    missing_reports: list[str] = []
    rules_mismatches: list[str] = []
    revision_mismatches: list[str] = []
    input_mismatches: list[str] = []
    strategy_intervals: list[dict] = []
    strategy_affected_windows: set[str] = set()

    for w in windows:
        fname = w.replace(':', '') + '.json'
        path = Path(folder) / fname
        if not path.is_file():
            missing_reports.append(w)
            continue
        q = json.loads(path.read_text(encoding='utf-8'))
        st = str(q.get('status') or 'unknown')
        statuses[st] = statuses.get(st, 0) + 1
        checker = str(q.get('checker') or '')
        if '/' in checker:
            c_sha, r_sha = checker.split('/', 1)
            checker_commits.add(c_sha)
            rules_digests.add(r_sha)
        else:
            r_sha = ''
        if not expected_rules_sha or r_sha != expected_rules_sha:
            rules_mismatches.append(w)
        exp_rev = expected_revs.get(w)
        if not exp_rev or q.get('archive_revision') != exp_rev:
            revision_mismatches.append(w)
        expected_inputs = {(c['exchange'], c['data_sha256'], c['manifest_sha256'])
                           for c in cands_by_window[w]}
        actual_inputs = {(p['exchange'], p['data_sha256'], p['manifest_sha256'])
                         for p in q.get('target_inputs', [])}
        if not expected_inputs or not expected_inputs.issubset(actual_inputs) or q.get('run_id') != run_id:
            input_mismatches.append(w)
        for market in q.get('market_availability', []):
            ex = market.get('exchange')
            ch = market.get('channel')
            if market.get('symbol') == 'BTCUSDT' and (ex, ch) in STRATEGY_CHANNELS:
                for iv in market.get('intervals', []):
                    if iv.get('status') != 'verified_usable':
                        s_ns = int(iv['start_wall_ns'])
                        e_ns = int(iv['end_wall_ns']) if iv.get('end_wall_ns') is not None else None
                        strategy_intervals.append({
                            'window_id': w,
                            'exchange': ex,
                            'channel': ch,
                            'qa_status': iv.get('status'),
                            'reason': iv.get('reason'),
                            'category': classify_qa_interval(iv.get('status'), iv.get('reason')),
                            'start_wall_ns': s_ns,
                            'end_wall_ns': e_ns,
                            'duration_ms': round((e_ns - s_ns) / 1e6, 3) if e_ns is not None else None,
                        })
                        strategy_affected_windows.add(w)

    binding_ok = (not missing_reports and not rules_mismatches
                  and not revision_mismatches and not input_mismatches
                  and len(windows) == 96)
    return {
        'windows_checked': len(windows) - len(missing_reports),
        'expected_windows': len(windows),
        'missing_reports': missing_reports,
        'expected_protocol_rules_sha': expected_rules_sha,
        'observed_checker_commits': sorted(checker_commits),
        'observed_rules_digests': sorted(rules_digests),
        'rules_digest_mismatches': rules_mismatches,
        'archive_revision_mismatches': revision_mismatches,
        'input_hash_mismatches': input_mismatches,
        'evidence_binding_verified': binding_ok,
        'production_qa_window_statuses_all_exchanges': statuses,
        'strategy_channels_affected_windows': sorted(strategy_affected_windows),
        'strategy_channel_intervals': strategy_intervals,
        'stage3_strict_qa_passed': (binding_ok and not strategy_intervals
                                    and statuses.get('passed') == len(windows)),
    }


def discover_and_qa() -> dict:
    import hourly_incremental_qa as archive
    from data_coverage import effective_config_hash
    import tokyo_l2_bt_fetch as F

    config = json.loads(CONFIG.read_text(encoding='utf-8'))
    day_path = F.find_day_report(QA_DAYS, DATE)
    day = json.loads(day_path.read_text(encoding='utf-8'))
    run_id = str(day['run_id'])
    integrity = F.integrity_gate(day, EXCHANGES, effective_config_hash(config), DATE)
    start_s = F.day_start_s(DATE)
    day_start_ns = start_s * NS
    day_end_ns = day_start_ns + DAY_NS

    cands, mode, warnings, _, index_run_id = archive._discover_verified_candidates(
        REMOTE, config, since_date=DATE, since_start_s=start_s, run_id_filter=run_id,
        download_retries=3, sleeper=time.sleep)
    if mode != 'archive_manifest_index':
        raise ValueError(f'verified archive index unavailable: mode={mode} warnings={warnings}')

    per_ex = {
        ex: sorted((c for c in cands if c['exchange'] == ex and c['date_utc'] == DATE),
                   key=lambda c: str(c['window_id']))
        for ex in EXCHANGES
    }
    for ex in EXCHANGES:
        wids = [c['window_id'] for c in per_ex[ex]]
        if len(wids) != 96 or len(set(wids)) != 96:
            raise ValueError(f'{ex}: expected 96 distinct windows for {DATE}, got {len(wids)} ({len(set(wids))} unique)')

    cands_by_window: dict[str, list[dict]] = {}
    for ex in EXCHANGES:
        for c in per_ex[ex]:
            cands_by_window.setdefault(c['window_id'], []).append(c)

    provenance = json.loads((QA_PROTOCOL / 'provenance.json').read_text(encoding='utf-8'))
    protocol_audit = audit_full_day_protocol_reports(QA_PROTOCOL, cands_by_window, run_id, provenance)

    meta_root = SCRATCH / 'meta_root'
    meta_files = F.fetch_meta(REMOTE, DATE, EXCHANGES, meta_root)
    live_contract = F.gate_live_contract(REMOTE, meta_root / 'meta', GATE_SYMBOL, day_start_ns)

    total_expected_bytes = {ex: sum(int(c.get('data_bytes') or 0) for c in per_ex[ex]) for ex in EXCHANGES}
    CTX.update(
        run_id=run_id,
        day_start_ns=day_start_ns,
        day_end_ns=day_end_ns,
        per_ex=per_ex,
        cands_by_window=cands_by_window,
        meta_dir=meta_root / 'meta',
        live_contract=live_contract,
        protocol_audit=protocol_audit,
        qa_provenance=provenance,
        integrity=integrity,
    )
    return {
        'day_report': day_path.name,
        'run_id': run_id,
        'index_effective_run_id': index_run_id,
        'verified_windows': {ex: len(v) for ex, v in per_ex.items()},
        'expected_compressed_bytes': total_expected_bytes,
        'expected_compressed_total_bytes': sum(total_expected_bytes.values()),
        'full_day_integrity_gate': integrity,
        'qa_provenance': {k: v for k, v in provenance.items() if k != 'window_archive_revisions'},
        'window_archive_revisions_count': len(provenance.get('window_archive_revisions') or {}),
        'protocol_audit': protocol_audit,
        'meta_files_count': len(meta_files),
        'gate_live_contract': live_contract,
    }


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
    archive._verify_three_way_index_manifest_data(
        cand=c, manifest_path=mpath, manifest=manifest, data_path=dpath, run_id_filter=run_id)
    return {
        'exchange': c['exchange'],
        'window_id': c['window_id'],
        'data_bytes': int(dpath.stat().st_size),
        'data_sha256': c['data_sha256'],
        'manifest_sha256': c['manifest_sha256'],
    }


def run_converter_child(argv: list[str]) -> dict:
    cmd = [sys.executable, '-u', str(CODE / 'repo' / 'scripts' / 'tokyo_l2_bt_stream_convert.py'), *argv]
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = round(time.time() - t0, 2)
    tail = (proc.stdout + '\n' + proc.stderr).strip().splitlines()[-15:]
    if proc.returncode != 0:
        raise RuntimeError(f'stream converter failed rc={proc.returncode}: ' + ' | '.join(tail))
    return {'elapsed_s': elapsed, 'tail': tail}


def artifact_names() -> dict:
    from tokyo_l2_bt_convert import native_symbols
    base = native_symbols(GATE_SYMBOL)[0]
    ymd = DATE.replace('-', '')
    return {
        'binance_bbo': f'binance_bbo_{base}_{ymd}.npz',
        'gate_l2_hbt': f'gate_futures_l2_{base}_{ymd}.npz',
        'gate_l2_edges': f'gate_futures_l2_edges_{base}_{ymd}.npz',
        'gate_l1_bbo': f'gate_futures_l1_{base}_{ymd}.npz',
    }


def fetch_and_convert() -> dict:
    run_id = CTX['run_id']
    windows = sorted(CTX['cands_by_window'].keys())
    state_dir = SCRATCH / 'stream_state'
    conv_out = SCRATCH / 'converted'
    batch_root = SCRATCH / 'batch_raw'

    dl_s_total = 0.0
    conv_s_total = 0.0
    dl_bytes_by_ex = {ex: 0 for ex in EXCHANGES}
    verified_parts_count = 0
    peak_scratch_bytes = 0
    batch_logs = []

    for b_idx in range(0, len(windows), BATCH_WINDOWS):
        b_wins = windows[b_idx:b_idx + BATCH_WINDOWS]
        b_cands = [c for w in b_wins for c in CTX['cands_by_window'][w]]
        shutil.rmtree(batch_root, ignore_errors=True)
        batch_root.mkdir(parents=True, exist_ok=True)

        t_dl = time.time()
        with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
            infos = list(pool.map(lambda c: download_one(c, batch_root, run_id), b_cands))
        dt_dl = time.time() - t_dl
        dl_s_total += dt_dl
        b_bytes = sum(i['data_bytes'] for i in infos)
        for i in infos:
            dl_bytes_by_ex[i['exchange']] += i['data_bytes']
        verified_parts_count += len(infos)
        peak_scratch_bytes = max(peak_scratch_bytes, du(SCRATCH))

        c_res = run_converter_child([
            '--fetched', str(batch_root),
            '--config', str(CONFIG),
            '--coverage-date', DATE,
            '--gate-symbol', GATE_SYMBOL,
            '--state', str(state_dir),
            '--delete-inputs',
        ])
        conv_s_total += c_res['elapsed_s']
        peak_scratch_bytes = max(peak_scratch_bytes, du(SCRATCH))
        shutil.rmtree(batch_root, ignore_errors=True)
        entry = {
            'batch': b_idx // BATCH_WINDOWS + 1,
            'windows': f'{b_wins[0]}..{b_wins[-1]}',
            'files': len(infos),
            'bytes': b_bytes,
            'download_s': round(dt_dl, 1),
            'convert_s': c_res['elapsed_s'],
        }
        batch_logs.append(entry)
        print('BATCH_DONE', json.dumps(entry), flush=True)

    fin_res = run_converter_child([
        '--config', str(CONFIG),
        '--coverage-date', DATE,
        '--gate-symbol', GATE_SYMBOL,
        '--state', str(state_dir),
        '--meta', str(CTX['meta_dir']),
        '--finalize',
        '--out', str(conv_out),
        '--diagnostic-allow-known-gaps',
    ])
    peak_scratch_bytes = max(peak_scratch_bytes, du(SCRATCH))
    stream_report = json.loads((conv_out / 'stream_report.json').read_text(encoding='utf-8'))
    if stream_report.get('status') not in ('ok', 'ok_with_known_gaps'):
        raise RuntimeError('stream converter blocked: ' + json.dumps(stream_report.get('failed_items')))

    # Clean intermediate part files once final outputs are written and verified
    state_bytes_before_cleanup = du(state_dir)
    shutil.rmtree(state_dir, ignore_errors=True)

    # Independent official HBT validate_event_order check on final gate_l2_hbt
    import numpy as np
    from hftbacktest.data.validation import validate_event_order
    names = artifact_names()
    hbt_path = conv_out / names['gate_l2_hbt']
    with np.load(hbt_path) as z:
        hbt_data = z['data']
        validate_event_order(hbt_data)
        hbt_rows = int(len(hbt_data))
        hbt_dtypes = {f: str(hbt_data.dtype[f]) for f in ('exch_ts', 'local_ts')}
        hbt_min_local = int(hbt_data['local_ts'].min())
        hbt_max_local = int(hbt_data['local_ts'].max())
    del hbt_data
    gc.collect()

    CTX.update(conv_out=conv_out, stream_report=stream_report)
    total_dl_bytes = sum(dl_bytes_by_ex.values())
    keep_keys = (
        'status', 'failed_items', 'mode', 'records', 'trade_dedup', 'hbt_event_types',
        'hbt_time_range', 'order_diagnostics', 'exch_ahead_of_local', 'special_flag_counts',
        'initialization_exclusion', 'switches', 'gaps', 'local_loss', 'local_loss_partitions',
        'binance_depth_meta', 'depth_unavailable_intervals', 'execution_unavailable_intervals',
        'unrebuildable_depth_intervals', 'eligibility', 'artifacts', 'peak_rss_mb',
        'finalize_s', 'out_bytes', 'chrony_uncertified_samples',
    )
    return {
        'verified_partitions_downloaded': verified_parts_count,
        'download_workers': DOWNLOAD_WORKERS,
        'download_bytes_by_exchange': dl_bytes_by_ex,
        'download_bytes_total': total_dl_bytes,
        'download_elapsed_s': round(dl_s_total, 1),
        'download_avg_mbps': round((total_dl_bytes / 1e6) / max(dl_s_total, 0.1), 2),
        'stream_convert_batches_elapsed_s': round(conv_s_total, 1),
        'stream_finalize_elapsed_s': fin_res['elapsed_s'],
        'stream_total_convert_elapsed_s': round(conv_s_total + fin_res['elapsed_s'], 1),
        'state_bytes_before_cleanup': state_bytes_before_cleanup,
        'peak_scratch_bytes': peak_scratch_bytes,
        'converted_output_bytes': du(conv_out),
        'official_hbt_validate_event_order': 'ok',
        'hbt_rows': hbt_rows,
        'hbt_dtypes': hbt_dtypes,
        'hbt_local_ts_range': [hbt_min_local, hbt_max_local],
        'batches': batch_logs,
        'stream_report': {k: stream_report.get(k) for k in keep_keys},
    }


def merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not intervals:
        return []
    ordered = sorted((int(a), int(b)) for a, b in intervals if b is not None and int(b) > int(a))
    if not ordered:
        return []
    merged = [ordered[0]]
    for s, e in ordered[1:]:
        if s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    return merged


def quietness_stats(ts_ns, excluded_intervals: list[tuple[int, int]]) -> dict:
    """Inter-arrival silence stats excluding known/unverified interruption intervals."""
    import numpy as np
    ts = np.asarray(ts_ns, dtype=np.int64)
    if len(ts) < 2:
        return {'events': int(len(ts)), 'pairs': 0}
    dt = np.diff(ts)
    starts, ends = ts[:-1], ts[1:]
    masked = np.zeros(len(dt), dtype=bool)
    for iv_s, iv_e in excluded_intervals:
        # Exclude any inter-event pair overlapping a known interruption interval
        masked |= (starts <= iv_e) & (ends >= iv_s)
    normal_dt = dt[~masked]
    if not len(normal_dt):
        return {'events': int(len(ts)), 'pairs': int(len(dt)), 'normal_pairs': 0}
    return {
        'events': int(len(ts)),
        'normal_pairs': int(len(normal_dt)),
        'excluded_interruption_pairs': int(masked.sum()),
        'normal_gt_1s_count': int((normal_dt > NS).sum()),
        'normal_gt_5s_count': int((normal_dt > 5 * NS).sum()),
        'normal_p99_ms': round(float(np.percentile(normal_dt, 99)) / 1e6, 3),
        'normal_p999_ms': round(float(np.percentile(normal_dt, 99.9)) / 1e6, 3),
        'normal_max_ms': round(float(normal_dt.max()) / 1e6, 3),
    }


_BOOK_PROBE_LOOP = None


def get_book_probe_loop():
    global _BOOK_PROBE_LOOP
    if _BOOK_PROBE_LOOP is None:
        from numba import njit

        @njit
        def loop(hbt, query_ts, vals):
            if hbt.elapse(0) != 0:
                return
            for i in range(len(query_ts)):
                target = query_ts[i]
                now = hbt.current_timestamp
                if target > now:
                    if hbt.elapse(target - now) != 0:
                        break
                d = hbt.depth(0)
                vals[i, 0] = d.best_bid
                vals[i, 1] = d.best_ask

        _BOOK_PROBE_LOOP = loop
    return _BOOK_PROBE_LOOP


def summarize_channel_anomalies(qa_intervals: list[dict], conv_exec_ivs: list[dict]) -> dict:
    """Group anomalies by channel, distinguishing confirmed gaps vs continuity unverified."""
    buckets = {
        'binance_bbo': [],
        'gate_bbo': [],
        'gate_l2': [],
        'gate_trades': [],
    }
    for iv in qa_intervals:
        ex, ch = iv['exchange'], iv['channel']
        if ex == 'binance_futures' and ch in ('bookTicker', 'book_ticker', '@bookTicker'):
            buckets['binance_bbo'].append(iv)
        elif ex == 'gate_futures' and ch == 'futures.book_ticker':
            buckets['gate_bbo'].append(iv)
        elif ex == 'gate_futures' and ch == 'futures.obu':
            buckets['gate_l2'].append(iv)
        elif ex == 'gate_futures' and ch == 'futures.trades':
            buckets['gate_trades'].append(iv)

    out = {}
    for name, items in buckets.items():
        conf = [x for x in items if x['category'] == 'confirmed_data_gap']
        unv = [x for x in items if x['category'] == 'continuity_unverified']
        out[name] = {
            'total_count': len(items),
            'total_duration_ms': round(sum(float(x.get('duration_ms') or 0.0) for x in items), 3),
            'confirmed_data_gap_count': len(conf),
            'confirmed_data_gap_duration_ms': round(sum(float(x.get('duration_ms') or 0.0) for x in conf), 3),
            'continuity_unverified_count': len(unv),
            'continuity_unverified_duration_ms': round(sum(float(x.get('duration_ms') or 0.0) for x in unv), 3),
            'intervals': items,
        }
    out['converter_execution_unavailable_intervals'] = conv_exec_ivs
    return out


def anomaly_and_book() -> dict:
    import numpy as np
    from hftbacktest import (BacktestAsset, ROIVectorMarketDepthBacktest,
                             DEPTH_CLEAR_EVENT, DEPTH_SNAPSHOT_EVENT,
                             DEPTH_EVENT, TRADE_EVENT, LOCAL_EVENT)

    names = artifact_names()
    conv_out = CTX['conv_out']
    day0, day1 = CTX['day_start_ns'], CTX['day_end_ns']
    tick = float(CTX['live_contract']['contract']['order_price_round'])
    qa_ivs = CTX['protocol_audit']['strategy_channel_intervals']
    conv_exec_ivs = list(CTX['stream_report'].get('execution_unavailable_intervals') or [])

    channel_summary = summarize_channel_anomalies(qa_ivs, conv_exec_ivs)

    # Build unified sorted list of non-overlapping interruption episodes for backtest auditing
    raw_bounds = []
    for iv in qa_ivs:
        if iv.get('start_wall_ns') is not None and iv.get('end_wall_ns') is not None:
            raw_bounds.append((int(iv['start_wall_ns']), int(iv['end_wall_ns'])))
    for iv in conv_exec_ivs:
        if iv.get('start_wall_ns') is not None and iv.get('end_wall_ns') is not None:
            raw_bounds.append((int(iv['start_wall_ns']), int(iv['end_wall_ns'])))
    merged_episodes = merge_intervals(raw_bounds)

    # Normal quietness stats (excluding interruptions) for all 4 channels
    with np.load(conv_out / names['binance_bbo']) as z:
        bbo_ts = z['local_ts_ns']
    bbo_excl = [(iv['start_wall_ns'], iv['end_wall_ns']) for iv in channel_summary['binance_bbo']['intervals']
                if iv.get('end_wall_ns') is not None]
    bbo_quiet = quietness_stats(bbo_ts, merge_intervals(bbo_excl))
    del bbo_ts

    with np.load(conv_out / names['gate_l1_bbo']) as z:
        gl1_ts = z['local_ts_ns']
    gl1_excl = [(iv['start_wall_ns'], iv['end_wall_ns']) for iv in channel_summary['gate_bbo']['intervals']
                if iv.get('end_wall_ns') is not None]
    gl1_quiet = quietness_stats(gl1_ts, merge_intervals(gl1_excl))
    del gl1_ts

    hbt_path = conv_out / names['gate_l2_hbt']
    with np.load(hbt_path) as z:
        hbt = z['data']
    is_local = (hbt['ev'] & np.uint64(LOCAL_EVENT)) != 0
    kind = hbt['ev'] & np.uint64(0xFF)
    l2_ts = np.unique(hbt['local_ts'][is_local & ((kind == DEPTH_EVENT) | (kind == DEPTH_SNAPSHOT_EVENT))])
    tr_ts = hbt['local_ts'][is_local & (kind == TRADE_EVENT)]
    snap_ts = [int(x) for x in np.unique(hbt['local_ts'][kind == DEPTH_SNAPSHOT_EVENT])[:50]]
    clear_ts = [int(x) for x in np.unique(hbt['local_ts'][kind == DEPTH_CLEAR_EVENT])[:50]]
    valid_px = hbt['px'][(hbt['px'] > 0.0) & np.isfinite(hbt['px'])]
    roi_low = float(valid_px.min())
    roi_high = float(valid_px.max())
    del hbt, is_local, kind, valid_px
    gc.collect()

    l2_excl = [(iv['start_wall_ns'], iv['end_wall_ns']) for iv in channel_summary['gate_l2']['intervals']
               if iv.get('end_wall_ns') is not None]
    tr_excl = [(iv['start_wall_ns'], iv['end_wall_ns']) for iv in channel_summary['gate_trades']['intervals']
               if iv.get('end_wall_ns') is not None]
    l2_quiet = quietness_stats(l2_ts, merge_intervals(l2_excl))
    tr_quiet = quietness_stats(tr_ts, merge_intervals(tr_excl))
    del l2_ts, tr_ts

    # Book health scan: 1s grid across 24h + exact before/after each interruption episode
    grid_1s = np.arange(day0 + NS, day1, NS, dtype=np.int64)
    ep_queries = []
    for s_ns, e_ns in merged_episodes:
        for t in (s_ns - 10_000_000, s_ns, e_ns, e_ns + 100_000_000, e_ns + NS):
            if day0 <= t < day1:
                ep_queries.append(int(t))
    all_queries = np.unique(np.concatenate([grid_1s, np.asarray(ep_queries, dtype=np.int64)]))

    margin = max(1000.0 * tick, 0.02 * (roi_low + roi_high) / 2.0)
    asset = (BacktestAsset().data([str(hbt_path)]).linear_asset(1.0)
             .tick_size(tick).lot_size(1.0)
             .roi_lb(max(tick, roi_low - margin)).roi_ub(roi_high + margin))
    hbt_eng = ROIVectorMarketDepthBacktest([asset])
    vals = np.full((len(all_queries), 2), np.nan, dtype=np.float64)
    try:
        get_book_probe_loop()(hbt_eng, all_queries, vals)
    finally:
        hbt_eng.close()

    q_map = {int(t): (float(b), float(a)) for t, (b, a) in zip(all_queries, vals)}
    grid_vals = np.array([q_map[int(t)] for t in grid_1s], dtype=np.float64)
    bid, ask = grid_vals[:, 0], grid_vals[:, 1]
    fin = np.isfinite(bid) & np.isfinite(ask) & (bid > 0) & (ask > 0)
    valid = fin & (ask > bid)
    first_valid = int(np.argmax(valid)) if valid.any() else None
    crossed = int((fin & (ask <= bid)).sum())
    invalid_after = int((~valid[first_valid:]).sum()) if first_valid is not None else None

    episode_recovery = []
    for idx, (s_ns, e_ns) in enumerate(merged_episodes):
        pre = q_map.get(s_ns - 10_000_000)
        at_s = q_map.get(s_ns)
        at_e = q_map.get(e_ns)
        post100 = q_map.get(e_ns + 100_000_000)
        post1s = q_map.get(e_ns + NS)

        def fmt_book(pair):
            if pair is None:
                return None
            b, a = pair
            if not (math.isfinite(b) and math.isfinite(a) and b > 0 and a > 0):
                return {'valid': False}
            return {'valid': a > b, 'bid': b, 'ask': a, 'mid': round((b + a) / 2.0, 4),
                    'spread_ticks': round((a - b) / tick, 2)}

        fb_pre = fmt_book(pre)
        fb_end = fmt_book(at_e)
        mid_jump_usdt = None
        mid_jump_bps = None
        if fb_pre and fb_pre.get('valid') and fb_end and fb_end.get('valid'):
            mid_jump_usdt = round(fb_end['mid'] - fb_pre['mid'], 4)
            mid_jump_bps = round(10_000.0 * (fb_end['mid'] - fb_pre['mid']) / fb_pre['mid'], 4)
        ep_channels = sorted({f"{iv['exchange']}:{iv['channel']}({iv['category']})"
                              for iv in qa_ivs
                              if iv.get('end_wall_ns') is not None
                              and iv['start_wall_ns'] <= e_ns and iv['end_wall_ns'] >= s_ns})
        episode_recovery.append({
            'episode_id': idx,
            'start_wall_ns': s_ns,
            'end_wall_ns': e_ns,
            'duration_ms': round((e_ns - s_ns) / 1e6, 3),
            'in_active_trading_session': e_ns >= day0 + TRAIN_MINUTES * 60 * NS,
            'channels': ep_channels,
            'book_pre_10ms': fb_pre,
            'book_at_start': fmt_book(at_s),
            'book_at_end': fb_end,
            'book_post_100ms': fmt_book(post100),
            'book_post_1s': fmt_book(post1s),
            'mid_jump_usdt': mid_jump_usdt,
            'mid_jump_bps': mid_jump_bps,
        })

    CTX.update(
        roi_low=roi_low,
        roi_high=roi_high,
        merged_episodes=merged_episodes,
        episode_recovery=episode_recovery,
        channel_summary=channel_summary,
    )
    return {
        'channel_anomalies': channel_summary,
        'normal_market_quietness_excluding_interruptions': {
            'binance_bbo': bbo_quiet,
            'gate_bbo': gl1_quiet,
            'gate_l2': l2_quiet,
            'gate_trades': tr_quiet,
        },
        'book_health_1s_24h': {
            'samples_total': int(len(grid_1s)),
            'valid_samples': int(valid.sum()),
            'first_valid_ts_ns': int(grid_1s[first_valid]) if first_valid is not None else None,
            'invalid_after_first_valid': invalid_after,
            'crossed_or_locked_samples': crossed,
            'snapshot_timestamps_ns': snap_ts,
            'clear_timestamps_ns': clear_ts,
            'roi_price_range': [roi_low, roi_high],
        },
        'interruption_episodes_book_recovery': episode_recovery,
    }


def dedup_last_by_ts(ts_ns, values):
    """Strictly increasing int64 timestamps keeping the last value at identical timestamps."""
    import numpy as np
    ts = np.asarray(ts_ns, dtype=np.int64)
    vals = np.asarray(values, dtype=np.float64)
    if len(ts) == 0:
        return ts, vals
    if len(ts) > 1 and np.any(np.diff(ts) < 0):
        raise ValueError('timestamps must be nondecreasing')
    keep = np.empty(len(ts), dtype=bool)
    keep[:-1] = ts[:-1] != ts[1:]
    keep[-1] = True
    return ts[keep], vals[keep]


def gate_scenario_from_live(pretrade_mid: float, contract: dict) -> dict:
    """Frozen Gate BTC scenario sizing (mirrors run_gate_btc_origin_glft.scenario)."""
    tick = float(contract['order_price_round'])
    lot = float(contract['order_size_min'])
    ctval = float(contract['quanto_multiplier'])
    if not (tick > 0 and lot > 0 and ctval > 0 and math.isfinite(pretrade_mid) and pretrade_mid > 0):
        raise ValueError('invalid Gate live contract or pretrade mid')
    qty = max(lot, round(TARGET_NOTIONAL_USDT / (pretrade_mid * ctval * lot)) * lot)
    return {
        'tick': tick,
        'lot': lot,
        'ctval': ctval,
        'qty': float(qty),
        'position_cap': float(qty * INVENTORY_MULTIPLE),
        'gamma_code': RISK_AVERSION_PER_USDT * ctval * tick,
        'approx_order_notional_usdt': round(qty * ctval * pretrade_mid, 4),
        'pretrade_mid_usdt': round(pretrade_mid, 4),
        'tick_relative_bps': round(10_000.0 * tick / pretrade_mid, 6),
        'maker_fee_rate': STUDY_MAKER_FEE_RATE,
        'taker_fee_rate': STUDY_TAKER_FEE_RATE,
        'api_maker_fee_rate': float(contract.get('maker_fee_rate') or 0.0),
        'api_taker_fee_rate': float(contract.get('taker_fee_rate') or 0.0),
    }


def descriptive_anomaly_impact(
    annotated: dict,
    episodes: list[tuple[int, int]],
    audit_counts,
    probe_ns,
    probe_rows,
    active_begin_ns: int,
) -> dict:
    """Section 4.3 anomaly impact statistics (descriptive diagnostics only; never alters net PnL)."""
    import numpy as np
    from analyze_gate_1645_fill_quality import HORIZONS, stats

    t_obs = np.asarray(annotated['local_observed_ns'], dtype=np.int64)
    t_exch = np.asarray(annotated['exchange_timestamp_ns'], dtype=np.int64)
    w = np.asarray(annotated['fill_notional_usdt'], dtype=np.float64)
    n_total = int(len(t_obs))
    total_notional = float(np.sum(w)) if n_total else 0.0

    # Map probe_ns -> row index for exact resting-order spanning audit
    p_map = {int(t): idx for idx, t in enumerate(probe_ns)} if probe_ns is not None else {}

    per_episode = []
    in_any_iv = np.zeros(n_total, dtype=bool)
    near_pm5s = np.zeros(n_total, dtype=bool)
    near_pm1s = np.zeros(n_total, dtype=bool)

    for k, (s_ns, e_ns) in enumerate(episodes):
        m_iv = ((t_obs >= s_ns) & (t_obs < e_ns)) | ((t_exch >= s_ns) & (t_exch < e_ns))
        in_any_iv |= m_iv
        if e_ns >= active_begin_ns:
            near_pm5s |= ((t_obs >= s_ns - 5 * NS) & (t_obs <= e_ns + 5 * NS)) | (
                (t_exch >= s_ns - 5 * NS) & (t_exch <= e_ns + 5 * NS))
            near_pm1s |= ((t_obs >= s_ns - NS) & (t_obs <= e_ns + NS)) | (
                (t_exch >= s_ns - NS) & (t_exch <= e_ns + NS))

        def probe_orders(ts_key):
            idx = p_map.get(ts_key)
            if idx is None:
                return None
            row = probe_rows[idx]
            if not math.isfinite(float(row[2])):
                return None
            cb, cs = int(row[2]), int(row[3])
            cc_b, cc_s = int(row[4]), int(row[5])
            return {
                'open_buy_orders': cb,
                'open_sell_orders': cs,
                'open_total_orders': cb + cs,
                'cancellable_buy_orders': cc_b,
                'cancellable_sell_orders': cc_s,
                'position_contracts': float(row[6]),
            }

        enter_ord = probe_orders(s_ns)
        exit_ord = probe_orders(e_ns)
        per_episode.append({
            'episode_id': k,
            'start_wall_ns': s_ns,
            'end_wall_ns': e_ns,
            'duration_ms': round((e_ns - s_ns) / 1e6, 3),
            'in_active_trading_session': e_ns >= active_begin_ns,
            'decisions_during_interval': int(audit_counts[k, 0]) if audit_counts is not None else 0,
            'submits_during_interval': int(audit_counts[k, 1]) if audit_counts is not None else 0,
            'cancels_during_interval': int(audit_counts[k, 2]) if audit_counts is not None else 0,
            'simulated_fills_during_interval': int(m_iv.sum()),
            'orders_entering_interval': enter_ord,
            'orders_exiting_interval': exit_ord,
            'spanning_open_orders_count': (enter_ord['open_total_orders'] if enter_ord is not None else 0),
        })

    def slice_markout(mask):
        cnt = int(mask.sum())
        notional = float(np.sum(w[mask])) if cnt else 0.0
        horizons = {}
        for h in HORIZONS:
            elig = mask & annotated[f'total_fresh_{h}ms'] & np.isfinite(annotated[f'total_markout_{h}ms_bps'])
            horizons[f'{h}ms'] = stats(annotated[f'total_markout_{h}ms_bps'][elig], w[elig]) if elig.any() else None
        return {
            'fills': cnt,
            'fill_fraction': round(cnt / n_total, 6) if n_total else 0.0,
            'notional_usdt': round(notional, 4),
            'notional_fraction': round(notional / total_notional, 6) if total_notional > 0 else 0.0,
            'total_markout': horizons,
        }

    return {
        'diagnostic_disclaimer': (
            'Descriptive diagnostic only; official HBT matching is preserved across interruptions '
            'and fills are never deleted to fabricate alternative net PnL.'
        ),
        'per_episode': per_episode,
        'totals_across_all_episodes': {
            'decisions_during_intervals': int(sum(e['decisions_during_interval'] for e in per_episode)),
            'submits_during_intervals': int(sum(e['submits_during_interval'] for e in per_episode)),
            'cancels_during_intervals': int(sum(e['cancels_during_interval'] for e in per_episode)),
            'fills_during_intervals': int(in_any_iv.sum()),
            'max_spanning_open_orders': max((e['spanning_open_orders_count'] for e in per_episode), default=0),
            'sum_spanning_open_orders_active_episodes': int(sum(
                e['spanning_open_orders_count'] for e in per_episode if e['in_active_trading_session'])),
        },
        'recovery_proximity_descriptive_slices': {
            'inside_interruption': slice_markout(in_any_iv),
            'recovery_pm1s': slice_markout(near_pm1s),
            'recovery_pm5s': slice_markout(near_pm5s),
            'outside_recovery_pm5s': slice_markout(~near_pm5s),
        },
    }


def run_three_models_on_arrays(
    hbt_path: Path,
    bbo_local_ns,
    bbo_mid,
    gl1_local_ns,
    gl1_exch_ns,
    gl1_mid,
    contract: dict,
    day_start_ns: int,
    day_end_ns: int,
    roi_low: float,
    roi_high: float,
    episodes: list[tuple[int, int]],
    trade_minutes: int = TRADE_MINUTES,
    witness_stop_ns: int | None = None,
) -> dict:
    """Core full-day (or synthetic test) 3-model official HBT runner.

    Shared by Kaggle full-day execution and deterministic unit tests.
    """
    import numpy as np
    from hftbacktest import BacktestAsset, Recorder, ROIVectorMarketDepthBacktest
    from core_glft_engine import run_core_glft
    from gate_basis_weighted_moments import exact_moments_5s
    from g0_glft_event_driven import summarize
    from analyze_gate_1645_fill_quality import model_day

    # 1. Enforce exact 0ms Binance extra delay (raw local receipt ns, no +2.5ms, no +1ns, no exch ts)
    binance_visible_ns = np.asarray(bbo_local_ns, dtype=np.int64) + int(BINANCE_EXTRA_VISIBLE_DELAY_NS)
    binance_mid = np.asarray(bbo_mid, dtype=np.float64)
    if BINANCE_EXTRA_VISIBLE_DELAY_NS != 0 or not np.array_equal(binance_visible_ns, bbo_local_ns):
        raise AssertionError('Binance visible clock must equal raw local_ts_ns with 0ms extra delay')

    # 2. Gate L1 unlock clock (strictly conservative filter if any rare exch_ts > local_ts)
    gl1_loc = np.asarray(gl1_local_ns, dtype=np.int64)
    gl1_exc = np.asarray(gl1_exch_ns, dtype=np.int64)
    gl1_m = np.asarray(gl1_mid, dtype=np.float64)
    exch_ahead_mask = gl1_exc > gl1_loc
    exch_ahead_count = int(exch_ahead_mask.sum())
    exch_ahead_max_ns = int((gl1_exc[exch_ahead_mask] - gl1_loc[exch_ahead_mask]).max()) if exch_ahead_count else 0
    if exch_ahead_count:
        keep_clk = ~exch_ahead_mask
        book_locals = np.ascontiguousarray(gl1_loc[keep_clk], dtype=np.int64)
        book_exchanges = np.ascontiguousarray(gl1_exc[keep_clk], dtype=np.int64)
    else:
        book_locals = np.ascontiguousarray(gl1_loc, dtype=np.int64)
        book_exchanges = np.ascontiguousarray(gl1_exc, dtype=np.int64)

    # 3. Strictly causal 600s time-weighted basis & sigma on 5s clock
    bin_ts_dedup, bin_mid_dedup = dedup_last_by_ts(binance_visible_ns, binance_mid)
    gate_ts_dedup, gate_mid_dedup = dedup_last_by_ts(gl1_loc, gl1_m)
    trade_start_ns = day_start_ns + TRAIN_MINUTES * 60 * NS
    trade_steps = int(trade_minutes * 600)
    trade_stop_ns = min(day_end_ns, trade_start_ns + trade_minutes * 60 * NS)
    grid_stop_ns = min(trade_stop_ns, int(min(bin_ts_dedup[-1], gate_ts_dedup[-1])) + 1)
    grid, basis, sigma, cov = exact_moments_5s(
        bin_ts_dedup, bin_mid_dedup, gate_ts_dedup, gate_mid_dedup, trade_start_ns, grid_stop_ns)

    pretrade_mid_arr = gate_mid_dedup[gate_ts_dedup < trade_start_ns]
    if not len(pretrade_mid_arr):
        raise ValueError('missing Gate pretrade L1 midpoints')
    cfg = gate_scenario_from_live(float(np.median(pretrade_mid_arr)), contract)

    def make_asset():
        margin = max(1000.0 * cfg['tick'], 0.02 * (roi_low + roi_high) / 2.0)
        return (
            BacktestAsset()
            .data([str(hbt_path)])
            .linear_asset(cfg['ctval'])
            .constant_order_latency(ORDER_ENTRY_LATENCY_NS, ORDER_RESPONSE_LATENCY_NS)
            .power_prob_queue_model(2.0)
            .no_partial_fill_exchange()
            .trading_value_fee_model(cfg['maker_fee_rate'], cfg['taker_fee_rate'])
            .tick_size(cfg['tick'])
            .lot_size(cfg['lot'])
            .roi_lb(max(cfg['tick'], roi_low - margin))
            .roi_ub(roi_high + margin)
            .last_trades_capacity(20_000)
        )

    core_options = {
        'min_requote_ns': int(MIN_REQUOTE_NS),
        'fit_mode': 'physical_1tick_poisson',
        'wait_book_after_fill': True,
        'post_only_clamp_mode': 'unified',
        'cap_depth_at_visible_levels': False,
        'quote_tick_mult': 1,
        'cross_timestamp_tie_mode': 'microsecond',
        'gate_book_local_ns': book_locals,
        'gate_book_exchange_ns': book_exchanges,
        'depth_is_20_level_snapshot': False,
        'forced_start_ns': 0,
    }

    # 4. Shared calibration witness (cap=0, feed-only)
    t_wit = time.time()
    w_stop = witness_stop_ns if witness_stop_ns is not None else min(trade_stop_ns, day_start_ns + 1800 * NS)
    shared_start = None
    for stop_try in (w_stop, trade_stop_ns):
        witness = ROIVectorMarketDepthBacktest([make_asset()])
        try:
            probe_diag = np.zeros(12, dtype=np.float64)
            raw_probe = run_core_glft(
                witness, binance_visible_ns, binance_mid, trade_steps,
                0.0, grid, basis, sigma, 0, cfg['gamma_code'],
                cfg['tick'], cfg['ctval'], cfg['qty'], 0.0, None,
                stop_try, diagnostics=probe_diag, **core_options,
            )
            if probe_diag[5] == 1 and probe_diag[3] >= 0:
                shared_start = int(raw_probe[-3]) + int(probe_diag[3])
                break
        finally:
            witness.close()
    if shared_start is None:
        raise RuntimeError('no valid first physical GLFT calibration in witness')
    witness_s = round(time.time() - t_wit, 2)
    core_options['forced_start_ns'] = shared_start
    print('WITNESS_DONE', 'shared_start_ns=' + str(shared_start), f'elapsed={witness_s}s', flush=True)

    # Prepare read-only interruption audit arrays & probes
    audit_iv_arr = (np.asarray(episodes, dtype=np.int64).reshape(-1, 2)
                    if episodes else np.empty((0, 2), dtype=np.int64))
    probe_list = sorted({t for s_ns, e_ns in episodes for t in (s_ns, e_ns)
                         if day_start_ns <= t < trade_stop_ns})
    probe_arr = np.asarray(probe_list, dtype=np.int64) if probe_list else None

    effective_parameters = {
        'binance_visible_clock': 'raw_local_ts_ns (exact local receipt timestamp)',
        'binance_extra_visible_delay_ns': int(BINANCE_EXTRA_VISIBLE_DELAY_NS),
        'binance_extra_visible_delay_ms': float(BINANCE_EXTRA_VISIBLE_DELAY_NS) / 1e6,
        'gate_market_data_clock': 'raw_local_ts_ns (exact local receipt timestamp)',
        'order_entry_latency_ns': int(ORDER_ENTRY_LATENCY_NS),
        'order_response_latency_ns': int(ORDER_RESPONSE_LATENCY_NS),
        'order_roundtrip_latency_ms': (ORDER_ENTRY_LATENCY_NS + ORDER_RESPONSE_LATENCY_NS) / 1e6,
        'order_latency_assumption_note': 'Uniform 1ms one-way simulated assumption; not measured live exchange order latency',
        'min_requote_ns': int(core_options['min_requote_ns']),
        'min_requote_ms': float(core_options['min_requote_ns']) / 1e6,
        'native_order_modify_enabled': False,
        'ack_gating_and_cancellable_preserved': True,
        'post_only_gtx_preserved': True,
        'fit_mode': core_options['fit_mode'],
        'wait_book_after_fill': bool(core_options['wait_book_after_fill']),
        'post_only_clamp_mode': core_options['post_only_clamp_mode'],
        'cap_depth_at_visible_levels': bool(core_options['cap_depth_at_visible_levels']),
        'quote_tick_mult': int(core_options['quote_tick_mult']),
        'cross_timestamp_tie_mode': core_options['cross_timestamp_tie_mode'],
        'train_seconds': TRAIN_MINUTES * 60,
        'recalibration_interval_seconds': 5,
        'trade_minutes': int(trade_minutes),
        'trade_steps': int(trade_steps),
        'shared_first_eligible_entry_ns': int(shared_start),
        'scenario': cfg,
        'queue_model': 'power_prob_queue_model(2.0)',
        'exchange_model': 'no_partial_fill_exchange',
        'primary_pnl_basis': 'zero_fee_gross_marked_pnl_usdt',
        'supplementary_fee_rates': {'maker': cfg['maker_fee_rate'], 'taker': cfg['taker_fee_rate']},
        'gate_l1_exch_ahead_filtered_count': exch_ahead_count,
        'gate_l1_exch_ahead_max_ns': exch_ahead_max_ns,
    }

    models_out = {}
    for name, beta, mode in MODELS:
        print('MODEL_START', name, flush=True)
        t_m = time.time()
        engine = ROIVectorMarketDepthBacktest([make_asset()])
        recorder = Recorder(engine.num_assets, trade_steps + 100)
        diag = np.zeros(12, dtype=np.float64)
        audit_counts = np.zeros((len(audit_iv_arr), 3), dtype=np.int64)
        try:
            raw = run_core_glft(
                engine, binance_visible_ns, binance_mid, trade_steps,
                beta, grid, basis, sigma, mode, cfg['gamma_code'],
                cfg['tick'], cfg['ctval'], cfg['qty'], cfg['position_cap'],
                recorder.recorder, trade_stop_ns, probe_arr,
                diagnostics=diag,
                audit_intervals_ns=(audit_iv_arr if len(audit_iv_arr) else None),
                audit_counts=(audit_counts if len(audit_iv_arr) else None),
                **core_options,
            )
            if diag[5] != 1:
                raise RuntimeError(f'{name}: shared GLFT calibration invalid')
            first_fit = int(raw[-3]) + int(diag[3])
            if first_fit != shared_start:
                raise RuntimeError(f'{name}: first_fit_ns {first_fit} != shared {shared_start}')

            summary = summarize(raw, trade_minutes, True, beta, cfg['position_cap'], cfg['qty'],
                                tail_tolerance_samples=25)
            end_time_ns = int(summary['end_time_ns'])
            if end_time_ns < trade_stop_ns - NS:
                raise RuntimeError(f'{name}: replay ended >1s early ({end_time_ns} < {trade_stop_ns})')

            records = recorder.get(0)
            if len(records) < 1 or np.any(np.diff(records['timestamp']) <= 0):
                raise RuntimeError(f'{name}: invalid Recorder timeline')

            trading = dict(summary['trading'])
            active_duration_s = max(len(records), 1) * 0.1
            pos_std_c = float(np.std(records['position']))
            pos_mean_c = float(np.mean(records['position']))
            trading['position_std_contracts'] = pos_std_c
            trading['position_std_base'] = pos_std_c * cfg['ctval']
            trading['position_mean_contracts'] = pos_mean_c
            trading['pos_std_over_order_qty'] = pos_std_c / cfg['qty']
            trading['mean_abs_inventory_contracts'] = float(raw[23]) / active_duration_s
            trading['rms_inventory_contracts'] = math.sqrt(max(0.0, float(raw[24])) / active_duration_s)
            trading['max_abs_position_base'] = float(trading['max_abs_position_contracts']) * cfg['ctval']
            trading['end_position_base'] = float(trading['end_position_contracts']) * cfg['ctval']
            trading['active_inventory_samples'] = int(len(records))
            trading['effective_trading_seconds'] = round(active_duration_s, 1)

            turnover = float(trading['trading_value_usdt'])
            net_2bps = float(trading['end_net_marked_equity_usdt'])
            fee_2bps = float(trading['fee_usdt'])
            balance = float(trading['balance_usdt'])
            end_pos = float(trading['end_position_contracts'])
            end_mid = float(trading['end_mid_usdt_per_btc'])
            gross = net_2bps + fee_2bps
            gross_from_ledger = balance + cfg['ctval'] * end_pos * end_mid

            # Hard accounting verification
            accounting_checks = {
                'fills_count_exact': (int(trading['buy_fills']) + int(trading['sell_fills'])
                                      == int(trading['engine_fills']) == int(len(raw[-5]))),
                'fee_turnover_identity': abs(fee_2bps - cfg['maker_fee_rate'] * turnover) <= 1e-6,
                'gross_ledger_identity': abs(gross - gross_from_ledger) <= 1e-6,
                'net_gross_fee_identity': abs(net_2bps - (gross - fee_2bps)) <= 1e-6,
                'position_within_cap': float(trading['max_abs_position_contracts']) <= cfg['position_cap'] + cfg['qty'] + 1e-9,
                'shared_start_exact': first_fit == shared_start,
            }
            if not all(accounting_checks.values()):
                raise RuntimeError(f'{name}: accounting verification failed: {accounting_checks}')

            trading['gross_marked_pnl_usdt'] = gross
            trading['gross_pnl_per_turnover_bps'] = (10_000.0 * gross / turnover) if turnover > 0 else None
            trading['net_marked_pnl_usdt_2bps'] = net_2bps
            trading['net_pnl_per_turnover_bps_2bps'] = (10_000.0 * net_2bps / turnover) if turnover > 0 else None
            trading['fee_usdt_2bps'] = fee_2bps
            trading['modify_requests'] = 0

            fill = {
                'exchange_timestamp_ns': raw[-5],
                'side': raw[-4],
                'local_observed_ns': raw[-2][1],
                'exec_price': raw[-2][2],
                'exec_qty': raw[-2][3],
            }
            annotated, quality = model_day(
                name, fill, {'trading': trading}, gate_ts_dedup, gate_mid_dedup, day_end_ns, cfg['ctval'])

            effective_begin = max(int(raw[-3]) + TRAIN_MINUTES * 60 * NS, int(shared_start))
            anomaly_impact = descriptive_anomaly_impact(
                annotated, episodes, audit_counts, probe_arr, raw[-2][4], effective_begin)

            origin_filter = {
                k: int(v) for k, v in zip(
                    ('binance_price_update_groups', 'eligible_gap_checks',
                     'above_threshold_checks', 'directional_dominance_checks',
                     'binance_origin_activations', 'active_decisions',
                     'same_timestamp_ties_blocked'),
                    raw[-1],
                )
            }
            origin_filter['microsecond_collisions'] = int(diag[11])

            m_elapsed = round(time.time() - t_m, 1)
            models_out[name] = {
                'quote_mode_name': name,
                'beta': beta,
                'quote_mode': mode,
                'elapsed_s': m_elapsed,
                't0_ns': int(raw[-3]),
                'first_fit_ns': first_fit,
                'effective_trading_start_ns': effective_begin,
                'end_time_ns': end_time_ns,
                'decisions': int(raw[1]),
                'submissions': int(trading['submissions']),
                'cancel_requests': int(trading['cancel_requests']),
                'modify_requests': 0,
                'max_ops_per_sec': int(diag[0]),
                'resting_time_ratio': {
                    'bid': float(diag[1]),
                    'ask': float(diag[2]),
                    'verified_exchange_resting': False,
                },
                'calibration_updates': {
                    'attempts': int(raw[2]),
                    'successes': int(raw[4]),
                    'rejects': int(raw[3]),
                    'initial_parameters': summary.get('initial_parameters'),
                    'last_parameters': summary.get('last_parameters'),
                },
                'trading': trading,
                'accounting_checks': accounting_checks,
                'binance_origin_filter': origin_filter,
                'fill_markout_quality': quality,
                'anomaly_impact': anomaly_impact,
            }
            print('MODEL_DONE', name, f'elapsed={m_elapsed}s',
                  'fills=' + str(trading['engine_fills']),
                  'gross=' + f"{gross:.4f}",
                  'net_2bps=' + f"{net_2bps:.4f}",
                  'max_ops_s=' + str(int(diag[0])), flush=True)
        finally:
            engine.close()
        gc.collect()

    return {
        'witness_elapsed_s': witness_s,
        'basis_grid': {
            'points': int(len(grid)),
            'min_coverage': float(np.min(cov)),
            'initial_basis_usdt': float(basis[0]),
            'initial_sigma_usdt': float(sigma[0]),
            'mean_sigma_usdt': float(np.mean(sigma)),
        },
        'effective_parameters': effective_parameters,
        'models': models_out,
    }


def backtest_models() -> dict:
    import numpy as np
    names = artifact_names()
    conv_out = CTX['conv_out']
    with np.load(conv_out / names['binance_bbo']) as z:
        bbo_local = z['local_ts_ns']
        bbo_mid = z['mid_px']
    with np.load(conv_out / names['gate_l1_bbo']) as z:
        gl1_local = z['local_ts_ns']
        gl1_exch = z['exch_ts_ns']
        gl1_mid = z['mid_px']

    res = run_three_models_on_arrays(
        conv_out / names['gate_l2_hbt'],
        bbo_local, bbo_mid,
        gl1_local, gl1_exch, gl1_mid,
        CTX['live_contract']['contract'],
        CTX['day_start_ns'], CTX['day_end_ns'],
        CTX['roi_low'], CTX['roi_high'],
        CTX['merged_episodes'],
        trade_minutes=TRADE_MINUTES,
    )
    return res


def summarize_overall(stages: dict) -> dict:
    stage_ok = {name: (stages.get(name) or {}).get('ok') is True for name in STAGES}
    dq = (stages.get('discover_and_qa') or {}).get('result') or {}
    fc = (stages.get('fetch_and_convert') or {}).get('result') or {}
    bt = (stages.get('backtest_models') or {}).get('result') or {}
    models = bt.get('models') or {}
    all_models_ok = (len(models) == len(MODELS) and all(
        all((models.get(n) or {}).get('accounting_checks', {}).values())
        for n, _, _ in MODELS
    ))
    eff = bt.get('effective_parameters') or {}
    params_verified = (
        eff.get('binance_extra_visible_delay_ns') == 0
        and eff.get('min_requote_ns') == 0
        and eff.get('order_entry_latency_ns') == 1_000_000
        and eff.get('order_response_latency_ns') == 1_000_000
        and eff.get('native_order_modify_enabled') is False
    )
    proto = dq.get('protocol_audit') or {}
    complete = (
        all(stage_ok.values())
        and fc.get('official_hbt_validate_event_order') == 'ok'
        and proto.get('evidence_binding_verified') is True
        and all_models_ok
        and params_verified
    )
    return {
        'diagnostic_full_day_complete': complete,
        'stage_ok': stage_ok,
        'all_three_models_accounting_passed': all_models_ok,
        'effective_latency_and_cooldown_verified': params_verified,
        'protocol_evidence_binding_verified': proto.get('evidence_binding_verified') is True,
        'stage3_strict_qa_passed': bool(proto.get('stage3_strict_qa_passed')),
        'stage4_authorized': False,
        'authorization_scope': (
            'First full-day (2026-11-23 UTC) diagnostic engineering backtest only; '
            'does not certify Stage 3 strict QA pass or authorize batch Stage 4 production runs.'
        ),
    }


def main() -> int:
    SCRATCH.mkdir(parents=True, exist_ok=True)
    try:
        if (stage('setup', setup)['ok']
                and stage('discover_and_qa', discover_and_qa)['ok']
                and stage('fetch_and_convert', fetch_and_convert)['ok']
                and stage('anomaly_and_book', anomaly_and_book)['ok']):
            stage('backtest_models', backtest_models)
    finally:
        shutil.rmtree(AUTH, ignore_errors=True)
        report['auth_dir_removed'] = not AUTH.exists()
        report['scratch_bytes_before_cleanup'] = du(SCRATCH)
        shutil.rmtree(SCRATCH, ignore_errors=True)
        report['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        report['summary'] = summarize_overall(report['stages'])
        report['status'] = 'complete' if report['summary']['diagnostic_full_day_complete'] else 'failed'
        save()
    print('FULL_DAY_DONE status=' + report['status'],
          json.dumps(report['summary'], default=str), flush=True)
    return 0 if report['status'] == 'complete' else 3


if __name__ == '__main__':
    raise SystemExit(main())
