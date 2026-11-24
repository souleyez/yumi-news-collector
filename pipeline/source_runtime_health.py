"""Metadata-only observation; called by the existing pipeline ExecStopPost.

No collector, editor, image generation, scheduler or production operation lives here.
The observation command is read-only. --publish uses the original worker uploader.
"""
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import sqlite3
import subprocess
import sys
import uuid
from zoneinfo import ZoneInfo

from runtime_config import EXPECTED_HOST as HOST
NAME = 'category-source-health'
FILE = NAME + '.v1.json'
SCHEMA = 'yumi.source-runtime-health.v1'
CATEGORIES = ['ai-general', 'frontier-models', 'github-projects', 'gaming', 'hardware']
MAX_BYTES = 65536


def encoded(v):
    return (json.dumps(v, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()


def digest(b):
    return hashlib.sha256(b).hexdigest()


def optional(path):
    try:
        if path.is_symlink() or path.stat().st_size > 8 * 1024 * 1024:
            raise ValueError('SOURCE_HEALTH_FILE_INVALID')
        return json.loads(path.read_bytes())
    except FileNotFoundError:
        return None


def systemd_time(value, zone):
    if not value:
        return None
    # systemd displays the host's timezone abbreviation. GNU date interprets
    # "CST" as US Central, even on a China-time host; use actual /etc/localtime.
    parts = value.split()
    parsed = datetime.strptime(' '.join(parts[1:3]), '%Y-%m-%d %H:%M:%S')
    return parsed.replace(tzinfo=zone).astimezone(timezone.utc).isoformat()


def unit_status(unit):
    props = ['ActiveState', 'Result', 'ExecMainExitTimestamp', 'UnitFileState']
    r = subprocess.run(['systemctl', 'show', unit, *['--property=' + p for p in props]],
                       capture_output=True, text=True, timeout=4)
    if r.returncode:
        return {'known': False}
    v = dict(line.split('=', 1) for line in r.stdout.splitlines() if '=' in line)
    completed = None
    if v.get('ExecMainExitTimestamp'):
        with Path('/etc/localtime').open('rb') as tz:
            completed = systemd_time(v['ExecMainExitTimestamp'], ZoneInfo.from_file(tz))
    return {'known': True, 'active': v.get('ActiveState'), 'enabled': v.get('UnitFileState'),
            'result': v.get('Result'), 'lastCompletedAt': completed}


def lock_state(path):
    try:
        with path.open('r') as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 'held'
            fcntl.flock(f, fcntl.LOCK_UN)
            return 'free'
    except OSError:
        return 'unknown'


def service_observation(root, now=None):
    """Fixed root pre-command: only four public systemd metadata reads."""
    if os.geteuid() != 0 or socket.gethostname() != HOST:
        raise ValueError('SOURCE_HEALTH_METADATA_HOST_CHANGED')
    now = now or datetime.now(timezone.utc)
    v = {'schema': 'yumi.source-service-state.v1', 'host': HOST,
         'observedAt': now.isoformat(), 'units': {}}
    for stem in ['yumi-trendradar', 'yumi-news-pipeline']:
        for suffix in ['.service', '.timer']:
            v['units'][stem + suffix] = unit_status(stem + suffix)
    target = root / 'source-service-state.json'
    temp = root / ('source-service-state.' + str(os.getpid()) + '.tmp')
    with temp.open('xb') as f:
        f.write(encoded(v)); f.flush(); os.fsync(f.fileno())
    temp.chmod(0o644)  # only nonsecret unit metadata; original worker can read it
    os.replace(temp, target)
    return {'serviceMetadataWritten': True, 'modelCalls': 0}


def observed_units(root, now=None):
    now = now or datetime.now(timezone.utc)
    v = optional(root / 'source-service-state.json') or {}
    try:
        age = (now - datetime.fromisoformat(v['observedAt'])).total_seconds()
        valid = (v.get('schema') == 'yumi.source-service-state.v1' and v.get('host') == HOST
                 and 0 <= age <= 900)
    except (KeyError, ValueError, TypeError):
        valid = False
    def read(name):
        value = dict(v.get('units', {}).get(name, {})) if valid else {'known': False}
        if name == 'yumi-news-pipeline.service' and os.environ.get('YUMI_HEALTH_WORKER_RESULT') in ['0', '1']:
            complete = os.environ.get('YUMI_HEALTH_WORKER_COMPLETED_AT')
            try:
                age = (now - datetime.fromisoformat(complete)).total_seconds()
                if 0 <= age <= 90:
                    value.update(known=True, result='success' if os.environ['YUMI_HEALTH_WORKER_RESULT'] == '0' else 'exit-code',
                                 active='health-publishing', lastCompletedAt=complete)
            except (ValueError, TypeError):
                pass
        return value
    return read


def finish_worker_health(root, modules, exit_code):
    """Same original ExecStart environment, after releasing its own worker lock."""
    env = {**os.environ, 'YUMI_HEALTH_WORKER_RESULT': '0' if exit_code == 0 else '1',
           'YUMI_HEALTH_WORKER_COMPLETED_AT': datetime.now(timezone.utc).isoformat()}
    try:
        r = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--root', str(root),
                            '--modules', str(modules), '--publish'], env=env,
                           capture_output=True, text=True, timeout=25)
        # Only the helper's protocol JSON, never traceback or subprocess stderr.
        for line in r.stdout.splitlines():
            try:
                value = json.loads(line)
                if isinstance(value, dict):
                    safe = {k: value[k] for k in ['requestId', 'sha256', 'originalBytesReadbackVerified', 'status', 'error'] if k in value}
                    print(json.dumps({'sourceHealth': safe}))
            except (ValueError, TypeError):
                pass
    except Exception:
        print(json.dumps({'sourceHealth': {'status': 'unknown', 'error': 'SOURCE_HEALTH_PUBLICATION_UNVERIFIED'}}))


def observe(root, now=None, units=unit_status, daily_reader=None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('SOURCE_HEALTH_TIME_INVALID')
    old = optional(root / 'source-runtime-health.json') or {}
    services = {}
    for key, stem in [('collector', 'yumi-trendradar'), ('editor', 'yumi-news-pipeline')]:
        service, timer = units(stem + '.service'), units(stem + '.timer')
        success = service.get('lastCompletedAt') if service.get('result') == 'success' else None
        services[key] = {**service, 'timerEnabled': timer.get('enabled'),
                         'timerActive': timer.get('active'),
                         'lastSuccessAt': success or old.get('services', {}).get(key, {}).get('lastSuccessAt')}
    worker = {'lockState': lock_state(root / 'worker.lock')}
    db_path = root / 'delivery.sqlite3'
    with sqlite3.connect(db_path.resolve(strict=True).as_uri() + '?mode=ro', uri=True,
                         timeout=2) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        models = dict(db.execute('SELECT state,count(*) FROM models GROUP BY state'))
        uploads = dict(db.execute('SELECT state,count(*) FROM uploads GROUP BY state'))
        if daily_reader is None:
            from daily_supply_status import from_ledger
            daily_reader = from_ledger
        daily = daily_reader(db, now=now)
        total = db.execute("SELECT count(*) FROM media_topics WHERE state='synced'").fetchone()[0]
    counts, missing, targets = [daily.get(k) for k in
        ['editorialCategoryCounts', 'editorialCategoryMissing', 'editorialCategoryTargets']]
    if not all(isinstance(v, dict) and all(type(v.get(k)) is int and v[k] >= 0 for k in CATEGORIES)
               for v in [counts, missing, targets]):
        raise ValueError('SOURCE_HEALTH_DAILY_COUNTS_INVALID')
    health = optional(root / 'health.json') or {}
    codes = [x for x in health.get('blockers', []) if isinstance(x, str)
             and re.fullmatch('[A-Z][A-Z0-9_:]{0,99}', x)]
    receipt = optional(root / 'category-production-supply-upload-receipt.json')
    supply = {'known': receipt is not None, 'lastVerifiedAt': None, 'blockerCodes': codes}
    if receipt:
        supply.update(lastVerifiedAt=receipt.get('createdAt'),
                      originalBytesReadbackVerified=receipt.get('originalBytesReadbackVerified') is True,
                      sha256=receipt.get('sha256'), version=receipt.get('version'))
    stamp = now.astimezone(timezone.utc).isoformat()
    return {'schema': SCHEMA, 'logicalName': NAME, 'contractVersion': 1, 'host': HOST,
            'observedAt': stamp, 'timezone': 'Asia/Singapore',
            'editionDate': now.astimezone(ZoneInfo('Asia/Singapore')).date().isoformat(),
            'services': services, 'worker': worker,
            'models': {'states': models, 'pending': models.get('pending', 0),
                       'unknown': sum(n for s, n in models.items() if s not in ['complete', 'pending'])},
            'uploads': {'states': uploads, 'pending': uploads.get('pending', 0),
                        'unknown': sum(n for s, n in uploads.items() if s not in ['synced', 'pending', 'failed']),
                        'failed': uploads.get('failed', 0)},
            'synced': {'total': total, 'day': daily['editionDate'], 'counts': counts,
                       'targets': targets, 'missing': missing,
                       'meaning': 'existing from_ledger canonical-story dedup; not finished media or live adoption'},
            'supply': supply, 'modelCalls': 0, 'productionCalls': 0}


def publish(root, snapshot, ledger, uploader, reader):
    """One original telemetry outbox survives unknown acknowledgement/readback."""
    from news_pipeline import atomic, DATASETS
    state_path = root / 'source-health-publication.json'
    previous = optional(state_path)
    if previous and previous.get('state') != 'completed':
        manifest = previous['manifest']
        path = root / manifest['relativePath']
        if (manifest.get('fileName') != FILE or manifest.get('kind') != 'source-health'
            or not path.resolve(strict=True).is_relative_to(root.resolve(strict=True))):
            raise ValueError('SOURCE_HEALTH_ORIGINAL_CHANGED')
        raw = path.read_bytes()
        if digest(raw) != manifest['sha256']:
            raise ValueError('SOURCE_HEALTH_ORIGINAL_CHANGED')
    else:
        raw = encoded(snapshot)
        sha = digest(raw)
        relative = 'media/outbox/ai-news/category-source-health/' + sha + '.json'
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() != raw:
                raise ValueError('SOURCE_HEALTH_ORIGINAL_CHANGED')
        else:
            with path.open('xb') as f:
                path.chmod(0o600)
                f.write(raw)
        manifest = {'key': digest(encoded([NAME, sha])), 'category': 'ai-news',
                    'dataset': DATASETS['ai-news'], 'kind': 'source-health',
                    'relativePath': relative, 'fileName': FILE, 'mediaType': 'text/plain',
                    'sha256': sha, 'sizeBytes': len(raw),
                    'requestId': str(uuid.uuid5(uuid.NAMESPACE_URL, NAME + ':' + sha)),
                    'processingMode': 'store-only', 'reviewRequired': True, 'broadcastApproved': False}
        atomic(state_path, {'state': 'unknown', 'manifest': manifest})
    if not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('SOURCE_HEALTH_SIZE_INVALID')
    receipt = uploader.sync(manifest)['receipt']
    base = '/datasets/' + manifest['dataset'] + '/uploads/' + receipt['id']
    current = uploader.api.request(base)['data']
    if any(current.get(k) != manifest[k] for k in ['fileName', 'sha256', 'sizeBytes', 'processingMode']):
        raise ValueError('SOURCE_HEALTH_READBACK_CHANGED')
    back = reader(uploader.api, base + '/original')
    if back != raw:
        raise ValueError('SOURCE_HEALTH_READBACK_CHANGED')
    atomic(state_path, {'state': 'completed', 'manifest': manifest,
                       'originalBytesReadbackVerified': True, 'assetId': receipt['assetId']})
    return {'requestId': manifest['requestId'], 'sha256': digest(raw), 'originalBytesReadbackVerified': True}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, default=Path('/var/lib/yumi-news-pipeline'))
    p.add_argument('--modules', type=Path, required=True)
    p.add_argument('--publish', action='store_true')
    p.add_argument('--service-state', action='store_true')
    args = p.parse_args()
    if socket.gethostname() != HOST:
        raise ValueError('SOURCE_HEALTH_HOST_CHANGED')
    sys.path.insert(0, str(args.modules))
    if args.service_state:
        print(json.dumps(service_observation(args.root)))
        return
    snapshot = observe(args.root, units=observed_units(args.root)) if args.publish else observe(args.root)
    if not args.publish:
        print(json.dumps(snapshot))
        return
    # Same worker lock and credential already provisioned to this same service.
    # No credential retrieval, rotation, new token, model or collection fallback.
    with (args.root / 'worker.lock').open('r') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        from news_pipeline import Ledger, Syncer, JsonHTTP, API, credential, atomic
        from supply_upload import read_original
        ledger = Ledger(args.root)
        try:
            atomic(args.root / 'source-runtime-health.json', snapshot)
            result = publish(args.root, snapshot, ledger,
                             Syncer(ledger, JsonHTTP(API, credential('datahub-token'))), read_original)
            print(json.dumps(result))
        finally:
            ledger.db.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Never print HTTP objects, credential paths, responses or SQLite payloads.
        code = str(error)
        if not re.fullmatch('[A-Z][A-Z0-9_]{0,99}', code):
            code = 'SOURCE_HEALTH_OBSERVATION_OR_UPLOAD_UNVERIFIED'
        print(json.dumps({'status': 'unknown', 'error': code}))
        sys.exit(1)
