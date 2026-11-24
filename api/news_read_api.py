"""Read-only news HTTP adapter; never edits source/production databases."""
import argparse
from contextlib import closing
import hashlib
import json
import os
import re
import sqlite3
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

MAX_SAFE = 9007199254740991
MAX_PAGE = 1024 * 1024
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'pipeline'))
from runtime_config import DATAHUB_API as AGENT_BASE, DATASETS, require_datahub_configuration

DATASET = DATASETS['ai-news']
AUTH_URL = AGENT_BASE + '/datasets/' + DATASET + '/items'
PUBLIC_BASE = os.environ.get('YUMI_NEWS_PUBLIC_BASE', '').rstrip('/')


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def project(material):
    identity = material.get('canonicalStoryId') or material['sourceUrl']
    news_id = digest(['yumi-news-v1', identity])
    native = dict(material)
    images = []
    for image in material.get('images', []):
        asset = image.get('asset', {})
        images.append({k: image.get(k) for k in (
            'sha256', 'mediaType', 'width', 'height', 'synthetic', 'generationProvider',
            'credit', 'sourcePageUrl', 'alt', 'safeZone')})
        images[-1]['assetRef'] = {'datasetId': asset.get('dataset'),
                                 'assetId': asset.get('assetId')}
        images[-1]['imageId'] = image.get('sha256')
        images[-1]['readPath'] = '/api/v1/media/' + str(image.get('sha256', ''))
        images[-1]['mime'] = image.get('mediaType') or asset.get('mime')
        if PUBLIC_BASE:
            images[-1]['url'] = PUBLIC_BASE + images[-1]['readPath']
    # Retain native source metadata as an authorized same-service detail, without
    # copying any customer-media URL into a public unauthenticated endpoint.
    return {'newsId': news_id, 'title': material['title'], 'isDeleted': False,
            'bodyMarkdown': '\n\n'.join(p['text'] for p in material['paragraphs']),
            'bodyText': '\n\n'.join(p['text'] for p in material['paragraphs']),
            'bodyKind': 'edited-broadcast-manuscript', 'paragraphs': material['paragraphs'],
            'fullTextAvailable': False,
            'category': material.get('editorialCategory'), 'tags': material.get('tags', []),
            'editorialCollection': material.get('editorialCollection'),
            'contentKind': material.get('contentKind'),
            'commercialContent': material.get('commercialContent', False),
            'sponsor': material.get('sponsor'), 'disclosure': material.get('disclosure'),
            'source': {'url': material['sourceUrl'], 'publication': material.get('publication'),
                       'createdAt': material.get('createdAt')},
            'originalArticle': {'availability': 'not-published-by-this-adapter'},
            'images': images, 'imageSequence': material.get('imageSequence', []),
            'mediaReadiness': material.get('mediaReadiness'),
            'manuscript': material.get('manuscript'),
            'manuscriptSha256': material.get('manuscriptSha256'),
            'productionBindings': {'materialKey': material.get('materialKey') or material['key'],
                                   'scriptSha256': material['scriptSha256']},
            'nativeMaterial': native}


class Index:
    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.Lock()
        with closing(self.connect()) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS records (news_id TEXT PRIMARY KEY, revision INTEGER UNIQUE NOT NULL, hash TEXT NOT NULL, body TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY CHECK(id=1), value INTEGER NOT NULL)')
            db.execute('INSERT OR IGNORE INTO counter VALUES(1,0)')

    def connect(self):
        return sqlite3.connect(self.path, timeout=10)

    def refresh(self, loader):
        # Lock before source observation: a late stale GET cannot roll back a
        # fresher revision committed by another request.
        with self.lock:
            materials = loader()
            latest = {}
            for material in materials:
                row = project(material)
                prev = latest.get(row['newsId'])
                if prev is None or material['createdAt'] > prev['createdAt']:
                    latest[row['newsId']] = material
            with closing(self.connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                for news_id, material in sorted(latest.items()):
                    body = project(material)
                    content_hash = digest(body)
                    old = db.execute('SELECT hash FROM records WHERE news_id=?', (news_id,)).fetchone()
                    if old and old[0] == content_hash:
                        continue
                    revision = db.execute('SELECT value FROM counter WHERE id=1').fetchone()[0] + 1
                    if revision > MAX_SAFE:
                        raise ValueError('REVISION_EXHAUSTED')
                    body.update(revision=revision, contentSha256=content_hash)
                    db.execute('UPDATE counter SET value=? WHERE id=1', (revision,))
                    db.execute('INSERT INTO records VALUES(?,?,?,?) ON CONFLICT(news_id) DO UPDATE SET revision=excluded.revision, hash=excluded.hash, body=excluded.body',
                               (news_id, revision, content_hash, encoded(body).decode()))
                # Absence never creates a tombstone: source has no retract feed.

    def page(self, after=0, after_id='', size=100, budget=MAX_PAGE):
        if type(after) is not int or not 0 <= after <= MAX_SAFE or not 1 <= size <= 100:
            raise ValueError('INVALID_PAGINATION')
        with closing(self.connect()) as db, db:
            rows = db.execute('SELECT body FROM records WHERE revision>? OR (revision=? AND news_id>?) ORDER BY revision,news_id LIMIT ?',
                              (after, after, after_id, size + 1)).fetchall()
        result = {'schema': 'yumi.news-read.v1', 'items': [], 'hasMore': False,
                  'deletesSupported': False}
        for raw, in rows[:size]:
            item = json.loads(raw)
            candidate = {**result, 'items': result['items'] + [item], 'hasMore': True}
            if len(encoded(candidate)) > budget:
                if not result['items']:
                    raise OverflowError('SINGLE_RECORD_EXCEEDS_PAGE_LIMIT')
                result['hasMore'] = True
                return result
            result['items'].append(item)
        result['hasMore'] = len(rows) > size
        return result

    def media_asset(self, image_hash):
        with closing(self.connect()) as db, db:
            for raw, in db.execute('SELECT body FROM records'):
                for image in json.loads(raw)['images']:
                    asset = image.get('assetRef', {})
                    if image['sha256'] == image_hash and asset.get('datasetId') == DATASET and asset.get('assetId'):
                        return asset['assetId']
        return None


def load_source(root):
    # Existing worker publishes this projection atomically. Reading it avoids
    # opening the live source SQLite WAL/shared-memory files from an HTTP unit.
    path = Path(root)
    with path.open('rb') as source:
        raw = source.read(8 * 1024 * 1024 + 1)
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError('SOURCE_TOO_LARGE')
    snapshot = json.loads(raw)
    if snapshot.get('schema') != 'yumi.category-supply-projection.v1':
        raise ValueError('SOURCE_SCHEMA_INVALID')
    for item in snapshot['materials']:
        native = (json.dumps(item['paragraphs'], ensure_ascii=False, sort_keys=True, indent=2)+'\n').encode()
        if hashlib.sha256(native).hexdigest() != item['scriptSha256']:
            raise ValueError('SOURCE_SCRIPT_HASH_INVALID')
    return snapshot['materials']


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def authorized(header):
    # Authenticate against the existing DataHub dataset ACL with the caller's
    # standard bearer. No new inbound key, copied secret, cookie or SSH identity.
    if not header or not header.startswith('Bearer ') or not 8 < len(header) <= 8192:
        return 401
    try:
        request = urllib.request.Request(AUTH_URL, headers={'Authorization': header, 'Accept': 'application/json'})
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=8) as response:
            return 200 if response.status == 200 else 403
    except urllib.error.HTTPError as error:
        return error.code if error.code in (401, 403) else 503
    except OSError:
        return 503


def read_media(asset_id, image_hash, bearer):
    opener = urllib.request.build_opener(NoRedirect())
    headers = {'Authorization': bearer, 'Accept': 'application/json'}
    path = '/datasets/' + DATASET + '/items/asset/' + urllib.parse.quote(asset_id, safe='')
    with opener.open(urllib.request.Request(AGENT_BASE + path, headers=headers), timeout=30) as response:
        detail_raw = response.read(MAX_PAGE + 1)
    if len(detail_raw) > MAX_PAGE:
        raise ValueError('ASSET_DETAIL_TOO_LARGE')
    detail = json.loads(detail_raw)
    detail = detail.get('data', detail)
    original = detail['original']
    download = original['downloadPath']
    if download.startswith(AGENT_BASE + '/'):
        download = download[len(AGENT_BASE):]
    elif download.startswith('/api/datafoundation/agent/v1/'):
        download = download[len('/api/datafoundation/agent/v1'):]
    if not re.fullmatch('/datasets/' + DATASET + r'/uploads/[a-zA-Z0-9-]+/original', download):
        raise ValueError('ASSET_PATH_INVALID')
    if original['sha256'] != image_hash:
        raise ValueError('ASSET_HASH_INVALID')
    headers['Accept'] = 'application/octet-stream'
    with opener.open(urllib.request.Request(AGENT_BASE + download, headers=headers), timeout=45) as response:
        raw = response.read(10 * MAX_PAGE + 1)
    # Standard original-download APIs may send application/octet-stream.
    # Identify the verified bytes, never relay the upstream type blindly.
    mime = image_mime(raw)
    if mime is None or len(raw) > 10 * MAX_PAGE:
        raise ValueError('IMAGE_TYPE_OR_SIZE_INVALID')
    if len(raw) != original['sizeBytes'] or hashlib.sha256(raw).hexdigest() != image_hash:
        raise ValueError('IMAGE_HASH_INVALID')
    return raw, mime


def image_mime(raw):
    if raw.startswith(b'\x89PNG\r\n\x1a\n'):
        return 'image/png'
    if raw.startswith(b'\xff\xd8\xff'):
        return 'image/jpeg'
    if raw.startswith((b'GIF87a', b'GIF89a')):
        return 'image/gif'
    if raw.startswith(b'RIFF') and raw[8:12] == b'WEBP':
        return 'image/webp'
    return None


def handler(index, loader):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # Never log request headers, query payloads or credentials.

        def send_json(self, status, value):
            raw = encoded(value)
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            url = urllib.parse.urlsplit(self.path)
            if url.path == '/healthz':
                self.send_json(200, {'status': 'ok', 'schema': 'yumi.news-read.v1'})
                return
            is_media = re.fullmatch(r'/api/v1/media/([a-f0-9]{64})', url.path)
            if url.path != '/api/v1/news' and not is_media:
                self.send_json(404, {'error': 'NOT_FOUND'})
                return
            status = authorized(self.headers.get('Authorization'))
            if status != 200:
                self.send_json(status, {'error': 'DATASET_ACCESS_REQUIRED' if status != 503 else 'AUTHORITY_UNAVAILABLE'})
                return
            try:
                if is_media:
                    if url.query:
                        raise ValueError('MEDIA_QUERY_INVALID')
                    image_hash = is_media[1]
                    asset_id = index.media_asset(image_hash)
                    if not asset_id:
                        self.send_json(404, {'error': 'IMAGE_NOT_FOUND'})
                        return
                    raw, mime = read_media(asset_id, image_hash, self.headers.get('Authorization'))
                    self.send_response(200)
                    self.send_header('Content-Type', mime)
                    self.send_header('Content-Length', str(len(raw)))
                    self.send_header('Cache-Control', 'private, no-store')
                    self.send_header('X-Content-SHA256', image_hash)
                    self.end_headers()
                    self.wfile.write(raw)
                    return
                args = urllib.parse.parse_qs(url.query, keep_blank_values=True, max_num_fields=8)
                if set(args) - {'pageSize', 'afterValue', 'afterId'} or any(len(v) != 1 for v in args.values()):
                    raise ValueError('INVALID_PAGINATION')
                after = args.get('afterValue', ['0'])[0]
                size = args.get('pageSize', ['100'])[0]
                after_id = args.get('afterId', [''])[0]
                if not re.fullmatch(r'[0-9]{1,16}', after) or not re.fullmatch(r'[0-9]{1,3}', size) or (after_id and not re.fullmatch('[a-f0-9]{64}', after_id)):
                    raise ValueError('INVALID_PAGINATION')
                index.refresh(loader)
                self.send_json(200, index.page(int(after), after_id, int(size)))
            except ValueError as error:
                allowed = {'ASSET_DETAIL_TOO_LARGE', 'ASSET_PATH_INVALID', 'ASSET_HASH_INVALID',
                           'IMAGE_TYPE_OR_SIZE_INVALID', 'IMAGE_HASH_INVALID'}
                code = str(error) if str(error) in allowed else 'INVALID_REQUEST_OR_REVISION'
                self.send_json(400, {'error': code})
            except OverflowError:
                self.send_json(413, {'error': 'SINGLE_RECORD_EXCEEDS_PAGE_LIMIT'})
            except Exception as error:
                import sys
                print('NEWS_SOURCE_EXCEPTION:' + type(error).__name__ + ':' + str(getattr(error, 'sqlite_errorname', 'non-sqlite')), file=sys.stderr, flush=True)
                self.send_json(503, {'error': 'NEWS_SOURCE_UNAVAILABLE'})
    return Handler


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bind', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=18692)
    parser.add_argument('--index', required=True, type=Path)
    parser.add_argument('--source', required=True, type=Path, help='Category supply JSON file')
    args = parser.parse_args()
    require_datahub_configuration()
    server = ThreadingHTTPServer((args.bind, args.port), handler(Index(args.index), lambda: load_source(args.source)))
    server.serve_forever()
