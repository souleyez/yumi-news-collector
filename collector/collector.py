"""Isolated public-data collector. No model, credentials, HTTP server or Data Hub writes."""
import argparse
from datetime import datetime, timedelta
import hashlib
import html
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
import uuid
from zoneinfo import ZoneInfo

TZ = ZoneInfo('Asia/Singapore')
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'pipeline'))
from runtime_config import DATASETS

DATASET = DATASETS['ai-news']
GROUPS = {
    'AI与模型': r'\bAI\b|人工智能|大模型|智能体|Agent|DeepSeek|Qwen|千问|Gemini|Claude|GPT|LLM|OpenAI',
    '数字人与内容制作': r'数字人|虚拟人|TTS|语音合成|视频生成|配音|ComfyUI|MuseTalk',
    '数据与知识库': r'数据库|知识库|向量|\bRAG\b|PostgreSQL|Data Hub|Embedding',
    '开源与开发工具': r'开源|GitHub|编程|开发者|Docker|Linux|\bMCP\b|Coding|Developer|Open Source',
    '产业应用': r'机器人|制造业|工业|自动化|企业服务|SaaS|招聘',
}


class PlainText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts, self.skip = [], 0

    def handle_starttag(self, tag, attrs):
        if tag in {'script', 'style'}:
            self.skip += 1

    def handle_endtag(self, tag):
        if tag in {'script', 'style'} and self.skip:
            self.skip -= 1

    def handle_data(self, value):
        if not self.skip:
            self.parts.append(value)


def plain(value):
    parser = PlainText()
    parser.feed(value or '')
    return re.sub(r'\s+', ' ', ' '.join(parser.parts)).strip()


def canonical_url(value):
    if not isinstance(value, str) or any(c in value for c in '\r\n\t<>'):
        raise ValueError('INVALID_SOURCE_URL')
    parsed = urlsplit(value)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError('HTTPS_SOURCE_URL_REQUIRED')
    if parsed.port not in {None, 443}:
        raise ValueError('NONSTANDARD_SOURCE_PORT')
    host = parsed.hostname.lower()
    if host in {'localhost', '127.0.0.1', '::1'} or host.endswith(('.local', '.internal')):
        raise ValueError('PUBLIC_SOURCE_URL_REQUIRED')
    query = urlencode([(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
                       if not k.lower().startswith('utm_') and k.lower() not in {'fbclid', 'gclid'}])
    return urlunsplit(('https', host, parsed.path or '/', query, ''))


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    temporary.replace(path)


def read_db(path, query):
    if not path.is_file():
        return []
    connection = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(query)]
    finally:
        connection.close()


def crawl_health(state, day):
    result = {}
    for kind, table, status_table in [('news', 'crawl_records', 'crawl_source_status'),
                                       ('rss', 'rss_crawl_records', 'rss_crawl_status')]:
        path = state / 'output' / kind / f'{day}.db'
        records = read_db(path, f'SELECT * FROM {table} ORDER BY id DESC LIMIT 1')
        statuses = read_db(path, f'SELECT * FROM {status_table} WHERE crawl_record_id=(SELECT MAX(id) FROM {table})')
        result[kind] = {'latest': records[0] if records else None,
                        'sources': statuses,
                        'success': sum(str(row.get('status', '')).lower() == 'success' for row in statuses),
                        'failed': sum(str(row.get('status', '')).lower() != 'success' for row in statuses)}
    return result


def export_pool(state, now):
    day = now.date().isoformat()
    rows = []
    for kind, query in [
        ('news', 'SELECT n.title,n.url,n.rank,n.first_crawl_time,n.last_crawl_time,p.name source FROM news_items n JOIN platforms p ON p.id=n.platform_id ORDER BY n.rank,n.id'),
        ('rss', 'SELECT r.title,r.url,r.summary,r.published_at,r.last_crawl_time,f.name source FROM rss_items r JOIN rss_feeds f ON f.id=r.feed_id ORDER BY r.published_at DESC')]:
        for item in read_db(state / 'output' / kind / f'{day}.db', query):
            item['kind'] = kind
            rows.append(item)
    unique, stale, invalid = {}, 0, 0
    for row in rows:
        try:
            url = canonical_url(row['url'])
        except (ValueError, TypeError):
            invalid += 1
            continue
        published = row.get('published_at')
        freshness = 'publication_time_unknown'
        if published:
            try:
                stamp = datetime.fromisoformat(published.replace('Z', '+00:00'))
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=ZoneInfo('UTC'))
                age = now - stamp
                if age > timedelta(hours=24) or age < -timedelta(minutes=10):
                    stale += 1
                    continue
                freshness = 'within_24h'
            except ValueError:
                freshness = 'invalid_publication_time'
        title = plain(row['title'])[:500]
        summary = plain(row.get('summary', ''))[:500]
        # HN RSS description commonly contains only discussion links/vote counts.
        evidence = 'rss_excerpt' if summary and row['source'] != 'Hacker News' else 'headline_only'
        identifier = hashlib.sha256(url.encode()).hexdigest()[:24]
        groups = [name for name, pattern in GROUPS.items() if re.search(pattern, title, re.I)]
        record = {'id': identifier, 'title': title, 'url': url, 'source': row['source'],
                  'kind': row['kind'], 'summary': summary, 'publishedAt': published or None,
                  'lastObservedAt': row.get('last_crawl_time'), 'freshness': freshness,
                  'evidenceLevel': evidence, 'keywordGroups': groups,
                  'rank': row.get('rank'), 'editorialState': 'candidate_not_broadcast_approved'}
        previous = unique.get(identifier)
        if not previous or (previous['evidenceLevel'] == 'headline_only' and evidence == 'rss_excerpt'):
            unique[identifier] = record
    items = sorted(unique.values(), key=lambda row: (-bool(row['keywordGroups']),
                   -len(row['keywordGroups']), row['rank'] or 999, row['id']))
    return {'schema': 'yumi.trendradar.candidates.v1', 'capturedAt': now.isoformat(),
            'rawRecordCount': len(rows), 'staleOrFutureRssExcluded': stale,
            'invalidSourceUrlsExcluded': invalid, 'freshUniqueCount': len(items),
            'keywordCandidateCount': sum(bool(row['keywordGroups']) for row in items),
            'modelCalls': 0, 'items': items}


def package_selected(state, selection_path):
    """Only explicit, actually selected source records become outbox documents."""
    selection = json.loads(selection_path.read_text(encoding='utf-8'))
    assert selection['schema'] in {'yumi.trendradar.selection.v1', 'yumi.trendradar.selection.v2'}
    assert selection['selectionMethod'] in {'human', 'existing_model_trial'}
    from category_routing import classify, load_catalog, multi_image_gate
    category = selection.get('category', 'ai-news')
    if category not in {'ai-news', 'entertainment'}:
        raise ValueError('UNKNOWN_SELECTED_CATEGORY')
    legacy = selection['schema'] == 'yumi.trendradar.selection.v1'
    if not legacy and 'category' not in selection:
        raise ValueError('EXPLICIT_SELECTED_CATEGORY_REQUIRED')
    if legacy and (category != 'ai-news' or any(
            row.get('category', 'ai-news') != 'ai-news' or
            ('entertainment' in classify(row['title']) and 'ai-news' not in classify(row['title']))
            for row in selection['items'])):
        raise ValueError('LEGACY_SELECTION_CANNOT_ROUTE_ENTERTAINMENT')
    catalog = load_catalog(os.environ.get('YUMI_CATEGORIES_PATH', '/etc/yumi-trendradar/categories.json'))
    dataset = catalog['categories'][category]['dataset']
    if not dataset:
        raise ValueError('SELECTED_DATASET_NOT_BOUND')
    outbox = state / 'outbox' if legacy else state / 'outbox' / category
    outbox.mkdir(parents=True, exist_ok=True)
    receipts = []
    for item in selection['items']:
        if item.get('category', category) != category:
            raise ValueError('MIXED_CATEGORY_SELECTION_REJECTED')
        media_gate = None
        if category == 'entertainment':
            media_gate = multi_image_gate(item, category, catalog, selection_path.parent)
            if media_gate['imageCount'] < catalog['categories'][category]['minimumImages']:
                raise ValueError('ENTERTAINMENT_REQUIRES_THREE_DISTINCT_IMAGE_FILES')
            if media_gate['imageCount'] != len(item.get('images', [])):
                raise ValueError('INVALID_OR_DUPLICATE_ENTERTAINMENT_IMAGE')
        url = canonical_url(item['url'])
        text = (f"# {plain(item['title'])}\n\n"
                "状态：选题待核验，不是可直接播出的稿件。\n\n"
                f"来源：{plain(item['source'])}\n\n原文链接：<{url}>\n\n"
                f"选题理由：{plain(item['reason'])}\n\n"
                f"待核内容：{plain(item['verifyNext'])}\n\n"
                f"采集摘要（非全文）：{plain(item.get('summary', ''))}\n\n"
                "配图：尚未自动取得，须附来源和许可；原创导图可另行制作。\n"
                "未授权公开发布，未自动调用模型、解析或生成数字人。\n")
        related_images = []
        if media_gate is not None:
            text = text.replace('配图：尚未自动取得，须附来源和许可；原创导图可另行制作。',
                                '配图：本包附多张原件及出处；使用范围仍需审核，未自动取得公开播出资格。')
            text += '\n## 配图原件与出处\n\n'
            for image in item['images']:
                source_path = (selection_path.parent / image['localPath']).resolve()
                image_body = source_path.read_bytes()
                image_sha = hashlib.sha256(image_body).hexdigest()
                if image_sha != image['sha256']:
                    raise ValueError('IMAGE_CHANGED_AFTER_GATE')
                image_mime = 'image/png' if image_body.startswith(b'\x89PNG\r\n\x1a\n') else 'image/jpeg'
                if image.get('mime', image_mime) != image_mime:
                    raise ValueError('IMAGE_MIME_SIGNATURE_MISMATCH')
                image_name = f"image-{image_sha[:24]}{'.png' if image_mime == 'image/png' else '.jpg'}"
                image_path = outbox / image_name
                if image_path.exists() and image_path.read_bytes() != image_body:
                    raise ValueError('IMMUTABLE_IMAGE_CONFLICT')
                if not image_path.exists():
                    image_path.write_bytes(image_body)
                image_manifest = {'schema':'yumi.datahub.outbox-item.v1','category':category,
                    'dataset':dataset,'requestId':str(uuid.uuid5(uuid.NAMESPACE_URL,f'yumi:{dataset}:image:{image_sha}')),
                    'fileName':image_name,'mediaType':image_mime,
                    'sha256':image_sha,'sizeBytes':len(image_body),'processingMode':'store-only',
                    'sourceUrl':image['sourcePageUrl'],'originalImageUrl':image.get('imageUrl'),
                    'credit':image['credit'],'caption':image['caption'],'rightsStatus':image.get('rightsStatus','review-required'),
                    'state':'pending_authorized_sync','broadcastApproved':False}
                image_manifest_path = outbox / f'{image_name}.json'
                if image_manifest_path.exists():
                    assert json.loads(image_manifest_path.read_text(encoding='utf-8')) == image_manifest
                else:
                    atomic_json(image_manifest_path, image_manifest)
                related_images.append(image_manifest)
                text += f"- {plain(image['caption'])}：{image_name}；署名：{plain(image['credit'])}；出处：{image['sourcePageUrl']}\n"
        body = text.encode('utf-8')
        sha = hashlib.sha256(body).hexdigest()
        request_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f'yumi:{dataset}:{url}:{sha}'))
        name = f'topic-{hashlib.sha256(url.encode()).hexdigest()[:24]}-{sha[:12]}.md'
        target = outbox / name
        if target.exists() and target.read_bytes() != body:
            raise ValueError('IMMUTABLE_TOPIC_CONFLICT')
        if not target.exists():
            target.write_bytes(body)
        manifest = {'schema': 'yumi.datahub.outbox-item.v1', 'dataset': dataset,
                    'requestId': request_id, 'fileName': name, 'mediaType': 'text/markdown',
                    'sha256': sha, 'sizeBytes': len(body), 'processingMode': 'store-only',
                    'sourceUrl': url, 'selectionMethod': selection['selectionMethod'],
                    'state': 'pending_authorized_sync', 'broadcastApproved': False}
        if not legacy:
            manifest.update({'category':category,'relatedImageFiles':related_images,
                             'mediaGate':media_gate})
        receipt_path = outbox / f'{name}.json'
        if receipt_path.exists():
            assert json.loads(receipt_path.read_text(encoding='utf-8')) == manifest
        else:
            atomic_json(receipt_path, manifest)
        receipts.append(manifest)
    return receipts


def finalize(state, now, before=None):
    after = crawl_health(state, now.date().isoformat())
    for kind in ['news', 'rss']:
        if not after[kind]['latest'] or (before is not None and after[kind]['latest'] == before[kind]['latest']):
            raise RuntimeError(f'NO_NEW_{kind.upper()}_CRAWL_RECEIPT')
    reports = sorted((state / 'output' / 'html' / now.date().isoformat()).glob('*.html'))
    if not reports or reports[-1].stat().st_size < 100:
        raise RuntimeError('NO_ACTUAL_HTML_REPORT')
    pool = export_pool(state, now)
    if pool['rawRecordCount'] == 0:
        raise RuntimeError('NO_ACTUAL_SOURCE_RECORDS')
    run_id = now.strftime('%Y%m%dT%H%M%S%z')
    atomic_json(state / 'runs' / f'{run_id}.json', pool)
    atomic_json(state / 'latest-candidates.json', pool)
    category_counts = categorize_saved_pool(state, pool)
    all_success = after['news']['success'] == 11 and after['rss']['success'] == 3
    status = {'schema': 'yumi.trendradar.health.v1', 'status': 'healthy' if all_success else 'degraded',
              'runId': run_id, 'completedAt': datetime.now(TZ).isoformat(),
              'sources': after, 'rawRecordCount': pool['rawRecordCount'],
              'freshUniqueCount': pool['freshUniqueCount'], 'keywordCandidateCount': pool['keywordCandidateCount'],
              'categoryCandidates': category_counts,
              'actualHtmlReport': str(reports[-1]), 'modelCalls': 0,
              'mode': 'fresh_crawl' if before is not None else 'cached_recovery_no_recrawl',
              'dataHubSync': 'pending_machine_authorization', 'lastSuccessfulOutput': run_id}
    atomic_json(state / 'health.json', status)
    print(json.dumps({key: value for key, value in status.items() if key != 'sources'}, ensure_ascii=False))


def categorize_saved_pool(state, pool=None):
    from category_routing import load_catalog, split_pool
    if pool is None:
        pool = json.loads((state / 'latest-candidates.json').read_text(encoding='utf-8'))
    catalog = load_catalog(os.environ.get('YUMI_CATEGORIES_PATH', '/etc/yumi-trendradar/categories.json'))
    outputs = split_pool(pool, catalog)
    counts = {}
    for category, output in outputs.items():
        atomic_json(state / 'categories' / category / 'latest-candidates.json', output)
        counts[category] = output['candidateCount']
    atomic_json(state / 'categories' / 'routing-status.json', {
        'schema': 'yumi.category-routing-status.v1', 'sourceCapturedAt': pool['capturedAt'],
        'categories': [{key: row[key] for key in ['category','dataset','categoryName','minimumImages','candidateCount']}
                       for row in outputs.values()], 'dataHubWrites': 0, 'modelCalls': 0})
    return counts


def collect(state, recover_cached=False):
    from trendradar.__main__ import NewsAnalyzer
    now = datetime.now(TZ)
    before = crawl_health(state, now.date().isoformat())
    analyzer = NewsAnalyzer()
    config = analyzer.ctx.config
    assert not config['AI_ANALYSIS']['ENABLED'] and not config['AI_TRANSLATION']['ENABLED']
    assert analyzer.ctx.filter_method == 'keyword' and not config['ENABLE_NOTIFICATION']
    config['DEBUG'] = True  # Upstream normally swallows exceptions and exits zero.
    if recover_cached:
        # Explicit method change after a render-only failure. Never recrawl.
        try:
            data = analyzer._load_analysis_data()
            if not data:
                raise RuntimeError('NO_CACHED_DATA_TO_RECOVER')
            results, names, _, _, _, _, _ = data
            rss = analyzer.storage_manager.get_latest_rss_data(now.date().isoformat())
            rss_stats, rss_new, raw_rss, rss_urls = None, None, None, set()
            if rss:
                analyzer._rss_source_total = len(rss.id_to_name)
                analyzer._rss_source_failed = len(rss.failed_ids)
                rss_stats, rss_new, raw_rss, rss_urls = analyzer._process_rss_data_by_mode(rss)
            analyzer._execute_mode_strategy(analyzer._get_mode_strategy(), results, names, [],
                rss_items=rss_stats, rss_new_items=rss_new, raw_rss_items=raw_rss, rss_new_urls=rss_urls)
        finally:
            analyzer.ctx.cleanup()
    else:
        analyzer.run()
    finalize(state, now, None if recover_cached else before)


def main():
    import fcntl
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', type=Path, default=Path('/var/lib/yumi-trendradar'))
    parser.add_argument('--selection', type=Path)
    parser.add_argument('--recover-cached', action='store_true')
    parser.add_argument('--categorize-cached', action='store_true')
    args = parser.parse_args()
    args.state.mkdir(parents=True, exist_ok=True)
    with (args.state / 'collector.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit('ANOTHER_COLLECTOR_IS_RUNNING')
        try:
            if args.categorize_cached:
                print(json.dumps({'categoryCandidates': categorize_saved_pool(args.state), 'dataHubWrites': 0, 'modelCalls': 0}))
            elif args.selection:
                print(json.dumps({'packagedTopics': len(package_selected(args.state, args.selection)),
                                  'dataHubWrites': 0, 'modelCalls': 0}))
            else:
                collect(args.state, args.recover_cached)
        except Exception as error:
            # Error text can contain a third party URL/body. Keep operational logs bounded.
            atomic_json(args.state / 'last-failure.json', {'at': datetime.now(TZ).isoformat(),
                        'errorType': type(error).__name__, 'previousSuccessfulOutputPreserved': True})
            print(json.dumps({'status': 'failed', 'errorType': type(error).__name__}))
            raise


if __name__ == '__main__':
    main()
