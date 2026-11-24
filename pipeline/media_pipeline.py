"""Finite, dated, 3-5-image news packages for the NEXT programme's review queue.

No broadcaster, audience Q&A, arbitrary agent, or Data Hub processing request.
Imports the preserved original worker only for its bounded editor and ledger.
"""
import argparse
import base64
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import escape
from io import BytesIO
import ipaddress
import json
import os
from pathlib import Path
import random
import re
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
import uuid
import warnings
from zoneinfo import ZoneInfo

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE/'deps'))
sys.path.insert(0, str(HERE.parent))
from bs4 import BeautifulSoup
from PIL import Image, ImageOps
from news_pipeline import (API, DATASETS, MODEL_API, Editor, GateError, JsonHTTP,
    Ledger, NoRedirect, UID, atomic, credential, encoded, immutable, local_file,
    require, sha, source_fingerprint, timestamp)
from runtime_config import DATAHUB_ORIGIN, EXPECTED_HOST, WORKBUDDY_REFERENCE_FILE
from category_supply import CATEGORIES, MODEL_BUDGET_DAILY, SOURCE_MAX_AGE_DAYS, classify, supply

Image.MAX_IMAGE_PIXELS = 25_000_000
warnings.simplefilter('error', Image.DecompressionBombWarning)
ARTICLE_HOSTS = {'huggingface.co', 'www.cerebras.ai', 'github.blog',
                 'www.anthropic.com', 'openai.com', 'www.news.cn', 'www.thepaper.cn','blog.playstation.com',
                 'github.com','news.samsung.com','www.raspberrypi.com','www.razer.com'}
IMAGE_RULES = {
    'github.com':{'raw.githubusercontent.com':('/',),'opengraph.githubassets.com':('/',),'repository-images.githubusercontent.com':('/',)},
    'news.samsung.com':{'img.global.news.samsung.com':('/global/wp-content/uploads/',)},
    'www.raspberrypi.com':{'www.raspberrypi.com':('/app/uploads/',)},
    'www.razer.com':{'press.razer.com':('/wp-content/uploads/',),'www.razer.com':('/newsroom/wp-content/uploads/',)},
    'blog.playstation.com': {'blog.playstation.com':('/tachyon/',), 'live.staticflickr.com':('/',)},
    'huggingface.co': {'www.datocms-assets.com': ('/64837/',),
        'cdn-uploads.huggingface.co': ('/production/uploads/',),
        'huggingface.co': ('/datasets/', '/blog/assets/')},
    'www.cerebras.ai': {'cdn.sanity.io': ('/images/e4qjo92p/',)},
    'github.blog': {'github.blog': ('/wp-content/uploads/',)},
    'www.anthropic.com': {'www-cdn.anthropic.com': ('/images/',),
        'cdn.sanity.io': ('/images/',)},
    'openai.com': {'images.ctfassets.net': ('/',)},
    'www.news.cn': {'www.news.cn': ('/ent/',)},
    'www.thepaper.cn': {'imgpai.thepaper.cn': ('/newpai/image/',)},
}
MIMES = {'JPEG': ('image/jpeg', '.jpg'), 'PNG': ('image/png', '.png'),
         'WEBP': ('image/webp', '.webp')}
MAX_IMAGE = 8*1024*1024
DISCOVERY = 'https://www.news.cn/ent/'


def plain(text):
    return re.sub(r'\s+', ' ', str(text or '')).strip()


def public_url(value, hosts):
    require(isinstance(value, str) and len(value) <= 3000 and not re.search(r'[\x00-\x20<>]', value), 'PUBLIC_URL_INVALID')
    u = urllib.parse.urlsplit(value)
    require(u.scheme == 'https' and u.hostname in hosts and not u.username and not u.password
            and u.port in {None, 443}, 'PUBLIC_HOST_REQUIRED')
    require(not any(p in {'.', '..'} for p in urllib.parse.unquote(u.path).split('/')), 'PUBLIC_PATH_INVALID')
    return urllib.parse.urlunsplit(('https', u.hostname, u.path, u.query, ''))


def image_url(source, value):
    source_host = urllib.parse.urlsplit(public_url(source, ARTICLE_HOSTS)).hostname
    rules = IMAGE_RULES[source_host]
    value = urllib.parse.urljoin(source, value)
    legacy = urllib.parse.urlsplit(value)
    # Publisher's verified permanent media migration; no arbitrary redirect following.
    if source_host=='www.razer.com' and legacy.hostname=='press.razer.com' and legacy.scheme in {'http','https'} and not legacy.username and not legacy.password and legacy.port in {None,80,443} and legacy.path.startswith('/wp-content/uploads/'):
        value=urllib.parse.urlunsplit(('https','www.razer.com','/newsroom'+legacy.path,legacy.query,''))
    url = public_url(value, rules)
    u = urllib.parse.urlsplit(url)
    require(any(u.path.startswith(p) for p in rules[u.hostname]), 'IMAGE_PATH_NOT_ALLOWED')
    require(not re.search(r'avatar|logo|icon|qrcode|loading|thumbnail', u.path, re.I), 'DECORATIVE_IMAGE')
    return url


class Reader:
    def __init__(self, interval=True):
        self.last = 0
        self.interval = interval
        self.robots = {}
        self.opener = urllib.request.build_opener(NoRedirect())
        self.requests = 0

    def raw(self, url, hosts, limit, types):
        url = public_url(url, hosts)
        host = urllib.parse.urlsplit(url).hostname
        addresses = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        require(addresses and all(ipaddress.ip_address(a[4][0]).is_global for a in addresses), 'NONPUBLIC_ADDRESS')
        if self.interval and self.last:
            time.sleep(max(0, self.last + random.uniform(5, 10) - time.monotonic()))
        self.last = time.monotonic()
        self.requests += 1
        req = urllib.request.Request(url, headers={'User-Agent': 'YumiNewsCollector/1.0',
            'Accept': 'application/vnd.github+json' if types=={'application/json'} else 'image/png,image/jpeg,image/webp,text/html,text/plain;q=0.9'})
        try:
            with self.opener.open(req, timeout=25) as r:
                require(r.status == 200, 'PUBLIC_HTTP_REJECTED')
                mime = r.headers.get_content_type()
                require(mime in types, 'PUBLIC_MIME_REJECTED')
                body = r.read(limit+1)
                require(0 < len(body) <= limit, 'PUBLIC_BODY_LIMIT')
                return body, mime, r.headers.get_content_charset() or 'utf-8'
        except urllib.error.HTTPError as e:
            raise GateError('PUBLIC_HTTP_'+str(e.code)) from None
        except (OSError, ValueError):
            raise GateError('PUBLIC_TRANSPORT_UNAVAILABLE') from None

    def read(self, url, hosts, limit, types):
        host = urllib.parse.urlsplit(public_url(url, hosts)).hostname
        if host not in self.robots:
            robot = urllib.robotparser.RobotFileParser()
            try:
                data, _, charset = self.raw('https://'+host+'/robots.txt', hosts, 128*1024,
                    {'text/plain', 'text/html', 'application/octet-stream'})
                robot.parse(data.decode(charset, errors='replace').splitlines())
            except GateError as e:
                if str(e) != 'PUBLIC_HTTP_404':
                    raise
                robot.parse([])
            self.robots[host] = robot
        require(self.robots[host].can_fetch('YumiNewsCollector', url), 'ROBOTS_DISALLOWED')
        return self.raw(url, hosts, limit, types)

    def html(self, url):
        body, _, charset = self.read(url, ARTICLE_HOSTS, 2*1024*1024,
            {'text/html', 'application/xhtml+xml'})
        return body.decode(charset, errors='replace'), sha(body)


def json_articles(soup):
    values = []
    def visit(value):
        if isinstance(value, list):
            for x in value: visit(x)
        elif isinstance(value, dict):
            kind = value.get('@type', [])
            if isinstance(kind, str): kind = [kind]
            if set(kind) & {'Article', 'NewsArticle', 'BlogPosting'}: values.append(value)
            if '@graph' in value: visit(value['@graph'])
    for node in soup.find_all('script', type='application/ld+json'):
        try: visit(json.loads(node.string or node.get_text()))
        except (ValueError, TypeError): pass
    return values


def parse_date(raw):
    value = plain(raw)
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return stamp.date().isoformat(), stamp.isoformat() if stamp.tzinfo else None
    except ValueError:
        pass
    for fmt in ['%B %d, %Y', '%b %d %Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M']:
        try: return datetime.strptime(value, fmt).date().isoformat(), None
        except ValueError: pass
    raise GateError('PUBLICATION_DATE_INVALID')


def publication(soup, source, now=None):
    """Only publication-labelled metadata or publisher-specific date headers.

    Generic <time> nodes are deliberately ignored: they include user comments.
    Modified date, URL date and RSS observation are not publication proof.
    """
    host = urllib.parse.urlsplit(source).hostname
    evidence, modified = [], []
    for meta in soup.find_all('meta'):
        name = meta.get('property', meta.get('name', '')).lower()
        if name in {'article:published_time', 'datepublished', 'publishdate', 'pubdate'} and meta.get('content'):
            evidence.append({'kind': 'publication_meta:'+name, 'raw': meta['content']})
        if name in {'article:modified_time', 'datemodified'} and meta.get('content'):
            modified.append(meta['content'])
    for article in json_articles(soup):
        if article.get('datePublished'):
            evidence.append({'kind': 'article_jsonld:datePublished', 'raw': article['datePublished']})
        if article.get('dateModified'): modified.append(article['dateModified'])
    # The text search is bounded to the header preceding the actual article.
    root = soup.find('main') or soup.body or soup
    header = plain(root.get_text(' ', strip=True))[:1600]
    if host == 'huggingface.co':
        match = re.search(r'Published\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})', header)
        if match: evidence.append({'kind': 'visible_published_header', 'raw': match[1]})
    elif host == 'www.cerebras.ai':
        match = re.search(r'\b([A-Z][a-z]{2}\s+\d{1,2}\s+20\d{2})\b', header)
        if match: evidence.append({'kind': 'visible_article_header', 'raw': match[1]})
    elif host == 'www.news.cn':
        match = re.search(r'\b(20\d{2}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s*来源', header)
        if match: evidence.append({'kind': 'visible_article_header', 'raw': match[1]})
    require(evidence, 'PUBLICATION_DATE_UNVERIFIED')
    accepted = []
    for item in evidence:
        try:
            day, precise = parse_date(item['raw'])
            accepted.append({**item, 'date': day, 'timestamp': precise})
        except GateError:
            continue
    require(accepted, 'PUBLICATION_DATE_UNVERIFIED')
    require(len({x['date'] for x in accepted}) == 1, 'PUBLICATION_DATE_CONFLICT')
    day = accepted[0]['date']
    today = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo('Asia/Singapore')).date()
    age = today - datetime.fromisoformat(day).date()
    require(timedelta(0) <= age <= timedelta(days=7), 'PUBLICATION_STALE_OR_FUTURE')
    precise = next((x['timestamp'] for x in accepted if x['timestamp']), None)
    if precise:
        require(datetime.fromisoformat(precise) <= (now or datetime.now(timezone.utc))+timedelta(minutes=10),
            'PUBLICATION_STALE_OR_FUTURE')
    return {'state': 'verified_publisher_publication', 'publishedDate': day, 'publishedAt': precise,
        'precision': 'timestamp_with_zone' if precise else 'calendar_day_only',
        'timezone': 'explicit_in_timestamp' if precise else 'not_declared_no_timezone_assumed',
        'evidence': accepted, 'modifiedRaw': sorted(set(modified)),
        'freshness': 'recent_report_not_today' if age.days else 'publisher_date_today',
        'eventDateStatus': 'not_inferred_from_publication_or_collection'}


def extract_article(html, url, html_sha=None):
    public_url(url, ARTICLE_HOSTS)
    soup = BeautifulSoup(html, 'html.parser')
    dated = publication(soup, url)
    host = urllib.parse.urlsplit(url).hostname
    selectors = {
        'huggingface.co': '.blog-content',
        'www.thepaper.cn': '[class*="cententWrap__"]',
        'www.news.cn': '#detail',
        'github.blog': '.post__content',
        'news.samsung.com': '.single_contents',
        'www.raspberrypi.com': 'article.c-blog-post-content',
    }
    content = soup.select_one(selectors[host]) if host in selectors else (soup.find('article') or soup.find('main'))
    require(content is not None, 'ARTICLE_SCOPE_REQUIRED')
    title_node = (content.find('h1') if host=='www.raspberrypi.com' else None) or soup.find('h1') or soup.find('title')
    require(title_node is not None, 'ARTICLE_TITLE_REQUIRED')
    title = plain(title_node.get_text(' ', strip=True))[:300]
    images = []
    for img in content.find_all('img'):
        if img.find_parent(['nav', 'footer', 'aside']): continue
        raw = img.get('data-src') or img.get('data-original') or img.get('src')
        if not raw: continue
        try: resolved = image_url(url, raw)
        except (GateError, ValueError): continue
        if any(x['url'] == resolved for x in images): continue
        parent = img.find_parent(['figure', 'p'])
        caption = plain(parent.get_text(' ', strip=True))[:400] if parent else ''
        caption_kind = 'image_paragraph_or_figure' if caption else 'none'
        if not caption:
            following = img.find_next('p')
            if following and content in following.parents and len(plain(following.get_text())) <= 300:
                caption = plain(following.get_text(' ', strip=True))
                caption_kind = 'adjacent_body_context_not_verified_caption'
        images.append({'url': resolved, 'alt': plain(img.get('alt'))[:400],
            'captionOriginal': caption, 'captionEvidence':caption_kind,
            'declaredWidth':int(img.get('width')) if str(img.get('width','')).isdigit() else None,
            'declaredHeight':int(img.get('height')) if str(img.get('height','')).isdigit() else None,
            'relationEvidence': 'embedded_in_article_body',
            'sourcePageUrl': url, 'rightsStatus': 'review-required', 'synthetic': False})
    for node in content.find_all(['script', 'style', 'nav', 'footer', 'aside']): node.decompose()
    paragraphs = []
    for node in content.find_all(['p', 'h1', 'h2', 'h3', 'li', 'figcaption']):
        text = plain(node.get_text(' ', strip=True))
        if 12 <= len(text) <= 10000 and text not in paragraphs:
            paragraphs.append(text[:1800])
    body_length=sum(map(len,paragraphs))
    photo_report=bool(host=='www.news.cn' and body_length>=80 and len(images)>=3)
    require(body_length>=160 or photo_report,'ARTICLE_BODY_REQUIRED')
    # Image completeness is a presentation state, never a text collection gate.
    text = plain(content.get_text(' ', strip=True))
    credits = re.findall(r'(?:新华社记者|新华社发|摄影[：:]|图片[来供]源[：:]|图[／/：:]|\S{2,12}记者\s)[^。\n]{0,100}?(?:摄|供图|摄影报道)', text)
    credit = '；'.join(dict.fromkeys(credits))[:500] or '原文配图；具体摄影/绘图署名及使用范围须审阅原文'
    design=re.search(r'Designs by [^.]{2,100}\.',text)
    if design: credit=design.group(0)
    for img in images:
        specific=re.search(r'图源[：:]\s*(.{2,100})',img['captionOriginal'])
        img['credit'] = specific.group(1) if specific else credit
    return {'url': url, 'title': title, 'source': host, 'publishedAt': dated['publishedAt'],
        'publishedDate': dated['publishedDate'], 'publication': dated, 'htmlSha256': html_sha or sha(html.encode()),
        'fetchedAt': timestamp(), 'evidenceLevel': 'publisher_photo_report' if photo_report and body_length<160 else 'publisher_article_body',
        'paragraphs': [{'id': 's'+str(i+1), 'text': p} for i,p in enumerate(paragraphs[:48])],
        'imageCandidates': images[:12], 'eventDateStatus': 'review_in_original_body',
        'eventDateStatements':[p[:350] for p in paragraphs if re.search(r'\d{1,2}月\d{1,2}日',p)][:3]}


def image_info(body, mime):
    require(0 < len(body) <= MAX_IMAGE, 'IMAGE_SIZE_LIMIT')
    try:
        with Image.open(BytesIO(body)) as original:
            require(original.format in MIMES and MIMES[original.format][0] == mime, 'IMAGE_MIME_MISMATCH')
            require(getattr(original, 'n_frames', 1) == 1, 'ANIMATED_IMAGE_REQUIRES_REVIEW')
            require(320 <= original.width <= 12000 and 180 <= original.height <= 12000
                and original.width*original.height <= 25_000_000, 'IMAGE_DIMENSIONS_REJECTED')
            original.verify()
        with Image.open(BytesIO(body)) as original:
            original.load()
            display = ImageOps.exif_transpose(original).convert('RGB')
            pixels = list(display.convert('L').resize((9,8)).get_flattened_data())
            bits = ''.join('1' if pixels[y*9+x] > pixels[y*9+x+1] else '0' for y in range(8) for x in range(8))
            return {'sha256':sha(body), 'sizeBytes':len(body),'mediaType':mime,
                'extension':MIMES[original.format][1],'width':display.width,'height':display.height,
                'dhash':format(int(bits,2),'016x'), 'fullDecodeVerified':True}
    except GateError: raise
    except Exception:
        raise GateError('IMAGE_DECODE_FAILED') from None


def visually_duplicate(a, b):
    return a['sha256'] == b['sha256'] or (int(a['dhash'],16)^int(b['dhash'],16)).bit_count() <= 3


def collect_images(reader, ledger, doc):
    result, errors = [], []
    started=time.monotonic();attempted=0;unavailable_hosts=set()
    def narrow(candidate):
        w,h=candidate.get('declaredWidth'),candidate.get('declaredHeight')
        return bool(w and h and w/h>3.5)
    # Prefer full diagrams/scenes; do not spend the quota on five formula strips.
    for candidate in sorted(doc['imageCandidates'],key=narrow):
        if len(result) == 5: break
        if attempted>=6 or time.monotonic()-started>=90:
            errors.append('IMAGE_COLLECTION_BUDGET_REACHED');break
        if len(result)>=3 and narrow(candidate): break
        try:
            url = image_url(doc['url'], candidate['url'])
            host=urllib.parse.urlsplit(url).hostname
            if host in unavailable_hosts:continue
            attempted+=1
            data, mime, _ = reader.read(url, IMAGE_RULES[urllib.parse.urlsplit(doc['url']).hostname],
                MAX_IMAGE, {x[0] for x in MIMES.values()})
            info = image_info(data, mime)
            if any(visually_duplicate(info, old) for old in result):
                errors.append('DUPLICATE_IMAGE_SKIPPED'); continue
            relative = 'media/images/'+info['sha256']+info['extension']
            immutable(local_file(ledger.root, relative), data)
            result.append({**candidate, **info, 'relativePath':relative})
        except (GateError, ValueError) as e:
            errors.append(str(e) if isinstance(e, GateError) else 'IMAGE_CONTRACT_INVALID')
            if str(e) in {'PUBLIC_HTTP_403','PUBLIC_HTTP_429','ROBOTS_DISALLOWED'} and 'url' in locals():
                unavailable_hosts.add(urllib.parse.urlsplit(url).hostname)
    require(0 <= len(result) <= 5, 'IMAGE_COUNT_LIMIT')
    return result, sorted(set(errors))


def init_media(ledger):
    ledger.db.executescript('''
    CREATE TABLE IF NOT EXISTS media_sources(key TEXT PRIMARY KEY,category TEXT,url TEXT,
      at TEXT,state TEXT,document TEXT,error TEXT);
    CREATE TABLE IF NOT EXISTS media_topics(key TEXT PRIMARY KEY,source_key TEXT,state TEXT,
      bundle TEXT,manifest TEXT,error TEXT,created TEXT);
    CREATE TABLE IF NOT EXISTS media_discovery(key TEXT PRIMARY KEY,at TEXT,value TEXT);
    ''')
    ledger.db.commit()


def media_put(ledger, table, key, **fields):
    require(table in {'media_sources','media_topics','media_discovery'}, 'MEDIA_TABLE_INVALID')
    if ledger.db.execute('SELECT 1 FROM '+table+' WHERE key=?',(key,)).fetchone():
        ledger.db.execute('UPDATE '+table+' SET '+','.join(k+'=?' for k in fields)+' WHERE key=?',(*fields.values(),key))
    else:
        values={'key':key,**fields}
        ledger.db.execute('INSERT INTO '+table+'('+','.join(values)+') VALUES('+','.join('?' for _ in values)+')',tuple(values.values()))
    ledger.db.commit()


def pool(ledger, collector, reader, issues=None):
    result = {}
    def add(category, url, title='', editorial_category=None, extra=None):
        if category not in DATASETS:
            return
        try:
            url = public_url(url, ARTICLE_HOSTS)
            u = urllib.parse.urlsplit(url)
            if category == 'entertainment':
                require((u.hostname=='www.news.cn' and re.fullmatch(r'/ent/20\d{6}/[a-f0-9]{32}/c.html',u.path))
                    or (u.hostname=='www.thepaper.cn' and re.fullmatch(r'/newsDetail_forward_\d+',u.path)), 'ENTERTAINMENT_ARTICLE_REQUIRED')
            else:
                require(u.hostname not in {'www.news.cn','www.thepaper.cn'} and u.path not in {'','/'},'AI_ARTICLE_REQUIRED')
            result[(category,url)]={**(extra or {}),'category':category,'url':url,'title':title,
                                    **({'editorialCategory':editorial_category} if editorial_category in CATEGORIES else {})}
        except (GateError, ValueError): pass
    from source_discovery import discover
    for item in discover(reader,ledger,media_put,issues if issues is not None else []):
        add(item['category'],item['url'],item['title'],item.get('editorialCategory'))
    from repository_sources import discover as discover_repositories
    for item in discover_repositories(reader,ledger,media_put,issues if issues is not None else []):
        add('ai-news',item['url'],item['title'],'github-projects',item)
    # Upgrade the two earlier dated-but-unillustrated drafts, without rewriting them.
    for row in ledger.db.execute("SELECT category,manifest FROM topics WHERE state='packaged' ORDER BY created"):
        m=json.loads(row['manifest']);add(row['category'],m['sourceUrl'])
    for category in DATASETS:
        path = Path(collector)/'categories'/category/'latest-candidates.json'
        if path.is_file():
            value=json.loads(path.read_text())
            age=datetime.now(timezone.utc)-datetime.fromisoformat(value['capturedAt'].replace('Z','+00:00'))
            require(-timedelta(minutes=10)<=age<=timedelta(hours=6),'COLLECTOR_SNAPSHOT_STALE')
            for item in value['items']: add(category,item['url'],item['title'])
    # Official RSS bodies are often omitted by keyword-only title classification.
    from news_pipeline import candidates
    for category,item in candidates(collector): add(category,item['url'],item['title'])
    if 'entertainment' in DATASETS:
        cached=ledger.db.execute("SELECT * FROM media_discovery WHERE key='xinhua-ent'").fetchone()
        if cached and datetime.now(timezone.utc)-datetime.fromisoformat(cached['at'])<timedelta(hours=6):
            found=json.loads(cached['value'])
        else:
            try:
                html,_=reader.html(DISCOVERY)
                soup=BeautifulSoup(html,'html.parser')
                found={}
                for a in soup.find_all('a',href=True):
                    url=urllib.parse.urljoin(DISCOVERY,a['href'])
                    if re.fullmatch(r'https://www.news.cn/ent/20\d{6}/[a-f0-9]{32}/c.html',url):
                        found.setdefault(url,plain(a.get_text(' ',strip=True)))
                found=[{'url':u,'title':t} for u,t in sorted(found.items(),reverse=True)[:12]]
                media_put(ledger,'media_discovery','xinhua-ent',at=timestamp(),value=json.dumps(found,ensure_ascii=False))
            except GateError as e:
                if issues is not None: issues.append('ENTERTAINMENT_DISCOVERY:'+str(e))
                found=json.loads(cached['value']) if cached else []
        for item in found: add('entertainment',item['url'],item['title'])
    # Interleave categories to avoid hot AI feeds starving entertainment forever.
    queues=[[x for x in result.values() if x['category']==cat] for cat in DATASETS]
    # Prefer under-supplied editorial categories without starving the independent entertainment queue.
    from daily_supply_status import from_ledger
    counts=from_ledger(ledger.db)['editorialCategoryCounts']
    for queue in queues:
        if queue and queue[0]['category']=='ai-news':
            queue.sort(key=lambda item:counts.get(classify(item),0))
    ordered=[]
    for i in range(max(map(len,queues),default=0)):
        for queue in queues:
            if i<len(queue): ordered.append(queue[i])
    from source_discovery import editorial_order
    return editorial_order(ordered,ledger.root)


def reuse_editor(ledger, category, doc):
    """Reuse an earlier draft only when all of its source paragraphs still exist."""
    for row in ledger.db.execute("SELECT manifest FROM topics WHERE category=? AND state='packaged'",(category,)):
        old=json.loads(row['manifest'])
        if old['sourceUrl']!=doc['url']: continue
        original=json.loads(local_file(ledger.root,old['sourcePath']).read_text())
        mapping={}
        for old_p in original['paragraphs']:
            matches=[p['id'] for p in doc['paragraphs'] if old_p['text'] in p['text']]
            if len(matches)!=1: break
            mapping[old_p['id']]=matches[0]
        if len(mapping)!=len(original['paragraphs']): continue
        key=sha(encoded([category,doc['url'],old['sourceSha256']]))
        model=ledger.row('models',key)
        if model and model['state']=='complete':
            result=json.loads(local_file(ledger.root,'models/'+key+'.json').read_text())
            if any(sid not in mapping for p in result['paragraphs'] for sid in p['sourceIds']): continue
            for p in result['paragraphs']: p['sourceIds']=[mapping[sid] for sid in p['sourceIds']]
            result['reusedFromDraft']=old['key']
            result['citationIdsRemappedToCurrentEvidence']=True
            return result
    return None


def artifact(ledger, category, body, name, mime, kind, key=None):
    digest=sha(body)
    key=key or sha(encoded(['next-programme',category,kind,digest]))
    relative='media/outbox/'+category+'/'+name
    immutable(local_file(ledger.root,relative),body)
    return {'key':key,'category':category,'dataset':DATASETS[category], 'kind':kind,
        'relativePath':relative,'fileName':name,'mediaType':mime,'sha256':digest,'sizeBytes':len(body),
        'requestId':str(uuid.uuid5(uuid.NAMESPACE_URL,'yumi-next:'+category+':'+kind+':'+digest)),
        'processingMode':'store-only','reviewRequired':True,'broadcastApproved':False}


class Upload:
    def __init__(self,ledger,api): self.ledger,self.api,self.posts=ledger,api,0
    def sync(self,m):
        require(m['category'] in DATASETS and m['dataset']==DATASETS[m['category']]
            and m['kind'] in {'image','manuscript','supply-projection'} and m['processingMode']=='store-only'
            and m['broadcastApproved'] is False and m['reviewRequired'] is True,'MEDIA_ROUTE_INVALID')
        body=local_file(self.ledger.root,m['relativePath']).read_bytes()
        require(sha(body)==m['sha256'] and len(body)==m['sizeBytes'],'MEDIA_BYTES_CHANGED')
        if m['kind']=='image': image_info(body,m['mediaType'])
        elif m['kind']=='supply-projection':
            from supply_upload import FILE_NAME,validate
            require(m['category']=='ai-news' and m['fileName']==FILE_NAME and m['mediaType']=='text/plain','SUPPLY_UPLOAD_ROUTE_INVALID')
            validate(body)
        else:
            require(m['mediaType']=='text/markdown' and len(body)<=256*1024
                and m.get('dateVerified') is True and 0<=m.get('imageCount',0)<=5,'MANUSCRIPT_GATE_INVALID')
        old=self.ledger.row('uploads',m['key'])
        if old and old['state']=='synced': return json.loads(old['receipt'])
        who=self.api.request('/me')['data']
        require(UID.fullmatch(who.get('credentialId','')),'CREDENTIAL_ID_INVALID')
        require(not old or old['credential_id']==who['credentialId'],'ORIGINAL_CREDENTIAL_CHANGED')
        caps=self.api.request('/capabilities')['data']
        require({'fileUploads.options','fileUploads.create','fileUploads.read','fileUploads.findRequest'}<=set(caps['operations']),'UPLOAD_CAPABILITIES_REQUIRED')
        base='/datasets/'+m['dataset']+'/uploads'
        found=self.api.request(base+'/by-request/'+m['requestId'])['data']
        if found['state']=='committed':
            receipt=found['receipt'];require(found.get('processingRuns')==0,'UNEXPECTED_PROCESSING')
        else:
            require(found['state']=='absent','ORIGINAL_UPLOAD_UNKNOWN')
            if old and old.get('body'): payload=json.loads(old['body'])
            else:
                options=self.api.request(base)['data']
                require(options.get('canUpload') is True and m['mediaType'] in options.get('mediaTypes',[])
                    and 'store-only' in options.get('processingModes',[]) and len(body)<=options.get('maxBytes',0),'UPLOAD_OPTIONS_CHANGED')
                from workbuddy_collection import KEY as COLLECTION_KEY, LABEL as COLLECTION_LABEL
                folder_name=COLLECTION_LABEL if m.get('editorialCollection')==COLLECTION_KEY else CATEGORIES.get(m.get('editorialCategory'))
                folder=next((f['id'] for f in options.get('folders',[]) if f['name']==folder_name and not f.get('parentId')),None)
                if m.get('editorialCollection')==COLLECTION_KEY:require(folder is not None,'WORKBUDDY_COLLECTION_FOLDER_REQUIRED')
                payload={'requestId':m['requestId'],'fileName':m['fileName'],'mediaType':m['mediaType'],
                    'sha256':m['sha256'],'folderId':folder,'folderRevision':options['revision'],
                    'originNote':m.get('originNote','Yumi next programme review material; source date checked; rights review required; no broadcast approval'),
                    'processingMode':'store-only','contentBase64':base64.b64encode(body).decode()}
            self.ledger.put('uploads',m['key'],state='intent',body=json.dumps(payload),credential_id=who['credentialId'],error=None)
            self.posts+=1
            receipt=self.api.request(base,payload)['data']['receipt']
        require(UID.fullmatch(receipt.get('id','')) and UID.fullmatch(receipt.get('assetId','')),'UPLOAD_RECEIPT_ID_INVALID')
        current=self.api.request(base+'/'+receipt['id'])['data']
        for item in [receipt,current]:
            require(all(item.get(k)==m[k] for k in ['fileName','mediaType','sha256','sizeBytes','processingMode'])
                and item.get('credentialId')==who['credentialId'] and item.get('parsingStatus')=='not-requested','UPLOAD_RECEIPT_CHANGED')
        proof={'dataset':m['dataset'],'requestId':m['requestId'],'verifiedAt':timestamp(),
            'receipt':current,'originalBytesDownloaded':False}
        path=local_file(self.ledger.root,'receipts/'+m['key']+'.json')
        if path.exists():
            prior=json.loads(path.read_text())
            require(prior['dataset']==proof['dataset'] and prior['requestId']==proof['requestId'] and prior['receipt']==current,'IMMUTABLE_RECEIPT_CHANGED')
            proof=prior
        else: immutable(path,encoded(proof))
        self.ledger.put('uploads',m['key'],state='synced',credential_id=who['credentialId'],receipt=json.dumps(proof))
        return proof


def bundle_key(category,doc,result):
    return sha(encoded(['next-programme-v1',category,doc['url'],doc['publishedDate'],
        source_fingerprint(doc),[x['sha256'] for x in doc['images']],result['paragraphs']]))


def editorial_cleanup(result):
    """Move unsupported absence claims out of speech; preserve the raw model file."""
    result=json.loads(json.dumps(result,ensure_ascii=False))
    kept,removed=[],[]
    for p in result['paragraphs']:
        text=p['text']
        missing_date=bool(re.search(r'发布日期|发布时间',text) and re.search(r'未知|未提供|没有给出|未给出|未明确',text))
        missing_full=bool(re.search(r'(?:正文|文章|本文).{0,12}(?:没有|未|尚未).{0,14}(?:具体实现|实测数据|详细介绍)',text))
        (removed if missing_date or missing_full else kept).append(p)
    require(kept,'NO_SAFE_SPOKEN_PARAGRAPHS')
    if removed:
        result['paragraphs']=kept
        result['editorialRemovedParagraphs']=removed
        result['reviewNotes']=result.get('reviewNotes',[])+[
            '已移出'+str(len(removed))+'段关于发布日期或全文内容缺项的无依据口播判断；模型接收的是正文节选，不能由节选推断全文缺项。原始模型响应保留，勿朗读移出段落。']
    return result


def prepare_editorial_corrections(ledger):
    """Prepare stable revisions of our unapproved drafts; do not mutate old bytes."""
    for row in ledger.db.execute("SELECT * FROM media_topics WHERE state='synced' AND bundle IS NOT NULL").fetchall():
        bundle=json.loads(row['bundle'])
        cleaned=editorial_cleanup(bundle['editor'])
        if cleaned['paragraphs']==bundle['editor']['paragraphs']: continue
        key=bundle_key(bundle['category'],bundle['document'],cleaned)
        existing=ledger.db.execute('SELECT 1 FROM media_topics WHERE key=?',(key,)).fetchone()
        if existing: continue
        revision={**bundle,'key':key,'createdAt':timestamp(),'editor':cleaned,'supersedes':row['key']}
        media_put(ledger,'media_topics',key,source_key=row['source_key'],state='prepared',
            bundle=json.dumps(revision,ensure_ascii=False),created=revision['createdAt'],error=None)


def repository_observation_fresh(observed_date, today):
    # Preserve the actual observation date; yesterday is still within 48 hours.
    try:
        observed = datetime.fromisoformat(observed_date).date()
        return 0 <= (today - observed).days <= 1
    except (TypeError, ValueError):
        return False


def deliver(ledger,uploader,bundle):
    category,doc,result=bundle['category'],bundle['document'],bundle['editor']
    editorial_category=doc.get('editorialCategory') or classify({'category':category,'title':result['title'],'sourceUrl':doc['url']})
    repository_observation=(editorial_category=='github-projects' and doc['publication']['state']=='verified_repository_observation'
        and repository_observation_fresh(doc['publication'].get('observedDate'), datetime.now(ZoneInfo('Asia/Singapore')).date())
        and bool(doc['publication'].get('trendingPageSha256')))
    require((doc['publication']['state']=='verified_publisher_publication' or repository_observation) and 0<=len(doc['images'])<=5,'PACKAGE_GATE_INVALID')
    age=datetime.now(ZoneInfo('Asia/Singapore')).date()-datetime.fromisoformat(doc['publishedDate']).date()
    require(timedelta(0)<=age<=timedelta(days=7),'PACKAGE_PUBLICATION_STALE_OR_FUTURE')
    require(len({x['sha256'] for x in doc['images']})==len(doc['images']),'PACKAGE_DUPLICATE_IMAGES')
    references=[]
    for img in doc['images']:
        raw=local_file(ledger.root,img['relativePath']).read_bytes()
        m=artifact(ledger,category,raw,'yumi-original-'+img['sha256'][:20]+img['extension'],img['mediaType'],'image')
        m['editorialCategory']=editorial_category
        from workbuddy_collection import tag as tag_collection
        context=tag_collection({'category':category,'title':result['title'],'sourceUrl':doc['url']})
        if context.get('editorialCollection'):m['editorialCollection']=context['editorialCollection']
        proof=uploader.sync(m)
        receipt=proof['receipt']
        references.append({**img,'asset':{'kind':'upload-original','dataset':DATASETS[category],
            'uploadId':receipt['id'],'assetId':receipt['assetId'],'sha256':img['sha256'],
            'mime':img['mediaType'],'width':img['width'],'height':img['height']},
            'assetPage':DATAHUB_ORIGIN+'/datasets/'+DATASETS[category]+'?contentKind=asset&contentId='+receipt['assetId'],
            'originalUrl':DATAHUB_ORIGIN+'/api/datafoundation/customer/v1/datasets/'+DATASETS[category]+'/items/asset/'+receipt['assetId']+'/original',
            'layout':'image-with-avatar-bottom-left'})
    manifest={'schema':'yumi.next-programme-material.v1','key':bundle['key'],'createdAt':bundle['createdAt'],
        'category':category,'editorialCategory':editorial_category,'productionRequested':category=='ai-news',
        'posePolicy':'least-assigned-valid-pose','dataset':DATASETS[category],'title':result['title'],'sourceUrl':doc['url'],
        'publication':doc['publication'],'sourceHtmlSha256':doc['htmlSha256'],
        'sourceBodySha256':source_fingerprint(doc),'images':references,'imageCount':len(references),
        'dateVerified':True,'nextProgrammeCandidate':True,'reviewRequired':True,'broadcastApproved':False,
        'mediaReadiness':'complete' if len(references)>=3 else 'pending-images',
        'missingImageCount':max(0,3-len(references)),
        'runtimeExecutable':False,'processingMode':'store-only','rightsApproved':False,
        'editorMethod':result['method'],'reusedFromDraft':result.get('reusedFromDraft'),
        'paragraphs':result['paragraphs'],'modelUsage':result.get('usage')}
    from local_image_policy import initialize as initialize_local_images
    initialize_local_images(manifest)
    from workbuddy_collection import tag as tag_application_collection
    tag_application_collection(manifest)
    if doc.get('editorialReferences'):manifest['editorialReferences']=doc['editorialReferences']
    if doc.get('evidenceLevel')=='publisher_photo_report':
        manifest['sourceEvidenceLevel']='publisher_photo_report'
    if bundle.get('supersedes'):
        manifest['supersedesPackage']=bundle['supersedes']
    if result.get('editorialRemovedParagraphs'):
        manifest['editorialReview']={'removedFromSpeech':result['editorialRemovedParagraphs'],
            'reason':'unsupported_missing_date_or_full_article_absence_claim'}
    def clean(value):
        return re.sub(r'([\\`*_{}\[\]()#+!|])',r'\\\1',escape(plain(value),quote=False))
    lines=['# '+clean(result['title']),'','> 下一场待审素材 · 未批准播出 · 图片使用范围待审','',
        '## 口播正文','']
    if bundle.get('supersedes'):
        lines[4:4]=['> 修订版：取代素材包 '+bundle['supersedes']+'。原稿中无依据的日期/全文缺项判断已移出口播；原件保留供追溯。','']
    lines += [clean(p['text'])+'\n' for p in result['paragraphs']]
    date_label='GitHub热门榜观察日期（不是项目发布日期）' if doc['publication']['state']=='verified_repository_observation' else '原站发布日期'
    lines += ['## 日期与来源核实（不朗读）','',
        '- '+date_label+'：'+doc['publishedDate'],
        '- 日期核实：'+('GitHub今日榜单快照与仓库API；不宣称项目今日发布。' if repository_observation else '原站发布字段/可见发布栏；不代表独立核实事件全部事实。'),
        '- 精确发布时刻：'+(doc['publishedAt'] or '原站未声明带时区时刻；仅核实日历日期'),
        '- 时效：'+doc['publication']['freshness']+'；不把抓取日期当成事件日期。',
        '- 原文：'+doc['url'],'- 抓取时间：'+doc['fetchedAt'],
        '- 事件日期：应以正文明确表述核对，不由发布日期推断。','',
        '## 配图与出处（不朗读）','']
    if len(references)<3:
        lines += ['> 配图待补：已保存正文与现有原图，不因图少于3张而丢弃。尚未满足播出配图要求。','']
    if doc.get('eventDateStatements'):
        index=lines.index('## 配图与出处（不朗读）')
        lines[index:index]=['- 原文事件日期句：'+clean(p) for p in doc['eventDateStatements']]+['']
    for i,img in enumerate(references,1):
        caption=img['captionOriginal'] or img['alt'] or '原文正文配图，具体含义待审核'
        lines += ['### 配图'+str(i),'',
            '!['+clean('原文配图'+str(i))+']('+img['originalUrl']+')','',
            '- 原文图注/相邻说明：'+clean(caption),
            '- 说明依据：'+img.get('captionEvidence','source_context_needs_review'),'- 图源：'+img['url'],
            '- 出处文章：'+img['sourcePageUrl'],'- 署名：'+clean(img['credit']),
            '- 用途：与本篇正文关联的原文配图；人物照片/场景图/示意图性质须审阅，不冒充现场实拍。',
            '- 权利状态：待审，公开可访问不等于已授权公开播出。',
            '- [查看已入库原件]('+img['assetPage']+')；SHA256：'+img['sha256'],'']
    notes=[n for n in result.get('reviewNotes',[]) if n.startswith('已移出') or not re.search('发布日期|publishedAt',n)]
    lines += ['## 编辑待审（不朗读）','']+['- '+clean(n) for n in notes]
    lines += ['- 日期已核实到原站发布记录；来源事实、数字、译名及图权仍需审核。',
        '- 仅为下一场候选；不修改本场正文、声音、镜头或动作，不自动解析或发布。','',
        '## 来源正文证据（不朗读）','']
    lines += ['- '+p['id']+'：'+clean(p['text']) for p in doc['paragraphs']]
    lines += ['- '+p['id']+'：'+clean(p['text'])+'；官方依据：'+p['sourceUrl']+'；核对日期：'+p['verifiedAt'] for p in doc.get('editorialReferences',[])]
    lines += ['','## 素材清单（机器字段，不朗读）','','```json',encoded(manifest).decode().strip(),'```','']
    body='\n'.join(lines).encode()
    safe=re.sub(r'[^\w\u4e00-\u9fff-]+','-',result['title']).strip('-')[:45] or '新闻'
    name='下一场待审-'+safe+'-'+bundle['key'][:10]+'.md'
    m=artifact(ledger,category,body,name,'text/markdown','manuscript')
    m.update(dateVerified=True,imageCount=len(references),editorialCategory=editorial_category)
    if manifest.get('editorialCollection'):m['editorialCollection']=manifest['editorialCollection']
    proof=uploader.sync(m)
    manifest.update(manuscript={**m,'receipt':proof['receipt']})
    immutable(local_file(ledger.root,'media/packages/'+bundle['key']+'.json'),encoded(manifest))
    return manifest


def candidate_rejection(code):
    return code.startswith(('PUBLICATION_DATE_','PUBLICATION_STALE_OR_FUTURE','ARTICLE_SCOPE_',
        'ARTICLE_TITLE_','ARTICLE_BODY_','ARTICLE_THREE_IMAGES_','THREE_DISTINCT_IMAGES_','ROBOTS_DISALLOWED'))


def run(state,collector,limit=2,collect_only=False):
    ledger=Ledger(state);init_media(ledger)
    reader=Reader()
    hub,model=credential('datahub-token'),credential('deepseek-token')
    uploader=Upload(ledger,JsonHTTP(API,hub)) if hub and not collect_only else None
    editor=Editor(ledger,JsonHTTP(MODEL_API,model),max_daily=MODEL_BUDGET_DAILY) if model and not collect_only else None
    report={'schema':'yumi.news-media-health.v1','at':timestamp(),'categories':{},'collected':0,
        'newPackages':0,'uploadsStarted':0,'modelCallsStarted':0,'editorResultsReused':0,
        'alreadyKnown':0,'blockers':[],'candidateRejections':[],'broadcastApproved':False,'nextProgrammeOnly':True}
    model_count_before=ledger.db.execute('SELECT COUNT(*) FROM models').fetchone()[0]
    for cat in DATASETS: report['categories'][cat]={'collected':0,'newPackages':0}
    try:
        from editor_response_recovery import recover as recover_editor_response
        report['editorResponseRecovery']=recover_editor_response(ledger,editor)
        if uploader and WORKBUDDY_REFERENCE_FILE:
            from workbuddy_collection import apply as apply_workbuddy_collection
            try:report['workbuddyCollection']=apply_workbuddy_collection(ledger,uploader)
            except (GateError,ValueError,KeyError) as error:
                report['blockers'].append('WORKBUDDY_COLLECTION:'+str(error))
        # Reconcile existing prepared packages before new network/model work.
        completed=0
        if uploader:
            prepare_editorial_corrections(ledger)
            pending=ledger.db.execute("SELECT * FROM media_topics WHERE state='prepared' ORDER BY created LIMIT ?",(limit,)).fetchall()
            for row in pending:
                try:
                    bundle=json.loads(row['bundle'])
                    manifest=deliver(ledger,uploader,bundle)
                    media_put(ledger,'media_topics',row['key'],state='synced',manifest=json.dumps(manifest,ensure_ascii=False),error=None)
                    if bundle.get('supersedes'): media_put(ledger,'media_topics',bundle['supersedes'],state='superseded')
                    media_put(ledger,'media_sources',row['source_key'],state='synced',error=None)
                    report['newPackages']+=1;report['categories'][manifest['category']]['newPackages']+=1;completed+=1
                except (GateError,ValueError,KeyError) as e:
                    code=str(e) if isinstance(e,GateError) else 'MEDIA_SYNC_CONTRACT_INVALID'
                    media_put(ledger,'media_topics',row['key'],error=code);report['blockers'].append(code);completed+=1
        candidates=pool(ledger,collector,reader,report['blockers'])
        from daily_supply_status import from_ledger
        target_counts=from_ledger(ledger.db)['editorialCategoryCounts']
        report['eligibleCandidates']=len(candidates)
        attempts=0
        for item in candidates:
            if completed>=limit or attempts>=4: break
            category,url=item['category'],item['url']
            selected_category=item.get('editorialCategory') or classify(item)
            if category=='ai-news' and target_counts.get(selected_category,0)>=5:continue
            key=sha(encoded([category,url]))
            prior=ledger.db.execute('SELECT * FROM media_sources WHERE key=?',(key,)).fetchone()
            if prior and prior['state']=='synced':
                report['alreadyKnown']+=1;continue
            if prior and prior['state']=='prepared': continue
            if prior and prior['state']=='blocked' and not str(prior['error'] or '').startswith(('ARTICLE_THREE_IMAGES_','THREE_DISTINCT_IMAGES_')) and datetime.now(timezone.utc)-datetime.fromisoformat(prior['at'])<timedelta(hours=6):
                continue
            attempts+=1
            try:
                if prior and prior['state']=='collected' and prior['document']:
                    doc=json.loads(prior['document'])
                else:
                    media_put(ledger,'media_sources',key,category=category,url=url,at=timestamp(),state='fetching',error=None)
                    if item.get('editorialCategory')=='github-projects' and urllib.parse.urlsplit(url).hostname=='github.com':
                        from repository_sources import collect as collect_repository
                        doc=collect_repository(reader,item)
                    else:
                        html,html_sha=reader.html(url)
                        doc=extract_article(html,url,html_sha)
                        doc['editorialCategory']=selected_category
                    require(0<=(datetime.now(ZoneInfo('Asia/Singapore')).date()-datetime.fromisoformat(doc['publishedDate']).date()).days<=SOURCE_MAX_AGE_DAYS.get(doc.get('editorialCategory'),2),
                            'PUBLICATION_OUTSIDE_DAILY_WINDOW')
                    doc['images'],doc['imageCollectionNotes']=collect_images(reader,ledger,doc)
                    immutable(local_file(ledger.root,'media/sources/'+key+'-'+sha(encoded(doc))[:12]+'.json'),encoded(doc))
                    media_put(ledger,'media_sources',key,state='collected',document=json.dumps(doc,ensure_ascii=False))
                    report['collected']+=1;report['categories'][category]['collected']+=1
                require(0<=(datetime.now(ZoneInfo('Asia/Singapore')).date()-datetime.fromisoformat(doc['publishedDate']).date()).days<=SOURCE_MAX_AGE_DAYS.get(doc.get('editorialCategory'),2),
                        'PUBLICATION_OUTSIDE_DAILY_WINDOW')
                if collect_only:
                    completed+=1;continue
                require(editor is not None and uploader is not None,'MEDIA_CREDENTIALS_REQUIRED')
                result=reuse_editor(ledger,category,doc)
                if result: report['editorResultsReused']+=1
                else:
                    from workbuddy_collection import enrich as enrich_workbuddy_evidence
                    doc=enrich_workbuddy_evidence(doc)
                    media_put(ledger,'media_sources',key,document=json.dumps(doc,ensure_ascii=False))
                    model_key=sha(encoded(['verified-date-editor-v2',category,url,source_fingerprint(doc),doc['publishedDate']]))
                    before=ledger.row('models',model_key)
                    result=editor.draft(model_key,{**doc,'publishedAt':doc['publishedAt'] or doc['publishedDate']})
                    report['modelCallsStarted']+=int(before is None)
                result=editorial_cleanup(result)
                bundle={'key':bundle_key(category,doc,result),'category':category,'document':doc,
                    'editor':result,'createdAt':timestamp()}
                media_put(ledger,'media_topics',bundle['key'],source_key=key,state='prepared',
                    bundle=json.dumps(bundle,ensure_ascii=False),created=bundle['createdAt'],error=None)
                media_put(ledger,'media_sources',key,state='prepared')
                completed+=1
                manifest=deliver(ledger,uploader,bundle)
                media_put(ledger,'media_topics',bundle['key'],state='synced',manifest=json.dumps(manifest,ensure_ascii=False))
                media_put(ledger,'media_sources',key,state='synced')
                report['newPackages']+=1;report['categories'][category]['newPackages']+=1
                target_counts=from_ledger(ledger.db)['editorialCategoryCounts']
            except (GateError,ValueError,KeyError) as e:
                code=str(e) if isinstance(e,GateError) else 'MEDIA_CONTRACT_INVALID'
                current=ledger.db.execute('SELECT state FROM media_sources WHERE key=?',(key,)).fetchone()
                if not current or current['state']!='prepared':
                    media_put(ledger,'media_sources',key,category=category,url=url,state='blocked',at=timestamp(),error=code)
                if candidate_rejection(code): report['candidateRejections'].append({'category':category,'url':url,'reason':code})
                else: report['blockers'].append(code)
        if uploader:
            from image_supplements import apply as apply_image_supplements
            report['imageSupplements']=apply_image_supplements(ledger,uploader)
        report['uploadsStarted']=uploader.posts if uploader else 0
        report['modelCallsStarted']=ledger.db.execute('SELECT COUNT(*) FROM models').fetchone()[0]-model_count_before
        report['publicRequests']=reader.requests
        report['totalPackages']=ledger.db.execute("SELECT COUNT(*) FROM media_topics WHERE state='synced'").fetchone()[0]
        report['pendingPackages']=ledger.db.execute("SELECT COUNT(*) FROM media_topics WHERE state='prepared'").fetchone()[0]
        report['blockers']=sorted(set(report['blockers']))
        from daily_supply_status import from_ledger
        daily=from_ledger(ledger.db)
        daily['workerStatus']='degraded' if report['blockers'] else 'ready'
        daily['workerBlockers']=report['blockers']
        daily['candidatePoolSize']=report.get('eligibleCandidates',0)
        daily['dshWorkflowBinding']=None
        daily['monitoringAuthority']='existing DSH workflow; binding not yet confirmed'
        from production_tracking import from_ledger as production_from_ledger
        from native_news_receipts import refresh as refresh_native_receipts
        if uploader:
            from completion_ingestion import ingest as ingest_category_completion
            try:
                report['categoryCompletionIngestion']=ingest_category_completion(ledger,uploader)
            except (GateError,ValueError,KeyError,TypeError,OSError) as error:
                code=str(error) if isinstance(error,(GateError,ValueError)) else type(error).__name__
                report['blockers']=sorted(set(report['blockers']+['CATEGORY_COMPLETION_INGESTION:'+code]))
        if not collect_only:
            refresh_native_receipts(ledger.db,ledger.root)
        tracking=production_from_ledger(ledger.db,ledger.root)
        if uploader:
            from production_tracking import publish_report
            try:
                tracking['dataHubReport']=publish_report(ledger,uploader,tracking)
            except (GateError,ValueError):
                tracking['reportSyncStatus']='pending'
                tracking['reportSyncError']='TRACKING_REPORT_SYNC_UNCONFIRMED'
        atomic(ledger.root/'production-tracking.json',tracking)
        atomic(ledger.root/'category-production-supply.json',supply(ledger.db))
        if uploader:
            from supply_upload import publish as publish_category_supply
            try:
                supply_receipt=publish_category_supply(ledger,uploader,supply(ledger.db))
                atomic(ledger.root/'category-production-supply-upload-receipt.json',supply_receipt)
                daily['dataHubCategorySupply']=supply_receipt
            except (GateError,ValueError,KeyError) as error:
                code=str(error) if isinstance(error,(GateError,ValueError)) else 'CONTRACT_INVALID'
                report['blockers']=sorted(set(report['blockers']+['CATEGORY_SUPPLY_SYNC:'+code]))
                daily['workerStatus']='degraded'
                daily['workerBlockers']=report['blockers']
        report['productionTracking']=tracking
        daily['completionMeaning']=tracking['completionMeaning']
        daily['finishedCount']=tracking['finished']
        daily['scheduledConfirmedCount']=tracking['scheduledConfirmed']
        daily['playedConfirmedCount']=tracking['playedConfirmed']
        daily['unknownPlaybackCount']=tracking['unknownPlayback']
        daily['minimumIsNotCompletionQuota']=True
        by_material={item['materialKey']:item for item in tracking['items']}
        for item in daily['items']:
            progress=by_material.get(item['materialKey'])
            if progress:
                item.update({key:progress[key] for key in ['productionStatus','audioStatus','lipSyncStatus','playableStatus','finished','finishedAt','broadcastStatus','scheduled','played','playbackEvidence']})
        report['dailySupply']=daily
        if daily['targetStatus']=='overdue':
            report['blockers']=sorted(set(report['blockers']+['DAILY_NEW_STORY_TARGET_UNMET']))
        atomic(ledger.root/'daily-supply-status.json',daily)
        (ledger.root/'daily-supply').mkdir(exist_ok=True)
        atomic(local_file(ledger.root,'daily-supply/'+daily['editionDate']+'.json'),daily)
        report['status']='degraded' if report['blockers'] else ('collected_only' if collect_only else 'ready')
        report['completedAt']=timestamp()
        atomic(ledger.root/'health-media.json',report)
        if not collect_only:
            atomic(ledger.root/'health.json',{'schema':'yumi.news-pipeline-health.v1',
                'pipelineVersion':'dated-media-v1','at':report['completedAt'],'status':report['status'],
                'blockers':report['blockers'],'draftsCreated':report['newPackages'],
                'uploadsVerified':report['newPackages'],'alreadyKnown':report['alreadyKnown'],
                'modelCallsStarted':report['modelCallsStarted'],'editorConfigured':bool(model),
                'syncConfigured':bool(hub),'pendingDraftCount':report['pendingPackages'],
                'modelBudgetDaily':MODEL_BUDGET_DAILY,'broadcastApproved':False,'detailHealth':'health-media.json',
                'dailySupply':daily,'productionTracking':tracking})
        return report
    finally: ledger.db.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state',type=Path,default=Path('/var/lib/yumi-news-pipeline'))
    parser.add_argument('--collector',type=Path,default=Path('/var/lib/yumi-trendradar'))
    parser.add_argument('--limit',type=int,default=2)
    parser.add_argument('--collect-only',action='store_true')
    args=parser.parse_args()
    require(socket.gethostname()==EXPECTED_HOST,'WRONG_TARGET')
    require(1<=args.limit<=2,'RUN_BUDGET_INVALID')
    args.state.mkdir(parents=True,exist_ok=True)
    import fcntl
    lock=(args.state/'worker.lock').open('a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    worker_exit=1
    try:
        report=run(args.state,args.collector,args.limit,args.collect_only)
        worker_exit=0
        print(json.dumps(report,ensure_ascii=False))
    except Exception as e:
        code=str(e) if isinstance(e,GateError) else type(e).__name__
        atomic(args.state/'failure-media.json',{'at':timestamp(),'error':code,'broadcastApproved':False})
        if not args.collect_only:
            atomic(args.state/'health.json',{'schema':'yumi.news-pipeline-health.v1','pipelineVersion':'dated-media-v1',
                'at':timestamp(),'status':'failed','blockers':[code],'broadcastApproved':False})
        print(json.dumps({'status':'failed','error':code}));raise SystemExit(1)
    finally:
        # Same ExecStart credentials; telemetry never repeats news work.
        fcntl.flock(lock,fcntl.LOCK_UN)
        try:
            from source_runtime_health import finish_worker_health
            if not args.collect_only and credential('datahub-token'):
                finish_worker_health(args.state,HERE,worker_exit)
        except Exception:
            print(json.dumps({'sourceHealth':{'status':'unknown','error':'SOURCE_HEALTH_HOOK_UNAVAILABLE'}}))


if __name__=='__main__': main()
