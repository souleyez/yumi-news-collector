"""Today's public GitHub Trending observations plus first-party repository README facts."""
import base64,hashlib,json,re
from datetime import datetime,timezone
from html.parser import HTMLParser
from urllib.parse import urljoin,urlsplit
from zoneinfo import ZoneInfo

REPO=re.compile(r'^/([A-Za-z0-9_.-]{1,100})/([A-Za-z0-9_.-]{1,100})/?$')

class Trending(HTMLParser):
    def __init__(self):super().__init__();self.active=False;self.h2=False;self.href=None;self.text=[];self.rows=[]
    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag=='article' and 'Box-row' in attrs.get('class','').split():self.active=True;self.href=None;self.text=[]
        if self.active and tag=='h2':self.h2=True
        if self.active and self.h2 and tag=='a' and REPO.fullmatch(attrs.get('href','')):self.href=attrs['href']
    def handle_data(self,value):
        if self.active:self.text.append(value)
    def handle_endtag(self,tag):
        if tag=='h2':self.h2=False
        if tag=='article' and self.active:
            text=' '.join(' '.join(self.text).split());match=re.search(r'([\d,]+)\s+stars?\s+today',text)
            if self.href:self.rows.append({'url':'https://github.com'+self.href,'title':self.href[1:],
                'editorialCategory':'github-projects','category':'ai-news','trendingRank':len(self.rows)+1,
                'starsToday':int(match[1].replace(',','')) if match else None,'trendingText':text[:1400]})
            self.active=False

def parse_trending(html):
    parser=Trending();parser.feed(html);return parser.rows[:25]

def discover(reader,ledger,put,issues):
    from news_pipeline import GateError
    now=datetime.now(timezone.utc);key='category-discovery:github-trending-daily'
    old=ledger.db.execute('SELECT * FROM media_discovery WHERE key=?',(key,)).fetchone()
    if old and 0<=(now-datetime.fromisoformat(old['at'])).total_seconds()<=7200:return json.loads(old['value'])
    try:
        html,digest=reader.html('https://github.com/trending');items=parse_trending(html)
        for item in items:item.update(trendingObservedAt=now.isoformat(),trendingPageSha256=digest)
        put(ledger,'media_discovery',key,at=now.isoformat(),value=json.dumps(items,ensure_ascii=False))
        return items
    except (GateError,ValueError,KeyError) as error:
        issues.append('GITHUB_TRENDING_DISCOVERY:'+str(error));return json.loads(old['value']) if old else []

def read_json(reader,url):
    raw,_,charset=reader.read(url,{'api.github.com'},512*1024,{'application/json'})
    return json.loads(raw.decode(charset)),hashlib.sha256(raw).hexdigest()

def readme_paragraphs(text):
    # Read-only text extraction: commands/code are not executed or interpreted as instructions.
    text=re.sub(r'```.*?```',' ',text,flags=re.S)
    text=re.sub(r'<[^>]+>',' ',text)
    text=re.sub(r'!\[[^\]]*\]\([^)]*\)',' ',text)
    text=re.sub(r'\[([^\]]+)\]\([^)]*\)',r'\1',text)
    blocks=[]
    for block in re.split(r'\n\s*\n',text):
        clean=' '.join(re.sub(r'^[#>*|\s-]+','',block,flags=re.M).split())
        if len(clean)>=70:blocks.append(clean[:1100])
    return blocks[:12]

def collect(reader,item):
    from news_pipeline import GateError
    parsed=urlsplit(item['url']);match=REPO.fullmatch(parsed.path)
    if parsed.scheme!='https' or parsed.hostname!='github.com' or parsed.query or parsed.fragment or not match:
        raise GateError('REPOSITORY_URL_INVALID')
    owner,repo=match.groups();base='https://api.github.com/repos/'+owner+'/'+repo
    metadata,metadata_sha=read_json(reader,base);readme,readme_sha=read_json(reader,base+'/readme')
    if metadata.get('private') is not False or readme.get('encoding')!='base64' or readme.get('size',10**9)>256*1024:
        raise GateError('REPOSITORY_PUBLIC_README_REQUIRED')
    raw=base64.b64decode(readme['content'],validate=False)
    if not 200<=len(raw)<=256*1024:raise GateError('REPOSITORY_README_SIZE_INVALID')
    text=raw.decode('utf-8');blocks=readme_paragraphs(text)
    if not blocks:raise GateError('REPOSITORY_README_BODY_REQUIRED')
    observed=datetime.fromisoformat(item['trendingObservedAt']).astimezone(ZoneInfo('Asia/Singapore'))
    if observed.date()!=datetime.now(ZoneInfo('Asia/Singapore')).date():raise GateError('REPOSITORY_TRENDING_OBSERVATION_STALE')
    day=observed.date().isoformat();license_name=(metadata.get('license') or {}).get('spdx_id') or '仓库未声明可识别许可证'
    facts=[f"本稿是GitHub今日热门项目观察；{day}是榜单观察日期，不是项目发布日期或新版本发布日期。项目{metadata['full_name']}在该次daily榜单观察中位于第{item['trendingRank']}位；页面显示今日新增星标{item.get('starsToday') if item.get('starsToday') is not None else '未显示'}，数字会变化。",
           f"GitHub仓库API显示：说明为{metadata.get('description') or '未声明'}；主要语言{metadata.get('language') or '未声明'}；总星标{metadata['stargazers_count']}；创建时间{metadata['created_at']}；最近代码推送{metadata['pushed_at']}；许可证字段{license_name}；归档状态{metadata.get('archived')}。"]+['以下是维护者README自述，性能与效果未经本稿独立测试：'+block for block in blocks]
    paragraphs=[{'id':'s'+str(i+1),'text':value} for i,value in enumerate(facts[:10])]
    html,html_sha=reader.html(item['url'])
    from media_pipeline import BeautifulSoup
    soup=BeautifulSoup(html,'html.parser');images=[]
    og=soup.select_one('meta[property="og:image"]')
    if og and og.get('content') and urlsplit(og['content']).hostname in {'opengraph.githubassets.com','repository-images.githubusercontent.com'}:
        images.append({'url':og['content'],'alt':metadata['full_name']+' 仓库社交预览图','captionOriginal':'GitHub仓库社交预览，不是现场照片',
                       'captionEvidence':'repository_og_metadata','declaredWidth':1200,'declaredHeight':600,'sourcePageUrl':item['url'],
                       'credit':'GitHub仓库页面预览；来源与使用范围见原仓库'})
    download=readme.get('download_url') or ''
    found_images=re.findall(r'!\[([^\]]*)\]\(([^\s)]+)(?:\s+[^)]*)?\)',text)
    found_images += [('',url) for url in re.findall(r'<img\b[^>]*\bsrc=[\"\']([^\"\']+)[\"\']',text,flags=re.I)]
    for alt,url in found_images:
        url=urljoin(download,url);u=urlsplit(url)
        if u.scheme=='https' and u.hostname=='raw.githubusercontent.com' and u.path.startswith('/'+owner+'/'+repo+'/'):
            images.append({'url':url,'alt':alt[:300] or metadata['full_name']+' README配图','captionOriginal':alt[:600],
                'captionEvidence':'repository_readme','declaredWidth':None,'declaredHeight':None,'sourcePageUrl':item['url'],
                'credit':'原仓库README配图；具体署名见原仓库'})
    snapshot={'metadata':metadata,'readmeSha256':hashlib.sha256(raw).hexdigest(),'metadataResponseSha256':metadata_sha,
              'readmeResponseSha256':readme_sha,'repositoryHtmlSha256':html_sha,'trending':item}
    return {'url':item['url'],'title':'GitHub今日热门观察：'+metadata['full_name'],
        'publishedDate':day,'publishedAt':None,'editorialCategory':'github-projects',
        'publication':{'state':'verified_repository_observation','publishedDate':None,'publishedAt':None,
            'observedDate':day,'observedAt':observed.isoformat(),'precision':'trending_observation_not_publication',
            'freshness':'repository_snapshot_today','eventDateStatus':'not_a_release_date','trendingUrl':'https://github.com/trending',
            'trendingPageSha256':item['trendingPageSha256'],'repositoryCreatedAt':metadata['created_at'],'repositoryPushedAt':metadata['pushed_at']},
        'paragraphs':paragraphs,'imageCandidates':images[:15],'images':[],'imageCollectionNotes':[],
        'htmlSha256':hashlib.sha256(json.dumps(snapshot,ensure_ascii=False,sort_keys=True).encode()).hexdigest(),
        'sourceSnapshot':snapshot,'fetchedAt':datetime.now(timezone.utc).isoformat(),'evidenceLevel':'repository_trending_and_first_party_README'}
