"""Bounded publisher discovery for the existing media worker, never an executor."""
from datetime import datetime,timedelta,timezone
from html.parser import HTMLParser
import json
import re
from urllib.parse import urljoin,urlsplit,urlunsplit
from zoneinfo import ZoneInfo
from pathlib import Path

SOURCES=[('ps-news','https://blog.playstation.com/'),('hf-community','https://huggingface.co/blog/community'),
         ('hardware-samsung','https://news.samsung.com/global/'),('hardware-pi','https://www.raspberrypi.com/news/'),
         ('hardware-razer','https://www.razer.com/newsroom')]
HARDWARE_SEEDS=[
 ('2026-11-03','https://news.samsung.com/global/video-inside-the-tech-samsung-micro-rgb-tv'),
 ('2026-11-02','https://www.raspberrypi.com/news/price-rises-for-some-16gb-raspberry-pi-products/'),
 ('2026-10-30','https://www.razer.com/newsroom/campaigns/mako-x-brings-end-of-abyss-beyond-the-screen'),
 ('2026-10-29','https://www.razer.com/newsroom/product-news/razer-evangelion-eva-01-collection'),
 ('2026-10-27','https://www.razer.com/newsroom/product-news/reclusa-mini-65')]

class Links(HTMLParser):
    def __init__(self):super().__init__();self.current=None;self.text=[];self.items=[]
    def handle_starttag(self,tag,attrs):
        if tag=='a':self.current=dict(attrs).get('href');self.text=[]
    def handle_data(self,data):
        if self.current:self.text.append(data)
    def handle_endtag(self,tag):
        if tag=='a' and self.current:
            self.items.append((self.current,' '.join(' '.join(self.text).split())[:300]));self.current=None

def parse_links(html,base,today=None):
    today=today or datetime.now(ZoneInfo('Asia/Singapore')).date()
    parser=Links();parser.feed(html);found={}
    for href,title in parser.items:
        u=urlsplit(urljoin(base,href));host=urlsplit(base).hostname
        if u.scheme!='https' or u.hostname!=host or u.username or u.password or u.port not in {None,443}:continue
        if u.query or u.fragment:continue
        if host=='blog.playstation.com':
            match=re.fullmatch(r'/(20\d{2})/(\d{2})/(\d{2})/[a-z0-9-]+/',u.path)
            if not match:continue
            try:day=datetime.strptime('/'.join(match.groups()),'%Y/%m/%d').date()
            except ValueError:continue
            if not 0<=(today-day).days<=7:continue
        elif host=='huggingface.co':
            if not re.fullmatch(r'/blog/[A-Za-z0-9_.-]+/[A-Za-z0-9._-]+',u.path):continue
        elif host=='news.samsung.com':
            if not re.fullmatch('/global/[a-z0-9-]+',u.path) or not re.search('rgb|galaxy|display|oled|ssd|chip|memory|semiconductor|tv|earbud',title,re.I):continue
        elif host=='www.raspberrypi.com':
            if not re.fullmatch('/news/[a-z0-9-]+/',u.path):continue
        elif host=='www.razer.com':
            if not re.fullmatch('/newsroom/(?:product-news|campaigns)/[a-z0-9-]+',u.path):continue
        else:continue
        url=urlunsplit(('https',host,u.path,'',''))
        item={'category':'ai-news','url':url,'title':title}
        if host in {'news.samsung.com','www.raspberrypi.com','www.razer.com'}:item['editorialCategory']='hardware'
        found.setdefault(url,item)
        if len(found)>=24:break
    return list(found.values())

def discover(reader,ledger,put,issues):
    from news_pipeline import GateError
    today=datetime.now(ZoneInfo('Asia/Singapore')).date()
    found=[{'category':'ai-news','editorialCategory':'hardware','url':url,'title':'官方硬件发布'}
           for day,url in HARDWARE_SEEDS if 0<=(today-datetime.fromisoformat(day).date()).days<=7]
    now=datetime.now(timezone.utc)
    sources=SOURCES+[('ps-weekly','https://blog.playstation.com/'+(today-timedelta(days=6)).strftime('%Y/%m/%d/'))]
    for label,url in sources:
        key='category-discovery:'+label
        failure=ledger.db.execute('SELECT * FROM media_discovery WHERE key=?',(key+':failure',)).fetchone()
        if failure and timedelta(0)<=now-datetime.fromisoformat(failure['at'])<=timedelta(hours=6):
            issues.append('CATEGORY_DISCOVERY:'+label+':BACKOFF_'+json.loads(failure['value'])['code']);continue
        old=ledger.db.execute('SELECT * FROM media_discovery WHERE key=?',(key,)).fetchone()
        if old and timedelta(0)<=now-datetime.fromisoformat(old['at'])<=timedelta(hours=2):
            found+=json.loads(old['value']);continue
        try:
            html,_=reader.html(url)
            items=parse_links(html,url)
            put(ledger,'media_discovery',key,at=now.isoformat(),value=json.dumps(items,ensure_ascii=False))
            found+=items
        except (GateError,ValueError,KeyError) as error:
            issues.append('CATEGORY_DISCOVERY:'+label+':'+str(error))
            if str(error) in {'PUBLIC_HTTP_403','PUBLIC_HTTP_429','ROBOTS_DISALLOWED'}:
                put(ledger,'media_discovery',key+':failure',at=now.isoformat(),value=json.dumps({'code':str(error)}))
            if old:found+=json.loads(old['value'])
    return found

def editorial_order(items,root):
    from workbuddy_collection import prioritize
    path=Path(root)/'editorial-priority.json'
    if not path.exists():return prioritize(items)
    if path.is_symlink() or path.stat().st_size>8192:raise ValueError('EDITORIAL_PRIORITY_FILE_INVALID')
    value=json.loads(path.read_text());urls=value.get('sourceUrls')
    if value.get('schema')!='yumi.editorial-priority.v1' or not isinstance(urls,list) or len(urls)>10 or not all(isinstance(u,str) for u in urls):
        raise ValueError('EDITORIAL_PRIORITY_CONTRACT_INVALID')
    # A priority can only reorder candidates already admitted by the normal public-source checks.
    order={url:i for i,url in enumerate(urls)}
    return prioritize(sorted(items,key=lambda item:order.get(item['url'],len(order))))
