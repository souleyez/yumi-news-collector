"""Read the existing news ledger; deterministic daily supply status, no executor."""
from datetime import datetime,time,timedelta,timezone
from urllib.parse import urlsplit,urlunsplit,parse_qsl,urlencode
from zoneinfo import ZoneInfo
from category_supply import augment,classify,SOURCE_MAX_AGE_DAYS

POLICY={'schema':'yumi.daily-supply-policy.v1','timezone':'Asia/Singapore','minimumNewStories':25,
        'sourceMaxAgeDays':2,'deadlineLocal':'22:30','modelBudgetDaily':30,
        'categories':['ai-news','entertainment']}

def canonical_url(url):
    p=urlsplit(url)
    if p.scheme!='https' or not p.hostname or p.username or p.password:
        raise ValueError('INVALID_SOURCE_URL')
    query=[(k,v) for k,v in parse_qsl(p.query,keep_blank_values=True)
           if not k.lower().startswith('utm_') and k.lower() not in {'fbclid','gclid'}]
    return urlunsplit(('https',p.netloc.lower(),p.path,urlencode(sorted(query)),''))

def stamp(value):
    d=datetime.fromisoformat(value.replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('UNZONED_TIMESTAMP')
    return d

def summarize(rows,model_calls,now=None,policy=None):
    policy={**POLICY,**(policy or {})};zone=ZoneInfo(policy['timezone'])
    now=(now or datetime.now(timezone.utc)).astimezone(zone);day=now.date()
    target=policy['minimumNewStories']
    if not isinstance(target,int) or not 1<=target<=50:raise ValueError('INVALID_DAILY_TARGET')
    first={};latest={};invalid=0;rejected=0
    for row in rows:
        m=row['manifest']
        if m.get('contentKind')=='sponsored-application':continue
        try:
            identity=m.get('canonicalStoryId') or canonical_url(m['sourceUrl'])
            created=stamp(m['createdAt']);receipt=m['manuscript']['receipt']
            if not receipt.get('assetId') or receipt.get('sha256')!=m['manuscript'].get('sha256'):
                raise ValueError('INVALID_MANUSCRIPT_RECEIPT')
        except (KeyError,ValueError,TypeError):invalid+=1;continue
        # Includes prior revisions/corrections: an old story never becomes new again.
        first[identity]=min(first.get(identity,created),created)
        if row['state']!='synced':
            rejected+=int(created.astimezone(zone).date()==day);continue
        if identity not in latest or created>latest[identity][0]:latest[identity]=(created,m)
    items=[];revised=0;stale=0
    for identity,(created,m) in latest.items():
        if created.astimezone(zone).date()!=day:continue
        if first[identity].astimezone(zone).date()!=day:revised+=1;continue
        try:
            repo_observation=m['publication']['state']=='verified_repository_observation' and classify(m)=='github-projects'
            source_day=datetime.fromisoformat(m['publication']['observedDate'] if repo_observation else (m.get('publishedDate') or m['publication']['publishedDate'])).date()
            if (m['publication']['state']!='verified_publisher_publication' and not repo_observation) or not m.get('dateVerified'):
                raise ValueError('UNVERIFIED_SOURCE_DATE')
            if repo_observation and (not m['publication'].get('trendingPageSha256') or not m['publication'].get('observedAt')):
                raise ValueError('UNVERIFIED_REPOSITORY_OBSERVATION')
            age=(day-source_day).days
            max_age=0 if repo_observation else SOURCE_MAX_AGE_DAYS.get(classify(m),policy['sourceMaxAgeDays'])
            if not 0<=age<=max_age:stale+=1;continue
            paragraphs=m['paragraphs']
            if not paragraphs or not all(p.get('text','').strip() and p.get('sourceIds') for p in paragraphs):
                raise ValueError('INVALID_SOURCE_BOUND_TEXT')
        except (ValueError,KeyError,TypeError):invalid+=1;continue
        from local_image_policy import programme_images, graphics_ready
        count=len(programme_images(m));receipt=m['manuscript']['receipt']
        items.append({'storyIdentity':identity,'materialKey':m['key'],'category':m['category'],
            'editorialCategory':m.get('editorialCategory'),
            'title':m['title'],'sourceUrl':m['sourceUrl'],'sourcePublishedDate':None if repo_observation else source_day.isoformat(),
            'recommendationObservedDate':source_day.isoformat() if repo_observation else None,'sourceAgeDays':age,
            'firstStoredAt':first[identity].isoformat(),'revisionStoredAt':created.isoformat(),
            'manuscriptAssetId':receipt['assetId'],'manuscriptSha256':receipt['sha256'],
            'imageCount':count,'mediaReadiness':'complete' if graphics_ready(m) else 'pending-images',
            'productionStatus':'unconfirmed','publicationStatus':'unconfirmed','playbackStatus':'no-receipt'})
    items.sort(key=lambda i:(i['category'],i['title']))
    count=len(items);missing=max(0,target-count);deadline=datetime.combine(day,time.fromisoformat(policy['deadlineLocal']),zone)
    budget_remaining=max(0,policy['modelBudgetDaily']-model_calls)
    status='met' if not missing else 'overdue' if now>=deadline else 'in-progress'
    reasons=[]
    if missing and not budget_remaining:reasons.append('EDITOR_DAILY_BUDGET_EXHAUSTED')
    if status=='overdue':reasons.append('DAILY_NEW_STORY_TARGET_UNMET')
    result = {'schema':'yumi.daily-supply-status.v1','observedAt':now.isoformat(),'editionDate':day.isoformat(),
        'timezone':policy['timezone'],'minimumNewStories':target,'newStoriesStored':count,'missingStories':missing,
        'targetStatus':status,'deadlineAt':deadline.isoformat(),'modelCallsToday':model_calls,
        'modelBudgetDaily':policy['modelBudgetDaily'],'modelCallsRemaining':budget_remaining,
        'mediaComplete':sum(i['mediaReadiness']=='complete' for i in items),'pendingImages':sum(i['mediaReadiness']!='complete' for i in items),
        'categoryCounts':{c:sum(i['category']==c for i in items) for c in policy['categories']},
        'revisedExistingStoriesExcluded':revised,'rejectedOrCorrectionRowsToday':rejected,
        'staleSourcesExcluded':stale,'invalidRowsExcluded':invalid,'reasons':reasons,
        'deduplication':'canonicalStoryId when supplied, otherwise normalized source URL; same-day revisions count once',
        'newContentMeaning':'source-bound draft stored today for a previously unseen story; not a first-publication-date or broadcast claim',
        'productionConfirmed':None,'publicationConfirmed':None,'playbackConfirmed':None,'items':items}
    return augment(result)

def from_ledger(db,now=None,policy=None):
    import json
    now=now or datetime.now(timezone.utc);policy={**POLICY,**(policy or {})}
    day=now.astimezone(ZoneInfo(policy['timezone'])).date().isoformat()
    rows=[{'manifest':json.loads(row[0]),'state':row[1]} for row in db.execute(
        'SELECT manifest,state FROM media_topics WHERE manifest IS NOT NULL')]
    calls=db.execute('SELECT COUNT(*) FROM models WHERE day=?',(day,)).fetchone()[0]
    return summarize(rows,calls,now,policy)

if __name__=='__main__':
    import argparse,json,sqlite3
    from pathlib import Path
    parser=argparse.ArgumentParser();parser.add_argument('--ledger',required=True);args=parser.parse_args()
    with sqlite3.connect(Path(args.ledger).resolve(strict=True).as_uri()+'?mode=ro',uri=True) as db:
        print(json.dumps(from_ledger(db),ensure_ascii=False,indent=2))
