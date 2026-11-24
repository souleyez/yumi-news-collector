"""Discover owner-uploaded completion evidence through existing DataHub reads."""
import hashlib,json,re
from pathlib import Path
from existing_voice_news_receipts import CONTRACT,validate
from supply_upload import read_original

SCHEMA='yumi.category-production-completion-upload.v1'
LOGICAL_NAME='category-production-completion'
FILE_PATTERN=re.compile(r'^category-production-completion\.v1\.([a-f0-9]{64})\.([a-f0-9]{64})\.json$')

def ingest(ledger,uploader,reader=read_original):
    from news_pipeline import DATASETS,GateError,atomic,encoded,immutable,sha
    base='/datasets/'+DATASETS['ai-news'];after=None;seen_cursors=set();candidates=[];pages=0
    for _ in range(200):
        result=uploader.api.request(base+'/items'+('?after='+after if after else ''))['data'];pages+=1
        for item in result['items']:
            if item.get('kind')=='asset' and FILE_PATTERN.fullmatch(item.get('title') or ''):candidates.append(item)
        after=result.get('nextCursor')
        if after is None:break
        if not re.fullmatch('[A-Za-z0-9_-]{1,1024}',after) or after in seen_cursors:
            raise ValueError('COMPLETION_DISCOVERY_CURSOR_INVALID')
        seen_cursors.add(after)
    else:raise ValueError('COMPLETION_DISCOVERY_PAGE_LIMIT')
    status={'schema':'yumi.category-completion-ingestion.v1','pages':pages,'discovered':len(candidates),
            'ingested':0,'alreadyRegistered':0,'unconfirmed':[],'productionDispatches':0}
    folder=Path(ledger.root)/'media/native-production-evidence';folder.mkdir(parents=True,exist_ok=True)
    for item in candidates:
        match=FILE_PATTERN.fullmatch(item['title']);key,script=match.groups()
        try:
            row=ledger.db.execute("SELECT manifest FROM media_topics WHERE key=? AND state='synced'",(key,)).fetchone()
            if row is None:raise ValueError('COMPLETION_SOURCE_NOT_REGISTERED')
            manifest=json.loads(row[0]);detail=uploader.api.request(base+'/items/asset/'+item['id'])['data']
            original=detail['original'];path=original['downloadPath']
            if not re.fullmatch(re.escape(base)+r'/uploads/[a-f0-9-]{36}/original',path or ''):
                raise ValueError('COMPLETION_ORIGINAL_ROUTE_INVALID')
            raw=reader(uploader.api,path)
            if len(raw)>2*1024*1024 or len(raw)!=original['sizeBytes'] or sha(raw)!=original['sha256']:
                raise ValueError('COMPLETION_ORIGINAL_HASH_INVALID')
            value=json.loads(raw)
            archive=Path(ledger.root)/'media/verified-completion-envelopes';archive.mkdir(parents=True,exist_ok=True)
            immutable(archive/(sha(raw)+'.json'),raw)
            if (value.get('schema')!=SCHEMA or value.get('logicalName')!=LOGICAL_NAME or value.get('contractVersion')!=1
                or value.get('materialKey')!=key or value.get('scriptSha256')!=script):
                raise ValueError('COMPLETION_ENVELOPE_INVALID')
            evidence=value['evidence']
            if evidence.get('contractVersion')!=CONTRACT or evidence.get('materialKey')!=key or evidence.get('scriptSha256')!=script:
                raise ValueError('COMPLETION_EVIDENCE_BINDING_INVALID')
            validate(manifest,evidence)
            destination=folder/('existing-voice-'+key+'-'+script+'.json');body=encoded(evidence)
            if destination.exists():
                if destination.is_symlink() or destination.read_bytes()!=body:raise ValueError('COMPLETION_ORIGINAL_EVIDENCE_CHANGED')
                status['alreadyRegistered']+=1
            else:
                immutable(destination,body);status['ingested']+=1
        except (GateError,ValueError,KeyError,TypeError,OSError) as error:
            reason=str(error)
            status['unconfirmed'].append({'assetId':item['id'],'materialKey':key,
                'reason':reason if re.fullmatch('[A-Z][A-Z0-9_:.-]{1,160}',reason) else type(error).__name__})
    atomic(Path(ledger.root)/'category-production-completion-ingestion.json',status)
    return status
