"""Versioned category supply publication through the existing DataHub upload API."""
import hashlib
import json
from pathlib import Path
import urllib.error
import urllib.request
import uuid

LOGICAL_NAME='category-production-supply'
FILE_NAME='category-production-supply.v1.json'
MAX_BYTES=8*1024*1024

def encoded(value):
    return (json.dumps(value,ensure_ascii=False,sort_keys=True,indent=2)+'\n').encode()

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def document(snapshot):
    value={k:v for k,v in snapshot.items() if k not in {'observedAt','version','logicalName','contractVersion'}}
    value.update(logicalName=LOGICAL_NAME,contractVersion=1)
    value['version']=sha(encoded(value))
    raw=encoded(value)
    validate(raw)
    return raw,value

def validate(raw):
    if not 0<len(raw)<=MAX_BYTES:
        raise ValueError('SUPPLY_SIZE_INVALID')
    value=json.loads(raw)
    if (value.get('schema')!='yumi.category-supply-projection.v1' or value.get('logicalName')!=LOGICAL_NAME
        or value.get('contractVersion')!=1 or not isinstance(value.get('materials'),list)):
        raise ValueError('SUPPLY_CONTRACT_INVALID')
    if value.get('version')!=sha(encoded({k:v for k,v in value.items() if k!='version'})):
        raise ValueError('SUPPLY_VERSION_INVALID')
    identities=set()
    for item in value['materials']:
        digest=item.get('scriptSha256')
        # The projection's script hash is the native paragraph digest, never the MD-file hash.
        if digest!=sha(encoded(item['paragraphs'])) or item.get('manuscriptSha256')!=item['manuscript']['sha256']:
            raise ValueError('SUPPLY_SCRIPT_HASH_INVALID')
        identity=(item['materialKey'],digest)
        if identity in identities:
            raise ValueError('SUPPLY_DUPLICATE_MATERIAL')
        identities.add(identity)
    return value

def read_original(api,path):
    from news_pipeline import GateError,NoRedirect
    request=urllib.request.Request(api.base+path,headers={'Authorization':'Bearer '+api.token,'Accept':'application/octet-stream'})
    try:
        with urllib.request.build_opener(NoRedirect()).open(request,timeout=45) as response:
            if response.status!=200:raise GateError('SUPPLY_ORIGINAL_HTTP_INVALID')
            raw=response.read(MAX_BYTES+1)
            if len(raw)>MAX_BYTES:raise GateError('SUPPLY_ORIGINAL_TOO_LARGE')
            return raw
    except urllib.error.HTTPError as error:
        raise GateError('SUPPLY_ORIGINAL_HTTP_'+str(error.code)) from None
    except OSError:
        raise GateError('SUPPLY_ORIGINAL_TRANSPORT_UNKNOWN') from None

def publish(ledger,uploader,snapshot,reader=read_original):
    raw,value=document(snapshot)
    digest=sha(raw)
    relative='media/outbox/ai-news/category-production-supply/'+digest+'.json'
    path=ledger.root/relative
    path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists():
        if path.read_bytes()!=raw:raise ValueError('SUPPLY_IMMUTABLE_BYTES_CHANGED')
    else:
        with path.open('xb') as output:
            path.chmod(0o600);output.write(raw)
    from news_pipeline import DATASETS
    manifest={'key':sha(encoded([LOGICAL_NAME,digest])),'category':'ai-news','dataset':DATASETS['ai-news'],
              'kind':'supply-projection','relativePath':relative,'fileName':FILE_NAME,'mediaType':'text/plain',
              'sha256':digest,'sizeBytes':len(raw),
              'requestId':str(uuid.uuid5(uuid.NAMESPACE_URL,'yumi-category-supply-v1:'+digest)),
              'processingMode':'store-only','reviewRequired':True,'broadcastApproved':False,
              'originNote':'Yumi category-production-supply v1; machine discovery projection; no processing or broadcast approval'}
    receipt=uploader.sync(manifest)['receipt']
    base='/datasets/'+manifest['dataset']
    current=uploader.api.request(base+'/uploads/'+receipt['id'])['data']
    if any(current.get(k)!=manifest[k] for k in ['fileName','mediaType','sha256','sizeBytes','processingMode']):
        raise ValueError('SUPPLY_UPLOAD_READBACK_CHANGED')
    if current['assetId']!=receipt['assetId']:
        raise ValueError('SUPPLY_ASSET_ID_CHANGED')
    original=reader(uploader.api,base+'/uploads/'+receipt['id']+'/original')
    if original!=raw or sha(original)!=digest:
        raise ValueError('SUPPLY_ORIGINAL_HASH_MISMATCH')
    validate(original)
    return {'schema':'yumi.category-supply-upload-receipt.v1','logicalName':LOGICAL_NAME,'fileName':FILE_NAME,
            'contractVersion':1,'version':value['version'],'dataset':manifest['dataset'],
            'requestId':manifest['requestId'],'uploadId':receipt['id'],'assetId':receipt['assetId'],
            'sha256':digest,'sizeBytes':len(raw),'createdAt':current['createdAt'],
            'materialCount':len(value['materials']),'processingMode':'store-only',
            'originalBytesReadbackVerified':True,'unchangedContentReusesUpload':True,
            'itemsPath':base+'/items','originalPath':base+'/items/asset/'+receipt['assetId']+'/original',
            'uploadOriginalPath':base+'/uploads/'+receipt['id']+'/original',
            'productionAcceptance':'consumer-receipt-required'}
