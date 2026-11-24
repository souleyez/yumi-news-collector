"""Apply explicit editorial stills in the existing worker; never dispatch production."""
import json
from pathlib import Path
from runtime_config import DATAHUB_ORIGIN

def apply(ledger,uploader):
 import media_pipeline as m
 path=ledger.root/'media/editorial-image-supplements.json'
 if not path.exists():return {'applied':0}
 m.require(not path.is_symlink() and path.stat().st_size<=256*1024,'IMAGE_SUPPLEMENT_INPUT_INVALID')
 plan=json.loads(path.read_text());m.require(plan.get('schema')=='yumi.editorial-image-supplements.v1','IMAGE_SUPPLEMENT_SCHEMA_INVALID')
 applied=0
 for item in plan['items']:
  candidates=[]
  for row in ledger.db.execute("SELECT key,manifest FROM media_topics WHERE state='synced'"):
   manifest=json.loads(row['manifest'])
   if manifest['sourceUrl']==item['sourceUrl']:candidates.append((manifest['createdAt'],row['key'],manifest))
  if not candidates:continue
  _,key,manifest=max(candidates)
  revision=m.sha(m.encoded(item))
  if manifest.get('imageSupplement',{}).get('revision')==revision:continue
  from local_image_policy import validate_completed
  validate_completed(manifest,item['images'])
  refs=[img for img in manifest['images'] if not img.get('synthetic')]
  archived=manifest.get('sourceImages',[])
  m.require(len(refs)+len(item['images'])<=5,'IMAGE_SUPPLEMENT_COUNT_INVALID')
  for img in item['images']:
   m.require(img.get('synthetic') is True and img.get('sourcePageUrl')==item['sourceUrl'],'IMAGE_SUPPLEMENT_PROVENANCE_INVALID')
   p=m.local_file(ledger.root,img['relativePath']);raw=p.read_bytes();info=m.image_info(raw,img['mediaType'])
   m.require(info['sha256']==img['sha256'],'IMAGE_SUPPLEMENT_HASH_MISMATCH')
   if any(old['sha256']==info['sha256'] for old in refs):continue
   asset=m.artifact(ledger,manifest['category'],raw,'yumi-summary-'+info['sha256'][:20]+info['extension'],info['mediaType'],'image')
   asset['editorialCategory']=manifest['editorialCategory']
   if manifest.get('editorialCollection'):asset['editorialCollection']=manifest['editorialCollection']
   receipt=uploader.sync(asset)['receipt']
   refs.append({**img,**info,'editorialSupplement':True,'asset':{'kind':'upload-original','dataset':manifest['dataset'],
    'uploadId':receipt['id'],'assetId':receipt['assetId'],'sha256':info['sha256'],'mime':info['mediaType'],'width':info['width'],'height':info['height']},
    'assetPage':DATAHUB_ORIGIN+'/datasets/'+manifest['dataset']+'?contentKind=asset&contentId='+receipt['assetId'],
    'originalUrl':DATAHUB_ORIGIN+'/api/datafoundation/customer/v1/datasets/'+manifest['dataset']+'/items/asset/'+receipt['assetId']+'/original',
    'layout':'image-with-avatar-bottom-left','credit':'Codex imagegen原创事实示意图；非新闻实拍',
    'captionEvidence':'editorial_fact_summary','rightsStatus':'editorial-generated','captionOriginal':img['alt'],
    'safeZone':{'left':0,'top':0.56,'width':0.32,'height':0.44}})
  lines=['# '+manifest['title']+' · 配图补充','','原始口播、materialKey与scriptSha256保持。','来源：'+item['sourceUrl'],'']
  for ref in refs:lines+=['!['+ref.get('alt','配图')+']('+ref['originalUrl']+')','',ref.get('credit','原文配图'),'']
  body='\n'.join(lines).encode();appendix=m.artifact(ledger,manifest['category'],body,'配图补充-'+key[:12]+'-'+revision[:12]+'.md','text/markdown','manuscript')
  appendix.update(dateVerified=True,imageCount=len(refs),editorialCategory=manifest['editorialCategory'])
  if manifest.get('editorialCollection'):appendix['editorialCollection']=manifest['editorialCollection']
  receipt=uploader.sync(appendix)['receipt']
  manifest.update(images=refs,imageCount=len(refs),mediaReadiness='complete' if len(refs)>=3 else 'pending-images',missingImageCount=max(0,3-len(refs)),
    imageSupplement={'revision':revision,'manuscript':{**appendix,'receipt':receipt},'originalManuscriptPreserved':True})
  if 'imageGenerationPolicy' in manifest:
   manifest['imageGenerationStatus']='complete' if len(refs)>=3 else 'waiting-local-imagegen'
  manifest['sourceImages']=archived
  manifest['imageGenerationPolicy']={'provider':'local-codex-builtin-imagegen','fallback':None,'offlineAction':'wait-for-local-imagegen'}
  manifest['imageGenerationStatus']='complete' if len(refs)>=3 else 'waiting-local-imagegen'
  # Enrich the source material in place; paragraph bytes and production identity never change.
  m.immutable(m.local_file(ledger.root,'media/packages/'+key+'-images-'+revision[:12]+'.json'),m.encoded(manifest))
  m.media_put(ledger,'media_topics',key,manifest=json.dumps(manifest,ensure_ascii=False));applied+=1
 return {'applied':applied,'productionDispatches':0}
