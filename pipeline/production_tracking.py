"""Evidence-only production tracking in the existing news worker; no scheduler."""
import hashlib,json,re
from datetime import datetime,timezone
from pathlib import Path
from runtime_config import DATAHUB_ORIGIN

CONTRACT='digital-human.news-production-readback.v1'
HASH=re.compile(r'^[a-f0-9]{64}$')
STAGES={'tts':'tts-audio','lipsync':'lip-video','playableVideo':'composed-playable-video'}
def digest(value):
 return hashlib.sha256((json.dumps(value,ensure_ascii=False,indent=2,sort_keys=True)+'\n').encode()).hexdigest()
def timestamp_ok(value):
 try:return datetime.fromisoformat(value.replace('Z','+00:00')).tzinfo is not None
 except (ValueError,TypeError,AttributeError):return False
def inspect(manifest,receipt=None):
 key=manifest['key'];paragraphs=manifest.get('paragraphs',[])
 script_hash=manifest.get('scriptSha256') or digest(paragraphs)
 document=manifest.get('manuscript',{});stored=document.get('receipt',{})
 text=bool(paragraphs and stored.get('assetId') and stored.get('sha256')==document.get('sha256'))
 from local_image_policy import programme_images, graphics_ready
 images=programme_images(manifest)
 pictures=graphics_ready(manifest) and all(HASH.fullmatch(x.get('asset',{}).get('sha256','')) and x['asset'].get('assetId') for x in images)
 ready=bool(text and pictures);reasons=[]
 result={'materialKey':key,'scriptSha256':script_hash,'title':manifest['title'],'category':manifest['category'],
  'sourceUrl':manifest['sourceUrl'],'dataset':manifest['dataset'],'manuscriptAssetId':stored.get('assetId'),'textReady':text,'imagesReady':pictures,
  'imageCount':len(images),'graphicsStatus':'complete' if ready else 'pending',
  'audioStatus':'no-receipt','lipSyncStatus':'no-receipt','playableStatus':'no-receipt',
  'productionStatus':'ready_for_production' if ready else 'awaiting_graphics',
  'finished':False,'finishedAt':None,'scheduled':None,'scheduledAt':None,'played':None,'playedAt':None,
  'broadcastStatus':'unknown','playbackEvidence':'unavailable','tasks':[],'artifacts':[],'reasons':reasons}
 if receipt is None:
  reasons.append('PRODUCTION_RECEIPT_NOT_AVAILABLE');return result
 if receipt.get('contractVersion') in {'digital-human.native-news-evidence.v1','digital-human.existing-voice-news-evidence.v1'}:
  from native_news_receipts import apply_to_tracking
  try:return apply_to_tracking(manifest,receipt,result)
  except (ValueError,KeyError,TypeError) as error:
   reasons.append(str(error) if isinstance(error,ValueError) else 'NATIVE_RECEIPT_CONTRACT_INVALID');return result
 if not (receipt.get('contractVersion')==CONTRACT and receipt.get('materialKey')==key and receipt.get('scriptSha256')==script_hash):
  reasons.append('RECEIPT_VERSION_OR_SCRIPT_MISMATCH');return result
 source=receipt.get('source',{});doc=source.get('document',{})
 if not (doc.get('dataset')==manifest['dataset'] and doc.get('assetId')==stored.get('assetId') and doc.get('sha256')==document.get('sha256') and doc.get('version') is not None and source.get('materialManifestSha256')==digest(manifest)):
  reasons.append('RECEIPT_SOURCE_BINDING_MISMATCH');return result
 pipeline=receipt.get('pipeline',{})
 if not all(pipeline.get(k) for k in ['rootHash','runId','batchId']):
  reasons.append('PRODUCTION_PIPELINE_BINDING_MISSING');return result
 required={p.get('id') for p in paragraphs}
 if None in required or not required:
  reasons.append('PARAGRAPH_MAPPING_REQUIRED');return result
 tasks=receipt.get('tasks',[]);artifacts=receipt.get('artifacts',[])
 result['tasks']=[{k:t.get(k) for k in ['taskId','paragraphId','stage','state','submittedAt','completedAt']} for t in tasks]
 result['artifacts']=[{k:a.get(k) for k in ['role','taskId','dataset','assetId','version','sha256','mime','byteLength','completedAt']} for a in artifacts]
 complete_stages=[]
 for stage,role in STAGES.items():
  covered=set()
  for task in tasks:
   if not (task.get('stage')==stage and task.get('taskId') and task.get('state')=='completed' and timestamp_ok(task.get('submittedAt')) and timestamp_ok(task.get('completedAt'))):continue
   for asset in artifacts:
    if (asset.get('role')==role and asset.get('taskId')==task['taskId'] and asset.get('dataset') and asset.get('assetId') and asset.get('version') is not None and HASH.fullmatch(asset.get('sha256','')) and asset.get('authenticatedReadbackHttp')==200 and asset.get('readbackSha256')==asset['sha256'] and isinstance(asset.get('byteLength'),int) and asset['byteLength']>0 and timestamp_ok(asset.get('completedAt'))):
     covered.add(task.get('paragraphId'))
  done=required<=covered and receipt.get('stages',{}).get(stage,{}).get('state')=='completed'
  complete_stages.append(done)
  result[{'tts':'audioStatus','lipsync':'lipSyncStatus','playableVideo':'playableStatus'}[stage]]='completed' if done else 'incomplete'
 if ready and all(complete_stages) and receipt.get('complete') is True and timestamp_ok(receipt.get('completedAt')):
  result.update(finished=True,finishedAt=receipt['completedAt'],productionStatus='finished')
 else:
  result['productionStatus']='in_production';reasons.append('REQUIRED_PRODUCTION_EVIDENCE_INCOMPLETE')
 # Broadcast evidence has its own playout contract. A production receipt never proves playback.
 if receipt.get('broadcast',{}).get('state') not in (None,'pending','unknown'):
  result['broadcastStatus']='awaiting-playout-contract-verification'
 return result

def from_ledger(db,root,now=None):
 root=Path(root);items=[]
 online_path=root/'live-page-online-status.json';online=json.loads(online_path.read_text()).get('materials',{}) if online_path.is_file() else {}
 for row in db.execute("SELECT manifest FROM media_topics WHERE state='synced' AND manifest IS NOT NULL ORDER BY created DESC"):
  manifest=json.loads(row[0]);key=manifest.get('key','');script=manifest.get('scriptSha256') or digest(manifest.get('paragraphs',[]))
  if not HASH.fullmatch(key) or not HASH.fullmatch(script):continue
  path=root/'media/production-receipts'/key/script/'receipt.json';receipt=None;invalid=False
  if path.is_file() and not path.is_symlink():
   try:
    if path.stat().st_size>2*1024*1024:raise ValueError('RECEIPT_TOO_LARGE')
    receipt=json.loads(path.read_text())
    if not isinstance(receipt,dict):raise ValueError('RECEIPT_OBJECT_REQUIRED')
   except (ValueError,OSError):invalid=True
  result=inspect(manifest,receipt)
  published=online.get(key,{})
  result.update(online=published.get('online') is True,onlineSource='own-live-page',onlineConfirmedAt=published.get('firstConfirmedAt'),onlineLastSeenAt=published.get('lastConfirmedAt'))
  if invalid:result['reasons'].append('PRODUCTION_RECEIPT_UNREADABLE')
  items.append(result)
 native_path=root/'native-production-readback-status.json';native=json.loads(native_path.read_text()) if native_path.is_file() else {'verified':0}
 return {'schema':'yumi.production-tracking.v1','observedAt':(now or datetime.now(timezone.utc)).isoformat(),
  'completionMeaning':'graphics + original paragraph-bound TTS/lipsync task receipts + exported playable artifacts with full hash readback; native content-addressed references retained without invented DataHub IDs',
  'receiptContract':CONTRACT,'nativeReceiptContract':'digital-human.native-news-evidence.v1','nativeReadback':native,
  'receiptTransport':'existing-native-evidence-and-public-readback' if native.get('verified',0) else 'existing-workflow-binding-not-yet-confirmed',
  'totalMaterials':len(items),'graphicsReady':sum(x['graphicsStatus']=='complete' for x in items),
  'finished':sum(x['finished'] for x in items),'scheduledConfirmed':sum(x['scheduled'] is True for x in items),'playedConfirmed':sum(x['played'] is True for x in items),
  'unknownPlayback':sum(x['played'] is None for x in items),'countsMeaning':'confirmed only; missing playback receipt remains unknown, not never-played',
  'partialPlaybackObserved':sum(x.get('broadcastStatus')=='observed-partial' for x in items),
  'fullPlaybackVerified':sum(x.get('fullPlaybackVerified') is True for x in items),
  'onlineConfirmed':sum(x['online'] for x in items),'onlineConfirmationRule':'our own live page lists the news; extra playback checks are not required',
  'unverifiedIsIncomplete':True,'items':items}

def render_report(snapshot):
 lines=['# 新闻采编、制作与上线跟踪','','在我们自己的直播页读到该条新闻，即记为已上线。制作细项用于了解进度和排查，不增加上线门槛。',
  '',f"当前记录 {snapshot['totalMaterials']} 篇；图文齐备 {snapshot['graphicsReady']} 篇；制作完成 {snapshot['finished']} 篇；已上线 {snapshot['onlineConfirmed']} 篇。",'',
  '| 稿件 | 图文 | 配音 | 口型 | 可播成品 | 制作 | 自有直播页 |','|---|---|---|---|---|---|---|']
 for item in snapshot['items']:
  page=DATAHUB_ORIGIN+'/datasets/'+item['dataset']+'?contentKind=asset&contentId='+str(item['manuscriptAssetId'])
  title=item['title'].replace('|','／').replace('\n',' ')
  stage=lambda name:'已核实' if item[name]=='completed' else '待回执'
  lines.append('| ['+title+']('+page+') | '+('齐备' if item['graphicsStatus']=='complete' else '待补齐')+' | '+stage('audioStatus')+' | '+stage('lipSyncStatus')+' | '+stage('playableStatus')+' | '+('完成' if item['finished'] else '进行中')+' | '+('已上线' if item.get('online') else '尚未读到上线')+' |')
 lines+=['','图文列表包含既有供稿记录。新闻原文日期、来源与图片类型见各篇稿件；修订不计为新来源。','']
 return '\n'.join(lines).encode()

def publish_report(ledger,uploader,snapshot):
 from media_pipeline import artifact,sha
 raw=render_report(snapshot)
 a=artifact(ledger,'ai-news',raw,'新闻制作与播出跟踪-'+sha(raw)[:16]+'.md','text/markdown','manuscript')
 a.update(dateVerified=True,imageCount=0)
 r=uploader.sync(a)['receipt']
 got=uploader.api.request('/datasets/'+a['dataset']+'/uploads/'+r['id'])['data']
 if got['sha256']!=a['sha256'] or got['assetId']!=r['assetId']:raise ValueError('TRACKING_REPORT_READBACK_MISMATCH')
 return {'dataset':a['dataset'],'assetId':r['assetId'],'uploadId':r['id'],'sha256':a['sha256'],
  'assetPage':DATAHUB_ORIGIN+'/datasets/'+a['dataset']+'?contentKind=asset&contentId='+r['assetId'],
  'allReadbacksVerified':True,'unchangedReportReusesExistingUpload':True}
