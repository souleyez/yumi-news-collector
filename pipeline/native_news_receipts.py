"""Read existing native receipts and independently verify existing public playout assets."""
import hashlib,json,re,urllib.request
from datetime import datetime,timezone
from pathlib import Path
from runtime_config import LIVE_ORIGIN

CONTRACT='digital-human.native-news-evidence.v1'
HASH=re.compile(r'^[a-f0-9]{64}$')
BASE=LIVE_ORIGIN+'/fashion/api/live-channel/main'
EXT={'video/mp4':'mp4','image/png':'png','image/jpeg':'jpg','image/webp':'webp'}
def digest(value):return hashlib.sha256((json.dumps(value,ensure_ascii=False,sort_keys=True,indent=2)+'\n').encode()).hexdigest()
def require(value,code):
 if not value:raise ValueError(code)
def validate_native(manifest,e):
 if e.get('contractVersion')=='digital-human.existing-voice-news-evidence.v1':
  from existing_voice_news_receipts import validate
  return validate(manifest,e)
 require(e.get('contractVersion')==CONTRACT and e.get('materialKey')==manifest['key'] and e.get('scriptSha256')==manifest['scriptSha256'],'NATIVE_MATERIAL_SCRIPT_MISMATCH')
 mapping=e['mapping'];source=mapping['source'];doc=source['document'];stored=manifest['manuscript']
 require(mapping['materialKey']==manifest['key'] and mapping['scriptSha256']==manifest['scriptSha256'],'NATIVE_MAPPING_REVISION_MISMATCH')
 require(doc['dataset']==manifest['dataset'] and doc['assetId']==stored['receipt']['assetId'] and doc['sha256']==stored['sha256'] and source['materialManifestSha256']==digest(manifest),'NATIVE_SOURCE_BINDING_MISMATCH')
 # The actual source service returns no document.version. Preserve null; its exact SHA binds the revision.
 mapping_by_id={x['sourceParagraphId']:x for x in mapping['paragraphs']};originals={x['id']:x['text'] for x in e['sourceParagraphs']}
 require(set(mapping_by_id)=={p['id'] for p in manifest['paragraphs']},'NATIVE_PARAGRAPH_MAPPING_MISSING')
 for p in manifest['paragraphs']:
  m=mapping_by_id[p['id']]
  require(hashlib.sha256(p['text'].encode()).hexdigest()==m['textSha256'] and originals.get(m['programmeParagraphId'])==p['text'],'NATIVE_PARAGRAPH_TEXT_MISMATCH')
 d=e['delivery'];batch=e['exportBatch']['value'];parts=d['parts']
 require(d['materialKey']==manifest['key'] and d['scriptSha256']==manifest['scriptSha256'],'NATIVE_DELIVERY_BINDING_MISMATCH')
 require(d['productionComplete'] is True and len(parts)==len(mapping_by_id) and {p['partIndex'] for p in parts}==set(range(len(parts))),'NATIVE_EXPORT_INCOMPLETE')
 require(batch['phase']=='completed' and batch['saved']==len(parts) and batch['remaining']==0 and batch['runId']==d['runId'] and batch['rootHash']==d['rootHash'] and batch['batchId']==d['batchId'],'NATIVE_BATCH_INCOMPLETE')
 require(all(x.get('exportJobId') for x in batch['items']),'NATIVE_EXPORT_TASK_MISSING')
 voices={v['requestId']:v for v in e['native']['voices'] if v.get('status')=='completed' and v.get('receiptPresent') is True}
 lips={l['receipt']['manifestSha256']:l for l in e['native']['lips'] if l.get('state.json',{}).get('status')=='completed' and l.get('receipt')}
 for part in parts:
  provenance=part['provenance'];require(provenance['rootHash']==d['rootHash'] and provenance['runId']==d['runId'],'NATIVE_RUN_MISMATCH')
  for clip in part['clips']:
   receipt_hash=clip['id'].removeprefix('speech:');lip=lips.get(receipt_hash);require(lip is not None,'NATIVE_LIP_TASK_MISSING')
   require(lip.get('requestId') and lip['receipt']['video']['fullDecode']=='pass','NATIVE_LIP_NOT_DECODED')
   voice=voices.get(lip['voiceRequestId']);require(voice is not None,'NATIVE_VOICE_TASK_INCOMPLETE')
   audio=next((a for a in voice['items'] if a['id']==lip['voiceItemId']),None)
   require(audio is not None and audio['audioSha256']==lip['sourceMp3Sha256']==lip['receipt']['audio']['sourceMp3Sha256'] and audio['byteLength']>0,'NATIVE_AUDIO_BINDING_MISMATCH')
 require(d['privateVerification']['privateArchiveFullReadbackPass'] is True,'NATIVE_ARCHIVE_UNVERIFIED')
 require(e['publicVerification']['jobId']==d['actualQueueJobId']==e['queueAcceptance']['job']['id'],'NATIVE_QUEUE_BINDING_MISMATCH')
 return mapping_by_id

def read_public_json():
 with urllib.request.urlopen(BASE+'/state',timeout=20) as r:
  raw=r.read(4*1024*1024+1);require(r.status==200 and len(raw)<=4*1024*1024,'PUBLIC_STATE_LIMIT')
 return json.loads(raw)

def update_online_status(db,root,state):
 from news_pipeline import atomic
 root=Path(root);path=root/'live-page-online-status.json';now=datetime.now(timezone.utc).isoformat()
 previous=json.loads(path.read_text()) if path.is_file() else {'materials':{}}
 materials=previous.get('materials',{})
 registered={r[0] for r in db.execute("SELECT key FROM media_topics WHERE state='synced'")}
 for item in materials.values():item['currentlyListed']=False
 for answer in state.get('programme',{}).get('answers',[]):
  qid=answer.get('questionId','');key=qid.removeprefix('news:')
  if not qid.startswith('news:') or key not in registered:continue
  old=materials.get(key,{})
  materials[key]={'online':True,'currentlyListed':True,'firstConfirmedAt':old.get('firstConfirmedAt',now),
   'lastConfirmedAt':now,'source':'own-live-page','page':LIVE_ORIGIN+'/fashion/live-channel.html',
   'contentId':answer.get('contentId'),'generation':state.get('programmeGeneration')}
 atomic(path,{'schema':'yumi.live-page-online.v1','observedAt':now,
  'confirmationRule':'listed on our own live page; no full-playback or third-party acceptance required','materials':materials})

def verify_public(manifest,e,state,root):
 mapping=validate_native(manifest,e)
 answer=next((x for x in state.get('programme',{}).get('answers',[]) if x.get('questionId')=='news:'+manifest['key']),None)
 require(answer is not None and answer.get('contractVersion')=='dsh.live-channel.news.v1','PUBLIC_NEWS_NOT_AVAILABLE')
 if e.get('contractVersion')=='digital-human.existing-voice-news-evidence.v1':
  reference=e.get('publicVerification',{}).get('answer')
  if reference is not None:require(reference.get('contentId')==answer.get('contentId'),'PUBLIC_CONTENT_REVISION_MISMATCH')
  required={c['media']['sha256'] for p in e['parts'] for c in p['piece']['clips'] if c.get('text')}
  require(required<={a['sha256'] for a in answer['assets']},'PUBLIC_EXISTING_EXPORT_ASSETS_MISMATCH')
 else:require(answer['contentId']==e['publicVerification']['newsContentId'],'PUBLIC_CONTENT_REVISION_MISMATCH')
 matches=[]
 for i,paragraph in enumerate(manifest['paragraphs']):
  pid=mapping[paragraph.get('id') or 'p'+str(i+1)]['programmeParagraphId']
  clips=[c for c in answer['clips'] if c.get('text') and c.get('cursor',{}).get('paragraphId')==pid]
  require(''.join(c['text'] for c in clips)==paragraph['text'],'PUBLIC_PARAGRAPH_COVERAGE_MISMATCH')
  matches.append(pid)
 verified=[];cache=Path(root)/'media/production-public-verified';cache.mkdir(parents=True,exist_ok=True)
 from news_pipeline import atomic
 for asset in answer['assets']:
  sha=asset['sha256'];mime=asset['mime'];size=asset['byteLength']
  require(HASH.fullmatch(sha) and mime in EXT and isinstance(size,int) and 0<size<=64*1024*1024,'PUBLIC_ASSET_CONTRACT_INVALID')
  file=cache/(sha+'.json');hit=None
  if file.is_file():
   candidate=json.loads(file.read_text())
   if candidate.get('sha256')==sha and candidate.get('bytes')==size and candidate.get('fullHashMatch') is True:hit=candidate
  if hit is None:
   with urllib.request.urlopen(BASE+'/media/'+sha+'.'+EXT[mime],timeout=30) as r:
    h=hashlib.sha256();length=0
    while True:
     chunk=r.read(1024*1024)
     if not chunk:break
     length+=len(chunk);require(length<=size,'PUBLIC_ASSET_OVERSIZE');h.update(chunk)
    require(r.status==200 and length==size and h.hexdigest()==sha,'PUBLIC_ASSET_READBACK_MISMATCH')
   hit={'sha256':sha,'mime':mime,'bytes':length,'http':200,'fullHashMatch':True,'verifiedAt':datetime.now(timezone.utc).isoformat(),'referenceKind':'existing-live-channel-content-addressed-media'}
   atomic(file,hit)
  verified.append(hit)
 insertion=next((x for x in state['programme'].get('clock',{}).get('insertions',[]) if x.get('answer',{}).get('id')=='news:'+manifest['key']),None)
 require(insertion is not None,'PUBLIC_SCHEDULE_INSERTION_MISSING')
 return {'observedAt':datetime.now(timezone.utc).isoformat(),'generation':state['programmeGeneration'],
  'contentId':answer['contentId'],'paragraphs':matches,'assets':verified,'allAssetsVerified':True,
  'exactSourceTextCoverage':True,'durationMs':sum(c['durationMs'] for c in answer['clips']),
  'clipCount':len(answer['clips']),'actualImageCount':sum(x['mime'].startswith('image/') for x in verified),
  'insertion':insertion,'fullPlaybackVerified':False}

def refresh(db,root):
 if not LIVE_ORIGIN:return {'status':'not-configured'}
 root=Path(root);folder=root/'media/native-production-evidence';results=[]
 state=None
 from news_pipeline import atomic
 online_error=None
 try:
  state=read_public_json();update_online_status(db,root,state)
 except (ValueError,KeyError,TypeError,OSError) as error:
  online_error=type(error).__name__
 for file in sorted(folder.glob('*.json')) if folder.is_dir() else []:
  try:
   require(not file.is_symlink() and file.stat().st_size<=2*1024*1024,'NATIVE_EVIDENCE_FILE_INVALID')
   e=json.loads(file.read_text());key=e['materialKey'];script=e['scriptSha256'];require(HASH.fullmatch(key) and HASH.fullmatch(script),'NATIVE_KEY_INVALID')
   row=db.execute("SELECT manifest FROM media_topics WHERE key=? AND state='synced'",(key,)).fetchone();require(row is not None,'NATIVE_SOURCE_NOT_REGISTERED')
   manifest=json.loads(row[0]);validate_native(manifest,e)
   require(state is not None,'OWN_LIVE_PAGE_READ_UNCONFIRMED')
   public=verify_public(manifest,e,state,root)
   dst=root/'media/production-receipts'/key/script;dst.mkdir(parents=True,exist_ok=True)
   receipt={'contractVersion':e['contractVersion'],'materialKey':key,'scriptSha256':script,'evidence':e,'independentPublicReadback':public}
   atomic(dst/'receipt.json',receipt)
   results.append({'materialKey':key,'status':'native-and-public-evidence-verified','actualPublicAssets':len(public['assets']),'paragraphs':len(public['paragraphs'])})
  except (ValueError,KeyError,TypeError,OSError) as error:
   results.append({'file':file.name,'status':'unconfirmed','reason':str(error) if isinstance(error,ValueError) else type(error).__name__})
 status={'configured':len(results),'verified':sum(x['status']=='native-and-public-evidence-verified' for x in results),'items':results,'onlineConfirmation':'own-live-page','onlineReadError':online_error}
 atomic(root/'native-production-readback-status.json',status);return status

def apply_to_tracking(manifest,receipt,result):
 if receipt.get('contractVersion')=='digital-human.existing-voice-news-evidence.v1':
  from existing_voice_news_receipts import apply
  return apply(manifest,receipt,result)
 e=receipt['evidence'];mapping=validate_native(manifest,e);public=receipt['independentPublicReadback']
 require(public.get('allAssetsVerified') is True and public.get('exactSourceTextCoverage') is True and set(public['paragraphs'])=={x['programmeParagraphId'] for x in mapping.values()},'NATIVE_PUBLIC_PROOF_INCOMPLETE')
 require(all(a.get('http')==200 and a.get('fullHashMatch') is True for a in public['assets']),'NATIVE_PUBLIC_HASH_UNVERIFIED')
 delivery=e['delivery'];result.update(audioStatus='completed',lipSyncStatus='completed',playableStatus='completed',productionStatus='finished',finished=True,
  finishedAt=e['exportBatch']['value']['updatedAt'],finishedScope='technical production; human audiovisual quality not certified',
  scheduled=True,scheduledAt=e['queueAcceptance']['at'],scheduledAtKind='queue-accepted-time',
  programmeGeneration=public['generation'],newsContentId=public['contentId'],queueJobId=delivery['actualQueueJobId'],
  actualPublishedImages=public['actualImageCount'],actualPublishedDurationMs=public['durationMs'],
  sourceDocumentVersion=e['mapping']['source']['document'].get('version'),sourceRevisionBinding='exact manuscript and material manifest SHA256',
  nativeRunId=delivery['runId'],nativeBatchId=delivery['batchId'],humanAudiovisualAccepted=False,
  tasks=[{'stage':'tts','taskId':v['requestId'],'state':v['status']} for v in e['native']['voices']]+[{'stage':'lipsync','taskId':l['requestId'],'state':l['state.json']['status']} for l in e['native']['lips']]+[{'stage':'programme-export','taskId':t['exportJobId'],'paragraphId':t['paragraphId'],'state':t['state']} for t in e['exportBatch']['value']['items']],
  artifacts=public['assets'],broadcastStatus='scheduled',playbackEvidence='scheduled-only',fullPlaybackVerified=False)
 observation=e.get('playbackObservation',{})
 seen=[x for x in observation.get('observations',[]) if x.get('kind')=='news' and x.get('paragraph') in public['paragraphs'] and x.get('paused') is False and x.get('readyState',0)>=3]
 if observation.get('newsContentId')==public['contentId'] and observation.get('jobId')==result['queueJobId'] and observation.get('newsObserved') is True and observation.get('noManualSeek') is True and seen:
  result.update(played=True,playedAt=seen[0]['at'],playedAtKind='partial-browser-observation-time',broadcastStatus='observed-partial',playbackEvidence='operator-browser-partial-observation',narrationResumed=observation.get('narrationResumed') is True,fullPlaybackVerified=False,bilibiliAudienceAudioVerified=False)
 return result
