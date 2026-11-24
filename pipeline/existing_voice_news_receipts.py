"""Recognize actual existing-voice whole-article evidence, without legacy export IDs."""
import hashlib,json,re

CONTRACT='digital-human.existing-voice-news-evidence.v1'
HASH=re.compile(r'^[a-f0-9]{64}$')

def need(value,code):
    if not value:raise ValueError(code)

def canonical(value):
    if isinstance(value,list):return [canonical(v) for v in value]
    if isinstance(value,dict):return {k:canonical(v) for k,v in value.items() if k!='bytes'}
    return value

def compact_hash(value):
    return hashlib.sha256(json.dumps(canonical(value),ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def paragraph_digest(value):
    return hashlib.sha256((json.dumps(value,ensure_ascii=False,sort_keys=True,indent=2)+'\n').encode()).hexdigest()

def text_length(text):return len(text.encode('utf-16-le'))//2
def text_slice(text,start,end):return text.encode('utf-16-le')[start*2:end*2].decode('utf-16-le')

def native_article_binding(e):
    # Current producer keeps articleId in the signed native inventory and mapping.
    # Normalize only in memory; preserve the uploaded evidence bytes unchanged.
    if 'articleId' not in e:
        article=e.get('lipArticle',{}).get('articleId')
        need(isinstance(article,str) and bool(article) and e.get('mapping',{}).get('articleId')==article,
             'EXISTING_NEWS_BINDING_MISSING')
        return {**e,'articleId':article}
    return e

def validate(manifest,e):
    e=native_article_binding(e)
    script=manifest.get('scriptSha256') or paragraph_digest(manifest['paragraphs'])
    need(e.get('contractVersion')==CONTRACT and e.get('materialKey')==manifest['key'] and e.get('scriptSha256')==script,'EXISTING_NEWS_MATERIAL_SCRIPT_CHANGED')
    need(e.get('manuscriptSha256')==manifest['manuscript']['sha256']==manifest['manuscript']['receipt']['sha256'],'EXISTING_NEWS_MANUSCRIPT_CHANGED')
    need(all(HASH.fullmatch(e.get(k,'')) for k in ['rootHash','libraryId']) and e.get('runId') and e.get('articleId'),'EXISTING_NEWS_BINDING_MISSING')
    lip=e['lipArticle'];snapshot=lip['snapshotSha256'];source_manifest=e.get('sourceManifestSha256',lip.get('sourceManifestSha256'))
    need(HASH.fullmatch(source_manifest or ''),'EXISTING_NEWS_SOURCE_MANIFEST_MISSING')
    need(lip.get('contractVersion')=='digital-human.existing-voice-article.v1' and lip.get('rootHash')==e['rootHash']
         and lip.get('articleId')==e['articleId'] and lip.get('sourceManifestSha256')==source_manifest
         and HASH.fullmatch(snapshot) and compact_hash({k:v for k,v in lip.items() if k!='snapshotSha256'})==snapshot,
         'EXISTING_NEWS_LIP_SNAPSHOT_CHANGED')
    need(lip.get('ready') is True and lip.get('pending')==[] and lip.get('noNewSynthesis') is True,'EXISTING_NEWS_LIP_NOT_READY')
    paragraphs=lip['paragraphs'];original=manifest['paragraphs']
    need(len(paragraphs)==len(original)>0 and len({p['id'] for p in paragraphs})==len(paragraphs),'EXISTING_NEWS_PARAGRAPHS_CHANGED')
    mapping={}
    for i,(source,paragraph) in enumerate(zip(original,paragraphs)):
        need(source['text']==paragraph['text'],'EXISTING_NEWS_PARAGRAPH_TEXT_CHANGED')
        mapping[source.get('id') or 'p'+str(i+1)]={'programmeParagraphId':paragraph['id']}
    if e.get('source'):
        need(e['source'].get('dataset')==manifest['dataset'] and e['source'].get('manuscriptAssetId')==manifest['manuscript']['receipt']['assetId'],
             'EXISTING_NEWS_SOURCE_DOCUMENT_CHANGED')
    if e.get('mapping'):
        rows=e['mapping']['paragraphs']
        need(e['mapping'].get('articleId')==e['articleId'] and len(rows)==len(original),'EXISTING_NEWS_PARAGRAPH_MAPPING_CHANGED')
        for i,(row,source) in enumerate(zip(rows,original)):
            source_id=source.get('id') or 'p'+str(i+1)
            need(row.get('originalId')==source_id and row.get('programmeParagraphId')==mapping[source_id]['programmeParagraphId']
                 and row.get('textSha256')==hashlib.sha256(source['text'].encode()).hexdigest(),'EXISTING_NEWS_PARAGRAPH_MAPPING_CHANGED')
    voice=e['voice'];audio=voice.get('audio',voice)
    need(voice.get('originalResultsVerified',True) is True and audio.get('contractVersion')=='souleye.news-programme-audio.v2'
         and audio.get('programmeManifestSha256')==e['rootHash'] and audio.get('articleId')==e['articleId']
         and audio.get('manifestSha256')==compact_hash({k:v for k,v in audio.items() if k!='manifestSha256'}),
         'EXISTING_NEWS_AUDIO_MANIFEST_CHANGED')
    need(audio.get('voiceBinding',{}).get('approved') is True and audio['voiceBinding'].get('voiceId')=='female-clear'
         and audio.get('requestIds'),'EXISTING_NEWS_ORIGINAL_VOICE_MISSING')
    segments={s['id']:s for s in audio['segments']};assets={a['sha256']:a for a in audio['assets']}
    chunks=lip['chunks'];need(0<len(chunks)<=64 and len({c['segmentId'] for c in chunks})==len(chunks),'EXISTING_NEWS_CHUNK_COVERAGE_CHANGED')
    need(len({c.get('requestId') for c in chunks})==len(chunks) and all(c.get('requestId') for c in chunks),'EXISTING_NEWS_LIP_REQUEST_MISSING')
    ends={p['id']:0 for p in paragraphs};by_id={p['id']:p for p in paragraphs};prior=None
    for c in chunks:
        paragraph=by_id[c['paragraphId']];start=c['start'];end=c['end'];segment=segments.get(c['segmentId'])
        need(type(start) is int and type(end) is int and start==ends[paragraph['id']] and start<end<=text_length(paragraph['text']),
             'EXISTING_NEWS_CHUNK_TEXT_CHANGED')
        need(segment is not None and segment['paragraphId']==paragraph['id'] and segment['start']==start and segment['end']==end
             and segment['text']==text_slice(paragraph['text'],start,end) and segment['audioSha256']==c['sourceMp3Sha256'],
             'EXISTING_NEWS_AUDIO_BINDING_CHANGED')
        asset=assets.get(c['sourceMp3Sha256'])
        need(asset is not None and asset.get('mime')=='audio/mpeg' and asset.get('fullDecode')=='pass'
             and type(asset.get('byteLength')) is int and asset['byteLength']>0,'EXISTING_NEWS_AUDIO_NOT_VERIFIED')
        need(c.get('ready') is True and c.get('state')=='completed' and all(HASH.fullmatch(c.get(k,'')) for k in ['receiptSha256','videoSha256','sourceMp3Sha256']),
             'EXISTING_NEWS_LIP_NOT_READY')
        begin=c['sourcePhase']['baseStartFrame'];cursor=c['actualSourceCursor']
        need(type(begin) is int and begin>=0 and type(cursor.get('nextSourceFrame')) is int and cursor['nextSourceFrame']>=0
             and cursor.get('receiptManifestSha256')==c['receiptSha256'] and begin==(0 if prior is None else prior),
             'EXISTING_NEWS_SOURCE_CURSOR_CHANGED')
        prior=cursor['nextSourceFrame'];ends[paragraph['id']]=end
    need(set(segments)=={c['segmentId'] for c in chunks} and all(ends[p['id']]==text_length(p['text']) for p in paragraphs),
         'EXISTING_NEWS_CHUNK_COVERAGE_CHANGED')
    job=e['exportJob'];parts=e['parts']
    need(job.get('id') and job.get('kind')=='window' and job.get('state')=='completed'
         and job.get('articleId')==e['articleId'] and job.get('partIndex')==0,'EXISTING_NEWS_EXPORT_JOB_INCOMPLETE')
    need(len(parts)==len(paragraphs) and {p['piece']['partIndex'] for p in parts}==set(range(len(parts))),'EXISTING_NEWS_EXPORT_PARTS_INCOMPLETE')
    chunk_by_request={c['requestId']:c for c in chunks};used=set();body_sha=None
    for part in parts:
        piece=part['piece'];provenance=part['provenance'];index=piece['partIndex'];paragraph=paragraphs[index]
        need(piece.get('kind')=='programme-part' and piece.get('articleId')==e['articleId']
             and provenance.get('rootHash')==e['rootHash'] and provenance.get('runId')==e['runId']
             and provenance.get('sourceManifestSha256')==source_manifest
             and provenance.get('articleSnapshotSha256')==snapshot and provenance.get('sourceKind')=='existing-voice-article-v1'
             and provenance.get('exportSha256')==compact_hash(piece),'EXISTING_NEWS_EXPORT_PROVENANCE_CHANGED')
        selected=[c for c in chunks if c['paragraphId']==paragraph['id']]
        need(provenance.get('receiptHashes')==[c['receiptSha256'] for c in selected],'EXISTING_NEWS_EXPORT_RECEIPTS_CHANGED')
        spoken=[c for c in piece['clips'] if c.get('text')]
        need(''.join(c['text'] for c in spoken)==paragraph['text'] and len(spoken)==len(selected),'EXISTING_NEWS_EXPORT_TEXT_CHANGED')
        media={a['sha256']:a for a in piece['assets']}
        for clip,c in zip(spoken,selected):
            need(clip.get('id')=='existing-article:'+c['requestId'] and clip['id'] not in used
                 and clip['cursor']['paragraphId']==paragraph['id'] and clip['cursor']['start']==c['start'] and clip['cursor']['end']==c['end'],
                 'EXISTING_NEWS_EXPORT_CHUNK_CHANGED')
            used.add(clip['id']);phase=clip['nativePhase']
            need(phase['firstFrame']==c['sourcePhase']['baseStartFrame'] and phase['nextSourceFrame']==c['actualSourceCursor']['nextSourceFrame']
                 and HASH.fullmatch(phase.get('sourceSha256','')) and (body_sha is None or body_sha==phase['sourceSha256']),
                 'EXISTING_NEWS_EXPORT_CURSOR_CHANGED')
            body_sha=phase['sourceSha256'];a=clip['media']
            need(HASH.fullmatch(a.get('sha256','')) and a.get('mime')=='video/mp4' and a.get('byteLength',0)>0
                 and media.get(a['sha256'])==a,'EXISTING_NEWS_EXPORT_MEDIA_CHANGED')
    if 'queue' in e:
        queue=e['queue']
        need(queue.get('jobId') and queue.get('state')=='completed','EXISTING_NEWS_QUEUE_JOB_MISSING')
        need(all(queue.get(k)==e[k] for k in ['materialKey','scriptSha256','rootHash','libraryId','articleId'])
             and queue.get('questionId')=='news:'+manifest['key'],'EXISTING_NEWS_QUEUE_BINDING_CHANGED')
    else:
        queue=e['queueAcceptance']['job']
        need(queue.get('id') and queue.get('kind')=='queue-news','EXISTING_NEWS_QUEUE_JOB_MISSING')
        if queue.get('questionId') is not None:need(queue['questionId']=='news:'+manifest['key'],'EXISTING_NEWS_QUEUE_BINDING_CHANGED')
    return mapping

def apply(manifest,receipt,result):
    e=native_article_binding(receipt['evidence']);mapping=validate(manifest,e);public=receipt['independentPublicReadback']
    need(public.get('allAssetsVerified') is True and public.get('exactSourceTextCoverage') is True
         and set(public['paragraphs'])=={m['programmeParagraphId'] for m in mapping.values()},'EXISTING_NEWS_PUBLIC_PROOF_INCOMPLETE')
    need(all(a.get('http')==200 and a.get('fullHashMatch') is True for a in public['assets']),'EXISTING_NEWS_PUBLIC_HASH_UNVERIFIED')
    audio=e['voice'].get('audio',e['voice']);job=e['exportJob']
    queue_id=e['queue']['jobId'] if 'queue' in e else e['queueAcceptance']['job']['id']
    result.update(audioStatus='completed',lipSyncStatus='completed',playableStatus='completed',productionStatus='finished',finished=True,
                  finishedAt=e.get('completedAt'),finishedScope='technical production; human audiovisual quality not certified',
                  finishedAtKind='completion-evidence-observed-time',
                  scheduled=True,scheduledAt=e.get('queueAcceptance',{}).get('at'),scheduledAtKind='not-recorded' if 'queue' in e else 'queue-accepted-time',
                  programmeGeneration=public['generation'],newsContentId=public['contentId'],queueJobId=queue_id,
                  actualPublishedImages=public['actualImageCount'],actualPublishedDurationMs=public['durationMs'],
                  sourceDocumentVersion=None,sourceRevisionBinding='exact manuscript and native paragraph SHA256',
                  nativeRunId=e['runId'],nativeBatchId=None,libraryId=e['libraryId'],humanAudiovisualAccepted=False,
                  tasks=[{'stage':'tts','taskId':i,'state':'completed'} for i in audio['requestIds']]
                    +[{'stage':'lipsync','taskId':c['requestId'],'state':c['state']} for c in e['lipArticle']['chunks']]
                    +[{'stage':'programme-export','taskId':job['id'],'articleId':e['articleId'],'state':job['state']}],
                  artifacts=public['assets'],broadcastStatus='scheduled',playbackEvidence='scheduled-only',fullPlaybackVerified=False)
    return result
