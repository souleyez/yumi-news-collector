"""Recover an already stored model response; never repeat an uncertain model call."""
import json

def recover(ledger,editor):
 import media_pipeline as m
 if editor is None:return {'recovered':0,'modelCalls':0}
 recovered=0;unconfirmed=[];rejected=[]
 for row in ledger.db.execute("SELECT * FROM models WHERE state='pending'").fetchall():
  key=row['key']
  if not (ledger.root/'models'/(key+'.response.json')).exists():continue
  for source in ledger.db.execute('SELECT * FROM media_sources WHERE document IS NOT NULL').fetchall():
   doc=json.loads(source['document'])
   identity=m.sha(m.encoded(['verified-date-editor-v2',source['category'],source['url'],m.source_fingerprint(doc),doc['publishedDate']]))
   if identity!=key:continue
   try:
    editor.draft(key,{**doc,'publishedAt':doc.get('publishedAt') or doc['publishedDate']})
    m.media_put(ledger,'media_sources',source['key'],state='collected',error=None)
    recovered+=1
   except (m.GateError,ValueError,KeyError) as error:
    reason=str(error) if isinstance(error,m.GateError) else 'SAVED_RESPONSE_INVALID'
    terminal=(isinstance(error,json.JSONDecodeError) or reason in {
     'EDITOR_JSON_INVALID','EDITOR_SCHEMA_INVALID','EDITOR_TITLE_INVALID',
     'EDITOR_PARAGRAPHS_INVALID','EDITOR_FACT_BINDING_INVALID','EDITOR_OUTPUT_LIMIT'})
    if terminal:
     try:
      editor.reject_saved_response(key,{**doc,'publishedAt':doc.get('publishedAt') or doc['publishedDate']},reason)
      m.media_put(ledger,'media_sources',source['key'],state='blocked',error='EDITOR_SAVED_RESPONSE_REJECTED')
      rejected.append({'key':key,'reason':reason})
     except (m.GateError,ValueError,KeyError,OSError):unconfirmed.append({'key':key,'reason':reason})
    else:unconfirmed.append({'key':key,'reason':reason})
   break
 resumed=0
 if ledger.db.execute("SELECT COUNT(*) FROM models WHERE state='pending'").fetchone()[0]==0:
  for source in ledger.db.execute("SELECT key FROM media_sources WHERE state='blocked' AND error='ORIGINAL_EDITOR_RESULT_UNKNOWN' AND document IS NOT NULL").fetchall():
   m.media_put(ledger,'media_sources',source['key'],state='collected',error=None);resumed+=1
 return {'recovered':recovered,'rejected':rejected,'resumedDependencySources':resumed,'unconfirmed':unconfirmed,'modelCalls':0}
