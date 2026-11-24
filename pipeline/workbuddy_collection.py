"""An editorial collection in the existing AI dataset, not a news quota or queue."""
import json,re,uuid
from pathlib import Path
from runtime_config import WORKBUDDY_REFERENCE_FILE

KEY='workbuddy-promotion'
LABEL='Workbuddy推广'
TOPICS={'video':'AI视频创作','workflow':'AI工作流','stocks':'AI股票研究','income':'AI接单与变现'}
DOC_ROOT='https://www.workbuddy.cn/docs/workbuddy/From-Beginner-to-Expert-Guide/Practice-Cases/'
REFERENCES={
 'video':('Practice-Four','官方教程介绍WorkBuddy可根据需求生成短视频选题、口播文案、镜头分镜和素材清单；不据此推断已内置第三方视频生成模型或自动剪辑能力。'),
 'workflow':('Practice-Five','官方教程介绍WorkBuddy可在配置QQ邮箱连接器后整理资讯简报，先手动验证，再配置定时推送；具体操作依赖已有授权连接器。'),
 'stocks':('Practice-Three','官方教程介绍WorkBuddy可读取用户提供的Excel或CSV，按需求统计、生成图表和报告；这是数据整理分析能力，不证明能预测股价、自动交易或获得收益。'),
 'income':('Practice-Four','官方教程介绍WorkBuddy可辅助制作选题、正文、封面文案和短视频脚本；可据此策划面向客户的内容交付样稿，但未证明已有客户、订单、收入或固定接单价格。'),
}

def enrich(doc):
 selected=topic(doc)
 if selected:
  suffix,text=REFERENCES[selected]
  doc={**doc,'editorialReferences':[{'id':'wb1','text':text,'sourceUrl':DOC_ROOT+suffix,
       'verifiedAt':'2026-11-03','kind':'official-capability-reference'}]}
 return doc

def prioritize(items):
 ai=iter(sorted((i for i in items if i.get('category')=='ai-news'),key=lambda i:0 if topic(i) else 1))
 return [next(ai) if i.get('category')=='ai-news' else i for i in items]

def topic(value):
 if not WORKBUDDY_REFERENCE_FILE:return None
 text=(str(value.get('title',''))+' '+str(value.get('sourceUrl',''))).lower()
 if re.search(r'视频|短片|剪辑|video|filmmak',text):return 'video'
 if re.search(r'股票|财报|投研|stock|equity|earnings',text):return 'stocks'
 if re.search(r'接单|变现|赚钱|freelanc|monetiz|side.hustle',text):return 'income'
 if re.search(r'工作流|办公自动|workflow|automat|自媒体|内容创作',text):return 'workflow'

def tag(value):
 selected=topic(value)
 if value.get('category')=='ai-news' and selected:
  value.update(editorialCollection=KEY,editorialCollectionLabel=LABEL,applicationTopic=selected)
 return value

def apply(ledger,uploader):
 if not WORKBUDDY_REFERENCE_FILE:return {'status':'disabled'}
 source=Path(WORKBUDDY_REFERENCE_FILE)
 items=json.loads(source.read_text(encoding='utf-8'))['items']
 import media_pipeline as m
 base='/datasets/'+m.DATASETS['ai-news']
 tree=uploader.api.request(base+'/folders')['data']
 folders=tree['folders'];folder=next((f for f in folders if f['name']==LABEL and not f.get('parentId')),None)
 if folder is None:
  identity=str(uuid.uuid5(uuid.NAMESPACE_URL,base+'/'+LABEL))
  next_folders=folders+[{'id':identity,'name':LABEL,'parentId':None}]
  body={'requestId':str(uuid.uuid5(uuid.NAMESPACE_URL,base+'/'+LABEL+'/'+tree['revision'])),
        'revision':tree['revision'],'folders':next_folders}
  uploader.api.request(base+'/folders',body)
  checked=uploader.api.request(base+'/folders')['data']
  assert all(f in checked['folders'] for f in folders),'EXISTING_FOLDERS_CHANGED'
  folder=next(f for f in checked['folders'] if f['id']==identity and f['name']==LABEL)
 receipts=[]
 for item in items:
  body=item['markdown'].encode('utf-8')
  asset=m.artifact(ledger,'ai-news',body,item['fileName'],'text/markdown','manuscript')
  asset.update(editorialCategory='ai-general',editorialCollection=KEY,dateVerified=True,imageCount=0)
  proof=uploader.sync(asset)
  assert proof['receipt']['folderId']==folder['id'],'COLLECTION_FOLDER_MISMATCH'
  receipts.append({'fileName':item['fileName'],'assetId':proof['receipt']['assetId'],
                   'sha256':asset['sha256'],'folderId':folder['id']})
 report={'schema':'yumi.editorial-collection-receipt.v1','name':LABEL,'folderId':folder['id'],
         'referenceBriefs':receipts,'notDailyNews':True,'productionDispatches':0}
 (ledger.root/'workbuddy-collection-receipt.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
 return report
