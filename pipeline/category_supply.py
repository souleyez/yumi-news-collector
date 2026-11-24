"""Read-only category quotas and the existing material supply contract projection."""
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

CATEGORIES = {
    'ai-general': 'AI综合新闻',
    'frontier-models': '前沿模型进展',
    'github-projects': 'GitHub热门开源项目推荐',
    'gaming': '游戏资讯',
    'hardware': '硬件资讯',
}
PER_CATEGORY = 5
MODEL_BUDGET_DAILY = 30
SOURCE_MAX_AGE_DAYS = {'hardware':7,'gaming':7}
SOURCE_CATEGORY_OVERRIDES = {
 'https://huggingface.co/blog/sergiopaniego/mopd-trl':'frontier-models',
 'https://huggingface.co/blog/jgalego/qwenguard':'frontier-models',
 'https://huggingface.co/blog/ibm-research/cuga-harness':'frontier-models',
 'https://news.samsung.com/global/samsung-champions-young-olympians-at-dakar-2026-first-ever-olympic-event-on-african-continent':'ai-general',
 'https://huggingface.co/blog/paidaxccc/how-to-build-your-own-hackertube-with-claude':'ai-general',
 'https://huggingface.co/blog/SamuelCagnani/olfacto-ai-br':'frontier-models',
 'https://huggingface.co/blog/harpreetsahota/boilingbench-multimodal-fiftyone-analysis':'frontier-models',
 'https://huggingface.co/blog/sirluk/sliding-window-beats-linear-attention':'frontier-models',
 'https://huggingface.co/blog/rairo/ouroboros-beam':'frontier-models',
}

def classify(value):
    if value.get('category') == 'entertainment':
        return None
    source = str(value.get('sourceUrl') or value.get('url') or '')
    if source in SOURCE_CATEGORY_OVERRIDES:return SOURCE_CATEGORY_OVERRIDES[source]
    explicit = value.get('editorialCategory')
    if explicit in CATEGORIES:
        return explicit
    title = str(value.get('title') or '')
    url = str(value.get('sourceUrl') or value.get('url') or '')
    text = (title + ' ' + url).lower()
    rules = [
        ('gaming', r'游戏|game\s*off|\bgaming\b|playstation|xbox|nintendo|\bsteam\b|game-off'),
        ('hardware', r'硬件|芯片|显卡|处理器|晶圆|制程|(?<![a-z0-9])(?:gpu|cpu)(?![a-z0-9])|geforce|radeon|rtx\s*\d|hardware|semiconductor'),
        ('github-projects', r'开源项目|开源工具|开源框架|github热门|github trending|github\.com/|open.source.project|open.source.tool'),
        ('frontier-models', r'前沿模型|大模型|新模型|模型发布|模型进展|基础模型|多模态|\bgpt[ -]?\d|(?<![a-z0-9])(?:claude|gemini|qwen|llama)(?![a-z0-9])|benchmark|dolphinbench|model.release'),
    ]
    for category, pattern in rules:
        if re.search(pattern, text):
            return category
    return 'ai-general'

def augment(status):
    counts = {key: 0 for key in CATEGORIES}
    for item in status['items']:
        category = classify(item)
        if category:
            item['editorialCategory'] = category
            item['editorialCategoryLabel'] = CATEGORIES[category]
            counts[category] += 1
    missing = {key: max(0, PER_CATEGORY - value) for key, value in counts.items()}
    status.update(
        editorialCategoryCounts=counts,
        editorialCategoryTargets={key: PER_CATEGORY for key in CATEGORIES},
        editorialCategoryLabels=CATEGORIES,
        editorialCategoryMissing=missing,
        minimumNewStories=PER_CATEGORY * len(CATEGORIES),
        newStoriesStored=sum(counts.values()),
        missingStories=sum(missing.values()),
        modelBudgetDaily=MODEL_BUDGET_DAILY,
        modelCallsRemaining=max(0, MODEL_BUDGET_DAILY-status['modelCallsToday']),
        productionRequested=True,
        posePolicy={'strategy': 'least-assigned-valid-pose', 'scope': 'cross-category-cross-batch',
                    'maxCountDifference': 1, 'fixedSceneCameraPosition': True,
                    'eligiblePoseAuthority': 'existing DSH workflow', 'excluded': ['retired-side-A']},
    )
    overdue = datetime.fromisoformat(status['observedAt']) >= datetime.fromisoformat(status['deadlineAt'])
    status['targetStatus'] = 'met' if not sum(missing.values()) else 'overdue' if overdue else 'in-progress'
    status['reasons'] = [r for r in status['reasons'] if r not in {'EDITOR_DAILY_BUDGET_EXHAUSTED','DAILY_NEW_STORY_TARGET_UNMET'}]
    if sum(missing.values()) and not status['modelCallsRemaining']:
        status['reasons'].append('EDITOR_DAILY_BUDGET_EXHAUSTED')
    if status['targetStatus'] == 'overdue':
        status['reasons'].append('DAILY_NEW_STORY_TARGET_UNMET')
    return status

def supply(db, day=None):
    """All current AI drafts are requested for production, including image-pending drafts.

    The consumer owns durable acceptance and valid-pose allocation. A projection is
    never an acceptance receipt, production completion, or permission to replay work.
    """
    day = day or datetime.now(ZoneInfo('Asia/Singapore')).date().isoformat()
    latest = {}
    for raw, in db.execute("SELECT manifest FROM media_topics WHERE state='synced' AND manifest IS NOT NULL"):
        value = json.loads(raw)
        if value.get('category') != 'ai-news':
            continue
        identity = value.get('canonicalStoryId') or value['sourceUrl']
        if identity not in latest or value['createdAt'] > latest[identity]['createdAt']:
            latest[identity] = value
    materials = []
    for value in latest.values():
        from local_image_policy import programme_view
        value = programme_view(value)
        if 'imageSequence' not in value and value.get('images') and value.get('paragraphs'):
            n=len(value['images']);p=len(value['paragraphs'])
            value['imageSequence']=[{'paragraphId':paragraph.get('id') or 'p'+str(i+1),
                'imageIndices':list(range(i*n//p,(i+1)*n//p)) or [min(n-1,i*n//p)]}
                for i,paragraph in enumerate(value['paragraphs'])]
            value['imageSequenceBasis']='article-image-order; existing article-related originals, no semantic caption claim'
        category = classify(value)
        manuscript = value['manuscript']
        digest = value.get('scriptSha256') or hashlib.sha256(
            (json.dumps(value['paragraphs'],ensure_ascii=False,indent=2,sort_keys=True)+'\n').encode()).hexdigest()
        if not re.fullmatch('[a-f0-9]{64}', digest):
            raise ValueError('INVALID_SCRIPT_HASH')
        # Preserve native paragraph/cue/image/source contracts rather than making a new queue.
        value.update(materialKey=value['key'], scriptSha256=digest, manuscriptSha256=manuscript['sha256'],
                     editorialCategory=category, editorialCategoryLabel=CATEGORIES[category],
                     productionRequested=True,
                     posePolicy='least-assigned-valid-pose', productionAcceptance='no-receipt')
        materials.append(value)
    materials.sort(key=lambda x:(x['createdAt'],x['materialKey']))
    return {'schema':'yumi.category-supply-projection.v1', 'editionDate':day,
            'observedAt':datetime.now(timezone.utc).isoformat(), 'materials':materials,
            'productionRequested':True, 'productionAcceptance':'consumer-receipt-required',
            'idempotency':'materialKey + scriptSha256; consumer reuses its existing business requestId',
            'purpose':'read-only discovery projection for the existing DSH/AP workflow; not a scheduler'}

if __name__ == '__main__':
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--ledger',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    with sqlite3.connect(args.ledger.resolve(strict=True).as_uri()+'?mode=ro',uri=True) as db:
        result=supply(db)
    temporary=args.output.with_suffix('.tmp')
    temporary.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    temporary.chmod(0o600)
    temporary.replace(args.output)
    print(json.dumps({'materials':len(result['materials']),'path':str(args.output),'productionAcceptance':result['productionAcceptance']}))
