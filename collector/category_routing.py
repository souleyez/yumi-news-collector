"""Two dataset routes, candidate classification, and multi-image editorial gates.

No network, credentials, model, dataset creation or broadcast approval.
"""
import hashlib
import json
from pathlib import Path
import re
import struct
from urllib.parse import urlsplit

from runtime_config import DATASETS

AI_DATASET = DATASETS['ai-news']
AI_WORDS = re.compile(r'(?<![A-Za-z0-9_])AI(?![A-Za-z0-9_])|AIGC|人工智能|大模型|智能体|Agent|DeepSeek|Qwen|千问|Gemini|Claude|GPT|LLM|OpenAI|NVIDIA|\bRAG\b|知识库|数据库|Docker|Linux|开源|GitHub|编程|开发者|机器人|数字人|语音合成|TTS|ComfyUI', re.I)
ENT_WORDS = re.compile(r'娱乐|八卦|明星|演员|导演|电影|票房|首映|综艺|新剧|电视剧|剧集|演唱会|音乐节|歌手|新专辑|红毯|时装周|时尚盛典|VOGUE|金鸡奖|百花奖|金像奖|金马奖|奥斯卡|戛纳|柏林电影节|威尼斯电影节', re.I)
RUMOR_WORDS = re.compile(r'网传|传闻|疑似|爆料|被曝|曝|恋情|出轨|私生|私密|偷拍|绯闻|离婚|怀孕|吸毒|被捕|指控')
KEY = re.compile(r'^[a-z0-9][a-z0-9._-]{0,127}$')
SHA = re.compile(r'^[a-f0-9]{64}$')


def load_catalog(path):
    if path is None or not Path(path).is_file():
        return {'schema': 'yumi.material-categories.v1', 'categories': {
            'ai-news': {'name': 'AI新闻', 'dataset': AI_DATASET, 'minimumImages': 0},
            'entertainment': {'name': '娱乐八卦', 'dataset': DATASETS.get('entertainment'), 'minimumImages': 3}}}
    catalog = json.loads(Path(path).read_text(encoding='utf-8-sig'))
    assert catalog['schema'] == 'yumi.material-categories.v1'
    assert set(catalog['categories']) == {'ai-news', 'entertainment'}
    categories = catalog['categories']
    for category, settings in categories.items():
        settings['dataset'] = DATASETS.get(category)
    assert categories['entertainment']['minimumImages'] >= 3
    targets = [row['dataset'] for row in categories.values() if row['dataset'] is not None]
    assert all(KEY.fullmatch(value) for value in targets) and len(set(targets)) == len(targets)
    return catalog


def classify(title):
    categories = []
    if AI_WORDS.search(title):
        categories.append('ai-news')
    if ENT_WORDS.search(title):
        categories.append('entertainment')
    return categories


def split_pool(pool, catalog):
    result = {key: {'schema': 'yumi.categorized-candidates.v1', 'category': key,
              'dataset': value['dataset'], 'categoryName': value['name'],
              'capturedAt': pool['capturedAt'], 'minimumImages': value['minimumImages'],
              'selectionMethod': 'keyword_candidates_not_model_scored', 'items': []}
              for key, value in catalog['categories'].items()}
    for row in pool['items']:
        matches = classify(row['title'])
        for key in matches:
            item = {**row, 'category': key, 'categoryCandidates': matches,
                    'routingState': 'candidate_only' if result[key]['dataset'] else 'dataset_not_bound',
                    'requiresExplicitSelection': True, 'broadcastApproved': False,
                    'imageCount': 0, 'minimumImages': result[key]['minimumImages']}
            if key == 'entertainment':
                item['editorialBlockers'] = ['needs_at_least_three_related_images', 'source_facts_not_verified']
                if RUMOR_WORDS.search(row['title']):
                    item['editorialBlockers'].append('unverified_personal_claim_do_not_present_as_fact')
            result[key]['items'].append(item)
    for value in result.values():
        value['candidateCount'] = len(value['items'])
    return result


def https_reference(url):
    if not isinstance(url, str) or any(c in url for c in '\r\n\t<>'):
        return False
    parsed = urlsplit(url)
    return parsed.scheme == 'https' and bool(parsed.hostname) and not parsed.username and not parsed.password


def multi_image_gate(item, category, catalog, package_root):
    """Require distinct real files and provenance; never confer playback approval."""
    if category not in catalog['categories']:
        raise ValueError('UNKNOWN_CATEGORY')
    rule = catalog['categories'][category]
    if not isinstance(item.get('images', []), list) or len(item.get('images', [])) > 24:
        raise ValueError('IMAGE_LIST_INVALID_OR_TOO_LARGE')
    root = Path(package_root).resolve()
    seen, valid, blockers = set(), [], []
    for image in item.get('images', []):
        if not isinstance(image, dict):
            blockers.append('invalid_image_descriptor')
            continue
        relative = image.get('localPath', '')
        if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
            blockers.append('image_path_must_be_package_relative')
            continue
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            blockers.append('image_file_missing_or_outside_package')
            continue
        if path.stat().st_size > 8 * 1024 * 1024:
            blockers.append('image_exceeds_eight_mib')
            continue
        body = path.read_bytes()
        sha = hashlib.sha256(body).hexdigest()
        if not SHA.fullmatch(image.get('sha256', '')) or image['sha256'] != sha:
            blockers.append('image_hash_mismatch')
            continue
        png = len(body) >= 24 and body[:8] == b'\x89PNG\r\n\x1a\n' and body[12:16] == b'IHDR'
        jpeg = len(body) >= 4 and body[:3] == b'\xff\xd8\xff' and body[-2:] == b'\xff\xd9'
        if not png and not jpeg:
            blockers.append('unsupported_image_signature')
            continue
        if png:
            width, height = struct.unpack('>II', body[16:24])
            if not (32 <= width <= 8192 and 32 <= height <= 8192 and width * height <= 25_000_000):
                blockers.append('image_dimensions_invalid')
                continue
        if sha in seen:
            blockers.append('duplicate_image_not_counted')
            continue
        seen.add(sha)
        if not https_reference(image.get('sourcePageUrl')) or not str(image.get('credit', '')).strip() or not str(image.get('caption', '')).strip():
            blockers.append('image_source_credit_or_relation_missing')
            continue
        valid.append(image)
        if image.get('rightsStatus') not in {'licensed', 'permission-confirmed', 'original'}:
            blockers.append('image_usage_needs_review')
    if len(valid) < rule['minimumImages']:
        blockers.append('not_enough_related_images')
    if not rule['dataset']:
        blockers.append('dataset_not_bound')
    if item.get('factsVerified') is not True:
        blockers.append('source_facts_not_verified')
    return {'category': category, 'dataset': rule['dataset'], 'imageCount': len(valid),
            'minimumImages': rule['minimumImages'], 'blockers': sorted(set(blockers)),
            'readyForEditorialReview': not blockers, 'broadcastApproved': False,
            'fullImageDecodeAndRuntimeAuthorizationRequiredBeforePlayback': True}
