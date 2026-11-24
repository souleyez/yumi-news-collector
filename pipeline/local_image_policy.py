"""Future programme stills are supplied by the local built-in tool only.

No generator, endpoint, network call or fallback exists in the remote worker.
Historical material remains unchanged; this policy is stamped at first delivery.
"""
PROVIDER = 'local-codex-builtin-imagegen'

def summary_images(manifest):
    return [i for i in manifest.get('images', []) if i.get('synthetic') is True and
            (i.get('generationProvider') == PROVIDER or
             (i.get('editorialSupplement') is True and 'Codex imagegen' in i.get('credit', '')
              and i.get('asset', {}).get('sha256') == i.get('sha256')
              and i.get('asset', {}).get('assetId')))]

def programme_images(manifest):
    # Actual publisher images stay eligible alongside verified local GPT summaries.
    return [i for i in manifest.get('images', []) if not i.get('synthetic') or i in summary_images(manifest)]

def graphics_ready(manifest):
    return 3 <= len(programme_images(manifest)) <= 5 and bool(summary_images(manifest))

def programme_view(manifest):
    """Migrate the view, never the original paragraphs, MD or archived images."""
    result = dict(manifest)
    selected = programme_images(manifest)
    result['sourceImages'] = manifest.get('sourceImages', [])
    result['images'] = selected
    result['imageGenerationPolicy'] = {'provider': PROVIDER, 'fallback': None,
                                      'offlineAction': 'wait-for-local-imagegen',
                                      'scope':'generated-summary-only', 'originalNewsImages':'retain-and-use'}
    result.update(imageCount=len(selected), missingImageCount=max(0,3-len(selected)),
                  mediaReadiness='complete' if graphics_ready(manifest) else 'pending-images',
                  summaryImageCount=len(summary_images(manifest)),
                  imageGenerationStatus='complete' if summary_images(manifest) else 'waiting-local-imagegen')
    if 'imageSequence' in manifest and selected != manifest.get('images', []):
        if manifest.get('imageSequenceBasis', '').startswith('article-image-order;'):
            result.pop('imageSequence')  # Existing automatic order is re-derived below.
        else:
            # Preserve manual order; original-only cues wait for local pictures.
            old = manifest['images']
            result['imageSequence'] = [{**cue, 'imageIndices':[
                selected.index(old[i]) for i in cue.get('imageIndices', [])
                if 0<=i<len(old) and old[i] in selected]} for cue in manifest['imageSequence']]
            if any(not cue['imageIndices'] for cue in result['imageSequence']):
                result.update(mediaReadiness='pending-images',imageGenerationStatus='waiting-local-imagegen')
    return result

def initialize(manifest):
    manifest['imageGenerationPolicy'] = {
        'provider': PROVIDER, 'fallback': None,
        'offlineAction': 'wait-for-local-imagegen',
    }
    manifest.update(mediaReadiness='pending-images',
                    missingImageCount=max(0,3-len(manifest['images'])), imageGenerationStatus='waiting-local-imagegen')

def validate_completed(manifest, images):
    existing = {i.get('sha256') for i in summary_images(manifest)}
    for image in images:
        if image.get('synthetic') is not True or (image.get('generationProvider') != PROVIDER
                                                   and image.get('sha256') not in existing):
            raise ValueError('LOCAL_BUILTIN_IMAGEGEN_REQUIRED_NO_FALLBACK')
