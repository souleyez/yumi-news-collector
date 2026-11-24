"""Deployment settings shared by the collector, worker and read API."""

import os
import socket
from urllib.parse import urlsplit


def https_origin(name):
    value = os.environ.get(name, '').rstrip('/')
    if not value:
        return ''
    parsed = urlsplit(value)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.path or parsed.query or parsed.fragment
            or any(character.isspace() for character in value)):
        raise ValueError(f'{name}_INVALID_HTTPS_ORIGIN')
    return value


DATAHUB_ORIGIN = https_origin('YUMI_DATAHUB_ORIGIN')
DATAHUB_API = (DATAHUB_ORIGIN or 'https://datahub.invalid') + '/api/datafoundation/agent/v1'
DATASETS = {
    'ai-news': os.environ.get('YUMI_AI_DATASET') or 'dataset-00000000-0000-0000-0000-000000000001',
}
if entertainment_dataset := os.environ.get('YUMI_ENTERTAINMENT_DATASET'):
    DATASETS['entertainment'] = entertainment_dataset
EXPECTED_HOST = os.environ.get('YUMI_EXPECTED_HOST') or socket.gethostname()
LIVE_ORIGIN = https_origin('YUMI_LIVE_ORIGIN')
WORKBUDDY_REFERENCE_FILE = os.environ.get('YUMI_WORKBUDDY_REFERENCE_FILE', '')


def require_datahub_configuration():
    if not DATAHUB_ORIGIN or not os.environ.get('YUMI_AI_DATASET'):
        raise ValueError('YUMI_DATAHUB_ORIGIN_AND_AI_DATASET_REQUIRED')
