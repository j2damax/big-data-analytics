"""
Configuration file for SNAP dataset acquisition and ingestion.

This module defines the datasets to be downloaded, their URLs, and metadata
for the Stanford SNAP repository datasets.
"""

import os

# Base directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')

# HDFS configuration
HDFS_HOST = 'hadoop'
HDFS_PORT = 9000
HDFS_BASE_PATH = '/user/root/snap_datasets'

# Dataset configurations
DATASETS = {
    'soc-Pokec': {
        'name': 'soc-Pokec',
        'description': 'Pokec social network (Slovakia, friendships)',
        'url': 'https://snap.stanford.edu/data/soc-pokec-relationships.txt.gz',
        'filename': 'soc-pokec-relationships.txt.gz',
        'type': 'social_network',
        'nodes': 1632803,
        'edges': 30622564,
        'size_mb': 215,
        'directed': True,
    },
    'email-EuAll': {
        'name': 'email-EuAll',
        'description': 'Email communication network from a EU research institution',
        'url': 'https://snap.stanford.edu/data/email-EuAll.txt.gz',
        'filename': 'email-EuAll.txt.gz',
        'type': 'communication',
        'nodes': 265214,
        'edges': 420045,
        'size_mb': 4,
        'directed': True,
    },
    'cit-Patents': {
        'name': 'cit-Patents',
        'description': 'US patent citation network',
        'url': 'https://snap.stanford.edu/data/cit-Patents.txt.gz',
        'filename': 'cit-Patents.txt.gz',
        'type': 'citation',
        'nodes': 3774768,
        'edges': 16518948,
        'size_mb': 161,
        'directed': True,
    },
    'soc-LiveJournal1': {
        'name': 'soc-LiveJournal1',
        'description': 'LiveJournal social network (for scalability testing)',
        'url': 'https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz',
        'filename': 'soc-LiveJournal1.txt.gz',
        'type': 'social_network',
        'nodes': 4847571,
        'edges': 68993773,
        'size_mb': 467,
        'directed': True,
    },
}

# Download configuration
CHUNK_SIZE = 8192  # 8KB chunks for downloading
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
TIMEOUT = 300  # seconds

# Logging configuration
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = 'INFO'
