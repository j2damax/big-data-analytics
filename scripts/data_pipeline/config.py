"""
Configuration file for SNAP dataset processing and ingestion.

This module defines the datasets and their metadata for validation and HDFS storage.
"""

import os

# Base directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')

# HDFS configuration
HDFS_BASE_PATH = '/user/root/snap_datasets'

# Dataset configurations
DATASETS = {
    'soc-Pokec': {
        'filename': 'soc-pokec-relationships.txt.gz',
        'nodes': 1632803,
        'edges': 30622564,
    },
    'email-EuAll': {
        'filename': 'email-EuAll.txt.gz',
        'nodes': 265214,
        'edges': 420045,
    },
    'cit-Patents': {
        'filename': 'cit-Patents.txt.gz',
        'nodes': 3774768,
        'edges': 16518948,
    },
    'soc-LiveJournal1': {
        'filename': 'soc-LiveJournal1.txt.gz',
        'nodes': 4847571,
        'edges': 68993773,
    },
}

# Logging configuration
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = 'INFO'
