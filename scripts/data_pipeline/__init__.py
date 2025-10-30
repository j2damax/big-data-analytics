"""
Data Pipeline Package

This package provides tools for acquiring, ingesting, and loading SNAP datasets
into HDFS for big data analytics processing.
"""

__version__ = '1.0.0'
__author__ = 'Big Data Analytics Team'

from . import config
from . import download_datasets
from . import ingest_datasets
from . import load_to_hdfs

__all__ = ['config', 'download_datasets', 'ingest_datasets', 'load_to_hdfs']
