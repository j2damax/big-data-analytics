#!/usr/bin/env python3
"""
Complete Data Pipeline Runner

This script orchestrates the complete data pipeline:
1. Download datasets from SNAP repository
2. Ingest and validate datasets
3. Load datasets into HDFS

Usage:
    python run_pipeline.py [--datasets DATASET1 DATASET2 ...] [--skip-download] [--skip-ingest] [--skip-hdfs]
"""

import argparse
import logging
import os
import sys
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_pipeline.config import DATASETS, LOG_FORMAT, LOG_LEVEL
from data_pipeline.download_datasets import download_datasets
from data_pipeline.ingest_datasets import ingest_datasets
from data_pipeline.load_to_hdfs import load_datasets

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler('data_pipeline.log', mode='a'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def run_complete_pipeline(
    dataset_names=None,
    skip_download=False,
    skip_ingest=False,
    skip_hdfs=False,
    force_download=False,
    replication=3
):
    """
    Run the complete data pipeline.
    
    Args:
        dataset_names: List of dataset names to process, or None for all
        skip_download: Skip download step
        skip_ingest: Skip ingestion step
        skip_hdfs: Skip HDFS loading step
        force_download: Force re-download of datasets
        replication: HDFS replication factor
        
    Returns:
        True if all steps successful, False otherwise
    """
    start_time = time.time()
    
    logger.info("="*70)
    logger.info("SNAP DATASET PIPELINE - STARTING")
    logger.info("="*70)
    logger.info(f"Datasets: {dataset_names if dataset_names else 'ALL'}")
    logger.info(f"Steps: Download={not skip_download}, Ingest={not skip_ingest}, HDFS={not skip_hdfs}")
    logger.info("="*70 + "\n")
    
    # Step 1: Download
    if not skip_download:
        logger.info("\n" + "="*70)
        logger.info("STEP 1: DOWNLOADING DATASETS")
        logger.info("="*70 + "\n")
        
        if not download_datasets(dataset_names=dataset_names, force=force_download):
            logger.error("Download step failed. Aborting pipeline.")
            return False
        
        logger.info("✓ Download step completed successfully\n")
    else:
        logger.info("Skipping download step\n")
    
    # Step 2: Ingest
    if not skip_ingest:
        logger.info("\n" + "="*70)
        logger.info("STEP 2: INGESTING AND VALIDATING DATASETS")
        logger.info("="*70 + "\n")
        
        if not ingest_datasets(dataset_names=dataset_names, skip_validation=False):
            logger.error("Ingestion step failed. Aborting pipeline.")
            return False
        
        logger.info("✓ Ingestion step completed successfully\n")
    else:
        logger.info("Skipping ingestion step\n")
    
    # Step 3: Load to HDFS
    if not skip_hdfs:
        logger.info("\n" + "="*70)
        logger.info("STEP 3: LOADING DATASETS TO HDFS")
        logger.info("="*70 + "\n")
        
        if not load_datasets(dataset_names=dataset_names, replication=replication):
            logger.error("HDFS loading step failed. Aborting pipeline.")
            return False
        
        logger.info("✓ HDFS loading step completed successfully\n")
    else:
        logger.info("Skipping HDFS loading step\n")
    
    # Pipeline complete
    elapsed_time = time.time() - start_time
    
    logger.info("\n" + "="*70)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("="*70)
    logger.info(f"Total execution time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    logger.info("="*70 + "\n")
    
    return True


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Run the complete SNAP dataset pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Pipeline Steps:
  1. Download: Fetch datasets from SNAP repository
  2. Ingest: Extract and validate datasets
  3. HDFS: Upload datasets to Hadoop Distributed File System

Examples:
  # Run complete pipeline for all datasets
  python run_pipeline.py
  
  # Run for specific datasets only
  python run_pipeline.py --datasets soc-Pokec email-EuAll
  
  # Skip download if files already exist
  python run_pipeline.py --skip-download
  
  # Download and ingest only (no HDFS)
  python run_pipeline.py --skip-hdfs
  
  # Force re-download of datasets
  python run_pipeline.py --force-download
        """
    )
    
    parser.add_argument(
        '--datasets',
        nargs='+',
        choices=list(DATASETS.keys()),
        help='Specific datasets to process (default: all)'
    )
    
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip download step (use existing files)'
    )
    
    parser.add_argument(
        '--skip-ingest',
        action='store_true',
        help='Skip ingestion step (use existing processed files)'
    )
    
    parser.add_argument(
        '--skip-hdfs',
        action='store_true',
        help='Skip HDFS loading step'
    )
    
    parser.add_argument(
        '--force-download',
        action='store_true',
        help='Force re-download even if files exist'
    )
    
    parser.add_argument(
        '--replication',
        type=int,
        default=3,
        help='HDFS replication factor (default: 3)'
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    success = run_complete_pipeline(
        dataset_names=args.datasets,
        skip_download=args.skip_download,
        skip_ingest=args.skip_ingest,
        skip_hdfs=args.skip_hdfs,
        force_download=args.force_download,
        replication=args.replication
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
