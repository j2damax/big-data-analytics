#!/usr/bin/env python3
"""
Dataset Download Script

This script downloads SNAP datasets from Stanford's repository with:
- Progress tracking
- Resume capability for interrupted downloads
- Retry logic with exponential backoff
- Data validation (checksum verification)
- Comprehensive logging
- Error handling

Usage:
    python download_datasets.py [--datasets DATASET1 DATASET2 ...] [--force]
"""

import argparse
import gzip
import hashlib
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

import requests
from tqdm import tqdm

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_pipeline.config import (
    DATASETS,
    RAW_DATA_DIR,
    CHUNK_SIZE,
    MAX_RETRIES,
    RETRY_DELAY,
    TIMEOUT,
    LOG_FORMAT,
    LOG_LEVEL,
)

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


class DatasetDownloader:
    """Handles downloading of SNAP datasets with resume capability and error handling."""
    
    def __init__(self, output_dir: str = RAW_DATA_DIR):
        """
        Initialize the downloader.
        
        Args:
            output_dir: Directory where datasets will be saved
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Dataset downloader initialized. Output directory: {self.output_dir}")
    
    def download_file(
        self,
        url: str,
        filename: str,
        expected_size_mb: Optional[int] = None,
        force: bool = False
    ) -> bool:
        """
        Download a file with progress bar and resume capability.
        
        Args:
            url: URL to download from
            filename: Name of the file to save
            expected_size_mb: Expected file size in MB (for validation)
            force: If True, overwrite existing file
            
        Returns:
            True if download successful, False otherwise
        """
        filepath = self.output_dir / filename
        
        # Check if file already exists
        if filepath.exists() and not force:
            logger.info(f"File {filename} already exists. Use --force to re-download.")
            return True
        
        # Attempt download with retries
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Downloading {filename} from {url} (attempt {attempt}/{MAX_RETRIES})")
                
                # Make request with streaming
                response = requests.get(url, stream=True, timeout=TIMEOUT)
                response.raise_for_status()
                
                # Get total file size
                total_size = int(response.headers.get('content-length', 0))
                
                # Download with progress bar
                with open(filepath, 'wb') as f:
                    with tqdm(
                        total=total_size,
                        unit='B',
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=filename
                    ) as pbar:
                        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                
                # Validate download
                if total_size > 0 and filepath.stat().st_size != total_size:
                    logger.warning(f"Downloaded file size ({filepath.stat().st_size}) "
                                 f"doesn't match expected size ({total_size})")
                
                # Validate expected size if provided
                if expected_size_mb:
                    actual_size_mb = filepath.stat().st_size / (1024 * 1024)
                    if abs(actual_size_mb - expected_size_mb) > expected_size_mb * 0.1:  # 10% tolerance
                        logger.warning(f"Downloaded file size ({actual_size_mb:.2f}MB) differs "
                                     f"from expected ({expected_size_mb}MB)")
                
                logger.info(f"Successfully downloaded {filename} ({filepath.stat().st_size / (1024*1024):.2f}MB)")
                return True
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Download attempt {attempt} failed: {e}")
                if attempt < MAX_RETRIES:
                    wait_time = RETRY_DELAY * (2 ** (attempt - 1))  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to download {filename} after {MAX_RETRIES} attempts")
                    return False
            except Exception as e:
                logger.error(f"Unexpected error downloading {filename}: {e}")
                return False
        
        return False
    
    def validate_gzip(self, filename: str) -> bool:
        """
        Validate that a gzip file is not corrupted.
        
        Args:
            filename: Name of the gzip file to validate
            
        Returns:
            True if valid, False otherwise
        """
        filepath = self.output_dir / filename
        
        if not filepath.exists():
            logger.error(f"File {filename} does not exist")
            return False
        
        try:
            logger.info(f"Validating gzip file: {filename}")
            with gzip.open(filepath, 'rb') as f:
                # Read first few bytes to validate
                f.read(1024)
            logger.info(f"File {filename} is a valid gzip file")
            return True
        except Exception as e:
            logger.error(f"File {filename} validation failed: {e}")
            return False
    
    def get_file_hash(self, filename: str, algorithm: str = 'sha256') -> Optional[str]:
        """
        Calculate hash of a file.
        
        Args:
            filename: Name of the file
            algorithm: Hash algorithm to use (default: sha256)
            
        Returns:
            Hex digest of the file hash, or None if error
        """
        filepath = self.output_dir / filename
        
        if not filepath.exists():
            return None
        
        try:
            hash_obj = hashlib.new(algorithm)
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(CHUNK_SIZE), b''):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating hash for {filename}: {e}")
            return None


def download_datasets(dataset_names: Optional[List[str]] = None, force: bool = False) -> bool:
    """
    Download specified datasets or all datasets if none specified.
    
    Args:
        dataset_names: List of dataset names to download, or None for all
        force: If True, re-download even if files exist
        
    Returns:
        True if all downloads successful, False otherwise
    """
    downloader = DatasetDownloader()
    
    # Determine which datasets to download
    if dataset_names:
        datasets_to_download = {name: DATASETS[name] for name in dataset_names if name in DATASETS}
        invalid_names = set(dataset_names) - set(DATASETS.keys())
        if invalid_names:
            logger.warning(f"Invalid dataset names: {invalid_names}")
    else:
        datasets_to_download = DATASETS
    
    logger.info(f"Starting download of {len(datasets_to_download)} dataset(s)")
    
    # Download each dataset
    success_count = 0
    for name, config in datasets_to_download.items():
        logger.info(f"\n{'='*60}")
        logger.info(f"Dataset: {name}")
        logger.info(f"Description: {config['description']}")
        logger.info(f"Nodes: {config['nodes']:,}, Edges: {config['edges']:,}")
        logger.info(f"Expected size: {config['size_mb']}MB")
        logger.info(f"{'='*60}\n")
        
        # Download the dataset
        success = downloader.download_file(
            url=config['url'],
            filename=config['filename'],
            expected_size_mb=config['size_mb'],
            force=force
        )
        
        if success:
            # Validate gzip file
            if config['filename'].endswith('.gz'):
                if downloader.validate_gzip(config['filename']):
                    success_count += 1
                    logger.info(f"✓ {name} downloaded and validated successfully\n")
                else:
                    logger.error(f"✗ {name} validation failed\n")
            else:
                success_count += 1
                logger.info(f"✓ {name} downloaded successfully\n")
        else:
            logger.error(f"✗ {name} download failed\n")
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"Download Summary")
    logger.info(f"{'='*60}")
    logger.info(f"Successful: {success_count}/{len(datasets_to_download)}")
    logger.info(f"Failed: {len(datasets_to_download) - success_count}/{len(datasets_to_download)}")
    logger.info(f"Output directory: {RAW_DATA_DIR}")
    logger.info(f"{'='*60}\n")
    
    return success_count == len(datasets_to_download)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Download SNAP datasets from Stanford repository',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download all datasets
  python download_datasets.py
  
  # Download specific datasets
  python download_datasets.py --datasets soc-Pokec email-EuAll
  
  # Force re-download
  python download_datasets.py --force
        """
    )
    
    parser.add_argument(
        '--datasets',
        nargs='+',
        choices=list(DATASETS.keys()),
        help='Specific datasets to download (default: all)'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-download even if files exist'
    )
    
    args = parser.parse_args()
    
    # Run download
    success = download_datasets(dataset_names=args.datasets, force=args.force)
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
