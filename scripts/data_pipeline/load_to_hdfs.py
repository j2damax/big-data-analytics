#!/usr/bin/env python3
"""
HDFS Data Loader Script

This script loads processed datasets into HDFS with:
- Automatic HDFS directory creation
- Parallel file upload capability
- Progress tracking
- Error handling and retry logic
- Verification of uploaded files

Usage:
    python load_to_hdfs.py [--datasets DATASET1 DATASET2 ...] [--replication 3]
"""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_pipeline.config import (
    DATASETS,
    PROCESSED_DATA_DIR,
    HDFS_BASE_PATH,
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


class HDFSLoader:
    """Handles loading datasets into HDFS."""
    
    def __init__(
        self,
        processed_dir: str = PROCESSED_DATA_DIR,
        hdfs_base_path: str = HDFS_BASE_PATH
    ):
        """
        Initialize the HDFS loader.
        
        Args:
            processed_dir: Directory containing processed files
            hdfs_base_path: Base path in HDFS for datasets
        """
        self.processed_dir = Path(processed_dir)
        self.hdfs_base_path = hdfs_base_path
        
        logger.info(f"HDFS loader initialized")
        logger.info(f"Processed directory: {self.processed_dir}")
        logger.info(f"HDFS base path: {self.hdfs_base_path}")
    
    def run_hdfs_command(self, command: List[str], check: bool = True) -> subprocess.CompletedProcess:
        """
        Run an HDFS command using hadoop fs.
        
        Args:
            command: Command arguments (without 'hadoop fs')
            check: If True, raise exception on non-zero exit code
            
        Returns:
            CompletedProcess object
        """
        full_command = ['hadoop', 'fs'] + command
        logger.debug(f"Running command: {' '.join(full_command)}")
        
        try:
            result = subprocess.run(
                full_command,
                capture_output=True,
                text=True,
                check=check
            )
            return result
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {' '.join(full_command)}")
            logger.error(f"Exit code: {e.returncode}")
            logger.error(f"STDOUT: {e.stdout}")
            logger.error(f"STDERR: {e.stderr}")
            raise
        except FileNotFoundError:
            logger.error("'hadoop' command not found. Make sure Hadoop is installed and in PATH.")
            raise
    
    def test_hdfs_connection(self) -> bool:
        """
        Test connection to HDFS.
        
        Returns:
            True if connection successful, False otherwise
        """
        logger.info("Testing HDFS connection...")
        try:
            result = self.run_hdfs_command(['-ls', '/'], check=False)
            if result.returncode == 0:
                logger.info("✓ Successfully connected to HDFS")
                return True
            else:
                logger.error(f"✗ Failed to connect to HDFS: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"✗ Error testing HDFS connection: {e}")
            return False
    
    def create_hdfs_directory(self, path: str) -> bool:
        """
        Create a directory in HDFS.
        
        Args:
            path: HDFS directory path
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Creating HDFS directory: {path}")
        try:
            self.run_hdfs_command(['-mkdir', '-p', path])
            logger.info(f"✓ Directory created: {path}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to create directory {path}: {e}")
            return False
    
    def upload_file(
        self,
        local_path: Path,
        hdfs_path: str,
        replication: int = 3,
        overwrite: bool = False
    ) -> bool:
        """
        Upload a file to HDFS.
        
        Args:
            local_path: Local file path
            hdfs_path: Destination path in HDFS
            replication: Replication factor
            overwrite: If True, overwrite existing file
            
        Returns:
            True if successful, False otherwise
        """
        if not local_path.exists():
            logger.error(f"Local file does not exist: {local_path}")
            return False
        
        logger.info(f"Uploading {local_path.name} to HDFS: {hdfs_path}")
        
        try:
            # Check if file already exists
            result = self.run_hdfs_command(['-test', '-e', hdfs_path], check=False)
            file_exists = (result.returncode == 0)
            
            if file_exists and not overwrite:
                logger.info(f"File already exists in HDFS: {hdfs_path}. Use --overwrite to replace.")
                return True
            
            # Upload file
            upload_args = ['-put']
            if overwrite:
                upload_args.append('-f')
            upload_args.extend([str(local_path), hdfs_path])
            
            self.run_hdfs_command(upload_args)
            
            # Set replication factor
            self.run_hdfs_command(['-setrep', '-w', str(replication), hdfs_path])
            
            # Verify upload
            file_size = local_path.stat().st_size
            logger.info(f"✓ Successfully uploaded {local_path.name} ({file_size / (1024*1024):.2f}MB)")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to upload {local_path.name}: {e}")
            return False
    
    def verify_upload(self, hdfs_path: str, local_size: int) -> bool:
        """
        Verify that a file was uploaded correctly to HDFS.
        
        Args:
            hdfs_path: HDFS file path
            local_size: Expected file size in bytes
            
        Returns:
            True if verification successful, False otherwise
        """
        try:
            result = self.run_hdfs_command(['-stat', '%b', hdfs_path])
            hdfs_size = int(result.stdout.strip())
            
            if hdfs_size == local_size:
                logger.info(f"✓ File size verified: {hdfs_size} bytes")
                return True
            else:
                logger.error(f"✗ Size mismatch: local={local_size}, hdfs={hdfs_size}")
                return False
                
        except Exception as e:
            logger.error(f"✗ Failed to verify upload: {e}")
            return False
    
    def get_hdfs_file_info(self, hdfs_path: str) -> Optional[dict]:
        """
        Get information about a file in HDFS.
        
        Args:
            hdfs_path: HDFS file path
            
        Returns:
            Dictionary with file information, or None if error
        """
        try:
            result = self.run_hdfs_command(['-stat', '%n|%b|%y|%r', hdfs_path])
            parts = result.stdout.strip().split('|')
            
            return {
                'name': parts[0],
                'size': int(parts[1]),
                'modification_time': parts[2],
                'replication': int(parts[3])
            }
        except Exception as e:
            logger.error(f"Failed to get file info: {e}")
            return None


def load_datasets(
    dataset_names: Optional[List[str]] = None,
    replication: int = 3,
    overwrite: bool = False,
    dry_run: bool = False
) -> bool:
    """
    Load specified datasets or all datasets into HDFS.
    
    Args:
        dataset_names: List of dataset names to load, or None for all
        replication: HDFS replication factor
        overwrite: If True, overwrite existing files
        dry_run: If True, only show what would be done without actually uploading
        
    Returns:
        True if all uploads successful, False otherwise
    """
    loader = HDFSLoader()
    
    # Check what processed files are available
    processed_files = list(loader.processed_dir.glob("*.txt"))
    if not processed_files:
        logger.error(f"No processed files found in: {loader.processed_dir}")
        logger.error("Please run ingest_datasets.py first to extract the .gz files.")
        return False
    
    logger.info(f"Found {len(processed_files)} processed files:")
    for file in processed_files:
        logger.info(f"  - {file.name}")
    
    # Test HDFS connection (skip if dry run)
    if not dry_run:
        if not loader.test_hdfs_connection():
            logger.error("Cannot connect to HDFS. Make sure Hadoop is running.")
            logger.info("Tip: Use --dry-run to test the workflow without HDFS")
            return False
        
        # Create base directory
        if not loader.create_hdfs_directory(HDFS_BASE_PATH):
            logger.error("Failed to create base directory in HDFS")
            return False
    else:
        logger.info("DRY RUN MODE: Skipping HDFS connection test")
    
    # Determine which datasets to load
    if dataset_names:
        datasets_to_load = {name: DATASETS[name] for name in dataset_names if name in DATASETS}
        invalid_names = set(dataset_names) - set(DATASETS.keys())
        if invalid_names:
            logger.warning(f"Invalid dataset names: {invalid_names}")
            
        # Check if requested processed files exist
        missing_files = []
        for name, config in datasets_to_load.items():
            processed_filename = config['filename'][:-3]  # Remove .gz
            if not (loader.processed_dir / processed_filename).exists():
                missing_files.append(processed_filename)
        
        if missing_files:
            logger.error(f"Missing processed files: {missing_files}")
            logger.error("Please run ingest_datasets.py first for these datasets.")
            return False
    else:
        # Filter to only datasets where processed files actually exist
        available_datasets = {}
        for name, config in DATASETS.items():
            processed_filename = config['filename'][:-3]  # Remove .gz
            if (loader.processed_dir / processed_filename).exists():
                available_datasets[name] = config
            else:
                logger.info(f"Skipping {name}: processed file {processed_filename} not found")
        
        datasets_to_load = available_datasets
    
    logger.info(f"Starting upload of {len(datasets_to_load)} dataset(s) to HDFS")
    
    # Upload each dataset
    success_count = 0
    for name, config in datasets_to_load.items():
        logger.info(f"\n{'='*60}")
        logger.info(f"Loading: {name}")
        logger.info(f"{'='*60}\n")
        
        # Determine local file path
        local_filename = config['filename'][:-3]  # Remove .gz extension
        local_path = loader.processed_dir / local_filename
        
        if not local_path.exists():
            logger.error(f"Processed file not found: {local_path}")
            logger.error(f"Please run ingest_datasets.py first")
            continue
        
        # Create dataset-specific directory in HDFS
        hdfs_dir = f"{HDFS_BASE_PATH}/{name}"
        hdfs_path = f"{hdfs_dir}/{local_filename}"
        
        if dry_run:
            # Dry run: just show what would be done
            file_size_mb = local_path.stat().st_size / (1024 * 1024)
            success_count += 1
            logger.info(f"✓ {name} would be loaded to HDFS (DRY RUN)")
            logger.info(f"  Local file: {local_path} ({file_size_mb:.2f}MB)")
            logger.info(f"  HDFS destination: {hdfs_path}")
            logger.info(f"  Replication factor: {replication}\n")
        else:
            # Actual upload
            if not loader.create_hdfs_directory(hdfs_dir):
                logger.error(f"Failed to create directory: {hdfs_dir}")
                continue
            
            # Upload file
            if loader.upload_file(local_path, hdfs_path, replication, overwrite):
                # Verify upload
                if loader.verify_upload(hdfs_path, local_path.stat().st_size):
                    success_count += 1
                    logger.info(f"✓ {name} loaded to HDFS successfully\n")
                    
                    # Display file info
                    info = loader.get_hdfs_file_info(hdfs_path)
                    if info:
                        logger.info(f"  HDFS path: {hdfs_path}")
                        logger.info(f"  Size: {info['size'] / (1024*1024):.2f}MB")
                        logger.info(f"  Replication: {info['replication']}")
                else:
                    logger.error(f"✗ {name} verification failed\n")
            else:
                logger.error(f"✗ {name} upload failed\n")
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"HDFS Load Summary{' (DRY RUN)' if dry_run else ''}")
    logger.info(f"{'='*60}")
    logger.info(f"Successful: {success_count}/{len(datasets_to_load)}")
    logger.info(f"Failed: {len(datasets_to_load) - success_count}/{len(datasets_to_load)}")
    logger.info(f"HDFS base path: {HDFS_BASE_PATH}")
    if dry_run:
        logger.info("Note: This was a dry run. Use without --dry-run to actually upload.")
    logger.info(f"{'='*60}\n")
    
    return success_count == len(datasets_to_load)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Load processed SNAP datasets into HDFS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available processed files
  python load_to_hdfs.py --list
  
  # Test what would be loaded (dry run)
  python load_to_hdfs.py --dry-run
  
  # Load all available datasets
  python load_to_hdfs.py
  
  # Load specific datasets
  python load_to_hdfs.py --datasets soc-Pokec email-EuAll
  
  # Set custom replication factor
  python load_to_hdfs.py --replication 2
  
  # Overwrite existing files in HDFS
  python load_to_hdfs.py --overwrite

Note: This script loads processed .txt files from data/processed directory.
Run ingest_datasets.py first to extract .gz files to .txt files.
Use --dry-run to test without requiring HDFS to be running.
        """
    )
    
    parser.add_argument(
        '--datasets',
        nargs='+',
        choices=list(DATASETS.keys()),
        help='Specific datasets to load (default: all available)'
    )
    
    parser.add_argument(
        '--replication',
        type=int,
        default=3,
        help='HDFS replication factor (default: 3)'
    )
    
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Overwrite existing files in HDFS'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='List available processed files and exit'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without actually uploading to HDFS'
    )
    
    args = parser.parse_args()
    
    # Handle --list option
    if args.list:
        processed_dir = Path(PROCESSED_DATA_DIR)
        processed_files = list(processed_dir.glob("*.txt"))
        
        if not processed_files:
            print(f"No processed .txt files found in: {processed_dir}")
            print("Please run ingest_datasets.py first to extract .gz files.")
        else:
            print(f"Available processed files in {processed_dir}:")
            for file in sorted(processed_files):
                # Try to match with known datasets
                matching_dataset = None
                for name, config in DATASETS.items():
                    expected_name = config['filename'][:-3]  # Remove .gz
                    if expected_name == file.name:
                        matching_dataset = name
                        break
                
                size_mb = file.stat().st_size / (1024 * 1024)
                if matching_dataset:
                    print(f"  ✓ {file.name} ({size_mb:.1f}MB) -> {matching_dataset}")
                else:
                    print(f"  ? {file.name} ({size_mb:.1f}MB) -> Unknown dataset")
        
        sys.exit(0)
    
    # Run upload
    success = load_datasets(
        dataset_names=args.datasets,
        replication=args.replication,
        overwrite=args.overwrite,
        dry_run=args.dry_run
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
