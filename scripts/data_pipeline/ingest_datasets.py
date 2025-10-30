#!/usr/bin/env python3
"""
Dataset Ingestion Script

This script processes and validates downloaded SNAP datasets:
- Extracts compressed files
- Validates data format and content
- Generates dataset statistics
- Prepares data for loading into HDFS

Usage:
    python ingest_datasets.py [--datasets DATASET1 DATASET2 ...] [--skip-validation]
"""

import argparse
import gzip
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_pipeline.config import (
    DATASETS,
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
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


class DatasetIngestor:
    """Handles ingestion and validation of SNAP datasets."""
    
    def __init__(self, raw_dir: str = RAW_DATA_DIR, processed_dir: str = PROCESSED_DATA_DIR):
        """
        Initialize the ingestor.
        
        Args:
            raw_dir: Directory containing raw downloaded files
            processed_dir: Directory for processed files
        """
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Dataset ingestor initialized.")
        logger.info(f"Raw directory: {self.raw_dir}")
        logger.info(f"Processed directory: {self.processed_dir}")
    
    def extract_gzip(self, gzip_filename: str, output_filename: Optional[str] = None) -> bool:
        """
        Extract a gzip file.
        
        Args:
            gzip_filename: Name of the gzip file in raw directory
            output_filename: Optional name for extracted file (default: remove .gz extension)
            
        Returns:
            True if extraction successful, False otherwise
        """
        input_path = self.raw_dir / gzip_filename
        
        if not input_path.exists():
            logger.error(f"Input file {gzip_filename} does not exist")
            return False
        
        # Determine output filename
        if output_filename is None:
            output_filename = gzip_filename[:-3] if gzip_filename.endswith('.gz') else gzip_filename
        
        output_path = self.processed_dir / output_filename
        
        # Check if already extracted
        if output_path.exists():
            logger.info(f"File {output_filename} already extracted. Skipping.")
            return True
        
        try:
            logger.info(f"Extracting {gzip_filename} to {output_filename}")
            
            with gzip.open(input_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    # Extract in chunks to avoid memory issues with large files
                    chunk_size = 8192
                    while True:
                        chunk = f_in.read(chunk_size)
                        if not chunk:
                            break
                        f_out.write(chunk)
            
            extracted_size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"Successfully extracted {output_filename} ({extracted_size_mb:.2f}MB)")
            return True
            
        except Exception as e:
            logger.error(f"Error extracting {gzip_filename}: {e}")
            if output_path.exists():
                output_path.unlink()  # Clean up partial file
            return False
    
    def analyze_graph_file(self, filename: str, sample_lines: int = 1000000) -> Dict:
        """
        Analyze a graph file and generate statistics.
        
        Args:
            filename: Name of the file in processed directory
            sample_lines: Number of lines to sample for quick analysis
            
        Returns:
            Dictionary containing statistics
        """
        filepath = self.processed_dir / filename
        
        if not filepath.exists():
            logger.error(f"File {filename} does not exist")
            return {}
        
        logger.info(f"Analyzing {filename}...")
        
        stats = {
            'filename': filename,
            'total_lines': 0,
            'comment_lines': 0,
            'edge_lines': 0,
            'nodes': set(),
            'edges': [],
            'min_node': float('inf'),
            'max_node': float('-inf'),
        }
        
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    stats['total_lines'] += 1
                    line = line.strip()
                    
                    # Skip empty lines
                    if not line:
                        continue
                    
                    # Skip comment lines
                    if line.startswith('#'):
                        stats['comment_lines'] += 1
                        continue
                    
                    # Parse edge
                    try:
                        parts = line.split()
                        if len(parts) >= 2:
                            src = int(parts[0])
                            dst = int(parts[1])
                            
                            stats['edge_lines'] += 1
                            stats['nodes'].add(src)
                            stats['nodes'].add(dst)
                            stats['min_node'] = min(stats['min_node'], src, dst)
                            stats['max_node'] = max(stats['max_node'], src, dst)
                            
                            # Store sample edges
                            if len(stats['edges']) < 10:
                                stats['edges'].append((src, dst))
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error parsing line {line_num}: {line[:50]}... - {e}")
                    
                    # Progress update
                    if line_num % 1000000 == 0:
                        logger.info(f"  Processed {line_num:,} lines...")
                    
                    # Limit for very large files
                    if sample_lines and line_num > sample_lines:
                        logger.info(f"  Reached sample limit of {sample_lines:,} lines")
                        break
            
            # Calculate final statistics
            stats['num_nodes'] = len(stats['nodes'])
            stats['num_edges'] = stats['edge_lines']
            del stats['nodes']  # Remove large set to save memory
            
            logger.info(f"Analysis complete for {filename}")
            logger.info(f"  Total lines: {stats['total_lines']:,}")
            logger.info(f"  Comment lines: {stats['comment_lines']:,}")
            logger.info(f"  Edge lines: {stats['edge_lines']:,}")
            logger.info(f"  Unique nodes: {stats['num_nodes']:,}")
            logger.info(f"  Node ID range: [{stats['min_node']}, {stats['max_node']}]")
            logger.info(f"  Sample edges: {stats['edges'][:5]}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error analyzing {filename}: {e}")
            return {}
    
    def validate_dataset(self, dataset_name: str, expected_config: Dict) -> bool:
        """
        Validate that a processed dataset matches expected properties.
        
        Args:
            dataset_name: Name of the dataset
            expected_config: Expected configuration from DATASETS
            
        Returns:
            True if validation passes, False otherwise
        """
        filename = expected_config['filename'][:-3]  # Remove .gz
        filepath = self.processed_dir / filename
        
        if not filepath.exists():
            logger.error(f"Processed file {filename} does not exist")
            return False
        
        logger.info(f"Validating {dataset_name}...")
        
        # Analyze the file
        stats = self.analyze_graph_file(filename, sample_lines=None)  # Full analysis
        
        if not stats:
            logger.error(f"Failed to analyze {filename}")
            return False
        
        # Validate against expected values
        validation_passed = True
        
        # Check edge count (allow 5% tolerance)
        expected_edges = expected_config['edges']
        actual_edges = stats['num_edges']
        edge_diff_pct = abs(actual_edges - expected_edges) / expected_edges * 100
        
        if edge_diff_pct > 5:
            logger.warning(f"Edge count mismatch: expected {expected_edges:,}, got {actual_edges:,} "
                         f"({edge_diff_pct:.2f}% difference)")
            validation_passed = False
        else:
            logger.info(f"✓ Edge count validated: {actual_edges:,}")
        
        # Check if file is non-empty
        if stats['num_edges'] == 0:
            logger.error(f"Dataset has no edges!")
            validation_passed = False
        else:
            logger.info(f"✓ Dataset has {stats['num_edges']:,} edges")
        
        # Check if nodes exist
        if stats['num_nodes'] == 0:
            logger.error(f"Dataset has no nodes!")
            validation_passed = False
        else:
            logger.info(f"✓ Dataset has {stats['num_nodes']:,} unique nodes")
        
        return validation_passed


def ingest_datasets(dataset_names: Optional[List[str]] = None, skip_validation: bool = False) -> bool:
    """
    Ingest specified datasets or all datasets if none specified.
    
    Args:
        dataset_names: List of dataset names to ingest, or None for all
        skip_validation: If True, skip validation step
        
    Returns:
        True if all ingestions successful, False otherwise
    """
    ingestor = DatasetIngestor()
    
    # Determine which datasets to ingest
    if dataset_names:
        datasets_to_ingest = {name: DATASETS[name] for name in dataset_names if name in DATASETS}
        invalid_names = set(dataset_names) - set(DATASETS.keys())
        if invalid_names:
            logger.warning(f"Invalid dataset names: {invalid_names}")
    else:
        datasets_to_ingest = DATASETS
    
    logger.info(f"Starting ingestion of {len(datasets_to_ingest)} dataset(s)")
    
    # Process each dataset
    success_count = 0
    for name, config in datasets_to_ingest.items():
        logger.info(f"\n{'='*60}")
        logger.info(f"Ingesting: {name}")
        logger.info(f"{'='*60}\n")
        
        # Extract compressed file
        if config['filename'].endswith('.gz'):
            if not ingestor.extract_gzip(config['filename']):
                logger.error(f"✗ Failed to extract {name}\n")
                continue
        
        # Validate dataset
        if not skip_validation:
            if ingestor.validate_dataset(name, config):
                success_count += 1
                logger.info(f"✓ {name} ingested and validated successfully\n")
            else:
                logger.error(f"✗ {name} validation failed\n")
        else:
            success_count += 1
            logger.info(f"✓ {name} ingested successfully (validation skipped)\n")
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"Ingestion Summary")
    logger.info(f"{'='*60}")
    logger.info(f"Successful: {success_count}/{len(datasets_to_ingest)}")
    logger.info(f"Failed: {len(datasets_to_ingest) - success_count}/{len(datasets_to_ingest)}")
    logger.info(f"Processed directory: {PROCESSED_DATA_DIR}")
    logger.info(f"{'='*60}\n")
    
    return success_count == len(datasets_to_ingest)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Ingest and validate SNAP datasets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest all datasets
  python ingest_datasets.py
  
  # Ingest specific datasets
  python ingest_datasets.py --datasets soc-Pokec email-EuAll
  
  # Skip validation
  python ingest_datasets.py --skip-validation
        """
    )
    
    parser.add_argument(
        '--datasets',
        nargs='+',
        choices=list(DATASETS.keys()),
        help='Specific datasets to ingest (default: all)'
    )
    
    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Skip validation step'
    )
    
    args = parser.parse_args()
    
    # Run ingestion
    success = ingest_datasets(dataset_names=args.datasets, skip_validation=args.skip_validation)
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
