# Manual Dataset Processing Workflow

This guide explains how to manually process SNAP datasets using a simple 2-step approach.

## Overview

The manual workflow consists of two simple steps:
1. **Manual Download**: Download dataset .gz files to `data/raw/`
2. **Process & Upload**: Extract, validate, and upload to HDFS using two focused scripts

## Directory Structure

```
big-data-analytics/
├── data/
│   ├── raw/          # Place manually downloaded .gz files here
│   └── processed/    # Extracted .txt files (created automatically)
└── scripts/data_pipeline/
    ├── ingest_datasets.py    # Extract & validate datasets
    ├── load_to_hdfs.py      # Upload to HDFS
    └── config.py            # Dataset configurations
```

## Simple Two-Step Process

The entire workflow is just two commands:

```bash
cd scripts/data_pipeline

# Step 1: Extract and validate datasets
python ingest_datasets.py

# Step 2: Upload to HDFS
python load_to_hdfs.py
```

## Step-by-Step Workflow

### Step 1: Manual Download

Download SNAP dataset files manually and place them in `data/raw/`:

```bash
# Create directory if it doesn't exist
mkdir -p data/raw

# Example: Download files manually using curl/wget
curl -o data/raw/soc-pokec-relationships.txt.gz \
     https://snap.stanford.edu/data/soc-pokec-relationships.txt.gz

curl -o data/raw/email-EuAll.txt.gz \
     https://snap.stanford.edu/data/email-EuAll.txt.gz
```

**Available Datasets** (from config.py):
- `soc-Pokec`: Social network (soc-pokec-relationships.txt.gz)
- `email-EuAll`: Email network (email-EuAll.txt.gz)
- `cit-Patents`: Citation network (cit-Patents.txt.gz)
- `soc-LiveJournal1`: Social network (soc-LiveJournal1.txt.gz)

### Step 2: Process & Upload

```bash
cd scripts/data_pipeline

# Step 1: Extract and validate all datasets
python ingest_datasets.py

# Step 2: Upload all processed datasets to HDFS  
python load_to_hdfs.py
```

**Advanced Options:**

For `ingest_datasets.py`:
```bash
python ingest_datasets.py --list                    # List available .gz files
python ingest_datasets.py --datasets soc-Pokec     # Process specific dataset
python ingest_datasets.py --skip-validation        # Skip validation for speed
```

For `load_to_hdfs.py`:
```bash
python load_to_hdfs.py --list                      # List processed files
python load_to_hdfs.py --datasets soc-Pokec       # Upload specific dataset
python load_to_hdfs.py --dry-run                   # Test without HDFS
python load_to_hdfs.py --overwrite                 # Overwrite existing files
```

## Complete Workflow Examples

### Process All Datasets (Simplest)

```bash
# After manually downloading .gz files to data/raw/
cd scripts/data_pipeline

python ingest_datasets.py    # Extract & validate all
python load_to_hdfs.py      # Upload all to HDFS
```

### Process Specific Dataset

```bash
# 1. Manual download (example)
curl -o data/raw/soc-pokec-relationships.txt.gz \
     https://snap.stanford.edu/data/soc-pokec-relationships.txt.gz

# 2. Process and upload
cd scripts/data_pipeline
python ingest_datasets.py --datasets soc-Pokec
python load_to_hdfs.py --datasets soc-Pokec
```

### Test Without HDFS

```bash
# Useful when Hadoop isn't running
python ingest_datasets.py
python load_to_hdfs.py --dry-run
```

## Troubleshooting

### No files found in raw directory
- Ensure .gz files are placed in `data/raw/` directory
- Use `--list` option to check what files are detected

### Missing processed files
- Run `ingest_datasets.py` first to extract .gz files
- Check `data/processed/` directory for .txt files

### HDFS connection errors
- Ensure Hadoop containers are running: `make up` or `docker-compose up -d`
- Check HDFS web UI at http://localhost:9870
- Use `--dry-run` flag to test workflow without requiring HDFS
- Run HDFS commands from within the container: `docker exec -it hadoop python3 /scripts/data_pipeline/load_to_hdfs.py`

### Dataset validation failures
- Use `--skip-validation` if you want to proceed anyway
- Check the logs for specific validation errors
- Verify file integrity of downloaded .gz files

## Configuration

Dataset configurations are defined in `config.py`:
- URLs and filenames
- Expected statistics for validation
- HDFS connection settings

To add new datasets, update the `DATASETS` dictionary in `config.py`.

## Logging

Both scripts generate detailed logs:
- Console output for progress tracking
- `data_pipeline.log` file for detailed logging
- Processing statistics and validation results