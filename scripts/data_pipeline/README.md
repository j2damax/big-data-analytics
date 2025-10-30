# Data Pipeline - SNAP Dataset Acquisition and Loading

This module provides a complete, production-ready pipeline for acquiring, processing, and loading graph datasets from the Stanford Network Analysis Project (SNAP) into HDFS for big data analytics.

## 📋 Overview

The data pipeline consists of three main stages:

1. **Download**: Fetch datasets from SNAP repository with resume capability and validation
2. **Ingest**: Extract, validate, and analyze dataset properties
3. **HDFS Load**: Upload processed datasets to Hadoop Distributed File System

## 🎯 Datasets

The pipeline supports four major SNAP datasets:

| Dataset | Description | Nodes | Edges | Size | Type |
|---------|-------------|-------|-------|------|------|
| **soc-Pokec** | Pokec social network (Slovakia) | 1.6M | 30.6M | 215MB | Social Network |
| **email-EuAll** | Email communication network | 265K | 420K | 4MB | Communication |
| **cit-Patents** | US patent citation network | 3.8M | 16.5M | 161MB | Citation |
| **soc-LiveJournal1** | LiveJournal social network | 4.8M | 69.0M | 467MB | Social Network |

## 🚀 Quick Start

### Run Complete Pipeline

The easiest way to run the entire pipeline:

```bash
# From the repository root
cd scripts/data_pipeline

# Run complete pipeline for all datasets
python run_pipeline.py

# Run for specific datasets only
python run_pipeline.py --datasets soc-Pokec email-EuAll
```

### Individual Steps

You can also run each step separately:

```bash
# Step 1: Download datasets
python download_datasets.py

# Step 2: Ingest and validate
python ingest_datasets.py

# Step 3: Load to HDFS
python load_to_hdfs.py
```

## 📖 Detailed Usage

### Download Datasets

```bash
# Download all datasets
python download_datasets.py

# Download specific datasets
python download_datasets.py --datasets soc-Pokec email-EuAll

# Force re-download (overwrite existing files)
python download_datasets.py --force

# Show help
python download_datasets.py --help
```

**Features:**
- Progress bars for each download
- Resume capability for interrupted downloads
- Automatic retry with exponential backoff (3 attempts)
- File validation (size checks, gzip validation)
- Comprehensive logging to `data_pipeline.log`

### Ingest Datasets

```bash
# Ingest all datasets
python ingest_datasets.py

# Ingest specific datasets
python ingest_datasets.py --datasets soc-Pokec email-EuAll

# Skip validation (faster, but not recommended)
python ingest_datasets.py --skip-validation

# Show help
python ingest_datasets.py --help
```

**Features:**
- Extracts gzip-compressed files
- Validates data format and content
- Generates dataset statistics (nodes, edges, ranges)
- Compares against expected values
- Comprehensive error handling

### Load to HDFS

```bash
# Load all datasets to HDFS
python load_to_hdfs.py

# Load specific datasets
python load_to_hdfs.py --datasets soc-Pokec email-EuAll

# Set custom replication factor
python load_to_hdfs.py --replication 2

# Overwrite existing files in HDFS
python load_to_hdfs.py --overwrite

# Show help
python load_to_hdfs.py --help
```

**Features:**
- Automatic HDFS directory creation
- Configurable replication factor (default: 3)
- Upload verification (size checks)
- Progress tracking and logging
- Handles large files efficiently

### Run Complete Pipeline

```bash
# Run all steps for all datasets
python run_pipeline.py

# Run for specific datasets
python run_pipeline.py --datasets soc-Pokec email-EuAll

# Skip download step (use existing files)
python run_pipeline.py --skip-download

# Skip HDFS loading (download and ingest only)
python run_pipeline.py --skip-hdfs

# Force re-download
python run_pipeline.py --force-download

# Show help
python run_pipeline.py --help
```

## 🏗️ Directory Structure

```
data/
├── raw/                          # Downloaded compressed files
│   ├── soc-pokec-relationships.txt.gz
│   ├── email-EuAll.txt.gz
│   ├── cit-Patents.txt.gz
│   └── soc-LiveJournal1.txt.gz
└── processed/                    # Extracted and validated files
    ├── soc-pokec-relationships.txt
    ├── email-EuAll.txt
    ├── cit-Patents.txt
    └── soc-LiveJournal1.txt

scripts/data_pipeline/
├── config.py                     # Configuration and dataset metadata
├── download_datasets.py          # Download module
├── ingest_datasets.py           # Ingestion module
├── load_to_hdfs.py              # HDFS loading module
├── run_pipeline.py              # Complete pipeline orchestrator
└── README.md                    # This file
```

## 🔧 Configuration

Edit `config.py` to customize:

```python
# HDFS settings
HDFS_HOST = 'hadoop'
HDFS_PORT = 9000
HDFS_BASE_PATH = '/user/root/snap_datasets'

# Download settings
CHUNK_SIZE = 8192  # Bytes
MAX_RETRIES = 3
RETRY_DELAY = 5    # Seconds
TIMEOUT = 300      # Seconds

# Logging
LOG_LEVEL = 'INFO'  # DEBUG, INFO, WARNING, ERROR
```

## 📊 HDFS Structure

After loading, datasets are organized in HDFS as:

```
/user/root/snap_datasets/
├── soc-Pokec/
│   └── soc-pokec-relationships.txt
├── email-EuAll/
│   └── email-EuAll.txt
├── cit-Patents/
│   └── cit-Patents.txt
└── soc-LiveJournal1/
    └── soc-LiveJournal1.txt
```

## 🔍 Verification

### Check Downloaded Files

```bash
ls -lh ../../data/raw/
ls -lh ../../data/processed/
```

### Check Files in HDFS

```bash
# From Hadoop container
docker exec hadoop hadoop fs -ls /user/root/snap_datasets/
docker exec hadoop hadoop fs -ls -R /user/root/snap_datasets/

# Check file size and replication
docker exec hadoop hadoop fs -stat "%n: %b bytes, %r replicas" /user/root/snap_datasets/*/\*.txt
```

### View Dataset Statistics

```bash
# Check pipeline log
tail -f data_pipeline.log

# View specific dataset info in HDFS
docker exec hadoop hadoop fs -cat /user/root/snap_datasets/email-EuAll/email-EuAll.txt | head -n 20
```

## 🛠️ Makefile Integration

Add these targets to the main `Makefile` for easy access:

```makefile
# Data pipeline targets
data-download: ## Download SNAP datasets
	cd scripts/data_pipeline && python download_datasets.py

data-ingest: ## Ingest and validate datasets
	cd scripts/data_pipeline && python ingest_datasets.py

data-load: ## Load datasets to HDFS
	docker exec hadoop python3 /scripts/data_pipeline/load_to_hdfs.py

data-pipeline: ## Run complete data pipeline
	cd scripts/data_pipeline && python download_datasets.py && \
	python ingest_datasets.py && \
	docker exec hadoop python3 /scripts/data_pipeline/load_to_hdfs.py
```

## ⚠️ Prerequisites

### System Requirements
- Python 3.7+
- Docker and Docker Compose
- 5GB+ free disk space (for all datasets)
- Network connectivity to download from snap.stanford.edu

### Python Dependencies
```bash
pip install requests tqdm
```

### Hadoop/HDFS
The Hadoop container must be running before loading to HDFS:
```bash
make up          # Start all services
make hadoop      # Start only Hadoop
```

## 🐛 Troubleshooting

### Download Issues

**Problem:** Download fails with timeout
```bash
# Solution: Increase timeout in config.py
TIMEOUT = 600  # 10 minutes
```

**Problem:** File validation fails
```bash
# Solution: Force re-download
python download_datasets.py --force --datasets DATASET_NAME
```

### Ingestion Issues

**Problem:** Edge count doesn't match expected value
```bash
# Solution: This is usually OK if within 5% tolerance
# The file format may have variations (comments, blank lines)
# Check the log for actual vs expected counts
```

**Problem:** Out of memory during analysis
```bash
# Solution: The script limits analysis to 1M lines by default
# For full analysis, ensure you have sufficient RAM
```

### HDFS Issues

**Problem:** Cannot connect to HDFS
```bash
# Solution: Verify Hadoop is running
docker ps | grep hadoop
docker exec hadoop hadoop fs -ls /

# Check HDFS configuration
docker exec hadoop cat /opt/hadoop/etc/hadoop/core-site.xml
```

**Problem:** Permission denied in HDFS
```bash
# Solution: Run command as root user or check HDFS permissions
docker exec -u root hadoop hadoop fs -chmod 777 /user/root/snap_datasets
```

**Problem:** Upload fails with "No space left"
```bash
# Solution: Check HDFS capacity
docker exec hadoop hadoop fs -df -h
docker exec hadoop hadoop dfsadmin -report
```

## 📈 Best Practices

### For Production Use

1. **Start Small**: Test with `email-EuAll` (smallest dataset) first
2. **Incremental Loading**: Process datasets one at a time for large deployments
3. **Monitoring**: Check logs regularly (`tail -f data_pipeline.log`)
4. **Validation**: Always run with validation enabled (don't use `--skip-validation`)
5. **Backups**: Keep raw downloaded files as backup
6. **Replication**: Use appropriate replication factor based on cluster size
7. **Cleanup**: Remove processed files after successful HDFS load if disk space is limited

### Performance Optimization

```bash
# For faster downloads (if bandwidth is not an issue)
# Modify CHUNK_SIZE in config.py to 65536 (64KB)

# For faster HDFS uploads
# Use lower replication factor during testing
python load_to_hdfs.py --replication 1

# Skip validation during development
python ingest_datasets.py --skip-validation
```

## 📚 Data Format

All datasets are in edge list format:
```
# Comment lines start with #
# Format: FromNodeId    ToNodeId
1    2
1    3
2    4
...
```

- Tab or space-separated
- Node IDs are integers
- Directed edges (u → v)
- May include comment headers

## 🔗 References

- [Stanford SNAP Project](https://snap.stanford.edu/)
- [SNAP Datasets](https://snap.stanford.edu/data/)
- [Hadoop HDFS Documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)

## 📝 License

This data pipeline is part of the Big Data Analytics project. The datasets themselves are provided by Stanford SNAP under their respective licenses.

## 🤝 Contributing

To add new datasets:
1. Add dataset configuration to `config.py` in the `DATASETS` dictionary
2. Update this README with dataset information
3. Test the complete pipeline

## 📞 Support

For issues or questions:
- Check logs: `data_pipeline.log`
- Review error messages carefully
- Ensure all prerequisites are met
- Verify Hadoop/HDFS is running properly
