# Data Acquisition and Preparation Pipeline

## Overview

This document describes the data acquisition and preparation infrastructure for loading Stanford SNAP datasets into HDFS for big data analytics with Hadoop and Spark.

## 🎯 Purpose

The data pipeline automates the complete workflow for:
1. Downloading large-scale graph datasets from Stanford SNAP repository
2. Validating and processing the downloaded data
3. Loading datasets into Hadoop Distributed File System (HDFS)

This enables performance comparisons between Hadoop MapReduce and Apache Spark using real-world, large-scale datasets.

## 📊 Supported Datasets

Four datasets have been selected for comprehensive performance analysis:

### 1. **soc-Pokec** - Social Network
- **Description**: Pokec social network from Slovakia (friendships)
- **Scale**: 1.6M nodes, 30.6M edges
- **Size**: ~215MB compressed
- **Use Case**: Medium-scale social network analysis
- **URL**: https://snap.stanford.edu/data/soc-Pokec.html

### 2. **email-EuAll** - Communication Network
- **Description**: Email communication network from EU research institution
- **Scale**: 265K nodes, 420K edges
- **Size**: ~4MB compressed
- **Use Case**: Small-scale testing and validation
- **URL**: https://snap.stanford.edu/data/email-EuAll.html

### 3. **cit-Patents** - Citation Network
- **Description**: US patent citation network
- **Scale**: 3.8M nodes, 16.5M edges
- **Size**: ~161MB compressed
- **Use Case**: Citation analysis and graph algorithms
- **URL**: https://snap.stanford.edu/data/cit-Patents.html

### 4. **soc-LiveJournal1** - Large Social Network
- **Description**: LiveJournal social network (scalability testing)
- **Scale**: 4.8M nodes, 69.0M edges
- **Size**: ~467MB compressed
- **Use Case**: Large-scale performance benchmarking
- **URL**: https://snap.stanford.edu/data/soc-LiveJournal1.html

**Total Storage Required**: ~850MB compressed, ~3.5GB uncompressed

## 🏗️ Architecture

### Pipeline Stages

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  Download   │ --> │   Ingest &   │ --> │  Load to    │
│  from SNAP  │     │   Validate   │     │    HDFS     │
└─────────────┘     └──────────────┘     └─────────────┘
     ↓                     ↓                     ↓
  data/raw/          data/processed/      hdfs://hadoop:9000/
  *.txt.gz              *.txt              /user/root/snap_datasets/
```

### Component Details

#### 1. **Download Module** (`download_datasets.py`)
- **Responsibility**: Fetch datasets from SNAP repository
- **Features**:
  - Progress tracking with tqdm
  - Resume capability for interrupted downloads
  - Retry logic with exponential backoff
  - File integrity validation (size and gzip format)
  - Comprehensive error handling and logging

#### 2. **Ingestion Module** (`ingest_datasets.py`)
- **Responsibility**: Extract and validate datasets
- **Features**:
  - Gzip extraction with memory-efficient streaming
  - Statistical analysis (node count, edge count, ranges)
  - Format validation (edge list parsing)
  - Comparison with expected dataset properties
  - Quality assurance checks

#### 3. **HDFS Loader** (`load_to_hdfs.py`)
- **Responsibility**: Upload datasets to Hadoop HDFS
- **Features**:
  - Automatic directory structure creation
  - Configurable replication factor
  - Upload verification (size matching)
  - Progress tracking and status reporting
  - Integration with Hadoop CLI tools

#### 4. **Pipeline Orchestrator** (`run_pipeline.py`)
- **Responsibility**: Coordinate all pipeline stages
- **Features**:
  - End-to-end workflow execution
  - Stage skipping for development
  - Timing and performance metrics
  - Comprehensive logging
  - Error propagation and handling

## 🚀 Quick Start

### Prerequisites

1. **System Requirements**:
   - Docker and Docker Compose installed
   - Python 3.7 or higher
   - 5GB+ free disk space
   - Network connectivity

2. **Start Hadoop**:
   ```bash
   make up
   # Or just Hadoop:
   make hadoop
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Pipeline

#### Option 1: Complete Pipeline (Recommended)
```bash
# Run all stages for all datasets
make data-pipeline

# Or manually:
cd scripts/data_pipeline
python run_pipeline.py
```

#### Option 2: Individual Stages
```bash
# Download only
make data-download

# Ingest only (after download)
make data-ingest

# Load to HDFS only (after ingest, requires Hadoop running)
make data-load
```

#### Option 3: Selective Datasets
```bash
cd scripts/data_pipeline

# Small dataset for testing
python run_pipeline.py --datasets email-EuAll

# Multiple datasets
python run_pipeline.py --datasets soc-Pokec email-EuAll
```

## 📖 Usage Examples

### Basic Usage

```bash
# Download and process all datasets
cd scripts/data_pipeline
python run_pipeline.py
```

### Development Workflow

```bash
# Download once, then iterate on ingestion/loading
python download_datasets.py
python ingest_datasets.py
python load_to_hdfs.py --overwrite
```

### Production Workflow

```bash
# Start with smallest dataset
python run_pipeline.py --datasets email-EuAll

# Then larger datasets incrementally
python run_pipeline.py --datasets soc-Pokec
python run_pipeline.py --datasets cit-Patents
python run_pipeline.py --datasets soc-LiveJournal1
```

### Advanced Options

```bash
# Force re-download
python run_pipeline.py --force-download

# Skip HDFS loading (for local development)
python run_pipeline.py --skip-hdfs

# Custom HDFS replication
python run_pipeline.py --replication 2

# Skip download if files exist
python run_pipeline.py --skip-download
```

## 🔧 Configuration

Edit `scripts/data_pipeline/config.py` to customize:

```python
# HDFS Configuration
HDFS_HOST = 'hadoop'           # Namenode hostname
HDFS_PORT = 9000              # Namenode port
HDFS_BASE_PATH = '/user/root/snap_datasets'

# Download Configuration
CHUNK_SIZE = 8192             # Download chunk size (bytes)
MAX_RETRIES = 3               # Number of retry attempts
RETRY_DELAY = 5               # Initial retry delay (seconds)
TIMEOUT = 300                 # Request timeout (seconds)

# Logging
LOG_LEVEL = 'INFO'            # DEBUG, INFO, WARNING, ERROR
```

## 📂 Directory Structure

After running the pipeline:

```
big-data-analytics/
├── data/
│   ├── raw/                           # Downloaded compressed files
│   │   ├── soc-pokec-relationships.txt.gz
│   │   ├── email-EuAll.txt.gz
│   │   ├── cit-Patents.txt.gz
│   │   └── soc-LiveJournal1.txt.gz
│   └── processed/                     # Extracted files
│       ├── soc-pokec-relationships.txt
│       ├── email-EuAll.txt
│       ├── cit-Patents.txt
│       └── soc-LiveJournal1.txt
├── scripts/
│   └── data_pipeline/
│       ├── config.py                  # Configuration
│       ├── download_datasets.py       # Download module
│       ├── ingest_datasets.py        # Ingestion module
│       ├── load_to_hdfs.py           # HDFS loading module
│       ├── run_pipeline.py           # Pipeline orchestrator
│       ├── __init__.py               # Package initialization
│       └── README.md                 # Detailed documentation
└── data_pipeline.log                 # Pipeline execution log
```

### HDFS Structure

```
hdfs://hadoop:9000/user/root/snap_datasets/
├── soc-Pokec/
│   └── soc-pokec-relationships.txt
├── email-EuAll/
│   └── email-EuAll.txt
├── cit-Patents/
│   └── cit-Patents.txt
└── soc-LiveJournal1/
    └── soc-LiveJournal1.txt
```

## 🔍 Verification and Testing

### Check Local Files

```bash
# View downloaded files
ls -lh data/raw/

# View processed files
ls -lh data/processed/

# Check file sizes
du -h data/raw/
du -h data/processed/
```

### Check HDFS Files

```bash
# List all datasets in HDFS
make data-status

# Or manually:
docker exec hadoop hadoop fs -ls -R /user/root/snap_datasets/

# Check specific dataset
docker exec hadoop hadoop fs -ls /user/root/snap_datasets/email-EuAll/

# View file properties (size, replication)
docker exec hadoop hadoop fs -stat "%n: %b bytes, %r replicas" \
  /user/root/snap_datasets/*/*.txt

# Preview file content
docker exec hadoop hadoop fs -cat \
  /user/root/snap_datasets/email-EuAll/email-EuAll.txt | head -20
```

### View Logs

```bash
# Real-time log monitoring
tail -f data_pipeline.log

# Search for errors
grep ERROR data_pipeline.log

# View statistics
grep "✓" data_pipeline.log
```

## 🛠️ Industry Best Practices Implemented

### 1. **Reliability**
- Retry logic with exponential backoff
- Resume capability for interrupted operations
- Comprehensive error handling
- Transaction-like operations (cleanup on failure)

### 2. **Observability**
- Structured logging with timestamps
- Progress tracking for long operations
- Performance metrics (timing, throughput)
- Detailed error messages with context

### 3. **Validation**
- File integrity checks (size, format)
- Data quality validation (edge counts, node ranges)
- Upload verification (checksums, size matching)
- Sanity checks at each stage

### 4. **Scalability**
- Memory-efficient streaming for large files
- Chunked downloads and uploads
- Parallel processing capability (foundation)
- Configurable resource usage

### 5. **Maintainability**
- Modular architecture (separation of concerns)
- Configuration-driven (no hardcoded values)
- Comprehensive documentation
- Type hints and docstrings

### 6. **Operational Excellence**
- Command-line interface with help
- Make targets for common operations
- Flexible workflows (skip stages, select datasets)
- Development vs production modes

## 🐛 Troubleshooting

### Common Issues

#### 1. Download Failures
```bash
# Problem: Network timeout
# Solution: Increase timeout in config.py
TIMEOUT = 600

# Problem: File validation fails
# Solution: Force re-download
python download_datasets.py --force
```

#### 2. Ingestion Errors
```bash
# Problem: Cannot extract gzip
# Solution: Re-download the file
rm data/raw/DATASET.txt.gz
python download_datasets.py --datasets DATASET_NAME

# Problem: Edge count mismatch
# Solution: Check if difference is within tolerance (5%)
# Small differences are normal due to format variations
```

#### 3. HDFS Upload Issues
```bash
# Problem: Cannot connect to HDFS
# Solution: Verify Hadoop is running
docker ps | grep hadoop
docker exec hadoop hadoop fs -ls /

# Problem: Permission denied
# Solution: Check HDFS permissions or run as root
docker exec hadoop hadoop fs -chmod 777 /user/root

# Problem: Disk space full
# Solution: Check HDFS capacity
docker exec hadoop hadoop dfsadmin -report
```

### Debug Mode

Enable debug logging for detailed troubleshooting:

```python
# In config.py
LOG_LEVEL = 'DEBUG'
```

Then run the pipeline and check logs:
```bash
python run_pipeline.py
tail -f data_pipeline.log
```

## 📈 Performance Considerations

### Download Stage
- **Bottleneck**: Network bandwidth
- **Optimization**: Run during off-peak hours, use faster network
- **Time Estimate**: 5-30 minutes depending on network speed

### Ingestion Stage
- **Bottleneck**: Disk I/O and CPU
- **Optimization**: Use SSD storage, skip validation for testing
- **Time Estimate**: 5-15 minutes for all datasets

### HDFS Upload Stage
- **Bottleneck**: Network and HDFS write throughput
- **Optimization**: Use replication=1 for testing, increase later
- **Time Estimate**: 5-20 minutes depending on cluster

### Total Pipeline Time
- **Development**: ~10-30 minutes (with optimizations)
- **Production**: ~20-60 minutes (with full validation)

## 🔒 Security Considerations

1. **Data Integrity**: All downloads are validated for size and format
2. **Error Handling**: No sensitive information in logs
3. **Access Control**: HDFS permissions set appropriately
4. **Network Security**: HTTPS downloads from trusted source (Stanford)

## 🚀 Next Steps

After loading datasets into HDFS:

1. **Run Hadoop MapReduce jobs**:
   ```bash
   make test-hadoop
   ```

2. **Run Spark analytics**:
   ```bash
   make test-spark
   ```

3. **Compare Performance**:
   - Analyze execution times
   - Compare resource utilization
   - Evaluate scalability characteristics

4. **Custom Analysis**:
   - Implement graph algorithms (PageRank, Connected Components)
   - Perform statistical analysis
   - Build machine learning models

## 📚 References

- [Stanford SNAP Project](https://snap.stanford.edu/)
- [SNAP Data Repository](https://snap.stanford.edu/data/)
- [Hadoop HDFS Guide](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)

## 📝 License

This pipeline implementation is part of the Big Data Analytics project. The datasets are provided by Stanford SNAP under their respective licenses.

---

For detailed technical documentation, see [scripts/data_pipeline/README.md](scripts/data_pipeline/README.md)
