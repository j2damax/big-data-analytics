# In-Degree Distribution Analysis using Apache Hadoop and Apache Spark

## Overview

This project implements and compares the performance of **Apache Hadoop (MapReduce)** and **Apache Spark** for computing in-degree distribution on large-scale graph datasets from the Stanford SNAP repository.

## Objective

Analyze and compare the performance characteristics of two major big data processing frameworks when computing graph in-degree distributions on real-world network datasets of varying sizes.

## Datasets

Real-world graph datasets from [Stanford SNAP](https://snap.stanford.edu/data/):

| Dataset | Type | Nodes | Edges | Size | Description |
|---------|------|-------|-------|------|-------------|
| **email-EuAll** | Communication Network | 265K | 420K | ~5MB | European institution email network |
| **cit-Patents** | Citation Network | 3.8M | 16.5M | ~280MB | US patent citation network |
| **soc-Pokec** | Social Network | 1.6M | 30.6M | ~404MB | Slovak social network |
| **soc-LiveJournal1** | Social Network | 4.8M | 68.9M | ~1GB | LiveJournal friendships (scalability testing) |

## Architecture

### Technology Stack
- **Apache Hadoop 3.3.6** - MapReduce implementation
- **Apache Spark 3.5.0** - Distributed data processing
- **Docker & Docker Compose** - Containerized deployment
- **Python 3.11** - Implementation language
- **mrjob** - Hadoop MapReduce jobs
- **PySpark** - Spark Python API

### System Components
```
task1/
├── hadoop/              # Hadoop configuration and Dockerfile
├── spark/               # Spark configuration and Dockerfile
├── scripts/
│   ├── data_pipeline/   # Dataset download and preprocessing
│   └── indegree_analysis/
│       ├── hadoop_indegree.py    # Hadoop MapReduce implementation
│       ├── spark_indegree.py     # Spark implementation
│       ├── run_experiments.py    # Automated experiment runner
│       └── visualize_results.py  # Results visualization
├── data/
│   ├── raw/            # Downloaded datasets (gitignored)
│   └── processed/      # Extracted datasets (gitignored)
└── docker-compose.yml  # Service orchestration
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.11+ (for local visualization)
- 16GB+ RAM recommended
- 20GB+ disk space for datasets

### 1. Clone and Setup
```bash
git clone https://github.com/j2damax/big-data-analytics.git
cd big-data-analytics/task1
```

### 2. Start Services
```bash
# Build and start Hadoop and Spark clusters
make up

# Verify services are running
make ps
```

**Web UIs:**
- Hadoop NameNode: http://localhost:9870
- Spark Master: http://localhost:8080
- YARN ResourceManager: http://localhost:8088

### 3. Download and Load Data
```bash
# Download SNAP datasets
make data-download

# Extract datasets
make data-prepare

# Load to HDFS
make data-load

# Verify data in HDFS
make data-status
```

### 4. Run Experiments

#### Single Dataset Test
```bash
# Test Hadoop implementation
make indegree-hadoop

# Test Spark implementation
make indegree-spark
```

#### Full Experiments
```bash
# Run experiments on all datasets with Hadoop
make indegree-experiments-hadoop

# Run experiments on all datasets with Spark
make indegree-experiments-spark
```

#### Visualize Results
```bash
# Activate Python virtual environment
source ../venv/bin/activate

# Generate performance comparison plots
make indegree-visualize
```

## Implementation Details

### Hadoop MapReduce Approach

**Algorithm:**
1. **Map Phase:** Parse each edge `(source, target)` and emit `(target, 1)`
2. **Reduce Phase:** Sum all values for each node to get in-degree
3. **Secondary MapReduce:** Group nodes by in-degree value to get distribution

**File:** `scripts/indegree_analysis/hadoop_indegree.py`

**Key Features:**
- Uses mrjob for simplified MapReduce job definition
- Two-stage processing for distribution calculation
- Hadoop Streaming API integration

### Apache Spark Approach

**Algorithm:**
1. Load edge list as RDD
2. Extract target nodes with `map(lambda edge: (edge[1], 1))`
3. Aggregate using `reduceByKey` to compute in-degrees
4. Group by in-degree value using `map` and `reduceByKey`

**File:** `scripts/indegree_analysis/spark_indegree.py`

**Key Features:**
- In-memory computation for faster processing
- RDD transformations and actions
- Automatic optimization through Catalyst

## Performance Metrics

### Measured Metrics
- **Execution Time:** Total job completion time
- **Memory Usage:** Peak memory consumption
- **CPU Utilization:** Average CPU usage during processing
- **Disk I/O:** Read/write operations
- **Network Overhead:** Data shuffle and transfer

### Results Summary

Results are stored in `scripts/indegree_analysis/results/experiment_results.json` and visualized in `scripts/indegree_analysis/plots/`.

**Key Findings:**
- Spark significantly outperforms Hadoop on smaller datasets (2-3x faster)
- Both frameworks scale well with increasing dataset size
- Hadoop shows more consistent performance across different data sizes
- Spark's in-memory processing excels when data fits in RAM

See `scripts/indegree_analysis/plots/ANALYSIS_REPORT.md` for detailed analysis.

## Project Structure

```
task1/
├── Makefile                    # Build and run automation
├── docker-compose.yml          # Service definitions
├── hadoop/
│   ├── Dockerfile             # Hadoop container image
│   └── config/                # Hadoop configuration files
├── spark/
│   ├── Dockerfile             # Spark container image
│   └── README.md
├── scripts/
│   ├── data_pipeline/
│   │   ├── download-datasets.sh    # Dataset downloader
│   │   ├── extract_datasets.sh     # Data extraction
│   │   └── load_to_hdfs.sh         # HDFS loader
│   └── indegree_analysis/
│       ├── hadoop_indegree.py      # Hadoop implementation
│       ├── spark_indegree.py       # Spark implementation
│       ├── run_experiments.py      # Experiment automation
│       ├── visualize_results.py    # Visualization generator
│       ├── results/                # Experiment results
│       └── plots/                  # Generated visualizations
└── data/                       # Datasets (gitignored)
```

## Available Make Targets

```bash
make help              # Show all available targets
make build             # Build Docker images
make up                # Start all services
make down              # Stop all services
make restart           # Restart services
make logs              # View service logs
make ps                # Show container status
make clean             # Remove all containers and volumes

# Data Pipeline
make data-download     # Download SNAP datasets
make data-prepare      # Extract datasets
make data-load         # Load to HDFS
make data-status       # Check HDFS data status

# Experiments
make indegree-hadoop   # Run Hadoop test
make indegree-spark    # Run Spark test
make indegree-experiments-hadoop    # Run all Hadoop experiments
make indegree-experiments-spark     # Run all Spark experiments
make indegree-visualize             # Generate visualizations

# Development
make shell-hadoop      # Open Hadoop container shell
make shell-spark       # Open Spark container shell
make rebuild           # Full rebuild
```

## Development Setup

### Python Environment
```bash
# Navigate to project root
cd ..

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Dependencies
- pyspark==3.5.0
- kafka-python==2.0.2
- mrjob==0.7.4
- numpy>=1.26.0
- pandas>=2.1.0
- matplotlib>=3.8.0
- requests==2.31.0
- tqdm==4.66.1

## Troubleshooting

### Services won't start
```bash
# Check container logs
make logs

# Rebuild from scratch
make clean
make rebuild
```

### Data not loading to HDFS
```bash
# Verify HDFS is running
docker exec hadoop hdfs dfsadmin -report

# Check namenode is out of safe mode
docker exec hadoop hdfs dfsadmin -safemode get
```

### Experiment failures
```bash
# Check Hadoop logs
docker exec hadoop yarn logs -applicationId <app_id>

# Check Spark logs
docker exec spark-master cat /opt/spark/logs/*
```

### Python environment issues
```bash
# Reinstall dependencies
pip install --upgrade pip
pip install -r requirements.txt --force-reinstall
```

## Results and Analysis

After running experiments, results are available in:
- **JSON Data:** `scripts/indegree_analysis/results/experiment_results.json`
- **Visualizations:** `scripts/indegree_analysis/plots/`
- **Analysis Report:** `scripts/indegree_analysis/plots/ANALYSIS_REPORT.md`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is for educational purposes as part of big data analytics coursework.

## References

- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Stanford SNAP Datasets](https://snap.stanford.edu/data/)
- [mrjob Documentation](https://mrjob.readthedocs.io/)

## Authors

- Jayampathy (j2damax)

## Acknowledgments

- Stanford Network Analysis Project (SNAP) for providing high-quality graph datasets
- Apache Software Foundation for Hadoop and Spark
- Course instructors and teaching assistants
