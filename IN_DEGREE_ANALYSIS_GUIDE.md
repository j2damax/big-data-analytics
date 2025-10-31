# In-Degree Distribution Analysis - Complete Guide

## Project Overview

This project implements and compares two approaches for computing in-degree distribution in large-scale graph datasets:
1. **Apache Hadoop MapReduce** - Traditional disk-based batch processing
2. **Apache Spark** - Modern in-memory distributed computing

The analysis follows a complete experimental methodology including implementation, execution, metrics collection, visualization, and comparative analysis.

## Educational Objectives

This implementation demonstrates:
- **MapReduce Programming**: Two-stage MapReduce paradigm for graph analysis
- **Spark RDD Operations**: Efficient in-memory data processing
- **Performance Analysis**: Comparing different big data frameworks
- **Network Science**: Understanding graph properties and power-law distributions
- **Big Data Pipeline**: End-to-end data processing workflow

## System Architecture

### Infrastructure Components

```
┌─────────────────────────────────────────────────────────┐
│                    Docker Network                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Hadoop     │  │ Spark Master │  │ Spark Worker │ │
│  │  - HDFS      │  │  - 8080      │  │  - 8081      │ │
│  │  - YARN      │  │  - 7077      │  │              │ │
│  │  - 9870,8088 │  │              │  │              │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────┘
                           │
                    HDFS Datasets
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                   │
    email-EuAll      cit-Patents         soc-Pokec
     (~420K)          (~16.5M)           (~30.6M)
```

### Data Flow

```
Input: Edge List in HDFS
         │
         ├──→ MapReduce Job          Spark Job ←──┤
         │    (2 stages)              (RDD ops)    │
         │         │                      │        │
         │    ┌────▼────┐            ┌────▼────┐  │
         │    │ Stage 1 │            │  Read   │  │
         │    │ Count   │            │ Extract │  │
         │    └────┬────┘            └────┬────┘  │
         │         │                      │        │
         │    ┌────▼────┐            ┌────▼────┐  │
         │    │ Stage 2 │            │Aggregate│  │
         │    │  Dist   │            │  Count  │  │
         │    └────┬────┘            └────┬────┘  │
         │         │                      │        │
         └─────────┴──────────────────────┴────────┘
                           │
                    Results JSON
                           │
              ┌────────────┴────────────┐
              │                         │
         Visualizations              Report
         - Distribution plots     - Performance
         - Log-log plots         - Comparison
         - Performance charts    - Analysis
```

## Implementation Details

### 1. Hadoop MapReduce Implementation

**File:** `scripts/indegree_mapreduce.py`

#### Stage 1: Calculate Individual Node In-Degrees

**Mapper:**
```python
Input:  "source destination"
Process: For each edge (u,v), emit destination
Output: (destination_node, 1)
```

**Reducer:**
```python
Input:  (node, [1, 1, 1, ...])
Process: Sum all 1s for each node
Output: (node, in-degree_count)
```

#### Stage 2: Calculate Distribution

**Mapper:**
```python
Input:  (node, k)
Process: Transform to use in-degree as key
Output: (k, 1)
```

**Reducer:**
```python
Input:  (in-degree_k, [1, 1, 1, ...])
Process: Count nodes with this in-degree
Output: (in-degree_k, number_of_nodes)
```

**Key Characteristics:**
- Uses mrjob library for Hadoop integration
- Two-stage pipeline with disk I/O between stages
- Fault-tolerant with automatic retries
- Scales to very large datasets
- Higher latency due to disk operations

### 2. Apache Spark Implementation

**File:** `scripts/indegree_spark.py`

```python
# Step 1: Read and extract destination nodes
edges_rdd = sc.textFile(hdfs_path)
destinations = edges_rdd
    .filter(lambda line: line.strip() and not line.startswith('#'))
    .map(lambda line: line.split())
    .filter(lambda parts: len(parts) >= 2)
    .map(lambda parts: parts[1])

# Step 2: Calculate in-degrees
indegrees = destinations
    .map(lambda node: (node, 1))
    .reduceByKey(lambda a, b: a + b)

# Step 3: Calculate distribution
distribution = indegrees
    .map(lambda node_count: (node_count[1], 1))
    .reduceByKey(lambda a, b: a + b)
    .sortByKey()
```

**Key Characteristics:**
- In-memory processing with RDD transformations
- DAG (Directed Acyclic Graph) execution model
- Lazy evaluation with optimization
- 10-100x faster than MapReduce for iterative tasks
- Better for interactive and exploratory analysis

## Usage Instructions

### Prerequisites

1. **Start the infrastructure:**
```bash
cd /home/runner/work/big-data-analytics/big-data-analytics
make up
```

2. **Verify services are running:**
```bash
make ps
```

3. **Check datasets are in HDFS:**
```bash
make data-status
```

### Quick Start

#### Option 1: Run Complete Analysis (All Datasets)

```bash
# Run the complete pipeline
make indegree-analysis
```

This will:
- Execute both MapReduce and Spark on all datasets
- Collect performance metrics
- Generate visualizations
- Create comprehensive report
- Package everything into a ZIP file

#### Option 2: Run on Single Dataset (Faster)

```bash
# Test with small dataset (~2-3 minutes)
make indegree-analysis-small
```

#### Option 3: Manual Execution

**Inside Hadoop container:**
```bash
docker exec -it hadoop bash

# Run complete analysis
bash /scripts/run_complete_analysis.sh email-EuAll

# Or run individual components
python3 /scripts/run_indegree_experiments.py email-EuAll
python3 /scripts/analyze_indegree_results.py
```

**Inside Spark container:**
```bash
docker exec -it spark-master bash

# Run Spark implementation directly
spark-submit \
    --master spark://spark-master:7077 \
    /scripts/indegree_spark.py \
    hdfs://hadoop:9000/user/root/snap_datasets/email-EuAll/email-EuAll.txt
```

### Viewing Results

#### 1. View Report
```bash
make indegree-report
```

#### 2. Check Results Location
```bash
make indegree-results
```

#### 3. Download Deliverables
```bash
make indegree-download
```

This downloads a ZIP file containing:
- All source code
- Experimental results (JSON)
- Visualizations (PNG images)
- Comprehensive analysis report (Markdown)

### Testing Locally

Test implementations without requiring full cluster:

```bash
# Test with sample data
make indegree-test
```

## Datasets

### Available SNAP Datasets

| Dataset | Nodes | Edges | Type | Description |
|---------|-------|-------|------|-------------|
| **email-EuAll** | ~265K | ~420K | Communication | European institution email network |
| **cit-Patents** | ~3.8M | ~16.5M | Citation | U.S. patent citation network |
| **soc-Pokec** | ~1.6M | ~30.6M | Social | Slovak social network |
| **soc-LiveJournal1** | ~4.8M | ~69M | Social | LiveJournal social network |

### Dataset Characteristics

**email-EuAll (Small)**
- Best for: Quick testing and validation
- Processing time: ~2-5 minutes
- Memory: Minimal
- Use case: Development and debugging

**cit-Patents (Medium)**
- Best for: Realistic performance testing
- Processing time: ~10-20 minutes
- Memory: Moderate
- Use case: Intermediate analysis

**soc-Pokec (Large)**
- Best for: Scalability testing
- Processing time: ~20-40 minutes
- Memory: Significant
- Use case: Performance comparison

**soc-LiveJournal1 (Very Large)**
- Best for: Stress testing
- Processing time: ~40-90 minutes
- Memory: High
- Use case: Production-scale validation

## Output Files and Structure

### Directory Structure

```
/tmp/
├── indegree_results/
│   └── experiment_results_<timestamp>.json
│       ├── Dataset configurations
│       ├── MapReduce results and metrics
│       ├── Spark results and metrics
│       └── Correctness verification
│
├── indegree_plots/
│   ├── indegree_dist_<dataset>_mapreduce.png
│   ├── indegree_dist_<dataset>_spark.png
│   ├── indegree_loglog_<dataset>_mapreduce.png
│   ├── indegree_loglog_<dataset>_spark.png
│   ├── performance_comparison.png
│   ├── speedup_analysis.png
│   └── IN_DEGREE_ANALYSIS_REPORT.md
│
└── indegree_analysis_deliverables_<timestamp>.zip
    ├── Source code (all .py files)
    ├── indegree_results/ (experimental data)
    └── indegree_plots/ (visualizations + report)
```

### Result Files

#### 1. Experiment Results JSON
```json
{
  "email-EuAll": {
    "dataset_info": {...},
    "mapreduce": {
      "success": true,
      "execution_time": 123.45,
      "results": {"1": 50000, "2": 30000, ...}
    },
    "spark": {
      "success": true,
      "execution_time": 45.67,
      "results": {"1": 50000, "2": 30000, ...}
    },
    "correctness_verified": true
  }
}
```

#### 2. Visualizations

**Distribution Plot** (`indegree_dist_*.png`)
- Standard scatter plot
- X-axis: In-degree (k)
- Y-axis: Number of nodes
- Shows frequency distribution

**Log-Log Plot** (`indegree_loglog_*.png`)
- Both axes logarithmic
- Reveals power-law distribution
- Straight line indicates scale-free network
- Common in real-world networks

**Performance Comparison** (`performance_comparison.png`)
- Bar chart comparing execution times
- MapReduce vs Spark side-by-side
- Across all datasets

**Speedup Analysis** (`speedup_analysis.png`)
- Spark speedup factor over MapReduce
- Shows relative performance gain
- Typically 2-10x depending on dataset size

#### 3. Analysis Report

Markdown document containing:
- Executive summary
- Implementation details
- Experimental results with tables
- Performance metrics and comparisons
- Visualizations (embedded images)
- Key findings and analysis
- Conclusions and recommendations

## Performance Metrics

### Metrics Collected

1. **Execution Time**
   - Total time from job submission to completion
   - Includes all stages and shuffles
   - Measured in seconds

2. **Correctness Verification**
   - Binary comparison of results
   - Ensures both implementations are correct
   - Critical for validation

3. **Throughput**
   - Edges processed per second
   - Derived metric: edges / execution_time
   - Indicates processing efficiency

4. **Speedup Factor**
   - Ratio: MapReduce_time / Spark_time
   - Values > 1.0 indicate Spark is faster
   - Typically increases with dataset size

### Expected Performance

Based on typical cluster configurations:

| Dataset | MapReduce | Spark | Speedup |
|---------|-----------|-------|---------|
| email-EuAll | ~60-90s | ~20-30s | ~3x |
| cit-Patents | ~300-600s | ~60-120s | ~4-5x |
| soc-Pokec | ~600-1200s | ~100-200s | ~5-6x |
| soc-LiveJournal1 | ~1200-2400s | ~200-400s | ~6-8x |

*Note: Actual times vary based on hardware, network, and cluster configuration*

## Understanding the Results

### In-Degree Distribution

The in-degree of a node is the number of edges pointing to it (incoming connections).

**Interpretation:**
- **High in-degree nodes**: Popular, influential, or important entities
- **Low in-degree nodes**: Less connected, peripheral entities
- **Distribution shape**: Reveals network structure

### Power-Law Distribution

Many real-world networks follow a power-law distribution:
```
P(k) ~ k^(-γ)
```

Where:
- P(k) = Probability of a node having in-degree k
- γ = Power-law exponent (typically 2-3)

**Characteristics:**
- Most nodes have few connections
- Few nodes have many connections (hubs)
- Scale-free property
- "Rich get richer" phenomenon

**Log-Log Plot:**
- Linear appearance indicates power-law
- Slope = -γ (power-law exponent)
- Deviations reveal network peculiarities

### MapReduce vs Spark Comparison

**When MapReduce is Better:**
- Very large datasets (exceeding cluster memory)
- Simple batch processing
- Strong fault tolerance requirements
- Disk-based storage preferred

**When Spark is Better:**
- Iterative algorithms
- Interactive analysis
- In-memory computation beneficial
- Complex DAG workflows
- Lower latency requirements

**Key Differences:**

| Aspect | MapReduce | Spark |
|--------|-----------|-------|
| **Processing** | Disk-based | In-memory |
| **Speed** | Slower | 10-100x faster |
| **Model** | Map → Shuffle → Reduce | DAG with transformations |
| **API** | Limited | Rich (map, filter, reduce, etc.) |
| **Caching** | No | Yes (memory/disk) |
| **Iterations** | Inefficient | Efficient |
| **Latency** | High (minutes) | Low (seconds) |
| **Memory** | Low requirements | Higher requirements |

## Troubleshooting

### Common Issues

#### 1. Services Not Running
```bash
# Check container status
make ps

# Restart services
make restart

# View logs
make logs
```

#### 2. HDFS Connectivity Issues
```bash
# Test HDFS access
docker exec hadoop hadoop fs -ls /

# Check datasets
make data-status

# Reload datasets if missing
make data-load
```

#### 3. MapReduce Job Fails
```bash
# Check Hadoop logs
docker logs hadoop

# Verify YARN
curl http://localhost:8088

# Check streaming jar exists
docker exec hadoop ls -l /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar
```

#### 4. Spark Job Fails
```bash
# Check Spark logs
docker logs spark-master
docker logs spark-worker

# Verify Spark cluster
curl http://localhost:8080

# Test Spark connectivity
docker exec spark-master spark-submit --version
```

#### 5. No Results Generated
```bash
# Check for errors in experiment
docker exec hadoop cat /tmp/indegree_results/experiment_results_*.json

# Verify write permissions
docker exec hadoop ls -la /tmp/

# Re-run with verbose output
docker exec hadoop bash -x /scripts/run_complete_analysis.sh email-EuAll
```

#### 6. Memory Issues
```bash
# Check Docker memory allocation
docker stats

# Increase Docker memory to 8GB+ in Docker Desktop settings

# Reduce dataset size for testing
make indegree-analysis-small
```

### Error Messages

**"mrjob not found"**
- Solution: Ensure requirements.txt installed in container
- Check: `docker exec hadoop pip list | grep mrjob`

**"Connection refused to spark-master"**
- Solution: Ensure Spark services running
- Check: `make ps` and verify spark-master status

**"HDFS path not found"**
- Solution: Verify datasets loaded
- Check: `make data-status`
- Fix: `make data-load`

**"Timeout after 1 hour"**
- Normal for very large datasets
- Consider: Run smaller dataset first
- Or: Increase timeout in experiment script

## Advanced Usage

### Custom Dataset

To analyze your own graph data:

1. **Prepare data in edge list format:**
```
# Format: source destination
1 2
1 3
2 4
3 4
```

2. **Load to HDFS:**
```bash
docker exec hadoop hadoop fs -put /path/to/your/data.txt /user/root/custom/
```

3. **Run analysis:**
```bash
docker exec hadoop python3 /scripts/run_indegree_experiments.py
# Edit script to add your custom dataset path
```

### Modify Experiment Parameters

Edit `scripts/run_indegree_experiments.py`:

```python
# Add custom dataset
self.datasets['my-dataset'] = {
    'name': 'my-dataset',
    'hdfs_path': 'hdfs://hadoop:9000/user/root/custom/my-data.txt',
    'size': 'Custom size'
}

# Adjust timeout
timeout=7200  # 2 hours
```

### Export Results for Further Analysis

```python
# In Python environment
import json

# Load results
with open('/tmp/indegree_results/experiment_results_*.json') as f:
    data = json.load(f)

# Extract specific metrics
for dataset, results in data.items():
    mr_time = results['mapreduce']['execution_time']
    spark_time = results['spark']['execution_time']
    print(f"{dataset}: MR={mr_time}s, Spark={spark_time}s")
```

## Educational Exercises

### Beginner Level

1. **Run and Understand:**
   - Execute analysis on email-EuAll dataset
   - Read the generated report
   - Understand the visualizations

2. **Modify Parameters:**
   - Change timeout values
   - Add print statements to track progress
   - Experiment with different datasets

### Intermediate Level

3. **Compare Implementations:**
   - Study MapReduce vs Spark code
   - Identify key differences
   - Explain why Spark is faster

4. **Extend Analysis:**
   - Add out-degree calculation
   - Calculate average degree
   - Find top-k most connected nodes

### Advanced Level

5. **Optimize Performance:**
   - Tune Spark parallelism
   - Adjust memory allocation
   - Optimize shuffle operations

6. **New Algorithms:**
   - Implement PageRank
   - Calculate clustering coefficient
   - Detect communities

## References

### Documentation
- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [mrjob Documentation](https://mrjob.readthedocs.io/)

### Datasets
- [SNAP: Stanford Network Analysis Project](https://snap.stanford.edu/data/)
- [Network Repository](http://networkrepository.com/)

### Network Science
- [Network Science Book by Albert-László Barabási](http://networksciencebook.com/)
- [Power-Law Distributions](https://en.wikipedia.org/wiki/Power_law)
- [Scale-Free Networks](https://en.wikipedia.org/wiki/Scale-free_network)

### Big Data Processing
- [MapReduce: Simplified Data Processing](https://research.google/pubs/pub62/)
- [Resilient Distributed Datasets (RDD) Paper](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)

## License

This project is provided for educational purposes as part of the big-data-analytics learning environment.

---

## Quick Command Reference

```bash
# Setup
make up                          # Start all services
make ps                          # Check status
make data-status                # Check HDFS datasets

# Run Analysis
make indegree-analysis-small    # Quick test (5-10 min)
make indegree-analysis          # Full analysis (30-120 min)
make indegree-test              # Local test (1 min)

# View Results
make indegree-results           # List results
make indegree-report            # View report
make indegree-download          # Download deliverables

# Debugging
make logs                       # View all logs
make shell-hadoop              # Hadoop container shell
make shell-spark               # Spark container shell

# Cleanup
make down                       # Stop services
make clean                      # Remove everything
```

---

**Questions or Issues?**
- Check the troubleshooting section
- Review log files
- Consult the documentation
- Open an issue in the repository
