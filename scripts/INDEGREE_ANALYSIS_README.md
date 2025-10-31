# In-Degree Distribution Analysis

This directory contains implementations for computing in-degree distribution of graph datasets using both Apache Hadoop MapReduce and Apache Spark.

## Overview

The in-degree distribution analysis computes how many nodes in a graph have a given number of incoming edges. This is a fundamental metric in network analysis, particularly useful for understanding the connectivity patterns in social networks, citation networks, and other graph structures.

## Implementations

### 1. Apache Hadoop MapReduce (`indegree_mapreduce.py`)

A traditional two-stage MapReduce implementation:

**Stage 1: Calculate Individual Node In-Degrees**
- **Mapper**: Reads each edge (u,v), outputs (destination v, 1)
- **Reducer**: Sums counts for each node, outputs (node, in-degree)

**Stage 2: Calculate the Distribution**
- **Mapper**: Transforms (node, k) to (k, 1)
- **Reducer**: Counts nodes per in-degree, outputs (k, count)

### 2. Apache Spark (`indegree_spark.py`)

An efficient in-memory implementation using PySpark RDDs:

1. Read edge list and extract destination nodes
2. Use `map` and `reduceByKey` to compute in-degrees
3. Transform and aggregate to calculate distribution
4. Leverages Spark's DAG execution and memory caching

## Usage

### Quick Start - Run Complete Analysis

The simplest way to run the complete analysis pipeline:

```bash
# Run on all datasets
bash /scripts/run_complete_analysis.sh

# Run on specific datasets
bash /scripts/run_complete_analysis.sh email-EuAll cit-Patents
```

This will:
1. Execute both MapReduce and Spark implementations
2. Collect performance metrics
3. Generate visualizations
4. Create a comprehensive report
5. Package everything into a ZIP file

### Running Individual Components

#### 1. Run Experiments Only

```bash
# Run on all datasets
python3 /scripts/run_indegree_experiments.py

# Run on specific datasets
python3 /scripts/run_indegree_experiments.py email-EuAll
```

#### 2. Generate Visualizations and Report

```bash
# Uses most recent experiment results
python3 /scripts/analyze_indegree_results.py

# Use specific results file
python3 /scripts/analyze_indegree_results.py /tmp/indegree_results/experiment_results_123456.json
```

#### 3. Run MapReduce Implementation Directly

```bash
# Using mrjob with Hadoop
python3 /scripts/indegree_mapreduce.py \
    -r hadoop \
    --hadoop-bin hadoop \
    --hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    --output-dir /tmp/mr_output \
    hdfs://hadoop:9000/user/root/snap_datasets/email-EuAll/email-EuAll.txt
```

#### 4. Run Spark Implementation Directly

```bash
# Using spark-submit
spark-submit \
    --master spark://spark-master:7077 \
    /scripts/indegree_spark.py \
    hdfs://hadoop:9000/user/root/snap_datasets/email-EuAll/email-EuAll.txt \
    /tmp/spark_output
```

## Datasets

The implementations work with the following SNAP datasets loaded in HDFS:

| Dataset | Size | HDFS Path |
|---------|------|-----------|
| email-EuAll | ~420K edges | `hdfs://hadoop:9000/user/root/snap_datasets/email-EuAll/email-EuAll.txt` |
| cit-Patents | ~16.5M edges | `hdfs://hadoop:9000/user/root/snap_datasets/cit-Patents/cit-Patents.txt` |
| soc-Pokec | ~30.6M edges | `hdfs://hadoop:9000/user/root/snap_datasets/soc-Pokec/soc-pokec-relationships.txt` |
| soc-LiveJournal1 | ~69M edges | `hdfs://hadoop:9000/user/root/snap_datasets/soc-LiveJournal1/soc-LiveJournal1.txt` |

## Output Files

After running the analysis pipeline, you'll find:

### Results Directory (`/tmp/indegree_results/`)
- `experiment_results_<timestamp>.json` - Raw experimental data including:
  - Execution times for each implementation
  - Performance metrics
  - Correctness verification results
  - Distribution data

### Plots Directory (`/tmp/indegree_plots/`)
- `indegree_dist_<dataset>_<impl>.png` - Standard distribution plots
- `indegree_loglog_<dataset>_<impl>.png` - Log-log plots for power-law analysis
- `performance_comparison.png` - Execution time comparison
- `speedup_analysis.png` - Spark speedup over MapReduce
- `IN_DEGREE_ANALYSIS_REPORT.md` - Comprehensive analysis report

### Deliverables Archive (`/tmp/indegree_analysis_deliverables_<timestamp>.zip`)
Complete package containing:
- All source code files
- Experimental results
- Visualizations
- Analysis report

## Performance Metrics Collected

The experiment runner collects:

1. **Execution Time**: Total time from job submission to completion
2. **Correctness**: Verification that both implementations produce identical results
3. **Throughput**: Edges processed per second
4. **Scalability**: Performance across different dataset sizes

Additional metrics (when running in full cluster):
- Memory usage
- CPU utilization
- Disk I/O
- Network overhead

## Example Workflow

### Within Hadoop Container

```bash
# Access Hadoop container
docker exec -it hadoop bash

# Run complete analysis on small dataset (quick test)
bash /scripts/run_complete_analysis.sh email-EuAll

# Run on multiple datasets
bash /scripts/run_complete_analysis.sh email-EuAll cit-Patents

# View results
ls -lh /tmp/indegree_plots/
cat /tmp/indegree_plots/IN_DEGREE_ANALYSIS_REPORT.md
```

### Within Spark Container

```bash
# Access Spark container
docker exec -it spark-master bash

# Run Spark implementation only
spark-submit \
    --master spark://spark-master:7077 \
    /scripts/indegree_spark.py \
    hdfs://hadoop:9000/user/root/snap_datasets/email-EuAll/email-EuAll.txt
```

## Understanding the Results

### Distribution Plots
- **X-axis**: In-degree (k) - number of incoming edges
- **Y-axis**: Count - number of nodes with that in-degree
- Shows the frequency distribution of node connectivity

### Log-Log Plots
- Both axes on logarithmic scale
- Useful for identifying power-law distributions
- Straight line indicates power-law behavior: P(k) ~ k^(-γ)
- Common in real-world networks (scale-free networks)

### Performance Comparison
- Direct comparison of execution times
- Highlights Spark's in-memory processing advantage
- Shows scalability with dataset size

### Speedup Analysis
- Quantifies Spark's performance gain
- Values > 1.0 indicate Spark is faster
- Typically increases with dataset size

## Technical Details

### MapReduce Approach
- **Advantages**:
  - Well-understood paradigm
  - Excellent fault tolerance
  - Handles very large datasets
  - Suitable for batch processing

- **Limitations**:
  - Disk I/O overhead between stages
  - Fixed two-stage execution model
  - Higher latency

### Spark Approach
- **Advantages**:
  - In-memory processing (10-100x faster)
  - Flexible DAG execution
  - Rich API with transformations
  - Better for iterative algorithms

- **Limitations**:
  - Requires more memory
  - Not suitable for extremely large datasets beyond cluster memory

## Troubleshooting

### MapReduce Job Fails
```bash
# Check Hadoop logs
docker logs hadoop

# Verify HDFS connectivity
hadoop fs -ls /user/root/snap_datasets

# Check YARN ResourceManager
curl http://localhost:8088
```

### Spark Job Fails
```bash
# Check Spark logs
docker logs spark-master

# Verify Spark cluster
curl http://localhost:8080

# Check if master is accessible
spark-submit --version
```

### No Results Generated
```bash
# Verify containers are running
docker ps

# Check file permissions
ls -la /tmp/indegree_results/
ls -la /tmp/indegree_plots/

# Re-run with verbose output
python3 /scripts/run_indegree_experiments.py 2>&1 | tee experiment.log
```

## Dependencies

### Python Packages
- `mrjob` - MapReduce framework for Hadoop
- `pyspark` - Spark Python API
- `matplotlib` - Plotting and visualization
- `numpy` - Numerical computations

These are pre-installed in the Docker containers.

### Required Services
- Hadoop container (HDFS + YARN)
- Spark cluster (Master + Worker)
- Network connectivity between containers

## Educational Notes

This implementation is designed for educational purposes to demonstrate:

1. **MapReduce Programming Model**: The classic two-stage pattern
2. **Spark RDD Operations**: Transformations and actions
3. **Big Data Processing**: Handling large-scale graph data
4. **Performance Analysis**: Comparing different frameworks
5. **Network Science**: Understanding graph properties

The code is intentionally simple and well-commented to facilitate learning. In production scenarios, you might add:
- More sophisticated error handling
- Checkpoint mechanisms
- Dynamic resource allocation
- Advanced monitoring and metrics
- Optimization tuning for specific cluster configurations

## References

- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [SNAP Dataset Collection](https://snap.stanford.edu/data/)
- [Network Science: In-Degree Distribution](http://networksciencebook.com/)

## License

This code is part of the big-data-analytics educational project and is provided for learning purposes.
