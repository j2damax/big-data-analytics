# In-Degree Distribution Analysis

This directory contains implementations for computing in-degree distribution on graph datasets using both **Hadoop MapReduce** and **Apache Spark**.

## Overview

**In-degree** of a node in a directed graph is the number of incoming edges to that node. This analysis:
1. Computes the in-degree for each node in the graph
2. Generates the distribution (how many nodes have each in-degree value)
3. Compares performance between Hadoop and Spark implementations

## Files

- **`hadoop_indegree.py`**: Hadoop MapReduce implementation using mrjob
- **`spark_indegree.py`**: Apache Spark implementation using PySpark
- **`run_experiments.py`**: Automated experiment runner for multiple datasets
- **`visualize_results.py`**: Visualization and analysis script
- **`README.md`**: This file

## Prerequisites

### Required Software
- Docker containers running (Hadoop and Spark)
- Python 3.x
- Required Python packages: `mrjob`, `pyspark`, `matplotlib`, `numpy`

### Dataset Requirements
Data should be in HDFS at: `/user/root/snap_datasets/`
- `email-EuAll/email-EuAll.txt`
- `cit-Patents/cit-Patents.txt`
- `soc-Pokec/soc-pokec-relationships.txt`
- `soc-LiveJournal1/soc-LiveJournal1.txt`

## Usage

### 1. Individual Implementations

#### Hadoop MapReduce

Run on a single dataset:
```bash
# From inside Hadoop container
docker exec -it hadoop bash

# Basic usage - compute distribution
python3 /scripts/indegree_analysis/hadoop_indegree.py \
    -r hadoop \
    --hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    /user/root/snap_datasets/email-EuAll/email-EuAll.txt \
    --output-dir /user/root/output/hadoop_email_distribution

# Output individual node in-degrees
python3 /scripts/indegree_analysis/hadoop_indegree.py \
    -r hadoop \
    --hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    --output-indegree \
    /user/root/snap_datasets/email-EuAll/email-EuAll.txt \
    --output-dir /user/root/output/hadoop_email_indegree
```

#### Apache Spark

Run on a single dataset:
```bash
# From inside Spark container
docker exec -it spark-master bash

# Basic usage - compute distribution
spark-submit \
    --master local[*] \
    /scripts/indegree_analysis/spark_indegree.py \
    /user/root/snap_datasets/email-EuAll/email-EuAll.txt \
    --output /user/root/output/spark_email_distribution

# Output individual node in-degrees
spark-submit \
    --master local[*] \
    /scripts/indegree_analysis/spark_indegree.py \
    /user/root/snap_datasets/email-EuAll/email-EuAll.txt \
    --output-indegree \
    --output /user/root/output/spark_email_indegree

# Local file (for testing)
spark-submit \
    --master local[*] \
    /scripts/indegree_analysis/spark_indegree.py \
    /data/processed/email-EuAll.txt
```

### 2. Automated Experiments

Run experiments on multiple datasets:

```bash
# From host machine
cd /home/runner/work/big-data-analytics/big-data-analytics

# Run on all datasets (recommended to run from inside container)
docker exec -it hadoop python3 /scripts/indegree_analysis/run_experiments.py \
    --datasets all \
    --output-dir /scripts/indegree_analysis/results

# Run on specific datasets
docker exec -it hadoop python3 /scripts/indegree_analysis/run_experiments.py \
    --datasets email-EuAll cit-Patents soc-Pokec \
    --output-dir /scripts/indegree_analysis/results
```

### 3. Visualization and Analysis

Generate plots and analysis report:

```bash
# After experiments complete
python3 /scripts/indegree_analysis/visualize_results.py \
    --results /scripts/indegree_analysis/results/experiment_results.json \
    --output-dir /scripts/indegree_analysis/plots
```

This generates:
- Performance comparison bar charts
- In-degree distribution plots (scatter and log-log)
- Comprehensive analysis report (`ANALYSIS_REPORT.md`)

## Expected Output

### Hadoop MapReduce Output Format
```
degree1    count1
degree2    count2
...
```

### Spark Output Format
```
(degree1, count1)
(degree2, count2)
...
```

### Example Statistics
```
Spark In-Degree Analysis Results
================================================================
Input file: /user/root/snap_datasets/email-EuAll/email-EuAll.txt
Total nodes with in-degree > 0: 265214
Maximum in-degree: 7636
Average in-degree: 1.58
Number of unique degree values: 1383
Execution time: 12.45 seconds
================================================================
```

## Performance Metrics Collected

For each experiment, the following metrics are recorded:
1. **Execution Time**: Total time to complete the computation
2. **Memory Usage**: Peak memory consumption (if available)
3. **CPU Utilization**: Average CPU usage during execution
4. **Disk I/O**: Read/write operations (Hadoop logs)
5. **Network Overhead**: Shuffle operations (from framework logs)

## Datasets

### Small Dataset (Testing)
- **email-EuAll**: ~420K edges, ~265K nodes
- Good for quick testing and validation

### Medium Datasets
- **cit-Patents**: ~16.5M edges, ~3.8M nodes
- **soc-Pokec**: ~30.6M edges, ~1.6M nodes

### Large Dataset (Scalability Testing)
- **soc-LiveJournal1**: ~69M edges, ~4.8M nodes
- Tests scalability and performance bottlenecks

## Analysis Components

### Part 1: Implementation and Performance Comparison
✅ Hadoop MapReduce implementation
✅ Apache Spark implementation
✅ Experiments on multiple datasets (3+)
✅ Performance metrics collection
✅ Distribution plots (scatter and log-log)
✅ Comparative analysis

### Part 2: Scalability and Optimization
✅ Large dataset testing (soc-LiveJournal1)
✅ Performance metrics at scale
✅ Bottleneck identification
✅ Optimization suggestions
✅ Critical analysis report

## Optimization Techniques

### Hadoop Optimizations
- **Combiners**: Reduce data shuffling
- **Compression**: Enable intermediate data compression
- **Block Size**: Adjust HDFS block size for large files
- **Reducers**: Tune number of reduce tasks

### Spark Optimizations
- **Caching**: Cache RDDs for reuse
- **Partitioning**: Optimize data partitioning
- **Serialization**: Use Kryo serialization
- **Memory**: Adjust executor memory and cores

## Troubleshooting

### Common Issues

1. **HDFS Connection Failed**
   - Ensure Hadoop container is running
   - Check HDFS is formatted and started: `hdfs dfsadmin -report`

2. **Spark Job Fails**
   - Check memory settings in spark-defaults.conf
   - Reduce parallelism for small datasets

3. **mrjob Not Found**
   - Install in container: `pip3 install mrjob`

4. **Hadoop Streaming JAR Not Found**
   - Locate JAR: `find /opt/hadoop -name "*streaming*.jar"`
   - Update path in commands

## References

- **SNAP Datasets**: http://snap.stanford.edu/data/
- **Hadoop MapReduce**: https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/
- **Apache Spark**: https://spark.apache.org/docs/latest/
- **mrjob Documentation**: https://mrjob.readthedocs.io/

## Project Structure

```
indegree_analysis/
├── hadoop_indegree.py          # Hadoop MapReduce implementation
├── spark_indegree.py           # Spark implementation
├── run_experiments.py          # Automated experiment runner
├── visualize_results.py        # Visualization and analysis
├── README.md                   # This file
├── results/                    # Experiment results (generated)
│   ├── experiment_results.json
│   ├── hadoop_*_distribution/
│   └── spark_*_distribution/
└── plots/                      # Generated visualizations
    ├── performance_comparison.png
    ├── *_distribution.png
    ├── *_loglog.png
    └── ANALYSIS_REPORT.md
```

## Next Steps

1. Run experiments on all datasets
2. Analyze performance patterns
3. Generate visualizations
4. Write critical analysis
5. Apply optimizations
6. Re-run experiments with optimizations
7. Compare optimized vs unoptimized results

## Authors

This implementation is part of the Big Data Analytics course project demonstrating practical differences between Hadoop MapReduce and Apache Spark for graph analytics.
