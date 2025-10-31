# In-Degree Distribution - Complete Usage Examples

This document provides **copy-paste ready commands** for running the in-degree analysis.

## Prerequisites Check

```bash
# 1. Navigate to project directory
cd /home/runner/work/big-data-analytics/big-data-analytics

# 2. Start Docker containers
docker compose up -d

# 3. Wait for services to be ready (30 seconds)
sleep 30

# 4. Verify Hadoop is running
docker exec hadoop jps
# Should see: NameNode, DataNode, ResourceManager, NodeManager

# 5. Verify data in HDFS
docker exec hadoop hdfs dfs -ls /user/root/snap_datasets/
# Should see: cit-Patents, email-EuAll, soc-LiveJournal1, soc-Pokec

# 6. Check HDFS data sizes (from problem statement)
docker exec hadoop hdfs dfs -du -h /user/root/snap_datasets/
```

Expected output:
```
267.5 M    /user/root/snap_datasets/cit-Patents
4.8 M      /user/root/snap_datasets/email-EuAll
1.0 G      /user/root/snap_datasets/soc-LiveJournal1
404.3 M    /user/root/snap_datasets/soc-Pokec
```

## Quick Start: Test on Small Dataset (5 minutes)

### Option 1: Using Makefile (Easiest)

```bash
# Test implementations locally (no Docker needed)
make indegree-test

# Run Hadoop on email-EuAll
make indegree-hadoop

# Run Spark on email-EuAll
make indegree-spark
```

### Option 2: Direct Commands

#### Hadoop MapReduce on email-EuAll
```bash
docker exec -it hadoop bash -c "
python3 /scripts/indegree_analysis/hadoop_indegree.py \
  -r hadoop \
  --hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  /user/root/snap_datasets/email-EuAll/email-EuAll.txt \
  --output-dir /user/root/output/hadoop_email_test
"
```

#### Apache Spark on email-EuAll
```bash
docker exec -it spark-master bash -c "
spark-submit \
  --master local[*] \
  /scripts/indegree_analysis/spark_indegree.py \
  /user/root/snap_datasets/email-EuAll/email-EuAll.txt
"
```

Expected output from Spark:
```
============================================================
Spark In-Degree Analysis Results
============================================================
Input file: /user/root/snap_datasets/email-EuAll/email-EuAll.txt
Total nodes with in-degree > 0: 265214
Maximum in-degree: ~7000-8000
Average in-degree: ~1.5-2.0
Execution time: 10-20 seconds
============================================================
```

## Run on Multiple Datasets

### Test Dataset (email-EuAll - 4.8 MB, ~30-60 seconds)

```bash
# Hadoop
docker exec hadoop python3 /scripts/indegree_analysis/hadoop_indegree.py \
  -r hadoop \
  --hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  /user/root/snap_datasets/email-EuAll/email-EuAll.txt \
  --output-dir /user/root/output/hadoop_email

# Spark
docker exec spark-master spark-submit \
  --master local[*] \
  /scripts/indegree_analysis/spark_indegree.py \
  /user/root/snap_datasets/email-EuAll/email-EuAll.txt \
  --output /user/root/output/spark_email
```

### Medium Dataset (cit-Patents - 268 MB, ~5-10 minutes)

```bash
# Hadoop
docker exec hadoop python3 /scripts/indegree_analysis/hadoop_indegree.py \
  -r hadoop \
  --hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  /user/root/snap_datasets/cit-Patents/cit-Patents.txt \
  --output-dir /user/root/output/hadoop_patents

# Spark
docker exec spark-master spark-submit \
  --master local[*] \
  /scripts/indegree_analysis/spark_indegree.py \
  /user/root/snap_datasets/cit-Patents/cit-Patents.txt \
  --output /user/root/output/spark_patents
```

### Medium-Large Dataset (soc-Pokec - 404 MB, ~8-15 minutes)

```bash
# Hadoop
docker exec hadoop python3 /scripts/indegree_analysis/hadoop_indegree.py \
  -r hadoop \
  --hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  /user/root/snap_datasets/soc-Pokec/soc-pokec-relationships.txt \
  --output-dir /user/root/output/hadoop_pokec

# Spark
docker exec spark-master spark-submit \
  --master local[*] \
  /scripts/indegree_analysis/spark_indegree.py \
  /user/root/snap_datasets/soc-Pokec/soc-pokec-relationships.txt \
  --output /user/root/output/spark_pokec
```

### Large Dataset (soc-LiveJournal1 - 1.0 GB, ~20-40 minutes)

```bash
# Hadoop (may take 20-40 minutes)
docker exec hadoop python3 /scripts/indegree_analysis/hadoop_indegree.py \
  -r hadoop \
  --hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  /user/root/snap_datasets/soc-LiveJournal1/soc-LiveJournal1.txt \
  --output-dir /user/root/output/hadoop_livejournal

# Spark (may take 5-15 minutes)
docker exec spark-master spark-submit \
  --master local[*] \
  /scripts/indegree_analysis/spark_indegree.py \
  /user/root/snap_datasets/soc-LiveJournal1/soc-LiveJournal1.txt \
  --output /user/root/output/spark_livejournal
```

## Automated Experiments (Complete Solution)

Run all datasets automatically with performance metrics:

```bash
# Enter Hadoop container
docker exec -it hadoop bash

# Navigate to scripts directory
cd /scripts/indegree_analysis

# Run experiments on all datasets (1-2 hours total)
python3 run_experiments.py --datasets all --output-dir results

# Or run on specific datasets
python3 run_experiments.py \
  --datasets email-EuAll cit-Patents soc-Pokec \
  --output-dir results

# Exit container
exit
```

This will:
1. Run Hadoop MapReduce on each dataset
2. Run Spark on each dataset
3. Collect all performance metrics
4. Save results to `results/experiment_results.json`
5. Print summary comparison

Expected output structure:
```
EXPERIMENT SUMMARY
============================================================
Hadoop MapReduce Results:
  ✓ email-EuAll        45.23s
  ✓ cit-Patents       312.45s
  ✓ soc-Pokec         567.89s
  ✓ soc-LiveJournal1  1234.56s

Apache Spark Results:
  ✓ email-EuAll        12.34s
  ✓ cit-Patents        89.01s
  ✓ soc-Pokec         156.78s
  ✓ soc-LiveJournal1  234.56s

Performance Comparison:
Dataset              Hadoop (s)    Spark (s)    Speedup
------------------------------------------------------------
email-EuAll               45.23        12.34      3.67x
cit-Patents              312.45        89.01      3.51x
soc-Pokec                567.89       156.78      3.62x
soc-LiveJournal1        1234.56       234.56      5.26x
============================================================
```

## Generate Visualizations

After running experiments:

```bash
# Install matplotlib if needed (on host machine)
pip3 install matplotlib

# Generate plots and analysis report
cd scripts/indegree_analysis
python3 visualize_results.py \
  --results results/experiment_results.json \
  --output-dir plots
```

Or using Makefile:
```bash
make indegree-visualize
```

This creates:
- `plots/performance_comparison.png` - Bar chart
- `plots/ANALYSIS_REPORT.md` - Comprehensive report

## View Results

```bash
# View experiment results JSON
cat scripts/indegree_analysis/results/experiment_results.json

# View analysis report
cat scripts/indegree_analysis/plots/ANALYSIS_REPORT.md

# View Hadoop output (first 20 lines)
docker exec hadoop hdfs dfs -cat /user/root/output/hadoop_email/part-* | head -20

# View Spark output (first 20 lines)
docker exec spark-master hdfs dfs -cat /user/root/output/spark_email/part-* | head -20
```

## Monitor Running Jobs

### Hadoop Web UIs

```bash
# HDFS NameNode
open http://localhost:9870

# YARN ResourceManager (job tracking)
open http://localhost:8088

# NodeManager
open http://localhost:8042
```

### Spark Web UIs

```bash
# Spark Master
open http://localhost:8080

# Spark Job UI (while job is running)
open http://localhost:4040
```

### Check Job Status

```bash
# Hadoop: Check YARN applications
docker exec hadoop yarn application -list

# Spark: Check running applications
docker exec spark-master spark-class org.apache.spark.deploy.Client \
  --master spark://spark-master:7077 \
  list
```

## Common Operations

### Get Node In-Degrees (Not Distribution)

```bash
# Hadoop
docker exec hadoop python3 /scripts/indegree_analysis/hadoop_indegree.py \
  -r hadoop \
  --hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  --output-indegree \
  /user/root/snap_datasets/email-EuAll/email-EuAll.txt \
  --output-dir /user/root/output/hadoop_email_nodes

# Spark
docker exec spark-master spark-submit \
  --master local[*] \
  /scripts/indegree_analysis/spark_indegree.py \
  /user/root/snap_datasets/email-EuAll/email-EuAll.txt \
  --output-indegree \
  --output /user/root/output/spark_email_nodes
```

### Clean Output Directories

```bash
# Remove previous outputs
docker exec hadoop hdfs dfs -rm -r /user/root/output/hadoop_*
docker exec hadoop hdfs dfs -rm -r /user/root/output/spark_*

# Clean local results
rm -rf scripts/indegree_analysis/results/*
rm -rf scripts/indegree_analysis/plots/*
```

### Check Disk Space

```bash
# HDFS usage
docker exec hadoop hdfs dfs -df -h

# Container disk usage
docker exec hadoop df -h

# Local disk usage
df -h
```

## Performance Optimization Examples

### Hadoop with Optimizations

```bash
docker exec hadoop python3 /scripts/indegree_analysis/hadoop_indegree.py \
  -r hadoop \
  --hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.reduces=8 \
  -D mapreduce.output.fileoutputformat.compress=true \
  -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec \
  /user/root/snap_datasets/cit-Patents/cit-Patents.txt \
  --output-dir /user/root/output/hadoop_patents_optimized
```

### Spark with More Memory

```bash
docker exec spark-master spark-submit \
  --master local[*] \
  --executor-memory 4G \
  --driver-memory 2G \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  /scripts/indegree_analysis/spark_indegree.py \
  /user/root/snap_datasets/cit-Patents/cit-Patents.txt \
  --output /user/root/output/spark_patents_optimized
```

## Troubleshooting Commands

### Check if mrjob is Installed

```bash
docker exec hadoop pip3 list | grep mrjob
# If not found:
docker exec hadoop pip3 install mrjob
```

### Check Hadoop Services

```bash
# Check all Java processes
docker exec hadoop jps

# Check HDFS health
docker exec hadoop hdfs dfsadmin -report

# Check YARN nodes
docker exec hadoop yarn node -list
```

### Check Spark Status

```bash
# Check Spark master
docker exec spark-master ps aux | grep spark

# Check Spark worker
docker exec spark-worker ps aux | grep spark
```

### View Logs

```bash
# Hadoop YARN logs (replace application_id)
docker exec hadoop yarn logs -applicationId application_1234567890_0001

# Spark logs
docker logs spark-master
docker logs spark-worker

# Container logs
docker compose logs -f hadoop
docker compose logs -f spark-master
```

## Complete Workflow Example

Here's a complete end-to-end example:

```bash
# 1. Start containers
cd /home/runner/work/big-data-analytics/big-data-analytics
docker compose up -d
sleep 30

# 2. Verify data
docker exec hadoop hdfs dfs -du -h /user/root/snap_datasets/

# 3. Test locally first
make indegree-test

# 4. Run quick test on small dataset
make indegree-hadoop
make indegree-spark

# 5. Run complete experiments (1-2 hours)
docker exec hadoop python3 /scripts/indegree_analysis/run_experiments.py \
  --datasets all \
  --output-dir /scripts/indegree_analysis/results

# 6. Generate visualizations
cd scripts/indegree_analysis
pip3 install matplotlib
python3 visualize_results.py \
  --results results/experiment_results.json \
  --output-dir plots

# 7. Review results
cat plots/ANALYSIS_REPORT.md
ls -lh plots/

# 8. Write your analysis using ANALYSIS_TEMPLATE.md as guide
cp ANALYSIS_TEMPLATE.md MY_ANALYSIS.md
# Edit MY_ANALYSIS.md with your findings
```

## Summary of Key Commands

```bash
# Quick tests
make indegree-test           # Local test
make indegree-hadoop         # Hadoop on small dataset
make indegree-spark          # Spark on small dataset

# Full experiments
make indegree-experiments    # All datasets, both frameworks

# Visualizations
make indegree-visualize      # Generate plots and report

# Direct execution (inside containers)
# Hadoop:
python3 /scripts/indegree_analysis/hadoop_indegree.py -r hadoop ...
# Spark:
spark-submit /scripts/indegree_analysis/spark_indegree.py ...
```

## Time Estimates

| Operation | Time |
|-----------|------|
| Local test | < 1 minute |
| email-EuAll (both) | 2-3 minutes |
| cit-Patents (both) | 8-15 minutes |
| soc-Pokec (both) | 12-25 minutes |
| soc-LiveJournal1 (both) | 30-60 minutes |
| **Total (all datasets)** | **1-2 hours** |
| Visualization | 1-2 minutes |

## Next Steps

1. ✅ Run tests to verify setup
2. ✅ Start with small dataset
3. ✅ Run complete experiments
4. ✅ Generate visualizations
5. 📝 Write critical analysis
6. 🔧 Apply optimizations (optional)
7. 🔄 Re-run with optimizations (optional)
8. 📊 Compare results

Happy analyzing! 🚀
