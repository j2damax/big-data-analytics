# In-Degree Distribution - Quick Start Guide

This guide will help you quickly run the in-degree distribution analysis on the SNAP datasets.

## Prerequisites

1. **Docker containers must be running**:
   ```bash
   cd /home/runner/work/big-data-analytics/big-data-analytics
   docker compose up -d
   ```

2. **Data must be in HDFS**:
   According to the problem statement, data is already loaded. Verify:
   ```bash
   docker exec hadoop hdfs dfs -ls /user/root/snap_datasets/
   ```

## Option 1: Run Complete Automated Experiments (Recommended)

This will run both Hadoop and Spark on all datasets and generate a comparison report.

```bash
# Enter Hadoop container
docker exec -it hadoop bash

# Run experiments on all datasets
cd /scripts/indegree_analysis
python3 run_experiments.py --datasets all --output-dir results

# This will:
# - Run Hadoop MapReduce on each dataset
# - Run Spark on each dataset  
# - Collect performance metrics
# - Save results to results/experiment_results.json
```

**Expected time**: 
- Small datasets: ~5-10 minutes each
- Large dataset (LiveJournal): ~30-60 minutes

## Option 2: Run Individual Experiments

### Test on Small Dataset First (email-EuAll)

#### Hadoop MapReduce
```bash
docker exec -it hadoop bash

# Run in-degree distribution
python3 /scripts/indegree_analysis/hadoop_indegree.py \
  -r hadoop \
  --hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  /user/root/snap_datasets/email-EuAll/email-EuAll.txt \
  --output-dir /user/root/output/hadoop_email

# View results
hdfs dfs -cat /user/root/output/hadoop_email/part-* | head -20
```

#### Apache Spark
```bash
docker exec -it spark-master bash

# Run in-degree distribution
spark-submit \
  --master local[*] \
  /scripts/indegree_analysis/spark_indegree.py \
  /user/root/snap_datasets/email-EuAll/email-EuAll.txt

# With output to HDFS
spark-submit \
  --master local[*] \
  /scripts/indegree_analysis/spark_indegree.py \
  /user/root/snap_datasets/email-EuAll/email-EuAll.txt \
  --output /user/root/output/spark_email
```

### Run on Medium Dataset (cit-Patents)

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
  /user/root/snap_datasets/cit-Patents/cit-Patents.txt
```

### Run on Large Dataset (soc-LiveJournal1)

```bash
# Hadoop (may take 30-60 minutes)
docker exec hadoop python3 /scripts/indegree_analysis/hadoop_indegree.py \
  -r hadoop \
  --hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  /user/root/snap_datasets/soc-LiveJournal1/soc-LiveJournal1.txt \
  --output-dir /user/root/output/hadoop_livejournal

# Spark (may take 10-20 minutes)
docker exec spark-master spark-submit \
  --master local[*] \
  /scripts/indegree_analysis/spark_indegree.py \
  /user/root/snap_datasets/soc-LiveJournal1/soc-LiveJournal1.txt
```

## Step 3: Generate Visualizations

After running experiments:

```bash
# From host machine
cd /home/runner/work/big-data-analytics/big-data-analytics/scripts/indegree_analysis

# Install matplotlib if not already installed
pip3 install matplotlib numpy

# Generate plots and analysis
python3 visualize_results.py \
  --results results/experiment_results.json \
  --output-dir plots

# View generated files
ls -lh plots/
```

This creates:
- `performance_comparison.png` - Bar chart comparing Hadoop vs Spark
- `ANALYSIS_REPORT.md` - Comprehensive analysis report

## Step 4: Review Results

```bash
# View experiment results
cat results/experiment_results.json

# View analysis report
cat plots/ANALYSIS_REPORT.md

# View performance comparison
# (Transfer to local machine to view image)
```

## Expected Results

### Performance Expectations

| Dataset | Size | Hadoop Time | Spark Time | Speedup |
|---------|------|-------------|------------|---------|
| email-EuAll | 4.8 MB | ~30-60s | ~10-20s | ~3x |
| cit-Patents | 268 MB | ~5-10min | ~1-3min | ~3-5x |
| soc-Pokec | 404 MB | ~8-15min | ~2-5min | ~3-5x |
| soc-LiveJournal1 | 1.0 GB | ~20-40min | ~5-15min | ~3-5x |

*Note: Times vary based on hardware and configuration*

### Distribution Characteristics

**email-EuAll**:
- Max in-degree: ~7,000-8,000
- Average: ~1.5-2.0
- Distribution: Power-law (typical for communication networks)

**cit-Patents**:
- Max in-degree: ~500-1,000
- Average: ~4-5
- Distribution: Power-law with long tail

**soc-LiveJournal1**:
- Max in-degree: ~10,000+
- Average: ~10-15
- Distribution: Strong power-law (typical for social networks)

## Troubleshooting

### Issue: "Hadoop streaming JAR not found"
```bash
# Find the JAR file
docker exec hadoop find /opt/hadoop -name "*streaming*.jar"

# Use the full path in commands
```

### Issue: "HDFS connection refused"
```bash
# Check if Hadoop is running
docker exec hadoop jps

# Should see: NameNode, DataNode, ResourceManager, NodeManager

# If not, start Hadoop
docker exec hadoop /opt/hadoop/sbin/start-dfs.sh
docker exec hadoop /opt/hadoop/sbin/start-yarn.sh
```

### Issue: "Spark job fails with memory error"
```bash
# Increase executor memory
spark-submit \
  --master local[*] \
  --executor-memory 4G \
  --driver-memory 2G \
  /scripts/indegree_analysis/spark_indegree.py \
  [input_path]
```

### Issue: "mrjob not found"
```bash
# Install mrjob in Hadoop container
docker exec hadoop pip3 install mrjob
```

## Quick Test (Without Docker)

If you want to test the implementations locally without Docker:

```bash
cd /home/runner/work/big-data-analytics/big-data-analytics/scripts/indegree_analysis

# Run local test
python3 test_local.py
```

This creates sample data and tests the Hadoop implementation in local mode.

## Next Steps

1. ✅ Run experiments on 3+ datasets
2. ✅ Generate visualizations  
3. ✅ Review analysis report
4. 📝 Write your own critical analysis based on results
5. 🔧 Apply optimizations (see README.md)
6. 🔄 Re-run experiments with optimizations
7. 📊 Compare optimized vs baseline results

## Files Generated

```
indegree_analysis/
├── results/
│   ├── experiment_results.json          # Raw experiment data
│   ├── hadoop_*_distribution/           # Hadoop output directories
│   └── spark_*_distribution/            # Spark output directories
└── plots/
    ├── performance_comparison.png       # Bar chart
    └── ANALYSIS_REPORT.md              # Analysis report
```

## Getting Help

- Check `README.md` for detailed documentation
- Review `ANALYSIS_TEMPLATE.md` for report structure
- See example commands in comments within each script
- Check Hadoop/Spark web UIs for job details:
  - Hadoop: http://localhost:8088
  - Spark: http://localhost:8080 and http://localhost:4040 (during jobs)

## Tips for Success

1. **Start small**: Test on email-EuAll first
2. **Monitor resources**: Use `docker stats` to watch memory/CPU
3. **Check logs**: Review Hadoop/Spark logs for errors
4. **Be patient**: Large datasets take time
5. **Save results**: Experiment output is valuable for analysis

Good luck with your analysis! 🚀
