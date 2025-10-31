# In-Degree Distribution Analysis Workflow

This document provides a visual guide to the complete workflow.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Environment                        │
│                                                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │    Hadoop       │  │  Spark Master   │  │  Spark Worker   │ │
│  │  HDFS + YARN    │  │   Port 8080     │  │   Port 8081     │ │
│  │  Ports 9870/8088│  │   Port 7077     │  │                 │ │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘ │
│           │                    │                     │          │
│           └────────────────────┴─────────────────────┘          │
│                              │                                   │
│                    bigdata-network                               │
└──────────────────────────────┼──────────────────────────────────┘
                               │
                    HDFS Storage Layer
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                       │
   email-EuAll           cit-Patents              soc-Pokec
    420K edges            16.5M edges            30.6M edges
```

## Complete Analysis Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│ Step 0: Prerequisites                                            │
├─────────────────────────────────────────────────────────────────┤
│ • Start services: make up                                        │
│ • Verify HDFS: make data-status                                 │
│ • Check containers: make ps                                      │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 1: Launch Analysis                                          │
├─────────────────────────────────────────────────────────────────┤
│ Command: make indegree-analysis-small                            │
│                                                                  │
│ Executes: docker exec hadoop bash run_complete_analysis.sh      │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 2: Run Experiments (run_indegree_experiments.py)           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  For each dataset:                                               │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 2A: MapReduce Job (indegree_mapreduce.py)                 │  │
│  ├───────────────────────────────────────────────────────────┤  │
│  │ Stage 1:                                                   │  │
│  │   Input:  hdfs://hadoop:9000/.../dataset.txt              │  │
│  │   Mapper:  (u,v) → (v, 1)                                 │  │
│  │   Reducer: (v, [1,1,1...]) → (v, degree)                  │  │
│  │                                                            │  │
│  │ Stage 2:                                                   │  │
│  │   Mapper:  (v, k) → (k, 1)                                │  │
│  │   Reducer: (k, [1,1,1...]) → (k, count)                   │  │
│  │                                                            │  │
│  │ Output: /tmp/indegree_results/mapreduce_dataset_*/        │  │
│  │ Metrics: Execution time, success/failure                   │  │
│  └───────────────────────────────────────────────────────────┘  │
│                               ↓                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 2B: Spark Job (indegree_spark.py)                         │  │
│  ├───────────────────────────────────────────────────────────┤  │
│  │ RDD Pipeline:                                              │  │
│  │   Input:  sc.textFile(hdfs_path)                          │  │
│  │   Step 1: edges.filter().map(split).map(dest)             │  │
│  │   Step 2: dest.map((n,1)).reduceByKey(sum)                │  │
│  │   Step 3: indeg.map((k,1)).reduceByKey(sum).sortByKey()   │  │
│  │                                                            │  │
│  │ Output: /tmp/indegree_results/spark_dataset_*/             │  │
│  │ Metrics: Execution time, success/failure                   │  │
│  └───────────────────────────────────────────────────────────┘  │
│                               ↓                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 2C: Correctness Verification                              │  │
│  ├───────────────────────────────────────────────────────────┤  │
│  │ Compare outputs:                                           │  │
│  │   mapreduce_results == spark_results ?                     │  │
│  │   → Pass/Fail for each dataset                            │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│ Save all results to JSON:                                        │
│   /tmp/indegree_results/experiment_results_<timestamp>.json     │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 3: Generate Analysis (analyze_indegree_results.py)         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 3A: Distribution Plots                                     │  │
│  ├───────────────────────────────────────────────────────────┤  │
│  │ For each dataset:                                          │  │
│  │   • Standard scatter plot (indegree vs count)             │  │
│  │   • Save as: indegree_dist_<dataset>_<impl>.png           │  │
│  └───────────────────────────────────────────────────────────┘  │
│                               ↓                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 3B: Log-Log Plots                                          │  │
│  ├───────────────────────────────────────────────────────────┤  │
│  │ For each dataset:                                          │  │
│  │   • Log-log scale plot (power-law analysis)               │  │
│  │   • Save as: indegree_loglog_<dataset>_<impl>.png         │  │
│  └───────────────────────────────────────────────────────────┘  │
│                               ↓                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 3C: Performance Comparison                                 │  │
│  ├───────────────────────────────────────────────────────────┤  │
│  │ • Bar chart: MapReduce vs Spark execution times           │  │
│  │ • Save as: performance_comparison.png                      │  │
│  └───────────────────────────────────────────────────────────┘  │
│                               ↓                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 3D: Speedup Analysis                                       │  │
│  ├───────────────────────────────────────────────────────────┤  │
│  │ • Bar chart: Spark speedup over MapReduce                 │  │
│  │ • Save as: speedup_analysis.png                            │  │
│  └───────────────────────────────────────────────────────────┘  │
│                               ↓                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ 3E: Generate Report                                        │  │
│  ├───────────────────────────────────────────────────────────┤  │
│  │ • Markdown report with all analysis                        │  │
│  │ • Embedded images                                          │  │
│  │ • Performance tables                                       │  │
│  │ • Save as: IN_DEGREE_ANALYSIS_REPORT.md                    │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                  │
│ All outputs saved to: /tmp/indegree_plots/                      │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 4: Package Deliverables (run_complete_analysis.sh)         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│ Create deliverables directory:                                  │
│   /tmp/indegree_deliverables_<timestamp>/                       │
│                                                                  │
│ Copy contents:                                                   │
│   ✓ Source files (*.py, *.sh)                                   │
│   ✓ Results (JSON data)                                         │
│   ✓ Plots (PNG images)                                          │
│   ✓ Report (Markdown)                                           │
│                                                                  │
│ Create ZIP archive:                                              │
│   /tmp/indegree_analysis_deliverables_<timestamp>.zip           │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 5: View & Download Results                                 │
├─────────────────────────────────────────────────────────────────┤
│ • View report:    make indegree-report                          │
│ • List results:   make indegree-results                         │
│ • Download ZIP:   make indegree-download                        │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow Detail

### MapReduce Data Flow

```
Input File (HDFS)
     │
     │ Each line: "source destination"
     │
     ↓
┌─────────────────────────────────────┐
│ Stage 1: Calculate In-Degrees       │
├─────────────────────────────────────┤
│ Map Phase:                          │
│   1 → 2    becomes    2 → 1         │
│   1 → 3    becomes    3 → 1         │
│   2 → 3    becomes    3 → 1         │
│   2 → 4    becomes    4 → 1         │
│                                     │
│ Shuffle & Sort: Group by node       │
│   2 → [1]                           │
│   3 → [1, 1]                        │
│   4 → [1]                           │
│                                     │
│ Reduce Phase: Sum counts            │
│   2 → 1                             │
│   3 → 2                             │
│   4 → 1                             │
└─────────────────────────────────────┘
     │
     │ Intermediate output: (node, in-degree)
     │
     ↓
┌─────────────────────────────────────┐
│ Stage 2: Calculate Distribution     │
├─────────────────────────────────────┤
│ Map Phase:                          │
│   2 → 1    becomes    1 → 1         │
│   3 → 2    becomes    2 → 1         │
│   4 → 1    becomes    1 → 1         │
│                                     │
│ Shuffle & Sort: Group by in-degree  │
│   1 → [1, 1]                        │
│   2 → [1]                           │
│                                     │
│ Reduce Phase: Count nodes           │
│   1 → 2    (2 nodes have indegree 1)│
│   2 → 1    (1 node has indegree 2) │
└─────────────────────────────────────┘
     │
     ↓
Final Output: (in-degree, node_count)
```

### Spark Data Flow

```
Input File (HDFS)
     │
     │ sc.textFile(hdfs_path)
     │
     ↓
RDD[String]: ["1 2", "1 3", "2 3", "2 4", ...]
     │
     │ .filter(not empty, not comment)
     │ .map(line => line.split())
     │ .filter(len >= 2)
     │ .map(parts => parts[1])
     │
     ↓
RDD[String]: ["2", "3", "3", "4", ...]  (destination nodes)
     │
     │ .map(node => (node, 1))
     │ .reduceByKey((a, b) => a + b)
     │
     ↓
RDD[(String, Int)]: [("2", 1), ("3", 2), ("4", 1), ...]  (in-degrees)
     │
     │ .map((node, count) => (count, 1))
     │ .reduceByKey((a, b) => a + b)
     │ .sortByKey()
     │
     ↓
RDD[(Int, Int)]: [(1, 2), (2, 1), ...]  (distribution)
     │
     │ .collect()
     │
     ↓
Final Output: [(in-degree, node_count), ...]
```

## Timeline Example (email-EuAll dataset)

```
0:00    Start: make indegree-analysis-small
        ├─ Container check
        └─ Script launch
        
0:05    MapReduce Job Start
        ├─ Stage 1: Map + Reduce (35s)
        ├─ Stage 2: Map + Reduce (30s)
        └─ Total: 65-85s
        
1:25    Spark Job Start
        ├─ Read + Transform (5s)
        ├─ Calculate in-degrees (10s)
        ├─ Calculate distribution (8s)
        └─ Total: 23-30s
        
1:55    Correctness Verification
        ├─ Load MapReduce output
        ├─ Load Spark output
        ├─ Compare results
        └─ Result: PASS ✓
        
2:00    Generate Visualizations
        ├─ Distribution plots (5s)
        ├─ Log-log plots (5s)
        ├─ Comparison charts (3s)
        └─ Generate report (2s)
        
2:15    Package Deliverables
        ├─ Copy files
        ├─ Create ZIP
        └─ Done!
        
2:20    Complete! Results ready for download
```

## Output Structure

```
/tmp/
├── indegree_results/
│   └── experiment_results_1234567890.json
│       {
│         "email-EuAll": {
│           "mapreduce": {
│             "success": true,
│             "execution_time": 85.3,
│             "results": {1: 50000, 2: 30000, ...}
│           },
│           "spark": {
│             "success": true,
│             "execution_time": 28.7,
│             "results": {1: 50000, 2: 30000, ...}
│           },
│           "correctness_verified": true
│         }
│       }
│
├── indegree_plots/
│   ├── indegree_dist_email-EuAll_spark.png
│   ├── indegree_loglog_email-EuAll_spark.png
│   ├── indegree_dist_email-EuAll_mapreduce.png
│   ├── indegree_loglog_email-EuAll_mapreduce.png
│   ├── performance_comparison.png
│   ├── speedup_analysis.png
│   └── IN_DEGREE_ANALYSIS_REPORT.md
│
└── indegree_analysis_deliverables_20251031_120000.zip
    ├── Source files/
    ├── indegree_results/
    └── indegree_plots/
```

## Quick Command Reference

```bash
# Full workflow in one command
make up && make indegree-analysis-small && make indegree-download

# Step by step
make up                          # Start services (30s)
make data-status                # Verify datasets
make indegree-analysis-small    # Run analysis (5-10 min)
make indegree-report            # View report
make indegree-download          # Get ZIP file

# Advanced
docker exec -it hadoop bash                              # Enter container
bash /scripts/run_complete_analysis.sh email-EuAll     # Manual run
ls -lh /tmp/indegree_plots/                            # List outputs
cat /tmp/indegree_plots/IN_DEGREE_ANALYSIS_REPORT.md   # View report
```

## Troubleshooting Flow

```
Problem: Analysis doesn't start
    ├─ Check: Are containers running?
    │   └─ Solution: make ps → make restart
    │
    ├─ Check: Are datasets in HDFS?
    │   └─ Solution: make data-status → make data-load
    │
    └─ Check: Docker memory sufficient?
        └─ Solution: Increase to 8GB+ in settings

Problem: Jobs fail
    ├─ Check Hadoop: docker logs hadoop
    ├─ Check Spark: docker logs spark-master
    └─ Retry with smaller dataset: make indegree-analysis-small

Problem: No results
    ├─ Check: /tmp/indegree_results/ exists?
    ├─ Check: Permissions correct?
    └─ Run with debug: bash -x /scripts/run_complete_analysis.sh

Problem: Can't download
    ├─ Check: ZIP file exists in /tmp/?
    └─ Manual copy: docker cp hadoop:/tmp/*.zip .
```

## Key Metrics to Watch

During execution, monitor:

1. **Execution Time**: How long each job takes
2. **Success/Failure**: Whether jobs complete successfully
3. **Correctness**: Whether results match between implementations
4. **Speedup**: How much faster Spark is than MapReduce
5. **Distribution**: Shape of the in-degree distribution curve

Expected speedup range: 2-8x (Spark vs MapReduce)
Expected correctness: 100% match between implementations

---

**Ready to start?** → `make up && make indegree-analysis-small`
