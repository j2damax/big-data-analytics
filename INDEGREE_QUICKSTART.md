# In-Degree Distribution Analysis - Quick Start

## What This Does

Compares Apache Hadoop MapReduce vs Apache Spark for computing in-degree distribution in large graphs.

**In-Degree**: Number of incoming edges to a node (how many connections point to it)

**Distribution**: How many nodes have 1 connection, 2 connections, 3 connections, etc.

## 5-Minute Quick Start

### 1. Start Services
```bash
cd big-data-analytics
make up
```
Wait ~30 seconds for services to initialize.

### 2. Run Analysis (Small Dataset)
```bash
make indegree-analysis-small
```
This will take 5-10 minutes and will:
- Run MapReduce implementation
- Run Spark implementation  
- Compare performance
- Generate plots
- Create report

### 3. View Results
```bash
# View the report
make indegree-report

# Download all files
make indegree-download
```

## What You Get

After running the analysis, you'll have:

### 📊 Visualizations
- **Distribution Plots**: Shows how many nodes have each in-degree
- **Log-Log Plots**: Reveals power-law patterns (common in social networks)
- **Performance Charts**: MapReduce vs Spark execution times
- **Speedup Analysis**: How much faster Spark is

### 📄 Report
- Complete analysis in markdown format
- Performance metrics and comparisons
- Embedded visualizations
- Key findings and conclusions

### 📦 Deliverables ZIP
- All source code
- Experimental results (JSON)
- All visualizations (PNG)
- Comprehensive report

## Understanding the Output

### Example Results

**In-Degree Distribution:**
```
In-Degree  |  Number of Nodes
-----------|------------------
    1      |      50,000
    2      |      30,000
    3      |      20,000
    4      |      15,000
   ...     |       ...
```

**Performance Comparison:**
```
Dataset: email-EuAll (~420K edges)
- MapReduce: 85 seconds
- Spark:     28 seconds
- Speedup:   3.0x (Spark is 3x faster)
```

### What the Plots Show

1. **Distribution Plot**: Most nodes have few connections, few nodes have many
2. **Log-Log Plot**: Straight line = power-law (scale-free network)
3. **Performance**: Spark consistently faster due to in-memory processing
4. **Speedup**: Increases with dataset size (larger = better Spark advantage)

## Run on Different Datasets

### Small Dataset (5-10 minutes)
```bash
make indegree-analysis-small
# email-EuAll: ~420K edges
```

### Multiple Datasets (30-60 minutes)
```bash
docker exec hadoop bash /scripts/run_complete_analysis.sh email-EuAll cit-Patents
# email-EuAll + cit-Patents: ~17M edges total
```

### All Datasets (1-2 hours)
```bash
make indegree-analysis
# All 4 datasets: ~116M edges total
```

## File Locations

Inside containers, results are at:
```
/tmp/indegree_results/     - JSON data
/tmp/indegree_plots/       - PNG images + report
/tmp/indegree_analysis_deliverables_*.zip  - Complete package
```

To download:
```bash
make indegree-download
```

## Troubleshooting

### Services Not Running
```bash
make ps              # Check status
make restart         # Restart services
```

### No Datasets in HDFS
```bash
make data-status     # Check datasets
make data-load       # Load datasets
```

### Analysis Fails
```bash
make logs            # View logs
make indegree-test   # Test locally first
```

### Out of Memory
```bash
# Increase Docker memory to 8GB+ in settings
# OR run smaller dataset only
make indegree-analysis-small
```

## Next Steps

1. ✅ **Review the report**: Understand the analysis
2. 📊 **Study the plots**: See the distribution patterns
3. 🔍 **Compare performance**: MapReduce vs Spark differences
4. 📖 **Read the full guide**: `IN_DEGREE_ANALYSIS_GUIDE.md`
5. 🧪 **Experiment**: Try different datasets, modify code

## Key Commands

```bash
# Core workflow
make up                         # Start everything
make indegree-analysis-small   # Run analysis
make indegree-report           # View report
make indegree-download         # Get deliverables

# Testing & debugging
make indegree-test             # Local test
make indegree-results          # Check results
make logs                      # View logs

# Manual execution
docker exec -it hadoop bash
bash /scripts/run_complete_analysis.sh email-EuAll
```

## Implementation Details (Brief)

### MapReduce (2 Stages)
1. **Stage 1**: Count incoming edges per node
2. **Stage 2**: Count nodes per in-degree value
- Uses disk for intermediate data
- Slower but handles unlimited data size

### Spark (In-Memory)
1. Read edges → Extract destinations
2. Group by node → Count connections
3. Group by count → Count nodes
- Keeps data in memory
- 3-10x faster for this workload

## Performance Expectations

| Dataset | Edges | MapReduce | Spark | Speedup |
|---------|-------|-----------|-------|---------|
| email-EuAll | 420K | ~80s | ~25s | ~3x |
| cit-Patents | 16.5M | ~400s | ~80s | ~5x |
| soc-Pokec | 30.6M | ~800s | ~150s | ~5x |
| soc-LiveJournal | 69M | ~1600s | ~250s | ~6x |

*Times vary based on hardware*

## Questions?

- 📖 Full guide: `IN_DEGREE_ANALYSIS_GUIDE.md`
- 📁 Detailed README: `scripts/INDEGREE_ANALYSIS_README.md`
- 🐛 Issues: Check logs with `make logs`
- 💡 Examples: See test files in `scripts/`

---

**Ready to start?**
```bash
make up && make indegree-analysis-small
```
