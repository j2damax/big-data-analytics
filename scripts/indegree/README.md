# In-Degree Distribution Analysis Tools

This directory contains specialized tools for computing and analyzing in-degree distributions in directed graphs.

## Files

### `indegree_distribution.py`
**Main analysis tool** for computing in-degree distributions from graph edge lists.

**Features:**
- Pure Python implementation (no external dependencies)
- Processes large datasets efficiently using standard library
- Supports both file input and stdin
- Clean, readable output format

**Usage:**
```bash
# From file
python3 scripts/indegree/indegree_distribution.py data/processed/email-EuAll.txt

# From stdin (useful with HDFS)
hdfs dfs -cat /path/to/graph.txt | python3 scripts/indegree/indegree_distribution.py -
```

**Input Format:**
```
# Comments start with #
source_node destination_node
1 2
1 3
2 3
```

**Output:**
```
📊 In-Degree Distribution:
In-Degree       Node Count
--------------------
1       61936
2       6769
3       2012
...

📈 Summary:
Total nodes: 74,660
Max in-degree: 7631
```

### `performance_comparison.py`
**Performance benchmarking tool** for comparing different implementations.

**Usage:**
```bash
python3 scripts/indegree/performance_comparison.py data/processed/email-EuAll.txt
```

## Available Datasets

The project includes several SNAP datasets for experimentation:

| Dataset | Nodes | Edges | Type | Make Target |
|---------|--------|-------|------|-------------|
| email-EuAll | 74K | 365K | Email communication | `make indegree-email` |
| cit-Patents | 3.8M | 16M | Citation network | `make indegree-patents` |
| soc-pokec-relationships | 1.6M | 22M | Social network | `make indegree-pokec` |
| soc-LiveJournal1 | 4.8M | 69M | Social network | `make indegree-livejournal` |

## Quick Start

```bash
# Run on email dataset
make indegree-email

# Run on all datasets
make indegree-all

# Direct usage
python3 scripts/indegree/indegree_distribution.py data/processed/email-EuAll.txt

# Performance comparison
python3 scripts/indegree/performance_comparison.py data/processed/email-EuAll.txt
```

## Dependencies

**None required** - uses only Python standard library (`sys`, `collections`).

## Algorithm

The implementation uses a two-stage MapReduce-style approach:
1. **Count Stage**: Count in-degrees for each destination node
2. **Distribution Stage**: Count how many nodes have each in-degree value

Time complexity: O(E + N) where E = edges, N = nodes
Space complexity: O(N) for storing node counts