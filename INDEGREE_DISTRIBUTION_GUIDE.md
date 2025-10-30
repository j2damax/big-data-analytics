# In-Degree Distribution Computation Guide

## Overview

This guide demonstrates how to compute in-degree distribution for large-scale directed graph datasets using the two-stage MapReduce paradigm implemented in `scripts/hadoop_indegree.py`.

## What is In-Degree Distribution?

In a directed graph:
- **In-degree** of a node is the number of edges pointing to it
- **In-degree distribution** shows how many nodes have each in-degree value

For example, if the output is:
```
1    1000
2    500
3    100
```
This means:
- 1000 nodes have in-degree 1 (1000 nodes each have 1 incoming edge)
- 500 nodes have in-degree 2 (500 nodes each have 2 incoming edges)
- 100 nodes have in-degree 3 (100 nodes each have 3 incoming edges)

## Implementation Details

The computation uses a **two-stage MapReduce pipeline**:

### Stage 1: Calculate Individual Node In-Degrees

**Mapper**: 
- Reads each edge `(u,v)` from the input file
- Outputs the destination node `v` as the key and value `1`
- Format: `(Destination Node, 1)`

**Reducer**: 
- Groups by destination node
- Receives `(Destination Node, list of 1s)`
- Sums the list of 1s to get in-degree
- Format: `(Destination Node, In-Degree Count k)`

### Stage 2: Calculate the Distribution

**Mapper**: 
- Reads output from Stage 1: `(Node, In-Degree Count k)`
- Outputs the in-degree count `k` as the key and value `1`
- Format: `(In-Degree Count k, 1)`

**Reducer**: 
- Groups by in-degree count
- Sums the list of 1s to count nodes
- Format: `(In-Degree k, Number of Nodes with In-Degree k)` ✅ **FINAL OUTPUT**

## Dataset Format

### Input Format
Each line represents a directed edge from source to destination:
```
source destination
```

Example:
```
# Comments starting with # are ignored
1 2
1 3
2 3
3 4
```

### Output Format
```
<in-degree>    <node-count>
```

## Usage Examples

### 1. Local Mode (for testing)

```bash
# Create a test file
cat > /tmp/test_graph.txt << 'EOF'
1 2
1 3
2 3
3 4
EOF

# Run the MapReduce job
python3 scripts/hadoop_indegree.py /tmp/test_graph.txt
```

Expected output:
```
1    2    # 2 nodes (nodes 2 and 4) have in-degree 1
2    1    # 1 node (node 3) has in-degree 2
```

### 2. With HDFS Datasets

The repository already has 4 large-scale graph datasets loaded in HDFS:

```
hdfs://hadoop:9000/user/root/snap_datasets/
├── soc-Pokec/
│   └── soc-pokec-relationships.txt      (30.6M edges)
├── email-EuAll/
│   └── email-EuAll.txt                  (420K edges)
├── cit-Patents/
│   └── cit-Patents.txt                  (16.5M edges)
└── soc-LiveJournal1/
    └── soc-LiveJournal1.txt             (69M edges)
```

#### Using Makefile (Recommended)

```bash
# Start Hadoop container
make hadoop

# Run in-degree distribution on email-EuAll dataset
make test-hadoop-indegree
```

#### Using Docker Directly

```bash
# Run on email-EuAll (smallest dataset - good for testing)
docker exec hadoop python3 /scripts/hadoop_indegree.py \
    hdfs:///user/root/snap_datasets/email-EuAll/email-EuAll.txt

# Run on cit-Patents (medium size)
docker exec hadoop python3 /scripts/hadoop_indegree.py \
    hdfs:///user/root/snap_datasets/cit-Patents/cit-Patents.txt

# Run on soc-Pokec (large dataset)
docker exec hadoop python3 /scripts/hadoop_indegree.py \
    hdfs:///user/root/snap_datasets/soc-Pokec/soc-pokec-relationships.txt

# Run on soc-LiveJournal1 (largest dataset)
docker exec hadoop python3 /scripts/hadoop_indegree.py \
    hdfs:///user/root/snap_datasets/soc-LiveJournal1/soc-LiveJournal1.txt
```

### 3. Saving Output to File

```bash
# Save output locally
python3 scripts/hadoop_indegree.py /tmp/test_graph.txt > indegree_dist.txt

# Save output from HDFS dataset
docker exec hadoop python3 /scripts/hadoop_indegree.py \
    hdfs:///user/root/snap_datasets/email-EuAll/email-EuAll.txt \
    > email_indegree_distribution.txt
```

### 4. Using with Hadoop Streaming (Alternative)

For running on actual Hadoop cluster with YARN:

```bash
# Inside Hadoop container
docker exec -it hadoop bash

# Run with Hadoop streaming
python3 /scripts/hadoop_indegree.py \
    -r hadoop \
    hdfs:///user/root/snap_datasets/email-EuAll/email-EuAll.txt \
    --output-dir hdfs:///user/root/output/indegree_email
```

## Performance Considerations

### Processing Time Estimates (Local Mode)

| Dataset | Edges | Approx. Time |
|---------|-------|--------------|
| email-EuAll | 420K | < 1 minute |
| cit-Patents | 16.5M | 5-10 minutes |
| soc-Pokec | 30.6M | 10-20 minutes |
| soc-LiveJournal1 | 69M | 20-40 minutes |

*Note: Times are estimates for local mode. Using actual Hadoop cluster with YARN will be faster for large datasets.*

### Optimization Tips

1. **Use Hadoop Cluster Mode**: For datasets > 10M edges, use `-r hadoop` flag to leverage distributed processing
2. **Increase Memory**: For very large datasets, adjust JVM heap size in Hadoop configuration
3. **Monitor Progress**: Use Hadoop Web UI (http://localhost:8088) to monitor job progress
4. **Partition Data**: For extremely large datasets, consider processing in batches

## Interpreting Results

### Understanding the Output

The output shows the power-law distribution typical in real-world networks:

```
1    45000    # Many nodes have only 1 incoming edge
2    20000    # Fewer nodes have 2 incoming edges
3    10000    # Even fewer have 3
...
100  50       # Very few nodes have 100 incoming edges
```

### Analysis Insights

- **Low in-degree nodes**: Represent leaf nodes or less popular entities
- **High in-degree nodes**: Represent hub nodes or popular entities
- **Distribution shape**: Most real-world networks show power-law distribution
  - Many nodes with low in-degree
  - Few nodes with very high in-degree (hubs)

## Troubleshooting

### Common Issues

1. **Module not found: mrjob**
   ```bash
   pip3 install mrjob
   ```

2. **HDFS connection failed**
   ```bash
   # Check Hadoop is running
   docker ps | grep hadoop
   
   # Verify HDFS files exist
   docker exec hadoop hdfs dfs -ls /user/root/snap_datasets/
   ```

3. **Out of memory**
   - Use smaller dataset for testing
   - Increase Docker memory allocation
   - Use Hadoop cluster mode instead of local mode

4. **Slow performance**
   - Use `-r hadoop` for distributed processing
   - Check system resources with `docker stats`

## Next Steps

After computing in-degree distribution, you can:

1. **Visualize the Distribution**: Plot log-log graph to see power-law
2. **Find Hub Nodes**: Filter for nodes with high in-degree
3. **Network Analysis**: Compare distributions across different datasets
4. **Page Rank**: Use in-degree as input for PageRank algorithms
5. **Anomaly Detection**: Identify unusual patterns in the distribution

## References

- SNAP Datasets: http://snap.stanford.edu/data/
- MapReduce Paper: https://research.google/pubs/pub62/
- mrjob Documentation: https://mrjob.readthedocs.io/

## Code Location

- **Script**: `scripts/hadoop_indegree.py`
- **Documentation**: `scripts/README.md`
- **Makefile targets**: `make test-hadoop-indegree`
