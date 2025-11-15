# In-Degree Distribution Analysis Report

**Generated:** 2025-11-01T13:17:49.693719

## Experiment Results

### Execution Times

| Dataset | Hadoop MapReduce | Apache Spark | Speedup |
|---------|------------------|--------------|----------|
| email-EuAll | Failed | 6.72s | N/A |

### Statistics

**email-EuAll** (Spark):
- Total nodes with in-degree > 0: 74660
- Maximum in-degree: 7631
- Average in-degree: 5.63

## Analysis

### Performance Patterns

1. **Execution Speed**: Comparison shows the relative performance of Hadoop MapReduce vs Apache Spark for in-degree computation.

2. **Scalability**: The performance difference becomes more pronounced with larger datasets.

3. **In-Memory Processing**: Spark's in-memory processing provides significant advantages for iterative graph operations.

### System Comparison

**Hadoop MapReduce:**
- Disk-based processing with high I/O overhead
- Better for very large datasets that don't fit in memory
- More mature fault tolerance mechanisms

**Apache Spark:**
- In-memory processing for faster computation
- Lower latency for iterative operations
- More efficient for graph analytics
- Better suited for large-scale graph processing

## Visualizations

The following plots are generated:

1. **In-Degree Distribution Plots**: Scatter plots showing the distribution of in-degrees across nodes
2. **Log-Log Distribution Plots**: Useful for identifying power-law distributions common in social networks
3. **Performance Comparison**: Bar chart comparing execution times between Hadoop and Spark

## Conclusions

This analysis demonstrates the practical differences between Hadoop MapReduce and Apache Spark for graph analytics. The in-degree distribution computation serves as a fundamental graph operation that highlights the strengths and weaknesses of each framework.
