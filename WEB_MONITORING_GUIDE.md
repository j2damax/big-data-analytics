# Big Data Performance Monitoring Guide

This guide shows how to monitor performance through web interfaces and integrate monitoring into your in-degree distribution analysis.

## Available Web Interfaces

### 1. Hadoop Monitoring
- **HDFS NameNode**: http://localhost:9870
  - File system status, disk usage, block information
  - Storage capacity and utilization
  - DataNode health and status

- **YARN ResourceManager**: http://localhost:8088
  - Cluster resource utilization (CPU, memory)
  - Running and completed applications
  - Job history and performance metrics
  - Queue status and scheduling information

- **NodeManager**: http://localhost:8042
  - Individual node resource usage
  - Container logs and metrics
  - Local application tracking

### 2. Spark Monitoring
- **Spark Master**: http://localhost:8080
  - Cluster overview and worker status
  - Running and completed applications
  - Resource allocation across workers

- **Spark Worker**: http://localhost:8081
  - Worker-specific metrics and logs
  - Executor information and resource usage

- **Spark Application UI**: http://localhost:4040 (when jobs are running)
  - Real-time job progress and stages
  - Task-level performance metrics
  - Storage and SQL query analysis

### 3. Flink Monitoring
- **Flink Dashboard**: http://localhost:8082
  - Job execution graphs and metrics
  - TaskManager resource utilization
  - Checkpoint and savepoint information

## Performance Monitoring Workflow

### Step 1: Pre-Execution Baseline
```bash
# Check system resources before running jobs
curl -s http://localhost:8088/ws/v1/cluster/metrics | jq '.clusterMetrics'
curl -s http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem | jq
```

### Step 2: Monitor During Execution
1. **Open monitoring dashboards in browser**:
   ```bash
   # macOS - open all monitoring interfaces
   open http://localhost:9870  # HDFS
   open http://localhost:8088  # YARN
   open http://localhost:8080  # Spark Master
   open http://localhost:8082  # Flink
   ```

2. **Watch resource utilization in real-time**
3. **Monitor job progress and performance metrics**

### Step 3: Post-Execution Analysis
Review completed job metrics through the web interfaces for optimization insights.

## Integration with In-Degree Analysis

### Enhanced Performance Monitoring

The in-degree distribution tools can be monitored through multiple interfaces:

1. **YARN Application Tracking**:
   - View Python processes as YARN applications
   - Monitor memory and CPU usage
   - Track execution time and resource allocation

2. **System Resource Monitoring**:
   - Watch cluster resource utilization during analysis
   - Monitor disk I/O for large dataset processing
   - Track memory usage patterns

3. **Comparative Analysis**:
   - Compare resource usage between different dataset sizes
   - Analyze scaling characteristics across frameworks

### Automated Monitoring Integration

For automated monitoring during in-degree analysis, you can:

1. **Capture metrics programmatically**:
   ```python
   import requests
   import json
   
   def get_yarn_metrics():
       response = requests.get('http://localhost:8088/ws/v1/cluster/metrics')
       return response.json()
   
   def get_hdfs_metrics():
       response = requests.get('http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem')
       return response.json()
   ```

2. **Log performance data**:
   - Capture before/after resource states
   - Track execution time vs resource usage
   - Generate performance reports

### Real-Time Monitoring Commands

```bash
# Monitor YARN applications
curl -s http://localhost:8088/ws/v1/cluster/apps | jq '.apps.app[] | select(.applicationType=="PYTHON")'

# Monitor HDFS usage
curl -s http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState | jq '.beans[0].CapacityUsed'

# Monitor system resources
curl -s http://localhost:8088/ws/v1/cluster/info | jq '.clusterInfo'
```

## Performance Analysis Dashboard

### Key Metrics to Monitor:

1. **Resource Utilization**:
   - Memory usage (heap and non-heap)
   - CPU utilization across cores
   - Disk I/O throughput
   - Network bandwidth usage

2. **Job Performance**:
   - Execution time per stage
   - Task completion rates
   - Data processing throughput (records/second)
   - Memory efficiency (GC patterns)

3. **Scalability Metrics**:
   - Performance vs dataset size
   - Resource scaling patterns
   - Bottleneck identification

### Monitoring Best Practices:

1. **Baseline Measurement**: Always capture baseline metrics before starting jobs
2. **Continuous Monitoring**: Keep web interfaces open during long-running jobs
3. **Historical Comparison**: Compare metrics across different runs and datasets
4. **Resource Optimization**: Use insights to tune memory and CPU allocations

## Quick Monitoring Setup

```bash
# Start all services with monitoring enabled
make up

# Open all monitoring interfaces (macOS)
make monitor-all    # We'll add this to Makefile

# Run in-degree analysis with monitoring
make indegree-email  # Monitor through web interfaces

# Check post-execution metrics
make metrics-summary  # We'll add this to Makefile
```

## Troubleshooting Monitoring Issues

1. **Port Conflicts**: Check if ports 8080, 8081, 8082, 8088, 9870 are available
2. **Service Status**: Verify all containers are running with `docker-compose ps`
3. **Network Access**: Ensure containers are on the same network
4. **Resource Limits**: Check if containers have sufficient memory allocation

This comprehensive monitoring approach provides both real-time visibility and historical analysis capabilities for your big data workloads.