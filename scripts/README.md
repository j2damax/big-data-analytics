# Example Scripts

This directory contains Python example scripts for each big data technology.

## Scripts Overview

### 1. hadoop_wordcount.py
**Purpose**: Demonstrates Hadoop MapReduce using mrjob library

**Features**:
- Map function to emit word counts
- Reduce function to aggregate counts
- Simple word frequency analysis

**Usage**:
```bash
python3 hadoop_wordcount.py input.txt
```

### 2. hadoop_indegree.py
**Purpose**: Computes in-degree distribution for directed graphs using two-stage MapReduce

**Features**:
- Stage 1: Calculate individual node in-degrees
- Stage 2: Calculate distribution of in-degrees
- Handles large-scale graph datasets
- Skips comment lines (starting with #)

**Usage**:
```bash
# Local mode
python3 hadoop_indegree.py graph_edges.txt

# With HDFS
python3 hadoop_indegree.py hdfs:///user/root/snap_datasets/email-EuAll/email-EuAll.txt

# Using Makefile
make test-hadoop-indegree
```

**Input Format**: Each line represents a directed edge: `source destination`

**Output Format**: `(in_degree, count)` - number of nodes with each in-degree value

### 3. spark_example.py
**Purpose**: Demonstrates Apache Spark operations with PySpark

**Features**:
- RDD-based word count
- DataFrame operations
- SQL queries on DataFrames
- In-memory processing

**Usage**:
```bash
# Run locally
python3 spark_example.py

# Submit to Spark cluster
spark-submit --master spark://spark-master:7077 spark_example.py
```

### 4. kafka_example.py
**Purpose**: Demonstrates Kafka producer and consumer operations

**Features**:
- Topic creation
- Message production with JSON serialization
- Message consumption with consumer groups
- Timestamp-based message tracking

**Usage**:
```bash
python3 kafka_example.py
```

**Note**: Requires Kafka broker to be running at kafka:9092

### 5. flink_example.py
**Purpose**: Demonstrates Apache Flink stream processing

**Features**:
- DataStream API operations
- Table API queries
- Streaming word count
- Stateful computations

**Usage**:
```bash
# Run directly
python3 flink_example.py

# Submit to Flink cluster
flink run -py flink_example.py
```

## Running Scripts in Containers

### Hadoop
```bash
docker exec -it hadoop bash
cd /scripts
python3 hadoop_wordcount.py sample.txt
```

### Spark
```bash
docker exec -it spark-master bash
cd /scripts
spark-submit spark_example.py
```

### Kafka
```bash
docker exec -it kafka bash
cd /scripts
python3 kafka_example.py
```

### Flink
```bash
docker exec -it flink-jobmanager bash
cd /scripts
python3 flink_example.py
```

## Modifying Scripts

Feel free to modify these scripts to:
- Process your own data
- Implement custom transformations
- Experiment with different APIs
- Build end-to-end data pipelines

## Best Practices

1. **Error Handling**: Add try-except blocks for production code
2. **Configuration**: Externalize connection strings and parameters
3. **Logging**: Use proper logging instead of print statements
4. **Testing**: Write unit tests for your processing logic
5. **Documentation**: Comment complex logic and algorithms

## Integration Example

Build an end-to-end pipeline:
1. Generate data with a producer script
2. Send data to Kafka
3. Process with Flink or Spark
4. Store results in HDFS or database

## Learning Path

1. Start with basic examples in each technology
2. Modify parameters and observe behavior
3. Combine examples to build pipelines
4. Implement real-world use cases
5. Optimize for performance

## Additional Resources

See the README files in each technology directory (hadoop/, spark/, kafka/, flink/) for more detailed information and commands.
