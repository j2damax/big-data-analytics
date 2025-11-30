# ⚡ Apache Spark Cluster

**Apache Spark 3.5.0** - Lightning-fast unified analytics engine for large-scale data processing with in-memory computing, SQL analytics, streaming, and machine learning.

## 🏗️ **Cluster Architecture**

| Component | Role | Container | Resources |
|-----------|------|-----------|-----------|
| **Spark Master** | Cluster coordinator & job scheduler | `spark-master:7077` | Manages workers, distributes tasks |
| **Spark Worker** | Task executor with in-memory storage | `spark-worker:8081` | 1GB memory, 1 CPU core allocated |
| **Driver Program** | Application entry point | Your code/shell | Coordinates job execution |
| **Executor** | JVM process running tasks | Within worker | Actual computation and data caching |

## 🚀 **Unified Processing Engine**

### **Core Capabilities**
- **⚡ In-Memory Processing**: 100x faster than disk-based systems for iterative algorithms
- **🔄 Unified API**: Same code works for batch, streaming, ML, and graph processing  
- **📊 SQL Analytics**: ANSI SQL support with Catalyst optimizer
- **🧠 MLlib**: Distributed machine learning library
- **🌐 GraphX**: Graph processing and graph-parallel computation
- **🔥 Streaming**: Micro-batch and continuous processing modes

## 🚀 **Quick Start**

```bash
# Access Spark cluster
make shell-spark  # Interactive PySpark shell

# Test cluster immediately
make test-spark   # Run comprehensive example

# Web interfaces:
# 🌐 Master UI: http://localhost:8080 (cluster status, applications)  
# 🌐 Worker UI: http://localhost:8081 (executor details, logs)
# 🌐 Application UI: http://localhost:4040 (when job running)
```

## 💻 **Interactive Development**

### **PySpark Shell Sessions**
```bash
# Start interactive PySpark
pyspark --master spark://spark-master:7077

# Or use the convenience command
make shell-spark
```

```python
# Quick DataFrame operations
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

# Spark context is pre-configured as 'spark'
df = spark.range(1000000).toDF("number")
df.filter(col("number") % 2 == 0).count()  # Count even numbers

# SQL interface
df.createOrReplaceTempView("numbers")
spark.sql("SELECT COUNT(*) FROM numbers WHERE number > 500000").show()
```

### **Scala Shell (Advanced)**
```bash
# Scala shell for performance-critical code
spark-shell --master spark://spark-master:7077
```

## 📊 **Data Processing Patterns**

### **DataFrame API (Recommended)**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DataAnalysis").getOrCreate()

# Read data (supports JSON, CSV, Parquet, Delta Lake)
df = spark.read.json("/shared/data/events.json")

# Transformations (lazy evaluation)
result = df.filter(col("status") == "active") \
          .groupBy("category") \
          .agg(count("*").alias("total"), 
               avg("amount").alias("avg_amount")) \
          .orderBy(desc("total"))

# Actions (trigger computation)  
result.show(20, truncate=False)
result.write.parquet("/shared/output/category_stats.parquet")
```

### **RDD API (Low-Level)**
```python
# When you need fine-grained control
rdd = spark.sparkContext.parallelize(range(1000000))
squared_rdd = rdd.map(lambda x: x ** 2)
even_squares = squared_rdd.filter(lambda x: x % 2 == 0)
result = even_squares.collect()  # Bring to driver
```

### **Spark SQL (Familiar Syntax)**
```python
# Register DataFrame as SQL table
df.createOrReplaceTempView("transactions")

# Complex analytics with ANSI SQL
spark.sql("""
    SELECT 
        DATE(timestamp) as date,
        category,
        COUNT(*) as transaction_count,
        SUM(amount) as total_revenue,
        AVG(amount) as avg_transaction
    FROM transactions 
    WHERE timestamp >= '2024-01-01'
    GROUP BY DATE(timestamp), category
    HAVING COUNT(*) > 100
    ORDER BY date DESC, total_revenue DESC
""").show()
```

## 🚀 **Job Submission & Deployment**

### **Local Development**
```bash
# Run Python script directly
python3 /scripts/spark_example.py

# Submit to cluster  
spark-submit --master spark://spark-master:7077 \
             --driver-memory 1g \
             --executor-memory 1g \
             --executor-cores 1 \
             /scripts/spark_example.py
```

### **Production Configuration**
```bash
# Optimized cluster submission
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 4 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  /scripts/production_job.py
```

### **Monitoring & Debugging**
```bash
# List running applications
spark-submit --status <app-id>

# Kill application
spark-submit --kill <app-id>

# Access application logs
yarn logs -applicationId <app-id>
```

## 🧠 **Machine Learning with MLlib**

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

# Prepare features
assembler = VectorAssembler(
    inputCols=["feature1", "feature2", "feature3"],
    outputCol="features"
)

# Create model
lr = LinearRegression(featuresCol="features", labelCol="target")

# Build pipeline
pipeline = Pipeline(stages=[assembler, lr])

# Train model
model = pipeline.fit(training_df)

# Make predictions
predictions = model.transform(test_df)
predictions.select("features", "target", "prediction").show()
```

## 🌊 **Streaming Processing**

```python
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import *

# Read streaming data (Kafka, files, sockets)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "events") \
    .load()

# Process streaming data
query = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .groupBy(window(col("timestamp"), "1 minute"), col("event_type")) \
 .count() \
 .writeStream \
 .outputMode("update") \
 .format("console") \
 .trigger(processingTime='30 seconds') \
 .start()

query.awaitTermination()
```

## 🎯 **Performance Optimization**

### **Memory Management**
```python
# Cache frequently accessed DataFrames
df.cache()  # or df.persist(StorageLevel.MEMORY_AND_DISK)

# Broadcast small lookup tables
broadcast_dict = spark.sparkContext.broadcast(lookup_dict)

# Optimize shuffles
df.repartition(col("partition_key"))  # Even distribution
df.coalesce(4)  # Reduce partitions without shuffle
```

### **SQL Optimization**
```python
# Enable adaptive query execution (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Broadcast joins for small tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "200MB")

# Partition pruning
df.filter(col("year") == 2024).explain()  # Check execution plan
```

## 🌐 **Integration Examples**

### **With HDFS (Shared Storage)**
```python
# Read from Hadoop
df = spark.read.parquet("hdfs://hadoop:9000/user/data/large_dataset.parquet")

# Write to HDFS with partitioning
df.write.partitionBy("year", "month") \
  .parquet("hdfs://hadoop:9000/user/output/partitioned_data")
```

### **With Kafka (Streaming)**
```python
# Read from Kafka topic
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user-events") \
    .load()

# Process and write back to Kafka
query = streaming_df.select(
    col("key").cast("string"),
    to_json(struct("processed_*")).alias("value")
).writeStream \
 .format("kafka") \
 .option("kafka.bootstrap.servers", "kafka:9092") \
 .option("topic", "processed-events") \
 .start()
```

## 📊 **Web UI Deep Dive**

### **Master UI (http://localhost:8080)**
- **Workers**: Live worker nodes, memory/CPU usage, running executors
- **Applications**: Completed and running Spark applications  
- **Status**: Cluster health, resource allocation, system metrics

### **Worker UI (http://localhost:8081)**  
- **Executors**: Active JVM processes, task execution details
- **Logs**: Worker logs, executor stdout/stderr
- **Environment**: JVM settings, classpath, Spark configuration

### **Application UI (http://localhost:4040)**
- **Jobs**: Job DAG visualization, task timelines
- **Stages**: Task distribution, shuffle read/write metrics  
- **Storage**: Cached DataFrames, memory usage
- **Environment**: Application configuration, runtime settings
- **Executors**: Per-executor metrics, GC time, task failures
