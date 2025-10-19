# Spark Setup

This directory contains the Dockerfile for Apache Spark.

## Components

- **Spark Master**: Cluster manager
- **Spark Worker**: Executor nodes
- **Spark Core**: Foundation for distributed processing
- **PySpark**: Python API for Spark

## Features

- In-memory processing
- Unified API for batch and streaming
- Support for SQL, streaming, ML, and graph processing

## Usage

### Accessing Spark

```bash
# Enter Spark Master container
docker exec -it spark-master bash

# Start Spark shell (Scala)
spark-shell

# Start PySpark shell (Python)
pyspark

# Start SparkSQL
spark-sql
```

### Submitting Spark Jobs

```bash
# Submit Python job
spark-submit \
  --master spark://spark-master:7077 \
  /scripts/spark_example.py

# Submit with configuration
spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 1g \
  --executor-memory 1g \
  /scripts/spark_example.py
```

### PySpark Examples

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

# Create DataFrame
df = spark.createDataFrame([
    (1, "Alice", 34),
    (2, "Bob", 45),
    (3, "Charlie", 29)
], ["id", "name", "age"])

# Show data
df.show()

# SQL query
df.createOrReplaceTempView("people")
spark.sql("SELECT name, age FROM people WHERE age > 30").show()
```

## Web UI

- Spark Master: http://localhost:8080
- Spark Worker: http://localhost:8081
- Spark Application: http://localhost:4040 (when running)

## Learning Resources

- Process large datasets with RDD and DataFrame APIs
- Use Spark SQL for data analysis
- Implement machine learning with MLlib
- Build streaming applications with Spark Streaming
