# Testing Guide

This guide provides step-by-step instructions for testing each component of the Big Data Analytics project.

## Prerequisites

Ensure all containers are running:
```bash
docker compose up -d
docker compose ps
```

All services should show "Up" status.

## Testing Hadoop

### 1. Access Hadoop Container
```bash
docker exec -it hadoop bash
```

### 2. Format NameNode (First Time Only)
```bash
hdfs namenode -format
```

### 3. Start HDFS
```bash
start-dfs.sh
```

### 4. Test HDFS Commands
```bash
# Create directory
hdfs dfs -mkdir -p /user/hadoop

# Upload sample file
hdfs dfs -put /scripts/sample_data.txt /user/hadoop/

# List files
hdfs dfs -ls /user/hadoop

# Read file
hdfs dfs -cat /user/hadoop/sample_data.txt

# Download file
hdfs dfs -get /user/hadoop/sample_data.txt ./downloaded.txt
```

### 5. Verify Web UI
Open http://localhost:9870 in your browser

Expected: Hadoop NameNode web interface showing cluster status

### 6. Test MapReduce (Optional)
```bash
# Run built-in WordCount example
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /user/hadoop/sample_data.txt /user/hadoop/output

# View results
hdfs dfs -cat /user/hadoop/output/part-r-00000
```

## Testing Spark

### 1. Access Spark Master Container
```bash
docker exec -it spark-master bash
```

### 2. Test PySpark Shell
```bash
pyspark
```

In PySpark shell:
```python
# Create RDD
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)
print(rdd.collect())

# Calculate sum
print(rdd.reduce(lambda a, b: a + b))

# Exit
exit()
```

### 3. Run Example Script
```bash
cd /scripts
python3 spark_example.py
```

Expected output:
- Word count results
- DataFrame display
- SQL query results

### 4. Submit to Cluster
```bash
spark-submit --master spark://spark-master:7077 /scripts/spark_example.py
```

### 5. Verify Web UIs
- Spark Master: http://localhost:8080
- Spark Worker: http://localhost:8081
- Spark Application: http://localhost:4040 (when job is running)

Expected: Cluster information, running applications, worker status

## Testing Kafka

### 1. Access Kafka Container
```bash
docker exec -it kafka bash
```

### 2. Create Topic
```bash
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --partitions 1 \
  --replication-factor 1
```

### 3. List Topics
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

Expected: Shows "test-topic"

### 4. Test Producer (Terminal 1)
```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic
```

Type messages and press Enter (each line is a message)

### 5. Test Consumer (Terminal 2)
Open a second terminal:
```bash
docker exec -it kafka bash

kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --from-beginning
```

Expected: Messages appear in consumer as you type them in producer

### 6. Run Python Example
```bash
cd /scripts
python3 kafka_example.py
```

Expected output:
- Topic creation confirmation
- Messages being produced
- Messages being consumed

## Testing Flink

### 1. Access Flink JobManager Container
```bash
docker exec -it flink-jobmanager bash
```

### 2. Test Flink CLI
```bash
# List running jobs
flink list

# Should show no jobs initially
```

### 3. Run Python Example
```bash
cd /scripts
python3 flink_example.py
```

Expected output:
- DataStream word count results
- Table API results
- No errors

### 4. Verify Web UI
Open http://localhost:8082 in your browser

Expected: Flink dashboard showing:
- JobManager status
- TaskManager status
- Available task slots
- Completed jobs (after running example)

### 5. Monitor Task Managers
```bash
# Check TaskManager is connected
docker compose logs flink-taskmanager
```

Expected: No errors, TaskManager registered with JobManager

## Integration Testing

### End-to-End Pipeline Test

#### 1. Kafka → Flink Pipeline

Terminal 1 - Start Kafka Producer:
```bash
docker exec -it kafka bash
cd /scripts

# Create a streaming producer (modify kafka_example.py to run continuously)
python3 -c "
from kafka import KafkaProducer
import json
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'stream-data'
for i in range(100):
    message = {'id': i, 'timestamp': datetime.now().isoformat(), 'value': i*10}
    producer.send(topic, value=message)
    print(f'Sent: {message}')
    time.sleep(1)
"
```

Terminal 2 - Process with Flink:
```bash
docker exec -it flink-jobmanager bash
# Create a Flink job to consume from Kafka
```

#### 2. HDFS → Spark Pipeline

```bash
# Store data in HDFS
docker exec -it hadoop bash
hdfs dfs -mkdir -p /data
hdfs dfs -put /scripts/sample_data.txt /data/

# Process with Spark
docker exec -it spark-master bash
pyspark

# In PySpark:
df = spark.read.text("hdfs://hadoop:9000/data/sample_data.txt")
word_count = df.selectExpr("explode(split(value, ' ')) as word").groupBy("word").count()
word_count.show()
```

## Performance Testing

### 1. Spark Performance
```bash
# Time a Spark job
docker exec -it spark-master bash
time spark-submit --master spark://spark-master:7077 /scripts/spark_example.py
```

### 2. Kafka Throughput
```bash
# Test producer throughput
docker exec -it kafka bash
kafka-producer-perf-test.sh \
  --topic perf-test \
  --num-records 10000 \
  --record-size 1000 \
  --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092
```

### 3. HDFS Write Speed
```bash
docker exec -it hadoop bash
# Create a large file
dd if=/dev/zero of=/tmp/testfile bs=1M count=100

# Upload to HDFS
time hdfs dfs -put /tmp/testfile /user/hadoop/
```

## Troubleshooting Tests

### Services Won't Start
```bash
# Check logs
docker compose logs [service-name]

# Restart service
docker compose restart [service-name]
```

### Connection Refused
- Wait longer for services to initialize (30-60 seconds)
- Check if service is running: `docker compose ps`
- Check network: `docker network ls`

### Out of Memory
- Increase Docker memory limit (8GB minimum recommended)
- Reduce parallelism in Spark/Flink jobs

### Port Conflicts
- Check what's using the port: `lsof -i :<port>` (Mac/Linux)
- Change port mapping in docker-compose.yml

## Automated Testing

Run all tests:
```bash
make test-all
```

Individual tests:
```bash
make test-spark
make test-kafka
make test-flink
```

## Test Cleanup

After testing, clean up:
```bash
# Delete test data from HDFS
docker exec -it hadoop bash
hdfs dfs -rm -r /user/hadoop/*

# Delete Kafka topics
docker exec -it kafka bash
kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic test-topic

# Stop all services
docker compose down

# Remove volumes (complete cleanup)
docker compose down -v
```

## Success Criteria

All tests pass when:
- [ ] All containers start without errors
- [ ] All web UIs are accessible
- [ ] Example scripts run successfully
- [ ] Data can be written and read from HDFS
- [ ] Kafka messages flow from producer to consumer
- [ ] Spark jobs execute and show results
- [ ] Flink jobs process streams successfully
- [ ] No memory or resource errors

## Reporting Issues

If tests fail:
1. Check the logs: `docker compose logs [service-name]`
2. Verify container status: `docker compose ps`
3. Check resource usage: `docker stats`
4. Review error messages
5. Open an issue on GitHub with:
   - Error message
   - Steps to reproduce
   - System information (OS, Docker version)
   - Logs output
