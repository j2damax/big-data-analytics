# 🔄 Apache Kafka Streaming Platform

**Apache Kafka 3.6.1** - Distributed event streaming platform for high-throughput, fault-tolerant real-time data pipelines and streaming applications.

## 🏗️ **Event Streaming Architecture**

| Component | Purpose | Container | Configuration |
|-----------|---------|-----------|--------------|
| **Kafka Broker** | Message storage & routing | `kafka:9092` | Single-node cluster with log retention |
| **Zookeeper** | Cluster coordination & metadata | `zookeeper:2181` | Kafka dependency for cluster management |
| **Topics** | Event category streams | Logical partitions | Configurable replication & retention |
| **Producers** | Event publishers | Client applications | Batching, compression, acknowledgments |
| **Consumers** | Event subscribers | Client applications | Group coordination, offset management |

## 🚀 **Core Event Streaming Concepts**

### **Event-Driven Architecture Benefits**
- **📈 High Throughput**: Millions of events per second with linear scaling
- **⚡ Low Latency**: Sub-millisecond message delivery for real-time systems
- **🔒 Durability**: Persistent event log with configurable retention policies
- **📊 Exactly-Once**: Guaranteed message delivery semantics
- **🔄 Replay**: Historical event replay for debugging and reprocessing
- **🌐 Decoupling**: Loose coupling between producers and consumers

## 🚀 **Quick Start**

```bash
# Access Kafka cluster
make shell-kafka  # Interactive Kafka environment

# Test messaging immediately
make test-kafka   # Producer/consumer example with JSON events

# Kafka starts automatically with:
# 🔗 Broker: kafka:9092 (internal) / localhost:9092 (external)  
# 📋 Zookeeper: localhost:2181
```

## 📋 **Topic Management**

### **Essential Topic Operations**
```bash
# Create production-ready topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic user-events \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config compression.type=lz4

# List all topics with details
kafka-topics.sh --list --bootstrap-server localhost:9092
kafka-topics.sh --describe --bootstrap-server localhost:9092

# Topic configuration management
kafka-topics.sh --alter \
  --bootstrap-server localhost:9092 \
  --topic user-events \
  --partitions 6  # Increase partitions (cannot decrease)

# Delete topic (use with caution!)
kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic test-topic
```

### **Advanced Topic Configuration**
```bash
# High-throughput topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic high-volume-events \
  --partitions 12 \
  --config segment.ms=3600000 \
  --config retention.bytes=1073741824 \
  --config min.insync.replicas=1

# Compacted topic (for key-value stores)
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic user-profiles \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.1
```

## 📤 **Producer Patterns**

### **High-Performance Python Producer**
```python
from kafka import KafkaProducer
import json
import time

# Optimized producer configuration
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    
    # Serialization
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    
    # Performance tuning
    batch_size=16384,          # Batch messages for throughput
    linger_ms=10,              # Wait up to 10ms to batch messages  
    compression_type='lz4',    # Compress messages
    
    # Reliability
    acks='1',                  # Wait for leader acknowledgment
    retries=3,                 # Retry on failure
    retry_backoff_ms=100,
    
    # Timeouts
    request_timeout_ms=30000,
    delivery_timeout_ms=60000
)

# Send events with error handling
def send_event(topic, key, event_data):
    try:
        future = producer.send(topic, key=key, value=event_data)
        record_metadata = future.get(timeout=10)
        print(f"Sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
    except Exception as e:
        print(f"Failed to send: {e}")

# Example usage
send_event('user-events', 'user123', {
    'user_id': 'user123', 
    'event': 'login',
    'timestamp': int(time.time() * 1000),
    'metadata': {'device': 'mobile', 'location': 'US'}
})

producer.flush()  # Ensure all messages are sent
producer.close()  # Clean shutdown
```

### **Command-Line Producer Testing**
```bash
# Interactive console producer  
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic user-events \
  --property "key.separator=:" \
  --property "parse.key=true"

# Batch produce from file
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic logs \
  < /scripts/sample_data.txt
```

## 📥 **Consumer Patterns**

### **Scalable Consumer Groups**
```python
from kafka import KafkaConsumer
import json

# Consumer group for horizontal scaling
consumer = KafkaConsumer(
    'user-events',
    bootstrap_servers=['kafka:9092'],
    
    # Consumer group coordination
    group_id='analytics-processors',    # Multiple consumers share load
    auto_offset_reset='earliest',       # Start from beginning for new group
    enable_auto_commit=True,           # Automatically commit offsets
    auto_commit_interval_ms=5000,      # Commit every 5 seconds
    
    # Deserialization  
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    
    # Performance tuning
    fetch_min_bytes=1024,              # Wait for at least 1KB
    fetch_max_wait_ms=500,             # Or 500ms timeout
    max_poll_records=500,              # Process up to 500 records per poll
    
    # Session management
    session_timeout_ms=30000,          # Group membership timeout
    heartbeat_interval_ms=3000         # Heartbeat frequency
)

# Process messages with error handling
try:
    for message in consumer:
        try:
            # Process the event
            event = message.value
            print(f"Processing {event['event']} for {event['user_id']}")
            
            # Your business logic here
            process_user_event(event)
            
        except Exception as e:
            print(f"Error processing message: {e}")
            # Optionally send to dead letter queue
            
except KeyboardInterrupt:
    print("Shutting down consumer...")
finally:
    consumer.close()
```

### **Advanced Consumer Control**
```bash
# Consumer with manual offset management
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic user-events \
  --group manual-processors \
  --enable-autocommit false \
  --property print.key=true \
  --property print.offset=true \
  --property print.partition=true

# Reset consumer group offset (reprocess data)
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group analytics-processors \
  --topic user-events \
  --reset-offsets --to-earliest --execute

# Monitor consumer group lag
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group analytics-processors \
  --describe
```

## 🔧 **Kafka Administration**

### **Cluster Health Monitoring**
```bash
# Broker information
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Topic and partition details
kafka-topics.sh --describe --bootstrap-server localhost:9092
kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe

# Consumer group management  
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group analytics-processors

# Performance testing
kafka-producer-perf-test.sh \
  --topic performance-test \
  --num-records 100000 \
  --record-size 1024 \
  --throughput 10000 \
  --producer-props bootstrap.servers=localhost:9092

kafka-consumer-perf-test.sh \
  --topic performance-test \
  --messages 100000 \
  --bootstrap-server localhost:9092
```

### **Configuration Management**
```bash
# View broker configuration
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type brokers --entity-name 1 --describe

# Update topic configuration
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name user-events \
  --alter --add-config retention.ms=259200000

# View current log segments  
kafka-dump-log.sh --files /var/lib/kafka/logs/user-events-0/00000000000000000000.log
```

## ⚙️ **Production Configuration**

### **Broker Settings** (`config/server.properties`)
```properties
# Broker identification
broker.id=1
listeners=PLAINTEXT://0.0.0.0:9092

# Log storage  
log.dirs=/var/lib/kafka/logs
num.network.threads=3
num.io.threads=8

# Retention policies
log.retention.hours=168              # 7 days default
log.retention.bytes=1073741824       # 1GB per partition
log.segment.bytes=1073741824         # 1GB log segments

# Replication (production cluster settings)
default.replication.factor=3         # 3 replicas for fault tolerance
min.insync.replicas=2               # Minimum in-sync replicas
unclean.leader.election.enable=false # Prevent data loss

# Performance tuning
socket.send.buffer.bytes=102400      # Network buffer sizes
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600   # Max request size
```

### **Zookeeper Integration** 
```properties
# Zookeeper connection
zookeeper.connect=zookeeper:2181
zookeeper.connection.timeout.ms=18000

# Consumer coordination (legacy - new consumers use Kafka directly)
group.initial.rebalance.delay.ms=3000
```

## 🌊 **Stream Processing Integration**

### **Kafka → Flink Pipeline**
```python
# Flink Kafka consumer
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer

env = StreamExecutionEnvironment.get_execution_environment()

# Consume from Kafka topic
kafka_consumer = FlinkKafkaConsumer(
    topics=['user-events'],
    deserialization_schema=JsonRowDeserializationSchema(),
    properties={
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-processors'
    }
)

stream = env.add_source(kafka_consumer)
# Process stream with Flink...
```

### **Kafka ↔ Spark Streaming**
```python
# Spark Structured Streaming with Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user-events") \
    .option("startingOffsets", "latest") \
    .load()

# Process and write back to Kafka
query = df.select(
    from_json(col("value").cast("string"), event_schema).alias("event")
).select("event.*") \
 .writeStream \
 .format("kafka") \
 .option("kafka.bootstrap.servers", "kafka:9092") \
 .option("topic", "processed-events") \
 .start()
```

### **Event Sourcing Pattern**
```python
# Event store implementation
class EventStore:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode()
        )
    
    def append_event(self, aggregate_id, event_type, event_data):
        event = {
            'aggregate_id': aggregate_id,
            'event_type': event_type,
            'event_data': event_data,
            'timestamp': int(time.time() * 1000),
            'version': self.get_next_version(aggregate_id)
        }
        
        self.producer.send(
            topic='event-store',
            key=aggregate_id,
            value=event
        )
    
    def replay_events(self, aggregate_id):
        consumer = KafkaConsumer(
            'event-store',
            bootstrap_servers=['kafka:9092'],
            key_deserializer=lambda k: k.decode('utf-8'),
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        
        events = []
        for message in consumer:
            if message.key == aggregate_id:
                events.append(message.value)
        
        return sorted(events, key=lambda e: e['version'])
```

## 🏭 **Production Use Cases**

### **1. Real-Time Analytics Pipeline**
```
Web App → Kafka (user-events) → Flink (real-time processing) → Dashboard
                              ↓
                         HDFS (long-term storage) → Spark (batch analytics)
```

### **2. Microservices Event Bus**
```
Order Service → Kafka (order-events) → [Inventory, Payment, Shipping Services]
                                   ↓
                              Event Sourcing Store
```

### **3. Log Aggregation**  
```
App Logs → Kafka (log-stream) → Elasticsearch → Kibana Dashboard
                             ↓  
                        Long-term Archive (HDFS)
```

### **4. CDC (Change Data Capture)**
```
Database → Kafka Connect → Kafka (table-changes) → [Cache, Search, Analytics]
```

## 📊 **Monitoring & Operations**

### **Key Metrics to Monitor**
```bash
# Throughput metrics
kafka-run-class.sh kafka.tools.JmxTool \
  --object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec

# Consumer lag monitoring  
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group analytics-processors

# Disk usage per topic
kafka-log-dirs.sh --bootstrap-server localhost:9092 --describe \
  --json | jq '.brokers[].logDirs[].partitions'
```

### **Troubleshooting Common Issues**
```bash
# Check broker health
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Verify topic configuration
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic user-events

# Consumer group issues
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group analytics-processors --verbose

# Network connectivity test
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-connectivity
```

## 🎯 **Best Practices**

### **Topic Design**
- **Partitioning Strategy**: Use meaningful keys (user_id, region) for even distribution
- **Naming Convention**: `domain.entity.event-type` (e.g., `user.profile.updated`)
- **Schema Evolution**: Use Avro/JSON Schema for backward compatibility
- **Retention Policy**: Balance storage cost vs replay requirements

### **Producer Optimization**
- **Batching**: Configure `batch.size` and `linger.ms` for throughput
- **Compression**: Use `lz4` or `snappy` for network efficiency  
- **Idempotence**: Enable `enable.idempotence=true` for exactly-once semantics
- **Error Handling**: Implement retry logic and dead letter queues

### **Consumer Optimization**
- **Consumer Groups**: Scale horizontally with multiple consumer instances
- **Offset Management**: Choose appropriate `auto.offset.reset` strategy
- **Session Timeout**: Tune `session.timeout.ms` for failure detection
- **Backpressure**: Monitor consumer lag and scale accordingly
