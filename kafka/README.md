# Kafka Setup

This directory contains the Dockerfile and configuration for Apache Kafka.

## Components

- **Kafka Broker**: Message broker server
- **Zookeeper**: Cluster coordination (managed separately)
- **Topics**: Message categories
- **Producers**: Applications that publish messages
- **Consumers**: Applications that subscribe to messages

## Features

- High-throughput, low-latency message streaming
- Fault-tolerant and scalable
- Persistent storage of message streams
- Real-time data processing

## Usage

### Accessing Kafka

```bash
# Enter Kafka container
docker exec -it kafka bash
```

### Topic Management

```bash
# Create topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partitions 1 \
  --replication-factor 1

# List topics
kafka-topics.sh --list \
  --bootstrap-server localhost:9092

# Describe topic
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic my-topic

# Delete topic
kafka-topics.sh --delete \
  --bootstrap-server localhost:9092 \
  --topic my-topic
```

### Producer and Consumer

```bash
# Start console producer
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic

# Start console consumer
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning
```

### Python Example

```python
from kafka import KafkaProducer, KafkaConsumer
import json

# Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send('my-topic', {'key': 'value'})
producer.flush()

# Consumer
consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    print(message.value)
```

## Configuration

The `config/server.properties` file contains Kafka broker settings:
- Broker ID and listeners
- Log directories
- Zookeeper connection
- Retention policies

## Use Cases

- Real-time data pipelines
- Event sourcing
- Log aggregation
- Stream processing with Flink or Spark
- Microservices communication

## Learning Resources

- Build producer and consumer applications
- Understand partitioning and replication
- Implement stream processing
- Monitor Kafka clusters
