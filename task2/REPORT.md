# Real-Time IoT Sensor Data Analytics using Apache Kafka

## Comprehensive Project Report

**Course:** Big Data Analytics  
**Task:** Real-Time IoT Sensor Data Streaming Pipeline  
**Dataset:** City of Austin Traffic Count Data (Camera Traffic Counts)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Understanding Apache Kafka - Key Concepts](#2-understanding-apache-kafka---key-concepts)
3. [Part 1: Setup and Environment Configuration](#3-part-1-setup-and-environment-configuration)
4. [Part 2: Data Source and Preprocessing](#4-part-2-data-source-and-preprocessing)
5. [Part 3: Streaming Data Processing and Analysis](#5-part-3-streaming-data-processing-and-analysis)
6. [Part 4: Visualization and Reporting](#6-part-4-visualization-and-reporting)
7. [Results and Screenshots](#7-results-and-screenshots)
8. [Conclusion and Future Improvements](#8-conclusion-and-future-improvements)

---

## 1. Executive Summary

This project implements a real-time IoT sensor data streaming pipeline using Apache Kafka to analyze urban traffic behavior in Austin, Texas. The solution streams, processes, and visualizes live traffic sensor data from GRIDSMART optical traffic detectors deployed by the City of Austin.

### What Does This Solution Do?

Think of this system like a traffic monitoring center that:

1. **Collects Data** - Reads traffic sensor information (like how many cars passed through each intersection)
2. **Sends Data Instantly** - Uses Apache Kafka as a "message delivery service" to send this data in real-time
3. **Processes Data** - Calculates useful statistics like hourly averages and daily peaks
4. **Stores Data** - Saves processed information in a database for historical analysis
5. **Displays Results** - Shows beautiful dashboards with charts and graphs

### Key Technologies Used

| Technology | Purpose | Simple Explanation |
|------------|---------|-------------------|
| **Apache Kafka** | Message Streaming | Acts as a high-speed postal service for data |
| **Python** | Programming | The language used to write all the code |
| **QuestDB** | Time-Series Database | Stores data optimized for time-based queries |
| **Grafana** | Visualization | Creates beautiful dashboards and charts |
| **Docker** | Containerization | Packages everything to run anywhere easily |

---

## 2. Understanding Apache Kafka - Key Concepts

Apache Kafka is the heart of our streaming solution. Let's understand its key components using simple analogies and real examples from our project.

### 2.1 What is Apache Kafka?

**Simple Explanation:** Imagine Apache Kafka as a super-fast postal service for data. Instead of delivering letters, it delivers messages (data) from one place to another, instantly and reliably.

**Technical Definition:** Apache Kafka is a distributed event streaming platform that enables applications to publish, subscribe to, store, and process streams of records in real-time.

### 2.2 Key Components of Kafka

#### 2.2.1 Kafka Broker

**Simple Explanation:** A broker is like a post office branch. It receives messages, stores them safely, and delivers them to the right recipients.

**From Our Solution:**
```yaml
# docker-compose.yml - Our Kafka Broker Configuration
kafka-broker:
  image: apache/kafka:latest
  container_name: kafka-broker
  ports:
    - "9092:9092"  # This is the "address" of our post office
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
```

The broker listens on port 9092 and handles all message traffic.

#### 2.2.2 Kafka Topics

**Simple Explanation:** Topics are like mailboxes organized by subject. All messages about a specific subject go to the same mailbox.

**From Our Solution - We have three main topics:**

| Topic Name | Purpose | Messages Stored |
|------------|---------|-----------------|
| `traffic_raw` | Raw sensor data | Original traffic readings from sensors |
| `hourly_average` | Processed metrics | Hourly average vehicle counts |
| `metric_availability` | Sensor health | Sensor availability percentages |

```python
# From sensor-data-producer.py
TOPIC = 'traffic_raw'  # Our main mailbox for raw sensor data
```

#### 2.2.3 Kafka Producer

**Simple Explanation:** A producer is like a person who writes and sends letters. It creates messages and sends them to the post office (broker).

**From Our Solution:**
```python
# sensor-data-producer.py - How we send traffic data

class SensorDataProducer:
    def _create_producer(self):
        """Create and configure Kafka producer."""
        self.producer = KafkaProducer(
            bootstrap_servers=[self.bootstrap_servers],
            # Convert data to JSON format (like writing in a common language)
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            # Use device ID as the key (like writing the sender's name)
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            # Wait for confirmation that message was received
            acks='all',
            retries=3,  # Try 3 times if sending fails
        )
```

**What This Code Does:**
1. Connects to the Kafka broker (the post office)
2. Converts data to JSON format (a common data format)
3. Ensures messages are delivered reliably with acknowledgments

#### 2.2.4 Kafka Consumer

**Simple Explanation:** A consumer is like a person who receives and reads letters. It subscribes to topics and receives messages when they arrive.

**From Our Solution:**
```python
# sensor-data-consumer.py - How we receive and process data

app = Application(
    broker_address=KAFKA_BROKER,
    auto_offset_reset="earliest",  # Start from the beginning if new
    consumer_group="traffic-metrics-consumer",  # Group name for coordination
)

# Subscribe to the topic
input_topic = app.topic(INPUT_TOPIC, value_deserializer="json")
sdf = app.dataframe(input_topic)

# Process each message
sdf.apply(process_with_state, stateful=True)
```

**What This Code Does:**
1. Connects to Kafka and joins a "consumer group"
2. Subscribes to the `traffic_raw` topic
3. Processes each message as it arrives

#### 2.2.5 Partitions

**Simple Explanation:** Partitions are like having multiple lanes at the post office. Instead of one line, you have several, so more people can be served at once.

**From Our Solution:**
```yaml
# docker-compose.yml
KAFKA_NUM_PARTITIONS: 3  # We have 3 partitions (3 lanes)
```

**Why Partitions Matter:**
- Messages are distributed across partitions for parallel processing
- We use `atd_device_id` as the key, so all data from the same sensor goes to the same partition
- This ensures data from one sensor is processed in order

#### 2.2.6 Consumer Groups

**Simple Explanation:** A consumer group is like a team of workers at the post office. They share the workload - if one worker is busy, another can help.

```python
# From sensor-data-consumer.py
consumer_group="traffic-metrics-consumer"  # Our team name
```

**Benefits:**
- Multiple consumers can share the work
- If one consumer fails, others continue
- Automatic load balancing

### 2.3 Kafka Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Apache Kafka System                          │
│                                                                     │
│  ┌───────────────┐         ┌────────────────┐       ┌────────────┐ │
│  │   PRODUCER    │         │  KAFKA BROKER  │       │  CONSUMER  │ │
│  │               │         │                │       │            │ │
│  │ Traffic Data  │ ──────► │ Topic: traffic │ ────► │  Metrics   │ │
│  │   Script      │  write  │     _raw       │  read │ Calculator │ │
│  │               │         │                │       │            │ │
│  └───────────────┘         │   Partition 0  │       └────────────┘ │
│                            │   Partition 1  │                      │
│                            │   Partition 2  │       ┌────────────┐ │
│                            │                │       │  CONSUMER  │ │
│                            └────────────────┘ ────► │  Hourly    │ │
│                                                     │  Totals    │ │
│                                                     └────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.4 How Messages Flow in Our System

```
Step 1: Data Collection
┌──────────────────────────────────────────────────────────────────┐
│  Traffic Sensor Data (JSON format)                               │
│  {                                                               │
│    "atd_device_id": "b2fd...",                                  │
│    "intersection_name": "BURNET RD / PALM WAY",                 │
│    "direction": "NB",                                            │
│    "volume": "42",                                               │
│    "read_date": "2024-07-01T00:00:00.000"                       │
│  }                                                               │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 2: Producer Sends to Kafka
┌──────────────────────────────────────────────────────────────────┐
│  Producer sends message to 'traffic_raw' topic                   │
│  Key: "b2fd..." (device_id for partitioning)                    │
│  Value: JSON data with timestamp metadata                        │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 3: Consumer Processes Data
┌──────────────────────────────────────────────────────────────────┐
│  Consumer receives message, calculates:                          │
│  - Hourly average vehicle count                                  │
│  - Daily sensor availability                                     │
│  - Running totals and peaks                                      │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 4: Results Stored and Published
┌──────────────────────────────────────────────────────────────────┐
│  - Metrics saved to QuestDB (for historical analysis)            │
│  - Metrics published to 'hourly_average' topic                   │
│  - Dashboard displays real-time visualizations                   │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. Part 1: Setup and Environment Configuration

### 3.1 Environment Overview

Our solution uses Docker Compose to orchestrate all services, making it easy to run anywhere with a single command.

### 3.2 Services Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Docker Compose Services                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    │
│  │  Kafka Broker   │    │    QuestDB      │    │    Grafana      │    │
│  │   Port: 9092    │    │  Port: 9000     │    │   Port: 3000    │    │
│  │                 │    │  (Web Console)  │    │   (Dashboard)   │    │
│  │  Message        │    │  Time-Series    │    │  Visualization  │    │
│  │  Streaming      │    │  Database       │    │  Platform       │    │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘    │
│           │                      │                      │              │
│           │                      │                      │              │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    │
│  │    Redpanda     │    │  Sensor Data    │    │  Hourly Total   │    │
│  │    Console      │    │   Consumer      │    │   Consumer      │    │
│  │   Port: 8083    │    │   (Python)      │    │   (Python)      │    │
│  │                 │    │                 │    │                 │    │
│  │  Kafka Web UI   │    │  Metrics        │    │  Peak Volume    │    │
│  │                 │    │  Calculator     │    │  Calculator     │    │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘    │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │              Sensor Data Producer (Python)                       │   │
│  │              Reads dataset and publishes to Kafka                │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Docker Compose Configuration

```yaml
# docker-compose.yml - Complete Service Configuration

services:
  # ═══════════════════════════════════════════════════════════
  # KAFKA BROKER - The Heart of Our Streaming System
  # ═══════════════════════════════════════════════════════════
  kafka-broker:
    image: apache/kafka:latest
    container_name: kafka-broker
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-broker:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-broker:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_NUM_PARTITIONS: 3  # 3 partitions for parallelism
    networks:
      - streaming-network

  # ═══════════════════════════════════════════════════════════
  # QUESTDB - Time-Series Database for Metrics Storage
  # ═══════════════════════════════════════════════════════════
  questdb:
    image: questdb/questdb:9.2.0
    container_name: questdb
    ports:
      - "9000:9000"   # Web Console
      - "8812:8812"   # PostgreSQL protocol
    environment:
      - QDB_PG_USER=admin
      - QDB_PG_PASSWORD=quest

  # ═══════════════════════════════════════════════════════════
  # GRAFANA - Visualization Dashboard
  # ═══════════════════════════════════════════════════════════
  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_INSTALL_PLUGINS=questdb-questdb-datasource
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro

networks:
  streaming-network:
    driver: bridge
```

### 3.4 Kafka Topics Design

Our solution uses three main Kafka topics:

| Topic Name | Purpose | Producers | Consumers |
|------------|---------|-----------|-----------|
| `traffic_raw` | Raw sensor readings | sensor-data-producer | sensor-data-consumer |
| `hourly_average` | Hourly metrics per device | sensor-data-consumer | hourly-total-consumer |
| `metric_availability` | Sensor health data | sensor-data-consumer | (Grafana via QuestDB) |

### 3.5 How to Start the Environment

```bash
# Step 1: Download the dataset
make download-dataset

# Step 2: Start all services
make up

# This will:
# - Start Kafka Broker
# - Start QuestDB database
# - Start Grafana dashboard
# - Start the data producer (automatically sends data)
# - Start the data consumers (automatically process data)
```

### 3.6 Web Interfaces

After starting the services, you can access:

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana Dashboard | http://localhost:3000 | admin / admin |
| QuestDB Console | http://localhost:9000 | admin / quest |
| Redpanda Console (Kafka UI) | http://localhost:8083 | No login required |

---

## 4. Part 2: Data Source and Preprocessing

### 4.1 Dataset Description

**Source:** City of Austin Open Data Portal  
**Dataset:** Camera Traffic Counts  
**URL:** https://data.austintexas.gov/Transportation-and-Mobility/Camera-Traffic-Counts/sh59-i6y9

This dataset contains traffic count data from GRIDSMART optical traffic detectors deployed throughout Austin, Texas.

### 4.2 Data Fields

| Field Name | Description | Example Value |
|------------|-------------|---------------|
| `atd_device_id` | Unique sensor identifier | "b2fd..." |
| `intersection_name` | Location of the sensor | "BURNET RD / PALM WAY" |
| `direction` | Traffic direction | "NB" (Northbound) |
| `volume` | Vehicle count | "42" |
| `read_date` | Timestamp of reading | "2024-07-01T00:00:00.000" |
| `day_of_week` | Day name | "Monday" |
| `hour` | Hour of day | "0" (midnight) |

### 4.3 Data Download Script

```python
#!/usr/bin/env python
# download-dataset.py - Downloads traffic data from Austin Open Data Portal

from sodapy import Socrata
import json

# Connect to Austin's Open Data API (no authentication needed)
client = Socrata("data.austintexas.gov", None)

# Download up to 1 million records, ordered by date
results = client.get("sh59-i6y9", limit=1000000, order="read_date DESC")

# Save to JSONL format (one JSON record per line)
with open('data/traffic.jsonl', 'w') as f:
    for record in reversed(results):  # Oldest first for streaming
        f.write(json.dumps(record) + '\n')

print(f"Dataset written with {len(results)} records")
```

**What This Script Does:**
1. **Connects** to Austin's Open Data API
2. **Downloads** up to 1 million traffic records
3. **Reverses** the order (oldest first) to simulate real-time streaming
4. **Saves** data in JSONL format (one JSON object per line)

### 4.4 Sample Data Record

```json
{
  "atd_device_id": "b2fdb16e-c9c3-470e-8b78-51e71a2ecd81",
  "intersection_name": "BURNET RD / PALM WAY",
  "direction": "NB",
  "volume": "42",
  "read_date": "2024-07-01T00:00:00.000",
  "day_of_week": "Monday",
  "hour": "0",
  "detectorid": "9612",
  "date_key": "20240701"
}
```

### 4.5 Data Preparation for Kafka

The producer script prepares each record for Kafka by:

1. **Adding Metadata:**
   ```python
   # Add producer timestamp (when the message was sent)
   current_record['producer_timestamp'] = int(time.time() * 1000)
   
   # Add sequence number for tracking
   current_record['stream_sequence'] = record_count
   
   # Copy the read_date as the event timestamp
   current_record['timestamp'] = current_record.get('read_date')
   ```

2. **Using Device ID as Partition Key:**
   ```python
   # All data from the same sensor goes to the same partition
   key = record.get('atd_device_id', str(record_index))
   ```

---

## 5. Part 3: Streaming Data Processing and Analysis

### 5.1 System Data Flow

```
┌────────────────────────────────────────────────────────────────────────┐
│                        DATA PROCESSING PIPELINE                         │
└────────────────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Dataset    │     │   Producer   │     │    Kafka     │
│  (JSONL)     │────►│   Script     │────►│  traffic_raw │
│              │     │              │     │    topic     │
└──────────────┘     └──────────────┘     └──────────────┘
                                                │
                                                │
                    ┌───────────────────────────┴────────────────────┐
                    │                                                │
                    ▼                                                ▼
          ┌──────────────────┐                           ┌──────────────────┐
          │   Sensor Data    │                           │                  │
          │   Consumer       │                           │   (Parallel)     │
          │                  │                           │   Processing     │
          │  • Hourly Avg    │                           │                  │
          │  • Availability  │                           │                  │
          └──────────────────┘                           └──────────────────┘
                    │
                    ├──────────────────────────────────────────┐
                    │                                          │
                    ▼                                          ▼
          ┌──────────────────┐                       ┌──────────────────┐
          │   Kafka Topic    │                       │     QuestDB      │
          │  hourly_average  │                       │    Database      │
          │                  │                       │                  │
          └──────────────────┘                       │  • hourly_average│
                    │                                │  • sensor_avail  │
                    │                                │  • running_data  │
                    ▼                                └──────────────────┘
          ┌──────────────────┐                                │
          │  Hourly Total    │                                │
          │   Consumer       │                                │
          │                  │                                │
          │  • Daily Peak    │                                │
          │  • Max Volume    │                                │
          └──────────────────┘                                │
                    │                                          │
                    ▼                                          │
          ┌──────────────────┐                                │
          │   QuestDB        │                                │
          │ daily_max_volume │◄───────────────────────────────┘
          └──────────────────┘
                    │
                    ▼
          ┌──────────────────┐
          │     Grafana      │
          │   Dashboard      │
          └──────────────────┘
```

### 5.2 Producer Logic - Sending Data to Kafka

The producer reads traffic data and sends it to Kafka, simulating real-time data collection.

#### Algorithm: Data Publishing

```
ALGORITHM: Publish Traffic Data to Kafka
=========================================

INPUT:  JSONL file with traffic records
OUTPUT: Messages published to 'traffic_raw' topic

BEGIN
    1. CONNECT to Kafka broker
    2. LOAD traffic records from file
    
    3. FOR each record in dataset:
        a. CREATE a copy of the record
        b. ADD producer_timestamp (current time)
        c. ADD stream_sequence (record number)
        d. ADD timestamp (event time from read_date)
        
        e. EXTRACT key = atd_device_id (for partitioning)
        f. SEND (key, record) to 'traffic_raw' topic
        g. WAIT 0.01 seconds (simulate real-time streaming)
        
    4. FLUSH remaining messages
    5. CLOSE producer connection
END
```

#### Code Implementation:

```python
# sensor-data-producer.py - Core Publishing Logic

def _send_record(self, record, record_index):
    """Send a single record to Kafka topic."""
    # Use device ID as partition key
    # Why? All data from same sensor goes to same partition
    # This ensures ordering for each sensor
    key = record.get('atd_device_id', str(record_index))
    
    # Send the record asynchronously
    future = self.producer.send(self.topic, key=key, value=record)
    
    # Add callbacks for monitoring success/failure
    future.add_callback(self._on_send_success, record_index, key)
    future.add_errback(self._on_send_error, record_index, key)
    
    return True

def start_streaming(self):
    """Main streaming loop."""
    while self.running and data_index < len(sensor_data):
        current_record = sensor_data[data_index].copy()
        
        # Add metadata
        current_record['producer_timestamp'] = int(time.time() * 1000)
        current_record['stream_sequence'] = record_count
        current_record['timestamp'] = current_record.get('read_date')
        
        # Send to Kafka
        if self._send_record(current_record, record_count):
            record_count += 1
            logger.info(f"Sent record #{record_count}: "
                       f"Device={current_record.get('atd_device_id')}, "
                       f"Volume={current_record.get('volume')}")
        
        data_index += 1
        time.sleep(0.01)  # Simulate real-time delay
```

### 5.3 Consumer Logic - Processing Metrics

The sensor data consumer calculates three key metrics:

1. **Hourly Average Vehicle Count** - Average traffic per sensor per hour
2. **Daily Sensor Availability** - Percentage of expected data received
3. **Running Hourly Data** - Real-time updates for visualization

#### Algorithm: Process Traffic Record

```
ALGORITHM: Process Traffic Record and Compute Metrics
=====================================================

INPUT:  Traffic record from Kafka (device_id, volume, timestamp)
OUTPUT: Metrics stored in QuestDB, published to Kafka

BEGIN
    1. PARSE the incoming record
       - Extract device_id, volume, timestamp
       - Convert timestamp to local time (UTC-6)
       
    2. CALCULATE hour_key (e.g., "2024-07-01 14:00")
    3. CALCULATE date_key (e.g., "2024-07-01")
    
    === HOURLY AVERAGE CALCULATION ===
    
    4. LOAD hourly state for this device from State Store
       - If no state exists: initialize {counts: [], last_hour: null}
       
    5. IF hour changed (last_hour != current_hour):
        a. CALCULATE average = sum(counts) / length(counts)
        b. EMIT hourly_average metric:
           {device_id, hour, average_vehicle_count, sample_count}
        c. STORE metric in QuestDB
        d. PUBLISH metric to 'hourly_average' topic
        e. RESET counts for new hour
        
    6. APPEND current volume to counts list
    7. SAVE updated state to State Store
    
    === SENSOR AVAILABILITY CALCULATION ===
    
    8. LOAD daily state for this device
       - If no state exists: initialize {data_points: 0, last_date: null}
       
    9. IF date changed (last_date != current_date):
        a. CALCULATE availability = (data_points / 288) * 100
           (288 = 24 hours * 12 readings per hour expected)
        b. EMIT availability metric
        c. STORE in QuestDB
        d. RESET for new day
        
    10. INCREMENT data_points counter
    11. SAVE updated state
    
    === RUNNING DATA FOR VISUALIZATION ===
    
    12. CALCULATE running_average = sum(current_counts) / length
    13. EMIT running_hourly_data metric
    14. STORE in QuestDB (for live dashboard)
END
```

#### Code Implementation:

```python
# sensor-data-consumer.py - Metric Processing Logic

def process_traffic_record(record: dict, state: State, producer) -> dict:
    """Process a single traffic record and compute metrics."""
    
    # Extract data from record
    device_id = record.get("atd_device_id")
    volume_str = record.get("volume", "0")
    vehicle_count = int(volume_str)  # Convert string to integer
    timestamp_str = record["timestamp"]
    
    # Parse timestamp and convert to Austin time (UTC-6)
    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    timestamp = timestamp - timedelta(hours=6)
    
    # Create time keys for grouping
    hour_key = timestamp.strftime("%Y-%m-%d %H:00")  # e.g., "2024-07-01 14:00"
    date_key = timestamp.strftime("%Y-%m-%d")        # e.g., "2024-07-01"
    
    # ═══════════════════════════════════════════════════════════
    # HOURLY AVERAGE CALCULATION
    # ═══════════════════════════════════════════════════════════
    
    # Load state from Quix State Store (persisted to Kafka changelog)
    hourly_state_key = f"{device_id}:hourly_tracker"
    hourly_state = state.get(hourly_state_key)
    
    if hourly_state:
        hourly_data = json.loads(hourly_state)
    else:
        hourly_data = {"counts": [], "last_hour": None}
    
    # Check if we moved to a new hour
    if hourly_data["last_hour"] is not None and hourly_data["last_hour"] != hour_key:
        # Hour changed! Calculate and emit the average for previous hour
        if hourly_data["counts"]:
            avg_count = sum(hourly_data["counts"]) / len(hourly_data["counts"])
            
            # Create the hourly average metric
            metric = {
                "metric_type": "hourly_average",
                "device_id": device_id,
                "hour": hourly_data["last_hour"],
                "average_vehicle_count": round(avg_count, 2),
                "sample_count": len(hourly_data["counts"]),
                "timestamp": timestamp.isoformat()
            }
            
            # Store in database and publish to Kafka
            store_metric_to_questdb(metric)
            emit_metric_to_kafka(metric, producer)
            
        # Reset for new hour
        hourly_data = {"counts": [vehicle_count], "last_hour": hour_key}
    else:
        # Same hour - add to running count
        hourly_data["counts"].append(vehicle_count)
        hourly_data["last_hour"] = hour_key
    
    # Save state (will be persisted to Kafka changelog topic)
    state.set(hourly_state_key, json.dumps(hourly_data))
    
    # ═══════════════════════════════════════════════════════════
    # EMIT RUNNING HOURLY DATA (for real-time visualization)
    # ═══════════════════════════════════════════════════════════
    
    if hourly_data["counts"]:
        running_avg = sum(hourly_data["counts"]) / len(hourly_data["counts"])
        running_metric = {
            "metric_type": "running_hourly_data",
            "device_id": device_id,
            "hour": hour_key,
            "current_average": round(running_avg, 2),
            "current_count": vehicle_count,
            "sample_count": len(hourly_data["counts"]),
            "timestamp": timestamp.isoformat()
        }
        store_metric_to_questdb(running_metric)
    
    return record
```

### 5.4 Hourly Total Consumer - Daily Peak Calculation

This consumer aggregates hourly totals across all devices and tracks daily peak volume.

#### Algorithm: Calculate Daily Peak Volume

```
ALGORITHM: Calculate Daily Peak Traffic Volume
==============================================

INPUT:  Hourly average messages from 'hourly_average' topic
OUTPUT: Daily maximum hourly volume stored in QuestDB

BEGIN
    FOR each hourly_average message:
        1. EXTRACT device_id, hour, average_vehicle_count, sample_count
        2. CALCULATE contribution = average * sample_count
        
        3. LOAD hourly aggregation state
        4. UPDATE hourly_bucket with device contribution
        
        5. CHECK if hour should be finalized:
           - IF watermark >= hour_deadline (hour_end + 10 minutes):
             a. GET total volume for the hour
             b. EXTRACT date from hour
             c. UPDATE daily_max bucket if total > current_max
             d. MARK hour as finalized
             
        6. CHECK if date should be finalized:
           - IF watermark >= date_deadline (date_end + 10 minutes):
             a. WRITE daily_max to QuestDB:
                {date, max_hourly_total, max_hour}
             b. MARK date as finalized
             
        7. SAVE updated state
END
```

#### Code Implementation:

```python
# hourly-total-consumer.py - Daily Peak Calculation

def process_record(value: dict, state: State):
    """Process hourly average and track daily peak volume."""
    
    # Extract data
    device_id = value.get("device_id")
    hour = value.get("hour")  # "YYYY-MM-DD HH:00"
    avg = float(value.get("average_vehicle_count", 0.0))
    cnt = int(value.get("sample_count", 0))
    
    # Calculate this device's contribution to the hour
    contribution = int(round(avg * cnt))
    
    # Load state for hour buckets
    hours_json = state.get("agg:hours")
    hours = json.loads(hours_json) if hours_json else {}
    
    # Get or create bucket for this hour
    hour_bucket = hours.get(hour)
    if not hour_bucket:
        hour_start = _parse_hour_start(hour)
        deadline = hour_start + timedelta(hours=1, minutes=LATE_THRESHOLD_MIN)
        hour_bucket = {
            "total": 0,
            "finalized": False,
            "deadline": deadline.isoformat(),
            "devices": {}
        }
    
    # Update device contribution (idempotent)
    devices = hour_bucket.get("devices", {})
    prev_contribution = int(devices.get(device_id, 0))
    if prev_contribution != contribution:
        devices[device_id] = contribution
        hour_bucket["devices"] = devices
        # Update total: subtract old, add new
        hour_bucket["total"] = hour_bucket["total"] - prev_contribution + contribution
    
    hours[hour] = hour_bucket
    
    # Check for hours ready to finalize
    watermark = get_current_watermark(state)
    
    for h, bucket in list(hours.items()):
        if not bucket.get("finalized"):
            deadline = datetime.fromisoformat(bucket["deadline"])
            if watermark >= deadline:
                # Finalize this hour
                total = bucket.get("total", 0)
                date = h.split(" ")[0]  # Extract date
                
                # Update daily max
                daily_max = load_daily_max_state(state)
                if date not in daily_max:
                    daily_max[date] = {
                        "max_total": 0,
                        "max_hour": h,
                        "finalized": False
                    }
                
                if total > daily_max[date]["max_total"]:
                    daily_max[date]["max_total"] = total
                    daily_max[date]["max_hour"] = h
                    logger.info(f"New daily max for {date}: {total} at hour {h}")
                
                bucket["finalized"] = True
                del hours[h]  # Free memory
                
                save_daily_max_state(state, daily_max)
    
    state.set("agg:hours", json.dumps(hours))

def store_daily_max(date: str, max_total: int, max_hour: str):
    """Write daily maximum to QuestDB."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO daily_max_hourly_volume (ts, date, max_hourly_total, max_hour)
            VALUES (%s, %s, %s, %s)
        """, (datetime.now(), date, max_total, max_hour))
        conn.commit()
    finally:
        db_pool.putconn(conn)
```

### 5.5 State Management with Quix Streams

Our solution uses Quix Streams for stateful processing. Here's why it's important:

**Problem:** In streaming, data arrives continuously. How do we remember previous data to calculate averages and totals?

**Solution:** State Store - A persistent key-value store that:
- Remembers data across messages
- Automatically backs up to Kafka (changelog topic)
- Recovers automatically if the consumer crashes

```python
# How state is used in our consumer

# Save state
state.set("device123:hourly_tracker", json.dumps({
    "counts": [42, 38, 45],
    "last_hour": "2024-07-01 14:00"
}))

# Load state (even after restart)
data = json.loads(state.get("device123:hourly_tracker"))
```

### 5.6 Database Tables Created

```sql
-- QuestDB Tables for Metric Storage

-- Hourly average per sensor
CREATE TABLE IF NOT EXISTS hourly_average (
    timestamp timestamp,
    device_id symbol,          -- Sensor identifier
    hour symbol,               -- "2024-07-01 14:00"
    average_vehicle_count double,
    sample_count int
) timestamp(timestamp) partition by DAY;

-- Daily sensor availability
CREATE TABLE IF NOT EXISTS sensor_availability (
    timestamp timestamp,
    device_id symbol,
    date symbol,
    availability_percentage double,
    data_points_received int
) timestamp(timestamp) partition by DAY;

-- Running hourly data for live visualization
CREATE TABLE IF NOT EXISTS running_hourly_data (
    timestamp timestamp,
    device_id symbol,
    hour symbol,
    current_average double,
    current_count int,
    sample_count int
) timestamp(timestamp) partition by DAY;

-- Daily peak volume
CREATE TABLE IF NOT EXISTS daily_max_hourly_volume (
    ts timestamp,
    date symbol,
    max_hourly_total long,
    max_hour symbol
) timestamp(ts) PARTITION BY DAY;
```

---

## 6. Part 4: Visualization and Reporting

### 6.1 Grafana Dashboard Overview

Our Grafana dashboard provides three key visualizations:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      SENSOR DATA METRICS DASHBOARD                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌────────────────────────────┐  ┌────────────────────────────────────┐ │
│  │   Sensor Availability      │  │   Daily Peak Traffic Volume        │ │
│  │        (Gauge)             │  │         (Bar Chart)                │ │
│  │                            │  │                                    │ │
│  │   ┌────┐  ┌────┐  ┌────┐  │  │    ████                            │ │
│  │   │92% │  │88% │  │95% │  │  │    ████  ████                      │ │
│  │   └────┘  └────┘  └────┘  │  │    ████  ████  ████                │ │
│  │  Device1 Device2 Device3  │  │    Mon   Tue   Wed                 │ │
│  │                            │  │                                    │ │
│  └────────────────────────────┘  └────────────────────────────────────┘ │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │              Hourly Average Vehicle Count per Sensor              │  │
│  │                       (Time Series)                               │  │
│  │                                                                   │  │
│  │    ╱╲    ╱╲     Device 1                                         │  │
│  │   ╱  ╲  ╱  ╲    Device 2                                         │  │
│  │  ╱    ╲╱    ╲   Device 3                                         │  │
│  │ ╱            ╲                                                    │  │
│  │──────────────────────────────────────────────────────────────────│  │
│  │ 00:00    06:00    12:00    18:00    24:00                        │  │
│  │                                                                   │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Panel 1: Sensor Availability (Gauge)

**Purpose:** Shows how reliable each sensor is - what percentage of expected data was received.

**Color Coding:**
- 🔴 **Red:** < 70% - Sensor needs attention
- 🟡 **Yellow:** 70-90% - Acceptable but could improve
- 🟢 **Green:** > 90% - Sensor working well

**SQL Query:**
```sql
SELECT 
    availability_percentage, 
    'ID='||device_id 
FROM "sensor_availability" 
LATEST ON timestamp 
PARTITION BY device_id 
ORDER BY timestamp DESC 
LIMIT 100
```

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: Sensor Availability Gauge Panel]
```

### 6.3 Panel 2: Daily Peak Traffic Volume (Bar Chart)

**Purpose:** Shows the highest traffic volume recorded each day - helps identify busy days.

**SQL Query:**
```sql
SELECT 
    date, 
    max_hourly_total as value 
FROM "daily_max_hourly_volume"
```

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: Daily Peak Traffic Volume Bar Chart]
```

### 6.4 Panel 3: Hourly Average Vehicle Count (Time Series)

**Purpose:** Shows traffic patterns throughout the day for each sensor.

**What to Look For:**
- Peak hours (usually morning and evening commute)
- Quiet periods (late night)
- Patterns that repeat daily

**SQL Query:**
```sql
SELECT 
    timestamp, 
    device_id, 
    current_average 
FROM "running_hourly_data" 
LATEST ON timestamp 
PARTITION BY device_id, hour
```

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: Hourly Average Vehicle Count Time Series]
```

### 6.5 Grafana Data Source Configuration

```yaml
# grafana/provisioning/datasources/datasources.yml

apiVersion: 1
datasources:
  - name: QuestDB
    type: questdb-questdb-datasource
    jsonData:
      server: questdb      # Container name
      port: 8812           # PostgreSQL protocol port
      username: admin
      tlsMode: disable
      maxOpenConnections: 100
    secureJsonData:
      password: quest
```

### 6.6 Dashboard Configuration

```yaml
# grafana/provisioning/dashboards/dashboards.yml

apiVersion: 1
providers:
  - name: 'default'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /var/lib/grafana/dashboards
```

---

## 7. Results and Screenshots

This section contains placeholders for actual results and screenshots from your running application.

### 7.1 Service Status

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: Docker Compose ps output showing all services running]

Expected output:
NAME                      STATUS          PORTS
kafka-broker              Up              0.0.0.0:9092->9092/tcp
questdb                   Up              0.0.0.0:9000->9000/tcp, 0.0.0.0:8812->8812/tcp
grafana                   Up              0.0.0.0:3000->3000/tcp
redpanda-console          Up              0.0.0.0:8083->8080/tcp
sensor-data-producer      Up              
sensor-data-consumer      Up              
hourly-total-consumer     Up              
```

### 7.2 Kafka Topics and Messages

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: Redpanda Console showing Kafka topics]
- traffic_raw topic with partitions
- hourly_average topic with messages
- metric_availability topic
```

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: Sample message in traffic_raw topic]
Expected format:
{
  "atd_device_id": "b2fd...",
  "intersection_name": "BURNET RD / PALM WAY",
  "volume": "42",
  "timestamp": "2024-07-01T00:00:00.000",
  "producer_timestamp": 1719792000000,
  "stream_sequence": 0
}
```

### 7.3 Producer Logs

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: Producer container logs]
Expected output:
2024-XX-XX 12:00:00 - INFO - === Sensor Data Producer Started ===
2024-XX-XX 12:00:00 - INFO - Kafka producer created successfully
2024-XX-XX 12:00:00 - INFO - Loaded 50000 sensor records
2024-XX-XX 12:00:01 - INFO - Sent record #1: Device=b2fd..., Volume=42
2024-XX-XX 12:00:01 - INFO - Sent record #2: Device=b2fd..., Volume=38
...
```

### 7.4 Consumer Logs

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: Sensor data consumer logs]
Expected output:
2024-XX-XX 12:00:30 - INFO - Starting Traffic Metrics Application
2024-XX-XX 12:00:30 - INFO - QuestDB tables initialized
2024-XX-XX 12:00:31 - INFO - Hourly metric emitted: device123 - 2024-07-01 14:00
2024-XX-XX 12:00:32 - INFO - Daily metrics emitted: device123 - 2024-07-01
...
```

### 7.5 QuestDB Data

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: QuestDB console showing hourly_average table data]

Query: SELECT * FROM hourly_average LIMIT 10;

Expected results table showing:
| timestamp | device_id | hour | average_vehicle_count | sample_count |
|-----------|-----------|------|----------------------|--------------|
| ...       | b2fd...   | 2024-07-01 14:00 | 45.67 | 12 |
```

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: QuestDB console showing sensor_availability table]

Query: SELECT * FROM sensor_availability LIMIT 10;
```

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: QuestDB console showing daily_max_hourly_volume table]

Query: SELECT * FROM daily_max_hourly_volume ORDER BY ts DESC LIMIT 10;
```

### 7.6 Grafana Dashboard

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: Complete Grafana dashboard view]
URL: http://localhost:3000
Dashboard: Sensor Data Metrics
```

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: Sensor Availability gauge panel - close up]
```

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: Daily Peak Traffic bar chart - close up]
```

**📸 Screenshot Placeholder:**
```
[INSERT SCREENSHOT: Hourly Average time series - close up]
```

### 7.7 Performance Metrics

**Fill in these metrics from your running system:**

| Metric | Value |
|--------|-------|
| Total records processed | [INSERT VALUE] |
| Average processing time per record | [INSERT VALUE] ms |
| Kafka throughput | [INSERT VALUE] messages/second |
| QuestDB write latency | [INSERT VALUE] ms |
| End-to-end latency (producer to dashboard) | [INSERT VALUE] seconds |

---

## 8. Conclusion and Future Improvements

### 8.1 What We Achieved

✅ **Real-time Data Streaming** - Successfully implemented a Kafka-based pipeline that streams traffic sensor data

✅ **Metric Computation** - Calculated:
- Hourly average vehicle count per sensor
- Daily peak traffic volume
- Sensor availability percentage

✅ **Persistent Storage** - Stored metrics in QuestDB for historical analysis

✅ **Live Visualization** - Created Grafana dashboards with real-time updates

✅ **Fault Tolerance** - Used Quix Streams state store for reliable processing

### 8.2 Key Learnings

1. **Kafka Partitioning** - Using device ID as the key ensures all data from one sensor goes to the same partition, maintaining order

2. **Event-Time Processing** - Using the actual sensor timestamp (not processing time) gives accurate results

3. **Late Data Handling** - Implementing watermarks and deadlines prevents incorrect results from late-arriving data

4. **State Management** - Stateful stream processing enables complex calculations like running averages

### 8.3 Future Improvements

| Improvement | Description | Benefit |
|-------------|-------------|---------|
| **Kafka Cluster** | Deploy multiple Kafka brokers | Higher availability, fault tolerance |
| **Schema Registry** | Add Avro/Protobuf schemas | Data validation, versioning |
| **Alerting** | Add Grafana alerts | Notify when sensors fail |
| **Machine Learning** | Add anomaly detection | Predict traffic congestion |
| **More Sensors** | Integrate additional data sources | Richer analysis |

### 8.4 Files Included in This Submission

```
task2/
├── REPORT.md                 # This comprehensive report
├── README.md                 # Quick start guide
├── GRAFANA_DASHBOARD_GUIDE.md # Dashboard usage guide
├── Makefile                  # Build and run commands
├── docker-compose.yml        # Service configuration
├── data/
│   └── .gitkeep             # Data directory (dataset downloaded separately)
├── scripts/
│   ├── download-dataset.py   # Dataset downloader
│   ├── sensor-data-producer.py   # Kafka producer
│   ├── sensor-data-consumer.py   # Metrics consumer
│   ├── hourly-total-consumer.py  # Daily peak consumer
│   └── start-producer.sh     # Producer startup script
└── grafana/
    ├── provisioning/
    │   ├── datasources/
    │   │   └── datasources.yml   # QuestDB connection
    │   └── dashboards/
    │       └── dashboards.yml    # Dashboard provisioning
    └── dashboards/
        └── sensor-data-metrics.json  # Dashboard definition
```

---

## Appendix A: Commands Reference

### Starting the System
```bash
# Download the dataset
make download-dataset

# Start all services
make up

# View logs
make logs

# Check service status
make ps
```

### Stopping the System
```bash
# Stop all services
make down

# Stop and remove all data
make clean
```

### Accessing Services
```bash
# Grafana
open http://localhost:3000  # admin/admin

# QuestDB
open http://localhost:9000

# Kafka UI (Redpanda Console)
open http://localhost:8083
```

### Useful Kafka Commands (inside kafka-broker container)
```bash
# List topics
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe a topic
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --describe --topic traffic_raw --bootstrap-server localhost:9092

# Read messages from a topic
docker exec kafka-broker /opt/kafka/bin/kafka-console-consumer.sh --topic traffic_raw --from-beginning --bootstrap-server localhost:9092 --max-messages 5
```

---

## Appendix B: Troubleshooting

| Problem | Solution |
|---------|----------|
| Services won't start | Run `docker-compose logs` to check errors |
| No data in Grafana | Wait 2-3 minutes for data to flow through |
| Kafka connection refused | Ensure kafka-broker is healthy: `docker-compose ps` |
| QuestDB tables empty | Check consumer logs: `docker-compose logs sensor-data-consumer` |
| Producer exits immediately | Ensure dataset file exists: `ls -la data/traffic.jsonl` |

---

**End of Report**

*Prepared as part of the Big Data Analytics course - Real-Time IoT Sensor Data Analytics using Apache Kafka*
