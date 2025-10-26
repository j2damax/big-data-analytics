# 🌊 Apache Flink Stream Processing

**Apache Flink 1.18.0** - Unified stream and batch processing engine with stateful computations, event-time processing, and exactly-once guarantees for mission-critical applications.

## 🏗️ **Distributed Stream Processing Architecture**

| Component | Role | Container | Capabilities |
|-----------|------|-----------|-------------|
| **JobManager** | Master coordinator | `flink-jobmanager:8082` | Job scheduling, checkpointing, recovery |
| **TaskManager** | Worker executor | `flink-taskmanager:6121-6125` | Task execution, state management, data exchange |
| **DataStream API** | Core streaming abstraction | Application layer | Event-driven processing, windowing |
| **Table API/SQL** | Relational stream processing | Application layer | SQL queries on infinite streams |
| **PyFlink** | Python development interface | Runtime integration | Pythonic stream processing |

## 🚀 **True Stream Processing Benefits**

### **Real-Time Processing Advantages**
- **⚡ Low Latency**: Sub-second event processing with millisecond response times
- **🔒 Exactly-Once**: Guaranteed state consistency even with failures
- **⏰ Event Time**: Process events based on when they occurred, not when processed
- **🌊 Infinite Streams**: Handle unbounded data streams with constant memory usage
- **🔄 Stateful Operations**: Maintain application state across events and failures
- **📊 Complex Analytics**: Windowing, joins, and aggregations on streaming data

## 🚀 **Quick Start**

```bash
# Access Flink cluster
make shell-flink  # Interactive PyFlink environment

# Test stream processing
make test-flink   # DataStream + Table API examples

# Web dashboard:
# 🌐 Flink Dashboard: http://localhost:8082 (jobs, metrics, checkpoints)
```

## 💻 **DataStream API Development**

### **Basic Stream Processing**
```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types

# Create execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)

# Simple transformation pipeline
data_stream = env.from_collection(
    collection=[('Alice', 1), ('Bob', 2), ('Charlie', 3)],
    type_info=Types.TUPLE([Types.STRING(), Types.INT()])
)

# Transform and process
result = data_stream \
    .map(lambda x: (x[0], x[1] * 2)) \
    .filter(lambda x: x[1] > 2) \
    .key_by(lambda x: x[0]) \
    .sum(1)

result.print()
env.execute("Simple Processing Job")
```

### **Advanced Stream Operations**
```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, FilterFunction
from pyflink.common import Types

class EventProcessor(MapFunction):
    def map(self, value):
        # Custom event processing logic
        user_id, event_type, timestamp = value
        return {
            'user_id': user_id,
            'event_type': event_type.upper(),
            'processed_at': timestamp,
            'hour_of_day': timestamp % (24 * 3600) // 3600
        }

class HighValueFilter(FilterFunction):
    def filter(self, value):
        return value.get('importance', 0) > 0.8

# Apply custom functions
env = StreamExecutionEnvironment.get_execution_environment()
stream = env.from_source(kafka_source, watermark_strategy, "Kafka Source")

processed_stream = stream \
    .map(EventProcessor()) \
    .filter(HighValueFilter()) \
    .key_by(lambda x: x['user_id'])

processed_stream.print()
env.execute("Advanced Event Processing")
```

## ⏰ **Event Time & Windowing**

### **Watermark and Event Time Configuration**
```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common import WatermarkStrategy, Time
from pyflink.common.watermark_strategy import TimestampAssigner

class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        # Extract event timestamp from your data
        return element[2] * 1000  # Convert to milliseconds

# Configure watermarks for late event handling
watermark_strategy = WatermarkStrategy \
    .for_bounded_out_of_orderness(Time.seconds(5)) \
    .with_timestamp_assigner(EventTimestampAssigner())

env = StreamExecutionEnvironment.get_execution_environment()
stream = env.from_source(source, watermark_strategy, "Events")

# Tumbling windows (non-overlapping)
windowed_stream = stream \
    .key_by(lambda x: x[0]) \
    .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
    .reduce(lambda a, b: (a[0], a[1] + b[1], max(a[2], b[2])))

# Sliding windows (overlapping)
sliding_windowed = stream \
    .key_by(lambda x: x[0]) \
    .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(2))) \
    .aggregate(EventAggregator())

windowed_stream.print()
env.execute("Windowed Event Processing")
```

### **Session Windows & Custom Triggers**
```python
from pyflink.datastream.window import ProcessWindowFunction, SessionWindows
from pyflink.common import Time

class SessionProcessor(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        # Process session window
        session_events = list(elements)
        session_duration = context.window().get_end() - context.window().get_start()
        
        result = {
            'user_id': key,
            'session_start': context.window().get_start(),
            'session_end': context.window().get_end(),
            'duration_ms': session_duration,
            'event_count': len(session_events),
            'events': session_events
        }
        out.collect(result)

# Session windows based on inactivity gap
session_stream = stream \
    .key_by(lambda x: x['user_id']) \
    .window(SessionWindows.with_gap(Time.minutes(15))) \
    .process(SessionProcessor())

session_stream.print()
env.execute("Session Analysis")
```

## 📊 **Table API & SQL**

### **Unified Batch & Stream SQL**
```python
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream import StreamExecutionEnvironment

# Create table environment  
env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = StreamTableEnvironment.create(env, settings)

# Define source table (Kafka, files, etc.)
table_env.execute_sql("""
    CREATE TABLE user_events (
        user_id STRING,
        event_type STRING,
        event_time TIMESTAMP(3),
        properties MAP<STRING, STRING>,
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'user-events',  
        'properties.bootstrap.servers' = 'kafka:9092',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
""")

# Complex streaming SQL query
result = table_env.sql_query("""
    SELECT 
        user_id,
        event_type,
        COUNT(*) as event_count,
        TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start,
        TUMBLE_END(event_time, INTERVAL '1' MINUTE) as window_end
    FROM user_events
    WHERE event_type IN ('login', 'purchase', 'logout')
    GROUP BY 
        user_id, 
        event_type,
        TUMBLE(event_time, INTERVAL '1' MINUTE)
    HAVING COUNT(*) > 5
""")

# Output results
table_env.execute_sql("""
    CREATE TABLE aggregated_events (
        user_id STRING,
        event_type STRING, 
        event_count BIGINT,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'aggregated-events',
        'properties.bootstrap.servers' = 'kafka:9092', 
        'format' = 'json'
    )
""")

result.execute_insert("aggregated_events")
```

### **Stream Joins & Enrichment**
```python
# Join streams with different time characteristics
user_events = table_env.from_path("user_events")
user_profiles = table_env.from_path("user_profiles") 

# Temporal join (stream with versioned table)
enriched_events = table_env.sql_query("""
    SELECT 
        e.user_id,
        e.event_type,
        e.event_time,
        p.user_name,
        p.user_tier,
        p.registration_date
    FROM user_events e
    JOIN user_profiles FOR SYSTEM_TIME AS OF e.event_time AS p
    ON e.user_id = p.user_id
    WHERE e.event_type = 'purchase'
""")

# Interval join (correlate events within time window)
correlation_query = table_env.sql_query("""
    SELECT 
        l.user_id,
        l.event_time as login_time,
        p.event_time as purchase_time,
        p.event_time - l.event_time as time_to_purchase
    FROM user_events l
    JOIN user_events p ON l.user_id = p.user_id
    WHERE 
        l.event_type = 'login' 
        AND p.event_type = 'purchase'
        AND p.event_time BETWEEN l.event_time AND l.event_time + INTERVAL '1' HOUR
""")

enriched_events.execute().print()
```

## 🔧 **Job Submission & Management**

### **Production Job Deployment**
```bash
# Submit job with configuration
flink run \
  --class com.example.FlinkJob \
  --parallelism 4 \
  --detached \
  /path/to/job.jar \
  --input kafka://kafka:9092/input-topic \
  --output kafka://kafka:9092/output-topic

# Python job submission
flink run \
  --python /scripts/production_job.py \
  --parallelism 6 \
  --jobmanager-memory 1024m \
  --taskmanager-memory 2048m

# Job management
flink list                    # List running jobs
flink info <job-id>           # Job details  
flink cancel <job-id>         # Graceful stop
flink stop <job-id>           # Stop with savepoint
```

### **Savepoints & Recovery**
```bash
# Create savepoint for job migration/upgrade
flink savepoint <job-id> /path/to/savepoints/

# Start job from savepoint
flink run \
  --fromSavepoint /path/to/savepoints/savepoint-123456 \
  --parallelism 8 \
  /path/to/updated-job.jar

# List available savepoints
flink savepoint --dispose /path/to/old-savepoint
```

## 📈 **State Management & Fault Tolerance**

### **Keyed State Operations**
```python
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types

class StatefulEventProcessor(KeyedProcessFunction):
    def __init__(self):
        self.user_session_state = None
        
    def open(self, runtime_context):
        # Initialize state descriptors
        self.user_session_state = runtime_context.get_state(
            ValueStateDescriptor(
                "user_session",
                Types.PICKLED_BYTE_ARRAY()
            )
        )
    
    def process_element(self, value, ctx, out):
        # Access and update keyed state
        current_session = self.user_session_state.value()
        
        if current_session is None:
            current_session = {
                'start_time': ctx.timestamp(),
                'event_count': 0,
                'last_activity': ctx.timestamp()
            }
        
        # Update session state
        current_session['event_count'] += 1
        current_session['last_activity'] = ctx.timestamp()
        
        # Check for session timeout (15 minutes)
        if ctx.timestamp() - current_session['last_activity'] > 15 * 60 * 1000:
            # Output completed session
            out.collect({
                'user_id': value['user_id'],
                'session_duration': current_session['last_activity'] - current_session['start_time'],
                'event_count': current_session['event_count']
            })
            # Reset session state
            current_session = None
            
        self.user_session_state.update(current_session)

# Apply stateful processing
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(60000)  # Checkpoint every minute

stream.key_by(lambda x: x['user_id']) \
      .process(StatefulEventProcessor()) \
      .print()
```

### **Checkpointing Configuration**
```python
from pyflink.datastream import CheckpointingMode

env = StreamExecutionEnvironment.get_execution_environment()

# Enable checkpointing with exactly-once semantics
env.enable_checkpointing(30000, CheckpointingMode.EXACTLY_ONCE)

# Configure checkpoint behavior
checkpoint_config = env.get_checkpoint_config()
checkpoint_config.set_min_pause_between_checkpoints(5000)
checkpoint_config.set_checkpoint_timeout(60000)
checkpoint_config.set_max_concurrent_checkpoints(1)
checkpoint_config.enable_externalized_checkpoints(True)

# Set state backend (RocksDB for large state)
env.set_state_backend("rocksdb")
```

## 🌐 **Web Dashboard Deep Dive**

### **Flink Dashboard** (http://localhost:8082)

| Section | Information | Use Cases |
|---------|-------------|-----------|
| **Overview** | Cluster status, running jobs, available slots | Health monitoring, capacity planning |
| **Jobs** | Job execution graphs, parallelism, duration | Performance analysis, debugging |
| **Task Managers** | Memory usage, CPU, network I/O | Resource optimization, bottleneck identification |
| **Checkpoints** | Checkpoint duration, size, success rate | Fault tolerance monitoring, state size analysis |
| **Configuration** | JVM settings, Flink configuration | Environment verification, tuning reference |

### **Job Execution Monitoring**
```bash
# REST API access (alternative to web UI)
curl http://localhost:8082/jobs                    # List jobs
curl http://localhost:8082/jobs/<job-id>           # Job details
curl http://localhost:8082/jobs/<job-id>/metrics   # Job metrics

# Metrics integration (Prometheus format)
curl http://localhost:8082/metrics
```

## 🔄 **Streaming Connectors & Integration**

### **Kafka Integration** (Most Common)
```python
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema

# Kafka source configuration
kafka_source = FlinkKafkaConsumer(
    topics=['user-events'],
    deserialization_schema=SimpleStringSchema(),
    properties={
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-processors',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': 'false'  # Flink manages offsets
    }
)

# Kafka sink configuration  
kafka_sink = FlinkKafkaProducer(
    topic='processed-events',
    serialization_schema=SimpleStringSchema(),
    producer_config={
        'bootstrap.servers': 'kafka:9092',
        'acks': '1',
        'compression.type': 'lz4'
    }
)

env = StreamExecutionEnvironment.get_execution_environment()
stream = env.add_source(kafka_source)

# Process and sink to Kafka
processed_stream = stream \
    .map(lambda x: json.dumps({'processed': x, 'timestamp': int(time.time())}))

processed_stream.add_sink(kafka_sink)
env.execute("Kafka Processing Pipeline")
```

### **HDFS/File System Integration**
```python
# File source (batch or streaming)
file_source = env.read_text_file("hdfs://hadoop:9000/user/data/input/")

# File sink with rolling policy
file_sink = StreamingFileSink \
    .for_row_format("/tmp/flink-output", SimpleStringEncoder()) \
    .with_rolling_policy(
        DefaultRollingPolicy.builder()
        .with_rollover_interval(60000)  # Roll every minute
        .with_inactivity_interval(30000) # Roll after 30s inactivity
        .build()
    ) \
    .build()

stream.add_sink(file_sink)
```

## 🏭 **Production Patterns & Best Practices**

### **Complex Event Processing (CEP)**
```python
from pyflink.cep import CEP, Pattern
from pyflink.cep.pattern_stream import PatternStream

# Define event pattern (fraud detection example)
pattern = Pattern.begin("login") \
    .where(lambda event: event['event_type'] == 'login') \
    .next("failed_payment") \
    .where(lambda event: event['event_type'] == 'payment_failed') \
    .within(Time.minutes(5))

# Apply pattern to keyed stream
pattern_stream = CEP.pattern(
    stream.key_by(lambda x: x['user_id']), 
    pattern
)

# Process matched patterns
alerts = pattern_stream.process(FraudAlertFunction())
alerts.print()
```

### **Backpressure Handling**
```python
# Configure backpressure and resource limits
env = StreamExecutionEnvironment.get_execution_environment()

# Buffer timeout for network efficiency
env.get_config().set_auto_watermark_interval(1000)
env.set_buffer_timeout(100)  # Milliseconds

# Parallelism configuration
env.set_parallelism(4)  # Global default
stream.set_parallelism(8)  # Operator-specific

# Resource limits
env.get_config().set_task_cancellation_timeout(30000)
```

### **Monitoring & Alerting Setup**
```python
# Custom metrics
from pyflink.common import Types
from pyflink.datastream.functions import RichMapFunction

class MetricsMapFunction(RichMapFunction):
    def __init__(self):
        self.processed_counter = None
        self.processing_time_histogram = None
    
    def open(self, runtime_context):
        self.processed_counter = runtime_context \
            .get_metrics_group() \
            .counter("events_processed")
            
        self.processing_time_histogram = runtime_context \
            .get_metrics_group() \
            .histogram("processing_time_ms", HistogramReporter())
    
    def map(self, value):
        start_time = time.time()
        
        # Your processing logic
        result = process_event(value)
        
        # Record metrics
        self.processed_counter.inc()
        processing_time = (time.time() - start_time) * 1000
        self.processing_time_histogram.update(processing_time)
        
        return result

# Apply with metrics
stream.map(MetricsMapFunction())
```

## 🎯 **Advanced Use Cases**

### **1. Real-Time Feature Store**
```python
# Online feature computation for ML models
feature_stream = user_events \
    .key_by(lambda x: x['user_id']) \
    .window(SlidingEventTimeWindows.of(Time.hours(24), Time.minutes(15))) \
    .aggregate(FeatureAggregator()) \
    .map(lambda x: {
        'user_id': x.user_id,
        'features': {
            'avg_session_duration': x.avg_session_duration,
            'purchase_frequency': x.purchase_frequency,
            'last_activity_hours': x.last_activity_hours
        },
        'computed_at': int(time.time() * 1000)
    })

# Serve features to online systems
feature_stream.add_sink(redis_sink)  # Real-time serving
feature_stream.add_sink(hdfs_sink)   # Batch training data
```

### **2. Multi-Stream Correlation**
```python
# Correlate user behavior across different event streams
login_stream = env.add_source(kafka_login_source)
purchase_stream = env.add_source(kafka_purchase_source)
support_stream = env.add_source(kafka_support_source)

# Combine streams with different processing logic
combined_stream = login_stream \
    .union(purchase_stream) \
    .union(support_stream) \
    .key_by(lambda x: x['user_id']) \
    .process(UserBehaviorCorrelator())

# Output user journey insights
combined_stream \
    .filter(lambda x: x['anomaly_score'] > 0.8) \
    .add_sink(alert_sink)
```

### **3. Streaming ETL Pipeline**
```python
# Real-time data cleansing and transformation
raw_events \
    .filter(EventValidator()) \
    .map(EventNormalizer()) \
    .key_by(lambda x: x['partition_key']) \
    .process(EventEnricher()) \
    .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
    .aggregate(EventBatcher()) \
    .add_sink(data_warehouse_sink)
```

## 📊 **Performance Tuning Guide**

### **Memory Optimization**
```bash
# TaskManager memory configuration
taskmanager.memory.process.size=2048m
taskmanager.memory.managed.fraction=0.4
taskmanager.memory.network.fraction=0.15

# State backend optimization
state.backend=rocksdb
state.backend.rocksdb.block.cache-size=64mb
state.backend.rocksdb.writebuffer.size=32mb
```

### **Checkpoint Optimization**
```python
# Optimize checkpoint performance
env.enable_checkpointing(300000)  # 5 minutes for large state
checkpoint_config = env.get_checkpoint_config()
checkpoint_config.set_checkpoint_storage("hdfs://hadoop:9000/checkpoints")
checkpoint_config.set_min_pause_between_checkpoints(60000)
checkpoint_config.set_max_concurrent_checkpoints(1)
```

### **Network Optimization**
```bash
# Network buffer configuration
taskmanager.memory.network.min=64mb
taskmanager.memory.network.max=1gb
taskmanager.network.number-of-buffers=8192
```

## 🚨 **Troubleshooting Common Issues**

### **Job Failures**
```bash
# Check job logs
docker exec flink-jobmanager tail -f /opt/flink/log/flink-*.log

# Inspect failed job
flink info <failed-job-id>
curl http://localhost:8082/jobs/<job-id>/exceptions

# Restart from last checkpoint
flink run --fromSavepoint /path/to/checkpoint /path/to/job.jar
```

### **Performance Issues**
```bash
# Check backpressure
curl http://localhost:8082/jobs/<job-id>/vertices/<vertex-id>/backpressure

# Analyze checkpoint duration
curl http://localhost:8082/jobs/<job-id>/checkpoints

# Monitor memory usage
curl http://localhost:8082/taskmanagers/<tm-id>/metrics
```

### **State Size Issues**
```python
# Monitor state size growth
@state_size_monitor
def process_element(self, value, ctx, out):
    # Your processing logic
    if ctx.timestamp() % 60000 == 0:  # Every minute
        state_size = self.get_state_size()
        self.metrics.gauge("state_size_mb", state_size / (1024 * 1024))
```
