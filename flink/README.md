# Flink Setup

This directory contains the Dockerfile for Apache Flink.

## Components

- **JobManager**: Coordinates distributed execution
- **TaskManager**: Executes tasks and data streams
- **DataStream API**: For stream processing
- **Table API**: For relational operations
- **PyFlink**: Python API for Flink

## Features

- True stream processing (not micro-batching)
- Event time processing and windowing
- Exactly-once state consistency
- Low latency and high throughput
- Support for batch and stream processing

## Usage

### Accessing Flink

```bash
# Enter JobManager container
docker exec -it flink-jobmanager bash
```

### Submitting Flink Jobs

```bash
# Submit Python job
flink run -py /scripts/flink_example.py

# Submit with parallelism
flink run -py /scripts/flink_example.py -p 2

# List running jobs
flink list

# Cancel job
flink cancel <job-id>
```

### PyFlink Examples

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# DataStream API
env = StreamExecutionEnvironment.get_execution_environment()
ds = env.from_collection([1, 2, 3, 4, 5])
ds.print()
env.execute()

# Table API
table_env = StreamTableEnvironment.create(env)
table = table_env.from_elements([(1, 'Alice'), (2, 'Bob')])
table.select('*').execute().print()
```

### Windowing Operations

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common import Time

env = StreamExecutionEnvironment.get_execution_environment()

# Create windowed stream
ds = env.from_collection([...])
ds.key_by(lambda x: x[0]) \
  .window(TumblingEventTimeWindows.of(Time.seconds(5))) \
  .reduce(lambda a, b: (a[0], a[1] + b[1]))
```

## Web UI

- Flink Dashboard: http://localhost:8082

The dashboard shows:
- Running and completed jobs
- Task metrics and statistics
- Job execution plans
- Checkpoint and savepoint information

## Key Concepts

### State Management
- Keyed state for stateful operations
- Checkpointing for fault tolerance
- Savepoints for version management

### Time Semantics
- Event time: When events actually occurred
- Processing time: When events are processed
- Ingestion time: When events enter Flink

### Windows
- Tumbling windows: Fixed-size, non-overlapping
- Sliding windows: Fixed-size, overlapping
- Session windows: Variable-size based on inactivity

## Use Cases

- Real-time analytics
- Event-driven applications
- Data pipeline and ETL
- Continuous queries on streams
- Complex event processing (CEP)

## Learning Resources

- Build stateful stream processing applications
- Implement windowing and time-based operations
- Use watermarks for handling late data
- Integrate with Kafka for end-to-end pipelines
