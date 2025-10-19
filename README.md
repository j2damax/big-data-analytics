# Big Data Analytics

This repository demonstrates big data analytics technologies with Docker-based project setup.

## Module Learning Outcomes

1. **Knowledge of big data technologies and principles**
   - Understanding distributed computing concepts
   - Data processing paradigms (batch vs streaming)
   - Scalability and fault tolerance principles

2. **Apply knowledge on Hadoop, Spark, Kafka, Flink**
   - Hadoop for distributed storage (HDFS) and MapReduce
   - Spark for fast in-memory processing
   - Kafka for real-time data streaming
   - Flink for stateful stream processing

3. **Knowledge on streaming data processing applications**
   - Real-time data ingestion with Kafka
   - Stream processing with Flink
   - Micro-batch processing with Spark Streaming

4. **Build up skills on learning new technologies**
   - Containerized development environments
   - Modern DevOps practices with Docker
   - Integration of multiple big data technologies

## Project Structure

```
big-data-analytics/
├── hadoop/                 # Hadoop configuration and Dockerfile
│   ├── Dockerfile
│   └── config/
│       ├── core-site.xml
│       ├── hdfs-site.xml
│       ├── mapred-site.xml
│       └── yarn-site.xml
├── spark/                  # Spark configuration and Dockerfile
│   └── Dockerfile
├── kafka/                  # Kafka configuration and Dockerfile
│   ├── Dockerfile
│   └── config/
│       └── server.properties
├── flink/                  # Flink configuration and Dockerfile
│   └── Dockerfile
├── scripts/                # Python example scripts
│   ├── hadoop_wordcount.py
│   ├── spark_example.py
│   ├── kafka_example.py
│   └── flink_example.py
├── docker-compose.yml      # Orchestration of all containers
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## Technologies Included

### 1. Apache Hadoop (v3.3.6)
- **Purpose**: Distributed storage and processing
- **Components**: HDFS, YARN, MapReduce
- **Ports**: 
  - 9870 (NameNode Web UI)
  - 8088 (ResourceManager Web UI)
  - 9000 (HDFS)

### 2. Apache Spark (v3.5.0)
- **Purpose**: Fast, in-memory data processing
- **Components**: Spark Master, Spark Worker
- **Ports**:
  - 8080 (Spark Master Web UI)
  - 7077 (Spark Master)
  - 4040 (Spark Application UI)
  - 8081 (Spark Worker Web UI)

### 3. Apache Kafka (v3.6.1)
- **Purpose**: Distributed streaming platform
- **Components**: Kafka Broker, Zookeeper
- **Ports**:
  - 9092 (Kafka Broker)
  - 2181 (Zookeeper)

### 4. Apache Flink (v1.18.0)
- **Purpose**: Stateful stream processing
- **Components**: JobManager, TaskManager
- **Ports**:
  - 8082 (Flink Web UI)

## Prerequisites

- Docker (version 20.10 or higher)
- Docker Compose (version 2.0 or higher)
- At least 8GB of RAM available for Docker
- At least 20GB of free disk space

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/j2damax/big-data-analytics.git
cd big-data-analytics
```

### 2. Build and Start All Containers

```bash
# Build all images
docker-compose build

# Start all containers in detached mode
docker-compose up -d

# Check container status
docker-compose ps
```

### 3. Access Web UIs

Once all containers are running, you can access the web interfaces:

- **Hadoop NameNode**: http://localhost:9870
- **Hadoop ResourceManager**: http://localhost:8088
- **Spark Master**: http://localhost:8080
- **Spark Worker**: http://localhost:8081
- **Flink Dashboard**: http://localhost:8082

## Running Examples

### Hadoop MapReduce Example

```bash
# Run the WordCount example
docker exec -it hadoop bash
cd /scripts
python3 hadoop_wordcount.py input.txt
```

### Spark Example

```bash
# Run Spark examples
docker exec -it spark-master bash
cd /scripts
python3 spark_example.py

# Or submit to Spark cluster
spark-submit --master spark://spark-master:7077 /scripts/spark_example.py
```

### Kafka Example

```bash
# Run Kafka producer and consumer example
docker exec -it kafka bash
cd /scripts
python3 kafka_example.py
```

### Flink Example

```bash
# Run Flink streaming example
docker exec -it flink-jobmanager bash
cd /scripts
python3 flink_example.py
```

## Python Development

### Install Dependencies Locally

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running Scripts Locally

The Python scripts in the `scripts/` directory can be modified and tested locally before running in containers.

## Managing the Environment

### Start specific services

```bash
# Start only Kafka and Zookeeper
docker-compose up -d zookeeper kafka

# Start only Spark
docker-compose up -d spark-master spark-worker
```

### View logs

```bash
# View logs for all services
docker-compose logs -f

# View logs for specific service
docker-compose logs -f spark-master
```

### Stop and remove containers

```bash
# Stop all containers
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

### Restart services

```bash
# Restart specific service
docker-compose restart kafka

# Restart all services
docker-compose restart
```

## Troubleshooting

### Container fails to start

```bash
# Check container logs
docker-compose logs <service-name>

# Rebuild specific container
docker-compose build --no-cache <service-name>
```

### Port conflicts

If you have port conflicts, modify the port mappings in `docker-compose.yml`:

```yaml
ports:
  - "YOUR_PORT:CONTAINER_PORT"
```

### Memory issues

Increase Docker memory allocation in Docker Desktop settings or modify container memory limits in `docker-compose.yml`.

## Learning Resources

### Hadoop
- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/)
- [Hadoop MapReduce Tutorial](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)

### Spark
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark Tutorial](https://spark.apache.org/docs/latest/api/python/)

### Kafka
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka Python Client](https://kafka-python.readthedocs.io/)

### Flink
- [Apache Flink Documentation](https://flink.apache.org/docs/stable/)
- [PyFlink Tutorial](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/overview/)

## Project Exercises

### Exercise 1: Word Count Pipeline
1. Create a text file with sample data
2. Store it in HDFS
3. Process with Hadoop MapReduce
4. Compare performance with Spark

### Exercise 2: Real-time Data Pipeline
1. Set up Kafka producer to generate streaming data
2. Create Kafka consumer to read data
3. Process stream with Flink
4. Visualize results

### Exercise 3: Batch vs Stream Processing
1. Process the same dataset with Spark (batch)
2. Process with Flink (streaming)
3. Compare results and performance

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open source and available under the MIT License.

## Contact

For questions or feedback, please open an issue in the repository.
