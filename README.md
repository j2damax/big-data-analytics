# Big Data Analytics

A comprehensive, production-ready big data analytics environment demonstrating Hadoop, Spark, Kafka, and Flink technologies running in Docker containers. This project provides a complete ecosystem for learning and experimenting with distributed computing, stream processing, and big data analytics..

## 🚀 **Quick Start (5 Minutes)**

```bash
# Clone and navigate
git clone https://github.com/j2damax/big-data-analytics.git
cd big-data-analytics

# Build and start (one command!)
make up

# Test everything works
make test-all
```

Access web interfaces:
- **Hadoop**: http://localhost:9870 (NameNode) & http://localhost:8088 (ResourceManager)
- **Spark**: http://localhost:8080 (Master) & http://localhost:8081 (Worker)  
- **Flink**: http://localhost:8082 (Dashboard)

## 🆕 **New: In-Degree Distribution Analysis**

Compare Hadoop MapReduce vs Apache Spark for graph analytics on large-scale network datasets (116M+ edges):

```bash
# Quick test
make indegree-test

# Run experiments on all datasets (1-2 hours)
make indegree-experiments

# Generate visualizations and analysis
make indegree-visualize
```

**Features**: Automated experiments, performance metrics, visualizations, comprehensive analysis. See [INDEGREE_ANALYSIS.md](INDEGREE_ANALYSIS.md) for details.

## 📚 **Learning Outcomes**

### 1. **Distributed Computing Mastery**
   - Understand HDFS, YARN, and MapReduce architectures
   - Learn fault tolerance and data replication strategies
   - Practice with cluster resource management

### 2. **Multi-Engine Processing**
   - **Hadoop**: Batch processing with MapReduce and HDFS storage
   - **Spark**: In-memory analytics, SQL queries, and machine learning
   - **Kafka**: Real-time message streaming and event-driven architectures  
   - **Flink**: Stateful stream processing with exactly-once guarantees

### 3. **Real-World Data Pipelines**
   - Build end-to-end streaming pipelines (Kafka → Flink)
   - Implement batch processing workflows (HDFS → Spark)
   - Compare performance across different engines

### 4. **Modern DevOps Practices**
   - Container orchestration with Docker Compose
   - Service networking and inter-container communication
   - Automated testing and deployment workflows

## 🏗️ **Architecture Overview**

```
big-data-analytics/
├── 🐘 hadoop/              # Hadoop ecosystem (HDFS + YARN + MapReduce)
│   ├── Dockerfile          # Java 8, Hadoop 3.3.6 configuration
│   └── config/             # Production-ready XML configurations
│       ├── core-site.xml   # Core Hadoop settings
│       ├── hdfs-site.xml   # Distributed storage configuration  
│       ├── mapred-site.xml # MapReduce job settings
│       ├── yarn-site.xml   # Resource manager configuration
│       └── hadoop-env.sh   # Environment variables (JAVA_HOME)
├── ⚡ spark/               # Apache Spark cluster
│   ├── Dockerfile          # Java 11, Spark 3.5.0, PySpark
│   └── README.md           # Spark-specific documentation
├── 🔄 kafka/               # Message streaming platform  
│   ├── Dockerfile          # Kafka 3.6.1 with Python client
│   ├── config/
│   │   └── server.properties # Broker configuration
│   └── README.md           # Kafka usage guide
├── 🌊 flink/               # Stream processing engine
│   ├── Dockerfile          # Java 11, Flink 1.18.0, PyFlink
│   ├── config/
│   │   └── flink-conf.yaml # Network and cluster settings
│   └── README.md           # Flink development guide
├── 📝 scripts/             # Ready-to-run Python examples
│   ├── hadoop_wordcount.py # MapReduce word counting
│   ├── spark_example.py    # DataFrames, SQL, and RDDs
│   ├── kafka_example.py    # Producer/consumer messaging
│   ├── flink_example.py    # Stream processing and Table API
│   ├── indegree_analysis/  # 🆕 Graph analytics (Hadoop vs Spark comparison)
│   │   ├── hadoop_indegree.py    # MapReduce implementation
│   │   ├── spark_indegree.py     # Spark implementation
│   │   ├── run_experiments.py    # Automated benchmarking
│   │   ├── visualize_results.py  # Performance plots
│   │   └── README.md             # Complete documentation
│   ├── sample_data.txt     # Test dataset
│   └── README.md           # Example script documentation
├── 🐳 docker-compose.yml   # 6-service orchestration with networking
├── 📋 Makefile             # Development commands and shortcuts
├── 📦 requirements.txt     # Shared Python dependencies
├── 🚀 QUICKSTART.md        # 5-minute setup guide
└── 📖 README.md            # This comprehensive guide
```

**Container Network**: All services communicate via `bigdata-network` bridge with shared volume mounting (`./scripts:/scripts`) for seamless development.

## 🛠️ **Technology Stack**

| Technology | Version | Purpose | Container Ports | Web UI |
|------------|---------|---------|-----------------|---------|
| **🐘 Hadoop** | 3.3.6 | Distributed storage & batch processing | 9870, 8088, 9000 | [NameNode](http://localhost:9870) \| [ResourceManager](http://localhost:8088) |
| **⚡ Spark** | 3.5.0 | In-memory analytics & machine learning | 8080, 8081, 7077, 4040 | [Master](http://localhost:8080) \| [Worker](http://localhost:8081) |
| **🔄 Kafka** | 3.6.1 | Real-time message streaming | 9092, 2181 | CLI-based management |
| **🌊 Flink** | 1.18.0 | Stateful stream processing | 8082 | [Dashboard](http://localhost:8082) |
| **📦 Zookeeper** | 7.5.0 | Distributed coordination | 2181 | Kafka dependency |

### **Container Architecture**
- **Java Environments**: Hadoop (OpenJDK 8), Spark/Flink (OpenJDK 11)
- **Python Integration**: Python 3.9+ with technology-specific libraries
- **Networking**: Isolated `bigdata-network` with DNS resolution
- **Storage**: Persistent volumes for data and shared script mounting
- **Resource Allocation**: Optimized memory settings for development use

## ⚡ **Quick Setup**

### **Prerequisites** ✅
- Docker Desktop 20.10+ with 8GB+ RAM allocated
- Docker Compose 2.0+
- 20GB+ free disk space
- macOS/Linux/Windows with WSL2

### **One-Command Deployment** 🚀

```bash
# Complete setup in one command
make up
```

This single command will:
1. Build all 6 Docker images (5-10 minutes first time)
2. Start services with health checks
3. Configure networking and volumes
4. Display access URLs when ready

### **Verify Installation** ✅

```bash
# Check all containers are running
make ps

# Test all examples work
make test-all

# Access web interfaces
open http://localhost:9870  # Hadoop NameNode
open http://localhost:8080  # Spark Master  
open http://localhost:8082  # Flink Dashboard
```

### **Development Commands** 🛠️

```bash
make help          # Show all available commands
make logs          # View real-time logs from all services
make shell-spark   # Interactive shell in Spark container
make restart       # Restart all services
make clean         # Complete cleanup (removes everything)
```

## 🎯 **Ready-to-Run Examples**

All examples are production-ready and demonstrate real-world patterns:

### **Quick Test All Technologies** 🚀
```bash
make test-all    # Runs all examples in sequence with error handling
```

### **Individual Technology Examples**

| Command | Technology | What It Demonstrates |
|---------|------------|---------------------|
| `make test-spark` | **Spark** | WordCount with RDDs, DataFrame operations, SQL queries |
| `make test-kafka` | **Kafka** | Producer/consumer messaging with JSON serialization |
| `make test-flink` | **Flink** | Stream processing, windowing, and Table API operations |
| `make test-hadoop` | **Hadoop** | MapReduce job execution with HDFS storage |
| `make indegree-test` | 🆕 **Graph Analytics** | In-degree distribution: Hadoop vs Spark comparison |

### **Advanced Usage Patterns**

```bash
# Interactive development shells
make shell-spark    # PySpark shell, spark-submit, MLlib
make shell-flink    # PyFlink, job submission, checkpointing  
make shell-kafka    # Topic management, console tools
make shell-hadoop   # HDFS commands, MapReduce jobs

# Real-time monitoring
make logs           # Watch all container logs
docker-compose logs -f flink-jobmanager  # Focus on specific service

# Development workflow
# 1. Edit scripts locally in ./scripts/
# 2. Test immediately: make test-spark
# 3. Scripts are auto-mounted, no rebuild needed!
```

### **Example Output Highlights**
- **Spark**: 25-word vocabulary analysis with DataFrame transformations
- **Kafka**: JSON message streaming with timestamp correlation
- **Flink**: Real-time word counting with exactly-once processing
- **Hadoop**: Distributed MapReduce execution across YARN cluster

## 💻 **Development Workflow**

### **Local Python Development**
```bash
# Set up local environment (optional - containers include all dependencies)
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Edit scripts locally - changes appear immediately in containers!
# ./scripts/ directory is volume-mounted to /scripts in all containers
```

### **Service Management**
```bash
# Granular service control
make spark          # Start only Spark cluster  
make kafka          # Start only Kafka + Zookeeper
make flink          # Start only Flink cluster
make hadoop         # Start only Hadoop ecosystem

# Development operations
make restart        # Restart all services
make rebuild        # Clean rebuild (use when changing Dockerfiles)
make down          # Stop all services
make clean         # Nuclear option: remove everything
```

### **Monitoring & Debugging**
```bash
# Real-time log monitoring
make logs                           # All services
docker-compose logs -f spark-master # Specific service

# Container inspection
make ps                            # Service status overview
docker exec -it flink-jobmanager htop  # Resource usage
docker stats                       # Live container metrics
```

### **Production Deployment Considerations**
- **Scaling**: Modify `docker-compose.yml` replica counts
- **Persistence**: Configure external volumes for production data
- **Security**: Enable authentication and SSL/TLS
- **Monitoring**: Integrate with Prometheus/Grafana stack
- **Resource Limits**: Adjust container CPU/memory constraints

## 🔧 **Troubleshooting Guide**

### **Common Issues & Solutions**

| Issue | Diagnosis | Solution |
|-------|-----------|----------|
| Container won't start | `make logs` | Check Java configuration, port conflicts |
| Out of memory | `docker stats` | Increase Docker Desktop memory to 8GB+ |
| Port conflicts | `netstat -tulpn` | Modify ports in `docker-compose.yml` |
| Web UI not accessible | `curl localhost:8080` | Check container networking and firewall |

### **Service-Specific Debugging**

```bash
# Hadoop: JAVA_HOME issues
docker exec hadoop echo $JAVA_HOME
docker-compose logs hadoop

# Spark: Cluster connectivity
docker exec spark-master curl spark-master:7077
make shell-spark  # Test PySpark shell

# Kafka: Topic and broker issues  
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
make shell-kafka

# Flink: Job submission problems
docker exec flink-jobmanager flink list
curl http://localhost:8082/jobs
```

### **Reset & Recovery**
```bash
# Complete reset (when things go wrong)
make clean          # Remove everything
make up            # Fresh start

# Selective recovery  
docker-compose restart flink-jobmanager  # Restart specific service
make rebuild                             # Force rebuild all images
```

## 📚 **Learning Path & Exercises**

### **Progressive Skill Building**

#### **Level 1: Individual Technologies** 🎯
1. **Explore Web UIs**: Navigate Hadoop NameNode, Spark Master, Flink Dashboard
2. **Run Examples**: Execute `make test-all` and understand each output
3. **Interactive Shells**: Use `make shell-spark` to experiment with PySpark
4. **Configuration**: Examine XML/YAML files in each technology's config/ directory

#### **Level 2: Cross-Technology Integration** 🔄  
1. **Kafka → Flink Pipeline**: Stream data from Kafka into Flink processing
2. **HDFS → Spark Analysis**: Store large datasets in Hadoop, analyze with Spark
3. **Multi-Engine Comparison**: Same dataset through Hadoop MapReduce vs Spark
4. **Real-time Dashboard**: Combine Kafka streaming + Flink processing + visualization

#### **Level 3: Production Scenarios** 🏭
1. **Fault Tolerance Testing**: Kill containers, observe recovery mechanisms  
2. **Performance Tuning**: Adjust memory, parallelism, partition settings
3. **Data Pipeline Orchestration**: Chain multiple technologies in workflows
4. **Monitoring & Alerting**: Set up observability for the entire stack

### **Hands-On Projects**
```bash
# Project 1: Log Analysis Pipeline
# 1. Generate logs → Kafka → Flink (real-time alerts) → HDFS (storage)
# 2. Historical analysis with Spark SQL on stored data

# Project 2: E-commerce Analytics  
# 1. Transaction events via Kafka
# 2. Real-time fraud detection with Flink
# 3. Batch customer segmentation with Spark MLlib  
# 4. Data warehouse queries on Hadoop

# Project 3: IoT Sensor Processing
# 1. Sensor data ingestion through Kafka
# 2. Anomaly detection with Flink CEP (Complex Event Processing)
# 3. Machine learning model training with Spark
```

### **Essential Resources**
| Technology | Official Docs | Interactive Tutorials | Community |
|------------|---------------|----------------------|-----------|
| **Hadoop** | [Apache Docs](https://hadoop.apache.org/docs/) | [Cloudera Tutorial](https://www.cloudera.com/tutorials.html) | [Stack Overflow](https://stackoverflow.com/questions/tagged/hadoop) |
| **Spark** | [Spark Guide](https://spark.apache.org/docs/latest/) | [Databricks Learning](https://databricks.com/learn) | [Spark User List](https://spark.apache.org/community.html) |
| **Kafka** | [Kafka Docs](https://kafka.apache.org/documentation/) | [Confluent Tutorials](https://developer.confluent.io/) | [Kafka Users](https://kafka.apache.org/contact) |
| **Flink** | [Flink Docs](https://flink.apache.org/docs/stable/) | [Ververica Training](https://training.ververica.com/) | [Flink Community](https://flink.apache.org/community.html) |

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open source and available under the MIT License.

## Contact

For questions or feedback, please open an issue in the repository.
