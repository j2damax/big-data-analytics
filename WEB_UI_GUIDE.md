# Big Data Analytics - Web UI Access Guide

## 🌐 Available Web Interfaces

Your Docker-based big data stack provides several web UIs for monitoring and management. All services are accessible via `localhost` with the following ports:

### 1. **Hadoop HDFS NameNode UI** 
- **URL**: http://localhost:9870
- **Purpose**: HDFS file system browser and cluster health
- **Features**:
  - Browse HDFS directories and files
  - View dataset locations (e.g., `/user/root/snap_datasets/`)
  - Monitor storage usage and replication
  - Check DataNode health and block information
  - View cluster configuration

### 2. **Hadoop YARN ResourceManager UI**
- **URL**: http://localhost:8088
- **Purpose**: Cluster resource management and job monitoring
- **Features**:
  - View running and completed applications
  - Monitor CPU and memory allocation
  - Check job queues and scheduling
  - Access application logs and metrics
  - View NodeManager status

### 3. **Hadoop YARN NodeManager UI**
- **URL**: http://localhost:8042
- **Purpose**: Individual node resource monitoring and container management
- **Features**:
  - View node resource usage (CPU, memory, disk)
  - Monitor running containers and applications on this node
  - Check node health status and logs
  - Access container logs for debugging applications
  - View node-specific metrics and configuration

### 4. **Apache Spark Master UI**
- **URL**: http://localhost:8080
- **Purpose**: Spark cluster management and application tracking
- **Features**:
  - View Spark worker nodes and resources
  - Monitor active and completed Spark applications
  - Check application execution details and stages
  - View executor information and task distribution
  - Access Spark application history

### 5. **Apache Flink Dashboard**
- **URL**: http://localhost:8082
- **Purpose**: Stream processing job management
- **Features**:
  - Monitor running Flink jobs and task status
  - View job execution graphs and data flow
  - Check throughput and latency metrics
  - Access checkpointing and savepoint information
  - Monitor TaskManager resources

### 6. **Spark Application UI** (When applications are running)
- **URL**: http://localhost:4040
- **Purpose**: Detailed view of individual Spark applications
- **Features**:
  - SQL query execution plans
  - Job stages and task details
  - Storage and executor information
  - Environment and configuration settings
  - Streaming statistics (for streaming apps)

### 7. **Spark Worker UI**
- **URL**: http://localhost:8081
- **Purpose**: Individual Spark worker node details
- **Features**:
  - Worker resource usage
  - Running executors
  - Completed applications on this worker
  - Worker logs and environment

## 📊 Data Visualization Capabilities

### Current Dataset Status
With your processed SNAP datasets, you can:

1. **View Files in HDFS**: Use the HDFS UI (port 9870) to browse uploaded datasets
2. **Monitor Processing Jobs**: Use YARN UI (port 8088) to track MapReduce/Spark jobs
3. **Spark Analytics**: Use Spark Master UI (port 8080) to monitor graph analytics jobs
4. **Stream Processing**: Use Flink Dashboard (port 8082) for real-time network analysis

### Available Datasets for Analysis
- **email-EuAll.txt**: 420K edges (4.8 MB) - Communication network
- **cit-Patents.txt**: 16.5M edges (268 MB) - Citation network  
- **soc-pokec-relationships.txt**: 30.6M edges (404 MB) - Social network
- **soc-LiveJournal1.txt**: 69M edges (1030 MB) - Large social network

## 🚀 Getting Started

### 1. Upload Datasets to HDFS
```bash
# Upload a dataset to view in HDFS UI
docker exec -it hadoop python3 /scripts/load_to_hdfs.py --datasets email-EuAll
```

### 2. Run Analytics Jobs
```bash
# Run Spark analysis (monitor via port 8080)
docker exec -it spark-master python3 /scripts/spark_example.py

# Run Flink streaming (monitor via port 8082)  
docker exec -it flink-jobmanager python3 /scripts/flink_example.py
```

### 3. Access Web UIs
Open your browser to any of the URLs above to monitor:
- File storage and distribution (HDFS)
- Resource usage and job progress (YARN)
- Distributed processing status (Spark)
- Stream processing metrics (Flink)

## 🔧 Troubleshooting

### If YARN UI (port 8088) is not accessible:
```bash
# Restart YARN services
docker exec hadoop /opt/hadoop/sbin/start-yarn.sh

# Check if services are running
docker exec hadoop jps
```

### If containers are not responding:
```bash
# Restart all services
docker-compose restart

# Check container status
docker-compose ps
```

### For detailed logs:
```bash
# View container logs
docker logs hadoop
docker logs spark-master
docker logs flink-jobmanager

# Access container shell for debugging
docker exec -it hadoop bash
```

## 📈 Next Steps

1. **Upload your datasets**: Use `load_to_hdfs.py` to see them in the HDFS UI
2. **Run example analytics**: Execute provided scripts to see jobs in action
3. **Create custom analyses**: Develop your own Spark/Flink jobs for the 116M+ edges
4. **Monitor performance**: Use the web UIs to optimize resource allocation

---
*All web interfaces are accessible at localhost with the specified ports.*  
*Ensure Docker containers are running with `docker-compose ps` before accessing UIs.*