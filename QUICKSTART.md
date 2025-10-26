# 🚀 Big Data Analytics - Quick Start

Get a complete big data environment running in **5 minutes** with Hadoop, Spark, Kafka, and Flink!

## ✅ **Prerequisites Check**

Ensure you have the following before starting:

| Requirement | Minimum | Check Command | Status |
|-------------|---------|---------------|--------|
| **Docker Desktop** | 20.10+ | `docker --version` | ☐ |
| **Docker Compose** | 2.0+ | `docker-compose --version` | ☐ |
| **Available RAM** | 8GB+ | Docker Desktop → Settings → Resources | ☐ |
| **Free Disk Space** | 20GB+ | `df -h` (Unix) or `dir` (Windows) | ☐ |

```bash
# Quick prerequisite check
docker --version && docker-compose --version
docker system info | grep "Total Memory"
```

## ⚡ **One-Command Setup**

```bash
# Clone the repository
git clone https://github.com/j2damax/big-data-analytics.git
cd big-data-analytics

# 🚀 Magic command - builds and starts everything!
make up
```

**What this does:**
1. 🏗️ Builds 6 Docker images (Hadoop, Spark Master/Worker, Kafka, Zookeeper, Flink JobManager/TaskManager)
2. 🌐 Creates `bigdata-network` with DNS resolution
3. 💾 Sets up persistent volumes and script mounting
4. 🔧 Configures all services with production-ready settings
5. ⏱️ Waits for health checks and displays access URLs

## ✅ **Verify Everything Works**

```bash
# Check all containers are running
make ps

# Expected output: 6 containers with "Up" status
# ✅ hadoop            Up      9000/tcp, 9870/tcp, 8088/tcp  
# ✅ spark-master      Up      7077/tcp, 8080/tcp
# ✅ spark-worker      Up      8081/tcp
# ✅ kafka             Up      9092/tcp
# ✅ zookeeper         Up      2181/tcp, 2888/tcp, 3888/tcp
# ✅ flink-jobmanager  Up      6123/tcp, 8082/tcp
# ✅ flink-taskmanager Up      6121-6125/tcp

# Test all examples work  
make test-all
# ✅ Spark: WordCount + DataFrame operations
# ✅ Kafka: Producer/consumer messaging  
# ✅ Flink: Stream processing + Table API
```

## 🎯 **Access Everything Instantly**

### **Web Interfaces (Click to Open)**
| Service | URL | What You'll See |
|---------|-----|-----------------|
| **🐘 Hadoop NameNode** | [localhost:9870](http://localhost:9870) | HDFS cluster overview, data nodes, file browser |
| **🐘 Hadoop ResourceManager** | [localhost:8088](http://localhost:8088) | YARN applications, cluster resources, job history |
| **⚡ Spark Master** | [localhost:8080](http://localhost:8080) | Cluster status, worker nodes, submitted applications |
| **⚡ Spark Worker** | [localhost:8081](http://localhost:8081) | Individual worker metrics, executor details |
| **🌊 Flink Dashboard** | [localhost:8082](http://localhost:8082) | Job manager, running jobs, task metrics, checkpoints |

### **Try Examples (30 seconds each)**
```bash
# 🎯 Test everything with one command
make test-all

# Or run individual examples
make test-spark    # ⚡ DataFrame operations, SQL queries, RDD transformations
make test-kafka    # 🔄 Producer/consumer with JSON messages  
make test-flink    # 🌊 Real-time stream processing, windowing operations
```

## 🛠️ **Essential Commands**

### **Service Management**
```bash
make up            # 🚀 Start all services
make down          # 🛑 Stop all services  
make restart       # 🔄 Restart everything
make ps            # 📊 Show container status
make logs          # 📝 View real-time logs
```

### **Interactive Development**
```bash
make shell-spark   # ⚡ Interactive PySpark shell
make shell-flink   # 🌊 PyFlink development environment  
make shell-kafka   # 🔄 Kafka topic management
make shell-hadoop  # 🐘 HDFS commands and MapReduce jobs
```

### **Development Workflow**  
```bash
# Edit Python scripts locally
code scripts/spark_example.py    # VS Code
vim scripts/flink_example.py     # Terminal editor

# Test immediately (no rebuild needed!)
make test-spark

# Scripts are volume-mounted - changes appear instantly in containers! 🎉
```

## 🚨 **Quick Troubleshooting**

### **Common Issues & 30-Second Fixes**

| Problem | Quick Check | Solution |
|---------|-------------|----------|
| **Container won't start** | `make logs` | `make rebuild` |
| **Port already in use** | `netstat -tulpn \| grep 8080` | Change ports in `docker-compose.yml` |
| **Out of memory** | `docker stats` | Docker Desktop → Settings → Resources → 8GB+ |
| **Web UI not loading** | `curl localhost:8080` | Check container is running: `make ps` |

### **Emergency Reset** 🔄
```bash
make clean    # Nuclear option: removes everything
make up      # Fresh start
```

### **Selective Debugging**
```bash
# Focus on problematic service
docker-compose logs -f spark-master
docker-compose restart flink-jobmanager
docker exec hadoop jps  # Check Java processes
```

## 🎓 **Next Steps (Choose Your Path)**

### **📚 For Learners**
1. ✅ **Understand the Architecture**: Read `README.md` sections on each technology
2. 🔍 **Explore Web UIs**: Click through Hadoop NameNode, Spark Master, Flink Dashboard  
3. 💻 **Modify Examples**: Edit `scripts/*.py` files and test immediately with `make test-all`
4. 🏗️ **Build Pipelines**: Combine Kafka → Flink → HDFS → Spark workflows

### **🚀 For Developers**
1. 📂 **Check Technology READMEs**: `hadoop/README.md`, `spark/README.md`, etc.
2. ⚙️ **Customize Configuration**: Modify XML/YAML files in `*/config/` directories
3. 🐳 **Extend Containers**: Add dependencies by editing `*/Dockerfile` files
4. 📊 **Production Setup**: Scale services, add monitoring, configure persistence

### **🎯 Quick Wins (Try Right Now!)**
```bash
# Real-time data pipeline (2 minutes)
make shell-kafka   # Terminal 1: Create topic, start producer
make shell-flink   # Terminal 2: Stream processing consumer
make logs         # Terminal 3: Watch real-time processing

# Performance comparison (3 minutes)  
time make test-spark  # Measure Spark performance
# Edit scripts/spark_example.py (increase dataset size)
time make test-spark  # Compare performance impact
```

## 💡 **Pro Tips**
- **Volume Mounting**: Scripts in `./scripts/` are live-mounted - edit locally, test instantly!
- **Container DNS**: Services can reach each other by name (e.g., `kafka:9092`, `spark-master:7077`)
- **Resource Monitoring**: Use `docker stats` to watch CPU/memory usage in real-time
- **Log Analysis**: `make logs | grep ERROR` to quickly find issues

## 🆘 **Get Help**
- 📖 **Detailed Docs**: Main `README.md` has comprehensive guides  
- 🐛 **Debug Logs**: `make logs` or `docker-compose logs <service-name>`
- 🔧 **Configuration**: Check `docker-compose.yml` and `*/config/` files
- 💬 **Community**: Open GitHub issues for bugs or questions

## 🧹 **Clean Shutdown**
```bash
# When finished experimenting:
make down        # Stop containers (keeps data)
make down -v     # Stop and remove data volumes  
make clean       # Complete cleanup (frees disk space)
```

**🎉 Congratulations!** You now have a complete big data analytics environment running. Start exploring! 🚀
