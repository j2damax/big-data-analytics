# 🐘 Hadoop Ecosystem

**Apache Hadoop 3.3.6** - The foundation of big data processing with distributed storage (HDFS), resource management (YARN), and MapReduce batch processing.

## 🏗️ **Architecture Overview**

| Component | Purpose | Container Role |
|-----------|---------|---------------|
| **HDFS NameNode** | Metadata management for distributed files | Primary storage coordinator |
| **HDFS DataNode** | Actual file storage across cluster | Distributed storage nodes |
| **YARN ResourceManager** | Cluster resource allocation | Job scheduling and resource management |
| **YARN NodeManager** | Per-node resource management | Task execution on individual nodes |
| **MapReduce** | Distributed batch processing framework | Large-scale data processing jobs |

## ⚙️ **Production Configuration**

### **Core Settings** (`config/core-site.xml`)
```xml
<!-- File system URI pointing to HDFS -->
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://hadoop:9000</value>
</property>
```

### **HDFS Settings** (`config/hdfs-site.xml`)  
```xml
<!-- Replication factor for fault tolerance -->
<property>
    <name>dfs.replication</name>
    <value>1</value>  <!-- Single-node setup -->
</property>
```

### **Environment Setup** (`config/hadoop-env.sh`)
```bash
export JAVA_HOME=/usr/local/openjdk-8
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export HADOOP_LOG_DIR=/opt/hadoop/logs
```

## 🚀 **Quick Start**

```bash
# Access Hadoop container
make shell-hadoop

# Hadoop starts automatically - verify with:
jps  # Should show NameNode, DataNode, ResourceManager, NodeManager

# Web interfaces available immediately:
# 🌐 NameNode UI: http://localhost:9870  
# 🌐 ResourceManager UI: http://localhost:8088
```

## 💾 **HDFS Operations**

### **Essential HDFS Commands**
```bash
# Directory management
hdfs dfs -mkdir -p /user/data/input
hdfs dfs -ls /                        # List root directory
hdfs dfs -ls -h /user/data            # Human-readable sizes

# File operations  
hdfs dfs -put sample_data.txt /user/data/input/
hdfs dfs -get /user/data/output/part-00000 ./result.txt
hdfs dfs -cat /user/data/input/sample_data.txt
hdfs dfs -tail /user/data/logs/application.log

# Advanced operations
hdfs dfs -cp /user/data/input/* /user/backup/
hdfs dfs -rm -r /user/data/temp        # Recursive delete
hdfs dfsadmin -report                  # Cluster health report
```

### **Storage Management**
```bash
# Check disk usage
hdfs dfs -du -h /user/data
hdfs dfsadmin -printTopology           # Cluster topology
hdfs fsck /user/data -files -blocks   # File system check

# Rebalance cluster (multi-node setups)
hdfs balancer -threshold 10
```

## ⚡ **MapReduce Processing**

### **Built-in Examples**
```bash
# Word count example
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
    wordcount /user/data/input /user/data/output

# Pi calculation (Monte Carlo method)
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
    pi 10 1000

# TeraSort benchmark
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
    teragen 1000000 /user/data/terasort-input
```

### **Python MapReduce (mrjob)**
```bash
# Run the example word count
cd /scripts
python3 hadoop_wordcount.py sample_data.txt

# Custom MapReduce job
python3 -m mrjob.cmd.run_job your_job.py input.txt
```

## 🌐 **Web Interface Deep Dive**

### **NameNode UI (port 9870)**
- **Overview**: Cluster summary, live/dead nodes, storage utilization
- **Datanodes**: Individual DataNode health and storage details  
- **Browse**: File system browser with directory navigation
- **Logs**: NameNode operation logs and error tracking

### **ResourceManager UI (port 8088)**
- **Applications**: Running and completed MapReduce jobs
- **Cluster**: Resource utilization, node status, queue information
- **Scheduler**: Job scheduling policies and queue management
- **Tools**: Application timeline, job history, metrics

## 🔧 **Configuration Tuning**

### **Performance Optimization**
```bash
# Increase Java heap for NameNode (production)
export HADOOP_NAMENODE_OPTS="-Xmx2g"

# Adjust block size for large files
hdfs dfs -Ddfs.block.size=268435456 -put large_file.txt /user/data/

# Configure replication based on cluster size
hdfs dfs -setrep 3 /user/data/critical_data/
```

### **Memory Management**
```bash
# Check Java processes and memory usage
jps -lvm
free -h

# Monitor YARN containers
yarn node -list -all
yarn application -list
```

## 🏭 **Production Patterns**

### **Data Lifecycle Management**
```bash
# Automated data retention (via cron)
hdfs dfs -find /user/logs -name "*.log" -mtime +30 -delete

# Compression for storage efficiency  
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
    wordcount -Dmapreduce.output.fileoutputformat.compress=true /input /output

# Archive old data
hadoop archive -archiveName historical.har -p /user/data/2023 /user/archives/
```

### **Integration with Other Technologies**
```bash
# Export to Spark (via HDFS shared storage)
hdfs dfs -put data.csv /shared/spark-input/

# Kafka log aggregation to HDFS
# Configure Kafka Connect HDFS sink connector

# Flink checkpointing to HDFS
# Set Flink state backend to HDFS in flink-conf.yaml
```

## 📊 **Monitoring & Debugging**
```bash
# Live monitoring
hdfs dfsadmin -report              # Storage and node status
yarn node -list                   # YARN cluster health  
mapred job -list                  # Active MapReduce jobs

# Performance analysis
yarn logs -applicationId application_xxx   # Application logs
hdfs fsck /user/data -files -blocks -locations  # Block placement

# Resource utilization
yarn top                          # Live resource usage
jstack <namenode-pid>             # Thread analysis for debugging
```
