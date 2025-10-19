# Hadoop Setup

This directory contains the Dockerfile and configuration files for Apache Hadoop.

## Components

- **HDFS (Hadoop Distributed File System)**: Distributed storage system
- **YARN (Yet Another Resource Negotiator)**: Resource management layer
- **MapReduce**: Distributed processing framework

## Configuration Files

- `config/core-site.xml`: Core Hadoop configuration
- `config/hdfs-site.xml`: HDFS-specific configuration
- `config/mapred-site.xml`: MapReduce configuration
- `config/yarn-site.xml`: YARN configuration

## Usage

### Starting Hadoop

```bash
# Enter container
docker exec -it hadoop bash

# Format NameNode (first time only)
hdfs namenode -format

# Start HDFS
start-dfs.sh

# Start YARN
start-yarn.sh
```

### Basic HDFS Commands

```bash
# Create directory
hdfs dfs -mkdir -p /user/hadoop

# Upload file
hdfs dfs -put local_file.txt /user/hadoop/

# List files
hdfs dfs -ls /user/hadoop

# Read file
hdfs dfs -cat /user/hadoop/local_file.txt

# Download file
hdfs dfs -get /user/hadoop/local_file.txt ./

# Delete file
hdfs dfs -rm /user/hadoop/local_file.txt
```

### Running MapReduce Jobs

```bash
# Run built-in example
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /input /output

# Run Python MapReduce with mrjob
python3 /scripts/hadoop_wordcount.py input.txt
```

## Web UI

- NameNode: http://localhost:9870
- ResourceManager: http://localhost:8088
