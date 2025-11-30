#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set Hadoop-specific environment variables here.

# Java home
export JAVA_HOME=/opt/java/openjdk

# Hadoop Configuration Directory
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/opt/hadoop/etc/hadoop}

# Hadoop Log Directory
export HADOOP_LOG_DIR=${HADOOP_LOG_DIR:-/opt/hadoop/logs}

# Hadoop PID Directory
export HADOOP_PID_DIR=${HADOOP_PID_DIR:-/opt/hadoop/pids}

# Extra Java runtime options for all Hadoop commands
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"

# Extra Java runtime options for Hadoop NameNode
export HDFS_NAMENODE_OPTS="-Dhadoop.security.logger=INFO,RFAS -Dhdfs.audit.logger=INFO,NullAppender $HDFS_NAMENODE_OPTS"

# Extra Java runtime options for Hadoop DataNode  
export HDFS_DATANODE_OPTS="-Dhadoop.security.logger=ERROR,RFAS $HDFS_DATANODE_OPTS"

# Extra Java runtime options for Hadoop SecondaryNameNode
export HDFS_SECONDARYNAMENODE_OPTS="-Dhadoop.security.logger=INFO,RFAS -Dhdfs.audit.logger=INFO,NullAppender $HDFS_SECONDARYNAMENODE_OPTS"

# Extra Java runtime options for YARN ResourceManager
export YARN_RESOURCEMANAGER_OPTS="$YARN_RESOURCEMANAGER_OPTS"

# Extra Java runtime options for YARN NodeManager
export YARN_NODEMANAGER_OPTS="$YARN_NODEMANAGER_OPTS"

# Prevent "WARN util.NativeCodeLoader: Unable to load native-hadoop library"
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"