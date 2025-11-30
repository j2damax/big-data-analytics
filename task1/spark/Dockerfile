FROM eclipse-temurin:11-jdk

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    procps \
    ssh \
    rsync \
    && rm -rf /var/lib/apt/lists/*

# Download and install Hadoop (for HDFS support)
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

RUN curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar -xz -C /opt/ && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

# Download and install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION_FOR_SPARK=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_FOR_SPARK}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_FOR_SPARK} ${SPARK_HOME}

# Install PySpark
RUN pip3 install --no-cache-dir --break-system-packages pyspark==${SPARK_VERSION}

WORKDIR $SPARK_HOME

# Expose ports for Spark UI, Master, and Worker
EXPOSE 8080 7077 4040 18080

CMD ["/bin/bash"]
