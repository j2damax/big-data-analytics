FROM openjdk:8-jdk

# Install required packages
RUN apt-get update && apt-get install -y \
    ssh \
    rsync \
    curl \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Download and install Hadoop
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

RUN curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar -xz -C /opt/ && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

# Configure SSH for passwordless login
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# Copy Hadoop configuration files
COPY config/* $HADOOP_CONF_DIR/

# Format namenode
RUN mkdir -p /opt/hadoop/data/namenode /opt/hadoop/data/datanode

WORKDIR $HADOOP_HOME

# Expose ports
EXPOSE 9870 8088 9000 8020

CMD ["bash"]
