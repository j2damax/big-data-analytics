FROM openjdk:11-jdk-slim

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Download and install Kafka
ENV KAFKA_VERSION=3.6.1
ENV SCALA_VERSION=2.13
ENV KAFKA_HOME=/opt/kafka
ENV PATH=$PATH:$KAFKA_HOME/bin

RUN curl -L https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME}

# Install Python Kafka client
RUN pip3 install --no-cache-dir kafka-python

# Copy configuration
COPY config/server.properties $KAFKA_HOME/config/server.properties

WORKDIR $KAFKA_HOME

# Expose Kafka port
EXPOSE 9092

CMD ["bin/kafka-server-start.sh", "config/server.properties"]
