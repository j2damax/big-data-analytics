FROM openjdk:11-jdk-slim

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Download and install Flink
ENV FLINK_VERSION=1.18.0
ENV FLINK_HOME=/opt/flink
ENV PATH=$PATH:$FLINK_HOME/bin

RUN curl -L https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/flink-${FLINK_VERSION} ${FLINK_HOME}

# Copy Flink configuration
COPY config/* $FLINK_HOME/conf/

# Install PyFlink
RUN pip3 install --no-cache-dir apache-flink==${FLINK_VERSION}

WORKDIR $FLINK_HOME

# Expose Flink ports (JobManager, TaskManager, Web UI)
EXPOSE 8081 6123 6124

CMD ["/bin/bash"]
