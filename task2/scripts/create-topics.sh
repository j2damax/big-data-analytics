#!/bin/bash
# Create topics
echo "-- Creating Topics --"
/opt/kafka/bin/kafka-topics.sh --create --topic traffic_raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --create --topic traffic_metrics --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Verify (optional)
echo "-- Listing Topics --"
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092