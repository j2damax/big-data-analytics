#!/usr/bin/env bash
set -euo pipefail

# Create twitter_posts topic (ignore if already exists)
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-broker:9092 --create --topic twitter_posts --partitions 1 --replication-factor 1 --if-not-exists

# Create tiktok_posts topic (ignore if already exists)
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-broker:9092 --create --topic tiktok_posts --partitions 1 --replication-factor 1 --if-not-exists

# List all topics
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-broker:9092 --list