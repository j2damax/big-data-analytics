#!/usr/bin/env bash
set -euo pipefail

echo "Submitting Twitter hashtag Java job to Flink cluster..."

JAR_PATH="/jars/task4-flink-jobs-1.0.0.jar"
MAIN_CLASS="org.example.task4.TwitterHashtagJob"

flink run -c "$MAIN_CLASS" "$JAR_PATH"

echo "Java job submitted successfully!"
