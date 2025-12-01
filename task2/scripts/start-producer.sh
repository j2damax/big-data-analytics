#!/bin/bash
set -e

echo "Installing required packages..."
pip install kafka-python

echo "Waiting for Kafka to be ready..."
sleep 30

echo "Starting sensor data producer..."
python3 sensor-data-producer.py