## Overview
- Streams traffic sensor data to Kafka and computes real-time metrics with Quix Streams.
- Persists metrics to QuestDB and exposes dedicated Kafka topics for dashboards.

## Dependencies
- Python packages: see `requirements.txt`

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick Start
```bash
make download-dataset   # fetch JSONL via Socrata
make up                 # start Kafka, QuestDB, Grafana
```

## Components
- `scripts/sensor-data-producer.py`: publishes `traffic.jsonl` records to Kafka.
- `scripts/sensor-data-consumer.py`: computes hourly averages and availability, writes to QuestDB.
- `scripts/hourly-total-consumer.py`: aggregates hourly totals and writes daily maxima to QuestDB.