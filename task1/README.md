# Task 1 — In-Degree Analysis (Email-EU dataset)

This folder contains the implementation and runner scripts to compute node in-degree distributions using Hadoop (MapReduce) and Spark.

## Folder Overview
- `Makefile`: Convenience targets to download, prepare, load, and run analyses.
- `scripts/indegree_analysis/`: Python implementations for Hadoop and Spark, plus experiment helpers.
- `hadoop/`, `spark/`: Docker images and configs for the cluster services.

## Prerequisites
- Docker and Docker Compose installed.
- Python 3.11 with virtualenv (optional for local helper scripts).

## Dependencies
- Python packages for scripts: see `requirements.txt`

### Install (optional, for local runs)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick Start
1. Start services:
   ```sh
   make up
   ```
2. Download + prepare dataset (Email-EU):
   ```sh
   make data-download
   make data-prepare
   ```
3. Load to HDFS:
   ```sh
   make data-load
   make data-status
   ```
4. Run in-degree analysis (Hadoop MapReduce):
   ```sh
   make indegree-hadoop
   ```
5. Run in-degree analysis (Spark):
   ```sh
   make indegree-spark
   ```
6. Visualize results (if generated):
   ```sh
   make indegree-visualize
   ```

- Web UIs (when `make up` is running):
  - Hadoop ResourceManager: `http://localhost:8088/`
  - HDFS NameNode: `http://localhost:9870/`
  - Spark Master: `http://localhost:8080/`
