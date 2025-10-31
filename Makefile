# Makefile for Big Data Analytics Project

.PHONY: help build up down restart logs clean ps test

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

build: ## Build all Docker images
	docker-compose build

up: ## Start all services
	docker-compose up -d
	@echo "Waiting for services to start..."
	@sleep 10
	@echo "Services started! Access web UIs:"
	@echo "  - Hadoop NameNode: http://localhost:9870"
	@echo "  - Spark Master: http://localhost:8080"
	@echo "  - Flink Dashboard: http://localhost:8082"

down: ## Stop all services
	docker-compose down

restart: ## Restart all services
	docker-compose restart

logs: ## View logs from all services
	docker-compose logs -f

ps: ## Show status of all containers
	docker-compose ps

clean: ## Stop and remove all containers, volumes, and images
	docker-compose down -v --rmi all

# Service-specific targets
hadoop: ## Start only Hadoop
	docker-compose up -d hadoop

spark: ## Start only Spark
	docker-compose up -d spark-master spark-worker

kafka: ## Start only Kafka (with Zookeeper)
	docker-compose up -d zookeeper kafka

flink: ## Start only Flink
	docker-compose up -d flink-jobmanager flink-taskmanager

# Run examples
test-spark: ## Run Spark example
	docker exec -it spark-master python3 /scripts/spark_example.py

test-kafka: ## Run Kafka example
	docker exec -it kafka python3 /scripts/kafka_example.py

test-flink: ## Run Flink example
	docker exec -it flink-jobmanager python3 /scripts/flink_example.py

test-all: ## Run all examples
	@echo "Running Spark example..."
	-docker exec spark-master python3 /scripts/spark_example.py
	@echo ""
	@echo "Running Kafka example..."
	-docker exec kafka python3 /scripts/kafka_example.py
	@echo ""
	@echo "Running Flink example..."
	-docker exec flink-jobmanager python3 /scripts/flink_example.py

# Shell access
shell-hadoop: ## Open shell in Hadoop container
	docker exec -it hadoop bash

shell-spark: ## Open shell in Spark Master container
	docker exec -it spark-master bash

shell-kafka: ## Open shell in Kafka container
	docker exec -it kafka bash

shell-flink: ## Open shell in Flink JobManager container
	docker exec -it flink-jobmanager bash

# Development
rebuild: ## Rebuild and restart all services
	docker-compose down
	docker-compose build --no-cache
	docker-compose up -d

# Data pipeline targets
data-download: ## Download SNAP datasets
	cd scripts/data_pipeline && python3 download_datasets.py

data-ingest: ## Ingest and validate datasets
	cd scripts/data_pipeline && python3 ingest_datasets.py

data-load: ## Load datasets to HDFS (requires Hadoop running)
	docker exec hadoop python3 /scripts/data_pipeline/load_to_hdfs.py

data-pipeline: ## Run complete data pipeline
	@echo "Starting complete data pipeline..."
	cd scripts/data_pipeline && python3 download_datasets.py
	cd scripts/data_pipeline && python3 ingest_datasets.py
	@echo "Waiting for Hadoop to be ready..."
	@sleep 5
	docker exec hadoop python3 /scripts/data_pipeline/load_to_hdfs.py
	@echo "Data pipeline completed!"

data-status: ## Check status of datasets in HDFS
	docker exec hadoop hadoop fs -ls -R /user/root/snap_datasets

data-clean: ## Remove downloaded and processed datasets
	rm -rf data/raw/* data/processed/*
	@echo "Local datasets cleaned"

# In-degree distribution analysis targets
indegree-test: ## Test in-degree implementations locally
	cd scripts/indegree_analysis && python3 test_local.py

indegree-hadoop: ## Run Hadoop in-degree on email-EuAll (test dataset)
	docker exec hadoop python3 /scripts/indegree_analysis/hadoop_indegree.py \
		-r hadoop \
		--hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
		/user/root/snap_datasets/email-EuAll/email-EuAll.txt \
		--output-dir /user/root/output/hadoop_email_indegree

indegree-spark: ## Run Spark in-degree on email-EuAll (test dataset)
	docker exec spark-master spark-submit \
		--master local[*] \
		/scripts/indegree_analysis/spark_indegree.py \
		/user/root/snap_datasets/email-EuAll/email-EuAll.txt

indegree-experiments: ## Run complete in-degree experiments on all datasets
	docker exec hadoop python3 /scripts/indegree_analysis/run_experiments.py \
		--datasets all \
		--output-dir /scripts/indegree_analysis/results

indegree-visualize: ## Generate visualizations and analysis report
	cd scripts/indegree_analysis && python3 visualize_results.py \
		--results results/experiment_results.json \
		--output-dir plots
