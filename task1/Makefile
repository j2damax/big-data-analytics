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
	docker-compose up --build -d
	@echo "Waiting for services to start..."
	@sleep 10
	@echo "Services started! Access web UIs:"
	@echo "  - Hadoop NameNode: http://localhost:9870"
	@echo "  - Spark Master: http://localhost:8080"

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

# Shell access
shell-hadoop: ## Open shell in Hadoop container
	docker exec -it hadoop bash

shell-spark: ## Open shell in Spark Master container
	docker exec -it spark-master bash

# Development
rebuild: ## Rebuild and restart all services
	docker-compose down
	docker-compose build --no-cache
	docker-compose up -d

# Data pipeline targets
data-download: ## Download SNAP datasets
	bash scripts/data_pipeline/download-datasets.sh

data-prepare: ## Ingest and validate datasets
	bash scripts/data_pipeline/extract_datasets.sh

data-load: ## Load datasets to HDFS (requires Hadoop running)
	docker exec hadoop bash /scripts/data_pipeline/load_to_hdfs.sh

data-status: ## Check status of datasets in HDFS
	docker exec hadoop hadoop fs -ls -R /user/root/snap_datasets

indegree-hadoop: ## Run Hadoop in-degree on email-EuAll (test dataset)
	docker exec hadoop python3 /scripts/indegree_analysis/hadoop_indegree.py \
		-r hadoop \
		--hadoop-streaming-jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
		hdfs://hadoop:9000/user/root/snap_datasets/email-euall/email-euall.txt \
		--output-dir hdfs://hadoop:9000/user/root/output/hadoop_email_indegree

indegree-spark: ## Run Spark in-degree on email-EuAll (test dataset)
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/scripts/indegree_analysis/spark_indegree.py \
		hdfs://hadoop:9000/user/root/snap_datasets/email-euall/email-euall.txt

indegree-experiments-small:
	@echo "Processing small datasets using Hadoop..."
	-docker exec hadoop python3 /scripts/indegree_analysis/run_experiments.py \
		--framework hadoop \
		--datasets email-EuAll cit-Patents soc-Pokec \
		--output-dir /scripts/indegree_analysis/results
	@echo "Processing small datasets using Spark..."
	-docker exec spark-master python3 /scripts/indegree_analysis/run_experiments.py \
		--framework spark \
		--datasets email-EuAll cit-Patents soc-Pokec \
		--output-dir /scripts/indegree_analysis/results

indegree-experiments-scaling:
	@echo "Processing small datasets using Hadoop..."
	-docker exec hadoop python3 /scripts/indegree_analysis/run_experiments.py \
		--framework hadoop \
		--datasets soc-LiveJournal1 \
		--output-dir /scripts/indegree_analysis/results
	@echo "Processing small datasets using Spark..."
	-docker exec spark-master python3 /scripts/indegree_analysis/run_experiments.py \
		--framework spark \
		--datasets soc-LiveJournal1 \
		--output-dir /scripts/indegree_analysis/results

indegree-experiments-hadoop:
	@echo "Processing All datasets using Hadoop..."
	-docker exec hadoop python3 /scripts/indegree_analysis/run_experiments.py \
		--framework hadoop \
		--datasets all \
		--output-dir /scripts/indegree_analysis/results

indegree-experiments-spark:
	@echo "Processing All datasets using Spark..."
	-docker exec spark-master python3 /scripts/indegree_analysis/run_experiments.py \
		--framework spark \
		--datasets all \
		--output-dir /scripts/indegree_analysis/results
		
indegree-visualize: ## Generate visualizations and analysis report
	cd scripts/indegree_analysis && python3 visualize_results.py \
		--results results/experiment_results.json \
		--output-dir plots
