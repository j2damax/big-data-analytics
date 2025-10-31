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

test-hadoop: ## Run Hadoop MapReduce example
	docker exec -it hadoop python3 /scripts/hadoop_wordcount.py /scripts/sample_data.txt

indegree-email: ## Run in-degree distribution on email-EuAll dataset (365K edges)
	@echo "Running in-degree distribution on email-EuAll dataset..."
	docker exec hadoop python3 /scripts/indegree/indegree_analysis.py /data/processed/email-EuAll.txt --method python

indegree-patents: ## Run in-degree distribution on cit-Patents dataset (16M+ edges)
	@echo "Running in-degree distribution on cit-Patents dataset..."
	docker exec hadoop python3 /scripts/indegree/indegree_analysis.py /data/processed/cit-Patents.txt --method python

indegree-livejournal: ## Run in-degree distribution on soc-LiveJournal1 dataset (69M+ edges)
	@echo "Running in-degree distribution on soc-LiveJournal1 dataset..."
	docker exec hadoop python3 /scripts/indegree/indegree_analysis.py /data/processed/soc-LiveJournal1.txt --method python

indegree-pokec: ## Run in-degree distribution on soc-pokec-relationships dataset (22M+ edges)
	@echo "Running in-degree distribution on soc-pokec-relationships dataset..."
	docker exec hadoop python3 /scripts/indegree/indegree_analysis.py /data/processed/soc-pokec-relationships.txt --method python

indegree-all: ## Run in-degree distribution on all datasets
	@echo "Running in-degree distribution on all available datasets..."
	@echo ""
	@make indegree-email
	@echo ""
	@make indegree-patents
	@echo ""
	@make indegree-pokec
	@echo ""
	@make indegree-livejournal

# Academic Framework Implementation
python-indegree: ## Run Pure Python in-degree analysis (baseline)
	@echo "🎯 Running Pure Python In-Degree Analysis..."
	docker exec hadoop python3 /scripts/indegree/indegree_analysis.py /data/processed/email-EuAll.txt --method python

hadoop-indegree: ## Run Hadoop MapReduce in-degree analysis
	@echo "🎯 Running Hadoop MapReduce In-Degree Analysis..."
	docker exec hadoop python3 /scripts/indegree/indegree_analysis.py /data/processed/email-EuAll.txt --method hadoop

spark-rdd-indegree: ## Run Spark RDD in-degree analysis
	@echo "🎯 Running Apache Spark RDD In-Degree Analysis..."
	docker exec spark-master python3 /scripts/indegree/indegree_analysis.py /data/processed/email-EuAll.txt --method spark-rdd

spark-dataframe-indegree: ## Run Spark DataFrame in-degree analysis
	@echo "🎯 Running Apache Spark DataFrame In-Degree Analysis..."
	docker exec spark-master python3 /scripts/indegree/indegree_analysis.py /data/processed/email-EuAll.txt --method spark-dataframe

unified-comparison: ## Run all methods for performance comparison
	@echo "🎯 Running All Methods Performance Comparison..."
	docker exec hadoop python3 /scripts/indegree/indegree_analysis.py /data/processed/email-EuAll.txt --method all --save-results

comprehensive-comparison: ## Run comprehensive analysis on multiple datasets
	@echo "🎯 Running Comprehensive Performance Comparison..."
	docker exec hadoop python3 /scripts/indegree/comprehensive_comparison.py \
		/data/processed/email-EuAll.txt email-EuAll \
		/data/processed/cit-Patents.txt patents \
		/data/processed/soc-pokec-relationships.txt pokec

academic-analysis: ## Run complete academic analysis (unified + comprehensive)
	@echo "🎓 Starting Complete Academic Analysis..."
	@make unified-comparison
	@echo ""
	@make comprehensive-comparison

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

# Monitoring and Web Interface Targets
monitor-all: ## Open all monitoring web interfaces
	@echo "Opening all monitoring interfaces..."
	@echo "  - Hadoop NameNode: http://localhost:9870"
	@echo "  - YARN ResourceManager: http://localhost:8088"
	@echo "  - Spark Master: http://localhost:8080"
	@echo "  - Spark Worker: http://localhost:8081"
	@echo "  - Flink Dashboard: http://localhost:8082"
	@if command -v open >/dev/null 2>&1; then \
		open "http://localhost:9870" && \
		open "http://localhost:8088" && \
		open "http://localhost:8080" && \
		open "http://localhost:8081" && \
		open "http://localhost:8082"; \
	else \
		echo "Please open these URLs manually in your browser"; \
	fi

monitor-hadoop: ## Open Hadoop monitoring interfaces
	@echo "Opening Hadoop monitoring interfaces..."
	@echo "  - HDFS NameNode: http://localhost:9870"
	@echo "  - YARN ResourceManager: http://localhost:8088"
	@echo "  - NodeManager: http://localhost:8042"
	@if command -v open >/dev/null 2>&1; then \
		open "http://localhost:9870" && \
		open "http://localhost:8088" && \
		open "http://localhost:8042"; \
	else \
		echo "Please open these URLs manually in your browser"; \
	fi

monitor-spark: ## Open Spark monitoring interfaces
	@echo "Opening Spark monitoring interfaces..."
	@echo "  - Spark Master: http://localhost:8080"
	@echo "  - Spark Worker: http://localhost:8081"
	@echo "  - Spark Application UI: http://localhost:4040 (when jobs running)"
	@if command -v open >/dev/null 2>&1; then \
		open "http://localhost:8080" && \
		open "http://localhost:8081"; \
	else \
		echo "Please open these URLs manually in your browser"; \
	fi

monitor-flink: ## Open Flink monitoring interface
	@echo "Opening Flink monitoring interface..."
	@echo "  - Flink Dashboard: http://localhost:8082"
	@if command -v open >/dev/null 2>&1; then \
		open "http://localhost:8082"; \
	else \
		echo "Please open http://localhost:8082 manually in your browser"; \
	fi

metrics-yarn: ## Get YARN cluster metrics via API
	@echo "🔍 YARN Cluster Metrics:"
	@curl -s http://localhost:8088/ws/v1/cluster/metrics | python3 -m json.tool 2>/dev/null || echo "YARN API not available"

metrics-hdfs: ## Get HDFS metrics via API
	@echo "🔍 HDFS Metrics:"
	@curl -s "http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState" | python3 -c "import sys,json; data=json.load(sys.stdin); print(f\"Capacity Used: {data['beans'][0]['CapacityUsed']:,} bytes\"); print(f\"Files: {data['beans'][0]['FilesTotal']:,}\"); print(f\"Blocks: {data['beans'][0]['BlocksTotal']:,}\")" 2>/dev/null || echo "HDFS API not available"

metrics-summary: ## Get comprehensive system metrics
	@echo "📊 System Metrics Summary"
	@echo "========================"
	@make metrics-yarn
	@echo ""
	@make metrics-hdfs
	@echo ""
	@echo "🔍 Container Status:"
	@docker-compose ps --format "table {{.Name}}\t{{.State}}\t{{.Ports}}"

monitor-indegree: ## Run in-degree analysis with monitoring setup
	@echo "🚀 Starting monitored in-degree analysis..."
	@echo "Opening monitoring interfaces..."
	@make monitor-hadoop
	@echo ""
	@echo "📈 Getting baseline metrics..."
	@make metrics-summary
	@echo ""
	@echo "⚡ Running in-degree analysis on email dataset..."
	@make indegree-email
	@echo ""
	@echo "📊 Final metrics:"
	@make metrics-summary
