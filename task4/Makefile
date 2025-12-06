.PHONY: help build up down restart logs clean ps test flink-submit-twitter flink-submit-tiktok flink-jobs flink-cancel task4-java-build flink-submit-tiktok-java java-build download-raw prepare start-producer kafka-prepare download-dataset

up: ## Start all services
	docker-compose up --build -d
	@echo "Waiting for services to start..."
	@sleep 10
	@echo "Services started! Access web UIs:"
	@echo "  - Redpanda (Kafka UI): http://localhost:8083"
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

prepare: ## Prepare datasets: sort by date_created ascending into .csv (expects data/*.raw.csv to be present)
	python3 scripts/sort_csv_by_date.py --input data/raw/tiktok-dataset.csv --output data/processed/tiktok-dataset.csv --date-col date_created
	python3 scripts/sort_csv_by_date.py --input data/raw/twitter-dataset.csv --output data/processed/twitter-dataset.csv --date-col date_posted
	
java-build:
	# Build the JAR using Maven with Amazon Corretto JDK inside Docker (no host Java required)
	docker run --rm \
	  -v $(CURDIR)/java:/app \
	  -v $$HOME/.m2:/root/.m2 \
	  -w /app \
	  maven:3.9-amazoncorretto-17 \
	  mvn -DskipTests package
	@echo "Built Java Flink jobs JAR at task4/java/target/task4-flink-jobs-1.0.0.jar"

flink-submit-tiktok: java-build
	docker exec flink-jobmanager bash -c "HASHTAG=SAE bash /scripts/submit-flink-tiktok-java.sh"

flink-submit-twitter: java-build ## Submit Twitter hashtag job (uses same jar, different main class)
	docker exec flink-jobmanager bash -c "HASHTAG=SAE bash /scripts/submit-flink-twitter-java.sh"