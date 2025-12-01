# Makefile for Big Data Analytics Project

.PHONY: help build up down restart logs clean ps test

up: ## Start all services
	docker-compose up --build -d
	@echo "Waiting for services to start..."
	@sleep 10
	@echo "Services started! Access web UIs:"
	@echo "  - Redpanda (Kafka UI): http://localhost:8083"
	@echo "  - QuestDB web console: http://localhost:9000"
	@echo "  - Grafana UI: http://localhost:3000"

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

kafka-prepare:
	docker exec kafka-broker /scripts/create-topics.sh
	
download-dataset:
	python scripts/download-dataset.py