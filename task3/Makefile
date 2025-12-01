# Makefile for Big Data Analytics Project

.PHONY: help build up down restart logs clean ps test

up: ## Start all services with automatic data loading
	docker-compose up --build -d
	@echo "Waiting for services to start..."
	@sleep 10
	@echo "Services started! Data automatically loaded into Neo4j."
	@echo "Access web UIs:"
	@echo "  - Neo4j Browser: http://localhost:7474 (user: neo4j, password: test1234)"
	@echo "Data loaded: 30 users and 56 friendship relationships"

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
