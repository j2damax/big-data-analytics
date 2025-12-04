# Makefile for TukTuk Pathfinder - Graph Database Demo
# MSc Big Data Module - Graph Database Coursework

.PHONY: help build up down restart logs clean ps test

up: ## Start all services with automatic data loading
	docker compose up --build -d
	@echo "Waiting for services to start..."
	@sleep 10
	@echo "Services started! Data automatically loaded into Neo4j."
	@echo ""
	@echo "Access Neo4j Browser:"
	@echo "  - URL: http://localhost:7474"
	@echo "  - Username: neo4j"
	@echo "  - Password: test1234"
	@echo ""
	@echo "Data loaded: Sri Lanka south coast tourist attractions (Kaliya - Zone 3)"
	@echo ""
	@echo "Try these Cypher queries in Neo4j Browser:"
	@echo ""
	@echo "  1. View all locations:"
	@echo "     MATCH (l:Location) RETURN l"
	@echo ""
	@echo "  2. Find most connected location (hub):"
	@echo "     MATCH (l:Location)-[d:DISTANCE]-() RETURN l.name, count(d) AS connections ORDER BY connections DESC LIMIT 5"
	@echo ""
	@echo "  3. Find shortest path:"
	@echo "     MATCH path = shortestPath((a:Location {name: 'Ice Hiriketiya'})-[:DISTANCE*]-(b:Location {name: 'Yala National Park'})) RETURN path"

down: ## Stop all services
	docker compose down

restart: ## Restart all services
	docker compose restart

logs: ## View logs from all services
	docker compose logs -f

ps: ## Show status of all containers
	docker compose ps

clean: ## Stop and remove all containers, volumes, and images
	docker compose down -v --rmi all
