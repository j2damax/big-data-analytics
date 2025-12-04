#!/bin/bash

# TukTuk Pathfinder - Neo4j Data Initialization Script
# MSc Big Data Module - Graph Database Coursework
# This script loads the "Mini-Map" dataset of Sri Lanka's south coast attractions.

# Wait for Neo4j to be ready
echo "Waiting for Neo4j to be ready..."
until cypher-shell -a neo4j://neo4j:7687 -u neo4j -p test1234 "RETURN 1;" > /dev/null 2>&1; do
  echo "Neo4j is not ready yet. Waiting 5 seconds..."
  sleep 5
done

echo "Neo4j is ready! Loading TukTuk Pathfinder Mini-Map data..."

# Execute the Cypher script to load data
cypher-shell -a neo4j://neo4j:7687 -u neo4j -p test1234 -f /var/lib/neo4j/import/load-data.cypher

echo "Data loading completed!"

# Verify data was loaded
echo ""
echo "=========================================="
echo "TukTuk Pathfinder - Data Summary"
echo "=========================================="
echo ""
echo "Location nodes:"
cypher-shell -a neo4j://neo4j:7687 -u neo4j -p test1234 "MATCH (l:Location) RETURN count(l) as location_count;"
echo ""
echo "Distance relationships:"
cypher-shell -a neo4j://neo4j:7687 -u neo4j -p test1234 "MATCH ()-[r:DISTANCE]->() RETURN count(r) as distance_relationships;"
echo ""
echo "Team nodes:"
cypher-shell -a neo4j://neo4j:7687 -u neo4j -p test1234 "MATCH (t:Team) RETURN t.name as team_name, t.country as country;"
echo ""
echo "Visited relationships:"
cypher-shell -a neo4j://neo4j:7687 -u neo4j -p test1234 "MATCH ()-[v:VISITED]->() RETURN count(v) as visited_count;"
echo ""
echo "High-value locations (10+ points):"
cypher-shell -a neo4j://neo4j:7687 -u neo4j -p test1234 "MATCH (l:Location) WHERE l.points >= 10 RETURN l.name, l.points ORDER BY l.points DESC LIMIT 5;"
echo ""
echo "=========================================="
echo "TukTuk Pathfinder Mini-Map ready!"
echo "=========================================="