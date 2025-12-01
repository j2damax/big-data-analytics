#!/bin/bash

# Wait for Neo4j to be ready
echo "Waiting for Neo4j to be ready..."
until cypher-shell -a neo4j://neo4j:7687 -u neo4j -p test1234 "RETURN 1;" > /dev/null 2>&1; do
  echo "Neo4j is not ready yet. Waiting 5 seconds..."
  sleep 5
done

echo "Neo4j is ready! Loading data..."

# Execute the Cypher script to load data
cypher-shell -a neo4j://neo4j:7687 -u neo4j -p test1234 -f /var/lib/neo4j/import/load-data.cypher

echo "Data loading completed!"

# Verify data was loaded
echo "Verifying data load..."
cypher-shell -a neo4j://neo4j:7687 -u neo4j -p test1234 "MATCH (n:User) RETURN count(n) as user_count;"
cypher-shell -a neo4j://neo4j:7687 -u neo4j -p test1234 "MATCH ()-[r:FRIEND]->() RETURN count(r) as relationship_count;"

echo "Data loading verification completed!"