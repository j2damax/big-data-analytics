# TukTuk Pathfinder

## MSc Big Data Module - Graph Database Coursework

This project demonstrates the application of **Graph Databases (Neo4j)** to solve real-world routing challenges, using the [TukTuk Tournament](https://tuktuktournament.com/) in Sri Lanka as a case study.

---

## Project Overview

### The Problem: Orienteering Challenge
The TukTuk Tournament is an annual adventure competition where teams navigate Sri Lanka in tuk-tuks, visiting checkpoints to earn points (Gems) while staying within a **1000km distance budget**. This is a variation of the classic "Orienteering Problem."

Traditional relational databases struggle with:
- Recursive pathfinding queries
- Multi-hop relationship traversals
- Distance-constrained route optimization
- Real-time shortest path calculations

### The Solution: Graph Database
We implemented a **Neo4j** graph model running in **Docker** that:
1. Models Sri Lankan tourist attractions as **nodes** with point values
2. Stores road distances as **edge weights** (relationships)
3. Uses **Cypher queries** for efficient path analysis
4. Enables constraint-based route optimization

### Mini-Map Demo
This demo creates a "Mini-Map" of the TukTuk Tournament using **~25 real Sri Lankan tourist attractions** from the southern coast ("Kaliya - Zone 3"), including:
- National Parks (Yala, Kumana, Lunugamvehera)
- Ancient Temples (Tissamaharama Stupa, Mulgirigala)
- Beaches and Surf Spots (Hiriketiya, Okanda Beach)
- Cultural Landmarks (Mahinda Rajapaksa Stadium, Cinema Village)

---

## Quick Start

```bash
# Start Neo4j with data auto-loaded
make up

# Access Neo4j Browser
# URL: http://localhost:7474
# Username: neo4j
# Password: test1234

# Stop services
make down

# Clean up (remove volumes and images)
make clean
```

### Docker Commands (Alternative)
```bash
docker pull neo4j:latest

docker run -d \
  --name neo4j-graphdb \
  -p7474:7474 -p7687:7687 \
  -e NEO4J_AUTH=neo4j/test1234 \
  neo4j:latest
```

---

## Data Model

### Nodes
| Node Type | Properties | Description |
|-----------|------------|-------------|
| **Location** | name, latitude, longitude, points, zone, challenge | Tourist attractions with gem values |
| **Team** | name, members, country | Tournament participants |

### Relationships
| Relationship | Properties | Description |
|--------------|------------|-------------|
| **DISTANCE** | km | Connects locations within 50km |
| **VISITED** | points_earned, visited_date | Links teams to visited locations |

---

## Coursework Queries

The following three Cypher queries demonstrate key graph database capabilities:

### Query 1: Find All Locations Connected to a Starting Point
*Demonstrates: Graph traversal and relationship navigation*

```cypher
// Find all locations within one hop of Yala National Park
MATCH (start:Location {name: 'Yala National Park'})-[d:DISTANCE]->(nearby:Location)
RETURN nearby.name AS location, 
       nearby.points AS gems, 
       d.km AS distance_km
ORDER BY d.km ASC;
```

**What this shows:** How graph databases efficiently traverse relationships to find connected nodes, something that would require complex JOINs in relational databases.

### Query 2: Find the Most Connected Node (Hub Location)
*Demonstrates: Aggregation and degree centrality analysis*

```cypher
// Find locations with the most connections (best hub locations)
MATCH (l:Location)-[d:DISTANCE]-()
RETURN l.name AS location, 
       l.points AS gems,
       count(d) AS connections,
       round(avg(d.km), 2) AS avg_distance_km
ORDER BY connections DESC
LIMIT 5;
```

**What this shows:** Graph databases can easily calculate node centrality metrics, useful for identifying strategic locations in routing problems.

### Query 3: Find Shortest Path Between Two Locations
*Demonstrates: Path-finding algorithms native to graph databases*

```cypher
// Find shortest path from Ice Hiriketiya to Yala National Park
MATCH path = shortestPath(
  (start:Location {name: 'Ice Hiriketiya'})-[:DISTANCE*]-(end:Location {name: 'Yala National Park'})
)
RETURN [node IN nodes(path) | node.name] AS route,
       length(path) AS hops,
       reduce(total = 0.0, r IN relationships(path) | total + r.km) AS total_distance_km;
```

**What this shows:** Neo4j's built-in `shortestPath` function solves complex routing problems efficiently, which would be extremely slow with recursive SQL queries.

---

## Additional Sample Queries

### View All Locations
```cypher
MATCH (l:Location) 
RETURN l.name, l.points, l.zone 
ORDER BY l.points DESC;
```

### Find High-Value Locations (10+ Gems)
```cypher
MATCH (l:Location) 
WHERE l.points >= 10 
RETURN l.name AS attraction, l.points AS gems
ORDER BY l.points DESC;
```

### View Team's Journey
```cypher
MATCH (t:Team)-[v:VISITED]->(l:Location)
RETURN t.name AS team, 
       collect(l.name) AS visited_locations,
       sum(v.points_earned) AS total_gems;
```

### Find Optimal Route Within Distance Budget
```cypher
// Find high-value locations reachable within 100km from starting point
MATCH (start:Location {name: 'Ice Hiriketiya'})
MATCH path = (start)-[:DISTANCE*1..3]->(destination:Location)
WITH destination, 
     reduce(total = 0.0, r IN relationships(path) | total + r.km) AS total_distance,
     destination.points AS gems
WHERE total_distance <= 100
RETURN DISTINCT destination.name AS location, 
       gems, 
       round(total_distance, 2) AS distance_km
ORDER BY gems DESC, distance_km ASC
LIMIT 10;
```

---

## Data Summary

- **~25 Locations** from Sri Lanka's southern coast (Kaliya - Zone 3)
- **Distance relationships** connecting nearby locations (within 50km)
- **1 Sample Team** (GreatDanes) with visited locations

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Environment                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐         ┌─────────────────────────┐   │
│  │   Neo4j Graph   │         │    Data Loader          │   │
│  │    Database     │◄────────│  (init-neo4j.sh)        │   │
│  │  Port: 7474     │         │                         │   │
│  │  Port: 7687     │         │  Loads:                 │   │
│  └────────┬────────┘         │  - kaliya-zone-3.csv    │   │
│           │                  └─────────────────────────┘   │
│           ▼                                                │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                   Graph Model                        │   │
│  │                                                      │   │
│  │    (Team)───[VISITED]───►(Location)                  │   │
│  │                              │                       │   │
│  │                          [DISTANCE]                  │   │
│  │                              │                       │   │
│  │                          (Location)                  │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## Why Graph Database?

| Challenge | Traditional DB + Maps API | Graph Database (Neo4j) |
|-----------|---------------------------|------------------------|
| Distance Calculation | A→B only, API calls | One-time import, native queries |
| Multi-Stop Routes | Complex recursive SQL | Simple path traversal |
| Path Finding | Timeouts on large data | Built-in algorithms |
| Connectivity Analysis | Multiple JOINs | Single query |
| Route Optimization | External processing | Native constraint handling |

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Database | Neo4j 5.x | Graph storage and queries |
| Query Language | Cypher | Declarative graph queries |
| Deployment | Docker Compose | Container orchestration |
| Data Format | CSV | Location import |

---

## License

MIT
