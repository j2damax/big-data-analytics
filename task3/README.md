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
- Distance constrained route optimization
- Real time shortest path calculations

### The Solution: Graph Database
We implemented a **Neo4j** graph model running in **Docker** that:
1. Models Sri Lankan tourist attractions as **nodes** with point values
2. Stores road distances as **edge weights** (relationships)
3. Uses **Cypher queries** for efficient path analysis
4. Enables constraint based route optimization

### Mini Map Demo
This demo creates a "Mini Map" of the TukTuk Tournament using **~25 real Sri Lankan tourist attractions** from the southern coast ("Kaliya - Zone 3"), including:
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


### Relationships
| Relationship | Properties | Description |
|--------------|------------|-------------|
| **DISTANCE** | km | Connects locations within 50km |

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
