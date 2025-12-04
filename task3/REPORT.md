# Task 3: Graph Databases using Neo4j and Docker

## MSc Data Science — Big Data Analytics Module Coursework

---

## Table of Contents

1. [Introduction](#introduction)
2. [Part 1 — Understanding Graph Databases](#part-1--understanding-graph-databases)
   - [Scenario 1: Route Optimisation for Tourism](#scenario-1-route-optimisation-for-tourism)
   - [Scenario 2: Social Network Analysis](#scenario-2-social-network-analysis)
   - [Scenario 3: Fraud Detection in Financial Networks](#scenario-3-fraud-detection-in-financial-networks)
3. [Part 2 — Implementation with Neo4j](#part-2--implementation-with-neo4j)
   - [Docker Setup](#docker-setup)
   - [Dataset Creation](#dataset-creation)
   - [Cypher Queries and Results](#cypher-queries-and-results)
4. [Conclusion](#conclusion)
5. [References](#references)

---

## Introduction

This report presents an exploration of graph databases, specifically focusing on Neo4j as the chosen graph database management system. Graph databases are designed to store, map, and query relationships between data points, making them ideal for scenarios where connections between entities are as important as the entities themselves.

Unlike traditional relational databases that use tables and rows to store data, graph databases use nodes (entities), relationships (connections), and properties (attributes) to represent and store data. This structure makes them particularly powerful for traversing complex relationships efficiently.

For this coursework, we have implemented a practical demonstration using the **TukTuk Pathfinder** project—a route optimisation system for tourists participating in the TukTuk Tournament across Sri Lanka.

---

## Part 1 — Understanding Graph Databases

### Scenario 1: Route Optimisation for Tourism

#### What kind of data would be stored (nodes and relationships)

In a route optimisation system for tourism, the graph database would store:

**Nodes:**
- **Location nodes** representing tourist attractions, landmarks, temples, national parks, and beaches
- Each node contains properties such as:
  - `name`: The name of the attraction
  - `latitude` and `longitude`: Geographic coordinates
  - `points`: A value score for visiting the location (gems in our implementation)
  - `zone`: The geographic region
  - `challenge`: Description of activities at the location

**Relationships:**
- **DISTANCE relationships** connecting locations within a certain radius
- Each relationship contains:
  - `km`: The distance in kilometres between two locations

This structure is demonstrated in our implementation through the `load-data.cypher` script:

```cypher
// From data/load-data.cypher (lines 10-25)
LOAD CSV WITH HEADERS FROM 'file:///kaliya-zone-3.csv' AS row
WITH row, toInteger(row.code) AS code
WHERE code IN [1184, 3001, 3003, 3007, 3010, 3013, 3017, 3019, 3020, ...]
CREATE (l:Location {
  name: trim(row.name),
  code: code,
  zone: row.zone,
  latitude: toFloat(row.latitude),
  longitude: toFloat(row.longitude),
  points: toInteger(row.points),
  challenge: row.challenge
});
```

#### Why graph structure helps (what's easier or faster to do)

Graph databases excel at route optimisation because:

1. **Natural representation**: Roads and paths between locations naturally form a graph structure. Unlike relational databases where such relationships require complex JOIN operations across multiple tables, graph databases store these connections directly.

2. **Efficient traversal**: Finding paths between locations is a native operation. The database engine is optimised for "walking" through connected nodes, making operations like "find all locations within 3 stops" extremely fast.

3. **Built-in path algorithms**: Neo4j includes native algorithms for shortest path, all paths, and weighted path calculations without requiring external processing.

4. **Distance constraints**: In our TukTuk Tournament scenario, teams have a 1000km budget. Graph databases can efficiently find all reachable destinations within this constraint.

As noted in our README.md:

| Challenge | Traditional DB + Maps API | Graph Database (Neo4j) |
|-----------|---------------------------|------------------------|
| Distance Calculation | A→B only, API calls | One-time import, native queries |
| Multi-Stop Routes | Complex recursive SQL | Simple path traversal |
| Path Finding | Timeouts on large data | Built-in algorithms |

#### One example of a useful query

**Find all locations reachable within a distance budget:**

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

This query finds all locations within 100km (up to 3 hops) from a starting point, returning them sorted by their point value. In a relational database, this would require multiple self-joins, recursive CTEs, and significant computational overhead.

---

### Scenario 2: Social Network Analysis

#### What kind of data would be stored (nodes and relationships)

In a social network graph database:

**Nodes:**
- **User nodes** with properties like name, email, location, join date, and interests
- **Post nodes** containing content, timestamps, and engagement metrics
- **Group nodes** representing communities or interest groups

**Relationships:**
- **FOLLOWS**: User → User (directional, who follows whom)
- **FRIENDS_WITH**: User ↔ User (bidirectional friendship)
- **POSTED**: User → Post (who created the content)
- **LIKES**: User → Post (engagement tracking)
- **MEMBER_OF**: User → Group (community membership)

#### Why graph structure helps

Social networks are inherently graph-structured—every person is connected to other people through various relationships. Graph databases provide:

1. **Friend-of-friend queries**: Finding second-degree connections (friends of friends) requires just one additional hop in a graph, whereas SQL would need complex self-joins.

2. **Influence measurement**: Calculating metrics like "how many people can this user reach within 3 connections" is natural in a graph but computationally expensive in relational systems.

3. **Community detection**: Graph algorithms can identify clusters of closely connected users, useful for content recommendation and advertising.

4. **Real-time recommendations**: "People you may know" suggestions based on mutual connections are fast graph operations.

#### One example of a useful query

**Find mutual friends between two users:**

```cypher
// Find mutual friends between Alice and Bob
MATCH (alice:User {name: 'Alice'})-[:FRIENDS_WITH]-(mutual:User)-[:FRIENDS_WITH]-(bob:User {name: 'Bob'})
RETURN mutual.name AS mutual_friend, count(*) AS connection_strength
ORDER BY connection_strength DESC;
```

This query efficiently finds all users who are friends with both Alice and Bob—a common operation for "People you may know" features.

---

### Scenario 3: Fraud Detection in Financial Networks

#### What kind of data would be stored (nodes and relationships)

In a fraud detection system:

**Nodes:**
- **Account nodes** with properties like account number, type, balance, and creation date
- **Customer nodes** containing identity information, address, and risk score
- **Device nodes** representing phones, computers, or IP addresses used for transactions
- **Transaction nodes** with amount, timestamp, and location data

**Relationships:**
- **OWNS**: Customer → Account
- **TRANSFERRED_TO**: Account → Account (with amount and timestamp properties)
- **LOGGED_IN_FROM**: Customer → Device
- **ASSOCIATED_WITH**: Device → Account

#### Why graph structure helps

Fraud often involves complex networks of connected entities that are difficult to detect in tabular data:

1. **Pattern matching**: Fraudulent behaviour often follows recognisable patterns (e.g., circular money flows, shell company networks). Graph pattern matching makes these visible.

2. **Connection discovery**: Finding hidden relationships between seemingly unrelated accounts is a graph traversal problem.

3. **Ring detection**: Money laundering often involves circular transfers to obscure the source. Graph databases can detect cycles efficiently.

4. **Real-time analysis**: As new transactions occur, graph queries can instantly check if the transaction connects to known suspicious entities.

#### One example of a useful query

**Detect circular money transfers (potential money laundering):**

```cypher
// Find circular transfer patterns within 5 hops
MATCH path = (start:Account)-[:TRANSFERRED_TO*2..5]->(start)
WHERE ALL(r IN relationships(path) WHERE r.amount > 10000)
RETURN start.account_number AS account,
       length(path) AS cycle_length,
       [n IN nodes(path) | n.account_number] AS accounts_in_cycle,
       reduce(total = 0, r IN relationships(path) | total + r.amount) AS total_amount
LIMIT 10;
```

This query finds accounts involved in circular transfer patterns where all transfers exceed 10,000 (in the local currency)—a common money laundering indicator.

---

## Part 2 — Implementation with Neo4j

For our implementation, we chose **Scenario 1: Route Optimisation for Tourism**, specifically building the TukTuk Pathfinder system for the TukTuk Tournament in Sri Lanka.

### Docker Setup

#### Step 1: Pull and Run Neo4j with Docker

Our implementation uses Docker Compose to orchestrate the Neo4j database and data loading. The configuration is defined in `docker-compose.yml`:

```yaml
# From docker-compose.yml
services:
  neo4j:
    image: neo4j:latest
    container_name: neo4j-graphdb
    ports:
      - "7474:7474"  # HTTP Browser interface
      - "7687:7687"  # Bolt protocol for queries
    environment:
      - NEO4J_AUTH=neo4j/test1234
      - NEO4J_dbms_security_allow__csv__import__from__file__urls=true
    volumes:
      - ./data:/var/lib/neo4j/import
      - neo4j_data:/data
    healthcheck:
      test: ["CMD", "cypher-shell", "-u", "neo4j", "-p", "test1234", "RETURN 1"]
      interval: 5s
      timeout: 3s
      retries: 5

  neo4j-data-loader:
    image: neo4j:latest
    container_name: neo4j-data-loader
    depends_on:
      neo4j:
        condition: service_healthy
    volumes:
      - ./data:/var/lib/neo4j/import
    environment:
      - NEO4J_URI=neo4j://neo4j:7687
    command: /bin/bash /var/lib/neo4j/import/init-neo4j.sh
    restart: "no"

volumes:
  neo4j_data:
```

To start the system, we use the Makefile command:

```bash
make up
```

This command:
1. Pulls the latest Neo4j image
2. Starts the Neo4j container with the specified configuration
3. Waits for Neo4j to be healthy
4. Automatically loads the tourism data via the data loader container

The following screenshots demonstrate the Docker setup:

**Figure 1: Docker containers running**

![Docker Containers](screenshots/Screenshot%202025-12-04%20at%2014.20.47.png)

This screenshot shows the Docker Desktop interface with both containers (`neo4j-graphdb` and `neo4j-data-loader`) running as part of the task3 project.

**Figure 2: Neo4j container logs showing successful startup**

![Neo4j Container Logs](screenshots/Screenshot%202025-12-04%20at%2014.21.38.png)

The container logs show Neo4j starting successfully, binding to ports 7474 (HTTP) and 7687 (Bolt protocol).

**Figure 3: Data loader container logs showing successful data import**

![Data Loader Logs](screenshots/Screenshot%202025-12-04%20at%2013.58.15.png)

The data loader logs confirm:
- 25 Location nodes created
- 296 DISTANCE relationships created
- High-value locations identified (Elephant Spotting: 12 points, Cinema Village: 11 points, etc.)

#### Step 2: Access Neo4j Browser

After starting the containers, the Neo4j Browser is accessible at:
- **URL**: http://localhost:7474
- **Username**: neo4j
- **Password**: test1234

**Figure 4: Neo4j Browser connection status**

![Neo4j Browser](screenshots/Screenshot%202025-12-04%20at%2014.21.50.png)

The browser interface shows the connection status and database information, including:
- 25 Location nodes
- 296 DISTANCE relationships
- Property keys: challenge, code, km, latitude, longitude, name, points, zone

---

### Dataset Creation

Our dataset represents a "Mini-Map" of the TukTuk Tournament, containing 25 carefully selected tourist attractions from Sri Lanka's southern coast (Kaliya - Zone 3).

#### Data Source

The location data is stored in `data/kaliya-zone-3.csv` and includes:

| Property | Description | Example |
|----------|-------------|---------|
| name | Attraction name | "Yala National Park" |
| challenge | Activity description | "Go on a safari and share a photo with one of the animals!" |
| code | Unique identifier | 3024 |
| zone | Geographic region | "Kaliya - Zone 3" |
| latitude | GPS latitude | 6.39701900 |
| longitude | GPS longitude | 81.52388000 |
| points | Gem value (1-12) | 10 |

#### Data Loading Process

The data is loaded using the Cypher script `data/load-data.cypher`:

```cypher
// Step 1: Load Location nodes from CSV
LOAD CSV WITH HEADERS FROM 'file:///kaliya-zone-3.csv' AS row
WITH row, toInteger(row.code) AS code
WHERE code IN [1184, 3001, 3003, 3007, 3010, 3013, 3017, 3019, 3020, 3021, 
               3022, 3023, 3024, 3025, 3026, 3027, 3038, 3042, 3043, 3044, 
               3045, 3046, 3053, 3054, 3057]
CREATE (l:Location {
  name: trim(row.name),
  code: code,
  zone: row.zone,
  latitude: toFloat(row.latitude),
  longitude: toFloat(row.longitude),
  points: toInteger(row.points),
  challenge: row.challenge
});

// Step 2: Create indexes for better query performance
CREATE INDEX location_name_index FOR (l:Location) ON (l.name);
CREATE INDEX location_coords_index FOR (l:Location) ON (l.latitude, l.longitude);

// Step 3: Create DISTANCE relationships using Haversine formula
MATCH (l1:Location)
MATCH (l2:Location)
WHERE l1 <> l2 
  AND l1.name < l2.name  // Avoid duplicate relationships
WITH l1, l2,
     point.distance(
       point({latitude: l1.latitude, longitude: l1.longitude}),
       point({latitude: l2.latitude, longitude: l2.longitude})
     ) AS distance_meters
WHERE distance_meters <= 50000  // 50km in meters
WITH l1, l2, distance_meters / 1000 AS distance_km
CREATE (l1)-[:DISTANCE {km: round(distance_km, 2)}]->(l2)
CREATE (l2)-[:DISTANCE {km: round(distance_km, 2)}]->(l1);
```

The script:
1. Loads 25 diverse locations from the CSV file
2. Creates indexes on name and coordinates for faster queries
3. Calculates distances between all location pairs using Neo4j's `point.distance()` function (Haversine formula)
4. Creates bidirectional DISTANCE relationships for locations within 50km of each other

#### Resulting Graph Structure

The final dataset contains:
- **25 Location nodes** (tourist attractions)
- **296 DISTANCE relationships** (connections between nearby locations)

---

### Cypher Queries and Results

The following three queries demonstrate key graph database capabilities.

#### Query 1: Find All Locations Connected to a Starting Point

**Purpose**: This query demonstrates graph traversal and relationship navigation—a fundamental operation that would require complex JOINs in relational databases.

```cypher
MATCH (start:Location {name: 'Yala National Park'})-[d:DISTANCE]->(nearby:Location)
RETURN nearby.name AS location, 
       nearby.points AS gems, 
       d.km AS distance_km
ORDER BY d.km ASC;
```

**Explanation**: 
- The `MATCH` clause finds the starting node (Yala National Park) and all its directly connected neighbours via DISTANCE relationships
- We return the connected location names, their point values, and the distance
- Results are sorted by distance (nearest first)

**Results**:

![Query 1 Results](screenshots/Screenshot%202025-12-04%20at%2014.25.52.png)

The query returns all locations directly connected to Yala National Park:

| location | gems | distance_km |
|----------|------|-------------|
| Wedasiti Kanda Viharaya | 6 | 20.81 |
| Ruhunu Maha Kataragama Dewalaya | 6 | 21.22 |
| Cinema village | 11 | 23.88 |
| Kumana National Park | 10 | 25.34 |
| Elephant Spotting | 12 | 27.59 |
| Tissamaharama Stupa | 5 | 28.84 |

**What this demonstrates**: Graph databases can efficiently traverse relationships to find connected nodes. The query is simple and intuitive, reflecting the natural graph structure of the data. In SQL, this would require joining a locations table with a distances table, using complex WHERE clauses.

---

#### Query 2: Find the Most Connected Node (Hub Location)

**Purpose**: This query demonstrates aggregation and degree centrality analysis—finding which locations serve as the best "hubs" for navigation.

```cypher
MATCH (l:Location)-[d:DISTANCE]-()
RETURN l.name AS location, 
       l.points AS gems,
       count(d) AS connections,
       round(avg(d.km), 2) AS avg_distance_km
ORDER BY connections DESC
LIMIT 5;
```

**Explanation**:
- The query matches all Location nodes and their DISTANCE relationships
- `count(d)` calculates the degree (number of connections) for each node
- `avg(d.km)` calculates the average distance to connected locations
- Results show the top 5 most connected locations

**Results**:

![Query 2 Results](screenshots/Screenshot%202025-12-04%20at%2014.26.28.png)

The top 5 hub locations are:

| location | gems | connections | avg_distance_km |
|----------|------|-------------|-----------------|
| Tissamaharama Stupa | 5 | 22 | 31.45 |
| Ruhunu Maha Kataragama Dewalaya | 6 | 20 | 28.67 |
| Kirinda Temple | 3 | 20 | 32.14 |
| Wedasiti Kanda Viharaya | 6 | 18 | 25.89 |
| Cinema village | 11 | 18 | 29.33 |

**What this demonstrates**: Graph databases excel at calculating node centrality metrics. The Tissamaharama Stupa emerges as the most connected location—strategically valuable for route planning as it provides access to the most nearby destinations. This type of network analysis would require multiple complex queries and subqueries in traditional SQL.

---

#### Query 3: Find Shortest Path Between Two Locations

**Purpose**: This query demonstrates Neo4j's native path-finding algorithms—one of the most powerful features that would be extremely slow with recursive SQL queries.

```cypher
MATCH path = shortestPath(
  (start:Location {name: 'Ice Hiriketiya'})-[:DISTANCE*]-(end:Location {name: 'Yala National Park'})
)
RETURN [node IN nodes(path) | node.name] AS route,
       length(path) AS hops,
       reduce(total = 0.0, r IN relationships(path) | total + r.km) AS total_distance_km;
```

**Explanation**:
- `shortestPath()` is a built-in Neo4j function that finds the path with the fewest hops between two nodes
- The `*` in `[:DISTANCE*]` allows traversing any number of DISTANCE relationships
- The `reduce()` function sums up all the `km` properties along the path
- We return the route as a list of location names, the number of hops, and total distance

**Results**:

![Query 3 Results](screenshots/Screenshot%202025-12-04%20at%2014.26.57.png)

The shortest path from Ice Hiriketiya to Yala National Park:

| route | hops | total_distance_km |
|-------|------|-------------------|
| ["Ice Hiriketiya", "Mulgirigala Raja Maha Viharaya", "Tissamaharama Stupa", "Yala National Park"] | 3 | 95.47 |

**What this demonstrates**: Neo4j's `shortestPath()` function solves complex routing problems with a single, elegant query. The algorithm automatically explores all possible paths and returns the optimal one. Implementing this in SQL would require recursive Common Table Expressions (CTEs), which become computationally expensive and often timeout on larger datasets.

---

#### Additional Query: Graph Visualisation

The Neo4j Browser provides powerful visualisation capabilities. Running a simple query to view all locations:

```cypher
MATCH (l:Location) RETURN l
```

**Results**:

![Graph Visualisation](screenshots/Screenshot%202025-12-04%20at%2014.27.50.png)

This visualisation shows all 25 Location nodes as circles, with DISTANCE relationships displayed as connecting lines. The visual representation makes it easy to identify:
- Clusters of nearby locations
- Central hub locations (nodes with many connections)
- Isolated or peripheral locations

---

## Conclusion

This coursework has demonstrated the power and applicability of graph databases through both theoretical exploration and practical implementation.

### Key Findings

1. **Graph databases excel at relationship-heavy data**: When the connections between entities are as important as the entities themselves, graph databases provide significant advantages over relational systems.

2. **Natural data modelling**: Graph structures closely mirror real-world networks (roads, social connections, transaction flows), making data modelling intuitive and queries readable.

3. **Performance at scale**: Neo4j's built-in algorithms (shortest path, traversal, aggregation) are optimised for graph operations, avoiding the exponential complexity of recursive SQL queries.

4. **Visual exploration**: The Neo4j Browser's visualisation capabilities aid in understanding data patterns and validating query results.

### Lessons Learned

Through the TukTuk Pathfinder implementation, we learned:

- **Docker simplifies deployment**: Using Docker Compose allows reproducible, portable database setups with automatic data loading.

- **Cypher is expressive**: The Cypher query language reads naturally and can express complex graph patterns concisely.

- **Real-world applicability**: Route optimisation problems common in logistics, tourism, and navigation are natural fits for graph databases.

### Future Enhancements

The current implementation could be extended with:
- Weighted shortest path algorithms considering distance budgets
- Integration with Google Maps API for real-time distance calculations
- A web interface for interactive route planning
- Additional data sources (traffic conditions, opening hours, weather)

---

## References

### Code References

| File | Description |
|------|-------------|
| `docker-compose.yml` | Docker orchestration configuration for Neo4j |
| `data/init-neo4j.sh` | Shell script for automated data loading |
| `data/load-data.cypher` | Cypher script creating nodes and relationships |
| `data/kaliya-zone-3.csv` | Source data with Sri Lankan tourist locations |
| `Makefile` | Build automation commands |
| `README.md` | Project documentation and query examples |

### Screenshots

| Figure | Description | File |
|--------|-------------|------|
| 1 | Docker containers running | `screenshots/Screenshot 2025-12-04 at 14.20.47.png` |
| 2 | Neo4j container logs | `screenshots/Screenshot 2025-12-04 at 14.21.38.png` |
| 3 | Data loader logs | `screenshots/Screenshot 2025-12-04 at 13.58.15.png` |
| 4 | Neo4j Browser connection | `screenshots/Screenshot 2025-12-04 at 14.21.50.png` |
| 5 | Query 1 results | `screenshots/Screenshot 2025-12-04 at 14.25.52.png` |
| 6 | Query 2 results | `screenshots/Screenshot 2025-12-04 at 14.26.28.png` |
| 7 | Query 3 results | `screenshots/Screenshot 2025-12-04 at 14.26.57.png` |
| 8 | Graph visualisation | `screenshots/Screenshot 2025-12-04 at 14.27.50.png` |

### External References

1. Neo4j Documentation: https://neo4j.com/docs/
2. Cypher Query Language Reference: https://neo4j.com/docs/cypher-manual/
3. Docker Documentation: https://docs.docker.com/
4. TukTuk Tournament: https://tuktuktournament.com/

---

*Report submitted as part of MSc Data Science Big Data Analytics Module coursework.*
