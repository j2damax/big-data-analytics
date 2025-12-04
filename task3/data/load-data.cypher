// TukTuk Pathfinder - Graph Database Demo (MSc Big Data Coursework)
// This script loads Sri Lanka tourism locations from the "Kaliya - Zone 3" 
// (Southern Coast) and creates distance relationships for the TukTuk Tournament
// routing optimization problem.
//
// Dataset: ~25 carefully selected locations from Sri Lanka's south coast
// including temples, national parks, beaches, and cultural landmarks.

// Step 1: Load Location nodes from the Kaliya Zone 3 CSV
// We select a subset of ~25 diverse attractions for the coursework demo
LOAD CSV WITH HEADERS FROM 'file:///kaliya-zone-3.csv' AS row
WITH row, toInteger(row.code) AS code
// Select specific locations to get a diverse mix of ~25 nodes
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

// Step 2: Create index on location name for better query performance
CREATE INDEX location_name_index FOR (l:Location) ON (l.name);

// Step 3: Create index on coordinates for proximity queries
CREATE INDEX location_coords_index FOR (l:Location) ON (l.latitude, l.longitude);

// Step 4: Create DISTANCE relationships between nearby locations (within ~50km)
// Using Neo4j's point.distance for Haversine formula calculation
// This creates a graph structure where locations can be traversed based on distance
// Note: For this demo dataset (25 locations), Cartesian product is acceptable
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