// Load nodes from CSV
LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS row
CREATE (u:User {
  userId: toInteger(row.userId),
  name: row.name,
  age: toInteger(row.age)
});

// Create index for better performance
CREATE INDEX user_id_index FOR (u:User) ON (u.userId);

// Load relationships from CSV
LOAD CSV WITH HEADERS FROM 'file:///relationships.csv' AS row
MATCH (start:User {userId: toInteger(row.START_ID)})
MATCH (end:User {userId: toInteger(row.END_ID)})
CREATE (start)-[:FRIEND]->(end);