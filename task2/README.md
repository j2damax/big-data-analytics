## Sequence of commands

```bash
make download-dataset
make up
```

## QuestDB SQL Queries for Insights

The file `scripts/questdb_insight_queries.sql` contains SQL queries that you can run directly in QuestDB's web console (http://localhost:9000) to analyze traffic sensor data.

### Available Queries

1. **Hourly Average Vehicle Count Per Sensor**
   - Get latest hourly averages for each sensor
   - Get hourly averages for a specific date
   - Get hourly averages grouped by hour across all sensors
   - Get running/live hourly data

2. **Daily Peak Traffic Volume Across All Sensors**
   - Get daily peak traffic volume (pre-computed)
   - Find the overall maximum peak traffic day
   - Calculate daily peak from raw hourly data
   - Get hourly traffic totals across all sensors

3. **Daily Sensor Availability (%) Based on Data Presence**
   - Get latest sensor availability for each sensor
   - Get average availability per sensor across all dates
   - Get daily availability overview
   - Find sensors with low availability (below 80%)

### How to Use

1. Start the services with `make up`
2. Open QuestDB web console at http://localhost:9000
3. Copy and paste queries from `scripts/questdb_insight_queries.sql`
4. Execute the queries to see the results