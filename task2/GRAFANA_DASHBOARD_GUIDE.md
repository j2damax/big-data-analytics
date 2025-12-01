# Grafana Dashboard Guide for Sensor Data Metrics

## Overview

This guide explains how to access and use the Grafana dashboard to visualize metrics produced by the sensor data consumer in Task2. The dashboard provides real-time insights into traffic sensor data including hourly averages, daily peaks, and sensor availability.

## Access Information

- **Grafana URL**: http://localhost:3000
- **Username**: admin
- **Password**: admin
- **Dashboard Name**: "Sensor Data Metrics"

## Dashboard Panels

### 1. Hourly Average Vehicle Count
- **Type**: Time series line chart
- **Description**: Shows the hourly average vehicle count for each sensor device
- **Data Source**: QuestDB (metrics table)
- **Metric Type**: `hourly_average`
- **Use Case**: Monitor traffic patterns throughout the day

### 2. Daily Peak Traffic Volume  
- **Type**: Bar chart
- **Description**: Displays the peak traffic volume recorded each day by device
- **Data Source**: QuestDB (metrics table)
- **Metric Type**: `daily_peak_traffic`
- **Use Case**: Identify high-traffic periods and compare device performance

### 3. Sensor Availability Percentage
- **Type**: Stat panel with color thresholds
- **Description**: Shows the availability percentage of each sensor device
- **Data Source**: QuestDB (metrics table)
- **Metric Type**: `sensor_availability`
- **Color Coding**:
  - Red: < 70% availability
  - Yellow: 70-90% availability  
  - Green: > 90% availability
- **Use Case**: Monitor sensor health and data quality

### 4. Real-time Vehicle Count by Device
- **Type**: Time series line chart
- **Description**: Shows the sample count (number of data points) received per hour by device
- **Data Source**: QuestDB (metrics table)
- **Metric Type**: `hourly_average` (sample_count field)
- **Use Case**: Monitor data ingestion rates and detect missing data

## Getting Started

### 1. Start the Infrastructure
```bash
cd /Users/lahiru/projects/big-data-analytics/task2
docker-compose up -d
```

### 2. Wait for Services to Initialize
- Kafka: ~30 seconds
- QuestDB: ~20 seconds
- Grafana: ~10 seconds
- Producer/Consumer: ~1 minute

### 3. Access Grafana
1. Open browser and navigate to http://localhost:3000
2. Login with admin/admin credentials
3. Navigate to "Dashboards" → "Sensor Data Metrics"

### 4. Verify Data Flow
1. Check that the sensor data producer is generating data
2. Verify the consumer is processing data and storing metrics in QuestDB
3. Confirm metrics appear in the Grafana dashboard panels

## Data Sources Configuration

The dashboard uses the following data source:
- **Name**: QuestDB
- **Type**: PostgreSQL
- **Host**: questdb:8812
- **Database**: qdb
- **Username**: admin
- **Password**: quest

## SQL Queries Used

### Hourly Average Vehicle Count
```sql
SELECT 
  to_timestamp(hour, 'yyyy-MM-dd HH24:MI') as time,
  device_id as metric,
  average_vehicle_count as value
FROM metrics 
WHERE metric_type = 'hourly_average'
  AND hour >= '${__from:date:YYYY-MM-DD HH24:MI}'
  AND hour <= '${__to:date:YYYY-MM-DD HH24:MI}'
ORDER BY time
```

### Daily Peak Traffic Volume
```sql
SELECT 
  date,
  device_id,
  peak_volume
FROM metrics 
WHERE metric_type = 'daily_peak_traffic'
  AND date >= '${__from:date:YYYY-MM-DD}'
  AND date <= '${__to:date:YYYY-MM-DD}'
ORDER BY date DESC
LIMIT 100
```

### Sensor Availability
```sql
SELECT 
  device_id,
  availability_percentage,
  data_points_received
FROM metrics 
WHERE metric_type = 'sensor_availability'
  AND date >= '${__from:date:YYYY-MM-DD}'
  AND date <= '${__to:date:YYYY-MM-DD}'
ORDER BY date DESC
```

## Troubleshooting

### No Data Appearing
1. Check if all containers are running: `docker-compose ps`
2. Verify QuestDB has data: Access QuestDB console at http://localhost:9000
3. Check sensor data consumer logs: `docker-compose logs sensor-data-consumer`
4. Verify producer is generating data: `docker-compose logs sensor-data-producer`

### Connection Issues
1. Ensure QuestDB container is healthy and accessible
2. Check Grafana logs: `docker-compose logs grafana`
3. Verify datasource configuration in Grafana UI

### Performance Issues
1. Adjust time ranges to reduce query load
2. Limit data retrieval with LIMIT clauses in queries
3. Consider data retention policies in QuestDB

## Customization

To modify the dashboard:
1. Edit `/Users/lahiru/projects/big-data-analytics/task2/grafana/dashboards/sensor-data-metrics.json`
2. Restart Grafana container: `docker-compose restart grafana`
3. Changes will be automatically provisioned on startup

## Metrics Schema

The dashboard expects the following table structure in QuestDB:

```sql
CREATE TABLE metrics (
    metric_type SYMBOL,
    device_id SYMBOL,
    hour SYMBOL,           -- for hourly_average
    date SYMBOL,           -- for daily metrics
    average_vehicle_count DOUBLE,
    sample_count INT,
    peak_volume DOUBLE,
    availability_percentage DOUBLE,
    data_points_received INT,
    timestamp TIMESTAMP
);
```

This dashboard provides comprehensive monitoring capabilities for the traffic sensor data pipeline, enabling real-time analysis of traffic patterns, sensor performance, and data quality.