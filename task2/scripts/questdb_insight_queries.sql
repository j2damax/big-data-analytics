-- ============================================================================
-- QuestDB SQL Queries for Traffic Sensor Data Insights
-- ============================================================================
-- These queries can be run directly in QuestDB's web console (http://localhost:9000)
-- to analyze traffic sensor data.
--
-- Table Structure Reference:
--   - hourly_average: Hourly average vehicle counts per sensor
--   - sensor_availability: Daily sensor availability percentages
--   - running_hourly_data: Live running hourly data for visualization
--   - daily_max_hourly_volume: Daily peak traffic volume records
-- ============================================================================


-- ============================================================================
-- 1. HOURLY AVERAGE VEHICLE COUNT PER SENSOR
-- ============================================================================

-- Query 1a: Get the latest hourly average for each sensor
-- Returns the most recent hourly average vehicle count per sensor
SELECT 
    device_id,
    hour,
    average_vehicle_count,
    sample_count,
    timestamp
FROM hourly_average
LATEST ON timestamp PARTITION BY device_id;

-- Query 1b: Get hourly averages for all sensors for a specific date
-- Replace '2024-07-01' with your desired date
SELECT 
    device_id,
    hour,
    average_vehicle_count,
    sample_count,
    timestamp
FROM hourly_average
WHERE hour LIKE '2024-07-01%'
ORDER BY device_id, hour;

-- Query 1c: Get hourly averages grouped by hour across all sensors
-- Shows average vehicle count per hour of day across all sensors
SELECT 
    hour,
    avg(average_vehicle_count) AS avg_vehicle_count,
    sum(sample_count) AS total_samples,
    count() AS sensor_count
FROM hourly_average
GROUP BY hour
ORDER BY hour;

-- Query 1d: Get the running/live hourly data (current state)
-- Shows the latest running averages for each sensor and hour
SELECT 
    device_id,
    hour,
    current_average,
    current_count,
    sample_count,
    timestamp
FROM running_hourly_data
LATEST ON timestamp PARTITION BY device_id, hour;

-- Query 1e: Get hourly trend for a specific sensor
-- Replace 'SENSOR_ID' with your actual sensor device_id
SELECT 
    hour,
    average_vehicle_count,
    sample_count,
    timestamp
FROM hourly_average
WHERE device_id = 'SENSOR_ID'
ORDER BY hour;


-- ============================================================================
-- 2. DAILY PEAK TRAFFIC VOLUME ACROSS ALL SENSORS
-- ============================================================================

-- Query 2a: Get daily peak traffic volume (pre-computed)
-- Shows the maximum hourly total volume for each date
SELECT 
    date,
    max_hourly_total,
    max_hour,
    ts AS timestamp
FROM daily_max_hourly_volume
ORDER BY date DESC;

-- Query 2b: Get the overall maximum peak traffic day
SELECT 
    date,
    max_hourly_total,
    max_hour
FROM daily_max_hourly_volume
ORDER BY max_hourly_total DESC
LIMIT 1;

-- Query 2c: Calculate daily peak from hourly_average table
-- Alternative query to compute daily peak from raw hourly data
-- This sums all sensor volumes per hour, then finds max per day
SELECT 
    substring(hour, 1, 10) AS date,
    max(hourly_total) AS peak_volume,
    first(hour) AS peak_hour
FROM (
    SELECT 
        hour,
        sum(average_vehicle_count * sample_count) AS hourly_total
    FROM hourly_average
    GROUP BY hour
)
GROUP BY substring(hour, 1, 10)
ORDER BY date;

-- Query 2d: Get hourly traffic totals across all sensors
-- Shows the total vehicle count per hour across all sensors
SELECT 
    hour,
    sum(average_vehicle_count * sample_count) AS total_volume,
    count(DISTINCT device_id) AS active_sensors
FROM hourly_average
GROUP BY hour
ORDER BY hour;

-- Query 2e: Find top 10 busiest hours by total traffic volume
SELECT 
    hour,
    sum(average_vehicle_count * sample_count) AS total_volume,
    count(DISTINCT device_id) AS active_sensors
FROM hourly_average
GROUP BY hour
ORDER BY total_volume DESC
LIMIT 10;


-- ============================================================================
-- 3. DAILY SENSOR AVAILABILITY (%) BASED ON DATA PRESENCE OR GAPS
-- ============================================================================

-- Query 3a: Get latest sensor availability for each sensor
SELECT 
    device_id,
    date,
    availability_percentage,
    data_points_received,
    timestamp
FROM sensor_availability
LATEST ON timestamp PARTITION BY device_id;

-- Query 3b: Get average availability per sensor across all dates
SELECT 
    device_id,
    avg(availability_percentage) AS avg_availability,
    min(availability_percentage) AS min_availability,
    max(availability_percentage) AS max_availability,
    count() AS days_recorded
FROM sensor_availability
GROUP BY device_id
ORDER BY avg_availability DESC;

-- Query 3c: Get daily availability overview (average across all sensors)
SELECT 
    date,
    avg(availability_percentage) AS avg_availability,
    min(availability_percentage) AS min_availability,
    max(availability_percentage) AS max_availability,
    count() AS sensor_count
FROM sensor_availability
GROUP BY date
ORDER BY date;

-- Query 3d: Find sensors with low availability (below 80%)
SELECT 
    device_id,
    date,
    availability_percentage,
    data_points_received
FROM sensor_availability
WHERE availability_percentage < 80
ORDER BY availability_percentage ASC;

-- Query 3e: Get sensor availability history for a specific sensor
-- Replace 'SENSOR_ID' with your actual sensor device_id
SELECT 
    date,
    availability_percentage,
    data_points_received,
    timestamp
FROM sensor_availability
WHERE device_id = 'SENSOR_ID'
ORDER BY date;

-- Query 3f: Calculate expected vs actual data points
-- Expected: 288 data points per day (24 hours * 12 readings per hour, i.e., every 5 minutes)
SELECT 
    device_id,
    date,
    data_points_received,
    288 AS expected_points,
    (data_points_received * 100.0 / 288) AS calculated_availability,
    availability_percentage AS stored_availability
FROM sensor_availability
ORDER BY date, device_id;


-- ============================================================================
-- BONUS: COMBINED INSIGHTS QUERIES
-- ============================================================================

-- Query B1: Daily summary - peak volume, average availability, active sensors
SELECT 
    d.date,
    d.max_hourly_total AS peak_volume,
    d.max_hour AS peak_hour,
    s.avg_availability,
    s.sensor_count
FROM daily_max_hourly_volume d
JOIN (
    SELECT 
        date,
        avg(availability_percentage) AS avg_availability,
        count() AS sensor_count
    FROM sensor_availability
    GROUP BY date
) s ON d.date = s.date
ORDER BY d.date;

-- Query B2: Hourly traffic pattern analysis
-- Shows average traffic by hour of day (0-23)
SELECT 
    substring(hour, 12, 5) AS hour_of_day,
    avg(average_vehicle_count) AS avg_vehicle_count,
    sum(sample_count) AS total_samples
FROM hourly_average
GROUP BY substring(hour, 12, 5)
ORDER BY hour_of_day;

-- Query B3: Sensor performance overview
-- Combines hourly averages and availability for each sensor
SELECT 
    h.device_id,
    avg(h.average_vehicle_count) AS avg_hourly_vehicles,
    sum(h.sample_count) AS total_samples,
    s.avg_availability
FROM hourly_average h
JOIN (
    SELECT 
        device_id,
        avg(availability_percentage) AS avg_availability
    FROM sensor_availability
    GROUP BY device_id
) s ON h.device_id = s.device_id
GROUP BY h.device_id, s.avg_availability
ORDER BY h.device_id;
