"""
Quix Streams application for real-time traffic metrics computation
Uses Quix State Store for durability + QuestDB for metrics storage
Computes:
- Hourly average vehicle count per sensor
- Daily peak traffic volume
- Daily sensor availability percentage
"""
from itertools import count

from quixstreams import Application
from quixstreams.state import State
from datetime import datetime, timedelta
import json
import logging
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import SimpleConnectionPool

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
import os
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
INPUT_TOPIC = "traffic_raw"
METRICS_TOPIC = "traffic_metrics"
# Separate topics for specific metrics
HOURLY_AVG_TOPIC = os.getenv("HOURLY_AVG_TOPIC", "hourly_average")
AVAILABILITY_TOPIC = os.getenv("AVAILABILITY_TOPIC", "metric_availability")

# QuestDB Configuration
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "localhost")
QUESTDB_PORT = int(os.getenv("QUESTDB_PORT", "8812"))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DB = os.getenv("QUESTDB_DB", "qdb")

# Initialize QuestDB connection pool
try:
    db_pool = SimpleConnectionPool(
        1, 5,
        host=QUESTDB_HOST,
        port=QUESTDB_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        database=QUESTDB_DB
    )
except Exception as e:
    logger.error(f"Failed to create connection pool: {e}")
    db_pool = None


# ============ HELPER FUNCTIONS ============

def init_questdb_tables():
    """Create tables if they don't exist"""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()

        # Hourly metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hourly_average (
                timestamp timestamp,
                device_id symbol,
                hour symbol,
                average_vehicle_count double,
                sample_count int
            ) timestamp(timestamp) partition by DAY;
        """)

        # Sensor availability table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sensor_availability (
                timestamp timestamp,
                device_id symbol,
                date symbol,
                availability_percentage double,
                data_points_received int
            ) timestamp(timestamp) partition by DAY;
        """)

        # Running hourly data table for live visualization
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS running_hourly_data (
                timestamp timestamp,
                device_id symbol,
                hour symbol,
                current_average double,
                current_count int,
                sample_count int
            ) timestamp(timestamp) partition by DAY;
        """)

        conn.commit()
        logger.info("QuestDB tables initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing tables: {e}")
    finally:
        db_pool.putconn(conn)


def store_metric_to_questdb(metric_data: dict):
    """Write metric to QuestDB for persistent storage and analysis"""
    if not db_pool:
        logger.error("Database pool not initialized")
        return

    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        timestamp = datetime.fromisoformat(metric_data["timestamp"].replace('Z', '+00:00'))

        if metric_data["metric_type"] == "hourly_average":
            cursor.execute("""
                INSERT INTO hourly_average (timestamp, device_id, hour, average_vehicle_count, sample_count)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                timestamp,
                metric_data["device_id"],
                metric_data["hour"],
                metric_data["average_vehicle_count"],
                metric_data["sample_count"]
            ))

        elif metric_data["metric_type"] == "sensor_availability":
            cursor.execute("""
                INSERT INTO sensor_availability (timestamp, device_id, date, availability_percentage, data_points_received)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                timestamp,
                metric_data["device_id"],
                metric_data["date"],
                metric_data["availability_percentage"],
                metric_data["data_points_received"]
            ))

        elif metric_data["metric_type"] == "running_hourly_data":
            cursor.execute("""
                INSERT INTO running_hourly_data (timestamp, device_id, hour, current_average, current_count, sample_count)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                timestamp,
                metric_data["device_id"],
                metric_data["hour"],
                metric_data["current_average"],
                metric_data["current_count"],
                metric_data["sample_count"]
            ))

        conn.commit()
        logger.debug(f"Stored metric to QuestDB: {metric_data['metric_type']}")

    except Exception as e:
        logger.error(f"Error storing metric to QuestDB: {e}", exc_info=True)
        conn.rollback()
    finally:
        db_pool.putconn(conn)


def emit_metric_to_kafka(metric_data: dict, producer):
    """Write metric to Kafka topic for real-time dashboards.
    Routes to dedicated topics for hourly averages and availability.
    """
    try:
        metric_type = metric_data.get("metric_type", "")
        key = metric_data["device_id"].encode('utf-8')
        if metric_type == "hourly_average":
            topic = HOURLY_AVG_TOPIC
            key = 'hourly_average'
        elif metric_type == "sensor_availability":
            topic = AVAILABILITY_TOPIC
        else:
            topic = METRICS_TOPIC

        producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(metric_data).encode('utf-8')
        )
    except Exception as e:
        logger.error(f"Error emitting metric to Kafka: {e}")


def process_traffic_record(record: dict, state: State, producer) -> dict:
    """
    Process traffic record using Quix State Store for durability
    State persists to Kafka changelog topic automatically
    Emits metrics to both QuestDB (analysis) and Kafka (real-time)
    """
    try:
        device_id = record.get("atd_device_id")
        # Extract volume from the actual data format and convert to int
        volume_str = record.get("volume", "0")
        try:
            vehicle_count = int(volume_str)
        except (ValueError, TypeError):
            vehicle_count = 0
        timestamp_str = record["timestamp"]

        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        # convert to UTC-6 Austin time
        timestamp = timestamp - timedelta(hours=6)

        hour_key = timestamp.strftime("%Y-%m-%d %H:00")
        date_key = timestamp.strftime("%Y-%m-%d")

        # ============ HOURLY STATE ============
        # Use device-specific key that persists across hours to track current hour state
        hourly_state_key = f"{device_id}:hourly_tracker"
        hourly_state = state.get(hourly_state_key)

        if hourly_state:
            hourly_data = json.loads(hourly_state)
        else:
            hourly_data = {"counts": [], "last_hour": None}

        # Check if hour changed
        if hourly_data["last_hour"] is not None and hourly_data["last_hour"] != hour_key:
            # Hour changed - emit final hourly average for previous hour
            if hourly_data["counts"]:
                avg_count = sum(hourly_data["counts"]) / len(hourly_data["counts"])
                metric = {
                    "metric_type": "hourly_average",
                    "device_id": device_id,
                    "hour": hourly_data["last_hour"],
                    "average_vehicle_count": round(avg_count, 2),
                    "sample_count": len(hourly_data["counts"]),
                    "timestamp": timestamp.isoformat()
                }
                store_metric_to_questdb(metric)
                emit_metric_to_kafka(metric, producer)
                logger.info(f"Hourly metric emitted: {device_id} - {hourly_data['last_hour']}")

            # Reset for new hour
            hourly_data = {"counts": [vehicle_count], "last_hour": hour_key}
        else:
            hourly_data["counts"].append(vehicle_count)
            hourly_data["last_hour"] = hour_key  # Update last_hour to current hour

        state.set(hourly_state_key, json.dumps(hourly_data))

        # ============ EMIT RUNNING HOURLY DATA TO KAFKA ============
        # Emit running hourly count data to Kafka (not waiting for window completion)
        if hourly_data["counts"]:
            running_avg = sum(hourly_data["counts"]) / len(hourly_data["counts"])
            running_metric = {
                "metric_type": "running_hourly_data",
                "device_id": device_id,
                "hour": hour_key,
                "current_average": round(running_avg, 2),
                "current_count": vehicle_count,
                "sample_count": len(hourly_data["counts"]),
                "timestamp": timestamp.isoformat()
            }
            store_metric_to_questdb(running_metric)
            emit_metric_to_kafka(running_metric, producer)

        # ============ DAILY AVAILABILITY STATE ============
        daily_availability_state_key = f"{device_id}:daily:availability"
        daily_state = state.get(daily_availability_state_key)

        if daily_state:
            daily_data = json.loads(daily_state)
        else:
            daily_data = {
                "data_points": 0,
                "last_date": None
            }

        # Check if day changed for this device
        if daily_data["last_date"] is not None and daily_data["last_date"] != date_key:
            # Day changed - emit final daily metrics for previous day (per device)
            expected_points = 288.0
            availability = min(100, (daily_data["data_points"] / expected_points) * 100)

            metric_availability = {
                "metric_type": "sensor_availability",
                "device_id": device_id,
                "date": daily_data["last_date"],
                "availability_percentage": round(availability, 2),
                "data_points_received": daily_data["data_points"],
                "timestamp": timestamp.isoformat()
            }
            store_metric_to_questdb(metric_availability)
            emit_metric_to_kafka(metric_availability, producer)
            logger.info(f"Daily metrics emitted: {device_id} - {daily_data['last_date']}")

            # Reset for new day for this device
            daily_data = {
                "data_points": 1,
                "last_date": date_key
            }
        else:
            daily_data["data_points"] += 1
            daily_data["last_date"] = date_key  # Update last_date to current date

        state.set(daily_availability_state_key, json.dumps(daily_data))

        logger.debug(f"Processed: device={device_id}, count={vehicle_count}, hour={hour_key}")

    except Exception as e:
        logger.error(f"Error processing record: {e}, record: {record}", exc_info=True)

    return record


# ============ MAIN APPLICATION ============

def main():
    """Main application entry point"""

    # Create Quix Streams application with state store
    app = Application(
        broker_address=KAFKA_BROKER,
        auto_offset_reset="earliest",
        consumer_group="traffic-metrics-consumer",
        state_dir="/tmp/quix_state"  # Local state store (backed by changelog topic)
    )

    # Define topics
    input_topic = app.topic(INPUT_TOPIC, value_deserializer="json")
    
    # Get the producer from the application
    producer = app.get_producer()

    # Create streaming dataframe
    sdf = app.dataframe(input_topic)

    # Process with stateful operation
    def process_with_state(record, state: State):
        """Stateful processing callback"""
        process_traffic_record(record, state, producer)
        return record

    # Apply stateful processing
    sdf.apply(process_with_state, stateful=True)

    # Run the application
    logger.info("Starting Traffic Metrics Quix Streams Application with State Store...")
    try:
        init_questdb_tables()
        app.run()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    finally:
        if db_pool:
            db_pool.closeall()


if __name__ == "__main__":
    main()