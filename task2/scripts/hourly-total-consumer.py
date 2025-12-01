"""
Consumer that subscribes to the `hourly_average` topic and computes:
- Total volume across all devices for each hour (sum of device hourly totals) in-memory
- Daily maximum hourly volume and its hour; only the daily maximum is written to QuestDB

Late data handling:
- Accept late device hourly results up to a configurable threshold after the hour
- Finalize an hour only after `hour_end + LATE_THRESHOLD`
- Finalize a date only after `date_end + LATE_THRESHOLD` and then write one record

Input message schema (from hourly_average topic):
{
  "metric_type": "hourly_average",
  "device_id": "...",
  "hour": "YYYY-MM-DD HH:00",
  "average_vehicle_count": float,
  "sample_count": int,
  "timestamp": iso8601
}

Total contribution per device-hour = average_vehicle_count * sample_count.
"""
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from quixstreams import Application
from quixstreams.state import State

import psycopg2
from psycopg2.pool import SimpleConnectionPool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
HOURLY_AVG_TOPIC = os.getenv("HOURLY_AVG_TOPIC", "hourly_average")

# Late data threshold in minutes
LATE_THRESHOLD_MIN = int(os.getenv("LATE_THRESHOLD_MIN", "10"))

# QuestDB config
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "localhost")
QUESTDB_PORT = int(os.getenv("QUESTDB_PORT", "8812"))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DB = os.getenv("QUESTDB_DB", "qdb")


def _make_pool():
    try:
        return SimpleConnectionPool(
            1,
            5,
            host=QUESTDB_HOST,
            port=QUESTDB_PORT,
            user=QUESTDB_USER,
            password=QUESTDB_PASSWORD,
            database=QUESTDB_DB,
        )
    except Exception as e:
        logger.error(f"Failed to create QuestDB connection pool: {e}")
        return None


db_pool = _make_pool()


def init_tables():
    if not db_pool:
        return
    conn = db_pool.getconn()
    try:
        cur = conn.cursor()
        # Stores max hourly total per date
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_max_hourly_volume (
                ts timestamp,
                date symbol,
                max_hourly_total long,
                max_hour symbol
            ) timestamp(ts) PARTITION BY DAY;
            """
        )

        conn.commit()
        logger.info("QuestDB table for daily max hourly volume initialized")
    except Exception as e:
        logger.error(f"Error initializing QuestDB tables: {e}")
        conn.rollback()
    finally:
        db_pool.putconn(conn)


def _parse_hour_start(hour_str: str) -> datetime:
    # hour_str format: "YYYY-MM-DD HH:00"
    # Interpret as UTC for consistency
    return datetime.strptime(hour_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_event_ts(ts_str: str) -> datetime | None:
    """Parse event timestamp string to aware UTC datetime.

    Supports ISO-8601 strings with 'Z' suffix or explicit offset.
    Returns None if parsing fails.
    """
    if not ts_str:
        return None
    try:
        # Normalize Z to +00:00 for fromisoformat
        norm = ts_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(norm)
        # Ensure timezone-aware; assume UTC if naive
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def store_daily_max(date: str, max_total: int, max_hour: str):
    if not db_pool:
        return
    conn = db_pool.getconn()
    try:
        cur = conn.cursor()
        # Store at end of the date
        date_dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        ts_naive = date_dt.replace(tzinfo=None)
        cur.execute(
            """
            INSERT INTO daily_max_hourly_volume (ts, date, max_hourly_total, max_hour)
            VALUES (%s, %s, %s, %s)
            """,
            (ts_naive, date, int(max_total), max_hour),
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to write daily max to QuestDB: {e}")
        conn.rollback()
    finally:
        db_pool.putconn(conn)


def process_record(value: dict, state: State):
    """
    Maintain per-hour aggregation and finalize after lateness threshold.
    Track max hourly total per date; write to QuestDB only once per date
    after the date's lateness deadline passes.

    State keys used:
    - agg:hours -> JSON object mapping hour -> {
          "total": int,
          "finalized": bool,
          "deadline": iso8601,
          "devices": { device_id: int_contribution }
      }
    - agg:daily_max -> JSON object mapping date -> {
          "max_total": int,
          "max_hour": str,
          "deadline": iso8601,   # date_start + 1d + LATE_THRESHOLD_MIN
          "finalized": bool      # written to QuestDB
      }
    """
    if not value:
        return

    metric_type = value.get("metric_type")
    if metric_type != "hourly_average":
        return

    device_id = value.get("device_id")
    hour = value.get("hour")  # "YYYY-MM-DD HH:00"
    avg = float(value.get("average_vehicle_count", 0.0) or 0.0)
    cnt = int(value.get("sample_count", 0) or 0)
    contribution = int(round(avg * cnt))

    # Event-time and watermark handling
    event_ts = _parse_event_ts(value.get("timestamp"))
    if event_ts is None:
        logger.warning("Skipping record without valid event timestamp")
        return

    # Maintain watermark as max observed event time
    wm_json = state.get("agg:max_event_ts")
    if wm_json:
        try:
            current_wm = datetime.fromisoformat(wm_json)
        except Exception:
            current_wm = None
    else:
        current_wm = None
    watermark = event_ts if current_wm is None or event_ts > current_wm else current_wm
    if current_wm is None or watermark != current_wm:
        state.set("agg:max_event_ts", watermark.isoformat())

    # Load state
    hours_json = state.get("agg:hours")
    hours = json.loads(hours_json) if hours_json else {}

    # Ensure hour bucket exists
    hour_bucket = hours.get(hour)
    if not hour_bucket:
        hour_start = _parse_hour_start(hour)
        deadline_dt = hour_start + timedelta(hours=1, minutes=LATE_THRESHOLD_MIN)
        hour_bucket = {
            "total": 0,
            "finalized": False,
            "deadline": deadline_dt.isoformat(),
            "devices": {},  # track per-device contribution for idempotency
        }

    # If already finalized, ignore late beyond threshold
    if hour_bucket.get("finalized"):
        logger.debug(f"Ignoring late record for finalized hour {hour} from {device_id}")
    else:
        # Upsert per-device contribution to be idempotent and concurrency safe
        devices = hour_bucket.get("devices") or {}
        prev = int(devices.get(device_id, 0) or 0)
        if prev != contribution:
            devices[device_id] = contribution
            hour_bucket["devices"] = devices
            hour_bucket["total"] = int(hour_bucket.get("total", 0)) - prev + contribution

    hours[hour] = hour_bucket

    # Load daily max state (per-date buckets)
    daily_max_json = state.get("agg:daily_max")
    daily_max = json.loads(daily_max_json) if daily_max_json else {}

    # Finalize any hours whose deadline is <= watermark (event-time based)
    changed = False

    for h, bucket in list(hours.items()):
        # consider only non-finalized
        if not bucket.get("finalized"):
            deadline = datetime.fromisoformat(bucket["deadline"])  # stored as isoformat UTC
            if watermark >= deadline:
                # Finalize the hour: update daily max bucket for its date
                total = int(bucket.get("total", 0))
                # Update max volume per date
                date = h.split(" ")[0]  # Extract date from "YYYY-MM-DD HH:00"
                # Ensure date bucket exists with deadline
                date_bucket = daily_max.get(date)
                if not date_bucket:
                    # date_start is midnight of that date UTC
                    date_start = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    date_deadline = date_start + timedelta(days=1, minutes=LATE_THRESHOLD_MIN)
                    date_bucket = {
                        "max_total": 0,
                        "max_hour": h,
                        "deadline": date_deadline.isoformat(),
                        "finalized": False,
                    }

                # Update running max
                if total > int(date_bucket.get("max_total", 0)):
                    date_bucket["max_total"] = total
                    date_bucket["max_hour"] = h
                    logger.info(f"Updated in-memory daily max for {date} to {total} at hour {h}")

                daily_max[date] = date_bucket

                # mark finalized and drop the bucket to free memory
                bucket["finalized"] = True
                changed = True
                del hours[h]


    # After processing hours, check if any date buckets are ready to be finalized (write once)
    dates_changed = False
    for d, dbuck in list(daily_max.items()):
        if not dbuck.get("finalized"):
            d_deadline = datetime.fromisoformat(dbuck["deadline"])  # UTC
            if watermark >= d_deadline:
                store_daily_max(d, int(dbuck.get("max_total", 0)), dbuck.get("max_hour"))
                logger.info(f"Finalized daily max for {d}: {dbuck['max_total']} at hour {dbuck['max_hour']}")
                dbuck["finalized"] = True
                daily_max[d] = dbuck
                dates_changed = True

    # Persist state
    if changed or dates_changed:
        state.set("agg:hours", json.dumps(hours))
        state.set("agg:daily_max", json.dumps(daily_max))
    else:
        # still persist any potential in-hour device upserts
        state.set("agg:hours", json.dumps(hours))


def main():
    init_tables()

    app = Application(
        broker_address=KAFKA_BROKER,
        consumer_group="hourly-total-consumer",
        auto_offset_reset="earliest",
        consumer_extra_config={"enable.auto.commit": True},
        state_dir="/tmp/quix_state"  # Local state store (backed by changelog topic)
    )

    input_topic = app.topic(HOURLY_AVG_TOPIC)
    sdf = app.dataframe(input_topic)

    def consume(record, state: State):
        process_record(record, state)

    sdf.apply(consume, stateful=True)

    app.run()


if __name__ == "__main__":
    main()
