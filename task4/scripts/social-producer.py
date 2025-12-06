import os
import json
import time
import csv
from datetime import datetime
from typing import Iterator, Dict

from kafka import KafkaProducer


def iter_csv(path: str) -> Iterator[Dict[str, str]]:
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize keys: strip whitespace
            yield {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}


def send_file(producer: KafkaProducer, topic: str, path: str, sleep_sec: float) -> int:
    sent = 0
    for row in iter_csv(path):
        payload = json.dumps(row).encode('utf-8')
        producer.send(topic, payload)
        print(f"[producer] Sent message {sent} to topic {topic}")
        sent += 1
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        producer.flush()
    return sent


def main():
    bootstrap = os.getenv('KAFKA_BROKER', 'localhost:9092')
    twitter_topic = os.getenv('TWITTER_TOPIC', 'twitter_posts')
    tiktok_topic = os.getenv('TIKTOK_TOPIC', 'tiktok_posts')
    twitter_csv = os.getenv('TWITTER_CSV', '../data/processed/twitter-dataset.csv')
    tiktok_csv = os.getenv('TIKTOK_CSV', '../data/processed/processed/tiktok-dataset.csv')
    interval_ms = int(os.getenv('SEND_INTERVAL_MS', '200'))
    sleep_sec = interval_ms / 1000.0

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap],
        acks='all',
        value_serializer=lambda v: v,  # already bytes
        retries=5,
        linger_ms=5,
    )

    total = 0
    if os.path.exists(tiktok_csv):
        print(f"[producer] Sending TikTok CSV {tiktok_csv} -> topic {tiktok_topic}")
        total += send_file(producer, tiktok_topic, tiktok_csv, sleep_sec)
    else:
        print(f"[producer] WARNING: TikTok CSV not found at {tiktok_csv}")

    if os.path.exists(twitter_csv):
        print(f"[producer] Sending Twitter CSV {twitter_csv} -> topic {twitter_topic}")
        total += send_file(producer, twitter_topic, twitter_csv, sleep_sec)
    else:
        print(f"[producer] WARNING: Twitter CSV not found at {twitter_csv}")

    print(f"[producer] Done. Total messages sent: {total}")


if __name__ == '__main__':
    main()
