#!/usr/bin/env bash
set -euo pipefail

echo "[start-producer] Installing dependencies..."
pip install --no-cache-dir kafka-python

sleep 10;

echo "[start-producer] Running social-producer.py"
exec python /scripts/social-producer.py
