## Overview
- Kafka producers stream TikTok/Twitter CSVs; Flink Java jobs compute hashtag metrics.

## Dependencies
- Python packages for helper scripts: see `requirements.txt`
- Java/Maven for Flink jobs: see `java/pom.xml`

## Setup (Python helpers)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick Start
```bash
make prepare
make up
make java-build
make flink-submit-tiktok
make flink-submit-twitter
```
