# Quick Start Guide

This guide will help you get started with the Big Data Analytics project in just a few minutes.

## Prerequisites Check

Before starting, ensure you have:
- [ ] Docker installed (version 20.10+)
- [ ] Docker Compose installed (version 2.0+)
- [ ] At least 8GB RAM allocated to Docker
- [ ] At least 20GB free disk space

Check versions:
```bash
docker --version
docker-compose --version
```

## 5-Minute Setup

### Step 1: Clone and Navigate
```bash
git clone https://github.com/j2damax/big-data-analytics.git
cd big-data-analytics
```

### Step 2: Build Containers (5-10 minutes)
```bash
docker-compose build
```

### Step 3: Start Services (1-2 minutes)
```bash
docker-compose up -d
```

### Step 4: Verify Services
```bash
docker-compose ps
```

All services should show "Up" status.

### Step 5: Access Web UIs

Open in your browser:
- Hadoop NameNode: http://localhost:9870
- Spark Master: http://localhost:8080
- Flink Dashboard: http://localhost:8082

## Try Your First Examples

### Example 1: Spark WordCount (30 seconds)
```bash
docker exec -it spark-master bash
cd /scripts
python3 spark_example.py
exit
```

### Example 2: Kafka Messaging (1 minute)
```bash
docker exec -it kafka bash
cd /scripts
python3 kafka_example.py
exit
```

### Example 3: Flink Streaming (1 minute)
```bash
docker exec -it flink-jobmanager bash
cd /scripts
python3 flink_example.py
exit
```

## Common Commands

### Start all services
```bash
docker-compose up -d
```

### Stop all services
```bash
docker-compose down
```

### View logs
```bash
docker-compose logs -f
```

### Restart a service
```bash
docker-compose restart spark-master
```

### Enter a container
```bash
docker exec -it <container-name> bash
```

## Troubleshooting

### Services won't start
```bash
# Check logs
docker-compose logs <service-name>

# Rebuild
docker-compose build --no-cache
```

### Port conflicts
Edit `docker-compose.yml` and change the port mapping:
```yaml
ports:
  - "NEW_PORT:ORIGINAL_PORT"
```

### Out of memory
Increase Docker memory in Docker Desktop settings to at least 8GB.

### Containers crash
```bash
# Remove and recreate
docker-compose down -v
docker-compose up -d
```

## Next Steps

1. **Read the main README.md** for detailed documentation
2. **Explore technology-specific READMEs** in each directory
3. **Modify example scripts** in the scripts/ directory
4. **Build your own data pipeline** combining multiple technologies
5. **Check learning resources** in each technology's README

## Getting Help

- Check the main README.md for detailed documentation
- View container logs: `docker-compose logs <service-name>`
- Open an issue on GitHub for bugs or questions

## Clean Up

When you're done experimenting:
```bash
# Stop and remove containers
docker-compose down

# Remove volumes (cleans all data)
docker-compose down -v

# Remove images (frees disk space)
docker-compose down --rmi all
```

Happy learning! 🚀
