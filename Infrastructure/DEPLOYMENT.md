# Olist Data Platform - Deployment Guide

This guide explains how to deploy the Olist Data Platform using Docker Compose on your local machine or a remote server.

## 📋 Prerequisites

- **Docker** (version 20.10+): [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose** (version 2.0+): [Install Docker Compose](https://docs.docker.com/compose/install/)
- **Git**: For cloning the repository
- **Disk Space**: At least 20GB available for containers, images, and data volumes
- **RAM**: Minimum 8GB (16GB recommended for optimal performance)

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/olist-data-platform.git
cd olist-data-platform
```

### 2. Set Up Environment Variables

```bash
# Copy the example env file
cp .env.example .env

# Edit .env with your values (especially for production)
# For local development, defaults are fine
nano .env  # or use your editor
```

**Key variables to update:**

- `MINIO_ROOT_PASSWORD` — MinIO admin password (change from default)
- `POSTGRES_META_PASSWORD` — Dagster metadata DB password
- `POSTGRES_DW_PASSWORD` — Data warehouse DB password
- `JUPYTER_TOKEN` — Jupyter Lab access token
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — AWS/MinIO credentials

### 3. Build Custom Images

Some services require custom Dockerfiles. Build them:

```bash
docker compose build
```

This builds:
- `spark-master` / `spark-worker` / `spark-streaming`
- `dagster-user-code` / `dagster-webserver` / `dagster-daemon`
- `jupyter`
- `streamlit`
- `kafka-connect`

### 4. Start the Platform

```bash
# Start all services in the background
docker compose up -d

# Or start with logs visible (useful for debugging)
docker compose up
```

### 5. Verify Services Are Running

```bash
# Check container status
docker compose ps

# View logs for a specific service
docker compose logs -f dagster-webserver
```

## 📊 Service URLs & Access

Once running, access these services:

| Service | URL | Credentials | Port |
|---------|-----|-------------|------|
| **Dagster UI** | http://localhost:3000 | None (local) | 3000 |
| **Jupyter Lab** | http://localhost:8888 | Token: `olist` | 8888 |
| **Streamlit Dashboard** | http://localhost:8501 | None | 8501 |
| **Spark Master UI** | http://localhost:8080 | None | 8080 |
| **Kafka UI** | http://localhost:8085 | None | 8085 |
| **MinIO Console** | http://localhost:9001 | User: `minioadmin` / Pass: `minioadmin` | 9001 |
| **PostgreSQL (Meta)** | localhost:5433 | User: `dagster` / Pass: `dagster` | 5433 |
| **PostgreSQL (DW)** | localhost:5432 | User: `olist` / Pass: `olist` | 5432 |
| **Redis** | localhost:6379 | None | 6379 |

## 🛑 Stop the Platform

```bash
# Stop all services gracefully
docker compose down

# Also remove volumes (deletes all data!)
docker compose down -v

# View logs after shutdown
docker compose logs
```

## 🔧 Common Operations

### View Real-Time Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f dagster-webserver
docker compose logs -f spark-master
```

### Execute Commands in a Container

```bash
# Access PostgreSQL
docker compose exec postgres-dw psql -U olist -d olist_dw

# Run a command in Spark container
docker compose exec spark-master spark-shell

# Access Jupyter terminal
docker compose exec jupyter bash
```

### Rebuild a Single Service

```bash
docker compose build spark-master --no-cache
docker compose up -d spark-master
```

### Check Container Resource Usage

```bash
docker stats
```

### Clean Up Everything

```bash
# Remove all containers, networks, volumes, images
docker compose down -v --remove-orphans --rmi all
```

## 📁 Project Structure

```
olist-data-platform/
├── docker-compose.yml          # Main composition
├── .env.example                # Environment template (commit this)
├── .env                        # Actual secrets (DO NOT COMMIT)
├── .gitignore                  # Git ignore rules
├── .dockerignore               # Docker build ignore rules
├── README.md                   # Project overview
├── DEPLOYMENT.md               # This file
│
├── dagster/
│   ├── Dockerfile_dagster      # Dagster webserver & daemon
│   ├── Dockerfile_user_code    # Dagster pipeline code
│   ├── workspace.yaml          # Dagster workspace config
│   ├── dagster.yaml            # Dagster instance config
│   └── pipelines/              # Your DAGs & assets
│
├── spark/
│   ├── Dockerfile              # Spark master/worker image
│   └── (other Spark config)
│
├── jupyter/
│   ├── Dockerfile              # Jupyter Lab image
│   └── spark_session.py        # Spark session setup
│
├── streamlit/
│   ├── Dockerfile              # Streamlit dashboard
│   └── dashboard.py            # Dashboard code
│
├── kafka-connect/
│   ├── Dockerfile              # Kafka Connect image
│   └── (connector configs)
│
├── sql/
│   └── init.sql                # PostgreSQL init scripts
│
├── spark_jobs/                 # Spark job scripts
│   ├── streaming_kafka_to_redis.py
│   └── (other jobs)
│
├── notebooks/                  # Jupyter notebooks
│   └── .gitkeep
│
└── data/                       # Local data directory (excluded from git)
    └── .gitkeep
```

## 🚨 Troubleshooting

### Containers Keep Restarting

```bash
# Check logs
docker compose logs <service-name>

# Common causes:
# 1. Port already in use → change port in docker-compose.yml or .env
# 2. Healthcheck failing → services not ready, wait longer
# 3. Out of memory → increase Docker memory limit
```

### Can't Connect to PostgreSQL

```bash
# Check if PostgreSQL is running
docker compose ps postgres-meta postgres-dw

# Test connection
docker compose exec postgres-dw psql -U olist -d olist_dw -c "SELECT 1"
```

### Spark/Kafka Not Starting

```bash
# Check Master is ready first
docker compose logs spark-master
docker compose logs kafka

# Restart dependent services
docker compose restart spark-worker spark-streaming
```

### MinIO Buckets Not Created

```bash
# Manually create buckets
docker compose exec minio-init /bin/sh -c "
  mc alias set local http://minio:9000 minioadmin minioadmin
  mc mb local/bronze --ignore-existing
  mc mb local/silver --ignore-existing
  mc mb local/gold --ignore-existing
"
```

### Disk Space Issues

```bash
# Check disk usage
docker system df

# Clean up unused images/volumes
docker system prune -a --volumes
```

## 🔐 Security Notes for Production

**DO NOT use default credentials in production!** Update in `.env`:

- Change all database passwords
- Use strong MinIO credentials
- Enable Jupyter authentication
- Use environment-specific `.env` files
- Never commit `.env` to Git
- Use secrets management (AWS Secrets Manager, HashiCorp Vault)
- Set firewall rules to restrict port access
- Use HTTPS/SSL for external services
- Enable container health checks and restart policies

## 📝 Useful Docker Compose Commands

```bash
# Start specific services
docker compose up -d postgres-dw redis kafka

# Pause/unpause services
docker compose pause
docker compose unpause

# Check service dependencies
docker compose config

# Validate compose file
docker compose config --quiet && echo "Valid"

# Export compose to YAML
docker compose config > docker-compose.resolved.yml
```

## 📚 Additional Resources

- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Dagster Deployment Guide](https://docs.dagster.io/deployment)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [MinIO Documentation](https://docs.min.io/)

## 🤝 Contributing

To contribute, ensure:

1. All custom Dockerfiles build successfully
2. `docker compose up` starts without errors
3. All environment variables in `.env.example` are documented
4. Update `.gitignore` if adding new directories
5. Document new services in this file

## 📞 Support

For issues or questions:

1. Check logs: `docker compose logs -f <service>`
2. Review Troubleshooting section above
3. Open an issue on GitHub with logs attached
