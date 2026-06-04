# Olist Data Platform 🚀

A complete, containerized data platform with **15 microservices** for data orchestration, processing, storage, and visualization.

## ⭐ What's Included

**Data Orchestration & Analytics**
- 🔄 **Dagster** - Workflow orchestration with 9 data assets (bronze/silver/gold layers)
- 📓 **Jupyter Lab** - Interactive notebooks with PySpark
- 📊 **Streamlit** - Real-time analytics dashboards

**Data Processing**
- ⚡ **Apache Spark** - Master + Worker cluster for distributed processing
- 📨 **Kafka** - Event streaming with Zookeeper coordination
- 🔗 **Kafka Connect** - Data connectors for integration

**Data Storage & Caching**
- 🗄️ **PostgreSQL** - 2x (Dagster metadata + Data warehouse)
- 🪣 **MinIO** - S3-compatible object storage (bronze/silver/gold buckets)
- 💾 **Redis** - In-memory cache

**Monitoring & UI**
- 🎯 **Kafka UI** - Event streaming monitor
- 🎛️ **Spark UI** - Cluster monitoring
- 🪣 **MinIO Console** - S3 file browser
---

## 🚀 Quick Start (5 minutes)

### Prerequisites
- **Docker Desktop** ([download](https://www.docker.com/products/docker-desktop))
- **Git** ([download](https://git-scm.com/downloads))
- 4+ CPU cores, 8GB+ RAM, 20GB disk space

### Start Everything
```bash
# Clone
git clone <this-repo>
cd olist-data-platform

# Setup
cp .env.example .env
docker compose build
docker compose up -d

# Access
# Dagster UI: http://localhost:3000
# Jupyter: http://localhost:8888 (token: olist)
# Streamlit: http://localhost:8501
```


---

## 📖 Documentation

| Document | Purpose |
|----------|---------|
| **[DEPLOYMENT.md](DEPLOYMENT.md)** | Commands, daily use, troubleshooting, Comprehensive setup guide |



---

## 🌐 Service Access

| Service | URL | Credentials |
|---------|-----|-------------|
| **Dagster** | http://localhost:3000 | none |
| **Jupyter** | http://localhost:8888 | token: `olist` |
| **Streamlit** | http://localhost:8501 | none |
| **Spark Master** | http://localhost:8080 | none |
| **Kafka UI** | http://localhost:8085 | none |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **PostgreSQL Meta** | localhost:5433 | dagster / dagster |
| **PostgreSQL DW** | localhost:5432 | olist / olist |
| **Redis** | localhost:6379 | none |

---

## 📊 Services

```
┌─────────────────────────────────────────────────────────┐
│                   OLIST DATA PLATFORM                   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │  ORCHESTRATION & ANALYTICS                      │    │
│  │  • Dagster (workflow engine)                    │    │
│  │  • Jupyter Lab (notebooks)                      │    │
│  │  • Streamlit (dashboards)                       │    │
│  └─────────────────────────────────────────────────┘    │
│                           ↓                             │
│  ┌─────────────────────────────────────────────────┐    │
│  │  PROCESSING LAYER                               │    │
│  │  • Spark Master + Worker (distributed)          │    │
│  │  • Kafka + Zookeeper (streaming)                │    │
│  │  • Kafka Connect (integration)                  │    │
│  └─────────────────────────────────────────────────┘    │
│                           ↓                             │
│  ┌──────────────────────────────────────────────────┐   │
│  │  STORAGE LAYER                                   │   │
│  │  • PostgreSQL x2 (metadata + DW)                 │   │
│  │  • MinIO (S3-compatible)                         │   │
│  │  • Redis (cache)                                 │   │
│  └──────────────────────────────────────────────────┘   │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**All services communicate via custom Docker network:**
- PostgreSQL Meta: postgres-meta:5432
- PostgreSQL DW: postgres-dw:5432
- Spark Master: spark-master:7077
- Kafka: kafka:29092 (internal) / localhost:9092 (external)
- MinIO: minio:9000
- Redis: redis:6379

---

## 📁 Project Structure

```
olist-data-platform/
├── docker-compose.yml              # Main compose file
├── .env.example                    # Environment template
├── .dockerignore                   # Docker build exclusions
│
├── dagster/                        # Orchestration
│   ├── Dockerfile_dagster          # Webserver + Daemon
│   ├── Dockerfile_user_code        # Pipeline code
│   ├── pipelines/
│   │   └── olist_assets.py         # data assets (bronze/silver/gold)
│   ├── workspace.yaml              # Code location config
│   └── dagster.yaml                # Instance config
│
├── spark/                          # Processing cluster
│   ├── Dockerfile                  # Spark base image
│   ├── spark-defaults.conf         # Configuration
│   └── entrypoint.sh               # Start script
│
├── jupyter/                        # Analytics notebooks
│   └── Dockerfile
│
├── streamlit/                      # Dashboards
│   └── Dockerfile
│
├── kafka-connect/                  # Data connectors
│   └── Dockerfile
│
├── sql/                            # Database init scripts
│   └── init.sql
│
├── data/                           # Local data volume
├── spark_jobs/                     # Spark job scripts
└── notebooks/                      # Jupyter notebooks
```

---

## 🎯 Typical Workflow

### 1. Develop in Jupyter
```bash
# Open http://localhost:8888
# Create notebooks in notebooks/ folder
# Changes auto-sync to container
```

### 2. Schedule in Dagster
```python
# Edit dagster/pipelines/olist_assets.py
@asset
def my_data() -> dict:
    return {"status": "processed"}

# Access http://localhost:3000 to trigger
```

### 3. Process with Spark
```python
# Submit jobs to spark-master:7077
spark = SparkSession.builder.master("spark://spark-master:7077").getOrCreate()
df = spark.read.parquet("s3a://bronze/data")
```

### 4. Visualize in Streamlit
```bash
# Create streamlit/app.py
# Access http://localhost:8501 instantly
```

---

## 🔄 Development

### Hot Reload (Code Changes Live)
All source directories are mounted as volumes:
- `./dagster/pipelines` → `/opt/dagster/app` (Dagster code)
- `./notebooks` → `/home/jovyan/work` (Jupyter)
- `./streamlit` → `/app` (Streamlit)
- `./spark_jobs` → `/opt/spark_jobs` (Spark jobs)
- `./data` → `/opt/data` (Shared data)

Edit files locally, changes appear instantly.

### Add Dependencies
```bash
# Edit Dockerfile in service directory, then:
docker compose build jupyter
docker compose up -d jupyter
```

---


## 🐛 Troubleshooting

### Services won't start?
```bash
docker compose down
docker compose up -d --remove-orphans
docker compose logs
```

### Check container health
```bash
docker compose ps  # All services shown here
docker logs jupyter  # View specific logs
docker exec -it jupyter bash  # Enter container
```

### Reset everything (WARNING: deletes data)
```bash
docker compose down -v  # Stop and remove volumes
docker compose up -d    # Start fresh
```

### Out of memory?
Increase Docker memory in Docker Desktop → Preferences → Resources


---

## 🔐 Security

### Development ✅
- Default credentials used (minioadmin, dagster, olist)
- `.env.example` as template
- No sensitive data in git

### Production ⚠️
- [ ] Change all default passwords
- [ ] Use secrets management (Vault, AWS Secrets Manager)
- [ ] Enable authentication in Dagster UI
- [ ] Use private container registry
- [ ] Isolate network access
- [ ] Rotate credentials regularly

---

## 📊 Performance Tips

- **Spark:** Increase `SPARK_WORKER_MEMORY` and `SPARK_WORKER_CORES` in docker-compose.yml
- **Disk:** Check `docker system df` and run `docker system prune` periodically
- **Memory:** Monitor with `docker stats`
- **Network:** Use internal Kafka address `kafka:29092` from containers

---

## 📚 Links

- [Docker Docs](https://docs.docker.com/)
- [Dagster](https://docs.dagster.io/)
- [Apache Spark](https://spark.apache.org/docs/)
- [Jupyter Lab](https://jupyterlab.readthedocs.io/)
- [Kafka](https://kafka.apache.org/documentation/)
- [PostgreSQL](https://www.postgresql.org/docs/)
- [MinIO](https://docs.min.io/)

---

## 🤝 Contributing

1. Create a branch: `git checkout -b feature/my-feature`
2. Make changes locally (hot reload works)
3. Test: `docker compose up -d && docker compose logs -f`
4. Commit: `git commit -m "Add feature"`
5. Push: `git push origin feature/my-feature`
6. Create Pull Request

---

## 📞 Support

For issues or questions:
1. Check [DEPLOYMENT.md](DEPLOYMENT.md)
2. View logs: `docker compose logs -f <service>`
3. Ask the team

Happy coding! 🚀
