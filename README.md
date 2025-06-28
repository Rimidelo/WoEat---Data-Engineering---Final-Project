# WoEat – Data Engineering Pipeline

## 1. Project Purpose
WoEat is a complete batch + streaming data pipeline that ingests food-delivery data, applies multi-layer processing (Bronze → Silver → Gold), and produces an analytics-ready dashboard. It showcases Apache Spark, Kafka, Iceberg, and Airflow running in Docker.

## 2. Prerequisites
• Docker Desktop ≥ 20.10 with **docker-compose** enabled
• Host machine ≈ 8 GB RAM free (Spark + Airflow containers)

## 2.1 Container Networking (read once)
All compose files share two named bridge networks so that Spark, Kafka and Airflow can talk to each other:
* **finalproject_iceberg_net** – storage / Spark / Airflow
* **finalproject_kafka_net**   – Kafka / Airflow

They are created automatically when you launch the first two stacks in the order shown below.  
If you ever start the stacks out of order and see network-not-found errors, create them manually:
```bash
docker network create finalproject_iceberg_net
docker network create finalproject_kafka_net
```
Then re-run the `docker-compose … up -d` commands.

---
## 3. Quick-Start (≈ 15 minutes)
```bash
# 1  Spin up core services (wait ~30 s between each command)
docker-compose -f docker-compose.spark.yml  up -d --build
docker-compose -f docker-compose.kafka.yml  up -d
docker-compose -f docker-compose.airflow.yml up -d

# 2  Generate initial 5 000 historical orders
docker exec spark-iceberg python /home/iceberg/processing/generate_5000_orders.py

# 3  Create the analytics dashboard (optional)
docker exec spark-iceberg python /home/iceberg/project/create_data_dashboard.py
```
When the commands finish you will have:
• Bronze, Silver and Gold Iceberg tables populated with batch data
• An HTML dashboard (`woeat_dashboard.html`) that you can copy out and open locally

---
## 4. Detailed Workflow (when you have more time)
### 4.1 Infrastructure
```bash
# Check containers
docker ps --format "table {{.Names}}\t{{.Status}}"
```
Required containers: `spark-iceberg`  `kafka`  `zookeeper`  `minio`  `iceberg-rest`  `airflow-webserver`  `airflow-scheduler`

### 4.2 (Optional) Real-Time Streaming
1. **Producer –> Kafka**
   ```bash
   docker exec -it spark-iceberg python - <<'PY'
   from streaming.orders_producer import OrdersProducer
   OrdersProducer().produce_orders(num_orders=4000, delay_seconds=0.05)
   PY
   ```
2. **Kafka → Bronze**   Handled automatically by either:
   • `processing/kafka_stream_ingestion.py` (manual run) or
   • Airflow task **bronze_stream_ingestion** (see §4.3)

### 4.3 Orchestration with Airflow
1. Open the UI → http://localhost:4040 (login: `admin` / `admin`)
2. Enable DAG `woeat_etl_pipeline` and click **Trigger DAG**
3. The DAG will execute:
   1. Bronze streaming ingestion
   2. Silver cleaning & validation
   3. Gold star-schema creation
   4. Business reports & data-quality checks

### 4.4 Verification
```bash
docker exec spark-iceberg python /home/iceberg/project/verify_orders.py
```
Expected (after full DAG run):
```
Bronze: ≥ 9 000 orders        # 5 000 batch + 4 000 streaming (example)
Silver: same count (cleaned)
Gold:   same count (analytics)
```

---
## 5. Service End-Points
| Service | URL | Default Credentials |
|---------|-----|---------------------|
| Airflow Web UI | http://localhost:4040 | admin / admin |
| Spark Master UI | http://localhost:8080 | – |
| Spark Jobs UI | http://localhost:4041 | – |
| MinIO Browser | http://localhost:9001 | admin / password |
| Kafka UI (Redpanda Console) | http://localhost:8081 | – |

---
## 6. Clean Restart
```bash
# Stop containers
docker-compose -f docker-compose.spark.yml  down
docker-compose -f docker-compose.kafka.yml  down
docker-compose -f docker-compose.airflow.yml down

# (Optional) remove volumes
docker volume prune -f

# Relaunch services
# …then repeat the Quick-Start
```

---
## 7. Need More Detail?
The full step-by-step demo with screenshots and advanced options lives in **[PROJECT_GUIDE.md](PROJECT_GUIDE.md)**.

---
© 2025 WoEat Data Team


