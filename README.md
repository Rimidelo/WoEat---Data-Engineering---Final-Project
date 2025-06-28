# Docker Networking
All docker-compose files use two shared bridge networks so Spark, Kafka and Airflow containers can discover each other:
* **finalproject_iceberg_net** – Spark / Iceberg / MinIO / Airflow
* **finalproject_kafka_net**   – Kafka / Zookeeper / Airflow

They are created automatically the first time you run the stack (any `docker-compose up`). If you launch the compose files out of order and encounter a *"network finalproject_… not found"* error, create them manually and retry:
```bash
docker network create finalproject_iceberg_net
docker network create finalproject_kafka_net
```

## WoEat
Complete food delivery data pipeline with real-time streaming, data quality management, and business intelligence. Processes 5,000+ orders through Bronze → Silver → Gold layers using Apache Spark, Kafka, Iceberg, and Airflow.

## Clean File Organization

### Processing Pipeline (Clean & Organized):
```
processing/
├── spark_config.py              # Spark session configuration
├── create_supporting_data.py    # Step 1: Create batch data (restaurants, drivers, etc.)
├── generate_5000_orders.py      # Step 2: Generate 5000 batch orders
├── kafka_stream_ingestion.py    # Step 3: Kafka streaming ingestion ONLY
├── silver_processing.py         # Step 4: Data cleaning & validation
├── gold_processing.py           # Step 5: Star schema & analytics
└── late_data_detector.py        # Step 6: Late data detection & reprocessing
```

## Step-by-Step Demo Guide

### STEP 1: Start the Infrastructure (5 minutes)

#### 1.1 Start Services
```bash
# Start Spark & Storage
docker-compose -f docker-compose.spark.yml up -d --build

# Start Kafka & Streaming  
docker-compose -f docker-compose.kafka.yml up -d

# Start Airflow & Orchestration
docker-compose -f docker-compose.airflow.yml up -d
```

#### 1.2 Verify All Services Running
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

**Expected containers:**
- `spark-iceberg` (Up)
- `kafka` (Up)
- `zookeeper` (Up)
- `minio` (Up)
- `iceberg-rest` (Up)
- `airflow-webserver` (Up)
- `airflow-scheduler` (Up)

### STEP 2: Access All System UIs

| **Service** | **URL** | **What You'll See** |
|-------------|---------|-------------------|
| **Airflow** | http://localhost:4040 | Workflow orchestration (admin/admin) |
| **Spark Master** | http://localhost:8080 | Cluster overview with 1 worker |
| **Spark Jobs** | http://localhost:4041 | Job execution details (empty initially) |
| **MinIO Storage** | http://localhost:9001 | Data lake storage (admin/password) |
| **Kafka UI** | http://localhost:8081 | Kafka topics and messages |

### STEP 3: Create Supporting Data (Bronze Layer)

#### 3.1 Create Supporting Data
```bash
# Create restaurants, drivers, menu items, weather data
docker exec -it spark-iceberg python /home/iceberg/processing/create_supporting_data.py
```

**Expected output:**
- Created bronze_restaurants with 15 records
- Created bronze_drivers with 20 records  
- Created bronze_menu_items with 13 records
- Created bronze_weather with 30 records

#### 3.2 Generate Orders Dataset
```bash
# Generate 5,000 historical orders
docker exec -it spark-iceberg python /home/iceberg/processing/generate_5000_orders.py
```

**What happens:**
- Creates 5,000+ orders with sample data
- Generates order items, ratings, customers
- Stores in Bronze Iceberg tables
- Takes ~2 minutes

#### 3.3 Verify Bronze Data
```bash
# Check data was created
docker exec spark-iceberg python /home/iceberg/project/verify_orders.py
```

**Expected output:**
```
Bronze: 5,000+ orders
Silver: 0 orders (not processed yet)
Gold: 0 orders (not processed yet)
```

### STEP 4: Start Real-Time Streaming

#### 4.1 Start Orders Producer
```bash
# Terminal 1: Start orders streaming
docker exec -it spark-iceberg python /home/iceberg/project/streaming/orders_producer.py
```

> Fast order simulation:
> ```powershell
> docker exec spark-iceberg python -c "import sys; sys.path.append('/home/iceberg/project'); from streaming.orders_producer import OrdersProducer as O; p=O(); p.produce_orders(num_orders=3000, delay_seconds=0.05, late_arrival_probability=0.1); p.close()"
> ```
> Generates 3 000 orders in ~2½ min at ~20 orders/s then exits.

**What happens:**
- Kafka topics (`orders-topic`, `order-items-topic`) are created automatically
- Producer starts sending real-time and late-arriving orders
- Each order includes multiple items and realistic data

**What you'll see:**
```
[REAL-TIME] Sent order 1/10: order_abc123 with 3 items - $45.67
[LATE] Sent order 2/10: order_def456 with 2 items - $23.45
```

## STEP 5-8: Choose Your Processing Method

You can process streaming data **manually** (Option A) or let **Airflow** run the whole pipeline for you (Option B). Pick ONE of the two workflows below; do not mix them.

---

### OPTION A — Manual Processing

#### STEP 5: Start Kafka Stream Ingestion
```bash
# WARNING: This runs FOREVER until you press Ctrl+C
# Terminal 2: Ingest streaming data to Bronze (STREAMING ONLY)
docker exec -it spark-iceberg python /home/iceberg/processing/kafka_stream_ingestion.py

```

#### STEP 5 (b): Verify Streaming Ingestion
```bash
# Terminal 3: Check that streaming data is being ingested
docker exec spark-iceberg python /home/iceberg/project/verify_orders.py
```

**Expected output (after a few minutes of streaming):**
```
Bronze: 5,000+ orders (batch) + streaming orders
Silver: 0 orders (not processed yet)
Gold: 0 orders (not processed yet)
```

**Troubleshooting streaming issues:**
```bash
# Check Kafka topics exist and have messages
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check topic message counts
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders-topic --from-beginning --max-messages 5
```

#### STEP 6: Process Silver Layer
```bash
# Clean and validate data
docker exec -it spark-iceberg python /home/iceberg/processing/silver_processing.py
```

#### STEP 6 (b): Verify Silver Processing
```bash
# Check Silver layer was processed
docker exec spark-iceberg python /home/iceberg/project/verify_orders.py
```

**Expected output:**
```
Bronze: 5,000+ orders
Silver: 5,000+ orders (Processed)
Gold: 0 orders (not processed yet)
```

#### STEP 7: Process Gold Layer
```bash
# Create star schema for analytics
docker exec -it spark-iceberg python /home/iceberg/processing/gold_processing.py
```

#### STEP 7 (b): Verify Gold Processing
```bash
# Check Gold layer was processed
docker exec spark-iceberg python /home/iceberg/project/verify_orders.py
```

**Expected output:**
```
Bronze: 5,000+ orders
Silver: 5,000+ orders (Processed)
Gold: 5,000+ orders (Analytics ready)
```

#### STEP 8: Late Data Detection & Restaurant End-of-Day Reports

```bash
# Simulate restaurant end-of-day delivery confirmations
docker exec -it spark-iceberg python /home/iceberg/processing/late_data_detector.py --mode simulate
```

**How Late Data Detection Works:**
1. **Reads Bronze orders table** to find incomplete deliveries
2. **Identifies orders** with status `preparing`, `picked_up`, or `ready`  
3. **Simulates restaurant reports** updating them to `delivered`
4. **Triggers Silver/Gold reprocessing** to update analytics
5. **Maintains data consistency** across all layers

#### STEP 8 (b): Final Verification
```bash
# Final check of all layers
docker exec spark-iceberg python /home/iceberg/project/verify_orders.py
```

**Expected output:**
```
Bronze: 5,000+ orders (including streaming data)
Silver: 5,000+ orders (Processed with late data)
Gold: 5,000+ orders (Analytics ready with updates)
```

---

### OPTION B — Airflow Automation

#### 5.1 Access Airflow
1. Go to **http://localhost:4040**
2. Login: `admin` / `admin`
3. Find DAG: `woeat_etl_pipeline`

#### 5.2 Enable & Trigger DAG
```bash
# Click the toggle to ENABLE the DAG
# Click "Trigger DAG" to run immediately
# Or wait - it runs automatically every 15 minutes!
```

**What the Airflow DAG does automatically:**
- Bronze Streaming: Kafka → Bronze ingestion
- Silver Processing: Data cleaning & validation  
- Gold Processing: Star schema creation
- Late Data Detection: Automatic detection & reprocessing
- Data Quality: Monitor all layers
- Business Reports: Generate analytics

**Monitor Progress:**
- **Airflow UI**: http://localhost:4040 - See workflow progress
- **Spark Jobs UI**: http://localhost:4041 - See Spark job execution
- **Kafka UI**: http://localhost:8081 - See message consumption

**Late Data Detection:**
- **Simulate Mode**: Finds incomplete orders and marks them as delivered (restaurant end-of-day reports)
- **Detect Mode**: Looks for recent data changes and simulates late arrival detection
- **Monitor Mode**: Continuously monitors for late data every 15 minutes
- **Automatic Reprocessing**: Triggers Silver/Gold layer updates when late data is detected
- **Real-World Scenario**: Simulates restaurants sending delivery confirmations at end of day

### STEP 9: Create Analytics Dashboard

#### 9.1 Generate Dashboard
```bash
# Generate interactive dashboard
docker exec -it spark-iceberg python /home/iceberg/project/create_data_dashboard.py

# Copy dashboard to your computer and open it
docker cp spark-iceberg:/home/iceberg/project/woeat_dashboard.html .
start woeat_dashboard.html
```

## Clean Restart

```bash
# Stop all containers
docker-compose -f docker-compose.spark.yml down
docker-compose -f docker-compose.kafka.yml down
docker-compose -f docker-compose.airflow.yml down

# Remove data (optional)
docker volume prune -f

# Restart fresh
docker-compose -f docker-compose.spark.yml up -d --build
docker-compose -f docker-compose.kafka.yml up -d
docker-compose -f docker-compose.airflow.yml up -d
```

## Troubleshooting

### If Streaming Fails:
```bash
# Check Kafka topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check Spark logs
docker logs spark-iceberg --tail 20
```

### If Tables Missing:
```bash
# Check what tables exist
docker exec -it spark-iceberg python /home/iceberg/project/show_tables.py
```

### If Ports Conflict:
- Airflow: localhost:4040
- Spark Master: localhost:8080  
- Spark Jobs: localhost:4041
- Kafka UI: localhost:8081
- MinIO: localhost:9001 



