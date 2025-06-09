# WoEat - Data Engineering Final Project

## Overview
Complete end-to-end data engineering solution for a food delivery platform, implementing a modern data pipeline with bronze/silver/gold architecture using Apache Iceberg, Kafka streaming, Spark processing, and Airflow orchestration.

## Architecture
- **Storage**: Apache Iceberg tables on MinIO (S3-compatible)
- **Processing**: Apache Spark for batch and streaming
- **Streaming**: Apache Kafka for real-time data
- **Orchestration**: Apache Airflow for pipeline scheduling
- **Data Layers**: Bronze (raw) → Silver (cleaned) → Gold (aggregated)

## Quick Start

### Prerequisites
- Docker Desktop installed and running
- At least 8GB RAM available for containers
- Ports 8080, 8181, 8888, 9000, 9001, 9092, 8081 available

### 1. Clone and Setup
```bash
git clone <your-repo-url>
cd WoEat---Data-Engineering---Final-Project
```

### 2. Start All Services
```bash
# Start Spark + Iceberg + MinIO
docker-compose -f docker-compose.spark.yml up -d

# Start Kafka
docker-compose -f docker-compose.kafka.yml up -d

# Start Airflow
docker-compose -f docker-compose.airflow.yml up -d
```

### 3. Access Services
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Spark UI**: http://localhost:4040
- **MinIO Console**: http://localhost:9001 (admin/password)
- **Kafka UI**: http://localhost:8081

### 4. Run the Complete Pipeline

#### Step 1: Create Bronze Layer (Raw Data)
```bash
# Create all Bronze tables with sample data
docker exec spark-iceberg python /home/iceberg/processing/bronze_simple.py
```

#### Step 2: Process Silver Layer (Cleaned Data)
```bash
# Clean and validate data
docker exec spark-iceberg python /home/iceberg/processing/silver_processing.py
```

#### Step 3: Process Gold Layer (Business Ready)
```bash
# Create dimension tables with SCD Type 2 and fact tables
docker exec spark-iceberg python /home/iceberg/processing/gold_processing.py
```

#### Step 4: Start Streaming Data (Optional)
```bash
# Terminal 1: Start orders producer
cd streaming && python orders_producer.py

# Terminal 2: Start driver locations producer  
cd streaming && python driver_locations_producer.py
```

### 5. Query Your Data
```sql
-- Check Bronze tables
SHOW TABLES FROM demo.bronze;

-- Check Silver tables  
SHOW TABLES FROM demo.silver;

-- Check Gold tables (Star Schema)
SHOW TABLES FROM demo.gold;

-- Sample business query
SELECT 
    dr.restaurant_name,
    COUNT(*) as total_orders,
    SUM(fo.total_amount) as total_revenue,
    AVG(fo.delivery_minutes) as avg_delivery_time
FROM demo.gold.fact_orders fo
JOIN demo.gold.dim_restaurants dr ON fo.restaurant_key = dr.restaurant_key  
WHERE dr.is_current = true
GROUP BY dr.restaurant_id, dr.restaurant_name
ORDER BY total_revenue DESC;
```

## Project Structure
```
/WoEat---Data-Engineering---Final-Project
├── /orchestration/          # Airflow DAGs and configurations
├── /streaming/             # Kafka producers and consumers
├── /processing/            # Spark applications
├── /docs/                  # Documentation and diagrams
├── docker-compose.spark.yml    # Spark + Iceberg + MinIO
├── docker-compose.kafka.yml    # Kafka services
├── docker-compose.airflow.yml  # Airflow orchestration
└── README.md
```

## Data Model
[Data model diagrams will be added here]

## Demo Instructions
[Step-by-step demo instructions will be added here]

steps for installtion:
docker compose -f docker-compose.spark.yml up -d
pip install pyspark


