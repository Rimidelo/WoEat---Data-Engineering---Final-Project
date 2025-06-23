# WoEat - Data Engineering Final Project

## Overview
Complete end-to-end data engineering solution for a food delivery platform, implementing a modern data pipeline with bronze/silver/gold architecture using Apache Iceberg, Kafka streaming, Spark processing, and Airflow orchestration.

## Key Features
- **Modern Architecture**: Bronze/Silver/Gold medallion architecture
- **Real-time Processing**: Kafka streaming with late-arriving data handling
- **Data Quality**: Comprehensive validation and monitoring
- **Historical Tracking**: SCD Type 2 implementation for dimension tables
- **Business Intelligence**: Star schema optimized for analytics
- **Scalable Storage**: Apache Iceberg with time travel capabilities

## Architecture
- **Storage**: Apache Iceberg tables on MinIO (S3-compatible)
- **Processing**: Apache Spark for batch and streaming
- **Streaming**: Apache Kafka for real-time data
- **Orchestration**: Apache Airflow for pipeline scheduling
- **Data Layers**: Bronze (raw) → Silver (cleaned) → Gold (analytics-ready)

## Updated Data Model

### Bronze Layer (Raw Data)
- `bronze_orders` - Order header information
- `bronze_order_items` - Individual items within orders
- `bronze_ratings` - Customer ratings for orders, drivers, and food
- `bronze_drivers` - Driver master data
- `bronze_restaurants` - Restaurant master data  
- `bronze_menu_items` - Menu item catalog
- `bronze_weather` - Weather data by zone

### Silver Layer (Cleaned & Enriched)
- `silver_orders` - Validated orders with calculated fields
- `silver_order_items` - Clean order items with extended pricing
- `silver_ratings` - Validated ratings data
- `silver_drivers` - Clean driver information
- `silver_restaurants` - Clean restaurant information
- `silver_menu_items` - Validated menu items
- `silver_restaurant_performance` - Daily restaurant metrics
- `silver_driver_performance` - Daily driver metrics
- `silver_weather` - Clean weather data

### Gold Layer (Analytics-Ready Star Schema)
#### Fact Tables
- `fact_orders` - Order transactions with business metrics
- `fact_order_items` - Item-level sales facts
- `fact_ratings` - Rating events and scores
- `fact_restaurant_daily` - Daily restaurant performance
- `fact_driver_daily` - Daily driver performance
- `fact_business_summary` - Overall business metrics

#### Dimension Tables
- `dim_drivers` - Driver dimension with SCD Type 2
- `dim_restaurants` - Restaurant dimension with SCD Type 2
- `dim_menu_items` - Menu item dimension
- `dim_date` - Date dimension for time-based analysis

## 🚀 Quick Start Guide (Follow These Steps Exactly)

### Step 1: Clone and Setup
```bash
git clone https://github.com/Rimidelo/WoEat---Data-Engineering---Final-Project
cd WoEat---Data-Engineering---Final-Project
```

### Step 2: Create Required Docker Networks
```bash
docker network create woeat---data-engineering---final-project_iceberg_net
docker network create woeat---data-engineering---final-project_kafka_net
```

### Step 3: Start All Services (Wait for each to complete)
```bash
# Start Spark + Iceberg + MinIO (wait for this to finish)
docker-compose -f docker-compose.spark.yml up -d

# Start Kafka services (wait for this to finish)
docker-compose -f docker-compose.kafka.yml up -d

# Start Airflow (wait for this to finish)
docker-compose -f docker-compose.airflow.yml up -d
```

### Step 4: Wait 2 Minutes
Wait for all services to fully start before proceeding.

### Step 5: Run the Complete Data Pipeline (One Command)
```bash
docker exec spark-iceberg python /home/iceberg/processing/run_full_pipeline.py
```

This single command will:
- Create all required tables
- Ingest sample data (Bronze layer)
- Generate 5000 orders with items and ratings
- Clean and process data (Silver layer)
- Create analytics-ready tables (Gold layer)

### Step 6: Access Your Data and UIs
- **Spark UI**: http://localhost:8080 (Monitor data processing jobs and performance)
- **Airflow UI**: http://localhost:4040 (admin/admin - Workflow orchestration and scheduling)
- **MinIO Console**: http://localhost:9001 (admin/password - View data files and storage)
- **Kafka UI**: http://localhost:8081 (Monitor streaming topics and messages)
- **Schema Registry**: http://localhost:8082 (Kafka schema management)

### Step 7: View Your Data
```bash
# Show all tables created
docker exec spark-iceberg python /home/iceberg/project/show_tables.py

# Show sample orders data
docker exec spark-iceberg python /home/iceberg/project/show_all_orders.py

# Verify data integrity
docker exec spark-iceberg python /home/iceberg/project/verify_orders.py
```


