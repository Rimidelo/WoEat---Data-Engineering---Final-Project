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

# Start Kafka (optional for streaming)
docker-compose -f docker-compose.kafka.yml up -d

# Start Airflow (optional for orchestration)
docker-compose -f docker-compose.airflow.yml up -d
```

### 3. Access Services
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Spark UI**: http://localhost:4040
- **MinIO Console**: http://localhost:9001 (admin/password)
- **Kafka UI**: http://localhost:8081

### 4. Run the Complete Pipeline

#### Option A: Full Automated Pipeline
```bash
# Run complete pipeline (Bronze → Silver → Gold)
docker exec spark-iceberg python /home/iceberg/processing/run_full_pipeline.py
```

#### Option B: Step-by-Step Execution

**Step 1: Bronze Layer - Raw Data Ingestion**
```bash
# Ingest raw data from multiple sources
docker exec spark-iceberg python /home/iceberg/processing/bronze_ingestion.py
```

**Step 2: Generate Large Dataset**
```bash
# Generate 5000 orders with items and ratings
docker exec spark-iceberg python /home/iceberg/processing/generate_5000_orders.py
```

**Step 3: Silver Layer - Data Quality & Cleaning**
```bash
# Clean and validate data, create performance aggregations
docker exec spark-iceberg python /home/iceberg/processing/silver_processing.py
```

**Step 4: Gold Layer - Dimensional Modeling**
```bash
# Create star schema with SCD Type 2 dimensions
docker exec spark-iceberg python /home/iceberg/processing/gold_processing.py
```

### 5. Start Real-time Streaming (Optional)
```bash
# Terminal 1: Start orders and order items producer
cd streaming && python orders_producer.py

# Terminal 2: Start driver locations producer  
cd streaming && python driver_locations_producer.py
```

### 6. Query Your Data

**Check Table Structure**
```sql
-- Bronze tables
SHOW TABLES FROM demo.bronze;

-- Silver tables  
SHOW TABLES FROM demo.silver;

-- Gold tables (Star Schema)
SHOW TABLES FROM demo.gold;
```

**Sample Business Queries**
```sql
-- Restaurant Performance Analysis
SELECT 
    dr.restaurant_name,
    dr.cuisine_type,
    COUNT(fo.order_key) as total_orders,
    SUM(fo.total_amount) as total_revenue,
    AVG(fo.delivery_minutes) as avg_delivery_time,
    AVG(fr.food_rating) as avg_food_rating
FROM demo.gold.fact_orders fo
JOIN demo.gold.dim_restaurants dr ON fo.restaurant_key = dr.restaurant_key  
LEFT JOIN demo.gold.fact_ratings fr ON fo.order_key = fr.order_key
WHERE dr.is_current = true
GROUP BY dr.restaurant_id, dr.restaurant_name, dr.cuisine_type
ORDER BY total_revenue DESC;

-- Driver Performance Tracking
SELECT 
    dd.name as driver_name,
    COUNT(fo.order_key) as orders_delivered,
    AVG(fo.delivery_minutes) as avg_delivery_time,
    AVG(fr.driver_rating) as avg_driver_rating,
    SUM(fo.tip_amount) as total_tips
FROM demo.gold.fact_orders fo
JOIN demo.gold.dim_drivers dd ON fo.driver_key = dd.driver_key
LEFT JOIN demo.gold.fact_ratings fr ON fo.order_key = fr.order_key
WHERE dd.is_current = true AND fo.cancelled = false
GROUP BY dd.driver_id, dd.name
ORDER BY orders_delivered DESC;

-- Daily Business Summary
SELECT 
    dd.full_date,
    fbs.total_orders,
    fbs.total_revenue,
    fbs.avg_order_value,
    fbs.overall_satisfaction,
    fbs.avg_delivery_time
FROM demo.gold.fact_business_summary fbs
JOIN demo.gold.dim_date dd ON fbs.date_key = dd.date_key
ORDER BY dd.full_date DESC
LIMIT 30;
```

## Key Improvements Made

### 1. **Proper Data Normalization**
- Separated orders from order items
- Created dedicated ratings table
- Normalized restaurant and driver data

### 2. **Enhanced Data Quality**
- Removed aggregated calculations from Bronze layer
- Added comprehensive data validation in Silver
- Implemented proper data lineage tracking

### 3. **Advanced Analytics Support**
- Full star schema implementation
- SCD Type 2 for historical tracking
- Business summary tables for executive dashboards

### 4. **Real-world Data Engineering Practices**
- Proper medallion architecture
- Data quality monitoring
- Late-arriving data handling
- Scalable processing patterns

## Project Structure
```
/WoEat---Data-Engineering---Final-Project
├── /Tables/                # Table schema documentation
│   ├── bronze_tables.md    # Bronze layer table definitions
│   ├── silver_tables.md    # Silver layer table definitions
│   ├── gold_tables.md      # Gold layer table definitions
│   └── table_changes_explanation.md
├── /orchestration/         # Airflow DAGs and configurations
├── /streaming/            # Kafka producers and consumers
├── /processing/           # Spark applications
│   ├── bronze_ingestion.py     # Raw data ingestion
│   ├── silver_processing.py    # Data cleaning & quality
│   ├── gold_processing.py      # Dimensional modeling
│   ├── generate_5000_orders.py # Large dataset generation
│   └── run_full_pipeline.py    # Complete pipeline runner
├── /docs/                 # Documentation and diagrams
├── docker-compose.spark.yml    # Spark + Iceberg + MinIO
├── docker-compose.kafka.yml    # Kafka services
├── docker-compose.airflow.yml  # Airflow orchestration
└── README.md
```

## Installation Steps
```bash
# 1. Start core services
docker compose -f docker-compose.spark.yml up -d

# 2. Install dependencies (if running locally)
pip install pyspark

# 3. Run the pipeline
docker exec spark-iceberg python /home/iceberg/processing/run_full_pipeline.py
```

## Expected Results
After running the complete pipeline, you'll have:
- **5,000+ orders** with realistic distribution
- **10,000+ order items** with proper pricing
- **3,500+ ratings** across drivers, food, and delivery
- **Star schema** ready for BI tools
- **Historical tracking** for changing dimensions
- **Business metrics** for executive dashboards

🚀 **Your data lakehouse is now ready for production analytics!**


