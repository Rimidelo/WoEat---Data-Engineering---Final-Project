# 🎯 WoEat Data Engineering Final Project - Video Demo Guide

## 🎬 **Video Recording Setup (Before Recording)**

### 1. **Environment Check** (30 seconds)
```bash
# Ensure all services are running
docker ps
# Should see: spark-iceberg, minio, kafka, airflow containers
```

### 2. **Clean Start** (Optional)
```bash
# If you want a fresh demo, restart services
docker-compose -f docker-compose.spark.yml down
docker-compose -f docker-compose.kafka.yml down  
docker-compose -f docker-compose.airflow.yml down

# Then restart (wait 2 minutes between each)
docker-compose -f docker-compose.spark.yml up -d
docker-compose -f docker-compose.kafka.yml up -d  
docker-compose -f docker-compose.airflow.yml up -d
```

### 3. **Demo Command Ready** (Copy to clipboard)
```bash
python run_demo.py
```

---

## 🎥 **Video Script & Timing (Total: ~12 minutes)**

### **🎬 Opening - Project Introduction** (1 minute)
**📝 Script:**
> "Hi! I'm presenting WoEat - a complete data engineering solution for a food delivery platform. This project demonstrates modern data lakehouse architecture solving real-world challenges with late-arriving data."

**🔧 Technical Stack:**
> "I'm using Apache Spark for distributed processing, Apache Iceberg for data lakehouse, Kafka for streaming, and Docker for containerization. The architecture follows the medallion pattern: Bronze for raw data, Silver for cleaned data, and Gold for business analytics."

---

### **🎬 Main Demo - Automated Pipeline** (8 minutes)
**📝 Script Before Running:**
> "Now let me show you the complete system in action. I'll run one command that demonstrates our entire data pipeline processing 5,000 orders, plus our main feature - handling late-arriving restaurant data."

**💻 Run Command:**
```bash
python run_demo.py
```

**🎙️ Talking Points While Demo Runs:**

#### **During Bronze Layer Processing** (2 minutes):
> "First, we're ingesting raw data from multiple sources - restaurant menus, driver information, and weather data. Then we're generating 5,000 realistic food delivery orders with proper business distribution across a 30-day period."

**📊 Point out the output:**
- "Notice we're creating 5,000 orders, over 10,000 order items, and 3,500+ ratings"
- "The data spans multiple restaurants with realistic order patterns"

#### **During Silver Layer Processing** (1.5 minutes):
> "Now the Silver layer is applying data quality rules and business logic. We're calculating delivery times, validating data integrity, and creating performance aggregations. This is where we ensure data reliability for downstream analytics."

#### **During Gold Layer Processing** (1.5 minutes):
> "The Gold layer creates our star schema for analytics - fact tables for orders and ratings, dimension tables for restaurants and drivers. We're implementing slowly changing dimensions Type 2 to track historical changes."

#### **During Late-Arriving Data Demo** (2 minutes):
> "Here's our main feature - handling late-arriving data. In real food delivery platforms, restaurant performance reports often arrive 24-48 hours late. Watch how our system detects this, categorizes the lateness, and triggers reprocessing."

**📊 Point out the alerts:**
- "See the CRITICAL alerts for 3+ day late data"
- "WARNING alerts for 2-day late data"
- "Automatic reprocessing triggered for affected dates"

#### **During Dashboard Generation** (1 minute):
> "Finally, we're generating a professional business analytics dashboard that will automatically open in the browser, showing real business metrics from our 5,000+ orders."

---

### **🎬 Dashboard Walkthrough** (2 minutes)
**📝 Script When Dashboard Opens:**
> "Here's our professional analytics dashboard showing real business insights:"

**📊 Point out key sections:**
- "Order status distribution - 88% delivery success rate"
- "Peak hours analysis - busiest times for delivery"
- "Restaurant performance metrics with ratings"
- "Revenue analytics and customer insights"
- "All generated from our 5,000+ order dataset"

---

### **🎬 Technical Deep Dive** (1 minute)
**📝 Script:**
> "Let me highlight the technical achievements here:"

**🔧 Architecture Points:**
- "Complete containerized deployment with Docker"
- "Distributed processing handling production-scale data"
- "ACID transactions with Apache Iceberg"
- "Real-time monitoring and alerting for data quality"
- "Schema evolution without breaking existing data"

**💡 Business Value:**
- "Solves real-world late data challenges"
- "Production-ready with comprehensive monitoring"
- "Scalable architecture for enterprise deployment"

---

## 🚨 **Backup Plans**

### **If Demo Fails:**
```bash
# Quick restart
docker restart spark-iceberg
# Wait 30 seconds, then retry
python run_demo.py
```
