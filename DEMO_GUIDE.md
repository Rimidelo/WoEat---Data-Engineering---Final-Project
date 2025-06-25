# 🎯 WoEat Data Engineering Final Project - Video Demo Guide



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

## 🌐 **Interactive Web UIs for Demo** 

### **🔥 Essential UIs to Show During Presentation:**

#### **1. Spark UI - `http://localhost:8080`**
- **When to show**: During any processing phase (Bronze/Silver/Gold)
- **What to highlight**: 
  - Active jobs and stages in real-time
  - SQL queries being executed
  - Memory usage and performance metrics
  - "Look at these Spark jobs processing our 5,000 orders in real-time!"

#### **2. MinIO UI - `http://localhost:9001`** 
- **Login**: `admin` / `password`
- **When to show**: When explaining data architecture
- **What to highlight**:
  - `warehouse/` bucket with Bronze/Silver/Gold folders
  - Iceberg table metadata files
  - Parquet data files organized by partition
  - "Here's our data lakehouse with all the processed orders stored as Iceberg tables"

#### **3. Kafka UI - `http://localhost:8081`**
- **When to show**: During late-arriving data demonstration
- **What to highlight**:
  - Topics: `orders`, `driver-locations`, `late-data-alerts`
  - Message flow and consumer lag
  - Real-time streaming data
  - "See the streaming data flowing through Kafka topics in real-time"

#### **4. Airflow UI - `http://localhost:4040`**
- **Login**: `admin` / `admin`  
- **When to show**: When discussing automation
- **What to highlight**:
  - DAG visualization of the complete pipeline
  - Task dependencies and scheduling
  - Success/failure status
  - "This is how we orchestrate the entire pipeline automatically"

### **🎯 Pro Tips for UI Demo:**
- **Open all UIs in separate browser tabs before starting**
- **Use Alt+Tab to quickly switch between UIs and terminal**
- **Refresh UIs during processing to show real-time updates**
- **Point out specific metrics and numbers in each UI**

---



### **🎬 Opening - Project Introduction** (1 minute)
**📝 Script:**
> "Hi! I'm presenting WoEat - a complete data engineering solution for a food delivery platform. This project demonstrates modern data lakehouse architecture solving real-world challenges with late-arriving data."

**🔧 Technical Stack:**
> "I'm using Apache Spark for distributed processing, Apache Iceberg for data lakehouse, Kafka for streaming, and Docker for containerization. The architecture follows the medallion pattern: Bronze for raw data, Silver for cleaned data, and Gold for business analytics."

**🌐 UI Setup:**
> "Before we start, let me open the web interfaces so you can see the actual systems in action."
- Open: `http://localhost:8080` (Spark UI)
- Open: `http://localhost:9001` (MinIO UI - login: admin/password)  
- Open: `http://localhost:8081` (Kafka UI)
- Open: `http://localhost:4040` (Airflow UI - login: admin/admin)

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

**🌐 Switch to Spark UI (localhost:8080):**
> "Look at the Spark UI - you can see the actual jobs processing our data in real-time, the SQL queries being executed, and memory usage."

**🌐 Switch to MinIO UI (localhost:9001):**
> "Here in MinIO, you can see our data lakehouse structure - the Bronze tables being created with all our raw order data organized by date partitions."

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

**🌐 Switch to Kafka UI (localhost:8081):**
> "In the Kafka UI, you can see the streaming late-data alerts being published to topics in real-time. Notice the message flow and how our system processes these alerts immediately."

**🌐 Switch to Airflow UI (localhost:4040):**
> "And here in Airflow, you can see how our DAGs automatically trigger reprocessing workflows when late data is detected. This is full automation!"

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
