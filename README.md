# WoEat - Data Engineering Final Project

## 🎯 What This Project Does
Complete food delivery data pipeline that processes 5,000+ orders and generates a professional business analytics dashboard. Demonstrates modern data engineering with real-time processing, data quality management, and business intelligence.

## 🚀 Quick Demo (3 Simple Steps)

### Step 1: Start the System
```bash
# Create required networks
docker network create woeat---data-engineering---final-project_iceberg_net
docker network create woeat---data-engineering---final-project_kafka_net

# Start all services (wait 2 minutes between each)
docker-compose -f docker-compose.spark.yml up -d
docker-compose -f docker-compose.kafka.yml up -d  
docker-compose -f docker-compose.airflow.yml up -d
```

### Step 2: Wait 2 Minutes
Let all services fully start.

### Step 3: Run the Complete Demo
```bash
python run_demo.py
```

**That's it!** This single command will:
- ✅ Process 5,000+ realistic food delivery orders
- ✅ Demonstrate late-arriving data handling (main technical feature)
- ✅ Generate and open a professional analytics dashboard
- ✅ Show complete Bronze → Silver → Gold data pipeline

The analytics dashboard will automatically open in your browser showing:
- 📊 Real-time business metrics and KPIs
- 📈 Interactive charts (order trends, restaurant performance)
- 💰 Revenue analysis and customer insights
- 🎯 Production-ready business intelligence

## 🌐 Access Points (After Demo Runs)
- **Analytics Dashboard**: `woeat_dashboard.html` (opens automatically)
- **Spark Processing UI**: http://localhost:8080
- **Airflow Workflows**: http://localhost:4040 (admin/admin)
- **Data Storage UI**: http://localhost:9001 (admin/password)

## 🏗️ Technical Architecture (Overview)
- **Modern Stack**: Apache Spark, Kafka, Iceberg, Airflow, Docker
- **Data Layers**: Bronze (raw) → Silver (cleaned) → Gold (analytics)
- **Scale**: 5,000+ orders, 10,000+ items, 3,500+ ratings
- **Features**: Real-time streaming, data quality, late data handling

## 📋 What You'll See
1. **Complete Data Pipeline**: Automated processing of realistic food delivery data
2. **Business Dashboard**: Professional analytics with charts and insights
3. **Late Data Handling**: Demonstration of real-world data engineering challenges
4. **Production Quality**: Enterprise-level data architecture and monitoring

---
**Total Demo Time**: ~8 minutes | **Result**: Professional business analytics dashboard


