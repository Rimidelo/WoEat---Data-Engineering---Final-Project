# WoEat - Data Engineering Final Project

## 🎯 What This Project Does
Complete food delivery data pipeline that processes 5,000+ orders and generates a professional business analytics dashboard. Demonstrates modern data engineering with real-time processing, data quality management, and business intelligence.

## Complete Demo Guide

### **For Full Step-by-Step Instructions:**
📖 **See [PROJECT_GUIDE.md](PROJECT_GUIDE.md)** - The ultimate guide covering:

- 🏗️ **Infrastructure setup** with all services
- 🔄 **Real-time streaming** with Kafka producers  
- ⚡ **Spark job monitoring** with UI access
- 🌊 **Data pipeline flow** (Bronze → Silver → Gold)
- 📊 **Airflow orchestration** and DAG execution
- 📈 **Business analytics** dashboard generation
- 🎯 **Late-arriving data** handling demonstration

### Quick Start (3 Commands)
```bash
# 1. Start infrastructure (wait 2 minutes between each)
docker-compose -f docker-compose.spark.yml up -d --build
docker-compose -f docker-compose.kafka.yml up -d 
docker-compose -f docker-compose.airflow.yml up -d

# 2. Generate initial data
docker exec -it spark-iceberg python /home/iceberg/processing/generate_5000_orders.py

# 3. Create analytics dashboard  
docker exec -it spark-iceberg python /home/iceberg/project/create_data_dashboard.py
```

## 🌐 Access Points (After Demo Runs)
- **Analytics Dashboard**: `woeat_dashboard.html` (opens automatically)
- **Spark Jobs UI**: http://localhost:4041 (shows individual job execution)
- **Spark Master UI**: http://localhost:8080 (cluster overview)
- **Airflow Workflows**: http://localhost:4040 (admin/admin)
- **Data Storage UI**: http://localhost:9001 (admin/password)

## 🏗️ Technical Architecture (Overview)
- **Modern Stack**: Apache Spark, Kafka, Iceberg, Airflow, Docker
- **Data Layers**: Bronze (raw) → Silver (cleaned) → Gold (analytics)
- **Scale**: 5,000+ orders, 10,000+ items, 3,500+ ratings
- **Features**: Real-time streaming, data quality, late data handling

## 📋 What You'll See
1. **Complete Data Pipeline**: Automated processing of food delivery data
2. **Business Dashboard**: Professional analytics with charts and insights
3. **Late Data Handling**: Demonstration of real-world data engineering challenges
4. **Production Quality**: Enterprise-level data architecture and monitoring

---
**Total Demo Time**: ~8 minutes | **Result**: Professional business analytics dashboard


