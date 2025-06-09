# ✅ Pre-Demo Checklist

## 🚀 **5 Minutes Before Demo**

### 1. **Verify Docker Environment**
```bash
# Check all containers are running
docker ps

# Should see: spark-iceberg, minio, kafka containers
```

### 2. **Copy Scripts to Container**
```bash
# Ensure all scripts are available
docker cp processing/bronze_simple.py spark-iceberg:/tmp/
docker cp processing/silver_processing.py spark-iceberg:/tmp/
docker cp processing/gold_processing.py spark-iceberg:/tmp/
docker cp processing/late_arriving_data.py spark-iceberg:/tmp/
```

### 3. **Quick Test Run** (Optional)
```bash
# Quick verification (1 minute)
docker exec spark-iceberg spark-submit /tmp/bronze_simple.py | tail -10
```

### 4. **Demo Commands Ready**
Copy these commands to a text file for easy access:

```bash
# Command 1: Bronze Layer
docker exec spark-iceberg spark-submit /tmp/bronze_simple.py

# Command 2: Silver Layer  
docker exec spark-iceberg spark-submit /tmp/silver_processing.py

# Command 3: Gold Layer
docker exec spark-iceberg spark-submit /tmp/gold_processing.py

# Command 4: Late Data Demo (MAIN FEATURE)
docker exec spark-iceberg spark-submit /tmp/late_arriving_data.py
```

## 🎯 **Key Demo Points to Remember**

1. **Start with the problem**: "Restaurant reports arrive late in real systems"
2. **Show the solution**: "Our system automatically handles this"
3. **Highlight the impact**: "10 late records, 2 affected dates, automatic reprocessing"
4. **Emphasize production readiness**: "Full monitoring and alerting"

## 📊 **Expected Timing**
- Bronze: ~30 seconds
- Silver: ~30 seconds  
- Gold: ~45 seconds
- Late Data: ~60 seconds (MAIN FEATURE)

## 🚨 **Backup Plan**
If live demo fails, show the documentation and explain the architecture using the README.md file.

**✅ You're ready to demonstrate a production-grade data engineering solution!** 