#!/usr/bin/env python3
"""
Late Data Detection & Reprocessing
Automatically detects late-arriving data and triggers reprocessing of Silver/Gold layers.
Can be run standalone or integrated with Airflow.
"""

import sys
import os
sys.path.append('/home/iceberg/processing')

from spark_config import create_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import time

def detect_late_data():
    """Detect late-arriving data in Bronze tables"""
    
    print("Starting late data detection...")
    
    spark = create_spark_session("Late-Data-Detection")
    
    try:
        # Check for new data in the last processing window
        current_time = datetime.now()
        lookback_minutes = 30  # Check last 30 minutes
        cutoff_time = current_time - timedelta(minutes=lookback_minutes)
        
        print(f"   Checking for data since: {cutoff_time}")
        
        # Check Bronze orders for late arrivals
        bronze_orders = spark.table("demo.bronze.bronze_orders")
        
        # Look for recently ingested records
        if "ingestion_timestamp" in bronze_orders.columns:
            late_orders = bronze_orders.filter(
                col("ingestion_timestamp") >= lit(cutoff_time.isoformat())
            )
            
            late_count = late_orders.count()
            
            if late_count > 0:
                print(f"🚨 Detected {late_count} late-arriving orders!")
                
                # Show sample of late data
                print("   📋 Sample late orders:")
                late_orders.select("order_id", "order_time", "ingestion_timestamp").show(5, truncate=False)
                
                return True, late_count
            else:
                print("No late data detected")
                return False, 0
        else:
            print("No ingestion_timestamp column found - cannot detect late data")
            return False, 0
            
    except Exception as e:
        print(f"Late data detection failed: {str(e)}")
        return False, 0
    finally:
        spark.stop()

def trigger_reprocessing():
    """Trigger reprocessing of Silver and Gold layers"""
    
    print("Starting automatic reprocessing...")
    
    try:
        # Import processing modules
        from silver_processing import process_silver_layer
        from gold_processing import process_gold_layer
        
        print("   🥈 Reprocessing Silver layer...")
        process_silver_layer()
        
        print("   🥇 Reprocessing Gold layer...")  
        process_gold_layer()
        
        print("Reprocessing completed successfully!")
        return True
        
    except Exception as e:
        print(f"Reprocessing failed: {str(e)}")
        return False

def continuous_monitoring(check_interval_minutes=15):
    """Continuously monitor for late data and trigger reprocessing"""
    
    print(f"Starting continuous late data monitoring (every {check_interval_minutes} minutes)...")
    
    while True:
        try:
            print(f"\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Checking for late data...")
            
            has_late_data, count = detect_late_data()
            
            if has_late_data:
                print(f"🚨 Found {count} late records - triggering reprocessing...")
                success = trigger_reprocessing()
                
                if success:
                    print("Late data reprocessing completed")
                else:
                    print("Late data reprocessing failed")
            
            print(f"😴 Sleeping for {check_interval_minutes} minutes...")
            time.sleep(check_interval_minutes * 60)
            
        except KeyboardInterrupt:
            print("\nStopping late data monitoring...")
            break
        except Exception as e:
            print(f"Monitoring error: {str(e)}")
            print(f"😴 Sleeping for {check_interval_minutes} minutes before retry...")
            time.sleep(check_interval_minutes * 60)

def simulate_late_data():
    """Simulate late-arriving restaurant reports for demonstration"""
    
    print("🎭 Simulating late-arriving restaurant reports...")
    
    spark = create_spark_session("Late-Data-Simulation")
    
    try:
        # Read existing orders
        bronze_orders = spark.table("demo.bronze.bronze_orders")
        
        # Select a few random orders to simulate as "late reports"
        sample_orders = bronze_orders.sample(0.05).limit(10)  # 5% sample, max 10 orders
        
        # Modify these orders to appear as late arrivals
        late_orders = sample_orders \
            .withColumn("status", lit("delivered")) \
            .withColumn("delivery_time", 
                       date_add(col("order_time"), 1).cast("string")) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("late_arrival_flag", lit(True))
        
        # Append to Bronze table
        late_orders.writeTo("demo.bronze.bronze_orders").append()
        
        count = late_orders.count()
        print(f"Simulated {count} late-arriving restaurant reports")
        
        return count
        
    except Exception as e:
        print(f"Late data simulation failed: {str(e)}")
        return 0
    finally:
        spark.stop()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Late Data Detection & Reprocessing")
    parser.add_argument("--mode", choices=["detect", "monitor", "simulate"], 
                       default="detect", help="Operation mode")
    parser.add_argument("--interval", type=int, default=15, 
                       help="Monitoring interval in minutes")
    
    args = parser.parse_args()
    
    if args.mode == "detect":
        has_late_data, count = detect_late_data()
        if has_late_data:
            print(f"🚨 Action needed: {count} late records detected")
            trigger_reprocessing()
    
    elif args.mode == "monitor":
        continuous_monitoring(args.interval)
    
    elif args.mode == "simulate":
        count = simulate_late_data()
        if count > 0:
            print("Triggering reprocessing for simulated late data...")
            trigger_reprocessing() 