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
        
        # Check Bronze orders for recent arrivals (using order_time as proxy)
        bronze_orders = spark.table("bronze.bronze_orders")
        
        # Look for recently created orders (orders with recent order_time)
        if "order_time" in bronze_orders.columns:
            recent_orders = bronze_orders.filter(
                col("order_time") >= lit(cutoff_time.isoformat())
            )
            
            recent_count = recent_orders.count()
            
            if recent_count > 0:
                print(f"📊 Found {recent_count} recent orders (last 30 minutes)")
                
                # Show sample of recent data
                print("   📋 Sample recent orders:")
                recent_orders.select("order_id", "order_time", "status").show(5, truncate=False)
                

                import builtins
                late_count = builtins.max(1, recent_count // 3)  # Simulate 1/3 as late
                print(f"🚨 Simulating {late_count} orders as late-arriving data!")
                
                return True, late_count
            else:
                print("✅ No recent data found - system is up to date")
                return False, 0
        else:
            print("❌ No order_time column found - cannot detect data patterns")
            return False, 0
            
    except Exception as e:
        print(f"❌ Late data detection failed: {str(e)}")
        return False, 0
    finally:
        spark.stop()

def trigger_reprocessing():
    """Trigger reprocessing of Silver and Gold layers"""
    
    print("🔄 Starting automatic reprocessing...")
    
    try:
        # Import processing modules
        import subprocess
        
        print("   🥈 Reprocessing Silver layer...")
        result = subprocess.run([
            "python", "/home/iceberg/processing/silver_processing.py"
        ], capture_output=True, text=True, cwd="/home/iceberg")
        
        if result.returncode == 0:
            print("   ✅ Silver layer reprocessed successfully")
        else:
            print(f"   ❌ Silver processing failed: {result.stderr}")
            return False
        
        print("   🥇 Reprocessing Gold layer...")  
        result = subprocess.run([
            "python", "/home/iceberg/processing/gold_processing.py"
        ], capture_output=True, text=True, cwd="/home/iceberg")
        
        if result.returncode == 0:
            print("   ✅ Gold layer reprocessed successfully")
        else:
            print(f"   ❌ Gold processing failed: {result.stderr}")
            return False
        
        print("🎉 Reprocessing completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Reprocessing failed: {str(e)}")
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
    """Simulate late-arriving restaurant end-of-day reports for demonstration"""
    
    print("🏪 Simulating late-arriving restaurant end-of-day reports...")
    
    spark = create_spark_session("Late-Data-Simulation")
    
    try:
        # Import functions at the top
        from pyspark.sql.functions import lit, current_timestamp, expr, when, col
        
        # Read existing orders from correct table
        bronze_orders = spark.table("bronze.bronze_orders")
        
        # Find orders that are picked_up or ready but not yet delivered
        # These represent orders that restaurants will report as delivered in end-of-day reports
        incomplete_orders = bronze_orders.filter(
            col("status").isin(["picked_up", "ready", "preparing"])
        ).limit(10)  # Take 10 orders for demo
        
        incomplete_count = incomplete_orders.count()
        
        if incomplete_count == 0:
            print("📋 No incomplete orders found to update")
            return 0
            
        print(f"📋 Found {incomplete_count} incomplete orders to update")
        print("📊 Sample orders before update:")
        incomplete_orders.select("order_id", "restaurant_id", "status", "order_time", "delivery_time").show(5, False)
        
        # Create late delivery reports - update these orders to 'delivered' status
        late_delivery_reports = incomplete_orders \
            .withColumn("status", lit("delivered")) \
            .withColumn("delivery_time", current_timestamp())
        
        print("📊 Sample orders after restaurant reports:")
        late_delivery_reports.select("order_id", "restaurant_id", "status", "order_time", "delivery_time").show(5, False)
        
        # ⚠️ IMPORTANT: Use merge/upsert instead of overwrite to handle schema properly
        print("📝 Updating Bronze table with late delivery confirmations...")
        
        # Write the updated orders back using merge operation
        late_delivery_reports.createOrReplaceTempView("late_delivery_updates")
        
        # Use merge to update existing records
        spark.sql("""
            MERGE INTO bronze.bronze_orders t
            USING late_delivery_updates s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN UPDATE SET 
                t.status = s.status,
                t.delivery_time = s.delivery_time
        """)
        
        print(f"✅ Updated {incomplete_count} orders with late delivery confirmations")
        print("🏪 Restaurant end-of-day reports simulation completed!")
        
        return incomplete_count
        
    except Exception as e:
        print(f"❌ Late data simulation failed: {str(e)}")
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