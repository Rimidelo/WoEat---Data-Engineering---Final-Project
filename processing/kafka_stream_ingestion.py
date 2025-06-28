#!/usr/bin/env python3
"""
Kafka Stream Ingestion
ONLY handles real-time streaming from Kafka topics to Bronze tables.
Stops automatically when no new data arrives for a specified timeout.
"""

import sys
import os
sys.path.append('/home/iceberg/processing')

from spark_config import create_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import *
import signal
import time
import threading

# Global variables for graceful shutdown
streaming_queries = []
last_data_time = time.time()
timeout_seconds = 60  # Stop if no data for 60 seconds
max_runtime_seconds = 300  # Maximum 5 minutes runtime (prevent infinite runs)
shutdown_event = threading.Event()
start_time = time.time()

def signal_handler(sig, frame):
    """Handle graceful shutdown"""
    print("\nStopping Kafka stream ingestion...")
    shutdown_event.set()
    for query in streaming_queries:
        if query.isActive:
            query.stop()
    print("All streaming queries stopped")
    exit(0)

signal.signal(signal.SIGINT, signal_handler)

class DataTimeoutMonitor:
    """Monitor for data timeout and trigger shutdown"""
    
    def __init__(self, timeout_seconds, max_runtime_seconds):
        self.timeout_seconds = timeout_seconds
        self.max_runtime_seconds = max_runtime_seconds
        self.last_data_time = time.time()
        self.start_time = time.time()
        self.monitoring = True
        self.total_records_processed = 0
        
    def update_data_time(self, record_count=0):
        """Update the last data received time"""
        self.last_data_time = time.time()
        self.total_records_processed += record_count
        
    def start_monitoring(self):
        """Start monitoring in a separate thread"""
        def monitor():
            while self.monitoring and not shutdown_event.is_set():
                time.sleep(10)  # Check every 10 seconds
                
                current_time = time.time()
                runtime = current_time - self.start_time
                time_since_data = current_time - self.last_data_time
                
                # Check maximum runtime limit (prevent infinite runs)
                if runtime > self.max_runtime_seconds:
                    print(f"\n⏰ Maximum runtime reached ({self.max_runtime_seconds}s)")
                    print(f"📊 Processed {self.total_records_processed} total records")
                    print("🏁 Stopping to prevent infinite execution (continuous producer detected)")
                    shutdown_event.set()
                    
                    # Stop all streaming queries
                    for query in streaming_queries:
                        if query.isActive:
                            print(f"Stopping query: {query.name}")
                            query.stop()
                    break
                
                # Check data timeout (no new data)
                elif time_since_data > self.timeout_seconds:
                    print(f"\n⏰ No new data received for {self.timeout_seconds} seconds")
                    print(f"📊 Processed {self.total_records_processed} total records")
                    print("🏁 Kafka topics appear to be empty - stopping ingestion")
                    shutdown_event.set()
                    
                    # Stop all streaming queries
                    for query in streaming_queries:
                        if query.isActive:
                            print(f"Stopping query: {query.name}")
                            query.stop()
                    break
                    
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()
        
    def stop_monitoring(self):
        """Stop monitoring"""
        self.monitoring = False

def ensure_bronze_tables_exist(spark):
    """Create Bronze tables if they don't exist"""
    print("🗃️ Ensuring Bronze tables exist...")
    
    try:
        # Create namespace if it doesn't exist
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")
            print("✅ Created namespace: bronze")
        except Exception as e:
            print(f"ℹ️ Namespaces may already exist: {str(e)}")
        
        # Define Bronze Orders table schema
        orders_schema = """
        CREATE TABLE IF NOT EXISTS bronze.bronze_orders (
            order_id STRING,
            customer_id STRING,
            restaurant_id STRING,
            driver_id STRING,
            order_time TIMESTAMP,
            status STRING,
            delivery_time TIMESTAMP,
            total_amount DOUBLE,
            prep_start_time TIMESTAMP,
            prep_end_time TIMESTAMP,
            tip_amount DOUBLE
        ) USING ICEBERG
        """
        
        # Define Bronze Order Items table schema
        order_items_schema = """
        CREATE TABLE IF NOT EXISTS bronze.bronze_order_items (
            order_item_id STRING,
            order_id STRING,
            item_id STRING,
            quantity INT,
            item_price DOUBLE,
            order_time TIMESTAMP
        ) USING ICEBERG
        """
        
        # Create tables
        spark.sql(orders_schema)
        print("✅ Bronze orders table ready")
        
        spark.sql(order_items_schema)
        print("✅ Bronze order items table ready")
        
    except Exception as e:
        print(f"⚠️ Error creating tables (may already exist): {str(e)}")

def create_kafka_stream_ingestion():
    """Create streaming ingestion from Kafka to Bronze tables"""
    
    print("🚀 Starting Kafka stream ingestion with auto-stop...")
    print(f"⏰ Will stop if no data for {timeout_seconds} seconds")
    print(f"🔒 Maximum runtime: {max_runtime_seconds} seconds (prevents infinite runs)")
    print("📍 Using 'earliest' offsets (processes unread data, not everything)")
    
    # Initialize timeout monitor
    timeout_monitor = DataTimeoutMonitor(timeout_seconds, max_runtime_seconds)
    timeout_monitor.start_monitoring()
    
    # Get Spark session with streaming configuration
    spark = create_spark_session("Kafka-Stream-Ingestion")
    
    # Ensure Bronze tables exist
    ensure_bronze_tables_exist(spark)
    
    # Define Kafka message schemas (what producer sends)
    kafka_orders_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("restaurant_id", StringType(), False),
        StructField("driver_id", StringType(), False),
        StructField("order_timestamp", StringType(), False),
        StructField("order_status", StringType(), False),
        StructField("delivery_timestamp", StringType(), True),
        StructField("total_amount", StringType(), False),
        StructField("prep_start_timestamp", StringType(), True),
        StructField("prep_end_timestamp", StringType(), True),
        StructField("tip_amount", StringType(), True)
    ])
    
    kafka_order_items_schema = StructType([
        StructField("order_item_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("item_id", StringType(), False),
        StructField("quantity", StringType(), False),
        StructField("item_price", StringType(), False),
        StructField("order_time", StringType(), False)
    ])
    
    try:
        # STEP 1: Start Orders Stream First
        print("📡 Connecting to Kafka orders topic...")
        orders_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "orders-topic") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON and transform to match Bronze table schema
        orders_parsed = orders_stream \
            .select(
                from_json(col("value").cast("string"), kafka_orders_schema).alias("order")
            ) \
            .select("order.*") \
            .select(
                col("order_id"),
                col("customer_id"),
                col("restaurant_id"),
                col("driver_id"),
                col("order_timestamp").cast("timestamp").alias("order_time"),
                col("order_status").alias("status"),
                col("delivery_timestamp").cast("timestamp").alias("delivery_time"),
                col("total_amount").cast("double"),
                col("prep_start_timestamp").cast("timestamp").alias("prep_start_time"),
                col("prep_end_timestamp").cast("timestamp").alias("prep_end_time"),
                col("tip_amount").cast("double")
            )
        
        # Custom streaming writer with data monitoring
        def write_orders_batch(batch_df, batch_id):
            record_count = batch_df.count()
            if record_count > 0:
                print(f"📦 Processing orders batch {batch_id} with {record_count} records")
                timeout_monitor.update_data_time(record_count)  # Update data time with count
                batch_df.writeTo("bronze.bronze_orders").append()
            else:
                print(f"📦 Orders batch {batch_id} - no new data")
        
        # Write orders to Bronze table with monitoring
        print("⚙️ Setting up orders streaming write...")
        orders_query = orders_parsed \
            .writeStream \
            .foreachBatch(write_orders_batch) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/checkpoints/orders") \
            .trigger(processingTime="10 seconds") \
            .queryName("Bronze-Orders-Stream") \
            .start()
        
        streaming_queries.append(orders_query)
        print("✅ Orders streaming query started")
        
        # WAIT FOR FIRST STREAM TO STABILIZE
        print("⏳ Waiting 15 seconds for orders stream to stabilize...")
        time.sleep(15)
        
        # Check if first stream is still active
        if not orders_query.isActive:
            print("ℹ️ Orders stream completed - likely no orders data available")
        else:
            print("✅ Orders stream is stable, starting order items stream...")
        
        # STEP 2: Start Order Items Stream Second
        print("📡 Connecting to Kafka order-items topic...")
        order_items_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "order-items-topic") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON and transform to match Bronze table schema
        order_items_parsed = order_items_stream \
            .select(
                from_json(col("value").cast("string"), kafka_order_items_schema).alias("order_item")
            ) \
            .select("order_item.*") \
            .select(
                col("order_item_id"),
                col("order_id"),
                col("item_id"),
                col("quantity").cast("int"),
                col("item_price").cast("double"),
                col("order_time").cast("timestamp")
            )
        
        # Custom streaming writer with data monitoring
        def write_order_items_batch(batch_df, batch_id):
            record_count = batch_df.count()
            if record_count > 0:
                print(f"📋 Processing order items batch {batch_id} with {record_count} records")
                timeout_monitor.update_data_time(record_count)  # Update data time with count
                batch_df.writeTo("bronze.bronze_order_items").append()
            else:
                print(f"📋 Order items batch {batch_id} - no new data")
        
        # Write order items to Bronze table with monitoring
        print("⚙️ Setting up order items streaming write...")
        order_items_query = order_items_parsed \
            .writeStream \
            .foreachBatch(write_order_items_batch) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/checkpoints/order_items") \
            .trigger(processingTime="10 seconds") \
            .queryName("Bronze-OrderItems-Stream") \
            .start()
        
        streaming_queries.append(order_items_query)
        print("✅ Order items streaming query started")
        
        print("\n🚀 Kafka streaming ingestion is running...")
        print("   📦 Orders: orders-topic -> bronze_orders")
        print("   📋 Order Items: order-items-topic -> bronze_order_items")
        print("   ⏱️ Processing every 10 seconds")
        print("   📍 First run: processes all unread data")
        print("   📍 Next runs: processes only new data since last checkpoint")
        print(f"   ⏰ Auto-stop after {timeout_seconds}s of no data")
        print(f"   🔒 Max runtime: {max_runtime_seconds}s (prevents infinite runs)")
        print("   🛑 Press Ctrl+C to stop manually")
        
        # Wait for termination or timeout
        while not shutdown_event.is_set():
            time.sleep(1)
            
            # Check if any query has failed (distinguish between failure and completion)
            for query in streaming_queries:
                if not query.isActive:
                    # Check if it's due to timeout/completion or actual error
                    if shutdown_event.is_set():
                        print(f"ℹ️ Query {query.name} stopped due to timeout/completion")
                    else:
                        # Check if there was an exception
                        try:
                            exception = query.exception()
                            if exception:
                                print(f"❌ Query {query.name} failed with error: {exception}")
                                shutdown_event.set()
                                break
                            else:
                                print(f"ℹ️ Query {query.name} completed - no more data to process")
                        except:
                            print(f"ℹ️ Query {query.name} completed - no more data to process")
        
        print("🏁 Stopping all streaming queries...")
        for query in streaming_queries:
            if query.isActive:
                query.stop()
                
        timeout_monitor.stop_monitoring()
        
        # Final summary
        runtime = time.time() - start_time
        print(f"✅ Kafka stream ingestion completed successfully")
        print(f"📊 Total runtime: {runtime:.1f} seconds")
        print(f"📈 Total records processed: {timeout_monitor.total_records_processed}")
            
    except Exception as e:
        print(f"❌ Streaming ingestion failed: {str(e)}")
        print("🔧 Stopping all active streams...")
        for query in streaming_queries:
            if query.isActive:
                query.stop()
        timeout_monitor.stop_monitoring()
        raise
    finally:
        print("🛑 Cleaning up streaming ingestion...")
        spark.stop()

if __name__ == "__main__":
    create_kafka_stream_ingestion() 