#!/usr/bin/env python3
"""
Kafka Stream Ingestion
ONLY handles real-time streaming from Kafka topics to Bronze tables.
"""

import sys
import os
sys.path.append('/home/iceberg/processing')

from spark_config import create_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import *
import signal
import time

# Global variables for graceful shutdown
streaming_queries = []

def signal_handler(sig, frame):
    """Handle graceful shutdown"""
    print("\nStopping Kafka stream ingestion...")
    for query in streaming_queries:
        if query.isActive:
            query.stop()
    print("All streaming queries stopped")
    exit(0)

signal.signal(signal.SIGINT, signal_handler)

def create_kafka_stream_ingestion():
    """Create streaming ingestion from Kafka to Bronze tables"""
    
    print("Starting Kafka stream ingestion...")
    
    # Get Spark session with streaming configuration
    spark = create_spark_session("Kafka-Stream-Ingestion")
    
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
        print("Connecting to Kafka orders topic...")
        orders_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "orders-topic") \
            .option("startingOffsets", "latest") \
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
        
        # Write orders to Bronze table
        print("Setting up orders streaming write...")
        orders_query = orders_parsed \
            .writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("path", "demo.bronze.bronze_orders") \
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
            raise Exception("Orders stream failed to start properly")
        
        print("✅ Orders stream is stable, starting order items stream...")
        
        # STEP 2: Start Order Items Stream Second
        print("Connecting to Kafka order-items topic...")
        order_items_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "order-items-topic") \
            .option("startingOffsets", "latest") \
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
        
        # Write order items to Bronze table
        print("Setting up order items streaming write...")
        order_items_query = order_items_parsed \
            .writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("path", "demo.bronze.bronze_order_items") \
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
        print("   🛑 Press Ctrl+C to stop")
        
        # Wait for termination
        for query in streaming_queries:
            query.awaitTermination()
            
    except Exception as e:
        print(f"❌ Streaming ingestion failed: {str(e)}")
        print("🔧 Stopping all active streams...")
        for query in streaming_queries:
            if query.isActive:
                query.stop()
        raise
    finally:
        print("🛑 Stopping streaming ingestion...")
        spark.stop()

if __name__ == "__main__":
    create_kafka_stream_ingestion() 