from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

class BronzeIngestion:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("WoEat - Bronze Layer Ingestion")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config("spark.sql.catalog.demo.s3.access-key-id", "admin")
            .config("spark.sql.catalog.demo.s3.secret-access-key", "password")
            .getOrCreate()
        )
        
    def ingest_orders_stream(self):
        """Ingest real-time orders from Kafka to Bronze layer"""
        print("🔄 Starting orders stream ingestion...")
        
        # Read from Kafka
        orders_stream = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:29092")
            .option("subscribe", "orders-topic")
            .option("startingOffsets", "latest")
            .load()
        )
        
        # Parse JSON and extract order data
        orders_parsed = (
            orders_stream
            .select(
                col("key").cast("string").alias("order_id"),
                from_json(col("value").cast("string"), self._get_orders_schema()).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            )
            .select(
                col("data.order_id"),
                col("data.customer_id"),
                col("data.restaurant_id"),
                col("data.driver_id"),
                col("data.items"),
                to_timestamp(col("data.order_timestamp")).alias("order_time"),
                col("data.order_status").alias("status"),
                when(col("data.delivery_timestamp").isNotNull(), 
                     to_timestamp(col("data.delivery_timestamp"))).alias("delivery_time"),
                current_timestamp().alias("ingest_timestamp"),
                col("kafka_timestamp")
            )
        )
        
        # Write to Bronze Iceberg table
        query = (
            orders_parsed
            .writeStream
            .format("iceberg")
            .outputMode("append")
            .option("path", "demo.bronze.bronze_orders")
            .option("checkpointLocation", "/tmp/checkpoints/bronze_orders")
            .trigger(processingTime="30 seconds")
            .start()
        )
        
        print("✅ Orders stream ingestion started")
        return query
    
    def ingest_driver_locations_stream(self):
        """Ingest real-time driver locations from Kafka"""
        print("🔄 Starting driver locations stream ingestion...")
        
        # Read from Kafka
        locations_stream = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:29092")
            .option("subscribe", "driver-locations-topic")
            .option("startingOffsets", "latest")
            .load()
        )
        
        # Parse JSON and extract location data
        locations_parsed = (
            locations_stream
            .select(
                col("key").cast("string").alias("driver_id"),
                from_json(col("value").cast("string"), self._get_locations_schema()).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            )
            .select(
                col("data.location_id"),
                col("data.driver_id"),
                to_timestamp(col("data.timestamp")).alias("location_timestamp"),
                col("data.latitude"),
                col("data.longitude"),
                col("data.status"),
                col("data.speed"),
                current_timestamp().alias("ingest_timestamp")
            )
        )
        
        # Write to Bronze Iceberg table
        query = (
            locations_parsed
            .writeStream
            .format("iceberg")
            .outputMode("append")
            .option("path", "demo.bronze.bronze_driver_locations")
            .option("checkpointLocation", "/tmp/checkpoints/bronze_driver_locations")
            .trigger(processingTime="10 seconds")
            .start()
        )
        
        print("✅ Driver locations stream ingestion started")
        return query
    
    def ingest_batch_data(self):
        """Ingest batch data sources to Bronze layer"""
        print("🔄 Starting batch data ingestion...")
        
        # 1. Ingest Menu Items (batch)
        self._ingest_menu_items()
        
        # 2. Ingest Drivers (batch)
        self._ingest_drivers()
        
        # 3. Ingest Restaurant Performance (late-arriving batch)
        self._ingest_restaurant_performance()
        
        # 4. Ingest Weather Data (external API simulation)
        self._ingest_weather_data()
        
        print("✅ Batch data ingestion completed")
    
    def _ingest_menu_items(self):
        """Ingest menu items from batch source"""
        print("📋 Ingesting menu items...")
        
        # Simulate menu items data
        menu_data = [
            ("item_001", "rest_001", "Margherita Pizza", "Pizza", 12.99),
            ("item_002", "rest_001", "Pepperoni Pizza", "Pizza", 14.99),
            ("item_003", "rest_001", "Caesar Salad", "Salad", 8.99),
            ("item_004", "rest_002", "Classic Burger", "Burger", 9.99),
            ("item_005", "rest_002", "Chicken Sandwich", "Sandwich", 11.99),
            ("item_006", "rest_002", "French Fries", "Sides", 4.99),
            ("item_007", "rest_003", "California Roll", "Sushi", 8.99),
            ("item_008", "rest_003", "Salmon Sashimi", "Sashimi", 15.99),
            ("item_009", "rest_003", "Miso Soup", "Soup", 3.99),
            ("item_010", "rest_004", "Beef Tacos", "Tacos", 7.99),
            ("item_011", "rest_004", "Chicken Quesadilla", "Quesadilla", 9.99),
            ("item_012", "rest_005", "Chicken Curry", "Curry", 13.99),
            ("item_013", "rest_005", "Naan Bread", "Bread", 2.99)
        ]
        
        schema = StructType([
            StructField("item_id", StringType(), False),
            StructField("restaurant_id", StringType(), False),
            StructField("item_name", StringType(), False),
            StructField("category", StringType(), False),
            StructField("base_price", FloatType(), False)
        ])
        
        menu_df = (
            self.spark.createDataFrame(menu_data, schema)
            .withColumn("ingest_timestamp", current_timestamp())
        )
        
        # Write to Bronze Iceberg table
        menu_df.writeTo("demo.bronze.bronze_menu_items").createOrReplace()
        print(f"✅ Ingested {menu_df.count()} menu items")
    
    def _ingest_drivers(self):
        """Ingest drivers from batch source"""
        print("🚗 Ingesting drivers...")
        
        # Simulate drivers data
        drivers_data = [
            ("driver_001", "Alice Johnson", 4.8, "North"),
            ("driver_002", "Bob Smith", 4.5, "South"),
            ("driver_003", "Carol Davis", 4.9, "East"),
            ("driver_004", "David Wilson", 4.2, "West"),
            ("driver_005", "Eva Brown", 4.7, "North"),
            ("driver_006", "Frank Miller", 4.6, "South"),
            ("driver_007", "Grace Lee", 4.8, "East"),
            ("driver_008", "Henry Taylor", 4.3, "West"),
            ("driver_009", "Ivy Chen", 4.9, "North"),
            ("driver_010", "Jack Anderson", 4.4, "South"),
            ("driver_011", "Kate Thompson", 4.7, "East"),
            ("driver_012", "Leo Garcia", 4.5, "West"),
            ("driver_013", "Mia Rodriguez", 4.8, "North"),
            ("driver_014", "Noah Martinez", 4.6, "South"),
            ("driver_015", "Olivia White", 4.9, "East"),
            ("driver_016", "Paul Harris", 4.3, "West"),
            ("driver_017", "Quinn Clark", 4.7, "North"),
            ("driver_018", "Ruby Lewis", 4.5, "South"),
            ("driver_019", "Sam Walker", 4.8, "East"),
            ("driver_020", "Tina Hall", 4.4, "West")
        ]
        
        schema = StructType([
            StructField("driver_id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("rating", FloatType(), False),
            StructField("zone", StringType(), False)
        ])
        
        drivers_df = (
            self.spark.createDataFrame(drivers_data, schema)
            .withColumn("ingest_timestamp", current_timestamp())
        )
        
        # Write to Bronze Iceberg table
        drivers_df.writeTo("demo.bronze.bronze_drivers").createOrReplace()
        print(f"✅ Ingested {drivers_df.count()} drivers")
    
    def _ingest_restaurant_performance(self):
        """Ingest restaurant performance reports (late-arriving data)"""
        print("🏪 Ingesting restaurant performance...")
        
        # Simulate restaurant performance data (daily reports)
        from datetime import datetime, timedelta
        import random
        
        restaurants = ["rest_001", "rest_002", "rest_003", "rest_004", "rest_005"]
        performance_data = []
        
        # Generate data for last 7 days
        for i in range(7):
            report_date = (datetime.now() - timedelta(days=i)).date()
            for restaurant_id in restaurants:
                performance_data.append((
                    report_date,
                    restaurant_id,
                    round(random.uniform(15, 45), 1),  # avg_prep_time
                    round(random.uniform(3.5, 5.0), 1),  # avg_rating
                    random.randint(20, 100),  # orders_count
                    round(random.uniform(0.02, 0.15), 3),  # cancel_rate
                    round(random.uniform(2.0, 8.0), 2)  # avg_tip
                ))
        
        schema = StructType([
            StructField("report_date", DateType(), False),
            StructField("restaurant_id", StringType(), False),
            StructField("avg_prep_time", FloatType(), False),
            StructField("avg_rating", FloatType(), False),
            StructField("orders_count", IntegerType(), False),
            StructField("cancel_rate", FloatType(), False),
            StructField("avg_tip", FloatType(), False)
        ])
        
        performance_df = (
            self.spark.createDataFrame(performance_data, schema)
            .withColumn("ingest_timestamp", current_timestamp())
        )
        
        # Write to Bronze Iceberg table
        performance_df.writeTo("demo.bronze.bronze_restaurant_performance").createOrReplace()
        print(f"✅ Ingested {performance_df.count()} restaurant performance records")
    
    def _ingest_weather_data(self):
        """Ingest weather data (external API simulation)"""
        print("🌤️ Ingesting weather data...")
        
        # Simulate weather data
        from datetime import datetime, timedelta
        import random
        
        zones = ["North", "South", "East", "West"]
        conditions = ["Sunny", "Cloudy", "Rainy", "Stormy"]
        weather_data = []
        
        # Generate hourly weather data for last 24 hours
        for i in range(24):
            weather_time = datetime.now() - timedelta(hours=i)
            for zone in zones:
                weather_data.append((
                    weather_time,
                    round(random.uniform(15, 35), 1),  # temperature
                    random.choice(conditions),  # condition
                    zone
                ))
        
        schema = StructType([
            StructField("weather_time", TimestampType(), False),
            StructField("temperature", FloatType(), False),
            StructField("condition", StringType(), False),
            StructField("zone", StringType(), False)
        ])
        
        weather_df = (
            self.spark.createDataFrame(weather_data, schema)
            .withColumn("ingest_timestamp", current_timestamp())
        )
        
        # Write to Bronze Iceberg table
        weather_df.writeTo("demo.bronze.bronze_weather").createOrReplace()
        print(f"✅ Ingested {weather_df.count()} weather records")
    
    def _get_orders_schema(self):
        """Define schema for orders JSON"""
        return StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("restaurant_id", StringType(), False),
            StructField("driver_id", StringType(), False),
            StructField("order_timestamp", StringType(), False),
            StructField("delivery_timestamp", StringType(), True),
            StructField("order_status", StringType(), False),
            StructField("items", ArrayType(StructType([
                StructField("item_id", StringType(), False),
                StructField("item_name", StringType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("unit_price", FloatType(), False),
                StructField("total_price", FloatType(), False)
            ])), False),
            StructField("total_amount", FloatType(), False),
            StructField("payment_method", StringType(), False)
        ])
    
    def _get_locations_schema(self):
        """Define schema for driver locations JSON"""
        return StructType([
            StructField("location_id", StringType(), False),
            StructField("driver_id", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("latitude", FloatType(), False),
            StructField("longitude", FloatType(), False),
            StructField("status", StringType(), False),
            StructField("speed", FloatType(), False)
        ])
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

if __name__ == "__main__":
    bronze_ingestion = BronzeIngestion()
    
    try:
        # Start streaming ingestion
        orders_query = bronze_ingestion.ingest_orders_stream()
        locations_query = bronze_ingestion.ingest_driver_locations_stream()
        
        # Ingest batch data
        bronze_ingestion.ingest_batch_data()
        
        print("🎯 Bronze layer ingestion running...")
        print("Press Ctrl+C to stop")
        
        # Keep streaming jobs running
        orders_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("🛑 Stopping bronze ingestion...")
    finally:
        bronze_ingestion.stop() 