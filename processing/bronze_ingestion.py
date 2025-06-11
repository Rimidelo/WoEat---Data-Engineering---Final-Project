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
                to_timestamp(col("data.order_timestamp")).alias("order_time"),
                col("data.order_status").alias("status"),
                when(col("data.delivery_timestamp").isNotNull(), 
                     to_timestamp(col("data.delivery_timestamp"))).alias("delivery_time"),
                col("data.total_amount").cast("float"),
                when(col("data.prep_start_timestamp").isNotNull(),
                     to_timestamp(col("data.prep_start_timestamp"))).alias("prep_start_time"),
                when(col("data.prep_end_timestamp").isNotNull(),
                     to_timestamp(col("data.prep_end_timestamp"))).alias("prep_end_time"),
                col("data.tip_amount").cast("float"),
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

    def ingest_order_items_stream(self):
        """Ingest real-time order items from Kafka"""
        print("🔄 Starting order items stream ingestion...")
        
        # Read from Kafka
        order_items_stream = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:29092")
            .option("subscribe", "order-items-topic")
            .option("startingOffsets", "latest")
            .load()
        )
        
        # Parse JSON and extract order items data
        order_items_parsed = (
            order_items_stream
            .select(
                col("key").cast("string").alias("order_item_id"),
                from_json(col("value").cast("string"), self._get_order_items_schema()).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            )
            .select(
                col("data.order_item_id"),
                col("data.order_id"),
                col("data.item_id"),
                col("data.quantity").cast("int"),
                col("data.item_price").cast("float"),
                to_timestamp(col("data.order_time")).alias("order_time")
            )
        )
        
        # Write to Bronze Iceberg table
        query = (
            order_items_parsed
            .writeStream
            .format("iceberg")
            .outputMode("append")
            .option("path", "demo.bronze.bronze_order_items")
            .option("checkpointLocation", "/tmp/checkpoints/bronze_order_items")
            .trigger(processingTime="30 seconds")
            .start()
        )
        
        print("✅ Order items stream ingestion started")
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
                col("data.speed")
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
        
        # 3. Ingest Restaurants (batch)
        self._ingest_restaurants()
        
        # 4. Ingest Ratings (batch)
        self._ingest_ratings()
        
        # 5. Ingest Weather Data (external API simulation)
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
        
        menu_df = self.spark.createDataFrame(menu_data, schema)
        
        # Write to Bronze Iceberg table
        menu_df.writeTo("demo.bronze.bronze_menu_items").createOrReplace()
        print(f"✅ Ingested {menu_df.count()} menu items")
    
    def _ingest_drivers(self):
        """Ingest drivers from batch source"""
        print("🚗 Ingesting drivers...")
        
        # Simulate drivers data (without ratings - those go to separate table)
        drivers_data = [
            ("driver_001", "Alice Johnson", "North", "2023-01-15"),
            ("driver_002", "Bob Smith", "South", "2023-02-20"),
            ("driver_003", "Carol Davis", "East", "2023-01-10"),
            ("driver_004", "David Wilson", "West", "2023-03-05"),
            ("driver_005", "Eva Brown", "North", "2023-02-15"),
            ("driver_006", "Frank Miller", "South", "2023-01-25"),
            ("driver_007", "Grace Lee", "East", "2023-03-10"),
            ("driver_008", "Henry Taylor", "West", "2023-02-08"),
            ("driver_009", "Ivy Chen", "North", "2023-01-30"),
            ("driver_010", "Jack Anderson", "South", "2023-03-15"),
            ("driver_011", "Kate Thompson", "East", "2023-02-25"),
            ("driver_012", "Leo Garcia", "West", "2023-01-20"),
            ("driver_013", "Mia Rodriguez", "North", "2023-03-01"),
            ("driver_014", "Noah Martinez", "South", "2023-02-10"),
            ("driver_015", "Olivia White", "East", "2023-01-05"),
            ("driver_016", "Paul Harris", "West", "2023-03-20"),
            ("driver_017", "Quinn Clark", "North", "2023-02-18"),
            ("driver_018", "Ruby Lewis", "South", "2023-01-12"),
            ("driver_019", "Sam Walker", "East", "2023-03-08"),
            ("driver_020", "Tina Young", "West", "2023-02-22")
        ]
        
        schema = StructType([
            StructField("driver_id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("zone", StringType(), False),
            StructField("created_at", StringType(), False)
        ])
        
        drivers_df = (
            self.spark.createDataFrame(drivers_data, schema)
            .withColumn("created_at", to_timestamp(col("created_at")))
        )
        
        # Write to Bronze Iceberg table
        drivers_df.writeTo("demo.bronze.bronze_drivers").createOrReplace()
        print(f"✅ Ingested {drivers_df.count()} drivers")

    def _ingest_restaurants(self):
        """Ingest restaurants from batch source"""
        print("🏪 Ingesting restaurants...")
        
        # Simulate restaurants data
        restaurants_data = [
            ("rest_001", "Pizza Palace", "Italian", "North", True, "2023-01-01"),
            ("rest_002", "Burger Barn", "American", "South", True, "2023-01-02"),
            ("rest_003", "Sushi Spot", "Japanese", "East", True, "2023-01-03"),
            ("rest_004", "Taco Town", "Mexican", "West", True, "2023-01-04"),
            ("rest_005", "Curry Corner", "Indian", "North", True, "2023-01-05")
        ]
        
        schema = StructType([
            StructField("restaurant_id", StringType(), False),
            StructField("restaurant_name", StringType(), False),
            StructField("cuisine_type", StringType(), False),
            StructField("zone", StringType(), False),
            StructField("active_flag", BooleanType(), False),
            StructField("created_at", StringType(), False)
        ])
        
        restaurants_df = (
            self.spark.createDataFrame(restaurants_data, schema)
            .withColumn("created_at", to_timestamp(col("created_at")))
        )
        
        # Write to Bronze Iceberg table
        restaurants_df.writeTo("demo.bronze.bronze_restaurants").createOrReplace()
        print(f"✅ Ingested {restaurants_df.count()} restaurants")

    def _ingest_ratings(self):
        """Ingest ratings from batch source"""
        print("⭐ Ingesting ratings...")
        
        # Simulate ratings data
        ratings_data = [
            ("rating_001", "order_001", "driver_001", "rest_001", 4.8, 4.5, 4.7, "2024-01-15 20:30:00", "order_completion"),
            ("rating_002", "order_002", "driver_002", "rest_002", 4.5, 4.2, 4.3, "2024-01-15 21:15:00", "order_completion"),
            ("rating_003", "order_003", "driver_003", "rest_003", 4.9, 4.8, 4.9, "2024-01-16 19:45:00", "order_completion"),
            ("rating_004", "order_004", "driver_004", "rest_004", 4.2, 4.0, 4.1, "2024-01-16 20:20:00", "order_completion"),
            ("rating_005", "order_005", "driver_005", "rest_005", 4.7, 4.6, 4.8, "2024-01-17 18:30:00", "order_completion"),
        ]
        
        schema = StructType([
            StructField("rating_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("driver_id", StringType(), False),
            StructField("restaurant_id", StringType(), False),
            StructField("driver_rating", FloatType(), False),
            StructField("food_rating", FloatType(), False),
            StructField("delivery_rating", FloatType(), False),
            StructField("rating_time", StringType(), False),
            StructField("rating_type", StringType(), False)
        ])
        
        ratings_df = (
            self.spark.createDataFrame(ratings_data, schema)
            .withColumn("rating_time", to_timestamp(col("rating_time")))
        )
        
        # Write to Bronze Iceberg table
        ratings_df.writeTo("demo.bronze.bronze_ratings").createOrReplace()
        print(f"✅ Ingested {ratings_df.count()} ratings")
        
    def _ingest_weather_data(self):
        """Ingest weather data from external API simulation"""
        print("🌤️ Ingesting weather data...")
        
        # Simulate weather data
        weather_data = [
            ("2024-01-15 12:00:00", 22.5, "Sunny", "North"),
            ("2024-01-15 12:00:00", 25.0, "Partly Cloudy", "South"),
            ("2024-01-15 12:00:00", 20.8, "Cloudy", "East"),
            ("2024-01-15 12:00:00", 23.2, "Sunny", "West"),
            ("2024-01-15 18:00:00", 19.5, "Cloudy", "North"),
            ("2024-01-15 18:00:00", 22.8, "Clear", "South"),
            ("2024-01-15 18:00:00", 18.9, "Rainy", "East"),
            ("2024-01-15 18:00:00", 21.3, "Partly Cloudy", "West"),
            ("2024-01-16 12:00:00", 24.1, "Sunny", "North"),
            ("2024-01-16 12:00:00", 26.5, "Hot", "South"),
            ("2024-01-16 12:00:00", 22.0, "Cloudy", "East"),
            ("2024-01-16 12:00:00", 25.8, "Sunny", "West")
        ]
        
        schema = StructType([
            StructField("weather_time", StringType(), False),
            StructField("temperature", FloatType(), False),
            StructField("condition", StringType(), False),
            StructField("zone", StringType(), False)
        ])
        
        weather_df = (
            self.spark.createDataFrame(weather_data, schema)
            .withColumn("weather_time", to_timestamp(col("weather_time")))
        )
        
        # Write to Bronze Iceberg table
        weather_df.writeTo("demo.bronze.bronze_weather").createOrReplace()
        print(f"✅ Ingested {weather_df.count()} weather records")
    
    def _get_orders_schema(self):
        """Schema for orders JSON from Kafka"""
        return StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("restaurant_id", StringType(), True),
            StructField("driver_id", StringType(), True),
            StructField("order_timestamp", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("delivery_timestamp", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("prep_start_timestamp", StringType(), True),
            StructField("prep_end_timestamp", StringType(), True),
            StructField("tip_amount", StringType(), True)
        ])

    def _get_order_items_schema(self):
        """Schema for order items JSON from Kafka"""
        return StructType([
            StructField("order_item_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("item_price", StringType(), True),
            StructField("order_time", StringType(), True)
        ])
    
    def _get_locations_schema(self):
        """Schema for driver locations JSON from Kafka"""
        return StructType([
            StructField("location_id", StringType(), True),
            StructField("driver_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("status", StringType(), True),
            StructField("speed", FloatType(), True)
        ])
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            print("🛑 Bronze ingestion stopped")

if __name__ == "__main__":
    ingestion = BronzeIngestion()
    try:
        # Ingest batch data first
        ingestion.ingest_batch_data()
        
        # Start streaming ingestion
        orders_query = ingestion.ingest_orders_stream()
        order_items_query = ingestion.ingest_order_items_stream()
        locations_query = ingestion.ingest_driver_locations_stream()
        
        # Keep running
        orders_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("⏹️ Stopping ingestion...")
    finally:
        ingestion.stop() 