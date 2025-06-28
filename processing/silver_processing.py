import sys
import os
sys.path.append('/home/iceberg/processing')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from spark_config import create_spark_session

class SilverProcessing:
    def __init__(self):
        self.spark = create_spark_session("WoEat - Silver Layer Processing")
    
    def process_all_silver_tables(self):
        """Process all Silver layer tables from Bronze data"""
        print("Starting Silver layer processing...")
        
        # Process each table
        self.process_silver_orders()
        self.process_silver_order_items()
        self.process_silver_menu_items()
        self.process_silver_drivers()
        self.process_silver_restaurants()
        self.process_silver_ratings()
        self.process_silver_restaurant_performance()
        self.process_silver_driver_performance()
        self.process_silver_weather()
        
        print("Silver layer processing completed")
    
    def process_silver_orders(self):
        """Clean and validate orders data with calculated fields"""
        print("Processing Silver Orders...")
        
        # Read from Bronze
        bronze_orders = self.spark.table("bronze.bronze_orders")
        
        # Data cleaning and validation with calculated fields
        silver_orders = (
            bronze_orders
            .filter(col("order_id").isNotNull())  # Remove null order IDs
            .filter(col("customer_id").isNotNull())  # Remove null customer IDs
            .filter(col("restaurant_id").isNotNull())  # Remove null restaurant IDs
            .filter(col("order_time").isNotNull())  # Remove null order times
            .filter(col("status").isin(["placed", "confirmed", "preparing", "ready", "picked_up", "delivered", "cancelled"]))  # Valid statuses only
            .withColumn("order_time", 
                       when(col("order_time") > current_timestamp(), current_timestamp())
                       .otherwise(col("order_time")))  # Fix future timestamps
            .withColumn("delivery_time",
                       when(col("delivery_time") < col("order_time"), None)
                       .otherwise(col("delivery_time")))  # Fix invalid delivery times
            .withColumn("prep_time_minutes",
                       when((col("prep_start_time").isNotNull()) & (col("prep_end_time").isNotNull()),
                            (unix_timestamp(col("prep_end_time")) - unix_timestamp(col("prep_start_time"))) / 60.0)
                       .otherwise(None))  # Calculate prep time in minutes
            .withColumn("delivery_time_minutes",
                       when((col("order_time").isNotNull()) & (col("delivery_time").isNotNull()),
                            (unix_timestamp(col("delivery_time")) - unix_timestamp(col("order_time"))) / 60.0)
                       .otherwise(None))  # Calculate delivery time in minutes
            .withColumn("total_amount",
                       when(col("total_amount") < 0, 0.0)
                       .otherwise(round(col("total_amount"), 2)))  # Non-negative amounts
            .withColumn("tip_amount",
                       when(col("tip_amount") < 0, 0.0)
                       .otherwise(round(col("tip_amount"), 2)))  # Non-negative tips
            .withColumn("cancelled", col("status") == "cancelled")  # Boolean cancelled flag
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "order_id",
                "customer_id", 
                "restaurant_id",
                "driver_id",
                "status",
                "order_time",
                "delivery_time",
                "total_amount",
                "prep_time_minutes",
                "delivery_time_minutes",
                "tip_amount",
                "cancelled",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        silver_orders.writeTo("silver.silver_orders").createOrReplace()
        print(f"Processed {silver_orders.count()} orders to Silver layer")

    def process_silver_order_items(self):
        """Clean and validate order items data with calculated fields"""
        print("Processing Silver Order Items...")
        
        # Read from Bronze
        bronze_order_items = self.spark.table("bronze.bronze_order_items")
        
        # Data cleaning and validation with calculated fields
        silver_order_items = (
            bronze_order_items
            .filter(col("order_item_id").isNotNull())  # Remove null item IDs
            .filter(col("order_id").isNotNull())  # Remove null order IDs
            .filter(col("item_id").isNotNull())  # Remove null item IDs
            .filter(col("quantity") > 0)  # Positive quantities only
            .filter(col("item_price") > 0)  # Positive prices only
            .withColumn("quantity", col("quantity").cast("int"))  # Ensure integer
            .withColumn("item_price", round(col("item_price"), 2))  # Round prices
            .withColumn("extended_price", round(col("quantity") * col("item_price"), 2))  # Calculate extended price
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "order_item_id",
                "order_id",
                "item_id",
                "quantity",
                "item_price",
                "extended_price",
                "order_time",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        silver_order_items.writeTo("silver.silver_order_items").createOrReplace()
        print(f"Processed {silver_order_items.count()} order items to Silver layer")
    
    def process_silver_menu_items(self):
        """Clean and validate menu items data"""
        print("Processing Silver Menu Items...")
        
        # Read from Bronze
        bronze_menu_items = self.spark.table("bronze.bronze_menu_items")
        
        # Data cleaning and validation
        silver_menu_items = (
            bronze_menu_items
            .filter(col("item_id").isNotNull())  # Remove null item IDs
            .filter(col("restaurant_id").isNotNull())  # Remove null restaurant IDs
            .filter(col("name").isNotNull())  # Remove null item names
            .filter(col("price") > 0)  # Remove invalid prices
            .withColumn("item_name", trim(col("name")))  # Trim whitespace and rename
            .withColumn("category", 
                       when(col("category").isNull(), "Other")
                       .otherwise(trim(col("category"))))  # Default category for nulls
            .withColumn("base_price", round(col("price"), 2))  # Round prices to 2 decimals
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "item_id",
                "restaurant_id",
                "item_name",
                "category",
                "base_price",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        silver_menu_items.writeTo("silver.silver_menu_items").createOrReplace()
        print(f"Processed {silver_menu_items.count()} menu items to Silver layer")
    
    def process_silver_drivers(self):
        """Clean and validate drivers data"""
        print("Processing Silver Drivers...")
        
        # Read from Bronze
        bronze_drivers = self.spark.table("bronze.bronze_drivers")
        
        # Data cleaning and validation
        silver_drivers = (
            bronze_drivers
            .filter(col("driver_id").isNotNull())  # Remove null driver IDs
            .filter(col("name").isNotNull())  # Remove null names
            .withColumn("name", trim(col("name")))  # Trim whitespace
            .withColumn("vehicle_type", 
                       when(col("vehicle_type").isNull(), "Unknown")
                       .otherwise(trim(col("vehicle_type"))))  # Default vehicle type for nulls
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "driver_id",
                "name",
                "phone",
                "vehicle_type",
                "rating",
                "is_active",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        silver_drivers.writeTo("silver.silver_drivers").createOrReplace()
        print(f"Processed {silver_drivers.count()} drivers to Silver layer")

    def process_silver_restaurants(self):
        """Clean and validate restaurants data"""
        print("Processing Silver Restaurants...")
        
        # Read from Bronze
        bronze_restaurants = self.spark.table("bronze.bronze_restaurants")
        
        # Data cleaning and validation
        silver_restaurants = (
            bronze_restaurants
            .filter(col("restaurant_id").isNotNull())  # Remove null restaurant IDs
            .filter(col("name").isNotNull())  # Remove null names
            .withColumn("restaurant_name", trim(col("name")))  # Trim whitespace and rename
            .withColumn("cuisine_type", 
                       when(col("cuisine_type").isNull(), "Other")
                       .otherwise(trim(col("cuisine_type"))))  # Default cuisine for nulls
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "restaurant_id",
                "restaurant_name",
                "cuisine_type",
                "address",
                "phone",
                "rating",
                "is_active",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        silver_restaurants.writeTo("silver.silver_restaurants").createOrReplace()
        print(f"Processed {silver_restaurants.count()} restaurants to Silver layer")

    def process_silver_ratings(self):
        """Clean and validate ratings data"""
        print("⭐ Processing Silver Ratings...")
        
        # Read from Bronze
        bronze_ratings = self.spark.table("bronze.bronze_ratings")
        
        # Data cleaning and validation
        silver_ratings = (
            bronze_ratings
            .filter(col("rating_id").isNotNull())  # Remove null rating IDs
            .filter(col("order_id").isNotNull())  # Remove null order IDs
            .filter(col("driver_rating").between(1.0, 5.0))  # Valid rating range
            .filter(col("food_rating").between(1.0, 5.0))  # Valid rating range
            .filter(col("delivery_rating").between(1.0, 5.0))  # Valid rating range
            .withColumn("driver_rating", round(col("driver_rating"), 1))  # Round rating to 1 decimal
            .withColumn("food_rating", round(col("food_rating"), 1))  # Round rating to 1 decimal
            .withColumn("delivery_rating", round(col("delivery_rating"), 1))  # Round rating to 1 decimal
            .withColumn("rating_type", 
                       when(col("rating_type").isNull(), "unknown")
                       .otherwise(trim(col("rating_type"))))  # Default type for nulls
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "rating_id",
                "order_id",
                "driver_id",
                "restaurant_id",
                "driver_rating",
                "food_rating",
                "delivery_rating",
                "rating_time",
                "rating_type",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        silver_ratings.writeTo("silver.silver_ratings").createOrReplace()
        print(f"Processed {silver_ratings.count()} ratings to Silver layer")

    def process_silver_restaurant_performance(self):
        """Create aggregated restaurant performance data from raw orders and ratings"""
        print("Processing Silver Restaurant Performance...")
        
        # Read Silver data
        silver_orders = self.spark.table("silver.silver_orders")
        silver_ratings = self.spark.table("silver.silver_ratings")
        
        # Calculate daily restaurant performance metrics
        restaurant_performance = (
            silver_orders
            .withColumn("report_date", to_date(col("order_time")))
            .groupBy("report_date", "restaurant_id")
            .agg(
                avg("prep_time_minutes").alias("avg_prep_time"),
                count("order_id").alias("orders_count"),
                sum(when(col("cancelled"), 1).otherwise(0)).alias("cancelled_orders"),
                avg("tip_amount").alias("avg_tip"),
                sum("total_amount").alias("total_revenue")
            )
            .withColumn("cancel_rate", 
                       round(col("cancelled_orders").cast("float") / col("orders_count"), 3))
            .withColumn("avg_prep_time", round(col("avg_prep_time"), 1))
            .withColumn("avg_tip", round(col("avg_tip"), 2))
            .withColumn("total_revenue", round(col("total_revenue"), 2))
        )
        
        # Add average ratings
        avg_ratings = (
            silver_ratings
            .withColumn("report_date", to_date(col("rating_time")))
            .groupBy("report_date", "restaurant_id")
            .agg(avg("food_rating").alias("avg_rating"))
            .withColumn("avg_rating", round(col("avg_rating"), 1))
        )
        
        # Join performance with ratings
        final_performance = (
            restaurant_performance
            .join(avg_ratings, ["report_date", "restaurant_id"], "left")
            .withColumn("avg_rating", 
                       when(col("avg_rating").isNull(), 4.0)
                       .otherwise(col("avg_rating")))  # Default rating if no ratings
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "report_date",
                "restaurant_id",
                "avg_prep_time",
                "avg_rating",
                "orders_count",
                "cancel_rate",
                "avg_tip",
                "total_revenue",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        final_performance.writeTo("silver.silver_restaurant_performance").createOrReplace()
        print(f"Processed {final_performance.count()} restaurant performance records to Silver layer")

    def process_silver_driver_performance(self):
        """Create aggregated driver performance data from raw orders and ratings"""
        print("Processing Silver Driver Performance...")
        
        # Read Silver data
        silver_orders = self.spark.table("silver.silver_orders")
        silver_ratings = self.spark.table("silver.silver_ratings")
        
        # Calculate daily driver performance metrics
        driver_performance = (
            silver_orders
            .filter(col("driver_id").isNotNull())
            .withColumn("report_date", to_date(col("order_time")))
            .groupBy("report_date", "driver_id")
            .agg(
                count("order_id").alias("orders_completed"),
                avg("delivery_time_minutes").alias("avg_delivery_time"),
                sum("tip_amount").alias("total_tips")
            )
            .withColumn("avg_delivery_time", round(col("avg_delivery_time"), 1))
            .withColumn("total_tips", round(col("total_tips"), 2))
        )
        
        # Add average ratings
        avg_ratings = (
            silver_ratings
            .withColumn("report_date", to_date(col("rating_time")))
            .groupBy("report_date", "driver_id")
            .agg(avg("driver_rating").alias("avg_rating"))
            .withColumn("avg_rating", round(col("avg_rating"), 1))
        )
        
        # Join performance with ratings
        final_performance = (
            driver_performance
            .join(avg_ratings, ["report_date", "driver_id"], "left")
            .withColumn("avg_rating", 
                       when(col("avg_rating").isNull(), 4.0)
                       .otherwise(col("avg_rating")))  # Default rating if no ratings
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "report_date",
                "driver_id",
                "avg_rating",
                "orders_completed",
                "avg_delivery_time",
                "total_tips",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        final_performance.writeTo("silver.silver_driver_performance").createOrReplace()
        print(f"Processed {final_performance.count()} driver performance records to Silver layer")
    
    def process_silver_weather(self):
        """Clean and validate weather data"""
        print("Processing Silver Weather...")
        
        # Read from Bronze
        bronze_weather = self.spark.table("bronze.bronze_weather")
        
        # Data cleaning and validation
        silver_weather = (
            bronze_weather
            .filter(col("date").isNotNull())  # Remove null dates
            .filter(col("temperature_celsius").between(-50, 60))  # Reasonable temperature range
            .withColumn("conditions", 
                       when(col("conditions").isNull(), "Unknown")
                       .otherwise(trim(col("conditions"))))  # Default condition for nulls
            .withColumn("temperature_celsius", round(col("temperature_celsius"), 1))  # Round to 1 decimal
            .withColumn("wind_speed_kmh", round(col("wind_speed_kmh"), 1))  # Round wind speed
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "date",
                "temperature_celsius",
                "humidity_percent",
                "conditions",
                "wind_speed_kmh",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        silver_weather.writeTo("silver.silver_weather").createOrReplace()
        print(f"Processed {silver_weather.count()} weather records to Silver layer")
    
    def run_data_quality_checks(self):
        """Run data quality checks on Silver layer"""
        print("Running data quality checks...")
        
        try:
            # Check orders data quality
            orders = self.spark.table("silver.silver_orders")
            orders_count = orders.count()
            invalid_orders = orders.filter(
                col("order_id").isNull() | 
                col("customer_id").isNull() | 
                col("restaurant_id").isNull()
            ).count()
            
            print(f"Orders: {orders_count} total, {invalid_orders} invalid")
            
            # Check order items data quality
            order_items = self.spark.table("silver.silver_order_items")
            items_count = order_items.count()
            invalid_items = order_items.filter(
                (col("quantity") <= 0) | 
                (col("item_price") <= 0)
            ).count()
            
            print(f"Order Items: {items_count} total, {invalid_items} invalid")
            
            # Check ratings data quality
            ratings = self.spark.table("silver.silver_ratings")
            ratings_count = ratings.count()
            invalid_ratings = ratings.filter(
                (col("driver_rating") < 1.0) | (col("driver_rating") > 5.0) |
                (col("food_rating") < 1.0) | (col("food_rating") > 5.0) |
                (col("delivery_rating") < 1.0) | (col("delivery_rating") > 5.0)
            ).count()
            
            print(f"Ratings: {ratings_count} total, {invalid_ratings} invalid")
            
            print("Data quality checks completed")
            
        except Exception as e:
            print(f"Data quality checks failed: {str(e)}")
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            print("Silver processing stopped")

    def process_streaming_silver_tables(self):
        """Process streaming data through Silver layer"""
        print("Processing streaming data through Silver layer...")
        
        try:
            # Process streaming orders
            self.process_silver_orders_streaming()
            
            # Process streaming order items
            self.process_silver_order_items_streaming()
            
                    # Note: Only processing orders and order items (original design)
            
            # Process daily performance for streaming data
            self.process_daily_performance_streaming()
            
            print("Streaming Silver processing completed")
            
        except Exception as e:
            print(f"Streaming Silver processing failed: {e}")
            raise
    
    def process_silver_orders_streaming(self):
        """Process streaming orders data through Silver layer"""
        print("Processing streaming orders...")
        
        # Read from Bronze streaming orders
        bronze_orders = self.spark.table("bronze.bronze_orders")
        
        # Apply Silver transformations for streaming data
        silver_orders = (
            bronze_orders
            .filter(col("order_id").isNotNull())
            .withColumn("delivery_time_minutes", 
                       when(col("delivery_time").isNotNull(),
                            (unix_timestamp("delivery_time") - unix_timestamp("order_time")) / 60.0)
                       .otherwise(None))
            .withColumn("prep_time_minutes",
                       when(col("prep_end_time").isNotNull() & col("prep_start_time").isNotNull(),
                            (unix_timestamp("prep_end_time") - unix_timestamp("prep_start_time")) / 60.0)
                       .otherwise(None))
            .withColumn("cancelled", col("status") == "cancelled")
            .withColumn("data_quality_score", self._calculate_quality_score_streaming())
            .withColumn("processing_timestamp", current_timestamp())
        )
        
        # Write to Silver table (append mode for streaming)
        (silver_orders
         .write
         .format("iceberg")
         .mode("append")
         .saveAsTable("silver.silver_orders"))
        
        print("Streaming orders processed to Silver")
    
    def process_silver_order_items_streaming(self):
        """Process streaming order items through Silver layer"""
        print("Processing streaming order items...")
        
        # Read from Bronze streaming order items
        bronze_order_items = self.spark.table("bronze.bronze_order_items")
        
        # Apply Silver transformations
        silver_order_items = (
            bronze_order_items
            .filter(col("order_item_id").isNotNull())
            .filter(col("order_id").isNotNull())
            .withColumn("extended_price", col("quantity") * col("item_price"))
            .withColumn("processing_timestamp", current_timestamp())
        )
        
        # Write to Silver table
        (silver_order_items
         .write
         .format("iceberg")
         .mode("append")
         .saveAsTable("silver.silver_order_items"))
        
        print("Streaming order items processed to Silver")
    
    def process_silver_driver_locations_streaming(self):
        """Process streaming driver locations through Silver layer"""
        print("Processing streaming driver locations...")
        
        # Read from Bronze streaming driver locations
        bronze_locations = self.spark.table("bronze.bronze_driver_locations")
        
        # Apply Silver transformations
        silver_locations = (
            bronze_locations
            .filter(col("location_id").isNotNull())
            .filter(col("driver_id").isNotNull())
            .withColumn("processing_timestamp", current_timestamp())
        )
        
        # Write to Silver table
        (silver_locations
         .write
         .format("iceberg")
         .mode("append")
         .saveAsTable("silver.silver_driver_locations"))
        
        print("Streaming driver locations processed to Silver")
    
    def process_daily_performance_streaming(self):
        """Create daily performance metrics from streaming data"""
        print("Processing daily performance from streaming data...")
        
        # Get streaming orders from Silver
        silver_orders = self.spark.table("silver.silver_orders")
        
        # Create daily aggregations for streaming data
        daily_performance = (
            silver_orders
            .filter(col("processing_timestamp") >= current_timestamp() - expr("INTERVAL 1 DAY"))
            .groupBy(
                to_date("order_time").alias("performance_date"),
                col("restaurant_id"),
                col("driver_id")
            )
            .agg(
                count("*").alias("total_orders"),
                countDistinct("customer_id").alias("unique_customers"),
                sum("total_amount").alias("total_revenue"),
                avg("delivery_time_minutes").alias("avg_delivery_time"),
                avg("prep_time_minutes").alias("avg_prep_time"),
                sum(when(col("cancelled"), 1).otherwise(0)).alias("cancelled_orders"),
                avg("data_quality_score").alias("avg_quality_score")
            )
            .withColumn("processing_timestamp", current_timestamp())
        )
        
        # Write to Silver daily performance (merge for streaming updates)
        (daily_performance
         .write
         .format("iceberg")
         .mode("append")
         .saveAsTable("silver.daily_performance"))
        
        print("Daily performance processed from streaming data")
    
    def _calculate_quality_score_streaming(self):
        """Calculate data quality score for streaming data"""
        return (
            when(col("order_id").isNull(), 0.0)
            .when(col("customer_id").isNull(), 0.2)
            .when(col("restaurant_id").isNull(), 0.1)
            .when(col("total_amount") <= 0, 0.3)
            .when(col("kafka_timestamp").isNull(), 0.4)
            .otherwise(1.0)
        )

if __name__ == "__main__":
    silver_processor = SilverProcessing()
    try:
        silver_processor.process_all_silver_tables()
        silver_processor.run_data_quality_checks()
    except Exception as e:
        print(f"Silver processing failed: {str(e)}")
    finally:
        silver_processor.stop() 
