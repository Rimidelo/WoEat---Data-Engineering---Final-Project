from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class SilverProcessing:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("WoEat - Silver Layer Processing")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config("spark.sql.catalog.demo.s3.access-key-id", "admin")
            .config("spark.sql.catalog.demo.s3.secret-access-key", "password")
            .getOrCreate()
        )
    
    def process_all_silver_tables(self):
        """Process all Silver layer tables from Bronze data"""
        print("🔄 Starting Silver layer processing...")
        
        # Process each table
        self.process_silver_orders()
        self.process_silver_menu_items()
        self.process_silver_drivers()
        self.process_silver_restaurant_performance()
        self.process_silver_weather()
        
        print("✅ Silver layer processing completed")
    
    def process_silver_orders(self):
        """Clean and validate orders data"""
        print("📦 Processing Silver Orders...")
        
        # Read from Bronze
        bronze_orders = self.spark.table("demo.bronze.bronze_orders")
        
        # Data cleaning and validation
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
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "order_id",
                "customer_id", 
                "restaurant_id",
                "driver_id",
                "items",
                "status",
                "order_time",
                "delivery_time",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        silver_orders.writeTo("demo.silver.silver_orders").createOrReplace()
        print(f"✅ Processed {silver_orders.count()} orders to Silver layer")
    
    def process_silver_menu_items(self):
        """Clean and validate menu items data"""
        print("🍕 Processing Silver Menu Items...")
        
        # Read from Bronze
        bronze_menu_items = self.spark.table("demo.bronze.bronze_menu_items")
        
        # Data cleaning and validation
        silver_menu_items = (
            bronze_menu_items
            .filter(col("item_id").isNotNull())  # Remove null item IDs
            .filter(col("restaurant_id").isNotNull())  # Remove null restaurant IDs
            .filter(col("item_name").isNotNull())  # Remove null item names
            .filter(col("base_price") > 0)  # Remove invalid prices
            .withColumn("item_name", trim(col("item_name")))  # Trim whitespace
            .withColumn("category", 
                       when(col("category").isNull(), "Other")
                       .otherwise(trim(col("category"))))  # Default category for nulls
            .withColumn("base_price", round(col("base_price"), 2))  # Round prices to 2 decimals
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
        silver_menu_items.writeTo("demo.silver.silver_menu_items").createOrReplace()
        print(f"✅ Processed {silver_menu_items.count()} menu items to Silver layer")
    
    def process_silver_drivers(self):
        """Clean and validate drivers data"""
        print("🚗 Processing Silver Drivers...")
        
        # Read from Bronze
        bronze_drivers = self.spark.table("demo.bronze.bronze_drivers")
        
        # Data cleaning and validation
        silver_drivers = (
            bronze_drivers
            .filter(col("driver_id").isNotNull())  # Remove null driver IDs
            .filter(col("name").isNotNull())  # Remove null names
            .filter(col("rating").between(1.0, 5.0))  # Valid rating range
            .withColumn("name", trim(col("name")))  # Trim whitespace
            .withColumn("zone", 
                       when(col("zone").isNull(), "Unknown")
                       .otherwise(trim(col("zone"))))  # Default zone for nulls
            .withColumn("rating", round(col("rating"), 1))  # Round rating to 1 decimal
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "driver_id",
                "name",
                "rating",
                "zone",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        silver_drivers.writeTo("demo.silver.silver_drivers").createOrReplace()
        print(f"✅ Processed {silver_drivers.count()} drivers to Silver layer")
    
    def process_silver_restaurant_performance(self):
        """Clean and validate restaurant performance data"""
        print("🏪 Processing Silver Restaurant Performance...")
        
        # Read from Bronze
        bronze_performance = self.spark.table("demo.bronze.bronze_restaurant_performance")
        
        # Data cleaning and validation
        silver_performance = (
            bronze_performance
            .filter(col("report_date").isNotNull())  # Remove null dates
            .filter(col("restaurant_id").isNotNull())  # Remove null restaurant IDs
            .filter(col("orders_count") >= 0)  # Non-negative order counts
            .filter(col("avg_prep_time") > 0)  # Positive prep times
            .filter(col("avg_rating").between(1.0, 5.0))  # Valid rating range
            .filter(col("cancel_rate").between(0.0, 1.0))  # Valid cancel rate range
            .withColumn("avg_prep_time", round(col("avg_prep_time"), 1))  # Round to 1 decimal
            .withColumn("avg_rating", round(col("avg_rating"), 1))  # Round to 1 decimal
            .withColumn("cancel_rate", round(col("cancel_rate"), 3))  # Round to 3 decimals
            .withColumn("avg_tip", 
                       when(col("avg_tip") < 0, 0.0)
                       .otherwise(round(col("avg_tip"), 2)))  # Non-negative tips
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "report_date",
                "restaurant_id",
                "avg_prep_time",
                "avg_rating",
                "orders_count",
                "cancel_rate",
                "avg_tip",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        silver_performance.writeTo("demo.silver.silver_restaurant_performance").createOrReplace()
        print(f"✅ Processed {silver_performance.count()} restaurant performance records to Silver layer")
    
    def process_silver_weather(self):
        """Clean and validate weather data"""
        print("🌤️ Processing Silver Weather...")
        
        # Read from Bronze
        bronze_weather = self.spark.table("demo.bronze.bronze_weather")
        
        # Data cleaning and validation
        silver_weather = (
            bronze_weather
            .filter(col("weather_time").isNotNull())  # Remove null timestamps
            .filter(col("zone").isNotNull())  # Remove null zones
            .filter(col("temperature").between(-50, 60))  # Reasonable temperature range
            .withColumn("zone", trim(col("zone")))  # Trim whitespace
            .withColumn("condition", 
                       when(col("condition").isNull(), "Unknown")
                       .otherwise(trim(col("condition"))))  # Default condition for nulls
            .withColumn("temperature", round(col("temperature"), 1))  # Round to 1 decimal
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                "zone",
                "weather_time",
                "temperature",
                "condition",
                "ingest_timestamp"
            )
        )
        
        # Write to Silver Iceberg table
        silver_weather.writeTo("demo.silver.silver_weather").createOrReplace()
        print(f"✅ Processed {silver_weather.count()} weather records to Silver layer")
    
    def run_data_quality_checks(self):
        """Run data quality checks on Silver layer"""
        print("🔍 Running data quality checks...")
        
        quality_results = {}
        
        # Check 1: Orders data quality
        orders_df = self.spark.table("demo.silver.silver_orders")
        quality_results["orders_total_count"] = orders_df.count()
        quality_results["orders_null_customer_id"] = orders_df.filter(col("customer_id").isNull()).count()
        quality_results["orders_future_timestamps"] = orders_df.filter(col("order_time") > current_timestamp()).count()
        
        # Check 2: Menu items data quality
        menu_df = self.spark.table("demo.silver.silver_menu_items")
        quality_results["menu_total_count"] = menu_df.count()
        quality_results["menu_invalid_prices"] = menu_df.filter(col("base_price") <= 0).count()
        
        # Check 3: Drivers data quality
        drivers_df = self.spark.table("demo.silver.silver_drivers")
        quality_results["drivers_total_count"] = drivers_df.count()
        quality_results["drivers_invalid_ratings"] = drivers_df.filter(~col("rating").between(1.0, 5.0)).count()
        
        # Print quality report
        print("📊 Data Quality Report:")
        for check, result in quality_results.items():
            status = "✅ PASS" if result == 0 or "total_count" in check else "❌ FAIL"
            print(f"  {check}: {result} {status}")
        
        return quality_results
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

if __name__ == "__main__":
    silver_processing = SilverProcessing()
    
    try:
        # Process all Silver tables
        silver_processing.process_all_silver_tables()
        
        # Run data quality checks
        silver_processing.run_data_quality_checks()
        
        print("🎯 Silver layer processing completed successfully!")
        
    except Exception as e:
        print(f"❌ Error in Silver processing: {str(e)}")
    finally:
        silver_processing.stop() 