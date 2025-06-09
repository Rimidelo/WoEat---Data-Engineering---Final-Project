from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

class SimpleBronzeIngestion:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("WoEat - Simple Bronze Ingestion")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config("spark.sql.catalog.demo.s3.access-key-id", "admin")
            .config("spark.sql.catalog.demo.s3.secret-access-key", "password")
            .getOrCreate()
        )
    
    def create_all_bronze_tables(self):
        """Create all Bronze tables with sample data"""
        print("🔄 Creating Bronze tables...")
        
        self.create_bronze_menu_items()
        self.create_bronze_drivers()
        self.create_bronze_restaurant_performance()
        self.create_bronze_weather()
        
        print("✅ All Bronze tables created")
    
    def create_bronze_menu_items(self):
        """Create bronze menu items table"""
        print("📋 Creating bronze menu items...")
        
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
        
        menu_df.writeTo("demo.bronze.bronze_menu_items").createOrReplace()
        print(f"✅ Created {menu_df.count()} menu items")
    
    def create_bronze_drivers(self):
        """Create bronze drivers table"""
        print("🚗 Creating bronze drivers...")
        
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
        
        drivers_df.writeTo("demo.bronze.bronze_drivers").createOrReplace()
        print(f"✅ Created {drivers_df.count()} drivers")
    
    def create_bronze_restaurant_performance(self):
        """Create bronze restaurant performance table"""
        print("🏪 Creating bronze restaurant performance...")
        
        # Simple static data
        performance_data = [
            ("2025-06-07", "rest_001", 25.5, 4.2, 45, 0.05, 3.50),
            ("2025-06-07", "rest_002", 18.2, 4.5, 62, 0.03, 4.20),
            ("2025-06-07", "rest_003", 32.1, 4.8, 38, 0.02, 5.10),
            ("2025-06-07", "rest_004", 22.8, 4.1, 55, 0.08, 2.80),
            ("2025-06-07", "rest_005", 28.5, 4.6, 41, 0.04, 4.75),
            ("2025-06-06", "rest_001", 24.1, 4.3, 48, 0.06, 3.25),
            ("2025-06-06", "rest_002", 19.5, 4.4, 58, 0.04, 4.10),
            ("2025-06-06", "rest_003", 31.2, 4.7, 42, 0.03, 5.25),
            ("2025-06-06", "rest_004", 21.9, 4.2, 52, 0.07, 3.15),
            ("2025-06-06", "rest_005", 27.8, 4.5, 39, 0.05, 4.60)
        ]
        
        schema = StructType([
            StructField("report_date", StringType(), False),
            StructField("restaurant_id", StringType(), False),
            StructField("avg_prep_time", FloatType(), False),
            StructField("avg_rating", FloatType(), False),
            StructField("orders_count", IntegerType(), False),
            StructField("cancel_rate", FloatType(), False),
            StructField("avg_tip", FloatType(), False)
        ])
        
        performance_df = (
            self.spark.createDataFrame(performance_data, schema)
            .withColumn("report_date", to_date(col("report_date"), "yyyy-MM-dd"))
            .withColumn("ingest_timestamp", current_timestamp())
            .withColumn("is_late_arrival", lit(False))
            .withColumn("days_late", lit(0))
        )
        
        performance_df.writeTo("demo.bronze.bronze_restaurant_performance").createOrReplace()
        print(f"✅ Created {performance_df.count()} restaurant performance records")
    
    def create_bronze_weather(self):
        """Create bronze weather table"""
        print("🌤️ Creating bronze weather...")
        
        weather_data = [
            ("2025-06-07 12:00:00", 22.5, "Sunny", "North"),
            ("2025-06-07 12:00:00", 24.1, "Cloudy", "South"),
            ("2025-06-07 12:00:00", 21.8, "Sunny", "East"),
            ("2025-06-07 12:00:00", 23.2, "Rainy", "West"),
            ("2025-06-07 11:00:00", 21.9, "Sunny", "North"),
            ("2025-06-07 11:00:00", 23.5, "Cloudy", "South"),
            ("2025-06-07 11:00:00", 20.8, "Sunny", "East"),
            ("2025-06-07 11:00:00", 22.1, "Stormy", "West")
        ]
        
        schema = StructType([
            StructField("weather_time", StringType(), False),
            StructField("temperature", FloatType(), False),
            StructField("condition", StringType(), False),
            StructField("zone", StringType(), False)
        ])
        
        weather_df = (
            self.spark.createDataFrame(weather_data, schema)
            .withColumn("weather_time", to_timestamp(col("weather_time"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("ingest_timestamp", current_timestamp())
        )
        
        weather_df.writeTo("demo.bronze.bronze_weather").createOrReplace()
        print(f"✅ Created {weather_df.count()} weather records")
    
    def create_sample_orders(self):
        """Create sample orders for testing"""
        print("📦 Creating sample orders...")
        
        # Sample order data
        orders_data = [
            ("order_001", "cust_001", "rest_001", "driver_001", 
             [{"item_id": "item_001", "item_name": "Margherita Pizza", "quantity": 1, "unit_price": 12.99, "total_price": 12.99}],
             "2025-06-07 12:30:00", "delivered", "2025-06-07 13:15:00"),
            ("order_002", "cust_002", "rest_002", "driver_002",
             [{"item_id": "item_004", "item_name": "Classic Burger", "quantity": 2, "unit_price": 9.99, "total_price": 19.98}],
             "2025-06-07 12:45:00", "delivered", "2025-06-07 13:30:00"),
            ("order_003", "cust_003", "rest_003", "driver_003",
             [{"item_id": "item_007", "item_name": "California Roll", "quantity": 1, "unit_price": 8.99, "total_price": 8.99}],
             "2025-06-07 13:00:00", "preparing", None)
        ]
        
        # Create orders DataFrame with proper schema
        orders_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("restaurant_id", StringType(), False),
            StructField("driver_id", StringType(), False),
            StructField("items", ArrayType(StructType([
                StructField("item_id", StringType(), False),
                StructField("item_name", StringType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("unit_price", FloatType(), False),
                StructField("total_price", FloatType(), False)
            ])), False),
            StructField("order_time", StringType(), False),
            StructField("status", StringType(), False),
            StructField("delivery_time", StringType(), True)
        ])
        
        orders_df = (
            self.spark.createDataFrame(orders_data, orders_schema)
            .withColumn("order_time", to_timestamp(col("order_time"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("delivery_time", 
                       when(col("delivery_time").isNotNull(), 
                            to_timestamp(col("delivery_time"), "yyyy-MM-dd HH:mm:ss"))
                       .otherwise(None))
            .withColumn("ingest_timestamp", current_timestamp())
        )
        
        # Create the orders table
        orders_df.writeTo("demo.bronze.bronze_orders").createOrReplace()
        print(f"✅ Created {orders_df.count()} sample orders")
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

if __name__ == "__main__":
    bronze = SimpleBronzeIngestion()
    
    try:
        bronze.create_all_bronze_tables()
        bronze.create_sample_orders()
        print("🎯 Simple Bronze ingestion completed!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        bronze.stop() 