from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

class GoldProcessing:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("WoEat - Gold Layer Processing")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config("spark.sql.catalog.demo.s3.access-key-id", "admin")
            .config("spark.sql.catalog.demo.s3.secret-access-key", "password")
            .getOrCreate()
        )
    
    def process_all_gold_tables(self):
        """Process all Gold layer tables from Silver data"""
        print("🔄 Starting Gold layer processing...")
        
        # Process dimension tables first (with SCD Type 2)
        self.process_dim_drivers_scd2()
        self.process_dim_restaurants_scd2()
        self.process_dim_menu_items()
        
        # Process fact tables
        self.process_fact_orders()
        self.process_fact_order_items()
        
        print("✅ Gold layer processing completed")
    
    def process_dim_drivers_scd2(self):
        """Process drivers dimension with SCD Type 2"""
        print("🚗 Processing Dim Drivers (SCD Type 2)...")
        
        # Read current Silver data
        silver_drivers = self.spark.table("demo.silver.silver_drivers")
        
        # Try to read existing Gold dimension
        try:
            existing_dim = self.spark.table("demo.gold.dim_drivers")
            has_existing_data = True
        except:
            has_existing_data = False
            existing_dim = None
        
        if not has_existing_data:
            # First load - create initial dimension
            dim_drivers = (
                silver_drivers
                .withColumn("driver_key", monotonically_increasing_id() + 1)
                .withColumn("record_start_date", current_date())
                .withColumn("record_end_date", lit("9999-12-31").cast(DateType()))
                .withColumn("is_current", lit(True))
                .withColumn("updated_at", current_timestamp())
                .select(
                    "driver_key",
                    "driver_id",
                    "name",
                    "rating",
                    "zone",
                    "record_start_date",
                    "record_end_date",
                    "is_current",
                    "ingest_timestamp",
                    "updated_at"
                )
            )
        else:
            # SCD Type 2 processing for updates
            # Get current active records
            current_records = existing_dim.filter(col("is_current") == True)
            
            # Join with new data to find changes
            changes = (
                silver_drivers.alias("new")
                .join(current_records.alias("curr"), 
                     col("new.driver_id") == col("curr.driver_id"), "left")
                .select(
                    col("new.*"),
                    col("curr.driver_key"),
                    col("curr.name").alias("curr_name"),
                    col("curr.rating").alias("curr_rating"),
                    col("curr.zone").alias("curr_zone")
                )
            )
            
            # Identify records that have changed
            changed_records = changes.filter(
                (col("name") != col("curr_name")) |
                (col("rating") != col("curr_rating")) |
                (col("zone") != col("curr_zone"))
            )
            
            # Identify new records
            new_records = changes.filter(col("driver_key").isNull())
            
            if changed_records.count() > 0 or new_records.count() > 0:
                # Close existing records that have changed
                if changed_records.count() > 0:
                    updated_existing = (
                        existing_dim
                        .join(changed_records.select("driver_id"), "driver_id", "inner")
                        .filter(col("is_current") == True)
                        .withColumn("record_end_date", current_date())
                        .withColumn("is_current", lit(False))
                        .withColumn("updated_at", current_timestamp())
                    )
                    
                    # Keep unchanged existing records
                    unchanged_existing = (
                        existing_dim
                        .join(changed_records.select("driver_id"), "driver_id", "left_anti")
                    )
                    
                    # Create new versions for changed records
                    max_key = existing_dim.agg(max("driver_key")).collect()[0][0]
                    new_versions = (
                        changed_records
                        .withColumn("driver_key", monotonically_increasing_id() + max_key + 1)
                        .withColumn("record_start_date", current_date())
                        .withColumn("record_end_date", lit("9999-12-31").cast(DateType()))
                        .withColumn("is_current", lit(True))
                        .withColumn("updated_at", current_timestamp())
                        .select(
                            "driver_key", "driver_id", "name", "rating", "zone",
                            "record_start_date", "record_end_date", "is_current",
                            "ingest_timestamp", "updated_at"
                        )
                    )
                    
                    # Combine all records
                    dim_drivers = unchanged_existing.union(updated_existing).union(new_versions)
                else:
                    dim_drivers = existing_dim
                
                # Add completely new records
                if new_records.count() > 0:
                    max_key = dim_drivers.agg(max("driver_key")).collect()[0][0] or 0
                    new_driver_records = (
                        new_records
                        .withColumn("driver_key", monotonically_increasing_id() + max_key + 1)
                        .withColumn("record_start_date", current_date())
                        .withColumn("record_end_date", lit("9999-12-31").cast(DateType()))
                        .withColumn("is_current", lit(True))
                        .withColumn("updated_at", current_timestamp())
                        .select(
                            "driver_key", "driver_id", "name", "rating", "zone",
                            "record_start_date", "record_end_date", "is_current",
                            "ingest_timestamp", "updated_at"
                        )
                    )
                    dim_drivers = dim_drivers.union(new_driver_records)
            else:
                dim_drivers = existing_dim
        
        # Write to Gold Iceberg table
        dim_drivers.writeTo("demo.gold.dim_drivers").createOrReplace()
        print(f"✅ Processed {dim_drivers.count()} driver dimension records")
    
    def process_dim_restaurants_scd2(self):
        """Process restaurants dimension with SCD Type 2"""
        print("🏪 Processing Dim Restaurants (SCD Type 2)...")
        
        # Read restaurant performance data (latest for each restaurant)
        restaurant_perf = (
            self.spark.table("demo.silver.silver_restaurant_performance")
            .withColumn("row_num", 
                       row_number().over(
                           Window.partitionBy("restaurant_id")
                           .orderBy(desc("report_date"))
                       ))
            .filter(col("row_num") == 1)
            .select("restaurant_id", "avg_prep_time", "avg_rating")
        )
        
        # Create restaurant dimension data
        restaurants_data = [
            ("rest_001", "Pizza Palace", "Italian", True),
            ("rest_002", "Burger Barn", "American", True),
            ("rest_003", "Sushi Spot", "Japanese", True),
            ("rest_004", "Taco Town", "Mexican", True),
            ("rest_005", "Curry Corner", "Indian", True)
        ]
        
        restaurant_schema = StructType([
            StructField("restaurant_id", StringType(), False),
            StructField("restaurant_name", StringType(), False),
            StructField("cuisine_type", StringType(), False),
            StructField("active_flag", BooleanType(), False)
        ])
        
        restaurant_base = self.spark.createDataFrame(restaurants_data, restaurant_schema)
        
        # Join with performance data
        silver_restaurants = (
            restaurant_base
            .join(restaurant_perf, "restaurant_id", "left")
            .withColumn("avg_prep_time", 
                       when(col("avg_prep_time").isNull(), 30.0)
                       .otherwise(col("avg_prep_time")))
            .withColumn("ingest_timestamp", current_timestamp())
        )
        
        # Try to read existing Gold dimension
        try:
            existing_dim = self.spark.table("demo.gold.dim_restaurants")
            has_existing_data = True
        except:
            has_existing_data = False
        
        if not has_existing_data:
            # First load
            dim_restaurants = (
                silver_restaurants
                .withColumn("restaurant_key", monotonically_increasing_id() + 1)
                .withColumn("record_start_date", current_date())
                .withColumn("record_end_date", lit("9999-12-31").cast(DateType()))
                .withColumn("is_current", lit(True))
                .withColumn("updated_at", current_timestamp())
                .select(
                    "restaurant_key", "restaurant_id", "restaurant_name", "cuisine_type",
                    "avg_prep_time", "active_flag", "record_start_date", "record_end_date",
                    "is_current", "ingest_timestamp", "updated_at"
                )
            )
        else:
            # For simplicity, just replace for now (in real scenario, implement full SCD2)
            max_key = existing_dim.agg(max("restaurant_key")).collect()[0][0] or 0
            dim_restaurants = (
                silver_restaurants
                .withColumn("restaurant_key", monotonically_increasing_id() + max_key + 1)
                .withColumn("record_start_date", current_date())
                .withColumn("record_end_date", lit("9999-12-31").cast(DateType()))
                .withColumn("is_current", lit(True))
                .withColumn("updated_at", current_timestamp())
                .select(
                    "restaurant_key", "restaurant_id", "restaurant_name", "cuisine_type",
                    "avg_prep_time", "active_flag", "record_start_date", "record_end_date",
                    "is_current", "ingest_timestamp", "updated_at"
                )
            )
        
        # Write to Gold Iceberg table
        dim_restaurants.writeTo("demo.gold.dim_restaurants").createOrReplace()
        print(f"✅ Processed {dim_restaurants.count()} restaurant dimension records")
    
    def process_dim_menu_items(self):
        """Process menu items dimension"""
        print("🍕 Processing Dim Menu Items...")
        
        # Read from Silver
        silver_menu_items = self.spark.table("demo.silver.silver_menu_items")
        
        # Get restaurant keys
        dim_restaurants = self.spark.table("demo.gold.dim_restaurants").filter(col("is_current") == True)
        
        # Create menu items dimension
        dim_menu_items = (
            silver_menu_items
            .join(dim_restaurants.select("restaurant_id", "restaurant_key"), "restaurant_id", "inner")
            .withColumn("menu_item_key", monotonically_increasing_id() + 1)
            .select(
                "menu_item_key",
                "item_id",
                "item_name",
                "category",
                "base_price",
                "restaurant_id",
                "restaurant_key",
                "ingest_timestamp"
            )
        )
        
        # Write to Gold Iceberg table
        dim_menu_items.writeTo("demo.gold.dim_menu_items").createOrReplace()
        print(f"✅ Processed {dim_menu_items.count()} menu item dimension records")
    
    def process_fact_orders(self):
        """Process orders fact table"""
        print("📦 Processing Fact Orders...")
        
        # Read from Silver
        silver_orders = self.spark.table("demo.silver.silver_orders")
        
        # Get dimension keys
        dim_drivers = self.spark.table("demo.gold.dim_drivers").filter(col("is_current") == True)
        dim_restaurants = self.spark.table("demo.gold.dim_restaurants").filter(col("is_current") == True)
        
        # Calculate delivery metrics
        fact_orders = (
            silver_orders
            .join(dim_drivers.select("driver_id", "driver_key"), "driver_id", "left")
            .join(dim_restaurants.select("restaurant_id", "restaurant_key"), "restaurant_id", "inner")
            .withColumn("order_key", monotonically_increasing_id() + 1)
            .withColumn("date_key", date_format(col("order_time"), "yyyyMMdd").cast(IntegerType()))
            .withColumn("delivery_minutes", 
                       when(col("delivery_time").isNotNull(),
                            (unix_timestamp(col("delivery_time")) - unix_timestamp(col("order_time"))) / 60)
                       .otherwise(None))
            .withColumn("sla_breached", 
                       when(col("delivery_minutes") > 45, True)
                       .otherwise(False))
            .withColumn("total_amount", 
                       aggregate(col("items"), lit(0.0), 
                                lambda acc, x: acc + x.getField("total_price")))
            .select(
                "order_key",
                "order_id",
                "date_key",
                "driver_key",
                "restaurant_key",
                "order_time",
                "delivery_time",
                "status",
                "total_amount",
                "delivery_minutes",
                "sla_breached",
                "ingest_timestamp"
            )
        )
        
        # Write to Gold Iceberg table
        fact_orders.writeTo("demo.gold.fact_orders").createOrReplace()
        print(f"✅ Processed {fact_orders.count()} order fact records")
    
    def process_fact_order_items(self):
        """Process order items fact table"""
        print("🛒 Processing Fact Order Items...")
        
        # Read from Silver and explode items
        silver_orders = self.spark.table("demo.silver.silver_orders")
        fact_orders = self.spark.table("demo.gold.fact_orders")
        dim_menu_items = self.spark.table("demo.gold.dim_menu_items")
        
        # Explode order items
        order_items_exploded = (
            silver_orders
            .select("order_id", explode("items").alias("item"))
            .select(
                "order_id",
                col("item.item_id"),
                col("item.quantity"),
                col("item.unit_price"),
                col("item.total_price").alias("extended_price")
            )
        )
        
        # Join with fact orders and menu items
        fact_order_items = (
            order_items_exploded
            .join(fact_orders.select("order_id", "order_key", "date_key"), "order_id", "inner")
            .join(dim_menu_items.select("item_id", "menu_item_key"), "item_id", "inner")
            .withColumn("order_item_key", monotonically_increasing_id() + 1)
            .select(
                "order_item_key",
                "order_key",
                "menu_item_key",
                "quantity",
                "extended_price",
                "date_key"
            )
        )
        
        # Write to Gold Iceberg table
        fact_order_items.writeTo("demo.gold.fact_order_items").createOrReplace()
        print(f"✅ Processed {fact_order_items.count()} order item fact records")
    
    def generate_business_metrics(self):
        """Generate business metrics and KPIs"""
        print("📊 Generating business metrics...")
        
        # Daily order metrics
        daily_metrics = (
            self.spark.table("demo.gold.fact_orders")
            .groupBy("date_key")
            .agg(
                count("order_key").alias("total_orders"),
                sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                avg("delivery_minutes").alias("avg_delivery_time"),
                (sum(when(col("sla_breached"), 1).otherwise(0)) / count("*") * 100).alias("sla_breach_rate")
            )
            .orderBy("date_key")
        )
        
        print("📈 Daily Metrics:")
        daily_metrics.show(10, False)
        
        # Restaurant performance
        restaurant_metrics = (
            self.spark.table("demo.gold.fact_orders")
            .join(self.spark.table("demo.gold.dim_restaurants").filter(col("is_current") == True), "restaurant_key")
            .groupBy("restaurant_id", "restaurant_name")
            .agg(
                count("order_key").alias("total_orders"),
                sum("total_amount").alias("total_revenue"),
                avg("delivery_minutes").alias("avg_delivery_time")
            )
            .orderBy(desc("total_revenue"))
        )
        
        print("🏪 Restaurant Performance:")
        restaurant_metrics.show(10, False)
        
        return daily_metrics, restaurant_metrics
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

if __name__ == "__main__":
    gold_processing = GoldProcessing()
    
    try:
        # Process all Gold tables
        gold_processing.process_all_gold_tables()
        
        # Generate business metrics
        gold_processing.generate_business_metrics()
        
        print("🎯 Gold layer processing completed successfully!")
        
    except Exception as e:
        print(f"❌ Error in Gold processing: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        gold_processing.stop() 