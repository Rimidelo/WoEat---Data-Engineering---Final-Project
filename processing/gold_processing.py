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
        print("Starting Gold layer processing...")
        
        # Process dimension tables first
        self.process_dim_date()
        self.process_dim_drivers_scd2()
        self.process_dim_restaurants_scd2()
        self.process_dim_menu_items()
        
        # Process fact tables
        self.process_fact_orders()
        self.process_fact_order_items()
        self.process_fact_ratings()
        self.process_fact_restaurant_daily()
        self.process_fact_driver_daily()
        self.process_fact_business_summary()
        
        print("Gold layer processing completed")

    def process_dim_date(self):
        """Process date dimension table"""
        print("Processing Dim Date...")
        
        # Generate date range (last 2 years to next 2 years)
        from datetime import datetime, timedelta
        import calendar
        
        start_date = datetime.now() - timedelta(days=730)  # 2 years ago
        end_date = datetime.now() + timedelta(days=730)    # 2 years from now
        
        date_data = []
        current_date = start_date
        date_key = 1
        
        while current_date <= end_date:
            date_data.append((
                date_key,
                current_date.date(),
                current_date.year,
                (current_date.month - 1) // 3 + 1,  # Quarter
                current_date.month,
                current_date.isocalendar()[1],  # Week
                current_date.weekday() + 1,  # Day of week (1-7)
                calendar.month_name[current_date.month],
                calendar.day_name[current_date.weekday()],
                current_date.weekday() >= 5,  # Is weekend
                False  # Is holiday (simplified)
            ))
            current_date += timedelta(days=1)
            date_key += 1
        
        schema = StructType([
            StructField("date_key", IntegerType(), False),
            StructField("full_date", DateType(), False),
            StructField("year", IntegerType(), False),
            StructField("quarter", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("week", IntegerType(), False),
            StructField("day_of_week", IntegerType(), False),
            StructField("month_name", StringType(), False),
            StructField("day_name", StringType(), False),
            StructField("is_weekend", BooleanType(), False),
            StructField("is_holiday", BooleanType(), False)
        ])
        
        dim_date = self.spark.createDataFrame(date_data, schema)
        
        # Write to Gold Iceberg table
        dim_date.writeTo("demo.gold.dim_date").createOrReplace()
        print(f"Processed {dim_date.count()} date dimension records")
    
    def process_dim_drivers_scd2(self):
        """Process drivers dimension with SCD Type 2"""
        print("Processing Dim Drivers (SCD Type 2)...")
        
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
                    "phone",
                    "vehicle_type",
                    "rating",
                    "is_active",
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
                    col("curr.vehicle_type").alias("curr_vehicle_type")
                )
            )
            
            # Identify records that have changed
            changed_records = changes.filter(
                (col("name") != col("curr_name")) |
                (col("vehicle_type") != col("curr_vehicle_type"))
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
                            "driver_key", "driver_id", "name", "phone", "vehicle_type", "rating", "is_active",
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
                            "driver_key", "driver_id", "name", "zone", "created_at",
                            "record_start_date", "record_end_date", "is_current",
                            "ingest_timestamp", "updated_at"
                        )
                    )
                    dim_drivers = dim_drivers.union(new_driver_records)
            else:
                dim_drivers = existing_dim
        
        # Write to Gold Iceberg table
        dim_drivers.writeTo("demo.gold.dim_drivers").createOrReplace()
        print(f"Processed {dim_drivers.count()} driver dimension records")
    
    def process_dim_restaurants_scd2(self):
        """Process restaurants dimension with SCD Type 2"""
        print("Processing Dim Restaurants (SCD Type 2)...")
        
        # Read current Silver data
        silver_restaurants = self.spark.table("demo.silver.silver_restaurants")
        
        # Try to read existing Gold dimension
        try:
            existing_dim = self.spark.table("demo.gold.dim_restaurants")
            has_existing_data = True
        except:
            has_existing_data = False
            existing_dim = None
        
        if not has_existing_data:
            # First load - create initial dimension
            dim_restaurants = (
                silver_restaurants
                .withColumn("restaurant_key", monotonically_increasing_id() + 1)
                .withColumn("record_start_date", current_date())
                .withColumn("record_end_date", lit("9999-12-31").cast(DateType()))
                .withColumn("is_current", lit(True))
                .withColumn("updated_at", current_timestamp())
                .select(
                    "restaurant_key",
                    "restaurant_id",
                    "restaurant_name",
                    "cuisine_type",
                    "address",
                    "phone",
                    "rating",
                    "is_active",
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
                silver_restaurants.alias("new")
                .join(current_records.alias("curr"), 
                     col("new.restaurant_id") == col("curr.restaurant_id"), "left")
                .select(
                    col("new.*"),
                    col("curr.restaurant_key"),
                    col("curr.restaurant_name").alias("curr_name"),
                    col("curr.cuisine_type").alias("curr_cuisine"),
                    col("curr.address").alias("curr_address"),
                    col("curr.is_active").alias("curr_active")
                )
            )
            
            # Identify records that have changed
            changed_records = changes.filter(
                (col("restaurant_name") != col("curr_name")) |
                (col("cuisine_type") != col("curr_cuisine")) |
                (col("address") != col("curr_address")) |
                (col("is_active") != col("curr_active"))
            )
            
            # Identify new records
            new_records = changes.filter(col("restaurant_key").isNull())
            
            if changed_records.count() > 0 or new_records.count() > 0:
                # Close existing records that have changed
                if changed_records.count() > 0:
                    updated_existing = (
                        existing_dim
                        .join(changed_records.select("restaurant_id"), "restaurant_id", "inner")
                        .filter(col("is_current") == True)
                        .withColumn("record_end_date", current_date())
                        .withColumn("is_current", lit(False))
                        .withColumn("updated_at", current_timestamp())
                    )
                    
                    # Keep unchanged existing records
                    unchanged_existing = (
                        existing_dim
                        .join(changed_records.select("restaurant_id"), "restaurant_id", "left_anti")
                    )
                    
                    # Create new versions for changed records
                    max_key = existing_dim.agg(max("restaurant_key")).collect()[0][0]
                    new_versions = (
                        changed_records
                        .withColumn("restaurant_key", monotonically_increasing_id() + max_key + 1)
                        .withColumn("record_start_date", current_date())
                        .withColumn("record_end_date", lit("9999-12-31").cast(DateType()))
                        .withColumn("is_current", lit(True))
                        .withColumn("updated_at", current_timestamp())
                        .select(
                            "restaurant_key", "restaurant_id", "restaurant_name", "cuisine_type",
                            "address", "phone", "rating", "is_active", "record_start_date", 
                            "record_end_date", "is_current", "ingest_timestamp", "updated_at"
                        )
                    )
                    
                    # Combine all records
                    dim_restaurants = unchanged_existing.union(updated_existing).union(new_versions)
                else:
                    dim_restaurants = existing_dim
                
                # Add completely new records
                if new_records.count() > 0:
                    max_key = dim_restaurants.agg(max("restaurant_key")).collect()[0][0] or 0
                    new_restaurant_records = (
                        new_records
                        .withColumn("restaurant_key", monotonically_increasing_id() + max_key + 1)
                        .withColumn("record_start_date", current_date())
                        .withColumn("record_end_date", lit("9999-12-31").cast(DateType()))
                        .withColumn("is_current", lit(True))
                        .withColumn("updated_at", current_timestamp())
                        .select(
                            "restaurant_key", "restaurant_id", "restaurant_name", "cuisine_type",
                            "address", "phone", "rating", "is_active", "record_start_date", 
                            "record_end_date", "is_current", "ingest_timestamp", "updated_at"
                        )
                    )
                    dim_restaurants = dim_restaurants.union(new_restaurant_records)
            else:
                dim_restaurants = existing_dim
        
        # Write to Gold Iceberg table
        dim_restaurants.writeTo("demo.gold.dim_restaurants").createOrReplace()
        print(f"Processed {dim_restaurants.count()} restaurant dimension records")
    
    def process_dim_menu_items(self):
        """Process menu items dimension"""
        print("Processing Dim Menu Items...")
        
        # Read from Silver
        silver_menu_items = self.spark.table("demo.silver.silver_menu_items")
        
        # Create dimension without restaurant association (as per feedback)
        dim_menu_items = (
            silver_menu_items
            .withColumn("menu_item_key", monotonically_increasing_id() + 1)
            .withColumn("active_flag", lit(True))
            .select(
                "menu_item_key",
                "item_id",
                "item_name",
                "category",
                "base_price",
                "active_flag",
                "ingest_timestamp"
            )
        )
        
        # Write to Gold Iceberg table
        dim_menu_items.writeTo("demo.gold.dim_menu_items").createOrReplace()
        print(f"Processed {dim_menu_items.count()} menu item dimension records")
    
    def process_fact_orders(self):
        """Process orders fact table"""
        print("Processing Fact Orders...")
        
        # Read source tables
        silver_orders = self.spark.table("demo.silver.silver_orders")
        dim_drivers = self.spark.table("demo.gold.dim_drivers").filter(col("is_current") == True)
        dim_restaurants = self.spark.table("demo.gold.dim_restaurants").filter(col("is_current") == True)
        dim_date = self.spark.table("demo.gold.dim_date")
        
        # Create fact table with surrogate keys
        fact_orders = (
            silver_orders
            .join(dim_drivers, silver_orders.driver_id == dim_drivers.driver_id, "left")
            .join(dim_restaurants, silver_orders.restaurant_id == dim_restaurants.restaurant_id, "left")
            .join(dim_date, to_date(silver_orders.order_time) == dim_date.full_date, "left")
            .withColumn("order_key", monotonically_increasing_id() + 1)
            .withColumn("delivery_minutes", 
                       when(col("delivery_time_minutes").isNotNull(), 
                            round(col("delivery_time_minutes"), 1))
                       .otherwise(None))
            .withColumn("prep_time_minutes", 
                       when(col("prep_time_minutes").isNotNull(), 
                            round(col("prep_time_minutes"), 1))
                       .otherwise(None))
            .withColumn("sla_breached", 
                       when(col("delivery_time_minutes") > 45, True)
                       .otherwise(False))  # 45 min SLA
            .select(
                col("order_key"),
                silver_orders.order_id,
                col("date_key"),
                col("driver_key"),
                col("restaurant_key"),
                silver_orders.order_time,
                silver_orders.delivery_time,
                silver_orders.status,
                silver_orders.total_amount,
                col("delivery_minutes"),
                col("prep_time_minutes"),
                col("sla_breached"),
                silver_orders.cancelled,
                silver_orders.tip_amount,
                silver_orders.ingest_timestamp
            )
        )
        
        # Write to Gold Iceberg table
        fact_orders.writeTo("demo.gold.fact_orders").createOrReplace()
        print(f"Processed {fact_orders.count()} order fact records")
    
    def process_fact_order_items(self):
        """Process order items fact table"""
        print("Processing Fact Order Items...")
        
        # Read source tables
        silver_order_items = self.spark.table("demo.silver.silver_order_items")
        fact_orders = self.spark.table("demo.gold.fact_orders")
        dim_menu_items = self.spark.table("demo.gold.dim_menu_items")
        dim_date = self.spark.table("demo.gold.dim_date")
        
        # Create fact table with surrogate keys
        fact_order_items = (
            silver_order_items
            .join(fact_orders, silver_order_items.order_id == fact_orders.order_id, "inner")
            .join(dim_menu_items, silver_order_items.item_id == dim_menu_items.item_id, "left")
            .join(dim_date, to_date(silver_order_items.order_time) == dim_date.full_date, "left")
            .withColumn("order_item_key", monotonically_increasing_id() + 1)
            .select(
                col("order_item_key"),
                fact_orders.order_key,
                col("menu_item_key"),
                silver_order_items.quantity,
                silver_order_items.item_price,
                silver_order_items.extended_price,
                dim_date.date_key,
                silver_order_items.order_time
            )
        )
        
        # Write to Gold Iceberg table
        fact_order_items.writeTo("demo.gold.fact_order_items").createOrReplace()
        print(f"Processed {fact_order_items.count()} order item fact records")

    def process_fact_ratings(self):
        """Process ratings fact table"""
        print("⭐ Processing Fact Ratings...")
        
        # Read source tables
        silver_ratings = self.spark.table("demo.silver.silver_ratings")
        fact_orders = self.spark.table("demo.gold.fact_orders")
        dim_drivers = self.spark.table("demo.gold.dim_drivers").filter(col("is_current") == True)
        dim_restaurants = self.spark.table("demo.gold.dim_restaurants").filter(col("is_current") == True)
        dim_date = self.spark.table("demo.gold.dim_date")
        
        # Create fact table with surrogate keys
        fact_ratings = (
            silver_ratings
            .join(fact_orders, silver_ratings.order_id == fact_orders.order_id, "left")
            .join(dim_drivers, silver_ratings.driver_id == dim_drivers.driver_id, "left")
            .join(dim_restaurants, silver_ratings.restaurant_id == dim_restaurants.restaurant_id, "left")
            .join(dim_date, to_date(silver_ratings.rating_time) == dim_date.full_date, "left")
            .withColumn("rating_key", monotonically_increasing_id() + 1)
            .select(
                col("rating_key"),
                fact_orders.order_key,
                dim_drivers.driver_key,
                dim_restaurants.restaurant_key,
                silver_ratings.driver_rating,
                silver_ratings.food_rating,
                silver_ratings.delivery_rating,
                silver_ratings.rating_time,
                silver_ratings.rating_type,
                dim_date.date_key
            )
        )
        
        # Write to Gold Iceberg table
        fact_ratings.writeTo("demo.gold.fact_ratings").createOrReplace()
        print(f"Processed {fact_ratings.count()} rating fact records")

    def process_fact_restaurant_daily(self):
        """Process restaurant daily performance fact table"""
        print("Processing Fact Restaurant Daily...")
        
        # Read source tables
        silver_performance = self.spark.table("demo.silver.silver_restaurant_performance")
        dim_restaurants = self.spark.table("demo.gold.dim_restaurants").filter(col("is_current") == True)
        dim_date = self.spark.table("demo.gold.dim_date")
        
        # Create fact table with surrogate keys
        fact_restaurant_daily = (
            silver_performance
            .join(dim_restaurants, silver_performance.restaurant_id == dim_restaurants.restaurant_id, "inner")
            .join(dim_date, silver_performance.report_date == dim_date.full_date, "inner")
            .withColumn("restaurant_daily_key", monotonically_increasing_id() + 1)
            .withColumn("active_menu_items", lit(5))  # Simplified - could be calculated
            .select(
                col("restaurant_daily_key"),
                dim_restaurants.restaurant_key,
                dim_date.date_key,
                silver_performance.avg_prep_time,
                silver_performance.avg_rating,
                silver_performance.orders_count,
                silver_performance.cancel_rate,
                silver_performance.avg_tip,
                silver_performance.total_revenue,
                col("active_menu_items"),
                silver_performance.ingest_timestamp
            )
        )
        
        # Write to Gold Iceberg table
        fact_restaurant_daily.writeTo("demo.gold.fact_restaurant_daily").createOrReplace()
        print(f"Processed {fact_restaurant_daily.count()} restaurant daily fact records")

    def process_fact_driver_daily(self):
        """Process driver daily performance fact table"""
        print("Processing Fact Driver Daily...")
        
        # Read source tables
        silver_performance = self.spark.table("demo.silver.silver_driver_performance")
        dim_drivers = self.spark.table("demo.gold.dim_drivers").filter(col("is_current") == True)
        dim_date = self.spark.table("demo.gold.dim_date")
        
        # Create fact table with surrogate keys
        fact_driver_daily = (
            silver_performance
            .join(dim_drivers, silver_performance.driver_id == dim_drivers.driver_id, "inner")
            .join(dim_date, silver_performance.report_date == dim_date.full_date, "inner")
            .withColumn("driver_daily_key", monotonically_increasing_id() + 1)
            .withColumn("total_earnings", col("total_tips") * 1.2)  # Simplified calculation
            .withColumn("hours_worked", col("orders_completed") * 0.75)  # Simplified calculation
            .select(
                col("driver_daily_key"),
                dim_drivers.driver_key,
                dim_date.date_key,
                silver_performance.avg_rating,
                silver_performance.orders_completed,
                silver_performance.avg_delivery_time,
                silver_performance.total_tips,
                col("total_earnings"),
                col("hours_worked"),
                silver_performance.ingest_timestamp
            )
        )
        
        # Write to Gold Iceberg table
        fact_driver_daily.writeTo("demo.gold.fact_driver_daily").createOrReplace()
        print(f"Processed {fact_driver_daily.count()} driver daily fact records")

    def process_fact_business_summary(self):
        """Process business summary fact table"""
        print("Processing Fact Business Summary...")
        
        # Read source tables
        fact_orders = self.spark.table("demo.gold.fact_orders")
        fact_ratings = self.spark.table("demo.gold.fact_ratings")
        dim_date = self.spark.table("demo.gold.dim_date")
        dim_drivers = self.spark.table("demo.gold.dim_drivers").filter(col("is_current") == True)
        dim_restaurants = self.spark.table("demo.gold.dim_restaurants").filter(col("is_current") == True)
        
        # Create daily business summary using aliases
        daily_summary = (
            fact_orders.alias("fo")
            .join(dim_date.alias("dd"), col("fo.date_key") == col("dd.date_key"), "inner")
            .groupBy(col("dd.date_key"), col("dd.full_date"))
            .agg(
                count("order_key").alias("total_orders"),
                sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                avg("delivery_minutes").alias("avg_delivery_time"),
                sum("tip_amount").alias("total_tips")
            )
        )
        
        # Add ratings summary using aliases
        ratings_summary = (
            fact_ratings.alias("fr")
            .join(dim_date.alias("dd2"), col("fr.date_key") == col("dd2.date_key"), "inner")
            .groupBy(col("dd2.date_key"))
            .agg(
                avg((col("driver_rating") + col("food_rating") + col("delivery_rating")) / 3.0).alias("overall_satisfaction")
            )
        )
        
        # Count active entities (simplified - could be more dynamic)
        active_drivers_count = dim_drivers.count()
        active_restaurants_count = dim_restaurants.count()
        
        # Combine summaries
        fact_business_summary = (
            daily_summary.alias("ds")
            .join(ratings_summary.alias("rs"), col("ds.date_key") == col("rs.date_key"), "left")
            .withColumn("summary_key", monotonically_increasing_id() + 1)
            .withColumn("time_period", lit("daily"))
            .withColumn("active_drivers", lit(active_drivers_count))
            .withColumn("active_restaurants", lit(active_restaurants_count))
            .withColumn("overall_satisfaction", 
                       when(col("overall_satisfaction").isNull(), 4.0)
                       .otherwise(round(col("overall_satisfaction"), 2)))
            .withColumn("total_revenue", round(col("total_revenue"), 2))
            .withColumn("avg_order_value", round(col("avg_order_value"), 2))
            .withColumn("avg_delivery_time", round(col("avg_delivery_time"), 1))
            .withColumn("total_tips", round(col("total_tips"), 2))
            .withColumn("ingest_timestamp", current_timestamp())
            .select(
                col("summary_key"),
                col("ds.date_key"),
                col("time_period"),
                col("total_orders"),
                col("total_revenue"),
                col("avg_order_value"),
                col("active_drivers"),
                col("active_restaurants"),
                col("overall_satisfaction"),
                col("avg_delivery_time"),
                col("total_tips"),
                col("ingest_timestamp")
            )
        )
        
        # Write to Gold Iceberg table
        fact_business_summary.writeTo("demo.gold.fact_business_summary").createOrReplace()
        print(f"Processed {fact_business_summary.count()} business summary fact records")
    
    def process_streaming_gold_tables(self):
        """Process streaming data through Gold layer"""
        print("Processing streaming data through Gold layer...")
        
        try:
            # Process streaming fact tables
            self.process_fact_orders_streaming()
            
            # Update dimension tables with streaming data
            self.update_dimensions_from_streaming()
            
            print("Streaming Gold processing completed")
            
        except Exception as e:
            print(f"Streaming Gold processing failed: {e}")
            raise
    
    def process_fact_orders_streaming(self):
        """Create fact_orders from streaming Silver data"""
        print("Processing streaming orders to fact table...")
        
        # Read streaming Silver data
        silver_orders = self.spark.table("demo.silver.silver_orders")
        restaurants = self.spark.table("demo.gold.dim_restaurants")
        drivers = self.spark.table("demo.gold.dim_drivers")
        
        # Create fact table with streaming data
        fact_orders_streaming = (
            silver_orders
            .filter(col("processing_timestamp") >= current_timestamp() - expr("INTERVAL 1 DAY"))
            .join(restaurants.filter(col("is_current") == True), "restaurant_id", "left")
            .join(drivers.filter(col("is_current") == True), "driver_id", "left")
            .select(
                monotonically_increasing_id().alias("order_key"),
                col("order_id"),
                col("customer_id"),
                coalesce(col("restaurant_key"), lit(-1)).alias("restaurant_key"),
                coalesce(col("driver_key"), lit(-1)).alias("driver_key"),
                to_date(col("order_time")).alias("order_date"),
                col("order_time"),
                col("delivery_time"),
                col("total_amount"),
                col("tip_amount"),
                col("delivery_time_minutes").alias("delivery_minutes"),
                col("prep_time_minutes").alias("prep_minutes"),
                col("cancelled"),
                col("status"),
                col("data_quality_score"),
                current_timestamp().alias("created_timestamp")
            )
        )
        
        # Write streaming data to Gold fact table
        (fact_orders_streaming
         .write
         .format("iceberg")
         .mode("append")
         .saveAsTable("demo.gold.fact_orders"))
        
        print("Streaming fact_orders processed")
    
    def update_dimensions_from_streaming(self):
        """Update dimension tables with any new data from streaming"""
        print("Updating dimensions from streaming data...")
        
        # For streaming, dimensions are mostly reference data
        # but we can update customer activity timestamps
        self._update_customer_activity_streaming()
        
        print("Dimension updates from streaming completed")
    
    def _update_customer_activity_streaming(self):
        """Update customer last order timestamp from streaming data"""
        print("👥 Updating customer activity from streaming...")
        
        # Get recent customer activity from streaming Silver data
        recent_customers = (
            self.spark.table("demo.silver.silver_orders")
            .filter(col("processing_timestamp") >= current_timestamp() - expr("INTERVAL 1 DAY"))
            .groupBy("customer_id")
            .agg(
                max("order_time").alias("last_order_time"),
                count("*").alias("recent_order_count")
            )
        )
        
        # This would typically be a merge operation in production
        # For now, we'll just track it as new records
        customer_updates = (
            recent_customers
            .select(
                col("customer_id"),
                col("last_order_time"),
                col("recent_order_count"),
                current_timestamp().alias("updated_timestamp")
            )
        )
        
        # Write customer activity updates
        (customer_updates
         .write
         .format("iceberg")
         .mode("append")
         .saveAsTable("demo.gold.customer_activity_streaming"))
        
        print("Customer activity updated from streaming")
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            print("Gold processing stopped")

if __name__ == "__main__":
    gold_processor = GoldProcessing()
    try:
        gold_processor.process_all_gold_tables()
        print("Gold layer processing completed successfully!")
    except Exception as e:
        print(f"Gold processing failed: {str(e)}")
    finally:
        gold_processor.stop() 