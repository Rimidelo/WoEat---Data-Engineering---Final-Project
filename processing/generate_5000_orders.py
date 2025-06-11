from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

def round_float(val, decimals):
    """Safe round function to avoid conflict with Spark functions"""
    return float(format(val, f'.{decimals}f'))

class OrdersGenerator:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("WoEat - 5000 Orders Generator")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config("spark.sql.catalog.demo.s3.access-key-id", "admin")
            .config("spark.sql.catalog.demo.s3.secret-access-key", "password")
            .getOrCreate()
        )
    
    def generate_5000_orders(self):
        """Generate 5000 orders with normalized structure"""
        print("🛍️ Generating 5000 orders with realistic distribution...")
        
        drivers = [f"driver_{i:03d}" for i in range(1, 201)]
        restaurants = [f"rest_{i:03d}" for i in range(1, 16)]
        items = [f"item_{i:04d}" for i in range(1, 301)]
        
        orders_data = []
        order_items_data = []
        ratings_data = []
        base_time = datetime.now() - timedelta(days=30)  # 30 days of order history
        
        print("📅 Generating orders across 30-day period...")
        
        for i in range(1, 5001):  # 5000 orders
            if i % 500 == 0:
                print(f"   ✨ Generated {i} orders...")
            
            # More realistic time distribution (peak hours)
            day_offset = random.randint(0, 29)
            
            # Peak hours: 12-14 (lunch), 18-21 (dinner)
            if random.random() < 0.6:  # 60% during peak hours
                if random.random() < 0.5:
                    hour = random.randint(12, 14)  # Lunch
                else:
                    hour = random.randint(18, 21)  # Dinner
            else:
                hour = random.randint(9, 23)  # Regular hours
            
            order_time = base_time + timedelta(
                days=day_offset,
                hours=hour,
                minutes=random.randint(0, 59)
            )
            
            # Generate order items (1-5 items per order, weighted toward 1-2 items)
            item_weights = [0.4, 0.3, 0.2, 0.07, 0.03]  # Favor smaller orders
            num_items = random.choices(range(1, 6), weights=item_weights)[0]
            
            total_amount = 0
            selected_restaurant = random.choice(restaurants)
            selected_driver = random.choice(drivers)
            order_id = f"order_{i:06d}"
            
            # Generate individual order items
            for item_idx in range(num_items):
                quantity = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
                unit_price = round_float(random.uniform(8.0, 35.0), 2)
                
                order_item = {
                    "order_item_id": f"{order_id}_item_{item_idx+1}",
                    "order_id": order_id,
                    "item_id": random.choice(items),
                    "quantity": quantity,
                    "item_price": unit_price,
                    "order_time": order_time
                }
                order_items_data.append(order_item)
                total_amount += quantity * unit_price
            
            # More realistic delivery times based on order size and time
            base_prep_time = 15 + (num_items * 5)  # More items = longer prep
            prep_time = base_prep_time + random.randint(-5, 15)
            prep_start_time = order_time + timedelta(minutes=random.randint(2, 8))
            prep_end_time = prep_start_time + timedelta(minutes=prep_time if prep_time > 10 else 10)
            
            # Rush hour penalties
            if hour in [12, 13, 19, 20]:
                delivery_time_minutes = random.randint(25, 75)  # Longer during rush
            else:
                delivery_time_minutes = random.randint(15, 45)  # Faster during off-peak
            
            delivery_time = order_time + timedelta(minutes=delivery_time_minutes)
            
            # Weekend vs weekday patterns
            weekday = (base_time + timedelta(days=day_offset)).weekday()
            is_weekend = weekday >= 5
            
            # Higher tips on weekends and dinner times
            if is_weekend or hour >= 18:
                tip_amount = round_float(random.uniform(3.0, 12.0), 2)
            else:
                tip_amount = round_float(random.uniform(1.0, 8.0), 2)
            
            # Determine order status
            status = random.choices(
                ["delivered", "cancelled"], 
                weights=[0.88, 0.12]  # 88% delivered, 12% cancelled
            )[0]
            
            order = {
                "order_id": order_id,
                "customer_id": f"customer_{random.randint(1, 1500):04d}",
                "restaurant_id": selected_restaurant,
                "driver_id": selected_driver,
                "order_time": order_time,
                "status": status,
                "delivery_time": delivery_time if status == "delivered" else None,
                "total_amount": round_float(total_amount, 2),
                "prep_start_time": prep_start_time,
                "prep_end_time": prep_end_time,
                "tip_amount": tip_amount
            }
            orders_data.append(order)
            
            # Generate ratings for completed orders (70% of delivered orders get rated)
            if status == "delivered" and random.random() < 0.7:
                rating_time = delivery_time + timedelta(minutes=random.randint(10, 120))
                
                # Generate realistic ratings (skewed toward higher ratings)
                driver_rating = random.choices([3, 4, 5], weights=[0.1, 0.3, 0.6])[0] + random.uniform(-0.5, 0.5)
                food_rating = random.choices([3, 4, 5], weights=[0.15, 0.35, 0.5])[0] + random.uniform(-0.5, 0.5)
                delivery_rating = random.choices([3, 4, 5], weights=[0.1, 0.3, 0.6])[0] + random.uniform(-0.5, 0.5)
                
                # Clamp ratings to valid range
                driver_rating = 1.0 if driver_rating < 1.0 else (5.0 if driver_rating > 5.0 else driver_rating)
                food_rating = 1.0 if food_rating < 1.0 else (5.0 if food_rating > 5.0 else food_rating)
                delivery_rating = 1.0 if delivery_rating < 1.0 else (5.0 if delivery_rating > 5.0 else delivery_rating)
                
                rating = {
                    "rating_id": f"rating_{i:06d}",
                    "order_id": order_id,
                    "driver_id": selected_driver,
                    "restaurant_id": selected_restaurant,
                    "driver_rating": round_float(driver_rating, 1),
                    "food_rating": round_float(food_rating, 1),
                    "delivery_rating": round_float(delivery_rating, 1),
                    "rating_time": rating_time,
                    "rating_type": "order_completion"
                }
                ratings_data.append(rating)
        
        print("📦 Creating Spark DataFrames...")
        
        # Create orders DataFrame
        orders_schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("restaurant_id", StringType(), False),
            StructField("driver_id", StringType(), False),
            StructField("order_time", TimestampType(), False),
            StructField("status", StringType(), False),
            StructField("delivery_time", TimestampType(), True),
            StructField("total_amount", DoubleType(), False),
            StructField("prep_start_time", TimestampType(), True),
            StructField("prep_end_time", TimestampType(), True),
            StructField("tip_amount", DoubleType(), False)
        ])
        
        orders_df = self.spark.createDataFrame(orders_data, orders_schema)
        
        # Create order items DataFrame
        order_items_schema = StructType([
            StructField("order_item_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("item_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("item_price", DoubleType(), False),
            StructField("order_time", TimestampType(), False)
        ])
        
        order_items_df = self.spark.createDataFrame(order_items_data, order_items_schema)
        
        # Create ratings DataFrame
        ratings_schema = StructType([
            StructField("rating_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("driver_id", StringType(), False),
            StructField("restaurant_id", StringType(), False),
            StructField("driver_rating", DoubleType(), False),
            StructField("food_rating", DoubleType(), False),
            StructField("delivery_rating", DoubleType(), False),
            StructField("rating_time", TimestampType(), False),
            StructField("rating_type", StringType(), False)
        ])
        
        ratings_df = self.spark.createDataFrame(ratings_data, ratings_schema)
        
        print("💾 Saving to Bronze tables...")
        
        # Save orders
        orders_df.writeTo("demo.bronze.bronze_orders").createOrReplace()
        orders_count = orders_df.count()
        print(f"✅ Created bronze_orders with {orders_count} records")
        
        # Save order items
        order_items_df.writeTo("demo.bronze.bronze_order_items").createOrReplace()
        items_count = order_items_df.count()
        print(f"✅ Created bronze_order_items with {items_count} records")
        
        # Save ratings
        ratings_df.writeTo("demo.bronze.bronze_ratings").createOrReplace()
        ratings_count = ratings_df.count()
        print(f"✅ Created bronze_ratings with {ratings_count} records")
        
        # Show some statistics
        print("\n📊 Order Statistics:")
        orders_df.groupBy("status").count().show()
        
        print("🍽️ Restaurant Order Distribution:")
        orders_df.groupBy("restaurant_id").count().orderBy("count", ascending=False).show(5)
        
        print("⭐ Rating Distribution:")
        ratings_df.select(
            avg("driver_rating").alias("avg_driver_rating"),
            avg("food_rating").alias("avg_food_rating"),
            avg("delivery_rating").alias("avg_delivery_rating")
        ).show()
        
        return orders_count, items_count, ratings_count

if __name__ == "__main__":
    generator = OrdersGenerator()
    orders_count, items_count, ratings_count = generator.generate_5000_orders()
    print(f"\n🎉 Successfully generated:")
    print(f"   📦 {orders_count} orders")
    print(f"   🛒 {items_count} order items")  
    print(f"   ⭐ {ratings_count} ratings")
    print("🚀 Your database now has impressive scale for demo!") 