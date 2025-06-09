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
        """Generate 5000 orders with complex structure"""
        print("🛍️ Generating 5000 orders with realistic distribution...")
        
        drivers = [f"driver_{i:03d}" for i in range(1, 201)]
        restaurants = [f"rest_{i:03d}" for i in range(1, 16)]
        items = [f"item_{i:04d}" for i in range(1, 301)]
        
        orders_data = []
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
            
            order_items = []
            total_amount = 0
            
            selected_restaurant = random.choice(restaurants)
            
            for _ in range(num_items):
                item = {
                    "item_id": random.choice(items),
                    "quantity": random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0],  # Most orders are single items
                    "unit_price": round_float(random.uniform(8.0, 35.0), 2)
                }
                item["total_price"] = round_float(item["quantity"] * item["unit_price"], 2)
                total_amount += item["total_price"]
                order_items.append(item)
            
            # More realistic delivery times based on order size and time
            base_prep_time = 15 + (num_items * 5)  # More items = longer prep
            prep_time = base_prep_time + random.randint(-5, 15)
            
            # Rush hour penalties
            if hour in [12, 13, 19, 20]:
                delivery_time = random.randint(25, 75)  # Longer during rush
            else:
                delivery_time = random.randint(15, 45)  # Faster during off-peak
            
            # Weekend vs weekday patterns
            weekday = (base_time + timedelta(days=day_offset)).weekday()
            is_weekend = weekday >= 5
            
            # Higher tips on weekends and dinner times
            if is_weekend or hour >= 18:
                tip_base = random.uniform(3.0, 12.0)
            else:
                tip_base = random.uniform(1.0, 8.0)
            
            order = {
                "order_id": f"order_{i:06d}",
                "customer_id": f"customer_{random.randint(1, 1500):04d}",
                "restaurant_id": selected_restaurant,
                "driver_id": random.choice(drivers),
                "order_timestamp": order_time,
                "items": order_items,
                "total_amount": round_float(total_amount, 2),
                "delivery_fee": round_float(random.uniform(2.0, 8.0), 2),
                "tip_amount": round_float(tip_base, 2),
                "status": random.choices(
                    ["completed", "cancelled", "refunded"], 
                    weights=[0.85, 0.12, 0.03]  # 85% completed, 12% cancelled, 3% refunded
                )[0],
                "prep_time_minutes": prep_time if prep_time >= 10 else 10,
                "delivery_time_minutes": delivery_time,
                "customer_rating": random.choices(
                    [None, 3, 4, 5], 
                    weights=[0.3, 0.1, 0.3, 0.3]  # 30% no rating, rest distributed
                )[0],
                "delivery_address": f"{random.randint(1, 999)} {random.choice(['Main St', 'Oak Ave', 'Elm St', 'Park Rd', 'First Ave', 'Second St', 'Broadway', 'Market St'])}",
                "payment_method": random.choices(
                    ["credit_card", "digital_wallet", "cash", "debit_card"],
                    weights=[0.45, 0.35, 0.15, 0.05]  # Modern payment distribution
                )[0]
            }
            orders_data.append(order)
        
        print("📦 Creating Spark DataFrame...")
        
        # Define complex schema with nested items
        item_schema = StructType([
            StructField("item_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DoubleType(), False),
            StructField("total_price", DoubleType(), False)
        ])
        
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("restaurant_id", StringType(), False),
            StructField("driver_id", StringType(), False),
            StructField("order_timestamp", TimestampType(), False),
            StructField("items", ArrayType(item_schema), False),
            StructField("total_amount", DoubleType(), False),
            StructField("delivery_fee", DoubleType(), False),
            StructField("tip_amount", DoubleType(), False),
            StructField("status", StringType(), False),
            StructField("prep_time_minutes", IntegerType(), False),
            StructField("delivery_time_minutes", IntegerType(), False),
            StructField("customer_rating", IntegerType(), True),
            StructField("delivery_address", StringType(), False),
            StructField("payment_method", StringType(), False)
        ])
        
        orders_df = (
            self.spark.createDataFrame(orders_data, schema)
            .withColumn("ingest_timestamp", current_timestamp())
        )
        
        print("💾 Saving to Bronze table...")
        orders_df.write.mode("overwrite").saveAsTable("demo.bronze.bronze_orders")
        
        record_count = orders_df.count()
        print(f"✅ Created bronze_orders with {record_count} records")
        
        # Show some statistics
        print("\n📊 Order Statistics:")
        orders_df.groupBy("status").count().show()
        
        print("💳 Payment Method Distribution:")
        orders_df.groupBy("payment_method").count().show()
        
        print("🍽️ Restaurant Order Distribution:")
        orders_df.groupBy("restaurant_id").count().orderBy("count", ascending=False).show(5)
        
        return record_count

if __name__ == "__main__":
    generator = OrdersGenerator()
    count = generator.generate_5000_orders()
    print(f"\n🎉 Successfully generated {count} orders!")
    print("🚀 Your database now has impressive scale for demo!") 