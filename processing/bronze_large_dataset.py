from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import math

def round_float(val, decimals):
    """Safe round function to avoid conflict with Spark functions"""
    return float(format(val, f'.{decimals}f'))

class LargeDatasetGenerator:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("WoEat - Large Dataset Generator")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config("spark.sql.catalog.demo.s3.access-key-id", "admin")
            .config("spark.sql.catalog.demo.s3.secret-access-key", "password")
            .getOrCreate()
        )
    
    def generate_drivers(self):
        """Generate 200 drivers"""
        print("🚗 Generating 200 drivers...")
        
        zones = ["North", "South", "East", "West", "Central", "Downtown", "Airport", "University"]
        first_names = ["Ahmed", "Sarah", "Mohammed", "Fatima", "Ali", "Nour", "Omar", "Layla", "Hassan", "Mona",
                      "Youssef", "Dina", "Khaled", "Rana", "Tamer", "Heba", "Amr", "Yasmin", "Karim", "Salma"]
        last_names = ["Hassan", "Ahmed", "Mohamed", "Ali", "Ibrahim", "Mahmoud", "Youssef", "Mostafa", "Abdel",
                     "Salem", "Farouk", "Nasser", "Rashid", "Mansour", "Gamal", "Saeed", "Fouad", "Zaki"]
        
        drivers_data = []
        for i in range(1, 201):  # 200 drivers
            driver = {
                "driver_id": f"driver_{i:03d}",
                "name": f"{random.choice(first_names)} {random.choice(last_names)}",
                "phone": f"+20{random.randint(1000000000, 1999999999)}",
                "zone": random.choice(zones),
                "rating": round_float(random.uniform(3.5, 5.0), 2),
                "vehicle_type": random.choice(["Car", "Motorcycle", "Bicycle"]),
                "license_plate": f"{random.choice(['ABC', 'XYZ', 'DEF'])}-{random.randint(1000, 9999)}",
                "registration_date": (datetime.now() - timedelta(days=random.randint(30, 730))).date()
            }
            drivers_data.append(driver)
        
        schema = StructType([
            StructField("driver_id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("zone", StringType(), False),
            StructField("rating", DoubleType(), False),
            StructField("vehicle_type", StringType(), False),
            StructField("license_plate", StringType(), False),
            StructField("registration_date", DateType(), False)
        ])
        
        drivers_df = (
            self.spark.createDataFrame(drivers_data, schema)
            .withColumn("ingest_timestamp", current_timestamp())
        )
        
        drivers_df.write.mode("overwrite").saveAsTable("demo.bronze.bronze_drivers")
        print(f"✅ Created bronze_drivers with {drivers_df.count()} records")
    
    def generate_menu_items(self):
        """Generate 300 menu items across 15 restaurants"""
        print("🍕 Generating 300 menu items for 15 restaurants...")
        
        restaurants = [f"rest_{i:03d}" for i in range(1, 16)]  # 15 restaurants
        categories = ["Pizza", "Burger", "Pasta", "Sushi", "Chinese", "Mexican", "Indian", "Lebanese", "Salads", "Desserts"]
        
        # Food items by category
        items_by_category = {
            "Pizza": ["Margherita", "Pepperoni", "Vegetarian", "BBQ Chicken", "Four Cheese", "Hawaiian"],
            "Burger": ["Classic Beef", "Chicken Burger", "Veggie Burger", "Cheese Burger", "BBQ Burger", "Fish Burger"],
            "Pasta": ["Spaghetti Bolognese", "Fettuccine Alfredo", "Penne Arrabbiata", "Lasagna", "Carbonara"],
            "Sushi": ["California Roll", "Salmon Sashimi", "Tuna Roll", "Tempura Roll", "Dragon Roll"],
            "Chinese": ["Sweet & Sour Chicken", "Kung Pao Chicken", "Fried Rice", "Chow Mein", "Spring Rolls"],
            "Mexican": ["Tacos", "Burrito", "Quesadilla", "Nachos", "Fajitas"],
            "Indian": ["Butter Chicken", "Biryani", "Dal", "Naan", "Samosa"],
            "Lebanese": ["Hummus", "Shawarma", "Tabbouleh", "Fattoush", "Manakish"],
            "Salads": ["Caesar Salad", "Greek Salad", "Garden Salad", "Quinoa Salad"],
            "Desserts": ["Chocolate Cake", "Tiramisu", "Ice Cream", "Cheesecake"]
        }
        
        menu_data = []
        item_id = 1
        
        for restaurant in restaurants:
            # Each restaurant has 20 items on average
            items_count = random.randint(18, 22)
            for _ in range(items_count):
                category = random.choice(categories)
                item_name = random.choice(items_by_category[category])
                
                menu_item = {
                    "item_id": f"item_{item_id:04d}",
                    "restaurant_id": restaurant,
                    "item_name": f"{item_name} - {restaurant.split('_')[1]}",
                    "category": category,
                    "price": round_float(random.uniform(5.0, 50.0), 2),
                    "description": f"Delicious {item_name.lower()} from {restaurant}",
                    "is_available": random.choice([True, True, True, False]),  # 75% available
                    "preparation_time": random.randint(10, 45),
                    "calories": random.randint(200, 800),
                    "is_vegetarian": random.choice([True, False])
                }
                menu_data.append(menu_item)
                item_id += 1
        
        schema = StructType([
            StructField("item_id", StringType(), False),
            StructField("restaurant_id", StringType(), False),
            StructField("item_name", StringType(), False),
            StructField("category", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("description", StringType(), True),
            StructField("is_available", BooleanType(), False),
            StructField("preparation_time", IntegerType(), False),
            StructField("calories", IntegerType(), True),
            StructField("is_vegetarian", BooleanType(), False)
        ])
        
        menu_df = (
            self.spark.createDataFrame(menu_data, schema)
            .withColumn("ingest_timestamp", current_timestamp())
        )
        
        menu_df.write.mode("overwrite").saveAsTable("demo.bronze.bronze_menu_items")
        print(f"✅ Created bronze_menu_items with {menu_df.count()} records")
    
    def generate_restaurant_performance(self):
        """Generate 300 restaurant performance records (30 days × 10 restaurants)"""
        print("📊 Generating 300 restaurant performance records...")
        
        restaurants = [f"rest_{i:03d}" for i in range(1, 11)]  # 10 main restaurants
        base_date = datetime.now() - timedelta(days=30)
        
        performance_data = []
        
        for restaurant in restaurants:
            for day in range(30):  # 30 days
                report_date = (base_date + timedelta(days=day)).date()
                
                # Simulate business patterns
                is_weekend = (base_date + timedelta(days=day)).weekday() >= 5
                base_orders = random.randint(80, 120) if is_weekend else random.randint(50, 90)
                
                performance = {
                    "report_date": report_date,
                    "restaurant_id": restaurant,
                    "avg_prep_time": round_float(random.uniform(15.0, 40.0), 1),
                    "avg_rating": round_float(random.uniform(3.5, 5.0), 2),
                    "orders_count": base_orders + random.randint(-10, 20),
                    "cancel_rate": round_float(random.uniform(0.02, 0.15), 3),
                    "avg_tip": round_float(random.uniform(2.0, 8.0), 2),
                    "revenue": round_float(random.uniform(500.0, 2000.0), 2),
                    "peak_hour": random.choice(["12:00", "13:00", "19:00", "20:00", "21:00"])
                }
                performance_data.append(performance)
        
        schema = StructType([
            StructField("report_date", DateType(), False),
            StructField("restaurant_id", StringType(), False),
            StructField("avg_prep_time", DoubleType(), False),
            StructField("avg_rating", DoubleType(), False),
            StructField("orders_count", IntegerType(), False),
            StructField("cancel_rate", DoubleType(), False),
            StructField("avg_tip", DoubleType(), False),
            StructField("revenue", DoubleType(), False),
            StructField("peak_hour", StringType(), False)
        ])
        
        performance_df = (
            self.spark.createDataFrame(performance_data, schema)
            .withColumn("ingest_timestamp", current_timestamp())
            .withColumn("is_late_arrival", lit(False))
            .withColumn("days_late", lit(0))
        )
        
        performance_df.write.mode("overwrite").saveAsTable("demo.bronze.bronze_restaurant_performance")
        print(f"✅ Created bronze_restaurant_performance with {performance_df.count()} records")
    
    def generate_weather_data(self):
        """Generate 100 weather records (5 zones × 20 hourly records)"""
        print("🌤️ Generating 100 weather records...")
        
        zones = ["North", "South", "East", "West", "Central"]
        weather_conditions = ["Sunny", "Cloudy", "Rainy", "Partly Cloudy", "Windy"]
        
        weather_data = []
        base_time = datetime.now() - timedelta(hours=24)
        
        for zone in zones:
            for hour in range(20):  # 20 hourly records per zone
                timestamp = base_time + timedelta(hours=hour)
                
                # Simulate realistic weather patterns
                base_temp = random.randint(15, 35)
                condition = random.choice(weather_conditions)
                
                if condition == "Rainy":
                    humidity = random.randint(70, 90)
                    wind_speed = random.randint(15, 30)
                elif condition == "Sunny":
                    humidity = random.randint(30, 50)
                    wind_speed = random.randint(5, 15)
                else:
                    humidity = random.randint(50, 70)
                    wind_speed = random.randint(10, 20)
                
                weather = {
                    "timestamp": timestamp,
                    "zone": zone,
                    "temperature": base_temp + random.randint(-3, 3),
                    "humidity": humidity,
                    "wind_speed": wind_speed,
                    "condition": condition,
                    "visibility": round_float(random.uniform(5.0, 15.0), 1),
                    "pressure": round_float(random.uniform(1010.0, 1025.0), 1)
                }
                weather_data.append(weather)
        
        schema = StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("zone", StringType(), False),
            StructField("temperature", IntegerType(), False),
            StructField("humidity", IntegerType(), False),
            StructField("wind_speed", IntegerType(), False),
            StructField("condition", StringType(), False),
            StructField("visibility", DoubleType(), False),
            StructField("pressure", DoubleType(), False)
        ])
        
        weather_df = (
            self.spark.createDataFrame(weather_data, schema)
            .withColumn("ingest_timestamp", current_timestamp())
        )
        
        weather_df.write.mode("overwrite").saveAsTable("demo.bronze.bronze_weather")
        print(f"✅ Created bronze_weather with {weather_df.count()} records")
    
    def generate_orders(self):
        """Generate 5000 orders with complex structure"""
        print("🛍️ Generating 5000 orders...")
        
        drivers = [f"driver_{i:03d}" for i in range(1, 201)]
        restaurants = [f"rest_{i:03d}" for i in range(1, 16)]
        items = [f"item_{i:04d}" for i in range(1, 301)]
        
        orders_data = []
        base_time = datetime.now() - timedelta(days=30)  # Extended to 30 days for more realistic spread
        
        for i in range(1, 5001):  # 5000 orders
            order_time = base_time + timedelta(
                hours=random.randint(0, 720),  # Within last 30 days (30*24=720 hours)
                minutes=random.randint(0, 59)
            )
            
            # Generate order items (1-5 items per order)
            num_items = random.randint(1, 5)
            order_items = []
            total_amount = 0
            
            selected_restaurant = random.choice(restaurants)
            
            for _ in range(num_items):
                item = {
                    "item_id": random.choice(items),
                    "quantity": random.randint(1, 3),
                    "unit_price": round_float(random.uniform(8.0, 35.0), 2)
                }
                item["total_price"] = round_float(item["quantity"] * item["unit_price"], 2)
                total_amount += item["total_price"]
                order_items.append(item)
            
            # Calculate times
            prep_time = random.randint(15, 45)
            delivery_time = random.randint(20, 60)
            
            order = {
                "order_id": f"order_{i:06d}",
                "customer_id": f"customer_{random.randint(1, 1500):04d}",  # More customers for 5000 orders
                "restaurant_id": selected_restaurant,
                "driver_id": random.choice(drivers),
                "order_timestamp": order_time,
                "items": order_items,
                "total_amount": round_float(total_amount, 2),
                "delivery_fee": round_float(random.uniform(2.0, 8.0), 2),
                "tip_amount": round_float(random.uniform(0.0, 15.0), 2),
                "status": random.choice(["completed", "completed", "completed", "cancelled"]),  # 75% completed
                "prep_time_minutes": prep_time,
                "delivery_time_minutes": delivery_time,
                "customer_rating": random.randint(3, 5) if random.random() > 0.2 else None,
                "delivery_address": f"{random.randint(1, 999)} {random.choice(['Main St', 'Oak Ave', 'Elm St', 'Park Rd'])}",
                "payment_method": random.choice(["credit_card", "cash", "digital_wallet"])
            }
            orders_data.append(order)
        
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
        
        orders_df.write.mode("overwrite").saveAsTable("demo.bronze.bronze_orders")
        print(f"✅ Created bronze_orders with {orders_df.count()} records")
    
    def run_all(self):
        """Generate all large datasets"""
        print("🚀 Starting Large Dataset Generation...")
        print("=" * 60)
        
        self.generate_drivers()          # 200 records
        self.generate_menu_items()       # 300 records  
        self.generate_restaurant_performance()  # 300 records
        self.generate_weather_data()     # 100 records
        self.generate_orders()           # 5000 records
        
        print("=" * 60)
        print("🎉 Large Dataset Generation Complete!")
        print("📊 Total Records: 5,900+ across all Bronze tables")
        print("✅ Ready for VERY impressive demo!")

if __name__ == "__main__":
    generator = LargeDatasetGenerator()
    generator.run_all() 