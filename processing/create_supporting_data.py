#!/usr/bin/env python3
"""
Supporting Data Creation Script - FIXED VERSION
Creates restaurants, drivers, menu items, and weather data in bronze namespace.
"""

import sys
import os
sys.path.append('/home/iceberg/processing')

from spark_config import create_spark_session
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

def create_supporting_data():
    """Create supporting data tables: restaurants, drivers, menu items, weather"""
    
    print("Creating supporting data for WoEat platform (Fixed Version)...")
    
    # Get Spark session
    spark = create_spark_session("Supporting-Data-Creation-Fixed")
    
    # First create the bronze namespace
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")
        print("Created namespace: bronze")
    except Exception as e:
        print(f"Namespace creation note: {e}")
    
    # Create restaurants data
    restaurants_data = [
        ("rest_001", "Pizza Palace", "Italian", "123 Main St", "555-0101", 4.2, True),
        ("rest_002", "Burger Barn", "American", "456 Oak Ave", "555-0102", 4.0, True),
        ("rest_003", "Sushi Spot", "Japanese", "789 Pine Rd", "555-0103", 4.5, True),
        ("rest_004", "Taco Town", "Mexican", "321 Elm St", "555-0104", 3.8, True),
        ("rest_005", "Curry Corner", "Indian", "654 Maple Dr", "555-0105", 4.3, True),
        ("rest_006", "Noodle House", "Chinese", "987 Cedar Ln", "555-0106", 4.1, True),
        ("rest_007", "Grill Master", "BBQ", "147 Birch Ave", "555-0107", 4.4, True),
        ("rest_008", "Veggie Delight", "Vegetarian", "258 Spruce St", "555-0108", 4.0, True),
        ("rest_009", "Fish & Chips", "British", "369 Willow Rd", "555-0109", 3.9, True),
        ("rest_010", "Pasta Paradise", "Italian", "741 Poplar Dr", "555-0110", 4.2, True),
        ("rest_011", "Thai Garden", "Thai", "852 Ash St", "555-0111", 4.3, True),
        ("rest_012", "Sandwich Shop", "Deli", "963 Hickory Ave", "555-0112", 3.7, True),
        ("rest_013", "Coffee & More", "Cafe", "159 Walnut Ln", "555-0113", 4.1, True),
        ("rest_014", "Steakhouse", "American", "357 Cherry St", "555-0114", 4.6, True),
        ("rest_015", "Mediterranean", "Greek", "468 Olive Dr", "555-0115", 4.4, True)
    ]
    
    restaurants_schema = StructType([
        StructField("restaurant_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("cuisine_type", StringType(), False),
        StructField("address", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("rating", DoubleType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    restaurants_df = spark.createDataFrame(restaurants_data, restaurants_schema)
    restaurants_df.writeTo("bronze.bronze_restaurants").createOrReplace()
    print(f"Created bronze_restaurants with {restaurants_df.count()} records")
    
    # Create drivers data
    drivers_data = []
    for i in range(1, 21):  # 20 drivers
        driver_id = f"driver_{str(i).zfill(3)}"
        name = f"Driver {i}"
        phone = f"555-{str(2000 + i).zfill(4)}"
        vehicle_type = random.choice(["car", "motorcycle", "bicycle"])
        rating = round(random.uniform(3.5, 5.0), 1)
        is_active = random.choice([True, True, True, False])  # 75% active
        
        drivers_data.append((driver_id, name, phone, vehicle_type, rating, is_active))
    
    drivers_schema = StructType([
        StructField("driver_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("vehicle_type", StringType(), False),
        StructField("rating", DoubleType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    drivers_df = spark.createDataFrame(drivers_data, drivers_schema)
    drivers_df.writeTo("bronze.bronze_drivers").createOrReplace()
    print(f"Created bronze_drivers with {drivers_df.count()} records")
    
    # Create menu items data
    menu_items_data = [
        # Pizza Palace
        ("item_001", "rest_001", "Margherita Pizza", "Classic tomato and mozzarella", 12.99, True, "main"),
        ("item_002", "rest_001", "Pepperoni Pizza", "Pepperoni and cheese", 14.99, True, "main"),
        ("item_003", "rest_001", "Caesar Salad", "Fresh romaine lettuce", 8.99, True, "salad"),
        
        # Burger Barn
        ("item_004", "rest_002", "Classic Burger", "Beef patty with lettuce", 9.99, True, "main"),
        ("item_005", "rest_002", "Chicken Sandwich", "Grilled chicken breast", 11.99, True, "main"),
        ("item_006", "rest_002", "French Fries", "Crispy golden fries", 4.99, True, "side"),
        
        # Sushi Spot
        ("item_007", "rest_003", "California Roll", "Crab and avocado roll", 8.99, True, "main"),
        ("item_008", "rest_003", "Salmon Sashimi", "Fresh salmon slices", 15.99, True, "main"),
        ("item_009", "rest_003", "Miso Soup", "Traditional soybean soup", 3.99, True, "soup"),
        
        # Taco Town
        ("item_010", "rest_004", "Beef Tacos", "Seasoned ground beef", 7.99, True, "main"),
        ("item_011", "rest_004", "Chicken Quesadilla", "Grilled chicken and cheese", 9.99, True, "main"),
        ("item_012", "rest_004", "Guacamole", "Fresh avocado dip", 5.99, True, "side"),
        
        # Curry Corner
        ("item_013", "rest_005", "Chicken Curry", "Spicy chicken curry", 13.99, True, "main")
    ]
    
    menu_items_schema = StructType([
        StructField("item_id", StringType(), False),
        StructField("restaurant_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("description", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("is_available", BooleanType(), False),
        StructField("category", StringType(), False)
    ])
    
    menu_items_df = spark.createDataFrame(menu_items_data, menu_items_schema)
    menu_items_df.writeTo("bronze.bronze_menu_items").createOrReplace()
    print(f"Created bronze_menu_items with {menu_items_df.count()} records")
    
    # Create weather data
    weather_data = []
    base_date = datetime.now() - timedelta(days=30)
    
    for i in range(30):  # 30 days of weather
        date = base_date + timedelta(days=i)
        temperature = round(random.uniform(15, 35), 1)  # Celsius
        humidity = random.randint(30, 90)
        conditions = random.choice(["sunny", "cloudy", "rainy", "partly_cloudy"])
        wind_speed = round(random.uniform(0, 25), 1)
        
        weather_data.append((
            date.strftime("%Y-%m-%d"),
            temperature,
            humidity,
            conditions,
            wind_speed
        ))
    
    weather_schema = StructType([
        StructField("date", StringType(), False),
        StructField("temperature_celsius", DoubleType(), False),
        StructField("humidity_percent", IntegerType(), False),
        StructField("conditions", StringType(), False),
        StructField("wind_speed_kmh", DoubleType(), False)
    ])
    
    weather_df = spark.createDataFrame(weather_data, weather_schema)
    weather_df.writeTo("bronze.bronze_weather").createOrReplace()
    print(f"Created bronze_weather with {weather_df.count()} records")
    
    print("\nSupporting data creation completed.")
    print("   15 restaurants")
    print("   20 drivers") 
    print("   13 menu items")
    print("   30 days of weather data")
    print("   All tables created in 'bronze' namespace")
    
    spark.stop()

if __name__ == "__main__":
    create_supporting_data() 