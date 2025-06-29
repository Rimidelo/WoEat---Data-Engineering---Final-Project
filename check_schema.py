import sys
import os
sys.path.append('/home/iceberg/processing')

from spark_config import create_spark_session

spark = create_spark_session("CheckSchema")

print("=== BRONZE ORDERS SCHEMA ===")
spark.sql('DESCRIBE bronze.bronze_orders').show(50, False)

print("\n=== BRONZE ORDERS SAMPLE ===")
spark.sql('SELECT * FROM bronze.bronze_orders LIMIT 3').show(100, False)

print("\n=== BRONZE RESTAURANTS SCHEMA ===")
spark.sql('DESCRIBE bronze.bronze_restaurants').show(50, False)

print("\n=== BRONZE RESTAURANTS SAMPLE ===")
spark.sql('SELECT * FROM bronze.bronze_restaurants LIMIT 3').show(100, False)

print("\n=== BRONZE RATINGS SCHEMA ===")
spark.sql('DESCRIBE bronze.bronze_ratings').show(50, False)

print("\n=== RECORD COUNTS ===")
orders_count = spark.sql('SELECT COUNT(*) FROM bronze.bronze_orders').collect()[0][0]
restaurants_count = spark.sql('SELECT COUNT(*) FROM bronze.bronze_restaurants').collect()[0][0]
ratings_count = spark.sql('SELECT COUNT(*) FROM bronze.bronze_ratings').collect()[0][0]

print(f"Orders: {orders_count}")
print(f"Restaurants: {restaurants_count}")
print(f"Ratings: {ratings_count}")

spark.stop() 