from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('VerifyOrders').getOrCreate()

print('📊 Order Count Verification Across All Layers:')
print('=' * 50)

bronze_count = spark.sql("SELECT COUNT(*) FROM demo.bronze.bronze_orders").collect()[0][0]
silver_count = spark.sql("SELECT COUNT(*) FROM demo.silver.silver_orders").collect()[0][0]  
gold_count = spark.sql("SELECT COUNT(*) FROM demo.gold.fact_orders").collect()[0][0]

print(f'🥉 Bronze Layer: {bronze_count:,} orders')
print(f'🥈 Silver Layer: {silver_count:,} orders') 
print(f'🥇 Gold Layer: {gold_count:,} orders')

if bronze_count == silver_count == gold_count == 5000:
    print('\n✅ SUCCESS: All layers have exactly 5,000 orders!')
    print('✅ Data consistency verified across Bronze → Silver → Gold')
else:
    print('\n❌ INCONSISTENCY DETECTED!')
    print(f'   Expected: 5,000 orders in each layer')
    print(f'   Found: Bronze={bronze_count}, Silver={silver_count}, Gold={gold_count}')

# Additional verification - show order items and ratings counts
items_count = spark.sql("SELECT COUNT(*) FROM demo.gold.fact_order_items").collect()[0][0]
ratings_count = spark.sql("SELECT COUNT(*) FROM demo.gold.fact_ratings").collect()[0][0]

print(f'\n📊 Additional Data Verification:')
print(f'🛒 Order Items: {items_count:,}')
print(f'⭐ Ratings: {ratings_count:,}')

# Show sample data to prove orders exist
print(f'\n📋 Sample Order Data (First 3 Orders):')
spark.sql("SELECT order_id, status, total_amount, order_time FROM demo.gold.fact_orders ORDER BY order_time LIMIT 3").show()

spark.stop() 