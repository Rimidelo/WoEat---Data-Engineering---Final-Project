from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('VerifyOrders').getOrCreate()

print('Data Verification Across All Layers:')
print('=' * 50)

def safe_count(table_name):
    """Safely count rows from a table, return 0 if table doesn't exist"""
    try:
        count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
        return count
    except:
        return 0

# Check Bronze layer
bronze_orders = safe_count("demo.bronze.bronze_orders")
bronze_items = safe_count("demo.bronze.bronze_order_items") 
bronze_ratings = safe_count("demo.bronze.bronze_ratings")
bronze_restaurants = safe_count("demo.bronze.bronze_restaurants")
bronze_drivers = safe_count("demo.bronze.bronze_drivers")
bronze_menu = safe_count("demo.bronze.bronze_menu_items")

print(f'BRONZE LAYER:')
print(f'   Orders: {bronze_orders:,}')
print(f'   Order Items: {bronze_items:,}')
print(f'   Ratings: {bronze_ratings:,}')
print(f'   Restaurants: {bronze_restaurants:,}')
print(f'   Drivers: {bronze_drivers:,}')
print(f'   Menu Items: {bronze_menu:,}')

# Check Silver layer  
silver_orders = safe_count("demo.silver.silver_orders")
silver_items = safe_count("demo.silver.silver_order_items")

print(f'\nSILVER LAYER:')
if silver_orders > 0:
    print(f'   Orders: {silver_orders:,}')
    print(f'   Order Items: {silver_items:,}')
else:
    print(f'   No data (not processed yet)')

# Check Gold layer
gold_orders = safe_count("demo.gold.fact_orders")
gold_items = safe_count("demo.gold.fact_order_items")

print(f'\nGOLD LAYER:')
if gold_orders > 0:
    print(f'   Orders: {gold_orders:,}')
    print(f'   Order Items: {gold_items:,}')
else:
    print(f'   No data (not processed yet)')

# Summary
print(f'\nSUMMARY:')
if bronze_orders >= 5000:
    print(f'Bronze: {bronze_orders:,} orders (Ready for processing)')
else:
    print(f'Bronze: {bronze_orders:,} orders (Expected 5,000+)')

if silver_orders > 0:
    print(f'Silver: {silver_orders:,} orders (Processed)')
else:
    print(f'Silver: 0 orders (Run silver processing next)')
    
if gold_orders > 0:
    print(f'Gold: {gold_orders:,} orders (Analytics ready)')
else:
    print(f'Gold: 0 orders (Run gold processing after silver)')

spark.stop() 