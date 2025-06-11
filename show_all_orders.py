from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ShowAllOrders').getOrCreate()

print('📦 WoEat - All Orders Report')
print('=' * 80)

# Get total count first
total_orders = spark.sql("SELECT COUNT(*) FROM demo.gold.fact_orders").collect()[0][0]
print(f'📊 Total Orders in Database: {total_orders:,}')
print('=' * 80)

# Show all orders with key information
print('🔍 All Orders Details:')
print()

# Query to get all orders with readable information
all_orders_query = """
SELECT 
    fo.order_id,
    fo.status,
    fo.order_time,
    fo.delivery_time,
    fo.total_amount,
    fo.tip_amount,
    dr.restaurant_name,
    dd.driver_name,
    CASE 
        WHEN fo.delivery_minutes IS NOT NULL 
        THEN CAST(fo.delivery_minutes AS INT) 
        ELSE NULL 
    END as delivery_minutes,
    fo.cancelled
FROM demo.gold.fact_orders fo
LEFT JOIN demo.gold.dim_restaurants dr ON fo.restaurant_key = dr.restaurant_key AND dr.is_current = true
LEFT JOIN demo.gold.dim_drivers dd ON fo.driver_key = dd.driver_key AND dd.is_current = true
ORDER BY fo.order_time
"""

# Execute query and show all results
orders_df = spark.sql(all_orders_query)

# Show all orders (this will display all 5000 orders)
print("📋 Showing ALL orders (this may take a moment for 5,000 orders)...")
orders_df.show(n=5000, truncate=False)

# Summary statistics
print("\n📊 Order Summary Statistics:")
print("=" * 50)

# Status breakdown
print("📈 Orders by Status:")
spark.sql("""
    SELECT status, COUNT(*) as count, 
           ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM demo.gold.fact_orders), 2) as percentage
    FROM demo.gold.fact_orders 
    GROUP BY status 
    ORDER BY count DESC
""").show()

# Restaurant breakdown
print("🏪 Orders by Restaurant:")
spark.sql("""
    SELECT dr.restaurant_name, COUNT(*) as order_count
    FROM demo.gold.fact_orders fo
    JOIN demo.gold.dim_restaurants dr ON fo.restaurant_key = dr.restaurant_key 
    WHERE dr.is_current = true
    GROUP BY dr.restaurant_name
    ORDER BY order_count DESC
""").show()

# Daily order distribution
print("📅 Orders by Date:")
spark.sql("""
    SELECT DATE(order_time) as order_date, COUNT(*) as daily_orders
    FROM demo.gold.fact_orders
    GROUP BY DATE(order_time)
    ORDER BY order_date
""").show(30)

# Revenue statistics
print("💰 Revenue Statistics:")
spark.sql("""
    SELECT 
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(AVG(total_amount), 2) as avg_order_value,
        ROUND(MIN(total_amount), 2) as min_order,
        ROUND(MAX(total_amount), 2) as max_order,
        ROUND(SUM(tip_amount), 2) as total_tips,
        ROUND(AVG(tip_amount), 2) as avg_tip
    FROM demo.gold.fact_orders
    WHERE status = 'delivered'
""").show()

print(f"\n✅ Successfully displayed all {total_orders:,} orders!")
print("📝 Use the scroll functionality in your terminal to view all orders above.")

spark.stop() 