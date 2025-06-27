from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ShowTables').getOrCreate()

print('Bronze Tables:')
spark.sql('SHOW TABLES IN demo.bronze').show(100, False)

print('\n🥈 Silver Tables:')
spark.sql('SHOW TABLES IN demo.silver').show(100, False)

print('\n🥇 Gold Tables:')
spark.sql('SHOW TABLES IN demo.gold').show(100, False)

# Sample data from key tables
print('\nSample Orders:')
spark.sql('SELECT * FROM demo.gold.fact_orders LIMIT 5').show()

print('\nBusiness Summary:')
spark.sql('SELECT * FROM demo.gold.fact_business_summary ORDER BY date_key DESC LIMIT 5').show()

print('\n⭐ Top Rated Restaurants:')
spark.sql('''
SELECT r.restaurant_name, AVG(fr.food_rating) as avg_rating, COUNT(*) as rating_count
FROM demo.gold.fact_ratings fr
JOIN demo.gold.dim_restaurants r ON fr.restaurant_key = r.restaurant_key
WHERE r.is_current = true
GROUP BY r.restaurant_name
ORDER BY avg_rating DESC
''').show()

spark.stop() 