import sys
import os
sys.path.append('/home/iceberg/processing')

from spark_config import create_spark_session

spark = create_spark_session("ShowTables")

print('Bronze Tables:')
spark.sql('SHOW TABLES IN bronze').show(100, False)

print('\n🥈 Silver Tables:')
spark.sql('SHOW TABLES IN silver').show(100, False)

print('\n🥇 Gold Tables:')
spark.sql('SHOW TABLES IN gold').show(100, False)

# Sample data from key tables
print('\nSample Orders:')
try:
    spark.sql('SELECT * FROM gold.fact_orders LIMIT 5').show()
except:
    print('No Gold orders table found')

print('\nBusiness Summary:')
try:
    spark.sql('SELECT * FROM gold.fact_business_summary ORDER BY date_key DESC LIMIT 5').show()
except:
    print('No Gold business summary table found')

print('\n⭐ Top Rated Restaurants:')
try:
    spark.sql('''
    SELECT r.restaurant_name, AVG(fr.food_rating) as avg_rating, COUNT(*) as rating_count
    FROM gold.fact_ratings fr
    JOIN gold.dim_restaurants r ON fr.restaurant_key = r.restaurant_key
    WHERE r.is_current = true
    GROUP BY r.restaurant_name
    ORDER BY avg_rating DESC
    ''').show()
except:
    print('No Gold ratings/restaurants tables found')

spark.stop() 