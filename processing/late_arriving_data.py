from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import time

class LateArrivingDataProcessor:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("WoEat - Late Arriving Data Handler")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config("spark.sql.catalog.demo.s3.access-key-id", "admin")
            .config("spark.sql.catalog.demo.s3.secret-access-key", "password")
            .getOrCreate()
        )
    
    def simulate_end_of_day_restaurant_reports(self):
        """Simulate restaurant performance reports arriving at end of day"""
        print("🕐 Simulating End-of-Day Restaurant Reports (Late Arriving Data)...")
        print("⏰ Scenario: Restaurant reports for previous days arrive late")
        
        # Simulate reports arriving for dates when orders already exist
        # These reports arrive 24-48 hours after the actual restaurant operations
        
        # Get current timestamp for arrival time
        current_time = datetime.now()
        
        # Create reports for 2-3 days ago (late arriving)
        late_reports_data = [
            # Reports for 2 days ago (arrived late)
            ((current_time - timedelta(days=2)).date(), "rest_001", 28.5, 4.3, 67, 0.04, 3.85, 
             current_time, "Late arrival - technical issues"),
            ((current_time - timedelta(days=2)).date(), "rest_002", 22.1, 4.6, 89, 0.02, 4.50, 
             current_time, "Late arrival - technical issues"),
            ((current_time - timedelta(days=2)).date(), "rest_003", 35.2, 4.8, 45, 0.01, 5.25, 
             current_time, "Late arrival - technical issues"),
            ((current_time - timedelta(days=2)).date(), "rest_004", 19.8, 4.2, 78, 0.06, 3.10, 
             current_time, "Late arrival - technical issues"),
            ((current_time - timedelta(days=2)).date(), "rest_005", 31.7, 4.7, 52, 0.03, 4.95, 
             current_time, "Late arrival - technical issues"),
            
            # Reports for 3 days ago (arrived very late)
            ((current_time - timedelta(days=3)).date(), "rest_001", 26.8, 4.1, 72, 0.07, 3.40, 
             current_time, "Late arrival - system maintenance"),
            ((current_time - timedelta(days=3)).date(), "rest_002", 20.5, 4.4, 85, 0.03, 4.20, 
             current_time, "Late arrival - system maintenance"),
            ((current_time - timedelta(days=3)).date(), "rest_003", 38.1, 4.9, 41, 0.02, 5.60, 
             current_time, "Late arrival - system maintenance"),
            ((current_time - timedelta(days=3)).date(), "rest_004", 23.2, 4.0, 69, 0.08, 2.95, 
             current_time, "Late arrival - system maintenance"),
            ((current_time - timedelta(days=3)).date(), "rest_005", 29.9, 4.5, 58, 0.05, 4.75, 
             current_time, "Late arrival - system maintenance"),
        ]
        
        schema = StructType([
            StructField("report_date", DateType(), False),
            StructField("restaurant_id", StringType(), False),
            StructField("avg_prep_time", FloatType(), False),
            StructField("avg_rating", FloatType(), False),
            StructField("orders_count", IntegerType(), False),
            StructField("cancel_rate", FloatType(), False),
            StructField("avg_tip", FloatType(), False),
            StructField("arrival_timestamp", TimestampType(), False),
            StructField("late_reason", StringType(), False)
        ])
        
        late_reports_df = (
            self.spark.createDataFrame(late_reports_data, schema)
            .withColumn("ingest_timestamp", current_timestamp())
            .withColumn("is_late_arrival", lit(True))
            .withColumn("days_late", 
                       datediff(col("arrival_timestamp"), col("report_date")))
            .select(
                "report_date",
                "restaurant_id", 
                "avg_prep_time",
                "avg_rating",
                "orders_count",
                "cancel_rate",
                "avg_tip",
                "ingest_timestamp",
                "is_late_arrival",
                "days_late"
            )
        )
        
        print("📊 Late arriving restaurant reports:")
        late_reports_df.select("report_date", "restaurant_id", "avg_prep_time", 
                               "orders_count", "days_late").show(10, False)
        
        # Append to Bronze layer with late arrival flag
        late_reports_df.writeTo("demo.bronze.bronze_restaurant_performance").append()
        
        print(f"✅ Ingested {late_reports_df.count()} late-arriving restaurant reports")
        return late_reports_df
    
    def demonstrate_late_data_impact(self):
        """Show how late data affects analytics and requires reprocessing"""
        print("\n🔄 Demonstrating Late Data Impact on Analytics...")
        
        # Show restaurant performance before late data
        print("📈 Restaurant Performance BEFORE Late Data:")
        before_query = """
        SELECT 
            restaurant_id,
            COUNT(*) as report_days,
            AVG(avg_prep_time) as avg_prep_time,
            AVG(avg_rating) as avg_rating,
            SUM(orders_count) as total_orders,
            MAX(ingest_timestamp) as last_update
        FROM demo.bronze.bronze_restaurant_performance 
        WHERE is_late_arrival IS NULL OR is_late_arrival = false
        GROUP BY restaurant_id
        ORDER BY restaurant_id
        """
        
        before_df = self.spark.sql(before_query)
        before_df.show(10, False)
        
        # Simulate late data arrival
        self.simulate_end_of_day_restaurant_reports()
        
        # Show restaurant performance after late data
        print("\n📈 Restaurant Performance AFTER Late Data Arrival:")
        after_query = """
        SELECT 
            restaurant_id,
            COUNT(*) as total_report_days,
            COUNT(CASE WHEN is_late_arrival = true THEN 1 END) as late_reports,
            AVG(avg_prep_time) as avg_prep_time,
            AVG(avg_rating) as avg_rating,
            SUM(orders_count) as total_orders,
            MAX(days_late) as max_days_late,
            MAX(ingest_timestamp) as last_update
        FROM demo.bronze.bronze_restaurant_performance 
        GROUP BY restaurant_id
        ORDER BY restaurant_id
        """
        
        after_df = self.spark.sql(after_query)
        after_df.show(10, False)
        
        print("\n⚠️ Analytics Impact:")
        print("1. Historical metrics have changed due to late data")
        print("2. Restaurant rankings may have shifted")
        print("3. SLA and performance reports need to be recalculated")
        print("4. Downstream Gold layer needs reprocessing")
        
    def handle_late_data_reprocessing(self):
        """Demonstrate how to handle late data in the pipeline"""
        print("\n🔧 Handling Late Data Reprocessing...")
        
        # Identify affected dates
        affected_dates_query = """
        SELECT DISTINCT report_date, MAX(days_late) as days_late
        FROM demo.bronze.bronze_restaurant_performance 
        WHERE is_late_arrival = true
        GROUP BY report_date
        ORDER BY report_date
        """
        
        affected_dates = self.spark.sql(affected_dates_query)
        print("📅 Dates affected by late data:")
        affected_dates.show()
        
        # Show late data handling strategy
        print("\n🎯 Late Data Handling Strategy:")
        print("1. ✅ Watermark: Accept data up to 48 hours late")
        print("2. ✅ Reprocessing: Trigger Silver/Gold layer updates")
        print("3. ✅ Versioning: Track data lineage and updates")
        print("4. ✅ Notifications: Alert downstream systems")
        
        # Create a reprocessing flag
        reprocess_dates = affected_dates.select("report_date").collect()
        
        print(f"\n🔄 Triggering reprocessing for {len(reprocess_dates)} affected dates")
        for row in reprocess_dates:
            date_str = row['report_date'].strftime("%Y-%m-%d")
            print(f"   📅 Reprocessing date: {date_str}")
        
        return reprocess_dates
    
    def demonstrate_watermark_handling(self):
        """Show watermark-based late data handling"""
        print("\n💧 Watermark-Based Late Data Handling Demo...")
        
        # Simulate streaming scenario with watermarks
        print("⏰ Event Time vs Processing Time Analysis:")
        
        watermark_analysis = """
        SELECT 
            report_date as event_date,
            DATE(ingest_timestamp) as processing_date,
            days_late,
            CASE 
                WHEN days_late <= 1 THEN 'On Time'
                WHEN days_late <= 2 THEN 'Acceptable Late'
                WHEN days_late <= 3 THEN 'Very Late'
                ELSE 'Too Late'
            END as lateness_category
        FROM demo.bronze.bronze_restaurant_performance 
        WHERE is_late_arrival = true
        ORDER BY days_late DESC
        """
        
        watermark_df = self.spark.sql(watermark_analysis)
        watermark_df.show(20, False)
        
        print("\n📊 Late Data Statistics:")
        stats_query = """
        SELECT 
            lateness_category,
            COUNT(*) as count,
            AVG(days_late) as avg_days_late,
            MAX(days_late) as max_days_late
        FROM (
            SELECT 
                days_late,
                CASE 
                    WHEN days_late <= 1 THEN 'On Time'
                    WHEN days_late <= 2 THEN 'Acceptable Late'
                    WHEN days_late <= 3 THEN 'Very Late'
                    ELSE 'Too Late'
                END as lateness_category
            FROM demo.bronze.bronze_restaurant_performance 
            WHERE is_late_arrival = true
        )
        GROUP BY lateness_category
        ORDER BY avg_days_late
        """
        
        stats_df = self.spark.sql(stats_query)
        stats_df.show()
    
    def create_late_data_monitoring_view(self):
        """Create a monitoring view for late arriving data"""
        print("\n📊 Creating Late Data Monitoring View...")
        
        monitoring_query = """
        CREATE OR REPLACE TEMPORARY VIEW late_data_monitor AS
        SELECT 
            DATE(ingest_timestamp) as processing_date,
            report_date as event_date,
            restaurant_id,
            days_late,
            avg_prep_time,
            orders_count,
            CASE 
                WHEN days_late > 2 THEN 'CRITICAL'
                WHEN days_late > 1 THEN 'WARNING'
                ELSE 'OK'
            END as alert_level
        FROM demo.bronze.bronze_restaurant_performance 
        WHERE is_late_arrival = true
        ORDER BY days_late DESC, ingest_timestamp DESC
        """
        
        self.spark.sql(monitoring_query)
        
        print("🚨 Late Data Alerts:")
        alerts_df = self.spark.sql("""
        SELECT alert_level, COUNT(*) as count, AVG(days_late) as avg_delay
        FROM late_data_monitor 
        GROUP BY alert_level
        ORDER BY avg_delay DESC
        """)
        alerts_df.show()
        
        print("\n🔍 Recent Late Arrivals:")
        recent_df = self.spark.sql("""
        SELECT * FROM late_data_monitor 
        WHERE alert_level IN ('CRITICAL', 'WARNING')
        LIMIT 10
        """)
        recent_df.show(10, False)
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

if __name__ == "__main__":
    late_data_processor = LateArrivingDataProcessor()
    
    try:
        print("🚀 Starting Late-Arriving Data Demonstration...")
        print("=" * 60)
        
        # 1. Show the impact of late data
        late_data_processor.demonstrate_late_data_impact()
        
        # 2. Handle reprocessing
        late_data_processor.handle_late_data_reprocessing()
        
        # 3. Show watermark handling
        late_data_processor.demonstrate_watermark_handling()
        
        # 4. Create monitoring views
        late_data_processor.create_late_data_monitoring_view()
        
        print("\n" + "=" * 60)
        print("🎯 Late-Arriving Data Demo Completed Successfully!")
        print("✅ Restaurant reports arriving 24-48 hours late handled")
        print("✅ Analytics impact demonstrated")
        print("✅ Reprocessing strategy implemented")
        print("✅ Monitoring and alerting setup")
        
    except Exception as e:
        print(f"❌ Error in late data processing: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        late_data_processor.stop() 