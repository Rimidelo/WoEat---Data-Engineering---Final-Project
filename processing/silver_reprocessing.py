from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class SilverReprocessing:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("WoEat - Silver Layer Reprocessing")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config("spark.sql.catalog.demo.s3.access-key-id", "admin")
            .config("spark.sql.catalog.demo.s3.secret-access-key", "password")
            .getOrCreate()
        )
    
    def detect_late_arriving_data(self):
        """Detect late arriving data in Bronze layer"""
        print("🔍 Detecting Late Arriving Data...")
        
        late_data_query = """
        SELECT 
            report_date,
            restaurant_id,
            ingest_timestamp,
            days_late,
            late_reason,
            is_late_arrival
        FROM demo.bronze.bronze_restaurant_performance 
        WHERE is_late_arrival = true
        ORDER BY days_late DESC, ingest_timestamp DESC
        """
        
        late_data_df = self.spark.sql(late_data_query)
        
        if late_data_df.count() > 0:
            print(f"⚠️ Found {late_data_df.count()} late arriving records:")
            late_data_df.show(10, False)
            
            # Get affected dates for reprocessing
            affected_dates = late_data_df.select("report_date").distinct().collect()
            return [row['report_date'] for row in affected_dates]
        else:
            print("✅ No late arriving data detected")
            return []
    
    def reprocess_silver_restaurant_performance(self, affected_dates=None):
        """Reprocess Silver restaurant performance with late data"""
        print("🔄 Reprocessing Silver Restaurant Performance...")
        
        # Read all Bronze restaurant performance data
        bronze_performance = self.spark.table("demo.bronze.bronze_restaurant_performance")
        
        if affected_dates:
            print(f"📅 Reprocessing specific dates: {[str(d) for d in affected_dates]}")
            # Filter to affected dates only for incremental reprocessing
            bronze_performance = bronze_performance.filter(
                col("report_date").isin(affected_dates)
            )
        
        # Enhanced cleaning and validation with late data handling
        silver_performance = (
            bronze_performance
            .filter(col("report_date").isNotNull())
            .filter(col("restaurant_id").isNotNull())
            .filter(col("orders_count") >= 0)
            .filter(col("avg_prep_time") > 0)
            .filter(col("avg_rating").between(1.0, 5.0))
            .filter(col("cancel_rate").between(0.0, 1.0))
            .withColumn("avg_prep_time", round(col("avg_prep_time"), 1))
            .withColumn("avg_rating", round(col("avg_rating"), 1))
            .withColumn("cancel_rate", round(col("cancel_rate"), 3))
            .withColumn("avg_tip", 
                       when(col("avg_tip") < 0, 0.0)
                       .otherwise(round(col("avg_tip"), 2)))
            .withColumn("ingest_timestamp", current_timestamp())
            .withColumn("reprocessed_timestamp", 
                       when(col("is_late_arrival") == True, current_timestamp())
                       .otherwise(None))
            .withColumn("data_quality_score",
                       when(col("is_late_arrival") == True, 0.8)  # Late data gets lower quality score
                       .otherwise(1.0))
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
                "days_late",
                "reprocessed_timestamp",
                "data_quality_score"
            )
        )
        
        if affected_dates:
            # Incremental update: merge with existing Silver data
            existing_silver = self.spark.table("demo.silver.silver_restaurant_performance")
            
            # Remove old records for affected dates
            updated_silver = existing_silver.filter(
                ~col("report_date").isin(affected_dates)
            ).union(silver_performance)
            
            # Replace Silver table
            updated_silver.writeTo("demo.silver.silver_restaurant_performance").createOrReplace()
            print(f"✅ Updated {silver_performance.count()} records for affected dates")
        else:
            # Full reprocessing
            silver_performance.writeTo("demo.silver.silver_restaurant_performance").createOrReplace()
            print(f"✅ Reprocessed {silver_performance.count()} restaurant performance records")
        
        return silver_performance
    
    def analyze_late_data_impact(self):
        """Analyze the impact of late data on Silver layer"""
        print("\n📊 Analyzing Late Data Impact on Silver Layer...")
        
        impact_analysis = """
        SELECT 
            restaurant_id,
            COUNT(*) as total_reports,
            COUNT(CASE WHEN is_late_arrival = true THEN 1 END) as late_reports,
            AVG(CASE WHEN is_late_arrival = false THEN avg_rating END) as on_time_avg_rating,
            AVG(CASE WHEN is_late_arrival = true THEN avg_rating END) as late_avg_rating,
            AVG(CASE WHEN is_late_arrival = false THEN avg_prep_time END) as on_time_avg_prep,
            AVG(CASE WHEN is_late_arrival = true THEN avg_prep_time END) as late_avg_prep,
            AVG(data_quality_score) as avg_quality_score,
            MAX(days_late) as max_days_late
        FROM demo.silver.silver_restaurant_performance 
        GROUP BY restaurant_id
        ORDER BY late_reports DESC
        """
        
        impact_df = self.spark.sql(impact_analysis)
        impact_df.show(10, False)
        
        # Show data quality impact
        quality_impact = """
        SELECT 
            CASE WHEN is_late_arrival = true THEN 'Late Arrival' ELSE 'On Time' END as arrival_type,
            COUNT(*) as record_count,
            AVG(data_quality_score) as avg_quality_score,
            AVG(avg_rating) as avg_rating,
            AVG(avg_prep_time) as avg_prep_time
        FROM demo.silver.silver_restaurant_performance 
        GROUP BY is_late_arrival
        """
        
        print("\n📈 Data Quality Impact:")
        quality_df = self.spark.sql(quality_impact)
        quality_df.show()
        
        return impact_df
    
    def create_late_data_metrics(self):
        """Create metrics for monitoring late data"""
        print("\n📊 Creating Late Data Monitoring Metrics...")
        
        # Late data summary metrics
        metrics_query = """
        SELECT 
            'restaurant_performance' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN is_late_arrival = true THEN 1 END) as late_records,
            ROUND(COUNT(CASE WHEN is_late_arrival = true THEN 1 END) * 100.0 / COUNT(*), 2) as late_percentage,
            AVG(CASE WHEN is_late_arrival = true THEN days_late END) as avg_lateness_days,
            MAX(days_late) as max_lateness_days,
            MIN(CASE WHEN is_late_arrival = true THEN ingest_timestamp END) as first_late_arrival,
            MAX(CASE WHEN is_late_arrival = true THEN ingest_timestamp END) as last_late_arrival
        FROM demo.silver.silver_restaurant_performance
        """
        
        metrics_df = self.spark.sql(metrics_query)
        print("🎯 Late Data Summary Metrics:")
        metrics_df.show(1, False)
        
        # Time-based late data pattern
        pattern_query = """
        SELECT 
            DATE(ingest_timestamp) as processing_date,
            COUNT(*) as total_records,
            COUNT(CASE WHEN is_late_arrival = true THEN 1 END) as late_records,
            AVG(CASE WHEN is_late_arrival = true THEN days_late END) as avg_lateness
        FROM demo.silver.silver_restaurant_performance 
        GROUP BY DATE(ingest_timestamp)
        ORDER BY processing_date DESC
        """
        
        print("\n📅 Daily Late Data Pattern:")
        pattern_df = self.spark.sql(pattern_query)
        pattern_df.show(10, False)
        
        return metrics_df, pattern_df
    
    def validate_reprocessing_success(self):
        """Validate that reprocessing was successful"""
        print("\n✅ Validating Reprocessing Success...")
        
        validation_checks = []
        
        # Check 1: No missing data for late arrival dates
        missing_data_query = """
        SELECT 
            report_date,
            COUNT(DISTINCT restaurant_id) as restaurant_count
        FROM demo.silver.silver_restaurant_performance 
        WHERE is_late_arrival = true
        GROUP BY report_date
        HAVING COUNT(DISTINCT restaurant_id) < 5  -- Expecting 5 restaurants
        """
        
        missing_df = self.spark.sql(missing_data_query)
        if missing_df.count() == 0:
            print("✅ Check 1 PASSED: All restaurants have data for late arrival dates")
            validation_checks.append(True)
        else:
            print("❌ Check 1 FAILED: Missing restaurant data")
            missing_df.show()
            validation_checks.append(False)
        
        # Check 2: Data quality scores are properly assigned
        quality_check_query = """
        SELECT 
            is_late_arrival,
            MIN(data_quality_score) as min_score,
            MAX(data_quality_score) as max_score,
            AVG(data_quality_score) as avg_score
        FROM demo.silver.silver_restaurant_performance 
        GROUP BY is_late_arrival
        """
        
        quality_df = self.spark.sql(quality_check_query)
        print("\n📊 Data Quality Score Validation:")
        quality_df.show()
        
        # Check that late data has lower quality scores
        late_score = quality_df.filter(col("is_late_arrival") == True).select("avg_score").collect()
        on_time_score = quality_df.filter(col("is_late_arrival") == False).select("avg_score").collect()
        
        if late_score and on_time_score and late_score[0]['avg_score'] < on_time_score[0]['avg_score']:
            print("✅ Check 2 PASSED: Late data has lower quality scores")
            validation_checks.append(True)
        else:
            print("❌ Check 2 FAILED: Quality scoring incorrect")
            validation_checks.append(False)
        
        # Check 3: Reprocessing timestamps are set
        reprocess_check_query = """
        SELECT 
            COUNT(CASE WHEN is_late_arrival = true AND reprocessed_timestamp IS NOT NULL THEN 1 END) as reprocessed_count,
            COUNT(CASE WHEN is_late_arrival = true THEN 1 END) as total_late_count
        FROM demo.silver.silver_restaurant_performance
        """
        
        reprocess_df = self.spark.sql(reprocess_check_query)
        reprocess_row = reprocess_df.collect()[0]
        
        if reprocess_row['reprocessed_count'] == reprocess_row['total_late_count']:
            print("✅ Check 3 PASSED: All late data has reprocessing timestamps")
            validation_checks.append(True)
        else:
            print("❌ Check 3 FAILED: Missing reprocessing timestamps")
            validation_checks.append(False)
        
        overall_success = all(validation_checks)
        print(f"\n🎯 Overall Validation: {'✅ PASSED' if overall_success else '❌ FAILED'}")
        
        return overall_success
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

if __name__ == "__main__":
    silver_reprocessor = SilverReprocessing()
    
    try:
        print("🚀 Starting Silver Layer Late Data Reprocessing...")
        print("=" * 60)
        
        # 1. Detect late arriving data
        affected_dates = silver_reprocessor.detect_late_arriving_data()
        
        # 2. Reprocess Silver layer
        if affected_dates:
            silver_reprocessor.reprocess_silver_restaurant_performance(affected_dates)
        else:
            print("ℹ️ No late data detected, running full reprocessing...")
            silver_reprocessor.reprocess_silver_restaurant_performance()
        
        # 3. Analyze impact
        silver_reprocessor.analyze_late_data_impact()
        
        # 4. Create monitoring metrics
        silver_reprocessor.create_late_data_metrics()
        
        # 5. Validate success
        silver_reprocessor.validate_reprocessing_success()
        
        print("\n" + "=" * 60)
        print("🎯 Silver Layer Reprocessing Completed!")
        
    except Exception as e:
        print(f"❌ Error in Silver reprocessing: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        silver_reprocessor.stop() 