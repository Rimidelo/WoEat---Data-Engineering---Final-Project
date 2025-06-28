#!/usr/bin/env python3
"""
Enhanced Spark Configuration for UI Visibility
This ensures all Spark jobs appear in the Spark UI with proper settings
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def create_spark_session(app_name="WoEat-DataPipeline"):
    """
    Create Spark session with enhanced UI visibility and performance settings
    """
    
    # Enhanced Spark configuration for UI visibility
    conf = SparkConf()
    
    # UI and Monitoring Settings
    conf.set("spark.ui.enabled", "true")
    conf.set("spark.ui.port", "4040")
    conf.set("spark.ui.retainedJobs", "100")           # Keep more jobs in UI
    conf.set("spark.ui.retainedStages", "100")         # Keep more stages in UI
    conf.set("spark.ui.retainedTasks", "1000")         # Keep more tasks in UI
    conf.set("spark.sql.ui.retainedExecutions", "100") # Keep SQL executions
    conf.set("spark.ui.showConsoleProgress", "true")   # Show progress in console
    
    # Performance Settings (makes jobs run longer and more visible)
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    # Streaming Settings
    conf.set("spark.sql.streaming.ui.enabled", "true")
    conf.set("spark.sql.streaming.ui.retainedBatches", "100")
    
    # Event Log Settings (disabled to avoid directory issues)
    conf.set("spark.eventLog.enabled", "false")
    
    # Memory Settings (prevents jobs from running too fast)
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.driver.maxResultSize", "1g")
    
    # Iceberg and S3 Settings - Using Hadoop catalog for reliability
    conf.set("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.demo.type", "hadoop")
    conf.set("spark.sql.catalog.demo.warehouse", "s3a://warehouse")
    conf.set("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000")
    conf.set("spark.sql.catalog.demo.s3.path-style-access", "true")
    conf.set("spark.sql.catalog.demo.s3.access-key-id", "admin")
    conf.set("spark.sql.catalog.demo.s3.secret-access-key", "password")
    
    # Kafka Settings for Streaming with S3 support
    conf.set("spark.jars.packages", 
             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
             "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,"
             "org.apache.hadoop:hadoop-aws:3.3.2,"
             "com.amazonaws:aws-java-sdk-bundle:1.11.1026")
    
    # S3 FileSystem configuration for Hadoop catalog
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    conf.set("spark.hadoop.fs.s3a.access.key", "admin")
    conf.set("spark.hadoop.fs.s3a.secret.key", "password")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    # Create session with enhanced config
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(conf=conf)
        .getOrCreate()
    )
    
    # Set log level to see more activity
    spark.sparkContext.setLogLevel("INFO")
    
    print(f"Spark Session '{app_name}' created")
    print(f"Spark UI available at: http://localhost:4041")
    print(f"Application ID: {spark.sparkContext.applicationId}")
    
    return spark

def create_long_running_spark_session(app_name="WoEat-LongRunning"):
    """
    Create a Spark session optimized for long-running jobs that stay visible in UI
    """
    spark = create_spark_session(app_name)
    
    # Additional settings for long-running jobs
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1000")  # Smaller batches = more stages
    
    return spark

if __name__ == "__main__":
    # Test the configuration
    spark = create_spark_session("Test-Spark-UI")
    
    # Create a sample job that will be visible in UI
    print("Running test job to verify UI visibility...")
    
    # Generate sample data that creates multiple stages
    df = spark.range(1000000).toDF("id")
    df = df.withColumn("doubled", df.id * 2)
    df = df.withColumn("squared", df.id * df.id)
    df = df.filter(df.id % 100 == 0)
    df = df.groupBy((df.id / 1000).cast("int").alias("group")).count()
    
    # This will create a job visible in Spark UI
    result = df.collect()
    print(f"Test job completed with {len(result)} results")
    print("Check Spark UI at http://localhost:4041 to see the job.")
    
    spark.stop() 