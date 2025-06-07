from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ──────────────────────────────────────────────────────────────
# 1.  SparkSession wired to the REST-catalog that is already
#     running in your docker-compose stack (service name “rest”)
# ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
        .appName("WoEat – bronze.test_drivers demo")
        .config("spark.sql.catalog.demo",              "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.demo.uri",          "http://rest:8181")
        .config("spark.sql.catalog.demo.io-impl",      "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.demo.s3.endpoint",  "http://minio:9000")
        .config("spark.sql.catalog.demo.s3.path-style-access", "true")
        .config("spark.sql.catalog.demo.s3.access-key-id",     "admin")
        .config("spark.sql.catalog.demo.s3.secret-access-key", "password")
        .getOrCreate()
)

# ──────────────────────────────────────────────────────────────
# 2.  Tiny dummy DataFrame (pretend it came from the Raw layer)
# ──────────────────────────────────────────────────────────────
schema = StructType([
    StructField("driver_id",   StringType(), False),
    StructField("driver_name", StringType(), False),
    StructField("rating",      IntegerType(), False),
])
data = [("d001", "Alice", 4),
        ("d002", "Bob",   5)]

df = spark.createDataFrame(data, schema)

# ──────────────────────────────────────────────────────────────
# 3.  Write to Iceberg → bucket warehouse/bronze/test_drivers/*
# ──────────────────────────────────────────────────────────────
df.writeTo("demo.bronze.test_drivers").createOrReplace()

print("✅  Table demo.bronze.test_drivers created!")

spark.stop()
