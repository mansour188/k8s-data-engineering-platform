from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    regexp_extract,
    hour,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
)


spark = (
    SparkSession.builder
    .appName("bronze-to-silver")
    .getOrCreate()
)


# Fluent Bit outer JSON
outer_schema = StructType([
    StructField("date", StringType(), True),
    StructField("log", StringType(), True),
])


# Original NGINX JSON
nginx_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("remote_addr", StringType(), True),
    StructField("method", StringType(), True),
    StructField("uri", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("request_time", DoubleType(), True),
    StructField("body_bytes_sent", LongType(), True),
    StructField("user_agent", StringType(), True),
])


bronze_path = "s3a://bronze/nginx/"
silver_path = "s3a://silver/nginx/"


# --------------------------------------------------
# 1. Read Bronze
# --------------------------------------------------

raw = spark.read.text(bronze_path)


# --------------------------------------------------
# 2. Parse Fluent Bit outer JSON
# --------------------------------------------------

outer = (
    raw
    .select(
        from_json(col("value"), outer_schema).alias("data")
    )
    .select("data.*")
)


# --------------------------------------------------
# 3. Extract the NGINX JSON from the CRI log line
# --------------------------------------------------

nginx_json = outer.withColumn(
    "nginx_json",
    regexp_extract(
        col("log"),
        r'(\{.*\})$',
        1,
    )
)


# --------------------------------------------------
# 4. Parse the embedded NGINX JSON
# --------------------------------------------------

silver = (
    nginx_json
    .select(
        from_json(
            col("nginx_json"),
            nginx_schema
        ).alias("nginx")
    )
    .select("nginx.*")
)


# --------------------------------------------------
# 5. Convert timestamp from string → timestamp type
# --------------------------------------------------

silver = silver.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)


# --------------------------------------------------
# 6. Create proper partition columns
# --------------------------------------------------

silver = silver.withColumn(
    "event_date",
    to_date(col("timestamp"))
)

silver = silver.withColumn(
    "hour",
    hour(col("timestamp"))
)


# --------------------------------------------------
# 7. Write Silver as partitioned Parquet
# --------------------------------------------------

(
    silver
    .write
    .mode("overwrite")
    .partitionBy("event_date", "hour")
    .parquet(silver_path)
)


spark.stop()
