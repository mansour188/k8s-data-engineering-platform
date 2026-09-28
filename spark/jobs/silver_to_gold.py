from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg


spark = (
    SparkSession.builder
    .appName("silver-to-gold")
    .getOrCreate()
)


silver_path = "s3a://silver/nginx/"
gold_path = "s3a://gold/nginx/"


# Read Silver
silver = spark.read.parquet(silver_path)


# Aggregate HTTP traffic
gold = (
    silver
    .groupBy("event_date", "hour", "status")
    .agg(
        count("*").alias("request_count"),
        avg("request_time").alias("avg_request_time"),
    )
    .orderBy("event_date", "hour", "status")
)


# Write Gold
(
    gold
    .write
    .mode("overwrite")
    .partitionBy("event_date", "hour")
    .parquet(gold_path)
)


spark.stop()
