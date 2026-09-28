from pyspark.sql import SparkSession
import os


spark = (
    SparkSession.builder
    .appName("gold-to-postgres")
    .getOrCreate()
)


gold_path = "s3a://gold/nginx/"

jdbc_url = (
    "jdbc:postgresql://"
    "postgres-data.data-platform.svc.cluster.local:5432/"
    "analytics"
)

jdbc_properties = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
}


gold = spark.read.parquet(gold_path)

print("Gold schema:")
gold.printSchema()

print("Gold data:")
gold.show(20, truncate=False)


(
    gold.write
    .mode("append")
    .jdbc(
        url=jdbc_url,
        table="nginx_hourly_metrics",
        properties=jdbc_properties,
    )
)


print("Gold data successfully written to PostgreSQL.")

spark.stop()
