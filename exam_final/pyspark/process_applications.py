from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum as spark_sum,
    round as spark_round,
    to_timestamp
)

BUCKET = "etl-module4-kononenko"

INPUT_PATH = f"s3a://{BUCKET}/input/applications.csv"
OUTPUT_PATH = f"s3a://{BUCKET}/output/applications_agg"

spark = (
    SparkSession
    .builder
    .appName("Module4ApplicationsETL")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)

df = (
    df
    .withColumn("event_timestamp", to_timestamp(col("event_time")))
    .withColumn("requested_amount", col("requested_amount").cast("double"))
    .withColumn("approved_amount", col("approved_amount").cast("double"))
    .withColumn("credit_score", col("credit_score").cast("double"))
    .withColumn("processing_time_sec", col("processing_time_sec").cast("double"))
)

result = (
    df
    .groupBy("region_code", "product_type", "decision_status", "risk_level")
    .agg(
        count("*").alias("applications_count"),
        spark_round(avg("requested_amount"), 2).alias("avg_requested_amount"),
        spark_round(avg("approved_amount"), 2).alias("avg_approved_amount"),
        spark_sum("approved_amount").alias("total_approved_amount"),
        spark_round(avg("credit_score"), 2).alias("avg_credit_score"),
        spark_round(avg("processing_time_sec"), 2).alias("avg_processing_time_sec")
    )
    .orderBy("region_code", "product_type", "decision_status")
)

result.write.mode("overwrite").parquet(OUTPUT_PATH)

spark.stop()