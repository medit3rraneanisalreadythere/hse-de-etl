from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)

BUCKET = "etl-module4-kononenko"
KAFKA_BOOTSTRAP_SERVERS = "rc1b-skfs8mrvagu61o2a.mdb.yandexcloud.net:9091"
KAFKA_TOPIC = "loan_applications_small"

OUTPUT_PATH = f"s3a://{BUCKET}/output/kafka_flattened"
CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/kafka_flattened"

KAFKA_USERNAME = "kafka_user"
KAFKA_PASSWORD = "Password1!"

schema = StructType([
    StructField("application_id", StringType(), True),
    StructField("customer", StructType([
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True),
    ]), True),
    StructField("loan", StructType([
        StructField("amount", IntegerType(), True),
        StructField("term_months", IntegerType(), True),
    ]), True),
    StructField("scoring", StructType([
        StructField("score", IntegerType(), True),
        StructField("risk_level", StringType(), True),
    ]), True),
    StructField("documents", ArrayType(StructType([
        StructField("type", StringType(), True),
        StructField("status", StringType(), True),
    ])), True),
    StructField("decision_status", StringType(), True),
    StructField("submitted_at", StringType(), True),
])


def main():
    spark = (
        SparkSession
        .builder
        .appName("module4-kafka-flatten-stream-once")
        .getOrCreate()
    )

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
        )
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "1000")
        .load()
    )

    json_df = raw_df.selectExpr("CAST(value AS STRING) AS json_value")

    parsed_df = json_df.select(
        from_json(col("json_value"), schema).alias("data")
    )

    flat_df = (
        parsed_df
        .withColumn("document", explode(col("data.documents")))
        .select(
            col("data.application_id").alias("application_id"),
            col("data.customer.customer_id").alias("customer_id"),
            col("data.customer.region").alias("region_code"),
            col("data.loan.amount").alias("loan_amount"),
            col("data.loan.term_months").alias("term_months"),
            col("data.scoring.score").alias("credit_score"),
            col("data.scoring.risk_level").alias("risk_level"),
            col("document.type").alias("document_type"),
            col("document.status").alias("document_status"),
            col("data.decision_status").alias("decision_status"),
            col("data.submitted_at").alias("submitted_at"),
        )
    )

    query = (
        flat_df.writeStream
        .trigger(once=True)
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()