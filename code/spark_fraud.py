#!/usr/bin/env python3
"""Spark Structured Streaming Kafka -> fraud scoring -> PostgreSQL.

Requires Kafka connector package matching your Spark/Scala versions.
Demo scoring rules are illustrative, NOT a validated fraud model.
"""
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F, types as T

load_dotenv()

schema = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("account_id", T.StringType()),
    T.StructField("merchant_id", T.StringType()),
    T.StructField("merchant_category", T.StringType()),
    T.StructField("channel", T.StringType()),
    T.StructField("amount_inr", T.DoubleType()),
    T.StructField("currency", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("device_id", T.StringType()),
    T.StructField("is_new_device", T.BooleanType()),
    T.StructField("synthetic_fraud_label", T.BooleanType()),
    T.StructField("synthetic_scenario", T.StringType()),
    T.StructField("schema_version", T.IntegerType()),
])

spark = SparkSession.builder.appName("ibm-fraud-monitor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
api_key = os.environ["KAFKA_API_KEY"]
if api_key == "replace_me":
    raise RuntimeError("Set a real KAFKA_API_KEY")

# Spark Kafka client uses SASL/PLAIN over TLS for IBM Event Streams.
jaas = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="token" password="%s";' % api_key.replace('\\', '\\\\').replace('"', '\\"')
raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", os.getenv("KAFKA_TOPIC", "bank.transactions"))
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas)
    .load())

parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
    .select("e.*")
    .filter(F.col("transaction_id").isNotNull())
    .withColumn("event_time", F.to_timestamp("event_time")))

# Stateful rolling velocity by customer using Spark's stream-stream windows would
# complicate an initial deployment. This demo scores per event only; it does NOT
# claim to detect velocity bursts. The velocity_count_60s field stays NULL.
score = (F.when(F.col("amount_inr") >= 100000, 65).otherwise(0)
         + F.when(F.col("country") != "IN", 50).otherwise(0)
         + F.when(F.col("is_new_device") == True, 45).otherwise(0))
scored = (parsed.withColumn("risk_score", F.least(F.lit(100), score))
    .withColumn("risk_level", F.when(F.col("risk_score") >= 60, "HIGH")
                                .when(F.col("risk_score") >= 30, "MEDIUM")
                                .otherwise("LOW"))
    .withColumn("velocity_count_60s", F.lit(None).cast("integer")))

cols = ["transaction_id", "event_time", "customer_id", "account_id", "merchant_id", "merchant_category",
        "channel", "amount_inr", "currency", "city", "country", "device_id", "is_new_device",
        "synthetic_fraud_label", "synthetic_scenario", "velocity_count_60s", "risk_score", "risk_level"]

# psycopg2 per partition, idempotent INSERT on transaction_id.
def write_batch(df, batch_id):
    import psycopg2
    from psycopg2.extras import execute_values
    def write_partition(rows):
        conn = psycopg2.connect(host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
                                port=int(os.getenv("POSTGRES_PORT", "5432")),
                                dbname=os.getenv("POSTGRES_DB", "fraud"),
                                user=os.getenv("POSTGRES_USER", "fraud_app"),
                                password=os.environ["POSTGRES_PASSWORD"],
                                connect_timeout=10)
        try:
            with conn:
                with conn.cursor() as cur:
                    batch = []
                    def flush():
                        if batch:
                            sql = "INSERT INTO fraud_events (" + ",".join(cols) + ") VALUES %s ON CONFLICT (transaction_id) DO NOTHING"
                            execute_values(cur, sql, batch, page_size=500)
                            batch.clear()
                    for row in rows:
                        batch.append(tuple(row[c] for c in cols))
                        if len(batch) >= 500:
                            flush()
                    flush()
        finally:
            conn.close()
    df.select(*cols).foreachPartition(write_partition)

query = (scored.writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", os.getenv("SPARK_CHECKPOINT", "/tmp/fraud-spark-checkpoint"))
    .trigger(processingTime="5 seconds")
    .start())
query.awaitTermination()
