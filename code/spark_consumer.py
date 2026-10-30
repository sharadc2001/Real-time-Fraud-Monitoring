#!/usr/bin/env python3
"""Consume synthetic bank transactions from IBM Event Streams with Spark Structured Streaming.

Requires a matching Spark Kafka connector supplied to spark-submit via --packages.
"""
import argparse
import os
import sys
from pathlib import Path
from postgres_sink import write_to_postgres

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def load_env(path: str) -> None:
    """Read a simple .env file without requiring python-dotenv on Spark executors."""
    env_file = Path(path)
    if not env_file.is_file():
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(env_file, override=False)
    except ImportError:
        for raw in env_file.read_text(encoding='utf-8').splitlines():
            line = raw.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            key = key.strip()
            if not key.isidentifier():
                continue
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def get_setting(*names: str, default: str = '') -> str:
    for name in names:
        val = os.getenv(name)
        if val:
            return val
    return default


def main() -> None:
    parser = argparse.ArgumentParser(description='IBM Event Streams Spark fraud consumer')
    parser.add_argument('--env-file', default='.env')
    parser.add_argument('--topic', default=None)
    parser.add_argument('--starting-offsets', choices=['earliest', 'latest'], default='latest')
    parser.add_argument('--checkpoint', default='./checkpoints/fraud_consumer')
    parser.add_argument('--trigger-seconds', type=int, default=5)
    parser.add_argument('--max-offsets-per-trigger', type=int, default=10000)
    args = parser.parse_args()
    load_env(args.env_file)

    brokers = get_setting('KAFKA_BOOTSTRAP_SERVERS', 'EVENT_STREAMS_BROKERS')
    api_key = get_setting('KAFKA_API_KEY', 'EVENT_STREAMS_API_KEY')
    topic = args.topic or get_setting('KAFKA_TOPIC', default='bank.transactions')
    if not brokers or not api_key:
        sys.exit('Missing KAFKA_BOOTSTRAP_SERVERS or KAFKA_API_KEY in environment/.env')
    if args.trigger_seconds < 1 or args.max_offsets_per_trigger < 1:
        sys.exit('--trigger-seconds and --max-offsets-per-trigger must be positive')
    if any(c in api_key for c in ('"', '\\', '\n', '\r')):
        sys.exit('API key contains characters requiring JAAS escaping; configure a safe secret injection method')

    # IBM Event Streams commonly uses SASL_SSL + PLAIN with username "token".
    # Do not log the JAAS string or the Spark Kafka options: they contain credentials.
    jaas = (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="token" password="{api_key}";'
    )

    spark = (
        SparkSession.builder
        .appName('IBMCloudFraudStreamingConsumer')
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.shuffle.partitions', '4')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')

    schema = T.StructType([
        T.StructField('transaction_id', T.StringType()),
        T.StructField('event_time', T.StringType()),
        T.StructField('customer_id', T.StringType()),
        T.StructField('account_id', T.StringType()),
        T.StructField('merchant_id', T.StringType()),
        T.StructField('merchant_category', T.StringType()),
        T.StructField('channel', T.StringType()),
        T.StructField('amount_inr', T.DoubleType()),
        T.StructField('currency', T.StringType()),
        T.StructField('city', T.StringType()),
        T.StructField('country', T.StringType()),
        T.StructField('device_id', T.StringType()),
        T.StructField('is_new_device', T.BooleanType()),
        T.StructField('synthetic_fraud_label', T.BooleanType()),
        T.StructField('synthetic_scenario', T.StringType()),
        T.StructField('schema_version', T.IntegerType()),
    ])

    raw = (
        spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', brokers)
        .option('subscribe', topic)
        .option('startingOffsets', args.starting_offsets)
        .option('maxOffsetsPerTrigger', args.max_offsets_per_trigger)
        .option('failOnDataLoss', 'true')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.mechanism', 'PLAIN')
        .option('kafka.sasl.jaas.config', jaas)
        .load()
    )

    parsed = (
        raw.select(
            F.col('timestamp').alias('kafka_timestamp'),
            F.col('partition').alias('kafka_partition'),
            F.col('offset').alias('kafka_offset'),
            F.from_json(F.col('value').cast('string'), schema).alias('txn'),
        )
        .select('kafka_timestamp', 'kafka_partition', 'kafka_offset', 'txn.*')
        .withColumn('event_timestamp', F.to_timestamp('event_time'))
        .filter(
            F.col('transaction_id').isNotNull()
            & F.col('event_timestamp').isNotNull()
            & F.col('amount_inr').isNotNull()
            & (F.col('amount_inr') >= 0)
        )
    )

    category = F.upper(F.coalesce(F.col('merchant_category'), F.lit('')))
    country = F.upper(F.coalesce(F.col('country'), F.lit('')))
    high_risk_categories = ['GAMBLING', 'CRYPTO', 'HIGH_RISK']

    scored = (
        parsed
        .withColumn('amount_points', F.when(F.col('amount_inr') > 100000, 35).otherwise(0))
        .withColumn('new_device_points', F.when(F.col('is_new_device') == True, 20).otherwise(0))
        .withColumn('foreign_country_points', F.when((country != '') & (country != 'IN'), 25).otherwise(0))
        .withColumn('merchant_points', F.when(category.isin(high_risk_categories), 15).otherwise(0))
        .withColumn('fraud_score', F.least(F.lit(100),
            F.col('amount_points') + F.col('new_device_points')
            + F.col('foreign_country_points') + F.col('merchant_points')))
        .withColumn('risk_level',
            F.when(F.col('fraud_score') >= 60, 'HIGH')
            .when(F.col('fraud_score') >= 30, 'MEDIUM')
            .otherwise('LOW'))
        .withColumn('processed_at', F.current_timestamp())
        .select('transaction_id', 'event_timestamp', 'customer_id', 'merchant_id',
                'channel', 'amount_inr', 'city', 'country', 'is_new_device',
                'fraud_score', 'risk_level', 'synthetic_fraud_label',
                'synthetic_scenario', 'processed_at', 'kafka_partition', 'kafka_offset')
    )

    print(f'Starting Spark Kafka consumer for topic: {topic}', flush=True)
    print('Fraud scores are illustrative; synthetic_fraud_label is simulator ground truth.', flush=True)
    print('Existing checkpoints override --starting-offsets.', flush=True)
    query = (
       scored.writeStream
       .foreachBatch(write_to_postgres)
       .outputMode('append')
       .option(
        'checkpointLocation',
        '/home/vpcuser/realtime_ibmcloud/checkpoints/fraud_postgres'
       )
       .trigger(processingTime=f'{args.trigger_seconds} seconds')
       .start()            
    )
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
    finally:
        spark.stop()


if __name__ == '__main__':
    main()

