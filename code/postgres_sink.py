"""
PostgreSQL Sink for IBM Cloud Real-Time Fraud Monitoring
========================================================

Pipeline:
IBM Event Streams (Kafka)
    -> Spark Structured Streaming
    -> Fraud Scoring
    -> PostgreSQL
    -> Apache Superset

Target:
    Database: fraud_db
    Schema: fraud_monitoring
    Table: transactions

Features:
    - Automatically loads .env
    - PostgreSQL connection management
    - Spark foreachBatch integration
    - Batched PostgreSQL UPSERT
    - Duplicate transaction prevention
    - Transaction rollback on failure
    - Detailed logging
    - PostgreSQL SSL support

Dependencies:
    pip install psycopg2-binary python-dotenv
"""

import os
import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


# ============================================================
# 1. LOAD ENVIRONMENT VARIABLES
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(
    dotenv_path=BASE_DIR / ".env",
    override=False
)


# ============================================================
# 2. LOGGING
# ============================================================

logger = logging.getLogger("PostgreSQLSink")

logger.setLevel(logging.INFO)

if not logger.handlers:

    handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    handler.setFormatter(formatter)

    logger.addHandler(handler)


# ============================================================
# 3. POSTGRESQL CONFIGURATION
# ============================================================

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud_db"),
    "user": os.getenv("POSTGRES_USER", "spark_writer"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "sslmode": os.getenv("POSTGRES_SSLMODE", "prefer"),
    "connect_timeout": 15,
}

BATCH_SIZE = 500


# ============================================================
# 4. POSTGRESQL UPSERT QUERY
# ============================================================

UPSERT_SQL = """
INSERT INTO fraud_monitoring.transactions (

    transaction_id,
    event_time,
    customer_id,
    merchant_id,
    channel,
    amount_inr,
    city,
    country,
    is_new_device,
    fraud_score,
    risk_level,
    synthetic_fraud_label,
    synthetic_scenario,
    processed_at

)

VALUES %s

ON CONFLICT (transaction_id)

DO UPDATE SET

    event_time = EXCLUDED.event_time,
    customer_id = EXCLUDED.customer_id,
    merchant_id = EXCLUDED.merchant_id,
    channel = EXCLUDED.channel,
    amount_inr = EXCLUDED.amount_inr,
    city = EXCLUDED.city,
    country = EXCLUDED.country,
    is_new_device = EXCLUDED.is_new_device,
    fraud_score = EXCLUDED.fraud_score,
    risk_level = EXCLUDED.risk_level,
    synthetic_fraud_label = EXCLUDED.synthetic_fraud_label,
    synthetic_scenario = EXCLUDED.synthetic_scenario,
    processed_at = EXCLUDED.processed_at
"""


# ============================================================
# 5. DATABASE CONNECTION
# ============================================================

def get_postgres_connection():

    if not POSTGRES_CONFIG["password"]:

        raise RuntimeError(
            "POSTGRES_PASSWORD is missing. "
            "Check the .env file."
        )

    try:

        connection = psycopg2.connect(
            **POSTGRES_CONFIG
        )

        logger.info(
            "Connected to PostgreSQL database: %s",
            POSTGRES_CONFIG["dbname"]
        )

        return connection

    except psycopg2.Error:

        logger.exception(
            "Unable to connect to PostgreSQL"
        )

        raise


# ============================================================
# 6. DATA NORMALIZATION
# ============================================================

def normalize_uuid(value):

    if value is None:

        raise ValueError(
            "transaction_id cannot be NULL"
        )

    return str(UUID(str(value)))


def normalize_timestamp(value):

    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    return datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )


def normalize_amount(value):

    if value is None:
        raise ValueError("amount_inr cannot be NULL")

    return Decimal(str(value))


# ============================================================
# 7. PREPARE SPARK TRANSACTION
# ============================================================

def prepare_transaction(row):

    data = row.asDict(recursive=True)

    # Spark uses event_timestamp.
    # PostgreSQL uses event_time.

    event_time = data.get("event_time")

    if event_time is None:
        event_time = data.get("event_timestamp")

    if event_time is None:
        raise ValueError(
            "Missing event_time/event_timestamp"
        )

    fraud_score = int(
        data.get("fraud_score") or 0
    )

    if fraud_score < 0 or fraud_score > 100:
        raise ValueError(
            f"Invalid fraud_score: {fraud_score}"
        )

    risk_level = data.get("risk_level") or "LOW"

    if risk_level not in ("LOW", "MEDIUM", "HIGH"):
        raise ValueError(
            f"Invalid risk_level: {risk_level}"
        )

    processed_at = data.get("processed_at")

    if processed_at is None:
        processed_at = datetime.now(timezone.utc)

    transaction = (

        normalize_uuid(
            data.get("transaction_id")
        ),

        normalize_timestamp(event_time),

        data.get("customer_id"),

        data.get("merchant_id"),

        data.get("channel"),

        normalize_amount(
            data.get("amount_inr")
        ),

        data.get("city"),

        data.get("country"),

        data.get("is_new_device"),

        fraud_score,

        risk_level,

        data.get("synthetic_fraud_label"),

        data.get("synthetic_scenario"),

        normalize_timestamp(processed_at)

    )

    return transaction


# ============================================================
# 8. SPARK FOREACHBATCH POSTGRESQL WRITER
# ============================================================

def write_to_postgres(batch_df, batch_id):

    logger.info(
        "Starting Spark microbatch: %s",
        batch_id
    )

    if batch_df.isEmpty():

        logger.info(
            "Microbatch %s is empty",
            batch_id
        )

        return

    connection = None

    total_rows = 0

    buffer = []

    try:

        connection = get_postgres_connection()

        connection.autocommit = False

        with connection.cursor() as cursor:

            for row in batch_df.toLocalIterator():

                transaction = prepare_transaction(row)

                buffer.append(transaction)

                if len(buffer) >= BATCH_SIZE:

                    execute_values(
                        cursor,
                        UPSERT_SQL,
                        buffer,
                        page_size=BATCH_SIZE
                    )

                    total_rows += len(buffer)

                    buffer.clear()

            # Insert remaining transactions

            if buffer:

                execute_values(
                    cursor,
                    UPSERT_SQL,
                    buffer,
                    page_size=BATCH_SIZE
                )

                total_rows += len(buffer)

                buffer.clear()

        # Commit entire Spark microbatch

        connection.commit()

        logger.info(
            "Batch %s successfully upserted %s transactions",
            batch_id,
            total_rows
        )

        print(
            f"[POSTGRES SUCCESS] "
            f"Batch={batch_id} "
            f"Transactions={total_rows}",
            flush=True
        )

    except Exception:

        if connection is not None:
            connection.rollback()

        logger.exception(
            "PostgreSQL write failed for batch %s",
            batch_id
        )

        # Propagate the exception to Spark.
        # Spark must not checkpoint a failed batch.

        raise

    finally:

        if connection is not None:
            connection.close()


# ============================================================
# 9. DATABASE CONNECTION TEST
# ============================================================

def test_connection():

    connection = None

    try:

        connection = get_postgres_connection()

        with connection.cursor() as cursor:

            cursor.execute(
                "SELECT current_database(), current_user"
            )

            result = cursor.fetchone()

            print("\nPOSTGRESQL CONNECTION SUCCESSFUL")

            print(
                "Database:",
                result[0]
            )

            print(
                "User:",
                result[1]
            )

            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'fraud_monitoring'
                    AND table_name = 'transactions'
                )
                """
            )

            table_exists = cursor.fetchone()[0]

            print(
                "Transactions table accessible:",
                table_exists
            )

            if not table_exists:
                raise RuntimeError(
                    "fraud_monitoring.transactions is not visible "
                    "to the configured PostgreSQL user"
                )

        print(
            "\nPostgreSQL sink configuration verified."
        )

    except Exception:

        logger.exception(
            "PostgreSQL connection test failed"
        )

        raise

    finally:

        if connection is not None:
            connection.close()


# ============================================================
# 10. MAIN
# ============================================================

if __name__ == "__main__":

    test_connection()
