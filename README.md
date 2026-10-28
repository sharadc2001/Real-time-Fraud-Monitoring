# Real-time-Fraud-Monitoring
Real time fraud monitoring on IBM Cloud

<H3>Reference Architecture</H3>

```text
                    SYNTHETIC BANKING TRANSACTIONS
                         Python Fraud Simulator
                      Cards | UPI | Online Banking
                                    |
                                    | JSON Events
                                    v
                         IBM EVENT STREAMS
                            Apache Kafka
                                    |
                         Topic: bank.transactions
                                    |
                                    v
                       APACHE SPARK STREAMING
                        Fraud Detection Engine
                                    |
                      +-------------+-------------+
                      |                           |
                      v                           v
                Fraud Rules                 Aggregations
              Risk Score 0–100           Merchant / Region
                      |                           |
                      +-------------+-------------+
                                    |
                                    v
                           PostgreSQL
                       Fraud Analytics Tables
                                    |
                                    v
                           APACHE SUPERSET
                         Monitoring Dashboards
                                    |
                     +--------------+--------------+
                     |              |              |
                 Fraud Rate    Risk by Region   High-Risk
                                                Merchants

             OPTIONAL HISTORICAL LAKEHOUSE PATH

                     Spark Structured Streaming
                                    |
                                    v
                         Apache Iceberg Tables
                                    |
                                    v
                       IBM Cloud Object Storage
                                    |
                                    v
                          IBM watsonx.data
                         Historical Analytics
```

<h3>Create Code folder</h3>

```python
mkdir realtime_ibmcloud

Copy code in this directory.

sudo mkdir -p /home/vpcuser/realtime_ibmcloud/checkpoints/fraud_consumer
sudo chown -R vpcuser:vpcuser \
  /home/vpcuser/realtime_ibmcloud/checkpoints
sudo chmod -R u+rwX \
  /home/vpcuser/realtime_ibmcloud/checkpoints

Verify write access:
touch ~/realtime_ibmcloud/checkpoints/fraud_consumer/.write_test \
  && rm ~/realtime_ibmcloud/checkpoints/fraud_consumer/.write_test \
  && echo "Checkpoint directory writable"
  
```

<H3>Install the Python dependencies:</H3>
```python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Generate five sample transactions without connecting to IBM Cloud:

```python
python simulator.py --dry-run --count 5
```

<img src="images/SC2.jpg" width="800">


This generates approximately 20 transactions per second, with 8% assigned synthetic fraud labels.

The Spark pipeline then processes the transactions and stores scored results for Superset.

<H3>Step 2: Configure IBM Event Streams</H3>

Use your existing Event Streams instance.

Create or confirm the Kafka topic:

<pre><code>bank.transactions</code></pre>

The simulator must use the broker endpoints and **SASL/TLS** authentication settings from your **Event Streams service** credentials.

For IBM Event Streams on IBM Cloud, the usual Kafka authentication mechanism is **SASL_SSL with PLAIN**, using the **API key as the password**. The username is typically token.

Keep credentials in your local **.env** file and never commit them to Git.

<img src="images/SC3.jpg" width="800">

<H3>Step 3: Start the simulator</H3>

Once credentials and topic configuration are validated:

```python
python simulator.py --rate 20 --fraud-rate 0.08
```

<img src="images/SC5.jpg" width="800">

Test with Spark consumer:

```python
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

```

```python
spark-submit \
  --master "local[4]" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4 \
  spark_consumer.py \
  --starting-offsets earliest
```

<img src="images/SC4.jpg" width="800">

The consumer should print micro-batches containing transaction identifiers, amounts, fraud scores, risk levels, and other transaction attributes.

```text
A successful test confirms that:
- Python publishes synthetic transactions to IBM Event Streams.
- Spark reads the Kafka topic.
- Spark parses transaction JSON.
- Fraud scoring produces values between 0 and 100.
- Transactions are classified into LOW, MEDIUM, and HIGH risk.
- Spark resumes from its checkpoint after restarting.
```
<H3>Create Database Objects</H3>

```text
| PostgreSQL user   | Purpose                    | Permissions            | 
|-------------------|----------------------------|------------------------|
| `postgres`        | Database administration    | Superuser              |
| `spark_writer`    | Spark Structured Streaming | SELECT, INSERT, UPDATE |
| `superset_reader` | Apache Superset dashboards | SELECT only            |
```

<H3>Create the spark writer user</H3>

```sql
CREATE ROLE spark_writer
WITH LOGIN PASSWORD 'REPLACE_WITH_STRONG_UNIQUE_PASSWORD';

GRANT CONNECT ON DATABASE fraud_db TO spark_writer;

GRANT USAGE ON SCHEMA fraud_monitoring TO spark_writer;

GRANT SELECT, INSERT, UPDATE
ON fraud_monitoring.transactions
TO spark_writer;
```

<H3>Create the Superset read-only user</H3>

```sql
CREATE ROLE superset_reader
WITH LOGIN PASSWORD 'REPLACE_WITH_ANOTHER_STRONG_PASSWORD';

GRANT CONNECT ON DATABASE fraud_db TO superset_reader;

GRANT USAGE ON SCHEMA fraud_monitoring TO superset_reader;

GRANT SELECT ON
    fraud_monitoring.transactions,
    fraud_monitoring.fraud_alerts,
    fraud_monitoring.merchant_risk_summary,
    fraud_monitoring.vw_fraud_dashboard,
    fraud_monitoring.vw_high_risk_transactions
TO superset_reader;

```

<H3>Configure permissions for future tables</H3>

```sql
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
IN SCHEMA fraud_monitoring
GRANT SELECT ON TABLES TO superset_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres
IN SCHEMA fraud_monitoring
GRANT SELECT, INSERT, UPDATE ON TABLES TO spark_writer;

```

<H3>Create Database</H3>

```sql
CREATE DATABASE fraud_db;
CREATE SCHEMA IF NOT EXISTS fraud_monitoring;
```

<H3>Create Transaction Table</H3>

```sql

CREATE TABLE IF NOT EXISTS fraud_monitoring.transactions (
    transaction_id UUID PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    customer_id VARCHAR(100),
    account_id VARCHAR(100),
    merchant_id VARCHAR(100),
    merchant_category VARCHAR(100),
    channel VARCHAR(50),
    amount_inr NUMERIC(18,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'INR',
    city VARCHAR(100),
    country VARCHAR(10),
    device_id VARCHAR(100),
    is_new_device BOOLEAN,
    synthetic_fraud_label BOOLEAN DEFAULT FALSE,
    synthetic_scenario VARCHAR(100),
    fraud_score INTEGER NOT NULL DEFAULT 0
        CHECK (fraud_score BETWEEN 0 AND 100),
    risk_level VARCHAR(20) NOT NULL DEFAULT 'LOW'
        CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH')),
    schema_version INTEGER DEFAULT 1,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

```

<H3>Create the fraud alerts table</H3>

```sql

CREATE TABLE IF NOT EXISTS fraud_monitoring.fraud_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL UNIQUE
        REFERENCES fraud_monitoring.transactions(transaction_id),
    customer_id VARCHAR(100),
    fraud_score INTEGER,
    risk_level VARCHAR(20),
    alert_reason TEXT,
    alert_status VARCHAR(30) DEFAULT 'OPEN',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ
);

```

<H3>Create merchant risk summary</H3>

```sql

CREATE TABLE IF NOT EXISTS fraud_monitoring.merchant_risk_summary (
    merchant_id VARCHAR(100) PRIMARY KEY,
    total_transactions BIGINT DEFAULT 0,
    suspicious_transactions BIGINT DEFAULT 0,
    total_transaction_amount NUMERIC(20,2) DEFAULT 0,
    suspicious_amount NUMERIC(20,2) DEFAULT 0,
    average_fraud_score NUMERIC(8,2) DEFAULT 0,
    last_updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

```

<H3>Create indexes</H3>

```sql

CREATE INDEX IF NOT EXISTS idx_transactions_event_time
ON fraud_monitoring.transactions(event_time DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_risk_level
ON fraud_monitoring.transactions(risk_level);

CREATE INDEX IF NOT EXISTS idx_transactions_customer
ON fraud_monitoring.transactions(customer_id);

CREATE INDEX IF NOT EXISTS idx_transactions_merchant
ON fraud_monitoring.transactions(merchant_id);

CREATE INDEX IF NOT EXISTS idx_alerts_status
ON fraud_monitoring.fraud_alerts(alert_status);

```

<H3>Create the fraud dashboard view</H3>

```sql

CREATE OR REPLACE VIEW fraud_monitoring.vw_fraud_dashboard AS
SELECT
    DATE_TRUNC('minute', event_time) AS transaction_minute,
    COUNT(*) AS total_transactions,

    COUNT(*) FILTER (
        WHERE risk_level = 'HIGH'
    ) AS high_risk_transactions,

    COUNT(*) FILTER (
        WHERE risk_level = 'MEDIUM'
    ) AS medium_risk_transactions,

    COUNT(*) FILTER (
        WHERE risk_level = 'LOW'
    ) AS low_risk_transactions,

    COUNT(*) FILTER (
        WHERE synthetic_fraud_label = TRUE
    ) AS simulated_fraud_transactions,

    SUM(amount_inr) AS total_transaction_amount,
    AVG(fraud_score) AS average_fraud_score

FROM fraud_monitoring.transactions
GROUP BY DATE_TRUNC('minute', event_time);

```

<H3>Create the high-risk transactions view</H3>

```sql

CREATE OR REPLACE VIEW fraud_monitoring.vw_high_risk_transactions AS
SELECT
    transaction_id,
    event_time,
    customer_id,
    merchant_id,
    merchant_category,
    channel,
    amount_inr,
    city,
    country,
    fraud_score,
    risk_level,
    synthetic_fraud_label,
    synthetic_scenario
FROM fraud_monitoring.transactions
WHERE risk_level = 'HIGH';

```
<H3>Switch to the correct database</H3>

```sql

\c fraud_db

```

<H3>Verify your tables and views</H3>

```sql

\dt fraud_monitoring.*

\dv fraud_monitoring.*

```

Now the database is configured, modify Spark command to add Postgresql database configuration

```python
spark-submit \
  --master "local[4]" \
  --packages \
org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4,org.postgresql:postgresql:42.7.8 \
  spark_consumer.py \
  --starting-offsets earliest

```

Install postgres python driver

```python
python -m pip install psycopg2-binary
```
