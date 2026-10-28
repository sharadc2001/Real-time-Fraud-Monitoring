# IBM Cloud Real-Time Fraud Monitoring — Synthetic Demo

**Architecture:** Python producer → IBM Event Streams (Kafka) → Spark Structured Streaming → PostgreSQL → Apache Superset.

Optional historical extension: Spark → Iceberg on IBM Cloud Object Storage → watsonx.data. This extension is **not implemented** in the demo.

## Prerequisites
- IBM Event Streams instance, bootstrap endpoint, and API key with produce/consume permissions.
- Kafka topic `bank.transactions` created in Event Streams (or configured alternate topic).
- Python 3.10+, Java and Spark 3.5.x, matching Kafka connector JARs.
- PostgreSQL database/user and Apache Superset configured to query PostgreSQL.
- Connectivity from the Spark driver **and executors** to Event Streams and PostgreSQL.
- Trusted server and secure secret storage. Do not commit `.env`.

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
chmod 600 .env
# Edit .env and supply Event Streams credentials and PostgreSQL connection details.
```

## Test locally without IBM credentials
```bash
python simulator.py --dry-run --count 5 --rate 5
```

## Start producing events
```bash
python simulator.py --rate 20 --fraud-rate 0.08
# Or for a finite test: python simulator.py --count 1000 --rate 20
```

## Initialize PostgreSQL
Run `schema.sql` using psql against your fraud database:
```bash
psql -h 127.0.0.1 -U fraud_app -d fraud -f schema.sql
```

## Run Spark streaming consumer
Spark 3.5 uses Scala 2.12 in common distributions; match Kafka package to your exact Spark build:
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --conf spark.executorEnv.POSTGRES_HOST="$POSTGRES_HOST" \
  spark_fraud.py
```

**Important:** Environment variables from `.env` are loaded by Python on the driver, but Spark executors must also receive DB settings. For local Spark, load variables in your shell (`set -a; source .env; set +a`) before `spark-submit`. For a real Spark cluster, pass credentials securely to executors via secret management; do not expose them in CLI arguments or Spark logs. Ensure psycopg2 is installed on each executor. Use durable checkpoint storage in production (not `/tmp`).

## Superset
Connect Superset to PostgreSQL with its PostgreSQL SQLAlchemy driver, register `fraud_events` as a dataset, and use the SQL in `dashboard_queries.sql` to build time-series, table, and geographic charts. Configure an appropriate refresh interval.

## Fraud scoring limitations
This is a synthetic demonstration, **not a production fraud detection model**. Risk score is based on simple amount, country, and new-device rules. `synthetic_fraud_label` is test ground truth and is deliberately **not used in scoring**. `velocity_burst` is a scenario label only; the current pipeline does **not** implement stateful 60-second velocity detection. Do not present the score as validated precision/recall or use for financial decisions.

## Security
Kafka uses SASL_SSL. Restrict PostgreSQL and Superset to private access or authenticated HTTPS; use IBM Cloud VPC security groups. Use IBM Secrets Manager or equivalent for secrets in production. Synthetic IDs only; no real PII. Review retention, encryption, access control, and audit logging before real workloads.
