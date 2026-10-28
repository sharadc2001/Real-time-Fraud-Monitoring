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


After configuring your Event Streams credentials in .env, start publishing transactions:

```python
python simulator.py --rate 20 --fraud-rate 0.08
```

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


