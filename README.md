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
<img src="images/SC4.jpg" width="800">

Test with Spark consumer:

```python
spark-submit \
  --master "local[4]" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4 \
  spark_consumer.py \
  --starting-offsets earliest
```
<img src="images/SC5.jpg" width="800">
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
