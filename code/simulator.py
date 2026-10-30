#!/usr/bin/env python3
"""Synthetic financial events to IBM Event Streams (Kafka); NO real customer data."""
import argparse
import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()
RUNNING = True

def stop(*_):
    global RUNNING
    RUNNING = False

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

CITIES = [("Mumbai", "IN"), ("Delhi", "IN"), ("Bengaluru", "IN"), ("Chennai", "IN"), ("Pune", "IN"), ("Singapore", "SG")]
MERCHANTS = ["GROCERY", "FUEL", "ECOMMERCE", "TRAVEL", "RESTAURANT", "ELECTRONICS"]
CHANNELS = ["UPI", "CARD", "NET_BANKING", "WALLET"]


def make_event(rng, customer_id, fraud_probability):
    fraud = rng.random() < fraud_probability
    city, country = rng.choice(CITIES[:5])
    merchant = rng.choice(MERCHANTS)
    amount = round(rng.lognormvariate(7.0, 0.9), 2)
    device_id = f"device-{rng.randint(1, 1500):05d}"
    scenario = "none"
    if fraud:
        scenario = rng.choice(["high_amount", "foreign_location", "unrecognized_device", "velocity_burst"])
        if scenario == "high_amount":
            amount = round(rng.uniform(100000, 500000), 2)
        elif scenario == "foreign_location":
            city, country = "Singapore", "SG"
        elif scenario == "unrecognized_device":
            device_id = f"unknown-{uuid.uuid4().hex[:10]}"
        elif scenario == "velocity_burst":
            amount = round(rng.uniform(1000, 12000), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "customer_id": customer_id,
        "account_id": f"account-{customer_id.split('-')[-1]}",
        "merchant_id": f"merchant-{rng.randint(1, 200):04d}",
        "merchant_category": merchant,
        "channel": rng.choice(CHANNELS),
        "amount_inr": amount,
        "currency": "INR",
        "city": city,
        "country": country,
        "device_id": device_id,
        "is_new_device": scenario == "unrecognized_device",
        "synthetic_fraud_label": fraud,
        "synthetic_scenario": scenario,
        "schema_version": 1,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=float(os.getenv("TRANSACTIONS_PER_SECOND", "10")))
    parser.add_argument("--fraud-rate", type=float, default=float(os.getenv("FRAUD_PROBABILITY", "0.08")))
    parser.add_argument("--count", type=int, default=0, help="Stop after N messages; 0 means unlimited")
    parser.add_argument("--dry-run", action="store_true", help="Print JSON without connecting to Kafka")
    args = parser.parse_args()
    if args.rate <= 0 or not 0 <= args.fraud_rate <= 1 or args.count < 0:
        parser.error("rate must be positive, fraud-rate 0..1, count >= 0")
    rng = random.Random(int(os.getenv("SEED", "42")))
    topic = os.getenv("KAFKA_TOPIC", "bank.transactions")
    producer = None
    if not args.dry_run:
        brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
        api_key = os.getenv("KAFKA_API_KEY", "")
        if not brokers or not api_key or api_key == "replace_me":
            parser.error("Set KAFKA_BOOTSTRAP_SERVERS and KAFKA_API_KEY in .env")
        producer = Producer({
            "bootstrap.servers": brokers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "token",
            "sasl.password": api_key,
            "client.id": "fraud-simulator",
            "enable.idempotence": True,
            "acks": "all",
        })
    sent = 0
    next_time = time.monotonic()
    failures = 0
    def delivery(err, _msg):
        nonlocal failures
        if err:
            failures += 1
            print(f"Delivery error: {err}", flush=True)
    try:
        while RUNNING and (args.count == 0 or sent < args.count):
            customer_id = f"customer-{rng.randint(1, 1000):05d}"
            event = make_event(rng, customer_id, args.fraud_rate)
            if args.dry_run:
                print(json.dumps(event))
            else:
                payload = json.dumps(event).encode("utf-8")
                while RUNNING:
                    try:
                        producer.produce(topic, key=customer_id.encode(), value=payload, callback=delivery)
                        break
                    except BufferError:
                        producer.poll(0.5)
                producer.poll(0)
            sent += 1
            next_time += 1.0 / args.rate
            delay = next_time - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            elif delay < -1:
                next_time = time.monotonic()
    finally:
        if producer:
            remaining = producer.flush(30)
            if remaining:
                print(f"WARNING: {remaining} messages not confirmed", flush=True)
            print(f"Enqueued={sent}, delivery_failures={failures}", flush=True)
        else:
            print(f"Generated={sent}", flush=True)

if __name__ == "__main__":
    main()
