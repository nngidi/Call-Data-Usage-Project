import logging
from kafka import KafkaAdminClient
from confluent_kafka import Consumer, KafkaError
import psycopg2
import json

logging.basicConfig(level=logging.DEBUG)

admin_client = KafkaAdminClient(
    bootstrap_servers="redpanda-0:9092", client_id="example-client"
)
topics = admin_client.list_topics()

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="wtc_analytics", user="postgres", password="postgres", host="postgres"
)
cursor = conn.cursor()


# Configure Kafka consumer
conf = {
    "bootstrap.servers": "redpanda-0:9092",  # Redpanda broker address
    "group.id": "process-data-to-postgress",
    "auto.offset.reset": "earliest",
    "debug": "cgrp,protocol",
}
consumer = Consumer(conf)


consumer.subscribe(["tick-data"])


def process_message(message, table_name):
    if table_name == "forex_data.analytics_data":
        data = json.loads(message.value().decode("utf-8"))
        cursor.execute(
            "INSERT INTO forex_data.analytics_data (timestamp, pair_name, bid_price, ask_price, spread) VALUES (%s, %s, %s, %s, %s)",
            (
                data["timestamp"],
                data["pair_name"],
                data["bid_price"],
                data["ask_price"],
                data["spread"],
            ),
        )
    elif table_name == "cdr_data.analytics_data":
        data = json.loads(message.value().decode("utf-8"))
        cursor.execute(
            "INSERT INTO cdr_data.analytics_data (msisdn, tower_id, call_type, dest_nr, call_duration_sec, start_time) VALUES (%s, %s, %s, %s, %s, %s)",
            (
                data["msisdn"],
                data["tower_id"],
                data["call_type"],
                data["dest_nr"],
                data["call_duration_sec"],
                data["start_time"],
            ),
        )
    conn.commit()


for topic in ["tick-data", "cdr-data"]:
    try:
        while True:
            msg = consumer.poll(1.0)  # Timeout after 1 second
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached: {msg}")
                else:
                    print(f"Error: {msg.error()}")
            else:
                if topic == "tick-data":
                    process_message(msg, "forex_data.analytics_data")
                elif topic == "cdr-data":
                    process_message(msg, "cdr_data.analytics_data")
    except KeyboardInterrupt:
        print("Terminating consumer...")
    finally:
        consumer.close()
        cursor.close()
        conn.close()
