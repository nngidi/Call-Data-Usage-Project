from confluent_kafka import Consumer, KafkaException
from scylla_client import ScyllaDBClient
from utils import process_cdr_data
from time import time

# Redpanda Kafka consumer configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'cdr-consumer-group',
    'auto.offset.reset': 'earliest',
}

TOPIC = "cdr-data"

def consume_and_process():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    scylla_client = ScyllaDBClient()

    last_log_time = 0  # Track the last time an idle message was logged

    try:
        print(f"Subscribed to topic {TOPIC}. Awaiting messages...")
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                current_time = time()
                if current_time - last_log_time >= 5:  # Log every 5 seconds
                    print("consumer waiting for messages...")
                    last_log_time = current_time
                continue

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print("End of partition reached.")
                    continue
                else:
                    print(f"Kafka error: {msg.error()}")
                    break

            try:
                raw_data = msg.value().decode('utf-8')
                print(f"Consumed message: {raw_data}")

                raw_data = raw_data.replace("'", '"')
                summarized_data = process_cdr_data(raw_data)
                scylla_client.write_data(summarized_data)

            except Exception as e:
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_and_process()
