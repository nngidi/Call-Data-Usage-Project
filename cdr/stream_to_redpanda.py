from confluent_kafka import Producer
import paramiko
import csv
import os
import logging

import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Redpanda Kafka configuration
KAFKA_BROKER = 'localhost:19092'  # Redpanda broker address
KAFKA_TOPIC = 'cdr-data'          # Kafka topic name

# SFTP Configuration
SFTP_HOSTNAME = "localhost"
SFTP_PORT = 10022
SFTP_USERNAME = "cdr_data"
SFTP_PASSWORD = "password"

# Kafka Producer Initialization
producer = Producer({'bootstrap.servers': KAFKA_BROKER})


def error_callback(err, msg):
    """Callback function to handle Kafka errors."""
    if err:
        logger.error(f"Kafka error: {err} for message: {msg}")
    else:
        logger.info(f"Message successfully produced to topic {msg.topic()} with key {msg.key()}")


def stream_to_redpanda(file_path):
    """Reads a file and streams its data to Redpanda topic."""
    try:
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for record in reader:
                producer.produce(KAFKA_TOPIC, key=record['msisdn'], value=str(record), callback=error_callback)
                producer.flush()
        logger.info(f"File {file_path} streamed successfully to topic {KAFKA_TOPIC}.")
    except Exception as e:
        logger.error(f"Failed to stream data from file {file_path}: {e}")


def download_from_sftp(remote_path, local_path):
    """Downloads files from SFTP."""
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(SFTP_HOSTNAME, port=SFTP_PORT, username=SFTP_USERNAME, password=SFTP_PASSWORD)
        sftp = ssh.open_sftp()
        sftp.get(remote_path, local_path)
        logger.info(f"Downloaded {remote_path} to {local_path}.")
        sftp.close()
        ssh.close()
    except Exception as e:
        logger.error(f"Failed to download {remote_path}: {e}")

def poll_sftp_and_process():
    while True:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(SFTP_HOSTNAME, port=SFTP_PORT, username=SFTP_USERNAME, password=SFTP_PASSWORD)
            sftp = ssh.open_sftp()
            files = sftp.listdir('/home/cdr_data')

            for file in files:
                remote_file = f"/home/cdr_data/{file}"
                local_file = file
                download_from_sftp(remote_file, local_file)
                stream_to_redpanda(local_file)
                sftp.remove(remote_file)  # Delete file after processing
                logger.info(f"Processed and deleted remote file {remote_file}.")

            sftp.close()
            ssh.close()
        except Exception as e:
            logger.error(f"Error during polling: {e}")

        time.sleep(30)  # Polling interval

# Example usage
if __name__ == "__main__":
    remote_file = "/home/cdr_data/cdr_data_20240101_000000.csv"
    local_file = "cdr_data_20240101_000000.csv"

    download_from_sftp(remote_file, local_file)
    stream_to_redpanda(local_file)
    poll_sftp_and_process()

    # Clean up
    if os.path.exists(local_file):
        os.remove(local_file)
        logger.info(f"Deleted local file {local_file}.")
