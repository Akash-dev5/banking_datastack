import boto3                #boto3 is the official Python SDK for AWS.
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime, timezone
import os
import tempfile
import logging
from dotenv import load_dotenv


# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

logger = logging.getLogger(__name__)
# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

# Kafka consumer settings
consumer = KafkaConsumer(
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MinIO client
s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        region_name=os.getenv("S3_REGION")
    )

bucket = os.getenv("S3_BUCKET")


# Consume and write function
def upload_to_s3(table_name, records):

    if not records:
        return

    current_time = datetime.now(timezone.utc)
    date_str = current_time.strftime("%Y-%m-%d")
    timestamp = current_time.strftime("%H%M%S%f")

    df = pd.DataFrame(records)

    with tempfile.NamedTemporaryFile(
        suffix=".parquet",
        delete=False
    ) as temp_file:                            
        file_path = temp_file.name

    try:
        df.to_parquet(                                         
            file_path,
            engine="fastparquet",
            index=False)

        s3_key = (
            f"{table_name}/date={date_str}/"
            f"{table_name}_{timestamp}.parquet")

        s3_client.upload_file(
            file_path,
            bucket,
            s3_key)

        logger.info(
            "Uploaded %d records to s3://%s/%s",
            len(records),
            bucket,
            s3_key)
            #logger.info(f"Uploaded {len(records)} records to s3://{bucket}/{s3_key}")

    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

# Batch consume
batch_size = 50
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

print("✅ Connected to Kafka. Listening for messages...")

for message in consumer:
    topic = message.topic
    event = message.value
    payload = event.get("payload", {})
    record = payload.get("after")  # Only take the actual row

    if record:
        buffer[topic].append(record)
        print(f"[{topic}] -> {record}")  # Debugging

    if len(buffer[topic]) >= batch_size:
        upload_to_s3(topic.split('.')[-1], buffer[topic])
        buffer[topic] = []
