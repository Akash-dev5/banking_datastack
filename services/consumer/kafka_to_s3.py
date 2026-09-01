import boto3                #boto3 is the official Python SDK for AWS.
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime, timezone
import os
import tempfile
import logging
#from dotenv import load_dotenv


logger = logging.getLogger(__name__)


#load_dotenv()

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

BATCH_TARGETS = {
    'banking_server.public.customers': 50,
    'banking_server.public.accounts': 100,
    'banking_server.public.transactions': 250
}

buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

logger.info("Connected to Kafka. Listening for messages...")

for message in consumer:
    topic = message.topic
    event = message.value
    payload = event.get("payload", {})
    record = payload.get("after")  

    # Only take the actual row
    if record:
        buffer[topic].append(record)
        logger.info("[%s] -> %s", topic, record)
        
    
    batch_complete = all(
        len(buffer[t]) >= BATCH_TARGETS[t]
        for t in BATCH_TARGETS
    )


    if batch_complete:
        logger.info("Complete batch received. Uploading to S3...")

        # Upload customers
        upload_to_s3(
            "customers",
            buffer["banking_server.public.customers"]
        )

        # Upload accounts
        upload_to_s3(
            "accounts",
            buffer["banking_server.public.accounts"]
        )

        # Upload transactions
        upload_to_s3(
            "transactions",
            buffer["banking_server.public.transactions"]
        )


        #Clear buffer for next batch
        for topic in buffer:
            buffer[topic] = []



        #------Create batch-complete marker------


        batch_id = datetime.now(timezone.utc).strftime(
            "%Y%m%d_%H%M%S%f"
        )

        marker_key = (
            f"_batch_complete/"
            f"batch_{batch_id}.done"
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=marker_key,
            Body=b""
        )

        logger.info(
            "✅ COMPLETE BATCH uploaded. "
            "Marker created: s3://%s/%s",
            bucket,
            marker_key
        )

        logger.info("Batch complete: %s", marker_key)