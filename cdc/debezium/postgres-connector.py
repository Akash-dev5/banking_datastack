import os
import requests
from requests.exceptions import RequestException
from dotenv import load_dotenv

load_dotenv()

connector_config = {
    "name": "banking-postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("DEBEZIUM_POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),

        # CDC Configuration
        "topic.prefix": os.getenv("KAFKA_TOPIC_PREFIX"),
        "table.include.list": os.getenv("POSTGRES_TABLE_INCLUDE_LIST"),
        "publication.autocreate.mode": os.getenv("PUBLICATION_AUTOCREATE_MODE"),
        "slot.name": os.getenv("SLOT_NAME"),
        "plugin.name": os.getenv("PLUGIN_NAME"),

        # Event Handling
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
        "time.precision.mode": "adaptive",
        "snapshot.mode": "initial",
        "heartbeat.interval.ms": "10000"
    },
}

url = f"{os.getenv('KAFKA_CONNECT_URL')}/connectors"

try:
    response = requests.post(url, json=connector_config, timeout=10)

    if response.status_code == 201:
        print("✅ Debezium connector created successfully.")
    elif response.status_code == 409:
        print("⚠️ Debezium connector already exists.")
    else:
        print(f"❌ Failed: Status code: {response.status_code}, Response: {response.text}")

except RequestException as e:
    print(f"❌ Connection failed. Error: {e}")