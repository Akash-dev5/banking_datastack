from airflow.sdk import dag, task, Asset, AssetWatcher
from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger

from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook

import logging

logger = logging.getLogger(__name__)

def get_snowflake_connection():
    import snowflake.connector #Airflow recommends keeping expensive imports and other heavy work out of top-level DAG code.

    sf_conn = BaseHook.get_connection("snowflake_conn")
    extra = sf_conn.extra_dejson

    return snowflake.connector.connect(
        user=sf_conn.login,
        password=sf_conn.password,
        account=extra.get("account"),
        warehouse=extra.get("warehouse"),
        database=extra.get("database"),
        role=extra.get("role"),
        schema=sf_conn.schema
    )



def copy_table(table_name):

    snowflake_conn = get_snowflake_connection()
    cursor = snowflake_conn.cursor()

    try:
        copy_sql = f"""
            COPY INTO BANKING_PROJECT.RAW.{table_name.upper()}
            FROM @BANKING_PROJECT.RAW.s3_banking_stage/{table_name}/
            FILE_FORMAT=(TYPE=PARQUET)
            Force = FALSE
            ON_ERROR='ABORT_STATEMENT';
        """
        cursor.execute(copy_sql)
        logger.info("Copied data into Snowflake table '%s'.",table_name)

    finally:
        cursor.close()
        snowflake_conn.close()


banking_s3_asset = Asset(
    name="banking_s3_data",
    uri="s3://snowflake-banking-datav1",
    watchers=[
        AssetWatcher(
            name="banking_s3_data_watcher",
            trigger=SqsSensorTrigger(
                sqs_queue="banking-s3-events",
                aws_conn_id=None,
                max_messages=5,
                wait_time_seconds=20,
                delete_message_on_reception=True,
                region_name="us-east-1",
            ),
        )
    ],
)

snowflake_raw_loaded = Asset(
    name="snowflake_raw_table_loaded",
)

@dag(
    dag_id="s3_stg_to_snowflake",
    schedule=[banking_s3_asset],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["banking", "snowflake"]
)


def s3_stg_to_snowflake():


    @task
    def load_customers():
         copy_table("customers")

    @task
    def load_accounts():
        copy_table("accounts")

    @task
    def load_transactions():
        copy_table("transactions")

    @task(outlets=[snowflake_raw_loaded])
    def raw_tables_loaded():
        logger.info("All RAW Snowflake tables loaded successfully.")

    customers = load_customers()
    accounts = load_accounts()
    transactions = load_transactions()

    completed = raw_tables_loaded()

    [customers, accounts, transactions] >> completed

s3_stg_to_snowflake()