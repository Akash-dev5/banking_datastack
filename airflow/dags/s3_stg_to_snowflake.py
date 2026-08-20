import snowflake.connector
import logging
from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook


logger = logging.getLogger(__name__)

def get_snowflake_connection():
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

@dag(
    dag_id="s3_stg_to_snowflake",
    schedule="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["banking", "snowflake"]
)


def s3_to_snowflake():


    @task
    def load_customers():
         copy_table("customers")

    @task
    def load_accounts():
        copy_table("accounts")

    @task
    def load_transactions():
        copy_table("transactions")


    load_customers()
    load_accounts()
    load_transactions()


s3_to_snowflake()