FROM apache/airflow:3.0.6

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-fab==2.3.0" \
    "apache-airflow-providers-amazon==9.9.0" \
    "astronomer-cosmos==1.10.3"

RUN python -m venv /home/airflow/dbt_venv

RUN /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
    dbt-core \
    dbt-snowflake

COPY airflow/dags /opt/airflow/dags

COPY banking_dbt /home/airflow/banking_dbt