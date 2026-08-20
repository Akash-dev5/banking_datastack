FROM apache/airflow:3.0.6

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-fab" \
    "astronomer-cosmos>=1.10.0"

RUN python -m venv /home/airflow/dbt_venv

RUN /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
    dbt-core \
    dbt-snowflake
    
#Airflow 3 separates authentication into an auth manager.
#The FAB auth manager provides the users CLI commands, including
#create, list, and reset-password.(In modern airflow 3+ without
#auth manager commands like (airflow users create) will not work)

#In airflow 3, dag processsor also need seperate container( it is not
#maintained by airflow scheduler like airflow 2.9 does)

#adding this line in docker environment settings to make fab auth manager work
#AIRFLOW__CORE__AUTH_MANAGER: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager

#--> now the FAB auth manager will be configured as the active auth manager.
#-->by default airflow is using the SimpleAuthManager