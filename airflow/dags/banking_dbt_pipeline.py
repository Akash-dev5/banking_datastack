from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, Asset
from cosmos import (DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig)

DBT_PROJECT_PATH = Path("/home/airflow/banking_dbt")
DBT_PROFILES_PATH = DBT_PROJECT_PATH / ".dbt" / "profiles.yml"
DBT_EXECUTABLE = "/home/airflow/dbt_venv/bin/dbt"


snowflake_raw_loaded = Asset(
    name="snowflake_raw_table_loaded",
)


@dag(
    dag_id="banking_dbt_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule=[snowflake_raw_loaded],
    catchup=False,
    max_active_runs=1,
    tags=["banking", "dbt", "snowflake"],
)


def banking_dbt_pipeline():

    dbt = DbtTaskGroup(
        group_id = "dbt_transformations",

        project_config=ProjectConfig(
            dbt_project_path = DBT_PROJECT_PATH,
        ),

        profile_config=ProfileConfig(
            profile_name="banking_dbt",
            target_name="dev",
            profiles_yml_filepath=DBT_PROFILES_PATH,
        ),

        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE,
        ),
    )

banking_dbt_pipeline()