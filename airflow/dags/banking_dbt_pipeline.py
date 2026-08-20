from datetime import datetime
from pathlib import Path

from airflow.sdk import dag
from cosmos import (DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig)

DBT_PROJECT_PATH = Path("/home/airflow/banking_dbt")
DBT_PROFILES_PATH = DBT_PROJECT_PATH / ".dbt" / "profiles.yml"
DBT_EXECUTABLE = "/home/airflow/dbt_venv/bin/dbt"


@dag(
    dag_id="banking_dbt_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
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

#DbtTaskGroup  It converts the dbt project's DAG into Airflow tasks while preserving the dbt dependencies.
#ProjectConfig   It tells cosmos where the dbt project is. (dbt_project.yml is the main configuration file for the dbt project.)
#ProfileConfig  It cosmos which dbt profile to use.
#ExecutionConfig   It tells cosmos "When you need to execute dbt, use this particular dbt executable.
