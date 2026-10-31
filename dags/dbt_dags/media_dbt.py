"""dbt builds silver then gold, each time media_ingestion_hourly finishes loading bronze."""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

from pipeline.assets import BRONZE_READY

DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"
DBT_ARGS = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"


@dag(
    dag_id="media_dbt",
    schedule=[BRONZE_READY],
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["media", "medallion", "dbt"],
)
def media_dbt():
    # Seeds feed silver and scd_authors reads std_authors, so both belong to this
    # layer. Cautious selection leaves tests that also read gold for dbt_gold, which
    # would otherwise run here against a gold table from the previous run.
    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command=(
            f"{DBT} build {DBT_ARGS} "
            '--select "resource_type:seed path:models/silver resource_type:snapshot" '
            "--indirect-selection cautious"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command=f'{DBT} build {DBT_ARGS} --select "path:models/gold"',
        execution_timeout=timedelta(minutes=30),
    )

    dbt_silver >> dbt_gold


media_dbt()
