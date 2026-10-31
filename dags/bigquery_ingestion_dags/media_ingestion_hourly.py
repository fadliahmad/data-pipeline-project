"""Hourly ingestion: Postgres (Neon) to BigQuery bronze."""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from pipeline.assets import BRONZE_READY
from pipeline.tables import TABLES


@dag(
    dag_id="media_ingestion_hourly",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    # Watermarks live in bronze, so one run catches up everything a missed run skipped.
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["media", "medallion", "ingestion"],
)
def media_ingestion_hourly():
    @task(max_active_tis_per_dag=3)
    def extract_load(table: str):
        from pipeline.extract_load import sync_table
        return sync_table(table)

    # Last task on purpose: it runs only if every table loaded, so the asset event
    # that starts media_dbt never fires on a partial bronze.
    @task(outlets=[BRONZE_READY])
    def snapshot_article_ids():
        from pipeline.extract_load import sync_article_ids
        return sync_article_ids()

    extract_load.expand(table=list(TABLES)) >> snapshot_article_ids()


media_ingestion_hourly()
