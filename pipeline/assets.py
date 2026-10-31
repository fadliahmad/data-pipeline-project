"""Airflow assets shared by the DAGs, so producer and consumer cannot drift apart."""
from airflow.sdk import Asset

BRONZE_READY = Asset("media_bronze_ready")
