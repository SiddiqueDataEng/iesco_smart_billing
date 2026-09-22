"""Airflow template for city energy source ingestion and standardization."""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def ingest_sources():
    """Replace with API, object-storage, and catalog adapters per city."""
    return "source extracts registered"


def validate_and_standardize():
    """Run schema, timestamp, unit, coordinate, and referential checks."""
    return "canonical tables validated"


def publish_to_postgres():
    """Load canonical dimensions, facts, observations, and metadata."""
    return "warehouse publish complete"


with DAG(
    dag_id="city_energy_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["energy", "city", "etl"],
) as dag:
    ingest = PythonOperator(task_id="ingest_sources", python_callable=ingest_sources)
    validate = PythonOperator(task_id="validate_and_standardize", python_callable=validate_and_standardize)
    publish = PythonOperator(task_id="publish_to_postgres", python_callable=publish_to_postgres)
    ingest >> validate >> publish
