"""
E-Commerce Analytics Pipeline DAG

Orchestrates the full pipeline:
  1. upload_bronze  — uploads raw Olist CSVs to S3 bronze layer
  2. spark_silver   — cleans and types each table, writes Parquet to S3 silver layer

Each task runs the corresponding Python script using the project's virtualenv.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path("/Users/satvikpanchal/Retail Intelligence Pipeline")
PYTHON       = PROJECT_ROOT / ".venv/bin/python"
JAVA_HOME    = "/opt/homebrew/opt/openjdk@17"

# ── Default task args ────────────────────────────────────────────────────────
default_args = {
    "owner": "satvik",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id="ecommerce_pipeline",
    description="Olist e-commerce data pipeline: bronze → silver",
    schedule=None,           # manual trigger only for now
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "bronze", "silver", "pyspark"],
)
def ecommerce_pipeline():

    upload_bronze = BashOperator(
        task_id="upload_bronze",
        bash_command=(
            f"{PYTHON} {PROJECT_ROOT}/src/ingestion/upload_bronze.py"
        ),
        env={
            "AIRFLOW_HOME": str(PROJECT_ROOT / "airflow"),
        },
        append_env=True,   # keep the existing system env vars
    )

    spark_silver = BashOperator(
        task_id="spark_silver",
        bash_command=(
            f"export JAVA_HOME={JAVA_HOME} && "
            f"export PATH={JAVA_HOME}/bin:$PATH && "
            f"{PYTHON} {PROJECT_ROOT}/src/spark/silver.py"
        ),
        env={
            "AIRFLOW_HOME": str(PROJECT_ROOT / "airflow"),
        },
        append_env=True,
        execution_timeout=timedelta(minutes=30),  # Spark can take a while
    )

    # Define order: bronze must finish before silver starts
    upload_bronze >> spark_silver


ecommerce_pipeline()
