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
            f'"{PYTHON}" "{PROJECT_ROOT}/src/ingestion/upload_bronze.py"'
        ),
        env={
            "AIRFLOW_HOME": str(PROJECT_ROOT / "airflow"),
        },
        append_env=True,
    )

    spark_silver = BashOperator(
        task_id="spark_silver",
        bash_command=(
            f'export JAVA_HOME="{JAVA_HOME}" && '
            f'export PATH="{JAVA_HOME}/bin":$PATH && '
            f'export SPARK_HOME="{PROJECT_ROOT}/.venv/lib/python3.13/site-packages/pyspark" && '
            f'export SPARK_CONF_DIR="{PROJECT_ROOT}/conf" && '
            f'export PYSPARK_PYTHON="{PYTHON}" && '
            f'export PYSPARK_DRIVER_PYTHON="{PYTHON}" && '
            f'unset HADOOP_CONF_DIR && '
            f'unset HADOOP_HOME && '
            f'unset YARN_CONF_DIR && '
            f'"{PYTHON}" "{PROJECT_ROOT}/src/spark/silver.py"'
        ),
        env={
            "AIRFLOW_HOME": str(PROJECT_ROOT / "airflow"),
            "PATH": f"{JAVA_HOME}/bin:/usr/bin:/bin",
        },
        append_env=False,
        execution_timeout=timedelta(minutes=30),
    )

    # Define order: bronze must finish before silver starts
    upload_bronze >> spark_silver


ecommerce_pipeline()
