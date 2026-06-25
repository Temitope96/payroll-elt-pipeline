# Airflow reads this

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "temitope",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="payroll_elt_pipeline",
    description="Global Payroll ELT: ingest CSVs → PostgreSQL → dbt",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["payroll", "elt", "portfolio"],
) as dag:

    def ingest_raw_data():
        """Task 1: Generate synthetic payroll CSV files"""
        import sys
        import subprocess
        result = subprocess.run(
            [sys.executable, "/opt/airflow/ingestion/generate_data.py"],
            capture_output=True,
            text=True,
            env={**os.environ, "DATA_OUTPUT_DIR": "/opt/airflow/data"}
        )
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        if result.returncode != 0:
            raise Exception(f"Data generation failed:\n{result.stderr}")

    def validate_files():
        """Task 2: Check expected files exist and are non-empty"""
        expected = [
            "/opt/airflow/data/employees.csv",
            "/opt/airflow/data/pay_periods.csv",
            "/opt/airflow/data/transactions.csv",
        ]
        for filepath in expected:
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"Missing: {filepath}")
            size = os.path.getsize(filepath)
            if size < 100:
                raise ValueError(f"File too small: {filepath} ({size} bytes)")
            print(f"OK: {filepath} ({size:,} bytes)")

    def load_to_staging():
        """Task 3: Load CSVs into PostgreSQL staging schema"""
        import sys
        import subprocess
        result = subprocess.run(
            [sys.executable, "/opt/airflow/ingestion/load_to_postgres.py"],
            capture_output=True,
            text=True,
            env={**os.environ, "DATA_OUTPUT_DIR": "/opt/airflow/data",
                 "DB_HOST": "postgres"}
        )
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        if result.returncode != 0:
            raise Exception(f"Load failed:\n{result.stderr}")

    task_ingest = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=ingest_raw_data,
    )

    task_validate = PythonOperator(
        task_id="validate_files",
        python_callable=validate_files,
    )

    task_load = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging,
    )

    task_dbt = BashOperator(
        task_id="trigger_dbt_run",
        bash_command="echo 'dbt step — coming soon'",
    )

    task_ingest >> task_validate >> task_load >> task_dbt


