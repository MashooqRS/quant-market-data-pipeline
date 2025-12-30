from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Define default arguments for the DAG
default_args = {
    "owner": "quant_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    dag_id="point72_quant_pipeline",  # The name you see in the UI
    default_args=default_args,
    description="End-to-end Quant Data Pipeline (Alpaca -> Postgres)",
    schedule_interval="0 22 * * 1-5", # At 22:00 UTC (17:00 EST) every weekday (i hr after market close)
    start_date=datetime(2023, 1, 1),
    catchup=False,  # Don't run for past dates
    tags=["quant", "etl"],
) as dag:

    # Task 1: Ingest market data (Alpaca)
    # IMPORTANT FIX:
    # Your docker-compose mounts repo root to /opt/airflow/project
    # so scripts must be called from /opt/airflow/project/...
    ingest_task = BashOperator(
    task_id="ingest_market_data",
    bash_command="python3 /opt/airflow/project/ingest_data.py",
    env={
        "APCA_API_KEY_ID": os.environ.get("APCA_API_KEY_ID", ""),
        "APCA_API_SECRET_KEY": os.environ.get("APCA_API_SECRET_KEY", ""),
    },
    append_env=True,
)

transform_task = BashOperator(
    task_id="transform_and_load",
    bash_command="python3 /opt/airflow/project/transform_data.py",
    env={"DB_HOST": "quant_db"},
    append_env=True,
)

validate_task = BashOperator(
    task_id='validate_loaded_data',
    bash_command='python3 /opt/airflow/project/validate_data.py',
    env={
        'DB_HOST': 'quant_db',
        'DB_PORT': '5432',
        'DB_NAME': 'quant_data_db',
        'DB_USER': 'mashooqrabbanishaik',
        'DB_PASS': 'quant@300',
        'DB_TABLE': 'intraday_data',
        'EXPECTED_BARS': '78',
        'FAIL_MISSING_BARS_GT': '2'
    },
    append_env=True
)


    # Final dependency chain
ingest_task >> transform_task >> validate_task
