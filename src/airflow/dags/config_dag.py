from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email": ["<EMAIL>"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}