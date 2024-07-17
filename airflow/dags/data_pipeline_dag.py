"""
Airflow setup to control the tasks of the app.
"""

import threading
from queue import Queue
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import src.get_api_data as api
import src.data_preparation as prep
import src.db_functions.data_upload_sequence as upload
import src.backup_functions as bckp
import src.logger as log


dag_logger = log.app_logger(__name__)

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["<EMAIL>"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "data_pipeline_dag",
    default_args=default_args,
    description='Data Pipeline DAG. Schedules data download, transformation and upload to a database.',
    schedule_interval=DATA_PIPELINE_DAG_SCHD,
    catchup=False,
) as data_pipeline_dag:

    # Tasks to be run
    def download_api_data():
        try:
            api.download_api_data()
        except Exception as e:
            dag_logger.error('Error occurred while downloading API data: %s\n', e, exc_info=True)

    def prepare_json_data():
        try:
            event = threading.Event()
            queue = Queue(maxsize=4)
            prep.prepare_json_data(queue, event)
        except Exception as e:
            dag_logger.error('Error occurred while transforming JSON data: %s\n', e, exc_info=True)

    def jobs_data_upload_to_db():
        try:
            event = threading.Event()
            queue = Queue(maxsize=4)
            upload.jobs_data_upload_to_db(queue, event)
        except Exception as e:
            dag_logger.error('Error occurred while uploading data to a database: %s\n', e, exc_info=True)


    # Setting tasks for Airflow
    task1 = PythonOperator(
        task_id='download_api_data',
        python_callable=download_api_data,
        dag=data_pipeline_dag,
    )

    task2 = PythonOperator(
        task_id='prepare_json_data',
        python_callable=prepare_json_data,
        dag=data_pipeline_dag,
    )

    task3 = PythonOperator(
        task_id='jobs_data_upload_to_db',
        python_callable=jobs_data_upload_to_db,
        dag=data_pipeline_dag,
    )

    # Task dependencies
    task1 >> task2 >> task3


with DAG(
    "database_backup_dag",
    default_args=default_args,
    description='Database backup DAG. Controls the backups schedule.',
    schedule_interval=DATABASE_BACKUP_DAG_SCHD,
    catchup=False,
) as backup_dag:

    def database_backup():
        try:
            bckp.database_backup()
        except Exception as e:
            dag_logger.error('Error occurred while backing up a database: %s\n', e, exc_info=True)


    task4 = PythonOperator(
        task_id='database_backup',
        python_callable=database_backup,
        dag=backup_dag,
    )

    task4