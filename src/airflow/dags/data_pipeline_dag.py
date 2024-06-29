from functools import partial
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator

from src.get_api_data import get_api_data
from src.data_preparation import *
from src.constants import *

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
    'get_api_data',
    default_args=default_args,
    description='Get data from multiple APIs',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    for api_name, api_url in API_DICT.items():
        task1_get_api_data = PythonOperator(
            task_id=f'get_{api_name}_data',
            python_callable=partial(get_api_data, api_name, api_url),
            dag=dag
        )


with DAG(
    'prepare_json_data',
    default_args=default_args,
    description='DAG to prepare JSON data for upload to a database',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    for json_file in get_files_in_directory(PATH_TO_DATA_STORAGE):
        task2_prepare_json_data = PythonOperator(
            task_id=f'preparing_"{json_file}"_data',
            python_callable=partial(create_dataframe, json_file, COLS_NORMALIZE),
            dag=dag
        )
