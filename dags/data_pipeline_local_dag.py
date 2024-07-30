from threading import Event
from queue import Queue
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pendulum import timezone

from sqlalchemy.exc import (OperationalError, DBAPIError, DatabaseError,
                            DisconnectionError, ProgrammingError, SQLAlchemyError)

import src.get_api_data as api
import src.data_preparation as prep
import src.db_functions.data_movement as db

from main import (upload_to_staging,
                  prepare_json_data,
                  jobs_data_upload_to_db)
# import src.get_api_data as api
import src.logger as log

from src.constants import (API_DICT, STAGING_TABLE,
                           CLEAN_DATA_TABLE, COLS_NORMALIZE, REGIONS,
                           COLUMN_RENAME_MAP, COMMON_TABLE_SCHEMA,
                           DATETIME_COLUMNS, STR_TO_FLOAT_SCHEMA)

dag_logger = log.app_logger(__name__)

# DAG SCHEDULES
DATA_PIPELINE_DAG_SCHD = "0 */4 * * *"
DATABASE_BACKUP_DAG_SCHD = "0 */6 * * *"

# DAGs SETUP
default_args = {
    "owner": "donatas_repecka",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 23, tzinfo=timezone('Europe/Vilnius')),
    "email": ["<EMAIL>"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

def upload_to_staging(json_data: dict,
                      api_name: str) -> None:
    """
    Setting up the sequence in which to move the API
    data to the staging table in the database.
    """
    try:
        with db.DataUpload() as upload:
            upload.create_schemas()
            upload.create_tables()
            tables_in_db = upload.get_tables_in_db()

            for schema, tables in tables_in_db.items():
                dag_logger.info('Tables in "%s" schema: "%s"', schema, tables)

            upload.load_to_staging(json_data, api_name, table_name=STAGING_TABLE)
            dag_logger.info('Loading "%s" API data to a "%s" table.', api_name, STAGING_TABLE)

    except (ProgrammingError, OperationalError, DatabaseError,
            DisconnectionError, DBAPIError, AttributeError) as e:
        dag_logger.error("A SQLAlchemy error occurred while loading the data: %s.", e, exc_info=True)
    except SQLAlchemyError as e:
        dag_logger.error("A SQLAlchemy error occurred while loading the data: %s", e, exc_info=True)
    except Exception as e:
        dag_logger.error("An unexpected error occurred while loading the data: %s.", e, exc_info=True)


# SETTING THE LOGIC FOR JSON FILE NORMALIZATION
def prepare_json_data(api_name: str,
                      queue: Queue,
                      event: Event) -> None:
    """
    Setting up the sequence in which
    to execute data preparation functions.
    The JSON files are turned into pandas DataFrame's
    and put into a queue.
    """
    while not event.is_set():
        try:
            with db.DataUpload() as upload:
                json_data = upload.get_data_from_staging(staging_schema='staging',
                                                         staging_table=STAGING_TABLE,
                                                         api_name=api_name)
                dag_logger.info('Retrieved data for "%s".', api_name)

                json_to_df = prep.create_dataframe(json_data, COLS_NORMALIZE)
                json_region = prep.assign_region(json_to_df, REGIONS)
                json_flat = prep.flatten_json_file(json_region)
                json_salary = prep.salary_extraction(json_flat)
                json_time = prep.add_timestamp(json_salary)
                json_names = prep.rename_columns(json_time, COLUMN_RENAME_MAP)
                json_reorder = prep.reorder_dataframe_columns(json_names, COMMON_TABLE_SCHEMA)
                json_time_format = prep.change_datetime_format(json_reorder, DATETIME_COLUMNS)
                json_dtypes = prep.str_to_float_schema(json_time_format, STR_TO_FLOAT_SCHEMA)

                dag_logger.info('A dataframe for "%s" was created.', api_name)

                queue.put([json_dtypes, api_name])
                dag_logger.info('A dataframe for "%s" was put to a queue.', api_name)

            event.set()

        except Exception as e:
            dag_logger.error("An error occurred while creating a dataframe:\n %s\n", e, exc_info=True)


def jobs_data_upload_to_db(queue: Queue, event: Event) -> None:
    """
    Setting up the sequence in which to execute data upload to database.
    The pandas DataFrame's of the JSON files are taken from a queue.
    The dataframe is then loaded into a dedicated table in the database.
    """
    try:
        upload = db.DataUpload()
        dag_logger.info('Preparing for data upload...')

        while not event.is_set() or not queue.empty():
            dag_logger.info('Getting data from queue...')
            dataframe, file_name = queue.get(timeout=5)

            upload.load_to_database(dataframe=dataframe, table_name=CLEAN_DATA_TABLE)
            dag_logger.info('Data for "%s" was uploaded to a "%s" table.', file_name, CLEAN_DATA_TABLE)

            queue.task_done()
            print()
    except Empty:
        dag_logger.error("Queue is empty.")
    except (ProgrammingError, OperationalError, DatabaseError,
            DisconnectionError, DBAPIError, AttributeError) as e:
        dag_logger.error("An error occurred while loading the data: %s.", e, exc_info=True)


with DAG(
    "data_pipeline_local_dag",
    default_args=default_args,
    description='Data Pipeline DAG. Schedules data download, transformation, and upload to a database.',
    schedule_interval=DATA_PIPELINE_DAG_SCHD,
    catchup=False,
) as data_pipeline_dag:

    for api_name, api_url in API_DICT.keys():

        event = Event()
        queue = Queue(maxsize=3)

        api_task = PythonOperator(
            task_id=f'download_{api_name}_data',
            python_callable=api.get_api_data,
            op_kwargs={'api_name': api_name, 'api_url': api_url},
            dag=data_pipeline_dag
        )

        staging_task = PythonOperator(
            task_id=f'upload_{api_name}_data_to_staging',
            python_callable=upload_to_staging,
            op_kwargs={'json_data': api_task, 'api_name': api_name},
            dag=data_pipeline_dag
        )

        prep_task = PythonOperator(
            task_id=f'prepare_{api_name}_data',
            python_callable=prepare_json_data,
            op_kwargs={'api_name': api_name, 'queue': queue, 'event': event},
            dag=data_pipeline_dag
        )

        storage_task = PythonOperator(
            task_id=f'upload_{api_name}_data_to_db',
            python_callable=jobs_data_upload_to_db,
            op_kwargs={'queue': queue, 'event': event},
            dag=data_pipeline_dag
        )

        api_task >> staging_task >> prep_task >> storage_task