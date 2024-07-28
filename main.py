"""
Main file to run the program using
Python's concurrent.futures module.
"""

import sys
from threading import Event
from queue import Queue
import concurrent.futures
from concurrent.futures import CancelledError, TimeoutError, BrokenExecutor, InvalidStateError

from sqlalchemy.exc import OperationalError, DBAPIError, DatabaseError, DisconnectionError, ProgrammingError

import src.get_api_data as api
import src.data_preparation as prep
import src.db_functions.data_movement as db
import src.logger as log
from src.constants import (read_dict, remove_files_in_directory,
                           API_DICT, PATH_TO_DATA_STORAGE, STAGING_TABLE, CLEAN_DATA_TABLE)

main_logger = log.app_logger(__name__)


# SETTING THE LOGIC FOR DATA UPLOAD TO A STAGING TABLE
def upload_to_staging(json_data: dict,
                      api_name: str) -> None:
    """
    Setting up the sequence in which to move the API
    data to the staging table in the database.
    """
    try:
        upload = db.DataUpload()
        upload.create_tables()
        tables_in_db = upload.get_tables_in_db()
        main_logger.info('Table(s) found in a database: %s\n', tables_in_db)

        upload.load_to_staging(json_data, api_name, table_name=STAGING_TABLE)

    except (ProgrammingError, OperationalError, DatabaseError,
            DisconnectionError, DBAPIError, AttributeError) as e:
        main_logger.error("A SQLAlchemy error occurred while loading the data: %s.", e, exc_info=True)
    except Exception as e:
        main_logger.error("An unexpected error occurred while loading the data: %s.", e, exc_info=True)


# SETTING THE LOGIC FOR JSON FILE NORMALIZATION
def prepare_json_data(queue: Queue, event: Event) -> None:
    """
    Setting up the sequence in which
    to execute data preparation functions.
    The JSON files are turned into pandas DataFrame's
    and put into a queue.
    """
    while not event.is_set():
        try:
            json_files = get_files_in_directory(PATH_TO_DATA_STORAGE)
            main_logger.info('Files found in a directory: %s', json_files)

            # json_data =

            for json_file in json_files:
                json_to_df = create_dataframe(json_file, COLS_NORMALIZE)
                json_region = assign_region(json_to_df, REGIONS)
                json_flat = flatten_json_file(json_region)
                json_salary = salary_extraction(json_flat)
                json_time = add_timestamp(json_salary)
                json_names = rename_columns(json_time, COLUMN_RENAME_MAP)
                json_reorder = reorder_dataframe_columns(json_names, COMMON_TABLE_SCHEMA)
                json_time_format = change_datetime_format(json_reorder, DATETIME_COLUMNS)
                json_dtypes = str_to_float_schema(json_time_format, STR_TO_FLOAT_SCHEMA)

                main_logger.info('A dataframe was created for a file: %s', json_file)

                queue.put([json_dtypes, json_file])

            event.set()
            print()
        except Exception as e:
            main_logger.error("An error occurred while creating a dataframe:\n %s\n", e, exc_info=True)


def jobs_data_upload_to_db(queue: Queue, event: Event) -> None:
    """
    Setting up the sequence in which to execute data upload to database.
    The pandas DataFrame's of the JSON files are taken from a queue.
    The dataframe is then loaded into a dedicated table in the database.
    """
    try:
        upload = DataUpload()

        while not event.is_set() or not queue.empty():
            main_logger.info('Getting data from queue...')
            dataframe, file_name = queue.get(timeout=5)

            upload.load_to_database(dataframe=dataframe, table_name=CLEAN_DATA_TABLE)
            main_logger.info('Data for "%s" was uploaded to a "%s" table.', file_name, CLEAN_DATA_TABLE)

            queue.task_done()
    except Empty:
        main_logger.error("Queue is empty.")
    except (ProgrammingError, OperationalError, DatabaseError,
            DisconnectionError, DBAPIError, AttributeError) as e:
        main_logger.error("An error occurred while loading the data: %s.", e, exc_info=True)


if __name__ == '__main__':

    if len(sys.argv) != 2:
        main_logger.error('API name is required as a command-line argument')
        sys.exit(1)

    api_name = sys.argv[1]

    api_dict_list = read_dict(API_DICT)

    api_url = None
    for name, url in api_dict_list:
        if name == api_name:
            api_url = url
            break

    if api_url is None:
        main_logger.error('API name "%s" not found in API_DICT or URL is missing', api_name)
        sys.exit(1)

    event = Event()
    queue = Queue(maxsize=3)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:

            tasks = [executor.submit(api.get_api_data(api_name, api_url)),
                     executor.submit(prep.prepare_json_data(queue, event)),
                     executor.submit(upload.jobs_data_upload_to_db(queue, event))]

            # remove_files_in_directory(PATH_TO_DATA_STORAGE)
            # main_logger.error('File "%s" was removed after data upload', api_name)

        concurrent.futures.wait(tasks)
    except CancelledError as e:
        main_logger.error('CancelledError occurred while running "main.py": %s\n', e, exc_info=True)
    except TimeoutError as e:
        main_logger.error('TimeoutError occurred while running "main.py": %s\n', e, exc_info=True)
    except BrokenExecutor as e:
        main_logger.error('BrokenExecutor error occurred while running "main.py": %s\n', e, exc_info=True)
    except InvalidStateError as e:
        main_logger.error('InvalidStateError occurred while running "main.py": %s\n', e, exc_info=True)
    except Exception as e:
        main_logger.error('An unexpected error occurred while running "main.py": %s\n', e, exc_info=True)
