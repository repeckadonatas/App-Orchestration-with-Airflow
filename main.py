"""
Main file to run the program using
Python's concurrent.futures module.
"""

import sys
from threading import Event
from queue import Queue
import concurrent.futures
from concurrent.futures import CancelledError, TimeoutError, BrokenExecutor, InvalidStateError

import src.get_api_data as api
import src.data_preparation as prep
import src.db_functions.data_upload_sequence as upload
import src.logger as log
from src.constants import read_dict, API_DICT

main_logger = log.app_logger(__name__)


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
