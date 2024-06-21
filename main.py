"""
Main file to run the program using
Python's concurrent.futures module.
The programs performance is also timed
and printed out.
"""

import threading
import concurrent.futures
from concurrent.futures import CancelledError, TimeoutError, BrokenExecutor, InvalidStateError
from queue import Queue

import src.get_api_data as api
import src.data_preparation as prep
import src.db_functions.data_upload_sequence as upload
import src.logger as log

main_logger = log.app_logger(__name__)

event = threading.Event()
queue = Queue(maxsize=4)
try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # api_data = executor.submit(api.download_api_data())
        json_prep = executor.submit(prep.prepare_json_data(queue, event))
        db_con = executor.submit(upload.jobs_data_upload_to_db(queue, event))

    #     tasks = [executor.submit(api.download_api_data()),
    #              executor.submit(prep.prepare_json_data(queue, event)),
    #              executor.submit(upload.jobs_data_upload_to_db(queue, event))]
    #
    # concurrent.futures.wait(tasks)
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
