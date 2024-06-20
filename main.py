"""
Main file to run the program using
Python's concurrent.futures module.
The programs performance is also timed
and printed out.
"""

import threading
import concurrent.futures
from queue import Queue

import src.get_api_data as jobs
import src.data_preparation as data
import src.db_functions.data_upload_sequence as upload
import src.logger as log

main_logger = log.app_logger(__name__)

event = threading.Event()
queue = Queue(maxsize=4)
try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # api_data = executor.submit(jobs.download_api_data())
        json_prep = executor.submit(data.prepare_json_data(queue, event))
        db_con = executor.submit(upload.jobs_data_upload_to_db(queue, event))

        # tasks = [executor.submit(jobs.download_api_data()),
        #          executor.submit(data.prepare_json_data(queue, event)),
        #          executor.submit(upload.jobs_data_upload_to_db(queue, event))]

    # concurrent.futures.wait(tasks)
except Exception as e:
    main_logger.error('Exception occurred while running "main.py": %s\n', e, exc_info=True)
