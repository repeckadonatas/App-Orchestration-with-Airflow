import threading
import concurrent.futures
from queue import Queue

import src.db_functions as db
import src.get_api_data as jobs
import src.data_preparation as data
import src.db_functions.data_upload_sequence as upload
import src.logger as log

main_logger = log.app_logger(__name__)

event = threading.Event()
queue = Queue(maxsize=4)
try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        json_prep = executor.submit(data.prepare_json_data(queue, event))
        db_con = executor.submit(upload.jobs_data_upload_to_db(queue, event))


        # api = jobs.download_api_data()
        # tasks = [executor.submit(metals.download_metals_data()),
        #          executor.submit(data.prepare_json_data(queue, event)),
        #          executor.submit(db.metals_price_data_upload_to_db(queue, event))]

    # concurrent.futures.wait(tasks)
except Exception as e:
    main_logger.error('Exception occurred while running "main.py": %s\n', e, exc_info=True)
