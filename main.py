import threading
import concurrent.futures
from queue import Queue

import src.db_functions as db
import src.get_api_data as jobs
import src.logger as log

main_logger = log.app_logger(__name__)

event = threading.Event()
queue = Queue(maxsize=4)
try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        db_con = db.jobs_data_upload_to_db()
        api = jobs.download_api_data()
        # tasks = [executor.submit(metals.download_metals_data()),
        #          executor.submit(data.prepare_json_data(queue, event)),
        #          executor.submit(db.metals_price_data_upload_to_db(queue, event))]

    #     training_models = executor.submit(mlm.train_price_prediction_models())
    #
    # concurrent.futures.wait(tasks)
except Exception as e:
    main_logger.error('Exception occurred while running "main.py": %s', e, exc_info=True)
