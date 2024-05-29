"""
The main logic of retrieving API responses
and saving them as JSON files.
download_api_data() function is controlling the
logic for data download and is used in concurrency.
"""

import json

import requests
from requests.exceptions import RequestException, URLRequired, InvalidURL

import src.logger as log
from src.constants import *

api_logger = log.app_logger(__name__)


def get_jobs_api_data(api_dict: dict) -> json:
    """
    Get API response data and save it as JSON files.
    The function uses a dictionary to retrieve an API URL.
    :param api_dict: a dictionary of API URLs
    """
    for api_name, api_url in api_dict.items():
        if not api_url:
            api_logger.info(f'Missing URL for "{api_name}"!\n')
        else:
            headers = {'accept': 'application/json; charset=utf-8'}
            response = requests.get(api_url, headers=headers)
            json_response = response.json()

            if response.status_code == 200:
                os.makedirs(PATH_TO_DATA_STORAGE, exist_ok=True)
                with open(PATH_TO_DATA_STORAGE / (api_name + '_response.json'), 'w', encoding='utf-8') as f:
                    json.dump(json_response, f, ensure_ascii=False, indent=4)
                    api_logger.info('Downloading API data for "{}..."'.format(api_name))
            else:
                api_logger.info(f'An error occurred: {response.status_code} - {response.text}\n')


def download_api_data():
    """

    """
    try:
        get_jobs_api_data(API_DICT)
    
    except (Exception, RequestException, URLRequired, InvalidURL) as e:
        api_logger.info('An exception occurred: %s', e, exc_info=True)
