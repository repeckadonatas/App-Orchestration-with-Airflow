"""
The main logic of retrieving API responses
and saving them as JSON files.
download_api_data() function is controlling the
logic for data download and is used in concurrency.
"""

import json
from json import JSONDecodeError

import cloudscraper
import requests
from requests.exceptions import RequestException, URLRequired, InvalidURL, HTTPError

import src.logger as log
from src.constants import *

api_logger = log.app_logger(__name__)


def get_api_data(api_name: str,
                 api_url: str) -> None:
    """
    Get API response and save it as a JSON file.
    The function uses a name and URL to retrieve an API response.
    :param api_name: a name of an API
    :param api_url: a URL of an API
    """
    try:
        if api_name == 'HIMALAYAS':
            scraper = cloudscraper.create_scraper(
                browser={'browser': 'chrome',
                         'platform': 'windows',
                         'desktop': True,
                         'mobile': False,
                         }
            )
            headers = {'Referer': 'https://himalayas.app/api',
                       'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) '
                                     'AppleWebKit/537.36 (KHTML, like Gecko) '
                                     'Gecko/20100101 Firefox/126.0 Chrome/91.0.4472.124 Safari/537.36',
                       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                       'Accept-Language': 'en-US,en;q=0.8',
                       'Connection': 'keep-alive'}

            response = scraper.get(api_url, headers=headers)

        else:
            headers = {'accept': 'application/json'}
            response = requests.get(api_url, headers=headers)

        response.raise_for_status()

        if (response.status_code != 204
                and response.headers["content-type"].strip().startswith("application/json")):
            try:
                json_response = response.json()

                os.makedirs(PATH_TO_DATA_STORAGE, exist_ok=True)

                with open(PATH_TO_DATA_STORAGE / (api_name + '_response.json'), 'w', encoding='utf-8') as f:
                    json.dump(json_response, f, ensure_ascii=False, indent=4)
                    api_logger.info('Downloaded API data from "%s."\n', api_name)

            except JSONDecodeError as e:
                api_logger.info('A JSON decode error occurred for "%s": %s\n', api_name, e, exc_info=True)

    except (RequestException, URLRequired, InvalidURL, HTTPError) as e:
        api_logger.error('An error occurred for "%s": %s', api_name, e, exc_info=True)

    except Exception as e:
        api_logger.error('Unexpected error occurred for "%s": %s\n', api_name, e, exc_info=True)
