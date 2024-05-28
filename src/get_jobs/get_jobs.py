"""
Functions to get API response and save it as 
JSON file.
download_metals_data() function is controlling the
logic for data download and is used in
concurrency.
"""

import json

import requests
from requests.exceptions import RequestException

import src.logger as log
from src.constants import *

api_logger = log.app_logger(__name__)


def get_metal_price_data(metals: list, 
                         API_KEY: str):
    """
    Get price data for a given precious metal.
    A JSON file is created containing price data 
    for every precious metal as it's name is being provided.
    :param metals: a list with name values of a precious metal
    :param API_KEY: API key for price API service used
    """
    for metal in metals:
        api_url = (f'https://api.metals.dev/v1/metal/spot'
                   f'?api_key={API_KEY}'
                   f'&metal={metal}'
                    '&currency=USD')

        headers = {'accept': 'application/json; charset=utf-8'}
        response = requests.get(api_url, headers=headers)
        json_response = response.json()

        if json_response["status"] != "success":
            api_logger.info(f'An error occurred: {json_response["status"]}')
        else:
            with open(PATH_TO_DATA_STORAGE / (metal + '_response.json'), 'w', encoding='utf-8') as f:
                json.dump(json_response, f, ensure_ascii=False, indent=4)
                api_logger.info('Downloading price API data for "{}..."'.format(metal))


def download_metals_data():
    """
    A function to download the price data for
    every precious metal provided.
    Gets API key from a text file.
    Reads a list of metals from a text file.
    A JSON file is saved for every request.
    """
    try:
        api_key = read_api()
        metals = read_metals_list()
        
        get_metal_price_data(metals, api_key)
        api_logger.info('Data downloaded successfully!\n')
    
    except (Exception, RequestException) as e:
        api_logger.info('An exception occured: {}'.format(e))