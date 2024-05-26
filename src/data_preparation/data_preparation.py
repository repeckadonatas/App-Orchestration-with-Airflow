"""
Functions used to prepare JSON files from
an API responses before uploading the data
to the database.
"""

import os
import json
import pandas as pd

import source.logger as log
from source.constants import *

data_logger = log.app_logger(__name__)


def get_files_in_directory() -> list:
    """
    Reads JSON files in a set directory.
    Returns a list of names of files in the directory
    to be iterated through.
    :return: a list of file names in the directory
    """
    files = os.scandir(PATH_TO_DATA_STORAGE)

    list_of_files = []
    for file in files:
        if file.is_dir() or file.is_file():
            list_of_files.append(file.name)
    return list_of_files


def create_dataframe(file_json: str) -> pd.DataFrame:
    """
    Creates a pandas dataframe from a JSON file.
    Requires a name of the file.
    """
    with open(PATH_TO_DATA_STORAGE / file_json) as jfile:
        json_data = json.load(jfile)
        df = pd.DataFrame(pd.json_normalize(json_data))
    return df


def change_column_names(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Changes the column names of the dataframe to their new column names.
    :param dataframe: a pandas dataframe to change column names
    :return: dataframe with new column names
    """
    new_names = {"rate.price": "rate_price",
                 "rate.ask": "rate_ask",
                 "rate.bid": "rate_bid", 
                 "rate.high": "rate_high",
                 "rate.low": "rate_low",
                 "rate.change": "rate_change",
                 "rate.change_percent": "rate_change_percent"}
    dataframe.rename(columns=new_names, inplace=True)
    return dataframe


def prepare_json_data(queue: str, event: str) -> None:
    """
    Setting up the sequence in which
    to execute data preparation functions.
    The JSON files are turned into pandas DataFrame's
    and put into a queue.
    """
    while not event.is_set():
        try:
            json_files = get_files_in_directory()
            data_logger.info('Files found in a directory: {}'.format(json_files))
            
            for json_file in json_files:
                json_to_df = create_dataframe(json_file)
                new_col_names = change_column_names(json_to_df)
                data_logger.info('A dataframe was created for a file: {}'.format(json_file))

                queue.put([new_col_names, json_file])

            event.set()
            print()
        except Exception as e:
            data_logger.error("An error occurred while creating a dataframe: {}\n".format(e))
        