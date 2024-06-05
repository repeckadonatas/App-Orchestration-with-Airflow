"""
Functions used to prepare JSON files from
an API responses before uploading the data
to the database.
"""

import json
import pandas as pd
from pandas.core import series

import src.logger as log
from src.constants import *

data_logger = log.app_logger(__name__)


def create_dataframe(file_json: str) -> pd.DataFrame:
    """
    Creates a pandas dataframe from a JSON file.
    Requires a name of the file.
    """
    with open(PATH_TO_DATA_STORAGE / file_json, 'r', encoding='utf-8') as json_file:
        json_data = json.load(json_file)
        df = pd.DataFrame(pd.json_normalize(json_data))
    return df


def flatten_json_file(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Flattens the supplied dataframe and returns a new dataframe.
    :param dataframe: dataframe to flatten
    :return: new dataframe with flattened json data
    """
    for column in dataframe.columns:
        if isinstance(dataframe[column], (dict, pd.core.series.Series)):
            dataframe_flat = dataframe[column].apply(pd.Series)
            dataframe_flat = dataframe_flat[0].apply(pd.Series)

            for col in dataframe_flat.columns:
                if isinstance(dataframe_flat[col][0], list):
                    list_df = dataframe_flat[col].apply(lambda x: pd.Series(x))
                    list_df = list_df.add_prefix(f"{col}_")
                    dataframe_flat = dataframe_flat.drop(col, axis=1).join(list_df)
                    dataframe_flat.fillna('')

                else:
                    continue

        else:
            dataframe_flat = dataframe[column].apply(pd.Series)
            dataframe_flat = dataframe_flat[0].apply(pd.Series)

    return dataframe_flat


def rename_columns(dataframe: pd.DataFrame,
                   column_rename_mapping: dict) -> pd.DataFrame:
    """
    Changes the column names of the dataframe to their new column names.
    :param dataframe: a pandas dataframe to change column names for
    :return: dataframe with new column names
    """    
    dataframe.rename(columns=column_rename_mapping, inplace=True)
    dataframe.reindex(columns=column_rename_mapping)
    return dataframe


def reorder_dataframe_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Reorders the columns of a dataframe to match a common schema.
    :param dataframe: dataframe to reorder columns for
    :return: dataframe with reordered columns
    """
    dataframe = dataframe.reindex(columns=COMMON_SCHEMA)
    return dataframe


# def change_column_names(dataframe: pd.DataFrame) -> pd.DataFrame:
#     """
#     Changes the column names of the dataframe to their new column names.
#     :param dataframe: a pandas dataframe to change column names
#     :return: dataframe with new column names
#     """
#     new_names = {"rate.price": "rate_price",
#                  "rate.ask": "rate_ask",
#                  "rate.bid": "rate_bid", 
#                  "rate.high": "rate_high",
#                  "rate.low": "rate_low",
#                  "rate.change": "rate_change",
#                  "rate.change_percent": "rate_change_percent"}
#     dataframe.rename(columns=new_names, inplace=True)
#     dataframe.reindex(columns=new_names)
#     return dataframe


def prepare_json_data(queue: str, event: str) -> None:
    """
    Setting up the sequence in which
    to execute data preparation functions.
    The JSON files are turned into pandas DataFrame's
    and put into a queue.
    """
    while not event.is_set():
        try:
            json_files = get_files_in_directory(PATH_TO_DATA_STORAGE)
            data_logger.info(f'Files found in a directory: {json_files}')
            
            for json_file in json_files:
                json_to_df = create_dataframe(json_file)
                # new_col_names = change_column_names(json_to_df)
                data_logger.info(f'A dataframe was created for a file: {json_file}')

                queue.put([new_col_names, json_file])

            event.set()
            print()
        except Exception as e:
            data_logger.error(f"An error occurred while creating a dataframe:\n {e}\n")
        