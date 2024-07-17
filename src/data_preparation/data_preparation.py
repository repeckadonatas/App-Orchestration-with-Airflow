"""
Functions used to prepare JSON files from
API responses before uploading the data
to the database.
"""

import json
import pytz
import pandas as pd
from threading import Event
from queue import Queue

import src.logger as log
from src.constants import *

data_logger = log.app_logger(__name__)


def create_dataframe(file_json: str,
                     cols_normalize: list) -> pd.DataFrame:
    """
    Creates a pandas dataframe from a JSON file.
    :param file_json: name of the JSON file.
    :param cols_normalize: a list with "path" values for json_normalize function.
    :returns: a normalized dataframe
    """
    with open(PATH_TO_DATA_STORAGE / file_json, 'r', encoding='utf-8') as json_file:
        json_data = json.load(json_file)
        if any(column in json_data for column in cols_normalize):
            df = pd.json_normalize(json_data, cols_normalize, sep='_')
        else:
            df = pd.json_normalize(json_data)
            
    return df


def assign_region(dataframe: pd.DataFrame,
                  regions_dict: dict[str, list[str]]) -> pd.DataFrame:
    """
    Assigns a region to each row in the dataframe based on the provided regions dictionary.
    :param dataframe: dataframe to process.
    :param regions_dict: a dictionary mapping region names to lists of country names.
    :returns: dataframe with added 'regions' column.
    """
    region_column = REGION_COLUMN
    region_column_in_df = [column for column in region_column if column in dataframe.columns]
    
    dataframe['region'] = pd.NA

    for col in region_column_in_df:
        dataframe[col] = dataframe[col].apply(lambda x: [x] if isinstance(x, str) else x)
        for index, row in dataframe.iterrows():
            regions_found = set()
            for country in row[col]:
                for region, countries in regions_dict.items():
                    if country in countries:
                        regions_found.add(region)
            if regions_found:
                dataframe.at[index, 'region'] = ', '.join(regions_found)

    dataframe = dataframe.drop(columns=region_column, errors='ignore')
            
    return dataframe


def flatten_json_file(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Flattens the supplied dataframe and returns a new dataframe.
    :param dataframe: dataframe to flatten
    :return: new dataframe with flattened JSON data
    """
    for column in dataframe.columns:
        if isinstance(dataframe[column], (dict, pd.Series)):
            dataframe_flat = dataframe[column].apply(pd.Series)
            dataframe_new = pd.concat([dataframe, dataframe_flat], axis=1)
            dataframe_new = dataframe_new.drop(columns=[column], axis=1)
            
            for col in dataframe_new.columns:
                if isinstance(dataframe_new[col][0], list):
                    df_lists = pd.DataFrame(dataframe_new[col].to_list(), index=dataframe_new.index)
                    df_lists.columns = [f'{col}_{i}' for i in range(df_lists.shape[1])]
                    dataframe_new = pd.concat([dataframe_new, df_lists], axis=1)
                    dataframe_new = dataframe_new.drop(columns=[col], axis=1)
                    dataframe_new.fillna(pd.NA)
        else:
            dataframe_new = dataframe.copy()
            
    return dataframe_new


def salary_extraction(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts the values for 'min_salary' and 'max_salary' when the JSON file
    in 'salary' column contains value as amount per hour.
    For columns where 'salary' is a single amount, ensures that the correct
    data type is enforced.
    :param dataframe: dataframe to process.
    :return: processed dataframe.
    """
    if 'salary' in dataframe.columns:
        pattern = r'\$(\d+)\s*-?\s*\$(\d+)\s*/hour'
        salary_extract = dataframe['salary'].str.extract(pattern)
        salary_extract.columns = ['min_salary', 'max_salary']
        salary_extract = salary_extract.apply(pd.to_numeric, errors='coerce')
        
        single_salary_mask = salary_extract.isna().all(axis=1)
        single_salaries = dataframe.loc[single_salary_mask, 'salary'].str.extract(r'\$(\d+)')
        single_salaries = single_salaries.apply(pd.to_numeric, errors='coerce')

        salary_extract['min_salary'] = salary_extract['min_salary'].astype(float)
        salary_extract['max_salary'] = salary_extract['max_salary'].astype(float)

        salary_extract.loc[single_salary_mask, 'salary'] = single_salaries[0].astype(float)

        salary_extract['min_salary'] = salary_extract['min_salary'] * 40 * 52
        salary_extract['max_salary'] = salary_extract['max_salary'] * 40 * 52
        salary_extract['salary_currency'] = 'USD'

        dataframe_new = dataframe.drop(columns=['salary'], axis=1)
        dataframe_new = dataframe_new.join(salary_extract)
    else:
        dataframe_new = dataframe.copy()
    
    return dataframe_new


def add_timestamp(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Adding a timestamp column to the dataframe.
    This column represents the time when the JSON data was uploaded to the database.
    :param dataframe: a dataframe to add timestamp column to.
    :return: a dataframe with a timestamp column.
    """
    local_timezone = pytz.timezone('Europe/Vilnius')
    dataframe['timestamp'] = datetime.now(tz=local_timezone)
    
    return dataframe


def rename_columns(dataframe: pd.DataFrame,
                   column_rename_map: dict) -> pd.DataFrame:
    """
    Changes the column names of the dataframe to their new column names.
    :param dataframe: a pandas dataframe to change column names for.
    :param column_rename_map: a dictionary containing new column names.
    :return: dataframe with new column names
    """
    dataframe.rename(columns=column_rename_map, inplace=True)
    dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]
    
    return dataframe


def reorder_dataframe_columns(dataframe: pd.DataFrame,
                              reorder_schema: list) -> pd.DataFrame:
    """
    Reorders the columns of a dataframe to match a common schema.
    :param dataframe: dataframe to reorder columns for.
    :param reorder_schema: a list containing the column order
    :return: dataframe with reordered columns
    """
    dataframe = dataframe.reindex(columns=reorder_schema, fill_value=None)
    
    return dataframe


def change_datetime_format(dataframe: pd.DataFrame,
                           datetime_columns_list: list) -> pd.DataFrame:
    """
    Changes datetime format for datetime columns of a given dataframe to ISO 8601 format.
    :param dataframe: dataframe to change datetime format for.
    :param datetime_columns_list: a list of columns for which the datetime format needs to be changed.
    :return: dataframe with changed datetime format.
    """
    for column in datetime_columns_list:
        if column in dataframe.columns:
            if dataframe[column].dtype == 'int64' or dataframe[column].dtype == 'float64':
                dataframe[column] = pd.to_datetime(dataframe[column], unit='s', errors='coerce')
            elif dataframe[column].dtype == 'object':
                dataframe[column] = pd.to_datetime(dataframe[column], errors='coerce')
        else:
            pass

    return dataframe


def str_to_float_schema(dataframe: pd.DataFrame,
                        str_to_float_columns: list) -> pd.DataFrame:
    """
    Ensures that columns referring to 'salary' that are of type 'str'
    are always of type 'float' after flattening JSON dataframe.
    :param dataframe: dataframe to change column dtypes.
    :param str_to_float_columns: a list of column names.
    :return: dataframe with formatted column values.
    """
    for column in str_to_float_columns:
        if column in dataframe.columns:
            dataframe[column] = pd.to_numeric(dataframe[column], errors='coerce').astype(float).fillna(pd.NA)
        else:
            pass

    return dataframe


# SETTING THE LOGIC FOR JSON FILE NORMALIZATION
def prepare_json_data(queue: Queue, event: Event) -> None:
    """
    Setting up the sequence in which
    to execute data preparation functions.
    The JSON files are turned into pandas DataFrame's
    and put into a queue.
    """
    while not event.is_set():
        try:
            json_files = get_files_in_directory(PATH_TO_DATA_STORAGE)
            data_logger.info('Files found in a directory: %s', json_files)

            for json_file in json_files:
                json_to_df = create_dataframe(json_file, COLS_NORMALIZE)
                json_region = assign_region(json_to_df, REGIONS)
                json_flat = flatten_json_file(json_region)
                json_salary = salary_extraction(json_flat)
                json_time = add_timestamp(json_salary)
                json_names = rename_columns(json_time, COLUMN_RENAME_MAP)
                json_reorder = reorder_dataframe_columns(json_names, COMMON_TABLE_SCHEMA)
                json_time_format = change_datetime_format(json_reorder, DATETIME_COLUMNS)
                json_dtypes = str_to_float_schema(json_time_format, STR_TO_FLOAT_SCHEMA)

                data_logger.info('A dataframe was created for a file: %s', json_file)

                queue.put([json_dtypes, json_file])

            event.set()
            print()
        except Exception as e:
            data_logger.error("An error occurred while creating a dataframe:\n %s\n", e, exc_info=True)
