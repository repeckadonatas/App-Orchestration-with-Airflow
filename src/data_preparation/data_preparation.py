"""
Functions used to prepare JSON files from
API responses before uploading the data
to the database.
"""
from datetime import datetime

import pytz
import pandas as pd
import numpy as np

import src.logger as log
from src.constants import REGION_COLUMN, STR_TO_FLOAT_SCHEMA

data_logger = log.app_logger(__name__)


def create_dataframe(json_data: dict,
                     cols_normalize: list) -> pd.DataFrame:
    """
    Creates a pandas dataframe from a JSON file.
    :param json_data: JSONB data from a staging table.
    :param cols_normalize: a list with "path" values for json_normalize function.
    :returns: a normalized dataframe
    """
    if not json_data:
        return pd.DataFrame()

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
        dataframe[col] = dataframe[col].apply(
            lambda x: [country.strip() for country in x.split(',')] if isinstance(x, str) else x)
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
    Extracts the values for 'min_salary' and 'max_salary' from the 'salary' column.
    Handles various formats including yearly range, single values, and hourly rates.
    :param dataframe: dataframe to process.
    :return: processed dataframe with 'salary', 'min_salary' and 'max_salary' columns.
    """
    if 'salary' in dataframe.columns:
        yearly_pattern = r'\$(\d+\.?\d*)[kK]?\s*-\s*\$(\d+\.?\d*)[kK]?'
        hourly_pattern = r'\$(\d+)\s*-?\s*\$(\d+)\s*/hour'
        single_salary_pattern = r'\$(\d+)'

        yearly_salary_extract = dataframe['salary'].str.extract(yearly_pattern)
        yearly_salary_extract.columns = ['min_salary', 'max_salary']
        yearly_salary_extract = yearly_salary_extract.apply(pd.to_numeric, errors='coerce')

        hourly_salary_extract = dataframe['salary'].str.extract(hourly_pattern)
        hourly_salary_extract.columns = ['min_salary', 'max_salary']
        hourly_salary_extract = hourly_salary_extract.apply(pd.to_numeric, errors='coerce')

        single_salary_mask = (yearly_salary_extract.isna().all(axis=1)
                              & hourly_salary_extract.isna().all(axis=1))
        single_salaries = dataframe.loc[single_salary_mask, 'salary'].str.extract(single_salary_pattern)
        single_salaries = single_salaries.apply(pd.to_numeric, errors='coerce')

        dataframe['min_salary'] = np.nan
        dataframe['max_salary'] = np.nan

        yearly_mask = ~yearly_salary_extract['min_salary'].isna()
        dataframe.loc[yearly_mask, 'min_salary'] = yearly_salary_extract.loc[yearly_mask, 'min_salary']
        dataframe.loc[yearly_mask, 'max_salary'] = yearly_salary_extract.loc[yearly_mask, 'max_salary']

        hourly_mask = ~hourly_salary_extract['min_salary'].isna()
        dataframe.loc[hourly_mask, 'min_salary'] = hourly_salary_extract.loc[hourly_mask, 'min_salary'] * 40 * 52
        dataframe.loc[hourly_mask, 'max_salary'] = hourly_salary_extract.loc[hourly_mask, 'max_salary'] * 40 * 52

        dataframe.loc[single_salary_mask, 'min_salary'] = single_salaries[0]
        dataframe.loc[single_salary_mask, 'max_salary'] = single_salaries[0]

        dataframe['min_salary'] = dataframe['min_salary'].apply(lambda x: x * 1000 if pd.notna(x) and x < 10000 else x)
        dataframe['max_salary'] = dataframe['max_salary'].apply(lambda x: x * 1000 if pd.notna(x) and x < 10000 else x)

        salary_float = dataframe['salary'].str.extract(single_salary_pattern)[0].astype(float)
        salary_k_mask = dataframe['salary'].str.contains('[kK]', na=False)
        dataframe['salary'] = salary_float * 1000 * salary_k_mask.astype(int) + salary_float * (~salary_k_mask)

        if 'salary_currency' not in dataframe.columns:
            dataframe['salary_currency'] = 'USD'

        for col in STR_TO_FLOAT_SCHEMA:
            if col in dataframe.columns:
                dataframe[col] = dataframe[col].astype(float)

        dataframe = dataframe.drop(columns=['salary'], axis=1)

    return dataframe


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
