"""
Constants that are used throughout the project.
"""

import os
import logging
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv, find_dotenv


# FOR LOGGER ONLY
LOG_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

FORMATTER = logging.Formatter(f'{LOG_TIME} :: %(name)s :: %(levelname)s :: %(funcName)s :: %(message)s')
PATH_TO_LOGS = Path(__file__).cwd()
LOG_FILE = PATH_TO_LOGS / 'logs/' / ("app_logger_" + datetime.today().strftime("%Y%m%d") + ".log")

# DATE
CURRENT_DATE = datetime.today().strftime("%Y%m%d_%H%m")

# FOR PG_DUMP FUNCTION
# PG_DUMP_PATH = r'C:\Program Files\PostgreSQL\16\bin\pg_dump.exe'
PG_DUMP_PATH = 'pg_dump'

# PATHS TO DATA AND FILES
PATH_TO_DATA_STORAGE = Path(__file__).cwd() / 'source/data/'
PATH_TO_API = Path(__file__).cwd() / 'source/api_key/api_key.txt'
PATH_TO_METALS_LIST = Path(__file__).cwd() / 'source/metals.txt'
ML_MODELS_PATH = Path(__file__).cwd() / 'trained_models'

# BACKUPS LOCATION
PATH_TO_BACKUPS = Path(__file__).cwd() / 'backups'
BACKUP_FOLDERS_TODAY = PATH_TO_BACKUPS / ("backup_" + CURRENT_DATE)

# BACKUP FOLDERS FOR DATABASE AND ML MODELS
DB_BACKUP_FILE = BACKUP_FOLDERS_TODAY / ("db_backup_" + CURRENT_DATE + ".sql")
ML_MODELS_BACKUP_FOLDER = BACKUP_FOLDERS_TODAY / ('ml_models_backup_' + CURRENT_DATE)

# ORGANIZING COMMODITIES AND DATA UPLOAD TO A DB
COMMODITIES = ["gold", "silver", "platinum", "palladium"]

TABLES_TO_CREATE = ['gold_historic',
                    'silver_historic',
                    'platinum_historic',
                    'palladium_historic',
                    'commodities_price_data_analytics']

TABLE_MAPPING = {'gold': 'gold_historic',
                 'silver': 'silver_historic',
                 'platinum': 'platinum_historic',
                 'palladium': 'palladium_historic'}

TRAINING_DATA_COLUMNS = ['rate_price', 'rate_ask']


# REUSABLE REPEATABLE FUNCTIONS
def env_config() -> os.environ:
    """
    Gets database connection credentials from .env file.
    :return: os.environ
    """
    load_dotenv(find_dotenv('.env', usecwd=True))

    return os.environ


def read_api() -> str:
    """
    Reads API key value from a specified .txt file.
    :return: API key value as a string
    """
    with open(PATH_TO_API, 'r', encoding='utf-8') as key:
        api_key = key.readline()
    return api_key
    
    
def read_metals_list() -> list:
    """
    Reads a list of commodities (precious metals in this case)
    to get the price information from an API.
    :return: a list of precious metals
    """
    metals = []
    with open(PATH_TO_METALS_LIST, 'r', encoding='utf-8') as file:
        for metal in file:
            metal = metal.strip().rstrip(',')
            metals.append(metal)
    return metals
