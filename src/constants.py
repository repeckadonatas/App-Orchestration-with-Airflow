"""
Constants that are used throughout the project.
"""

import os
import logging
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv, find_dotenv

# DATE
CURRENT_DATE = datetime.today().strftime("%Y%m%d_%H%m")


# FOR LOGGER ONLY
LOG_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

FORMATTER = logging.Formatter(f'{LOG_TIME} :: %(name)s :: %(levelname)s :: %(funcName)s :: %(message)s')
PATH_TO_LOGS = Path(__file__).cwd()
LOG_FILE = PATH_TO_LOGS / 'logs/' / ("app_logger_" + datetime.today().strftime("%Y%m%d") + ".log")


# DATABASE INITIALIZATION
INIT_DB = Path(__file__).cwd() / 'sql/init.sql'


# FOR PG_DUMP FUNCTION
# PG_DUMP_PATH = r'C:\Program Files\PostgreSQL\16\bin\pg_dump.exe'
PG_DUMP_PATH = 'pg_dump'


# API URLs
REMOTIVE_API = "https://remotive.com/api/remote-jobs?limit=1"
HIMALAYAS_API = "https://himalayas.app/jobs/api?limit=1"
JOBICY_API = "https://jobicy.com/api/v2/remote-jobs?count=1"

API_DICT = {'REMOTIVE': REMOTIVE_API,
            'HIMALAYAS': HIMALAYAS_API,
            'JOBICY': JOBICY_API
            }


# PATHS TO DATA AND FILES
PATH_TO_DATA_STORAGE = Path(__file__).cwd() / 'src/data'


# BACKUPS LOCATION
PATH_TO_BACKUPS = Path(__file__).cwd() / 'backups'
BACKUP_FOLDERS_TODAY = PATH_TO_BACKUPS / ("backup_" + CURRENT_DATE)


# BACKUP FOLDERS FOR DATABASE AND ML MODELS
DB_BACKUP_FILE = BACKUP_FOLDERS_TODAY / ("db_backup_" + CURRENT_DATE + ".sql")


# FOR DATAFRAME
COLUMN_RENAME_MAP = {
    "title": "job_title",
    "jobTitle": "job_title",
    "seniority_0": "seniority",
    "jobLevel": "seniority",
    "companyName": "company_name",
    "applicationLink": "job_ad_link",
    "url": "job_ad_link",
    "minSalary": "min_salary",
    "annualSalaryMin": "min_salary",
    "maxSalary": "max_salary",
    "annualSalaryMax": "max_salary",
    "salaryCurrency": "salary_currency",
    "pubDate": "pub_date_timestamp",
    "publication_date": "pub_date_timestamp",
    "expiryDate": "expiry_date_timestamp",
    "locationRestrictions_0": "region",
    "jobGeo": "region",
    "candidate_required_location": "region",
    "jobType": "job_type"
    }

COMMON_TABLE_SCHEMA = [
    'job_title',
    'seniority',
    'company_name',
    'job_type',
    'region',
    'salary',
    'min_salary',
    'max_salary',
    'salary_currency',
    'pub_date_timestamp',
    'expiry_date_timestamp',
    'timestamp',
    'job_ad_link'
]

DATA_TYPES_SCHEMA = {
    'job_title': '',
    'seniority': '',
    'company_name': '',
    'job_type': '',
    'region': '',
    'salary': '',
    'min_salary': '',
    'max_salary': '',
    'salary_currency': '',
    'pub_date_timestamp': '',
    'expiry_date_timestamp': '',
    'timestamp': '',
    'job_ad_link': ''
    }

DATETIME_COLUMNS = ['pub_date_timestamp', 'expiry_date_timestamp']


# TABLES FOR DB
TABLES_TO_CREATE = ['remotive_data',
                    'himalayas_data',
                    'jobicy_data']

TABLE_MAPPING = {#'remotive': 'remotive_data',
                 #'himalayas': 'himalayas_data',
                 #'jobicy': 'jobicy_data',
                 'remotive': 'jobs_listings_data',
                 'himalayas': 'jobs_listings_data',
                 'jobicy': 'jobs_listings_data'
                 }


# REUSABLE FUNCTIONS
def env_config() -> os.environ:
    """
    Gets database connection credentials from .env file.
    :return: os.environ.
    """
    load_dotenv(find_dotenv('.env', usecwd=True))

    return os.environ


def read_dict(dict_name: dict) -> list:
    """
    Reads a dictionary to get the keys and values.
    :param dict_name: the name of a dictionary to read.
    :return: a list of key/value pairs.
    """
    return [(dict_key, dict_value) for dict_key, dict_value in dict_name.items()]


def get_files_in_directory(dir_path: str) -> list:
    """
    Reads files in a set directory.
    Returns a list of names of files in the directory
    to be iterated through.
    :param dir_path: path to a directory to read.
    :return: a list of file names in the directory.
    """
    files = os.scandir(dir_path)

    list_of_files = []
    for file in files:
        if file.is_dir() or file.is_file():
            list_of_files.append(file.name)

    return list_of_files


# def init_db():
#     """
#     Initiate a database upon first connection
#     if it doesn't exist.
#     """
#     with open(INIT_DB, 'r') as db_f:
#         db_init = db_f.read()
#         env_config().get('PG_PASSWORD')
#         psycopg.connect().cursor().execute(db_init)
