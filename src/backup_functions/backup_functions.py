"""
Functions used to control the pipeline 
of creating backups of a database
and where the backups are saved.
"""

import subprocess

import src.db_functions.db_connection as dbc
import src.logger as log
from src.constants import *

backup_logger = log.app_logger(__name__)


def database_backup() -> None:
    """
    A function to create a backup of a database.
    Takes values of variables BACKUP_FOLDER_TODAY, PG_DUMP, DB_BACKUP_FILE
    from constants.py file.
    """
    try:
        os.makedirs(BACKUP_FOLDER_TODAY, exist_ok=True)
        with dbc.JobsDataDatabase():
            database_user = env_config().get('PGUSER')
            database_name = env_config().get('PGDATABASE')

            subprocess.run([PG_DUMP, '-U', database_user, '-d', database_name, '-f', DB_BACKUP_FILE], check=True)

            backup_logger.info('Database "%s" backup completed\n', database_name)

    except subprocess.CalledProcessError as e:
        backup_logger.error('CalledProcessError: %s', e)
    except FileNotFoundError as e:
        backup_logger.error('Backup file was not found: %s', e)
    except Exception as e:
        backup_logger.error('An unexpected error occurred during database backup: %s', e)
