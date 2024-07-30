"""
Functions used to control the pipeline 
of creating backups of a database
and where the backups are saved.
"""

import shutil
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


def remove_old_backups() -> None:
    """
    A function to keep only the latest 10 backups.
    Backups are stored in folders named 'backup_'
    followed with a date for the day of the backup.
    Only the latest 10 backups are stored while the
    older backups are removed.
    """
    try:
        backup_folders = [folder for folder in PATH_TO_BACKUPS.iterdir()
                          if folder.is_dir() and folder.name.startswith('backup_')]
        backup_folders.sort()

        if len(backup_folders) > 10:
            folders_to_remove = backup_folders[10:]
            for folder in folders_to_remove:
                shutil.rmtree(folder)

                backup_logger.info('Removed old backup folder: %s', folder)
    except Exception as e:
        backup_logger.error('Error occured during old backup removal: %s', str(e))
