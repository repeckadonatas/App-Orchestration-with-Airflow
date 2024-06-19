"""
Functions used to control the pipeline 
of creating backups of a database and
ML models and where the backups are saved.
"""

import os
import shutil
import subprocess

import source.db_functions as db_conn
import source.logger as log
from source.constants import *

backup_logger = log.app_logger(__name__)


def database_backup() -> None:
    """
    A function to create a backup of a database.
    A backup of a whole database is created.
    """
    try:
        os.makedirs(PATH_TO_BACKUPS, exist_ok=True)
        with db_conn.MetalsPriceDataDatabase() as db:
            db.conn
            backup_file = DB_BACKUP_FILE
            database_user = env_config().get('PGUSER')
            database_name = env_config().get('PGDATABASE')
            pg_dump_path = PG_DUMP_PATH
            
            subprocess.run([pg_dump_path, '-U', database_user, '-d', database_name, '-f', backup_file], check=True)
            
            backup_logger.info('Database "{}" backup completed.'.format(database_name))
    except (Exception, subprocess.CalledProcessError) as e:
        backup_logger.error('Error occured during database backup: {}'.format(e))


def backup_ml_models() -> None:
    """
    A function to create backup copies of ML models.
    """
    try:
        os.makedirs(PATH_TO_BACKUPS, exist_ok=True)
        ml_models = ML_MODELS_PATH
        backup_destination = ML_MODELS_BACKUP_FOLDER
        
        shutil.copytree(src=ml_models, dst=backup_destination, dirs_exist_ok=True)
        
        backup_logger.info('Backing up ML models folders from "{}" completed.'.format(ml_models))
    except (Exception, shutil.Error) as e:
        backup_logger.error('Error occured during ML models backup: {}'.format(e))


def remove_old_backups() -> None:
    """
    A function to keep only the latest 20 backups.
    Backups are stored in folders named 'backup_'
    followed with a date for the day of the backup.
    Only the latest 20 backups are stored while the
    older backups are removed. 
    """
    try:
        backup_folders = [folder.name for folder in Path(PATH_TO_BACKUPS).iterdir() \
                        if folder.is_dir() and folder.name.startswith('backup_')]
        backup_folders.sort()
        
        if len(backup_folders) > 20:
            folders_to_remove = backup_folders[20:]
            for folder in folders_to_remove:
                shutil.rmtree(PATH_TO_BACKUPS, folder)
                
                backup_logger.info(f'Removed old backup folder: {folder}')
    except Exception as e:
        backup_logger.error('Error occured during old backup removal: {}'.format(e))