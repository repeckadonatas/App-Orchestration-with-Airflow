"""
The main .py file to run backup functions from.
"""

import source.backup_functions as bckp


try:
    bckp.database_backup()
    bckp.backup_ml_models()
    bckp.remove_old_backups()
except Exception as e:
    raise e
