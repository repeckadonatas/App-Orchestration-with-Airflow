"""
The main .py file to run backup functions.
"""

import src.backup_functions as bckp


try:
    bckp.database_backup()
    bckp.remove_old_backups()
except Exception as e:
    raise e
