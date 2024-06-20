"""
The main .py file to run backup functions.
"""

import src.backup_functions as bckp


try:
    bckp.database_backup()
except Exception as e:
    raise e
