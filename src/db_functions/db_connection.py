from sqlalchemy_utils import create_database, database_exists
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, DBAPIError, DatabaseError, DisconnectionError

import src.logger as log
from src.constants import *


db_logger = log.app_logger(__name__)


class JobsDataDatabase:
    """
    Database connection functions.
    Used to create a connection with a database
    and load data to it.
    """

    def __init__(self):
        """
        Retrieves parsed config parameters from .env file.
        Creates database URL using parsed configuration variables.
        Creates a database instance using connection parameters
        if the database does not exist.
        Creates a connection engine.
        """
        try:
            self.params = env_config()
            self.db_url = URL.create('postgresql+psycopg',
                                     username=self.params.get('PGUSER'),
                                     password=self.params.get('PGPASSWORD'),
                                     host=self.params.get('PGHOST'),
                                     port=self.params.get('PGPORT'),
                                     database=self.params.get('PGDATABASE'))

            if not database_exists(self.db_url):
                create_database(self.db_url)
                db_logger.info('Database created. Database URL: %s', self.db_url)
            else:
                pass

            self.engine = create_engine(self.db_url, pool_pre_ping=True)

        except (OperationalError, DatabaseError, DisconnectionError, DBAPIError, AttributeError) as err:
            db_logger.error("A configuration error has occurred: %s", err, exc_info=True)

    def __enter__(self):
        """
        Creates a connection to the database when main.py is run
        and sets autocommit flag to True.
        :return: connection to a database
        """
        try:
            self.conn = self.engine.connect().execution_options(autocommit=True)

            db_logger.info("Connected to the database.")

        except (OperationalError, DatabaseError, DisconnectionError, DBAPIError, AttributeError) as err:
            db_logger.error("The following connection error has occurred: %s", err, exc_info=True)
            self.conn = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Closes the connection to the database once the program has finished.
        :param exc_type: exception type
        :param exc_val: exception value
        :param exc_tb: exception traceback
        """
        try:
            if self.conn is not None:
                self.conn.close()

                db_logger.info('Connection closed.\n')
            elif exc_val:
                raise

        except (OperationalError, DatabaseError, DisconnectionError, DBAPIError,
                AttributeError, exc_type, exc_val, exc_tb) as err:
            db_logger.error("Connection was not closed: %s\n", err, exc_info=True)
