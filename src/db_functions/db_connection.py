import pandas as pd
from sqlalchemy_utils import create_database, database_exists
from sqlalchemy.engine import URL
from sqlalchemy import create_engine, Table, Column, String, Float, Integer, TIMESTAMP, MetaData
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
                db_logger.info(f'Database created. Database URL: {self.db_url}')
            else:
                pass

        except (OperationalError, DatabaseError, DisconnectionError, DBAPIError, AttributeError) as err:
            db_logger.error("A configuration error has occurred: %s", err)

    def __enter__(self):
        """
        Creates a connection to the database when main.py is run.
        Creates a connection engine and sets autocommit flag to True.
        :return: connection to a database
        """
        try:
            self.engine = create_engine(self.db_url, pool_pre_ping=True)
            self.conn = self.engine.connect().execution_options(autocommit=True)

            db_logger.info("Connected to the database.")

        except (OperationalError, DatabaseError, DisconnectionError, DBAPIError, AttributeError) as err:
            db_logger.error("The following connection error has occurred: %s", err)
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

        except (Exception, AttributeError, exc_type, exc_val, exc_tb) as err:
            db_logger.error("Connection was not closed: %s\n", err)


#
#     def get_tables_in_db(self) -> list:
#         """
#         Returns a list of all the tables in the database.
#         """
#         table_list = []
#         for table, self.metadata in self.metadata.tables.items():
#             if self.engine.dialect.has_table(self.conn, table):
#                 table_list.append(table)
#         db_logger.info('Table(s) found in a database: {}'.format(table_list))
#
#         return table_list
#
#     def determine_table_name(self, file_name: str) -> (str | None):
#         """
#         To determine the table name based on the file name.
#         """
#         for prefix, table in TABLE_MAPPING.items():
#             if prefix in file_name:
#                 return table
#         return db_logger.error('Table "{}" not found in the database'.format(table))
#
#     def load_to_database(self, dataframe: pd.DataFrame, table_name: str) -> None:
#         """
#         Function to load the data of a dataframe to a specified table in the database.
#         :param dataframe: dataframe to load data from.
#         :param table_name: table to load the data to.
#         """
#         try:
#             dataframe.to_sql(table_name, con=self.engine, if_exists='append', index=None, dtype={'timestamp': TIMESTAMP()})
#         except Exception as e:
#             db_logger.error("An error occurred while loading the data: {}. Rolling back the last transaction".format(e))
#             self.conn.rollback()
#
#
# def jobs_data_upload_to_db(queue: str, event: str) -> None:
def jobs_data_upload_to_db() -> None:
    """
    Setting up the sequence in which
    to execute data upload to database.
    The pandas DataFrame's of the JSON files
    are taken from a queue.
    The dataframe is then loaded into a
    dedicated table in the database.
    """
    with JobsDataDatabase() as db:
        try:
            db_logger.info('Hello')
            # db.create_tables()
            # db_tables = db.get_tables_in_db()
            # print()
            # while not event.is_set() or not queue.empty():
            #     dataframe, file_name = queue.get()
            #
            #     db.load_to_database(dataframe=dataframe, table_name='commodities_price_data_analytics')
            #     db_logger.info('Dataframe "{}" loaded to a table "commodities_price_data_analytics"'.format(file_name))
            #
            #     table = db.determine_table_name(file_name)
            #
            #     if table in db_tables:
            #         db.load_to_database(dataframe=dataframe, table_name=table)
            #         db_logger.info('Dataframe "{}" loaded to a table "{}"'.format(file_name, table))
            #     else:
            #         db_logger.error('Table "{}" not found in the database'.format(table))
            #
            #     queue.task_done()
        except Exception as e:
            db_logger.error("An error occurred while loading the data: {}.".format(e), exc_info=True)
