import pandas as pd
from sqlalchemy_utils import create_database, database_exists
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, DBAPIError, DatabaseError, DisconnectionError

import src.logger as log
from src.constants import *
import src.db_functions.db_tables as db_tables

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

        except (OperationalError, DatabaseError, DisconnectionError, DBAPIError, AttributeError, exc_type, exc_val, exc_tb) as err:
            db_logger.error("Connection was not closed: %s\n", err, exc_info=True)

    def create_tables(self) -> None:
        """
        Creates tables in a database if they do not exist.
        """
        try:
            for __tablename__ in db_tables.Base.metadata.tables.keys():
                if not self.engine.dialect.has_table(self.conn, __tablename__):
                    db_tables.Base.metadata.create_all(self.engine, checkfirst=True)
                    db_logger.info('Table "%s" created successfully!\n', __tablename__)
                else:
                    db_logger.info('Table "%s" already exists. Skipping creation.\n', __tablename__)
        except (OperationalError, DatabaseError, DisconnectionError, DBAPIError, AttributeError) as e:
            db_logger.error('An error occurred while creating "%s" table: %s\n', __tablename__, e, exc_info=True)
            self.conn.rollback()


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

    def determine_table_name(self, file_name: str) -> (str | None):
        """
        To determine the table name based on the prefix of a file name.
        The function is used make sure that the data of a dataframe
        is loaded into a correct table in the database.
        Mapping logic is determined by TABLE_MAPPING dictionary.
        :param file_name: file name to determine the table name.
        """
        for prefix, table in TABLE_MAPPING.items():
            if prefix in file_name:
                return table
        # return db_logger.error('Table "{}" not found in the database'.format(table))

    def load_to_database(self, dataframe: pd.DataFrame, table_name: str) -> None:
        """
        Function to load the data of a dataframe to a specified table in the database.
        :param dataframe: dataframe to load data from.
        :param table_name: table to load the data to.
        """
        try:
            dataframe.to_sql(table_name, con=self.engine, if_exists='append', index=None)
        except Exception as e:
            db_logger.error("An error occurred while loading the data: %s. "
                            "Rolling back the last transaction", e, exc_info=True)
            self.conn.rollback()
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
            db.create_tables()
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
            db_logger.error("An error occurred while loading the data: %s.", e, exc_info=True)
