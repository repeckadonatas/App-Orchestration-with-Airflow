import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.exc import OperationalError, DBAPIError, DatabaseError, DisconnectionError, ProgrammingError

import src.db_functions.db_tables as db_tables
from src.db_functions.db_connection import db_logger, JobsDataDatabase


class DataUpload(JobsDataDatabase):

    def __init__(self):
        super().__init__()
        super().__enter__()

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

    def get_tables_in_db(self) -> list:
        """
        Returns a list of all the tables in the database.
        """
        inspect_db = inspect(self.engine)
        tables_list = inspect_db.get_table_names()

        return tables_list

    def determine_table_name(file_name: str,
                             table_mapping: dict) -> (str | None):
        """
        To map the correct dataframe with the table to load the data to.
        The function is used to make sure that the data of a dataframe
        is loaded into a correct table in the database.
        Mapping logic is determined by a supplied table mapping dictionary.
        :param file_name: file name to determine the table name.
        :param table_mapping: a dictionary with dataframe names and matching table names.
        """
        file_name_lower = file_name.lower()
        for prefix, table in table_mapping.items():
            if file_name_lower.startswith(prefix.lower()):
                return table

    def load_to_database(self, dataframe: pd.DataFrame, table_name: str) -> None:
        """
        Function to load the data of a dataframe to a specified table in the database.
        :param dataframe: dataframe to load data from.
        :param table_name: table to load the data to.
        """
        try:
            dataframe.to_sql(table_name, con=self.engine, if_exists='append', index=False)
        except Exception as e:
            db_logger.error("An error occurred while loading the data: %s. "
                            "Rolling back the last transaction", e, exc_info=True)
            self.conn.rollback()


def jobs_data_upload_to_db(queue: str, event: str) -> None:
    """
    Setting up the sequence in which
    to execute data upload to database.
    The pandas DataFrame's of the JSON files
    are taken from a queue.
    The dataframe is then loaded into a
    dedicated table in the database.
    """
    try:
        upload = DataUpload()
        upload.create_tables()
        tables_in_db = upload.get_tables_in_db()
        db_logger.info('Table(s) found in a database: %s\n', tables_in_db)

        while not event.is_set() or not queue.empty():
            dataframe, file_name = queue.get()

            upload.load_to_database(dataframe=dataframe, table_name='jobs_listings_data')
            db_logger.info('Dataframe "%s" loaded to a table "jobs_listings_data"', file_name)

            # table = db.determine_table_name(file_name, TABLE_MAPPING)
            #
            # if table in tables_in_db:
            #     db.load_to_database(dataframe=dataframe, table_name=table)
            #     db_logger.info('Dataframe "{}" loaded to a table "{}"'.format(file_name, table))
            # else:
            #     db_logger.error('Table "{}" not found in the database'.format(table))
        #
            queue.task_done()
    except (ProgrammingError, OperationalError, DatabaseError,
            DisconnectionError, DBAPIError, AttributeError) as e:
        db_logger.error("An error occurred while loading the data: %s.", e, exc_info=True)
