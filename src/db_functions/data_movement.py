from datetime import datetime

import pytz
import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.schema import CreateSchema
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError, OperationalError, DBAPIError, DatabaseError, DisconnectionError

import src.db_functions.db_tables as db_tables
from src.db_functions.db_connection import db_logger, JobsDataDatabase
from src.constants import SCHEMAS_IN_DB


class DataUpload(JobsDataDatabase):
    """
    Functions that determine the logic of data upload process.
    """

    def __init__(self):
        super().__init__()

    def create_schemas(self) -> None:
        """
        Creates schemas in a database if they do not exist.
        """
        try:
            inspector = inspect(self.engine)
            schemas = inspector.get_schema_names()

            for schema_name in SCHEMAS_IN_DB:
                if schema_name not in schemas:
                    with self.engine.begin() as connection:
                        connection.execute(CreateSchema(schema_name))
                    db_logger.info('Schema "%s" was created successfully!', schema_name)
                else:
                    db_logger.info('Schema "%s" already exists. Skipping creation.', schema_name)

        except (OperationalError, DatabaseError, DisconnectionError, DBAPIError) as e:
            db_logger.error('An error occurred while creating schemas: %s', e, exc_info=True)

    def create_tables(self) -> None:
        """
        Creates tables in a database if they do not exist.
        """
        try:
            inspector = inspect(self.engine)

            for table in db_tables.Base.metadata.tables.values():
                table_name = table.name
                schema_name = table.schema or 'public'

                if not inspector.has_table(table_name, schema=schema_name):
                    with self.engine.begin() as connection:
                        table.create(connection)
                    db_logger.info('Table "%s" in schema "%s" created successfully!', table_name, schema_name)
                else:
                    db_logger.info('Table "%s" in schema "%s" already exists. Skipping creation.',
                                   table_name, schema_name)

            print()
        except (OperationalError, DatabaseError, DisconnectionError, DBAPIError, AttributeError) as e:
            db_logger.error('An error occurred while creating tables: %s', e, exc_info=True)

    def get_tables_in_db(self) -> dict:
        """
        Returns a list of all the tables in the database.
        """
        inspect_db = inspect(self.engine)
        tables_dict = {}

        for schema in SCHEMAS_IN_DB:
            tables_list = inspect_db.get_table_names(schema=schema)
            tables_dict[schema] = tables_list

        return tables_dict

    def load_to_staging(self,
                        json_data: dict,
                        api_name: str,
                        table_name: str) -> None:
        """
        Function to load the JSON data from an API
        to a specified staging table in the database.
        :param json_data: JSON data from the API.
        :param api_name: API source name.
        :param table_name: table to load the data to.
        """
        try:
            staging_dataframe = pd.DataFrame([{
                'api_source': api_name,
                'timestamp': datetime.now(tz=pytz.timezone('Europe/Vilnius')),
                'data': json_data
            }])

            dtypes = {'data': JSONB}

            staging_dataframe.to_sql(table_name,
                                     con=self.engine,
                                     schema='staging',
                                     if_exists='append',
                                     index=False,
                                     dtype=dtypes)

        except SQLAlchemyError as e:
            db_logger.error("A SQLAlchemy error occurred while loading the data: %s. "
                            "Rolling back the last transaction", e, exc_info=True)
            self.conn.rollback()
        except Exception as e:
            db_logger.error("An error occurred while loading the data: %s. "
                            "Rolling back the last transaction", e, exc_info=True)
            self.conn.rollback()

    def get_data_from_staging(self,
                              staging_schema: str,
                              staging_table: str,
                              api_name: str) -> dict:
        """
        Gets the latest uploaded API data from the staging table.
        """
        try:
            staging_query = f"""
            SELECT data FROM {staging_schema}.{staging_table}
            WHERE api_source = '{api_name}'
            ORDER BY timestamp DESC
            LIMIT 1"""

            dataframe = pd.read_sql_query(staging_query,
                                          con=self.engine,
                                          params={'api_source': api_name})

            if not dataframe.empty:
                return dataframe['data'].iloc[0]
            else:
                db_logger.info('No data found for "%s" in "%s".', api_name, staging_table)
                return {}

        except Exception as e:
            db_logger.info('An error occurred while retrieving the data: %s.', e, exc_info=True)
            return {}

    def load_to_database(self,
                         dataframe: pd.DataFrame,
                         table_name: str) -> None:
        """
        Function to load the data of a dataframe to a specified table in the database.
        :param dataframe: dataframe to load data from.
        :param table_name: table to load the data to.
        """
        try:
            dataframe.to_sql(table_name,
                             con=self.engine,
                             if_exists='append',
                             index=False)
        except Exception as e:
            db_logger.error("An error occurred while loading the data: %s. "
                            "Rolling back the last transaction", e, exc_info=True)
            self.conn.rollback()
