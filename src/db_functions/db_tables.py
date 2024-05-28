from sqlalchemy import Table, Column, String, Float, Integer, TIMESTAMP, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import src.logger as log
from src.constants import *

tables_logger = log.app_logger(__name__)


Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    age = Column(Integer, nullable=False)
    email = Column(String, unique=True, nullable=False)


def create_session(db_url):
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    return engine, Session


def create_tables(self) -> Table:
    """
    Creates tables in a database if they do not exist.
    Returns a list of tables in a database.
    """
    try:
        self.metadata = MetaData()

        for table in TABLES_TO_CREATE:
            self.price_table = Table(
                f'{table}',
                self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('status', String(20)),
                Column('timestamp', TIMESTAMP()),
                Column('currency', String(5)),
                Column('unit', String(5)),
                Column('metal', String(20)),
                Column('rate_price', Float()),
                Column('rate_ask', Float()),
                Column('rate_bid', Float()),
                Column('rate_high', Float()),
                Column('rate_low', Float()),
                Column('rate_change', Float()),
                Column('rate_change_percent', Float())
            )

        for table, metadata in self.metadata.tables.items():
            if self.engine.dialect.has_table(self.conn, table):
                db_logger.info('Table "{}" already exists.'.format(table))
            else:
                metadata.create(self.engine, checkfirst=True)
                db_logger.info('Table "{}" was created successfully.'.format(table))

        self.conn.rollback()
    except (Exception, OperationalError) as e:
        db_logger.error("An error occurred while creating a table: {}".format(e))
        self.conn.rollback()