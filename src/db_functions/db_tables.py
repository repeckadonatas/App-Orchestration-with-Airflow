from sqlalchemy import Table, Column, String, Float, Integer, TIMESTAMP, MetaData
from sqlalchemy.ext.declarative import declarative_base

import src.logger as log

tables_logger = log.app_logger(__name__)

Base = declarative_base()


class JobsListingsData(Base):
    __tablename__ = 'jobs_listings_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_title = Column(String())
    company_name = Column(String())
    job_ad_link = Column(String())
    job_type = Column(String())
    region = Column(String())
    salary = Column(String())
    timestamp = Column(TIMESTAMP())
