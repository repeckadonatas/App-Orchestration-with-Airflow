from sqlalchemy import Column, String, Float, Integer, TIMESTAMP, DateTime
from sqlalchemy.ext.declarative import declarative_base

import src.logger as log

tables_logger = log.app_logger(__name__)

Base = declarative_base()


class JobsListingsData(Base):
    __tablename__ = 'jobs_listings_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_title = Column(String())
    company_name = Column(String())
    job_type = Column(String())
    region = Column(String())
    salary = Column(Float())
    min_salary = Column(Float())
    max_salary = Column(Float())
    salary_currency = Column(String())
    pub_date_timestamp = Column(TIMESTAMP())
    expiry_date_timestamp = Column(TIMESTAMP())
    timestamp = Column(DateTime(timezone=True))
    job_ad_link = Column(String())
