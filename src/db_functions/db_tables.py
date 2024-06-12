from sqlalchemy import Column, String, Float, Integer, TIMESTAMP, DateTime
from sqlalchemy.ext.declarative import declarative_base

import src.logger as log

tables_logger = log.app_logger(__name__)

Base = declarative_base()


class JobsListingsData(Base):
    __tablename__ = 'jobs_listings_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_title = Column(String(), nullable=False)
    seniority = Column(String(), nullable=True)
    company_name = Column(String(), nullable=False)
    job_type = Column(String(), nullable=True)
    region = Column(String(), nullable=True)
    salary = Column(Float(), nullable=True)
    min_salary = Column(Float(), nullable=True)
    max_salary = Column(Float(), nullable=True)
    salary_currency = Column(String(), nullable=True)
    pub_date_timestamp = Column(TIMESTAMP(), nullable=True)
    expiry_date_timestamp = Column(TIMESTAMP(), nullable=True)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    job_ad_link = Column(String(), nullable=True)
