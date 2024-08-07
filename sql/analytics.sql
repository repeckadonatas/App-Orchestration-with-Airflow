set search_path to jobs_data_db, public;

select distinct
    job_title,
    company_name,
    job_type
from
    jobs_listings_data
where
    job_type in ('full_time', 'Full Time');


select distinct
    job_title,
    company_name,
    job_type,
    timestamp
from
    jobs_listings_data
where
    job_title in ('Data Engineer', 'data engineer');


-- The number of new job ads that contain "data engineer" in the title (per day)
select distinct
    company_name,
    date(timestamp) as date,
    count(*) as new_data_engineering_jobs
from
    jobs_listings_data
where
    lower(job_title) like '%data engineer%'
group by
    company_name, date(timestamp)
order by
    date;


-- The number of new job ads that contain "data engineer" in the title and are remote friendly (per day)
select distinct
    company_name,
    region,
    date(timestamp) as date,
    count(*) as new_data_engineering_jobs
from
    jobs_listings_data
where
    lower(job_title) like '%engineer%'
and
    lower(job_title) like '%remote%'
group by
    company_name, region, date(timestamp)
order by
    date;


-- The maximum, minimum, average, and standard deviation
-- of salaries of job ads that contain "data engineer" in the title (total and per day).
select distinct
    region,
    date(timestamp) as date,
    max(max_salary) as max_salary,
    min(min_salary) as min_salary,
    cast(stddev(max_salary) as numeric(10,2)) as stddev_max_salary,
    cast(stddev(min_salary) as numeric(10,2)) as stddev_min_salary,
    cast(avg(max_salary) as numeric(10,2)) as avg_max_salary,
    cast(avg(min_salary) as numeric(10,2)) as avg_min_salary
from
    jobs_listings_data
where
    lower(job_title) like '%data engineer%'
group by
    region, date(timestamp)
order by
    date;