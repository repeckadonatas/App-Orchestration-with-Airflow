CREATE ROLE root;
ALTER ROLE root WITH LOGIN;
CREATE DATABASE "root";
CREATE DATABASE "jobs_data_db";

CREATE SCHEMA IF NOT EXISTS staging;

CREATE EXTENSION IF NOT EXISTS dblink;

DO
$do$
BEGIN
   IF EXISTS (SELECT FROM pg_database WHERE datname = 'jobs_data_db') THEN
      RAISE NOTICE 'Database already exists';
   ELSE
      PERFORM dblink_exec('dbname=' || current_database()
                        , 'CREATE DATABASE "jobs_data_db"');
   END IF;
END
$do$;

CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE jobs_data_db TO airflow;