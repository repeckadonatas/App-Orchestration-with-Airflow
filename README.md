# Job Application System


## About

This project is an exercise in learning to ingest data from multiple sources and orchestrating processes of the application using Apache Airflow. The project envelopes the use of Python, SQL, Docker and Airflow.

The data for this project gathered from remote job sites **[Remotive](https://remotive.com/)**, **[Jobicy](https://jobicy.com/)** and **[Himalayas](https://himalayas.app/jobs)** using their APIs. After the data is downloaded, it is first stored in the staging layer in PostgreSQL then taken from it, transformed and prepared to be uploaded back to the database for final storage. 

Although this project could have been done in an easier way, for this project I chose to dockerize my application so that if needed it could be deployed to any machine with ease and as a good way to practice creating solutions that would require using Docker for application running and scheduling would be done using Apache Airflow. 

The application itself uses Threading as a concurrency method. Threading is suitable when looking to parallelize heavy I/O work, such as HTTP calls, reading from files and processing data.


## Tech Stack Used

* Programing language - **Python**;
* Data storage and querying - **PostgreSQL**;
* Data cleaning and normalization - **Pandas**;
* Package and dependency management - **Poetry**;
* Containerization and Image storage - **Docker**, **DockerHub**;
* Orchestration - **Airflow**


## Project Deliveries Plan

* **Database**:
  * For a database I chose PostgreSQL as it offers more features than some other databases (i.e. MySQL), it is free, easy-to-implement database management system and is better suited for enterprise-level applications. One major advantage Postgres offers is the ability to store JSON files in a BINARY format (JSONB data type). This allows to use the Postgres database for staging.


* **Data preparation**:
  * A successful response of API calls returns JSON data that is then stored in a staging layer within Postgres (`staging` schema in Postgres). 
  * **Pandas** library is used to create dataframes from JSON files. The changes done to dataframes of JSON files:
    * Data is normalized;
    * A region is assigned depending on the country/countries mentioned in the data;
    * A salary value is normalized;
    * A timestamp is added of when the data was retrieved;
    * Columns are renamed to match a common table schema.


* **Data upload to a database**:
  * Once the data is prepared for uploading, datasets are first placed in a queue and after matching the datasets with the table in a database, the data is then uploaded to corresponding table.


* **Tables**:
  * The database contains multiple tables for data storage. 
  * For storing JSON data in a `staging` layer:
    * `staging_jobs_listings_data`
  * For analytical purposes, the normalized data is contained in a table in `public` schema:
    * `jobs_listings_data`.


* **Backups**
  * Backups are created for a database periodically every six (6) hours.
  * Only the latest 10 backups are kept.


* **Containerization**
  * The project is containerized using Docker. The project utilizes multiple Images built from the same base app. The Images are stored in **[this location](https://hub.docker.com/repositories/notexists)** on Docker Hub and are retrieved when a `docker-compose.app.yaml` file is run on a set schedule.
  * The scheduling for running the YAML file is done by using **Apache Airflow**.
  * Current Airflow DAG schedule:
    * For data pipeline: every 4 hours `0 */4 * * *`
    * For database backup: every 6 hours `0 */6 * * *`


## How To Use The Program

### 1. Using Docker Images

To run the program in a Docker container:
- Download the `docker-compose.app.yaml` and `docker-compose.airflow.yaml` files of this project.
- Download `scripts` folder.
- Make sure to have `.env` file ready (check **Important** section below).
- In the terminal window navigate to `scripts` folder and run `create_folders_set_permissions.sh` script.
- The necessary folders `logs`, `dags` and `backups` will be created automatically.
- In the terminal window navigate to `scripts` folder and run `start_app_airflow_containers.sh` script. This will start Docker containers of application and Airflow images.
- The Images of the applications are pulled from a public repository on Docker Hub.
- In case a restart is needed, run `restart_app_airflow_containers.sh` script.
- To stop running images and remove them, run `stop_app_airflow_containers.sh` script.



\
For a **Production application:**
- Create and store `.env` file in the same folder as `docker-compose` files.
- For a more secure method of keeping secrets, utilize **GitHub Secrets** or a similar service to hide secrets.
- The Docker Image should be stored in a secure Image container.

\
Running the project with Docker will use **PostgreSQL** database image from `https://hub.docker.com/_/postgres/` and the images of **Job-Application-System** and **Database-Backup** from my publicly available **[Docker Hub](https://hub.docker.com/repository/docker/notexists/kaggle-data-download-app/general)** repository.
The Docker compose YAML file should be on a target machine first.

**Note:**

Every time the program runs, a log file is created in `logs` folder for that day. Information about any subsequent run is appended to the log file of that day. The program logs every data download, data transformation and data upload to the database. Errors are also logged into the same log file for the current day of the run.

Additionally, when a backup service is run, backups for a database are created in `backups` folder.


When running `docker-compose.app.yaml` and `docker-compose.airflow.yaml` on a target machine, in order to store logs and backups, the `volumes` section for the `project-app` and `airflow-*` should be adjusted accordingly. For **presentation purposes** logs and backups are directed to be stored in the project folder.

- In case a restart is needed, run `restart_app_airflow_containers.sh` script.


### **Important:**

1. **For database connection:**

To connect to the database, the `src/db_functions/db_connection.py` file needs to read connection values from `.env` file:

|                                                        |
|--------------------------------------------------------|
| POSTGRES_USER = postgres_user                          |
| POSTGRES_PASSWORD = postgres_password                  |
| PGUSER = db_username                                   | 
| PGPASSWORD = user_password                             | 
| PGHOST = host_name                                     |
| PGPORT = port                                          |
| PGDATABASE =  database_name                            |

\
Additionally, **Airflow's** backend database also needs these values:

|                                                        |
|--------------------------------------------------------|
| AIRFLOW_UID = 50000 (from Airflow documentation)       |
| AIRFLOW_GID = 0                                        |
| AIRFLOW__WEBSERVER__SECRET_KEY = your_webserver_secret |
| AIRFLOW__CORE__FERNET_KEY = fernet_key                 |
\
1.1. **For PostgreSQL Docker image:**

When using Docker, **PostgreSQL** needs **POSTGRES_USER** and **POSTGRES_PASSWORD** environment variables to be passed. For this reason the YAML file can be set up to read these environment variables from `.env` file.


1.2 **To Connect to a PostgreSQL database from OUTSIDE the Docker container:**

When connecting to a database from **outside** the Docker container, for connection parameters in database connection window use:
* **PORT**,
* **DATABASE**,
* **USER**, 
* **PASSWORD** 
* and **HOST** variable. 

For **HOST** use `localhost` or `127.0.0.1`. \
For the application running in Docker, **HOST** should be the name of Postgres database service in YAML file. In this case it is `project-db`.

\
**Note:**

Store the `.env` file in the root directory of the project folder to connect to the database and use it with no issues.


## Concurrency method used

The program uses Threading as concurrency method to fetch, transform and upload the data. Threading is suitable when looking to parallelize heavy I/O work, such as HTTP calls, reading from files and processing data. 

Here, Python's own `threading` and `concurrent.futures` modules are used. The `concurrent.futures` module provides a high-level interface for asynchronously executing callables. The asynchronous execution is performed with threads using `threading` module.
The `concurrent.futures` module allows for an easier way to run multiple tasks simultaneously using multi-threading to go around GIL limitations.


Using **ThreadPoolExecutor** subclass uses a pool of threads to execute calls asynchronously. All threads enqueued to **ThreadPoolExecutor** will be joined before the interpreter can exit.

## What could be improved or changed

* A better solution to parse and normalize JSON files;
* Additional DAG to run the application locally, since that would be enough for this project and the maintenance would be easier;
* Webscraping scripts to not be dependent on APIs only.

