from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta

from src.constants import DATA_PIPELINE_DAG_SHD, DATABASE_BACKUP_DAG_SHD

# Define default arguments
default_args = {
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["<EMAIL>"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Data Pipeline DAG
with DAG(
    "data_pipeline_docker_dag",
    default_args=default_args,
    description='Data Pipeline DAG. Schedules data download, transformation, and upload to a database.',
    schedule_interval=DATA_PIPELINE_DAG_SHD,
    catchup=False,
) as data_pipeline_dag:

    task1 = DockerOperator(
        task_id='download_api_data',
        image='notexists/job-application-system-app:1.0',
        api_version='auto',
        auto_remove=True,
        command='python -c "import src.get_api_data as api; api.download_api_data()"',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        dag=data_pipeline_dag,
    )

    task2 = DockerOperator(
        task_id='prepare_json_data',
        image='notexists/job-application-system-app:1.0',  # the name of your application's Docker image
        api_version='auto',
        auto_remove=True,
        command='python -c "import src.data_preparation as prep; prep.prepare_json_data(queue, event)"',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        dag=data_pipeline_dag,
    )

    task3 = DockerOperator(
        task_id='jobs_data_upload_to_db',
        image='notexists/job-application-system-app:1.0',  # the name of your application's Docker image
        api_version='auto',
        auto_remove=True,
        command='python -c "import src.db_functions.data_upload_sequence as upload; '
                'upload.jobs_data_upload_to_db(queue, event)"',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        dag=data_pipeline_dag,
    )

    # Task dependencies
    task1 >> task2 >> task3


with DAG(
    "database_backup_docker_dag",
    default_args=default_args,
    description='Database backup DAG. Controls the backup schedule.',
    schedule_interval=DATABASE_BACKUP_DAG_SHD,
    catchup=False,
) as backup_dag:

    task4 = DockerOperator(
        task_id='database_backup',
        image='notexists/db-backup-app:1.0',
        api_version='auto',
        auto_remove=True,
        command='python -c "import src.backup_functions as bckp; bckp.database_backup()"',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        dag=backup_dag,
    )

    task4
