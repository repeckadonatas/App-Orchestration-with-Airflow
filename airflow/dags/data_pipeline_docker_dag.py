from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from src.constants import API_DICT

DATA_PIPELINE_DAG_SCHD = "0 */6 * * *"
DATABASE_BACKUP_DAG_SCHD = "0 */6 * * *"

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["<EMAIL>"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "data_pipeline_docker_dag",
    default_args=default_args,
    description='Data Pipeline DAG. Schedules data download, transformation, and upload to a database.',
    schedule_interval=DATA_PIPELINE_DAG_SCHD,
    catchup=False,
) as data_pipeline_dag:

    for api_name in API_DICT.keys():
        task1 = DockerOperator(
            task_id=f'download_{api_name}_data',
            image='notexists/job-application-system-app:1.0',
            command=["python3", "main.py", api_name],
            docker_url='unix://var/run/docker.sock',
            volume=[],
            network_mode='bridge',
            api_version='auto',
            auto_remove=True,
            dag=data_pipeline_dag
        )


with DAG(
    "database_backup_docker_dag",
    default_args=default_args,
    description='Database backup DAG. Controls the backup schedule.',
    schedule_interval=DATABASE_BACKUP_DAG_SCHD,
    catchup=False,
) as backup_dag:

    task2 = DockerOperator(
        task_id='database_backup',
        image='notexists/db-backup-app:1.0',
        command=["python3", "backup_main.py"],
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        api_version='auto',
        auto_remove=True,
        dag=backup_dag
    )
