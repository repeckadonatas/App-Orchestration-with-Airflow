from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


# API URLs
COUNT_LIMIT = 2

REMOTIVE_API = f"https://remotive.com/api/remote-jobs?limit={COUNT_LIMIT}"
HIMALAYAS_API = f"https://himalayas.app/jobs/api?limit={COUNT_LIMIT}"
JOBICY_API = f"https://jobicy.com/api/v2/remote-jobs?count={COUNT_LIMIT}"

API_DICT = {
    'REMOTIVE': REMOTIVE_API,
    'HIMALAYAS': HIMALAYAS_API,
    'JOBICY': JOBICY_API
    }

DATA_PIPELINE_DAG_SCHD = "0 */6 * * *"
DATABASE_BACKUP_DAG_SCHD = "0 */6 * * *"

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["<EMAIL>"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
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
            image='notexists/job-application-system-app:1.1',
            command=["python3", "main.py", api_name],
            docker_url='unix:///var/run/docker.sock',
            network_mode='bridge',
            api_version='1.45',
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
        image='notexists/db-backup-app:1.1',
        command=["python3", "backup_main.py"],
        docker_url='unix:///var/run/docker.sock',
        network_mode='bridge',
        api_version='1.45',     # 1.45
        auto_remove=True,
        dag=backup_dag
    )
