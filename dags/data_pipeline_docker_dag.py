import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.connection import Connection

from src.constants import API_DICT


DATA_PIPELINE_DAG_SCHD = "0 */6 * * *"
DATABASE_BACKUP_DAG_SCHD = "0 */6 * * *"

DB_HOST = "project-db"

conn_args = Connection(
    conn_id="project_db",
    conn_type="postgres",
    description="Project database in Docker container",
    login=os.environ.get("PGUSER"),
    password=os.environ.get("PGPASSWORD"),
    host=os.environ.get("PGHOST"),
    port=os.environ.get("PGPORT"),
    schema=os.environ.get("PGDATABASE")
)

conn_args.get_uri()

default_args = {
    "owner": "donatas_repecka",
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
            network_mode='my-bridge-network',
            api_version='1.45',
            auto_remove=True,
            environment={
                'PGUSER': os.environ.get('PGUSER'),
                'PGPASSWORD': os.environ.get('PGPASSWORD'),
                'PGHOST': DB_HOST,
                'PGPORT': os.environ.get('PGPORT'),
                'PGDATABASE': os.environ.get('PGDATABASE')
            },
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
        network_mode='my-bridge-network',
        api_version='1.45',     # 1.45
        auto_remove=True,
        environment={
            'PGUSER': os.environ.get('PGUSER'),
            'PGPASSWORD': os.environ.get('PGPASSWORD'),
            'PGHOST': DB_HOST,
            'PGPORT': os.environ.get('PGPORT'),
            'PGDATABASE': os.environ.get('PGDATABASE')
        },
        dag=backup_dag
    )
