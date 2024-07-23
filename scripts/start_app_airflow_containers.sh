#!/usr/bin/env sh

echo "Starting app and airflow Docker containers..." \

docker compose -f docker-compose.app.yaml -p project_container up -d;
docker compose -f docker-compose.airflow.yaml -p airflow_container up -d;