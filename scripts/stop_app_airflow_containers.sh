#!/usr/bin/env sh

echo "Stopping app and airflow Docker containers..." \

docker compose -f docker-compose.app.yaml -p project_container down;
docker compose -f docker-compose.airflow.yaml -p airflow_container down;