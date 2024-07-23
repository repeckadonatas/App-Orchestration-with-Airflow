#!/usr/bin/env sh

echo "Restarting app and airflow Docker containers..." \

docker compose -f docker-compose.app.yaml -p project_container down;
docker compose -f docker-compose.airflow.yaml -p arflow_container down;
docker compose -f docker-compose.app.yaml -p project_container -d;
docker compose -f docker-compose.airflow.yaml -p arflow_container -d;