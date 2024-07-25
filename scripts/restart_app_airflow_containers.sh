#!/usr/bin/env sh

echo "Restarting app and airflow Docker containers..."
docker compose -f docker-compose.app.yaml -p project_container down
docker compose -f docker-compose.airflow.yaml -p airflow_container down

echo "Starting app Docker container..."
docker compose -f docker-compose.app.yaml -p project_container up -d

echo "Starting airflow Docker container..."
docker compose -f docker-compose.airflow.yaml -p airflow_container up -d

echo "Displaying logs..."
docker compose -f docker-compose.app.yaml -p project_container logs -f &
docker compose -f docker-compose.airflow.yaml -p airflow_container logs -f