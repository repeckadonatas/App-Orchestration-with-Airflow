#!/usr/bin/env sh

mkdir -p secrets

grep "^POSTGRES_USER=" .env | cut -d '=' -f2 > secrets/postgres_user.txt
grep "^POSTGRES_PASSWORD=" .env | cut -d '=' -f2 > secrets/postgres_password.txt
grep "^PGUSER=" .env | cut -d '=' -f2 > secrets/pguser.txt
grep "^PGPASSWORD=" .env | cut -d '=' -f2 > secrets/pgpassword.txt
grep "^PGPORT=" .env | cut -d '=' -f2 > secrets/pgport.txt
grep "^PGDATABASE=" .env | cut -d '=' -f2 > secrets/pgdatabase.txt
grep "^AIRFLOW__WEBSERVER__SECRET_KEY=" .env | cut -d '=' -f2 > secrets/airflow_secret_key.txt

echo "Secrets created successfully in the 'secrets' folder."