#!/bin/bash

POSTGRES_USER=$(grep "^POSTGRES_USER=" .env | cut -d '=' -f2)
POSTGRES_PASSWORD=$(grep "^POSTGRES_PASSWORD=" .env | cut -d '=' -f2)
PGHOST=$(grep "^PGHOST=" .env | cut -d '=' -f2)
PGPORT=$(grep "^PGPORT=" .env | cut -d '=' -f2)
PGDATABASE=$(grep "^PGDATABASE=" .env | cut -d '=' -f2)

echo -n "POSTGRES_USER" | docker secret create POSTGRES_USER -
echo -n "POSTGRES_PASSWORD" | docker secret create POSTGRES_PASSWORD -
echo -n "PGHOST" | docker secret create PGHOST -
echo -n "PGPORT" | docker secret create PGPORT -
echo -n "PGDATABASE" | docker secret create PGDATABASE -