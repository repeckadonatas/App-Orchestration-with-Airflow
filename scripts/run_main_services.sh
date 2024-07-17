#!/usr/bin/env bash

cd /mnt/d/TURING/PROJECTS/MODULE_3/Job-Application-System

echo "Running cron-job of main service 'project-app'"

docker compose restart project-app
