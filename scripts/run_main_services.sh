#!/bin/bash

cd /mnt/c/Users/HP/Desktop/TURING/Projects/'Module 2'/DE-Capstone-2

echo "Running cron-job of main services 'project-db' and 'project-app'"

docker compose restart project-db project-app
