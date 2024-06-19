#!/bin/bash

cd /mnt/c/Users/HP/Desktop/TURING/Projects/'Module 2'/DE-Capstone-2

echo "Running cron-job of all services"

docker compose restart backup-app
