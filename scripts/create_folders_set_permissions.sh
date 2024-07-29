#!/usr/bin/env sh

AIRFLOW_GID=0

force_create_and_set_permissions() {
    sudo mkdir -p "$1"
    sudo chown -R "$(id -u):$AIRFLOW_GID" "$1"
    sudo chmod -R 775 "$1"
    echo "Created/Updated directory and set permissions for: $1"
}

if [ "$(id -u)" -ne 0 ]; then
    echo "This script must be run as root. Please use sudo."
    exit 1
fi

groupadd -g $AIRFLOW_GID airflow 2>/dev/null || true

usermod -a -G airflow "$(id -un)"

force_create_and_set_permissions "./logs"
force_create_and_set_permissions "./dags"
force_create_and_set_permissions "./backups"

if [ -e /var/run/docker.sock ]; then
    chmod 666 /var/run/docker.sock
    echo "Set permissions for Docker socket"
else
    echo "Docker socket not found at /var/run/docker.sock"
fi

echo "Setup complete. Please restart your shell or run 'newgrp airflow' to apply group changes."