#!/bin/bash
set -e

# Initialize or upgrade the Airflow database (only in webserver to avoid race conditions)
if [ "$1" = "webserver" ]; then
    echo "Initializing/Migrating Airflow database..."
    airflow db migrate # create metadata in postgre

    # Create admin user if it does not exist
    echo "Creating admin user..."
    airflow users create \
        --username airflow \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password airflow || true
elif [ "$1" = "scheduler" ]; then
    echo "Waiting for Airflow Webserver to initialize database..."
    until curl -s http://airflow-webserver:8080/health >/dev/null; do
        echo "Webserver not ready yet, sleeping 3 seconds..."
        sleep 3
    done
    echo "Airflow Webserver is ready. Database migrated successfully!"
fi

echo "Airflow setup completed. Launching command: $@"
exec airflow "$@"
