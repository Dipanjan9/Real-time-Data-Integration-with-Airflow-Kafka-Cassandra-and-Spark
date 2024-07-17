#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  pip install --user -r /opt/airflow/requirements.txt
fi

# Replace `airflow db init` with `airflow db migrate`
airflow db migrate

# Continue with other initialization steps if needed
# For example, creating an admin user (if not already done)
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

exec airflow webserver

