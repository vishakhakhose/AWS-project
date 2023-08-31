#!/bin/bash
# entrypoint.sh
cd ${AIRFLOW_HOME}
# Perform any necessary setup
# Initialize the Airflow database
airflow db init

# Start the Airflow scheduler in the background
airflow users create -u "airflow" -e "admin@dmin.com" -f "ad" -l "min" -p "password" -r "Admin"

airflow scheduler &> /dev/null &

# Start the Airflow webserver
exec airflow webserver
