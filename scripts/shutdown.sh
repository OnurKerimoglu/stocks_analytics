#!/bin/bash
set -e

USER_NAME="onur"  #to know where the REPO_DIR is
LOG_FILE="/var/log/shutdown-script.log"
REPO_DIR="/home/${USER_NAME}/stocks-analytics"

if [ -f ${LOG_FILE} ]; then
  echo "removing the log file: ${LOG_FILE}"  
  rm $LOG_FILE
fi

# start logging
{
echo "[$(date)] --- Shutdown script begins ---"
echo "Shutting down Airflow..."
cd $REPO_DIR
docker compose -f Docker/airflow/docker-compose.yaml down

# echo "Cleaning up..."
# rm -rf $REPO_DIR
} >> $LOG_FILE 2>&1
