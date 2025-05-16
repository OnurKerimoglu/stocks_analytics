#!/bin/bash
set -e

USER_NAME="onur"  #to know where the REPO_DIR is
LOG_FILE="/var/log/startup-script.log"
REPO_DIR="/home/${USER_NAME}/stocks-analytics"
REPO_URL="https://github.com/OnurKerimoglu/stocks_analytics.git"

if [ -f $LOG_FILE ]; then
  echo "removing log file: ${LOG_FILE}"
  rm $LOG_FILE
fi

# Start logging
{
echo "[$(date)] --- Startup script begins ---"

echo "Repo exists at $REPO_DIR. Pulling latest changes..."
cd $REPO_DIR
git reset --hard
git clean -fd
git pull
git submodule update --recursive --remote

echo "Rebuilding and starting Airflow..."
docker compose -f Docker/airflow/docker-compose.yaml build
docker compose -f Docker/airflow/docker-compose.yaml up -d

echo "Done rebuilding and starting  Airflow"
} >> "$LOG_FILE" 2>&1
