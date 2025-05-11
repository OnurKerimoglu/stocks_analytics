#!/bin/bash
set -e

LOG_FILE="/var/log/startup-script.log"
REPO_DIR="stocks-analytics"

{
echo "[$(date)] --- Shutdown script begins ---"
echo "Shutting down Airflow..."
cd $REPO_DIR
docker compose -f Docker/airflow/docker-compose.yaml down

# echo "Cleaning up..."
# rm -rf $REPO_DIR
} >> $LOG_FILE 2>&1