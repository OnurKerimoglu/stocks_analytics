#!/bin/bash
set -e

LOG_FILE="/var/log/startup-script.log"
# LOG_FILE="startup-script.log"
REPO_DIR="stocks-analytics"
REPO_URL="https://github.com/OnurKerimoglu/stocks_analytics.git"

rm $LOG_FILE

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
