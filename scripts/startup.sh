#!/bin/bash
set -e

LOG_FILE="/var/log/startup-script.log"
REPO_DIR="stocks-analytics"
REPO_URL="https://github.com/OnurKerimoglu/stocks_analytics.git"

# Start logging
{
echo "[$(date)] --- Startup script begins ---"

if [ -d "$REPO_DIR/.git" ]; then
  echo "Repo exists at $REPO_DIR. Pulling latest changes..."
  cd $REPO_DIR
  git reset --hard
  git clean -fd
  git pull
  git submodule update --recursive --remote
else
  echo "Cloning repo for the first time to $REPO_DIR..."
  git clone --recursive $REPO_URL $REPO_DIR
fi

cd $REPO_DIR

echo "Starting Airflow..."
docker compose -f Docker/airflow/docker-compose.yaml build
docker compose -f Docker/airflow/docker-compose.yaml up

echo -e "AIRFLOW_UID=$(id -u)" > Docker/airflow/.env

echo "Done starting and setting up Airflow"
} >> "$LOG_FILE" 2>&1