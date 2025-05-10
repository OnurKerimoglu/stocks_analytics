#!/bin/bash
set -e

REPO_DIR="stocks-analytics"

echo "Shutting down Airflow..."
cd $REPO_DIR
docker compose -f Docker/airflow/docker-compose.yaml down

# echo "Cleaning up..."
# rm -rf $REPO_DIR