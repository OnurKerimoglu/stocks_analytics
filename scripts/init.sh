#!/bin/bash
set -e
INIT_FLAG="${HOME}/.VM_init-complete"

if [[ -f "$INIT_FLAG" ]]; then
  echo "[INIT] Already initialized, skipping."
  exit 0
fi

USER_NAME="onur"  #i.e., $USER that will be operating airflow
LOG_FILE="/var/log/init-script.log"
# LOG_FILE="init-script.log"
REPO_DIR="stocks-analytics"
REPO_URL="https://github.com/OnurKerimoglu/stocks_analytics.git"

# Start logging
{
echo "[$(date)] --- Startup script begins ---"

# installing git, docker, docker-compose
apt update
apt -y install git
  echo "installing docker compose"
  sudo apt-get -y install ca-certificates curl gnupg
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  sudo chmod a+r /etc/apt/keyrings/docker.gpg
  # Add the repository to Apt sources:
  echo \
    "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
    "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt update
  apt -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
#fi
sudo service docker start
sudo usermod -aG docker ${USER_NAME}
# here $USER won't work, as the init.sh will run as root during the creation of the VM, and not as $USER

if ! [ -d "/secrets/gcp-keys" ]; then
  echo "creating folder gcp-keys"
  sudo mkdir -p /secrets/gcp-keys
fi
shopt -s nullglob
json_files=(*.json)
if [ ${#json_files[@]} -gt 0 ]; then
    echo "Moving *.json files to gcp-keys folder"
    sudo mv -- "${json_files[@]}" /secrets/gcp-keys/
fi

echo "Cloning repo for the first time to $REPO_DIR..."
git clone --recursive $REPO_URL $REPO_DIR

echo "Starting Airflow..."
cd $REPO_DIR
docker compose -f Docker/airflow/docker-compose.yaml build
docker compose -f Docker/airflow/docker-compose.yaml up -d

# sleep 180
echo -e "AIRFLOW_UID=$(id -u)" > Docker/airflow/.env

docker compose -f Docker/airflow/docker-compose.yaml down
echo "Done starting and setting up Airflow"
} >> "$LOG_FILE" 2>&1

# Mark init as done
touch "$INIT_FLAG"
