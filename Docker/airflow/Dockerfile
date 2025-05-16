# First-time build can take upto 10 mins.

FROM apache/airflow:2.10.5

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq

# Installing Java and Spark: start
RUN apt-get install -y --no-install-recommends openjdk-17-jdk
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME="/opt/spark"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Port master will be exposed
ENV SPARK_MASTER_PORT="7077"
# Name of master container and also counts as hostname
ENV SPARK_MASTER_HOST="spark-master"

RUN mkdir -p ${SPARK_HOME}
# If it breaks in this step go to https://dlcdn.apache.org/spark/ and choose higher spark version instead
RUN curl https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz -o spark-3.5.5-bin-hadoop3.tgz \
    && tar xvzf spark-3.5.5-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf spark-3.5.5-bin-hadoop3.tgz

# Copy custom configuration for master url and events logging
# COPY Docker/airflow/config/spark-defaults.conf "${SPARK_HOME}/conf"

RUN chown -R airflow ${SPARK_HOME}

RUN apt update
RUN apt-get -y install procps
# Installing Java and Spark: end

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"

# install google cloud sdk
# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

USER root
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=516.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

# COPY scripts scripts
# RUN chmod +x scripts

USER $AIRFLOW_UID
