# Stocks Analytics

## Motivation and Objective

## Data Sources

### ETF Holding Compositions

### Stock Prices

### Stock Information

## Data Warehouse Design

## Tools and Technical Setup
### First steps
The repository should be cloned with the `--recursive` argument, i.e., `git clone --recursive git@github.com:OnurKerimoglu/stocks_analytics.git`, such that the [stocks_dbt](https://github.com/OnurKerimoglu/stocks_dbt.git) repository is pulled as a submodule into the `dbt` folder (see the [Airflow](#Airflow) section below for information on the integration of dbt with Airflow).

### Terraform

### dlt
Some part of the ingestion is done via dlt (data load tool), orchestrated with Airflow. 

### dbt
Transformations are done with dbt (data build tool), orchestrated with Airflow.

### Airflow

1. build and run the docker image (see https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html for general information on how to run airflow with docker compose. Here project specific steps are provided). 
    -  Under the project root, issue:
        - `docker compose -f Docker/airflow/docker-compose.yaml build` to build the containers
        - `docker compose -f Docker/airflow/docker-compose.yaml up` to start the containers
    - Save the Airflow UUID (output of `echo -e "AIRFLOW_UID=$(id -u)"`) into Docker/airflow/.env together with other parameters that might be relevant (see the [.env_example](Docker/airflow/.env_example))

2. export dlt_settings: In Airflow UI, under Admin > Variables create a `dlt_secrets_toml' variable with the contents of the secret.toml (under the default ~/.dlt folder), i.e.
```
destination.bigquery]
location = ****

[destination.bigquery.credentials]
project_id = ****
private_key = ****
client_email = ****
``` 
3. no additional configuration step is needed for dbt. For info, the dbt models are made available to Airflow while building the container with [docker-compose.yaml](Docker/airflow/docker-compose.yaml) by mounting the `dbt` folder  to `/opt/airflow/dbt`, which contains the dbt repository [stocks_dbt](https://github.com/OnurKerimoglu/stocks_dbt.git) as a submodule, which in turn contains the (profiles.yml) file inside a [config](dbt/stocks/dbt/config) folder (which by default is located under the dbt folder, e.g., $HOME/.dbt on Unix systems.). This non-default location for the profiles.yml file requires its specification while making a call to the dbt client (e.g., as in `bash_command=f"dbt run -s <model-name>  --profiles-dir {dbt_dir}/config --project-dir {dbt_dir}"`, where, `dbt_dir` points to `/opt/airflow/dbt/stocks_dbt`). 

## Data Ingestion Pipeline

### Ingestion DAG
The ingestion DAG, i.e., [ingest_raw_data_dag](dags/ingest_raw_data_dag.py) looks like this:

![airflow ingestino dag](documentation/images/airflow_ingestion_dag.png)

The involved tasks are as follows:

