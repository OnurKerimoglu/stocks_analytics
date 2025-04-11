# Stocks Analytics

## Problem Description

## Data Sources

### ETF Holding Compositions

### Stock Prices

### Stock Information

## Data Warehouse Design

## Tools and Technical Setup
### First steps
The repository should be cloned with the `--recursive` argument, i.e., `git clone --recursive git@github.com:OnurKerimoglu/stocks_analytics.git`, such that the [stocks_dbt](https://github.com/OnurKerimoglu/stocks_dbt.git) repository is pulled as a submodule into the `dbt` folder (see the [Airflow](#Airflow) section below for information on the integration of dbt with Airflow).

### Platform: Google Cloud
#### Terraform

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

## Data Ingestion

### Ingestion DAG
The ingestion DAG, i.e., [ingest_raw_data_dag](dags/ingest_raw_data_dag.py) looks like this:

<!---
![airflow ingestion dag](documentation/images/airflow_ingestion_dag.png)
-->

<img src="documentation/images/airflow_ingestion_dag.png" alt="ingestion dag" width="1200"/>

Involved tasks are as follows:

- 

## Data Transformations

### Ticker Transformations DAG
The [ticker_transformations_dag](dags/ticker_transformations_dag.py)) contains a single dbt task, [price_technicals_lastday.sql](dbt/stocks_dbt/models/stocks/price_technicals_lastday.sql):

<img src="documentation/images/airflow_ticker_transformations_dag.png" alt="ticker transformations dag" width="150"/>

The purpose of this task is, for each ticker in the stocks_raw.stocks_prices table, calculating technical indicators that can be summarized for the last day available (so that one record can be produced per ticker in the target table), and write these results into the stocks_refined_(dev/prod).price_technicals_lastday table. Currently, the only indicator calculated is the (Bollinger Band Strategy)[https://en.wikipedia.org/wiki/Bollinger_Bands], according to which,

$$
\textrm{BR} = 
\left\{
  \begin{array}{ c l }
    \textrm{sell} & \quad \textrm{if } P \geq \mu_{n}(P) + K\sigma_{n}(P) \\
    \textrm{buy} & \quad \textrm{if } P \leq \mu_{n}(P) - K\sigma_{n}(P) \\
    \textrm{hold}                 & \quad \textrm{otherwise}
  \end{array}
\right.
$$

where BR stands for the Bollinger Recommendation, P is the (closing) Price, $\mu_n(P)$ and $\sigma_n(P)$ are the $n$-day rolling average and standard deviation of Price for the time period, and $K$ is a factor (in the current implementation, $n$=30, $K$=2). As in the current implementation we need the BR only for the last day, the calculation truncates to simple average and standard deviation calculations for the chosen period ($n$=30 days).  

### ETF Transformations DAG
The []()

<img src="documentation/images/airflow_etf_transformations_dag.png" alt="etf transformations dag" width="320"/>