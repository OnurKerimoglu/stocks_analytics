# Detailed Documentation

## Solution Architecture
Here the illustration of solution architecture high-level flowchart:

<img src="images/Solution_Architecture_GCP.png" alt="data_lake_structure" width="1000"/>  
<br/><br/>

3 main environments can be identified:
1. A local development environment (green box): this is where the code is developed/maintained and necessary cloud services (via [Terraform](#terraform)) are managed
2. The cloud environment (blue box): currently the [Google Cloud Platform](https://cloud.google.com/) (see: [cloud services](#cloud-services)), where source code is pulled from GitHub (automated via [scripts/startup.sh](scripts/startup.sh), which is set with the creation of the compute engine resource with Terraform), ran in a [Docker](https://www.docker.com/) container (see [Docker](Docker/airflow)), and data is persisted and processed in a data lake and data warehouse
3. [Streamlit Web-App](#streamlit-web-app) (orange box) that contains publicly accessible dashboards and admin interfaces to browse and manage ETFs to track.