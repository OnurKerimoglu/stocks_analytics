# Detailed Documentation

## Solution Architecture

### Alternatives

For the coupled operation of ML and Analytics platforms, following altenative architectures were considered (see the [blogpost](https://medium.com/@kerimoglu.o/an-ml-enhanced-analytics-platform-79e79709c9e6)).

#### The Monolithic Architecture

```mermaid
---
config:
  layout: dagre
  look: handDrawn
  theme: light
---
flowchart LR
 subgraph Ext["External Systems"]
        DataSrc["Data Sources"]
  end
 subgraph Product["Analytics + ML Platform"]
        Acore["Ingestion, Transform, Warehouse"]
        MLcore["Training, Monitoring, Registry, Inference"]
        Dashboards["Dashboards"]
  end
    DataSrc -- Raw Data --> Acore
    Acore -- Cleaned Data --> MLcore
    Acore -- Refined Data --> Dashboards
    MLcore -- Forecasts --> Acore
    Dashboards -- Visualizations --> User(["User"])
```

#### Microservice Architecture
```mermaid
---
config:
  layout: dagre
  look: handDrawn
  theme: light
---
flowchart LR
  User(["User"])
  subgraph Ext["External Systems"]
    DataSrc["Data Sources"]
  end


    S_Ingest["Ingestion Service"]
    S_Transform["Transform Service"]
    S_Warehouse["Lake/Warehouse"]
    S_UI["Dashboards"]
    S_Train["Training Service"]
    S_Reg["Model Registry Service"]
    S_API["Forecasting API"]
    S_Mon["Monitoring / Drift Service"]


  %% External
  DataSrc -- Raw Data--> S_Ingest
  S_Warehouse -- Normalized Data --> S_Train

  %% Analytics flow
  S_Ingest -- Normalized Data --> S_Warehouse
  S_Warehouse -- Normalized Data --> S_Transform
  S_Transform -- Refined Data -->  S_Warehouse
  S_Warehouse -- Refined Data --> S_UI
  S_UI --> User

  %% ML flow
  S_Train -- Model --> S_Reg
  S_Reg -- Model --> S_API

  %% Integration seam
  S_Warehouse -- Recent Data --> S_API
  S_API -- Forecasts --> S_Warehouse

  %% Ops
  S_Warehouse <-- Features, Estimations --> S_Mon
  S_Reg -- Models --> S_Mon -- Trigger --> S_Train
```

#### The Macroservice Architecture
```mermaid
---
config:
  layout: dagre
  look: handDrawn
  theme: light
---
flowchart LR
  User(["User"])
  subgraph Ext["External Systems"]
    DataSrc["Data Sources"]
  end
  subgraph Analytics["Analytics Platform"]
    Acore["Ingestion, Transform, Warehouse"]
  end
  subgraph ML["ML Platform"]
    API["Forecasting API"]
    MLcore["Training, Monitoring, Registry"]
  end
  DataSrc -- Raw Data --> Acore
  DataSrc -- Raw Data --> MLcore
  Acore -- Refined Data --> Dashboards
  MLcore -- Model --> API
  Acore -- Past Data --> API
  API -- Forecasts -->  Acore
  Dashboards -- Visualizations --> User
```

 ## Checklist:
  

| Option \ Factor          | Low complexity | Large team | Deep platform expertise | Ship fast |  Low ops budget | Indep. scaling needs |
| ------------------------ | -------------: | ---------: | ----------------------: | ----------------: |  -------------: | ------------------------------: |
| **Monolith**             |              ✓ |          ✗ |                       ✗ |                 ✓ |                                ✓ |                               ✗ |
| **Microservices** |              ✗ |          ✓ |                       ✓ |                 ✗ |                                ✗ |                               ✓ |
| **Macroservices**  |              ✓ |          ✓ |                       ✓ |                 ✓ |                               ✓ |                               ✗ |

Based on these considerations, the Macroservice architecture was adopted. 

## Detailed Architecture of the Analytics Platform
Here the L3-flowchart of the solution architecture of the Analytics Platform specifically:

<img src="images/Solution_Architecture_GCP.png" alt="data_lake_structure" width="1000"/>  
<br/><br/>

3 main environments can be identified:
1. A local development environment (green box): this is where the code is developed/maintained and necessary cloud services (via [Terraform](#terraform)) are managed
2. The cloud environment (blue box): currently the [Google Cloud Platform](https://cloud.google.com/) (see: [cloud services](#cloud-services)), where source code is pulled from GitHub (automated via [scripts/startup.sh](scripts/startup.sh), which is set with the creation of the compute engine resource with Terraform), ran in a [Docker](https://www.docker.com/) container (see [Docker](Docker/airflow)), and data is persisted and processed in a data lake and data warehouse
3. [Streamlit Web-App](#streamlit-web-app) (orange box) that contains publicly accessible dashboards and admin interfaces to browse and manage ETFs to track.

