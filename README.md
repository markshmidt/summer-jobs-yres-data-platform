# CSJ Pipeline

Airflow + Databricks pipeline that extracts Canada Summer Jobs (CSJ)
funding data from Google Sheets and processes it through a medallion
architecture (Bronze → Silver → Gold).

## Architecture

```
Google Sheets → [Airflow DAG] → Databricks Notebooks
                                  ├── 01_bronze_ingestion.py   (raw data ingestion)
                                  ├── 02_silver_cleaning.py    (data cleaning & transformation)
                                  └── 03_gold_aggregation.py   (business-level aggregation)
```

- **Airflow** orchestrates the pipeline as a DAG, running inside Docker containers with a LocalExecutor and PostgreSQL backend.
- **Databricks** executes the notebook jobs for each medallion layer.
- **Google Sheets** serves as the data source, accessed via `gspread` and service account credentials.

## Prerequisites

- Docker and Docker Compose
- A Databricks workspace with a SQL warehouse
- A Databricks personal access token
- A Google Cloud service account with access to the source Google Sheet

## Project Structure

```
csj-pipeline/
├── dags/
│   └── csj_pipeline_dag.py        # Airflow DAG definition
├── notebooks/
│   ├── 01_bronze_ingestion.py     # Raw ingestion from Google Sheets
│   ├── 02_silver_cleaning.py      # Data cleaning and validation
│   └── 03_gold_aggregation.py     # Aggregated business metrics
├── config/                        # Configuration files
├── plugins/                       # Custom Airflow plugins
├── logs/                          # Airflow logs (gitignored)
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── google-credentials.json        # Google service account key
└── .env                           # Environment variables
```

## Setup

### 1. Configure environment variables

Create a `.env` file in the project root:

```env
DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
DATABRICKS_TOKEN=<your-databricks-personal-access-token>
DATABRICKS_SQL_WAREHOUSE_ID=<your-sql-warehouse-id>
GOOGLE_SHEET_ID=<your-google-sheet-id>
```

### 2. Add Google credentials

Place your Google Cloud service account JSON key as `google-credentials.json` in the project root.

### 3. Build and start services

```bash
# Build the custom Airflow image
docker compose build

# Initialize the database and create the admin user
docker compose up airflow-init

# Start all services in background
docker compose up -d

# Check everything is running
docker compose ps
```

### 4. Access the Airflow UI

Open http://localhost:8081 and log in with:

- **Username:** `admin`
- **Password:** `admin`