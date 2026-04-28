"""
CSJ Pipeline DAG — extracts Canada Summer Jobs data from Google Sheets
and loads it into a Delta table on Databricks (bronze layer).
"""

import os
import json
import time
import base64
import tempfile

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
CREDENTIALS_PATH = "/opt/airflow/google-credentials.json"

# Where the Parquet file lands on DBFS before the bronze notebook reads it
DBFS_UPLOAD_PATH = "/FileStore/csj_pipeline/raw_funding.parquet"

# Path to notebooks in Databricks Repos
NOTEBOOK_BASE = "/Repos/masha.shmidt.04@gmail.com/summer-jobs-yres-data-platform/notebooks"

HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}


# Extract from Google Sheets and upload to DBFS 

def extract_and_upload(**context):
    """
    Reads all rows from the Google Sheet, converts to Parquet,
    and uploads to Databricks DBFS via the REST API.
    This runs inside the Airflow container — no Spark needed.
    """
    # Authenticate with Google Sheets using service account credentials
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(
        json.load(open(CREDENTIALS_PATH)), scopes=scopes
    )
    gc = gspread.authorize(creds)

    # Fetch all data from the worksheet
    sheet = gc.open_by_key(GOOGLE_SHEET_ID)
    worksheet = sheet.worksheet("AB, BC, ON")
    data = worksheet.get_all_values()

    df = pd.DataFrame(data[1:], columns=data[0])
    print(f"Extracted {len(df)} rows from Google Sheets")

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        df.to_parquet(tmp.name, index=False)
        tmp_path = tmp.name

    # Upload to DBFS using the streaming API
    dbfs_api = f"{DATABRICKS_HOST}/api/2.0/dbfs"

    # Create target directory on DBFS
    requests.post(
        f"{dbfs_api}/mkdirs",
        headers=HEADERS,
        json={"path": os.path.dirname(DBFS_UPLOAD_PATH)},
    )

    with open(tmp_path, "rb") as f:
        file_bytes = f.read()

    # Open a DBFS upload handle
    handle_resp = requests.post(
        f"{dbfs_api}/create",
        headers=HEADERS,
        json={"path": DBFS_UPLOAD_PATH, "overwrite": True},
    )
    handle_resp.raise_for_status()
    handle = handle_resp.json()["handle"]

    # Stream file
    chunk_size = 1024 * 1024
    for i in range(0, len(file_bytes), chunk_size):
        chunk = base64.b64encode(file_bytes[i : i + chunk_size]).decode()
        requests.post(
            f"{dbfs_api}/add-block",
            headers=HEADERS,
            json={"handle": handle, "data": chunk},
        ).raise_for_status()

    # Finalize the upload
    requests.post(
        f"{dbfs_api}/close", headers=HEADERS, json={"handle": handle}
    ).raise_for_status()

    os.unlink(tmp_path)
    print(f"Uploaded parquet to dbfs:{DBFS_UPLOAD_PATH}")


# Helper function to run a Databricks notebook via REST API

def _run_notebook(notebook_path, parameters=None):
    """
    Submits a one-time notebook run to Databricks using the Jobs API,
    then polls every 30s until it completes or fails.
    Uses a single-node cluster (num_workers=0) to minimize cost.
    """
    run_payload = {
        "new_cluster": {
            "spark_version": "14.3.x-scala2.12",
            "num_workers": 0,
            "node_type_id": "i3.xlarge",
        },
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": parameters or {},
        },
    }

    # Submit the run
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit",
        headers=HEADERS,
        json=run_payload,
    )
    resp.raise_for_status()
    run_id = resp.json()["run_id"]
    print(f"Submitted run {run_id} for {notebook_path}")

    # Poll until the run finishes
    while True:
        status_resp = requests.get(
            f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
            headers=HEADERS,
            params={"run_id": run_id},
        )
        status_resp.raise_for_status()
        state = status_resp.json()["state"]
        life_cycle = state["life_cycle_state"]
        print(f"  Run {run_id}: {life_cycle}")

        if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result_state = state.get("result_state", "UNKNOWN")
            if result_state != "SUCCESS":
                msg = state.get("state_message", "")
                raise RuntimeError(
                    f"Notebook {notebook_path} failed: {result_state} - {msg}"
                )
            print(f"  Run {run_id}: SUCCESS")
            return

        time.sleep(30)


# Trigger bronze notebook on Databricks

def run_bronze(**context):
    """Triggers the bronze ingestion notebook, passing the DBFS parquet path."""
    _run_notebook(
        f"{NOTEBOOK_BASE}/01_bronze_ingestion",
        parameters={"input_path": f"dbfs:{DBFS_UPLOAD_PATH}"},
    )
def run_silver(**context):
    """Triggers the silver cleaning notebook, passing the DBFS parquet path."""
    _run_notebook(
        f"{NOTEBOOK_BASE}/02_silver_cleaning",
        parameters={"input_path": f"dbfs:{DBFS_UPLOAD_PATH}"},
    )
def run_gold(**context):
    """Triggers the gold aggregation notebook, passing the DBFS parquet path."""
    _run_notebook(
        f"{NOTEBOOK_BASE}/03_gold_aggregation",
        parameters={"input_path": f"dbfs:{DBFS_UPLOAD_PATH}"},
    )


# DAG definition

ALERT_EMAIL = os.environ.get("ALERT_EMAIL")

default_args = {
    "owner": "mariia",
    "email": [ALERT_EMAIL],
    "email_on_failure": True,   # sends email if a task fails
    "email_on_retry": False,
    "retries": 1,               # retry once before marking as failed
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="csj_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,     # manual trigger only
    catchup=False,
    tags=["csj"],
) as dag:

    extract_upload = PythonOperator(
        task_id="extract_google_sheets_to_dbfs",
        python_callable=extract_and_upload,
    )
    bronze = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze,
    )
    silver = PythonOperator(
        task_id="silver_cleaning",
        python_callable=run_silver,
    )
    gold = PythonOperator(
        task_id="gold_aggregation",
        python_callable=run_gold,
    )
    extract_upload >> bronze >> silver >> gold
