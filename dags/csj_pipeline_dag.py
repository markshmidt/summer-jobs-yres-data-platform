"""
CSJ Pipeline DAG — extracts Canada Summer Jobs data from Google Sheets
and loads it through a medallion architecture on Databricks.

Flow: Google Sheets → bronze Delta table (via SQL API) → silver → gold
Triggered manually. Sends email alerts on task failure.
"""

import os
import json
import time

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
DATABRICKS_SQL_WAREHOUSE_ID = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
CREDENTIALS_PATH = "/opt/airflow/google-credentials.json"

NOTEBOOK_BASE = "/Workspace/Users/masha.shmidt.04@gmail.com/summer-jobs-yres-data-platform/notebooks"

HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}


def _run_sql(statement):
    """Execute a SQL statement on Databricks via the SQL Statement API."""
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/sql/statements",
        headers=HEADERS,
        json={
            "warehouse_id": DATABRICKS_SQL_WAREHOUSE_ID,
            "statement": statement,
            "wait_timeout": "50s",
        },
    )
    resp.raise_for_status()
    result = resp.json()
    state = result.get("status", {}).get("state")
    if state != "SUCCEEDED":
        error = result.get("status", {}).get("error", {}).get("message", "Unknown error")
        raise RuntimeError(f"SQL failed: {error}")
    return result


# Extract from Google Sheets and load into bronze Delta table via SQL

def extract_and_load_bronze(**context):
    """
    Reads all rows from the Google Sheet and writes them directly
    into the bronze.raw_funding Delta table using the SQL Statement API.
    No DBFS upload needed.
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

    header = data[0][:6]
    rows = [row[:6] for row in data[1:]]
    print(f"Extracted {len(rows)} rows from Google Sheets")

    # Create bronze schema and table
    _run_sql("CREATE SCHEMA IF NOT EXISTS bronze")
    _run_sql("""
        CREATE TABLE IF NOT EXISTS bronze.raw_funding (
            `Program Year / Année du programme` STRING,
            `Region / Région` STRING,
            `Activity Constituency` STRING,
            `Organization Common Name / Nom commun de l organisme` STRING,
            `Amount Paid / Montant payé` STRING,
            `Confirmed Jobs Created / Emplois confirmés créés` STRING,
            _ingestion_timestamp TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion' = '2',
            'delta.minWriterVersion' = '5'
        )
    """)

    # Truncate before full reload
    _run_sql("TRUNCATE TABLE bronze.raw_funding")

    # Insert in batches 
    batch_size = 2000
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values_list = []
        for row in batch:
            escaped = [v.replace("\\", "\\\\").replace("'", "''") for v in row]
            values_str = ", ".join(f"'{v}'" for v in escaped)
            values_list.append(f"({values_str}, current_timestamp())")

        values_sql = ",\n".join(values_list)
        _run_sql(f"""
            INSERT INTO bronze.raw_funding VALUES
            {values_sql}
        """)

        print(f"  Inserted rows {i + 1} to {min(i + batch_size, len(rows))}")

    print(f"Bronze load complete: {len(rows)} rows -> bronze.raw_funding")


# Helper function to run a Databricks notebook via REST API

def _run_notebook(notebook_path, parameters=None):
    """
    Submits a one-time notebook run to Databricks using the Jobs API,
    then polls every 30s until it completes or fails.
    """
    run_payload = {
        "tasks": [{
            "task_key": "notebook_run",
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": parameters or {},
                "source": "WORKSPACE",
            },
        }],
    }

    # Submit the run
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit",
        headers=HEADERS,
        json=run_payload,
    )
    if resp.status_code != 200:
        print(f"Databricks API error: {resp.status_code} - {resp.text}")
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


# Trigger silver/gold notebooks on Databricks

def run_silver(**context):
    """Triggers the silver cleaning notebook — reads from bronze Delta table."""
    _run_notebook(f"{NOTEBOOK_BASE}/02_silver_cleaning")


def run_gold(**context):
    """Triggers the gold aggregation notebook — reads from silver Delta table."""
    _run_notebook(f"{NOTEBOOK_BASE}/03_gold_aggregation")


# DAG definition

ALERT_EMAIL = os.environ.get("ALERT_EMAIL")

default_args = {
    "owner": "mariia",
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="csj_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["csj"],
) as dag:

    bronze = PythonOperator(
        task_id="extract_and_load_bronze",
        python_callable=extract_and_load_bronze,
    )

    silver = PythonOperator(
        task_id="silver_cleaning",
        python_callable=run_silver,
    )

    gold = PythonOperator(
        task_id="gold_aggregation",
        python_callable=run_gold,
    )

    bronze >> silver >> gold
